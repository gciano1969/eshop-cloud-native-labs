// Resilient client for the only synchronous call between services: order-service -> catalog-service.
// Four nested protections, from the outside in:
//   1 bulkhead (capacity)  2 circuit breaker  3 retry with backoff + jitter  4 timeout per attempt
const CircuitBreaker = require('opossum');
const log = require('./logger');
const { client } = require('./metrics');

const num = (name, def) => Number(process.env[name] ?? def);
const cfg = {
  url: process.env.CATALOG_URL,
  timeoutMs: num('CATALOG_TIMEOUT_MS', 800),
  retries: num('CATALOG_RETRIES', 2),
  baseMs: num('RETRY_BASE_MS', 100),
  capMs: num('RETRY_CAP_MS', 1000),
  resetMs: num('CB_RESET_MS', 15000),
};

class HttpError extends Error {
  constructor(status) { super(`catalog-service answered ${status}`); this.status = status; }
}
// Transient: network errors, timeouts, 502-503-504. A 404 or a 400 does not change by retrying.
const isTransient = (err) => !(err instanceof HttpError) || [502, 503, 504].includes(err.status);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function getOnce(id) {
  const res = await fetch(`${cfg.url}/products/${encodeURIComponent(id)}`, {
    signal: AbortSignal.timeout(cfg.timeoutMs),            // 4 · timeout per attempt
  });
  if (!res.ok) throw new HttpError(res.status);
  return res.json();
}

// 3 · retry with exponential backoff and full jitter: wait = random(0, min(cap, base * 2^attempt))
async function getWithRetry(id) {
  for (let attempt = 1; ; attempt++) {
    try {
      return await getOnce(id);
    } catch (err) {
      if (attempt > cfg.retries || !isTransient(err)) throw err;
      const wait = Math.random() * Math.min(cfg.capMs, cfg.baseMs * 2 ** attempt);
      log.warn({ dependency: 'catalog-service', attempt, wait_ms: Math.round(wait), error: err.message }, 'retrying the call');
      await sleep(wait);
    }
  }
}

// 2 · circuit breaker, with 1 · bulkhead (capacity)
const breaker = new CircuitBreaker(getWithRetry, {
  timeout: num('CB_TIMEOUT_MS', 2500),                     // overall budget, attempts included
  errorThresholdPercentage: num('CB_ERROR_PERCENT', 50),
  volumeThreshold: num('CB_VOLUME', 10),
  rollingCountTimeout: num('CB_WINDOW_MS', 10000),
  resetTimeout: cfg.resetMs,
  capacity: num('CB_CAPACITY', 50),
  // a 4xx is the caller's mistake, not a catalog failure: it must not open the circuit
  errorFilter: (err) => err instanceof HttpError && err.status < 500,
});

const state = new client.Gauge({ name: 'circuit_breaker_state', help: '0 closed, 1 half-open, 2 open', labelNames: ['dependency'] });
const calls = new client.Counter({ name: 'dependency_calls_total', help: 'Outcomes of the calls to dependencies', labelNames: ['dependency', 'outcome'] });
const dep = { dependency: 'catalog-service' };
state.set(dep, 0);
breaker.on('open', () => { state.set(dep, 2); log.error({ ...dep, circuit: 'open' }, 'circuit open'); });
breaker.on('halfOpen', () => { state.set(dep, 1); log.warn({ ...dep, circuit: 'half-open' }, 'circuit half-open: trial call'); });
breaker.on('close', () => { state.set(dep, 0); log.info({ ...dep, circuit: 'closed' }, 'circuit closed'); });
for (const outcome of ['success', 'failure', 'timeout', 'reject', 'semaphoreLocked']) {
  breaker.on(outcome, () => calls.inc({ ...dep, outcome }));
}

module.exports = {
  getProduct: (id) => breaker.fire(id),
  circuitState: () => (breaker.opened ? 'open' : breaker.halfOpen ? 'half-open' : 'closed'),
  retryAfterSeconds: Math.ceil(cfg.resetMs / 1000),
  config: cfg,
  HttpError,
};
