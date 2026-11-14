// order-service: owns orders. Reads product name and price from catalog-service through a
// resilient client, confirms orders and publishes OrderConfirmed through a transactional outbox.
// Lab 3: resilience on order -> catalog, JSON logs with trace id, RED metrics, liveness/readiness.
const express = require('express');
const { context, propagation } = require('@opentelemetry/api');
const log = require('./logger');
const { httpMetrics, metricsEndpoint } = require('./metrics');
const catalog = require('./catalog-client');
const { pool, initSchema, loadOrder } = require('./db');
const { startRelay } = require('./relay');
const { consume } = require('./kafka-consumer');
const { startSaga, onPaymentEvent, expireSagas } = require('./saga');

const PORT = Number(process.env.PORT || 8080);
if (!process.env.CATALOG_URL) {
  log.error('CATALOG_URL is not set');
  process.exit(1);
}

const app = express();
app.use(express.json());
app.use(httpMetrics);
app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => log.info({ method: req.method, path: req.originalUrl, status: res.statusCode, duration_ms: Date.now() - start }, 'request'));
  next();
});

const isId = (v) => /^[1-9][0-9]{0,9}$/.test(String(v));

// Liveness: the process only. Never the database, Kafka or the catalog.
app.get('/health/live', (req, res) => res.json({ status: 'ok' }));

// Readiness: the database only. NOT catalog-service: if the catalog is down, order-service must stay
// in the load balancer and answer 503 fast thanks to the circuit breaker.
async function ready(req, res) {
  try {
    await pool.query('SELECT 1');
    res.json({ status: 'ready' });
  } catch (err) {
    log.warn({ error: err.message }, 'not ready: database not reachable');
    res.status(503).json({ status: 'not ready' });
  }
}
app.get('/health/ready', ready);
app.get('/health', ready);
app.get('/metrics', metricsEndpoint);

// Fallback: fail fast and clearly. No invented price: selling at an old price is a business
// decision nobody has taken.
function catalogFailure(res, err) {
  if (err instanceof catalog.HttpError && err.status === 404) {
    return res.status(422).json({ error: 'product does not exist' });
  }
  // circuit open, bulkhead full, budget expired or attempts exhausted
  log.error({ dependency: 'catalog-service', circuit: catalog.circuitState(), code: err.code, error: err.message }, 'catalog not available');
  res.set('Retry-After', String(catalog.retryAfterSeconds));
  return res.status(503).json({ error: 'catalog temporarily unavailable, retry later' });
}

function validOrder(body) {
  return body && typeof body.customer_id === 'string' && body.customer_id.trim() !== ''
    && Array.isArray(body.lines) && body.lines.length > 0
    && body.lines.every((l) => l && isId(l.product_id) && Number.isInteger(l.quantity) && l.quantity > 0);
}

app.post('/orders', async (req, res) => {
  if (!validOrder(req.body)) {
    return res.status(400).json({ error: 'customer_id and at least one line with product_id and quantity > 0 are required' });
  }

  const lines = [];
  for (const line of req.body.lines) {
    try {
      const p = await catalog.getProduct(line.product_id);
      lines.push({ product_id: p.id, product_name: p.name, unit_price_cents: p.price_cents, quantity: line.quantity });
    } catch (err) {
      return catalogFailure(res, err);
    }
  }

  const db = await pool.connect();
  try {
    await db.query('BEGIN');
    const { rows } = await db.query(
      'INSERT INTO customer_order (customer_id) VALUES ($1) RETURNING id', [req.body.customer_id.trim()]);
    const orderId = rows[0].id;
    for (const l of lines) {
      await db.query(
        `INSERT INTO order_line (order_id, product_id, product_name, unit_price_cents, quantity)
         VALUES ($1, $2, $3, $4, $5)`,
        [orderId, l.product_id, l.product_name, l.unit_price_cents, l.quantity]);
    }
    await db.query('COMMIT');
    log.info({ order_id: orderId, lines: lines.length }, 'order created');
    res.status(201).json(await loadOrder(orderId, db));
  } catch (err) {
    await db.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    db.release();
  }
});

// Lab 4: confirming no longer confirms. It STARTS the saga and answers 202 Accepted:
// the order moves to PAYMENT_PENDING and the outcome arrives later, by event.
app.post('/orders/:id/confirm', async (req, res) => {
  if (!isId(req.params.id)) return res.status(404).json({ error: 'order not found' });
  const db = await pool.connect();
  try {
    await db.query('BEGIN');
    const carrier = {};
    propagation.inject(context.active(), carrier);       // the trace of the HTTP request follows the command
    const r = await startSaga(db, req.params.id, carrier.traceparent);
    await db.query('COMMIT');
    if (r.http === 404) return res.status(404).json({ error: 'order not found' });
    if (r.http === 202) log.info({ order_id: Number(req.params.id) }, 'saga started');
    res.status(r.http).json({ id: Number(req.params.id), status: r.status });
  } catch (err) {
    await db.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    db.release();
  }
});

app.get('/orders/:id', async (req, res) => {
  if (!isId(req.params.id)) return res.status(404).json({ error: 'order not found' });
  const order = await loadOrder(req.params.id);
  if (!order) return res.status(404).json({ error: 'order not found' });
  res.json(order);
});

app.use((err, req, res, next) => {
  log.error({ error: err.message }, 'unhandled error');
  res.status(500).json({ error: 'internal error' });
});

log.info({ config: { db_host: process.env.DB_HOST, kafka: process.env.KAFKA_BOOTSTRAP, relay: process.env.RELAY_ENABLED,
  catalog: catalog.config, circuit: { timeout_ms: process.env.CB_TIMEOUT_MS, reset_ms: process.env.CB_RESET_MS }, saga_timeout_s: process.env.SAGA_TIMEOUT_S } }, 'configuration');

initSchema()
  .then(() => {
    app.listen(PORT, '0.0.0.0', () => log.info({ port: PORT }, 'order-service listening'));
    startRelay(pool);

    // replies from payment-service
    consume({ topic: 'payment.events', groupId: 'order-service-saga', dlqTopic: 'orders.dlq', pool, handler: onPaymentEvent })
      .then(() => log.info('consumer: listening on payment.events'))
      .catch((err) => { log.error({ error: err.message }, 'saga consumer: startup failed'); process.exit(1); });

    // saga timeouts, every 5 seconds
    setInterval(async () => {
      let db;
      try {
        db = await pool.connect();
        await db.query('BEGIN');
        const n = await expireSagas(db);
        await db.query('COMMIT');
        if (n > 0) log.warn({ cancelled: n }, 'expired sagas cancelled');
      } catch (err) {
        if (db) await db.query('ROLLBACK').catch(() => {});
        log.error({ error: err.message }, 'saga timeout check failed');
      } finally {
        if (db) db.release();
      }
    }, 5000).unref();
  })
  .catch((err) => {
    log.error({ error: err.message }, 'startup failed, database not reachable');
    process.exit(1);
  });
