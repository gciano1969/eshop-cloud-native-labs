# Lab 3 — Resilience, observability, configuration, scaling

**Concepts:** timeouts, retry with exponential backoff and jitter, circuit breaker, bulkhead,
fallback; structured logs, W3C trace context, RED metrics, metric cardinality; externalised
configuration; liveness vs readiness; Horizontal Pod Autoscaler; network policies.

## Goal

Lab 3 makes the e-shop dependable without adding features: the call from order-service to
catalog-service becomes resilient, the three services become observable, configuration moves out of
the code, and catalog-service scales on its own.

Five new pieces:

- **externalised configuration**: one ConfigMap per service plus the existing Secrets; no value
  written in the code or set by hand with `oc set env`;
- **resilience on order → catalog**: timeout per attempt, two retries with exponential backoff and
  jitter, circuit breaker with a 503 fallback, a cap on concurrent calls;
- **JSON logs with trace id** in order-service, catalog-service and notification-service, propagated
  across Kafka too;
- **RED metrics** on `/metrics` in Prometheus format, including the circuit state;
- **proper probes** and a **Horizontal Pod Autoscaler** on catalog-service.

The lab is complete when:

- with catalog-service slowed down to 5 seconds, after a few seconds order-service answers 503 in
  under 10 ms instead of hanging;
- once the delay is removed, the circuit closes by itself without restarting anything;
- a non-existent product is not retried and does not open the circuit;
- the same trace id appears in the logs of order-service and catalog-service for an order creation,
  and in those of notification-service for a confirmation;
- with order-db stopped, order-service Pods leave the load balancer but are not restarted;
- under load catalog-service grows to 3 replicas and then shrinks back;
- changing a database password needs a rollout, not a new build;
- code, manifests, decisions and verifications are in Git, without Secrets.

Estimated time: 5–6 hours. The examples use Node.js with Express as before, plus four libraries:
[`opossum`](https://github.com/nodeshift/opossum) 10 for the circuit breaker,
[`pino`](https://getpino.io/) 10 for logs, [`prom-client`](https://github.com/siimon/prom-client)
15 for metrics, and the [OpenTelemetry SDK](https://opentelemetry.io/docs/languages/js/) 3.x for
the trace id. The resilience, logging, tracing and metrics code in this folder was run and checked
locally (results quoted below); the `oc` commands must be tried on your Sandbox.

```text
lab-3-resilience-observability/
├── catalog-service/        # + tracing.js, logger.js, metrics.js, injectable delay, live/ready probes
├── order-service/          # + catalog-client.js (resilience), tracing, logs, metrics, probes
├── notification-service/   # + trace context from Kafka, logs, metrics, retry with jitter
└── k8s/
    ├── config/             # new: one ConfigMap per service
    ├── secrets.example.yaml # Secrets with placeholder values; the real ones NEVER in Git
    ├── catalog-hpa.yaml    # new
    ├── servicemonitor.yaml # new, optional
    └── networkpolicy.yaml  # new, optional
```

## Prerequisites, Sandbox constraints and ground rules

Start from a working Lab 2: catalog-service, order-service and notification-service running, each
with its own PostgreSQL, Kafka in the namespace, code and manifests in Git.

| Constraint | Consequence for this lab |
| --- | --- |
| No cluster-admin rights | No operators: no OpenTelemetry Collector and no Tempo, so no UI for traces. The trace id shows up in the logs, which is what the lab needs. Network policies are optional (Step 7). |
| Monitoring of user projects | Check in Step 0: if you can create ServiceMonitors, metrics appear in the console under Observe → Metrics; otherwise read them with `curl` on `/metrics`, which is enough. |
| Quota of about 3 cores and 14 GB RAM | Three catalog replicas, plus Kafka, three services and three databases: set requests low (Step 5) and check the quota before the load test. |
| Pods stopped after 12 consecutive hours | No impact: circuit state and metrics are in memory and start from zero, as after any restart. |
| Expires after 30 days | Everything recreatable from Git: ConfigMaps, HPA, probes and resources in `k8s/`, Secrets described but not stored. |

Ground rules, the same as the previous labs:

- no real data, no real email addresses, no company code or credentials;
- passwords and keys generated for the exercise and never reused, including the fake email provider key;
- Secrets **excluded** from commits: the repository only holds an example with placeholders;
- the load test of Step 5 stays in your own namespace, lasts no more than five minutes and is stopped by hand.

## Step 0 — Restart from Lab 2

1. **Log in and check the state.**

```bash
oc project                                                 # <username>-dev
oc get deploy -l app.kubernetes.io/part-of=eshop          # 7 Deployments: 3 services, 3 databases, kafka
oc scale deploy -l app.kubernetes.io/part-of=eshop --replicas=1
oc describe quota                                          # note used and available CPU and memory
```

2. **Lab 2 test.** Create and confirm an order as in Step 5 of Lab 2 and check the notification
   arrives. If it does not, fix Lab 2 first.
3. **Permissions to check.** The answers decide two optional parts; note them in
   `docs/verifications-lab3.md`.

```bash
oc auth can-i create servicemonitors.monitoring.coreos.com   # yes: metrics in the console (Step 3)
oc auth can-i create networkpolicies                          # yes: Step 7 is possible
oc auth can-i create horizontalpodautoscalers                 # must be yes (Step 5)
oc get networkpolicy                                          # those already in the namespace
```

4. **Point the builds at this lab.**

```bash
REPO=https://github.com/<your-github-user>/eshop-cloud-native-labs
LAB=lab-3-resilience-observability
for s in catalog-service order-service notification-service; do
  oc patch bc/$s --type=merge -p "{\"spec\":{\"source\":{\"contextDir\":\"$LAB/$s\"}}}"
done
```

The builds start in Step 1, after the ConfigMaps exist.

## Step 1 — Externalised configuration

Every value that changes from one environment to another leaves the code and `oc set env`:
addresses, timeouts, circuit thresholds, log level. Credentials stay in the Secrets created by the
database templates; business rules stay in the code.

### One ConfigMap per service

[`k8s/config/order-config.yaml`](k8s/config/order-config.yaml):

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: order-config
  labels: { app.kubernetes.io/part-of: eshop }
data:
  OTEL_SERVICE_NAME: order-service
  LOG_LEVEL: info
  DB_HOST: order-db
  DB_PORT: "5432"
  KAFKA_BOOTSTRAP: kafka:9092
  RELAY_ENABLED: "true"
  CATALOG_URL: http://catalog-service:8080
  CATALOG_TIMEOUT_MS: "800"      # per attempt: just above the real catalog latency
  CATALOG_RETRIES: "2"           # extra attempts after the first
  RETRY_BASE_MS: "100"
  RETRY_CAP_MS: "1000"
  CB_TIMEOUT_MS: "2500"          # overall budget, attempts included
  CB_ERROR_PERCENT: "50"
  CB_VOLUME: "10"
  CB_WINDOW_MS: "10000"
  CB_RESET_MS: "15000"
  CB_CAPACITY: "50"              # bulkhead: concurrent calls to the catalog
```

The other two, [`catalog-config.yaml`](k8s/config/catalog-config.yaml) and
[`notification-config.yaml`](k8s/config/notification-config.yaml), follow the same pattern. Values are
strings: YAML would read `800` as a number and the ConfigMap would reject it, hence the quotes.

### Wire them to the Deployments

Variables written one by one in the Deployment **win** over those of the ConfigMap: remove them first,
otherwise the ConfigMap is silently ignored. Variables coming from a Secret (`DB_DATABASE_USER`,
`DB_DATABASE_PASSWORD`, `DB_DATABASE_NAME`) stay where they are.

```bash
oc apply -f $LAB/k8s/config/

# 1. see the current variables and remove those now in the ConfigMap
oc set env deploy/order-service --list
oc set env deploy/order-service CATALOG_URL- KAFKA_BOOTSTRAP- RELAY_ENABLED- DB_HOST- DB_PORT-

# 2. wire the ConfigMap with envFrom (repeat for catalog and notification)
oc patch deploy/order-service --type=json -p '[{"op":"add","path":"/spec/template/spec/containers/0/envFrom","value":[{"configMapRef":{"name":"order-config"}}]}]'

# 3. check every key arrives exactly once
oc set env deploy/order-service --list --resolve
```

Avoid `oc set env --from=configmap/…` here: it copies each key into the Deployment as a separate
variable, which is exactly the "written one by one" situation that hides later ConfigMap changes.

### The Secrets

The database Secrets exist from the previous labs. Add one for the fake email provider key of
notification-service:

```bash
oc create secret generic notification-email --from-literal=EMAIL_API_KEY="$(openssl rand -hex 16)"
oc set env deploy/notification-service --from=secret/notification-email
```

The repository only holds [`k8s/secrets.example.yaml`](k8s/secrets.example.yaml), with the keys and
placeholder values, so whoever recreates the environment knows what to create.

### Reading configuration in the code

Each service reads the values **once, at startup**, and writes them to the startup log, without
secrets. A ConfigMap change takes effect only with a rollout: that is deliberate, because a
configuration change is a release like any other and must leave a trace.

```bash
oc start-build catalog-service; oc start-build order-service; oc start-build notification-service
oc rollout restart deploy/order-service deploy/catalog-service deploy/notification-service
```

## Step 2 — Resilience on order → catalog

The call where order-service reads product name and price is the only synchronous one between the
e-shop services: this is where a slow catalog would block orders. It gets wrapped in four
protections:

```text
 POST /orders
   └─ 1 bulkhead      more than 50 calls in flight?         → reject at once (503)
      └─ 2 circuit    open?                                  → do not even try (503, Retry-After)
         └─ 3 retry   transient error (network, 502-504)?    → wait random(0, min(cap, base·2^n)), try again
            └─ 4 timeout  each single attempt: 800 ms
```

Read it from the outside in: if 50 calls are already in flight, refuse at once; if the circuit is
open, do not even start; inside the circuit, retry only transient errors; every single attempt has
its own timeout.

### The catalog client

`npm install opossum@10`, then a new module in order-service:
[`order-service/catalog-client.js`](order-service/catalog-client.js). Its core:

```js
// transient: network, timeouts, 502-503-504. A 404 or a 400 does not change by retrying.
const isTransient = (err) => !(err instanceof HttpError) || [502, 503, 504].includes(err.status);

async function getOnce(id) {
  const res = await fetch(`${cfg.url}/products/${encodeURIComponent(id)}`, {
    signal: AbortSignal.timeout(cfg.timeoutMs),       // 4 · timeout per attempt
  });
  if (!res.ok) throw new HttpError(res.status);
  return res.json();
}

// 3 · retry with exponential backoff and full jitter
async function getWithRetry(id) {
  for (let attempt = 1; ; attempt++) {
    try { return await getOnce(id); }
    catch (err) {
      if (attempt > cfg.retries || !isTransient(err)) throw err;
      const wait = Math.random() * Math.min(cfg.capMs, cfg.baseMs * 2 ** attempt);
      log.warn({ dependency: 'catalog-service', attempt, wait_ms: Math.round(wait) }, 'retrying the call');
      await sleep(wait);
    }
  }
}

// 2 · circuit breaker, with 1 · bulkhead (capacity)
const breaker = new CircuitBreaker(getWithRetry, {
  timeout: 2500, errorThresholdPercentage: 50, volumeThreshold: 10,
  rollingCountTimeout: 10000, resetTimeout: 15000, capacity: 50,
  errorFilter: (err) => err instanceof HttpError && err.status < 500,   // a 404 is not a catalog failure
});
```

The module also exports two metrics: `circuit_breaker_state` (0 closed, 1 half-open, 2 open) and
`dependency_calls_total` by outcome.

### The fallback in the route

Where Lab 1 called the catalog with `fetch`, the route now calls `catalog.getProduct(id)`. The chosen
fallback is to fail fast and clearly: no invented price, because selling at an old price is a business
decision nobody has taken.

```js
function catalogFailure(res, err) {
  if (err instanceof catalog.HttpError && err.status === 404) {
    return res.status(422).json({ error: 'product does not exist' });
  }
  // circuit open, bulkhead full, budget expired or attempts exhausted
  log.error({ dependency: 'catalog-service', circuit: catalog.circuitState(), error: err.message }, 'catalog not available');
  res.set('Retry-After', String(catalog.retryAfterSeconds));
  return res.status(503).json({ error: 'catalog temporarily unavailable, retry later' });
}
```

Four points to be able to explain:

- **Why the retry sits inside the circuit.** The circuit counts the whole sequence of attempts as one
  outcome; when it opens, the attempts stop. With the opposite order the retry would keep knocking on
  an open circuit.
- **Why `errorFilter`.** A non-existent product is the caller's mistake: it must not be retried and must
  not open the circuit, otherwise twenty bad orders would switch the catalog off for everyone. (opossum
  counts a filtered error as a `success` in its statistics.)
- **What the circuit timeout really does.** At 2.5 seconds opossum stops waiting and answers, but
  attempts already started finish on their own: the budget protects the customer, not the catalog. So
  the per-attempt timeouts must fit in the budget: 800 ms × 3 plus the waits stays under 2.5 s in most cases.
- **Only one layer retries.** The gateway and the customer do not retry; there is no service mesh in
  the namespace adding more attempts.

### The injectable delay in catalog-service

The tests need a slow catalog on demand, without touching the code: a ConfigMap variable. Probes do
not go through `/products`, so the Pod stays ready even when slow.

```js
const DELAY_MS = Number(process.env.CATALOG_DELAY_MS || 0);
if (DELAY_MS > 0) log.warn({ delay_ms: DELAY_MS }, 'injected delay active');
app.use('/products', (req, res, next) => (DELAY_MS > 0 ? setTimeout(next, DELAY_MS) : next()));
```

### notification-service retry

In Lab 2 the consumer retried three times with a fixed 2-second wait. Replace the fixed wait with the
same formula, reading `MAX_ATTEMPTS`, `RETRY_BASE_MS` and `RETRY_CAP_MS` from the ConfigMap; the rest
(DLQ, exit on persistent error) does not change.

```js
const wait = Math.random() * Math.min(cfg.capMs, cfg.baseMs * 2 ** attempt);
```

## Step 3 — JSON logs with trace id, RED metrics

The three services get the same three modules: [`tracing.js`](order-service/tracing.js) gives every
request a trace id, [`logger.js`](order-service/logger.js) writes one JSON line per event on stdout
with that trace id, [`metrics.js`](order-service/metrics.js) exposes metrics on `/metrics`.

```bash
npm install pino@10 prom-client@15 @opentelemetry/api @opentelemetry/sdk-node \
  @opentelemetry/auto-instrumentations-node @opentelemetry/sdk-trace
```

OpenTelemetry JS 3.x renamed the old `@opentelemetry/sdk-trace-base` package to
`@opentelemetry/sdk-trace`; with SDK 0.30x the old package no longer resolves alongside the new API.

### The trace id

`tracing.js` must load **before** any other module, because it has to hook `http`, Express, `fetch`,
`pg` and `pino` before they are used. In `package.json`:
`"start": "node --require ./tracing.js server.js"`.

```js
const { NodeSDK } = require('@opentelemetry/sdk-node');
const { getNodeAutoInstrumentations } = require('@opentelemetry/auto-instrumentations-node');
const { NoopSpanProcessor } = require('@opentelemetry/sdk-trace');

new NodeSDK({
  serviceName: process.env.OTEL_SERVICE_NAME,
  // no Collector in the Sandbox: spans are created (and give the logs their trace id) but not exported
  spanProcessors: [new NoopSpanProcessor()],
  instrumentations: [getNodeAutoInstrumentations({ '@opentelemetry/instrumentation-fs': { enabled: false } })],
}).start();
```

With a Collector available, replace the `NoopSpanProcessor` with an OTLP exporter and the traces show
up in Tempo or Jaeger: the service code does not change. HTTP calls between services propagate the
`traceparent` header by themselves, because the instrumentation also covers `fetch`.

### The logger

```js
module.exports = pino({
  level: process.env.LOG_LEVEL || 'info',
  base: { service: process.env.OTEL_SERVICE_NAME },
  timestamp: pino.stdTimeFunctions.isoTime,
  formatters: { level: (label) => ({ level: label }) },
});
```

Replace every `console.log` with `log.info({ ...fields }, 'message')`: domain data goes in the fields
(`order_id`, `product_id`), not in the text. The instrumentation adds `trace_id` and `span_id` by
itself. A line of order-service, from the local run of this code:

```json
{"level":"info","time":"2026-11-12T18:49:52.410Z","service":"order-service","trace_id":"d992fe94b93a5749cc5f4b59132470cc","span_id":"5c1d0e4b7d2a9f13","trace_flags":"01","order_id":1,"lines":1,"msg":"order created"}
```

### The trace id across Kafka

The Confluent Kafka client is not instrumented: the context travels by hand, in three places. With the
outbox it must be saved in the row, because the relay publishes later, outside the request.

```sql
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS traceparent TEXT;
```

```js
// 1. order-service, at confirmation: save the context with the event
const carrier = {};
propagation.inject(context.active(), carrier);
// INSERT INTO outbox (id, topic, msg_key, payload, traceparent) VALUES (..., carrier.traceparent)

// 2. order-service, in the relay: add the header to the message (SELECT traceparent too)
headers: { 'content-type': 'application/cloudevents+json', ...(r.traceparent && { traceparent: r.traceparent }) },

// 3. notification-service: resume the context and open a child span around the processing
const headers = Object.fromEntries(Object.entries(message.headers || {}).map(([k, v]) => [k, v?.toString()]));
const parent = propagation.extract(context.active(), headers);
return tracer.startActiveSpan('orders.events process', {}, parent, async (span) => {
  try { /* the Lab 2 handle() body */ } finally { span.end(); }
});
```

### The metrics

One histogram of the requests received is enough for the three RED metrics: its `_count` gives the
requests per second, the same `_count` filtered by `status` gives the errors, its buckets give the
duration percentiles.

```js
const httpDuration = new client.Histogram({
  name: 'http_server_duration_seconds',
  help: 'Duration of the HTTP requests received',
  labelNames: ['method', 'route', 'status'],
  buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
});

function httpMetrics(req, res, next) {
  const end = httpDuration.startTimer();
  res.on('finish', () => end({
    method: req.method,
    route: req.route ? req.baseUrl + req.route.path : 'unmatched',   // /orders/:id, never /orders/1042
    status: res.statusCode,
  }));
  next();
}
```

In `server.js`: `app.use(httpMetrics)` before the routes and `app.get('/metrics', metricsEndpoint)`.
The `route` line is the one to understand: using `req.path` would put every order id in a different
label value, and monitoring would fill up with series (cardinality).

Extra metrics per service, besides RED:

| Service | Metric | Type | Meaning |
| --- | --- | --- | --- |
| order-service | `circuit_breaker_state` | gauge | 0 closed, 1 half-open, 2 open (from Step 2) |
| order-service | `dependency_calls_total` | counter | outcomes of the calls to the catalog: success, failure, timeout, reject (a timeout also counts as a failure) |
| order-service | `outbox_pending` | gauge | outbox rows not yet published, updated by the relay every second |
| notification-service | `notifications_total` | counter | by `outcome`: sent, duplicate, dlq |

### In the console, if allowed

If in Step 0 `oc auth can-i create servicemonitors.monitoring.coreos.com` answered `yes`,
[`k8s/servicemonitor.yaml`](k8s/servicemonitor.yaml) makes the platform Prometheus scrape the metrics
and show them under Observe → Metrics. The Service must name its port (`8080-tcp` with `oc new-app`).

If not, read everything with `oc rsh deploy/order-service curl -s localhost:8080/metrics`: enough for the lab.

## Step 4 — Proper probes

In the previous labs each service had a single `/health`, used as readiness, checking the database.
Now there are two questions with two endpoints: liveness asks whether the process is alive, readiness
whether it can take traffic. The database goes **only** into readiness.

```js
// liveness: the process only. Never the database, never Kafka, never the catalog.
app.get('/health/live', (req, res) => res.status(200).json({ status: 'ok' }));

// readiness: the dependencies without which the service cannot work
app.get('/health/ready', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    // notification-service: also consumerReady, as in Lab 2
    res.status(200).json({ status: 'ready' });
  } catch (err) {
    res.status(503).json({ status: 'not ready' });
  }
});
```

catalog-service is **not** part of order-service's readiness: if the catalog is down, order-service must
stay in the load balancer and answer 503 fast, thanks to the circuit. Taking all order-service Pods out
of the load balancer would help nobody.

```bash
for s in order-service catalog-service notification-service; do
  oc set probe deploy/$s --remove --readiness --liveness
  oc set probe deploy/$s --readiness --get-url=http://:8080/health/ready \
    --initial-delay-seconds=5 --period-seconds=10 --failure-threshold=3
  oc set probe deploy/$s --liveness --get-url=http://:8080/health/live \
    --initial-delay-seconds=15 --period-seconds=20 --failure-threshold=3
done
```

The old `/health` stays as an alias of `/health/ready` while scripts still use it; remove it afterwards.

## Step 5 — HPA on catalog-service

catalog-service is the right candidate: it is stateless, receives mostly reads and its work is
computation, so CPU describes it well. notification-service is not: it waits on the provider, and its
natural ceiling is the 3 partitions of the topic.

### Requests first

The HPA measures CPU as a percentage of the container's request: without a request it cannot compute,
and the `TARGETS` column stays `<unknown>`. Low values help stay within the quota and see scaling with
little load.

```bash
oc set resources deploy/catalog-service --requests=cpu=100m,memory=128Mi --limits=cpu=500m,memory=256Mi
oc describe quota      # three replicas = 300m more requests and 1.5 cores more limits: check they fit
```

### The manifest

[`k8s/catalog-hpa.yaml`](k8s/catalog-hpa.yaml), applied with `oc apply -f $LAB/k8s/catalog-hpa.yaml`:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: catalog-service
spec:
  scaleTargetRef: { apiVersion: apps/v1, kind: Deployment, name: catalog-service }
  minReplicas: 1            # at least 2 in production; 1 here because of the quota
  maxReplicas: 3            # the ceiling protects catalog-db and the quota
  metrics:
  - type: Resource
    resource:
      name: cpu
      target: { type: Utilization, averageUtilization: 70 }
  behavior:
    scaleDown: { stabilizationWindowSeconds: 120 }   # scale down after 2 minutes instead of 5
```

After a minute `oc get hpa catalog-service` must show a value in `TARGETS`, for example `cpu: 3%/70%`.

### The load test

A temporary Pod in the namespace calls catalog-service in eight parallel loops for five minutes, then
stops and deletes itself. Watch the HPA from a second terminal.

```bash
# terminal 1
oc run load --image=registry.access.redhat.com/ubi9/ubi --restart=Never --rm -it -- \
  bash -c 'for i in $(seq 8); do (timeout 300 bash -c "while true; do curl -s -o /dev/null http://catalog-service:8080/products/1; done") & done; wait'

# terminal 2
oc get hpa catalog-service -w
oc get pods -l deployment=catalog-service -w
```

Expected: within one or two minutes `REPLICAS` goes to 2 and then 3, never beyond; after the load ends,
two minutes later it is back to 1. If CPU does not exceed 70%, raise the parallel loops from 8 to 16. If
a new Pod stays `Pending`, the quota is full: lower the limits or `maxReplicas`.

The `deployment=catalog-service` label is the one `oc new-app` puts on the Pods; if the Deployment was
created another way, use its selector label
(`oc get deploy catalog-service -o jsonpath='{.spec.selector}'`).

## Step 6 — Verify the concepts

Each test ties a command to a concept. Write in `docs/verifications-lab3.md` the command, what you
observed and what it proves. Shortcuts used below:

```bash
ORDERS=https://$(oc get route order-service -o jsonpath='{.spec.host}')    # the edge Route from Lab 1
NEW='{"customer_id":"C-001","lines":[{"product_id":1,"quantity":1}]}'
W='%{http_code} %{time_total}s\n'                                          # curl output format
```

Use `https://`: the Route created in Lab 1 redirects plain HTTP, and `curl` without `-L` would only see
the redirect.

### 6.1 A slow catalog opens the circuit

```bash
oc patch configmap catalog-config -p '{"data":{"CATALOG_DELAY_MS":"5000"}}'
oc rollout restart deploy/catalog-service && oc rollout status deploy/catalog-service

# 20 orders in parallel
seq 20 | xargs -P 20 -I{} curl -s -o /dev/null -w "$W" -X POST -H 'Content-Type: application/json' -d "$NEW" $ORDERS/orders

# the twenty-first, from inside the Pod: timing without the external network, and response headers
oc rsh deploy/order-service curl -s -o /dev/null -w "$W" -X POST -H 'Content-Type: application/json' -d "$NEW" localhost:8080/orders
oc rsh deploy/order-service curl -s -i -X POST -H 'Content-Type: application/json' -d "$NEW" localhost:8080/orders | grep -i -E '^HTTP|retry-after'

oc logs deploy/order-service --since=2m | grep -E 'retrying the call|circuit open' | tail -3
```

Expected: the first 20 requests answer `503` after about 2.5 seconds, with two `retrying the call` lines
each; then one `circuit open` line; the twenty-first answers `503` within milliseconds, with the header
`Retry-After: 15`. The 20 requests run in parallel because the circuit counts errors in a 10-second
window: one at a time, at 2.5 seconds each, it would never reach the minimum volume of 10.

Local run of the reference code: twenty `503` at 2.50 s, 40 retries, then the 21st `503` in 1.7 ms with
`Retry-After: 15`.

### 6.2 Remove the delay, the circuit closes by itself

```bash
oc patch configmap catalog-config -p '{"data":{"CATALOG_DELAY_MS":"0"}}'
oc rollout restart deploy/catalog-service && oc rollout status deploy/catalog-service
sleep 15
oc rsh deploy/order-service curl -s -o /dev/null -w "$W" -X POST -H 'Content-Type: application/json' -d "$NEW" localhost:8080/orders
oc logs deploy/order-service --since=1m | grep -E 'half-open|circuit closed'
oc rsh deploy/order-service curl -s localhost:8080/metrics | grep '^circuit_breaker_state'
```

Expected: `201`, then the lines `circuit half-open: trial call` and `circuit closed`, and the metric at
`0`. No restart of order-service.

### 6.3 A non-existent product is not retried

```bash
BAD='{"customer_id":"C-001","lines":[{"product_id":999999,"quantity":1}]}'
seq 20 | xargs -P 20 -I{} curl -s -o /dev/null -w '%{http_code}\n' -X POST -H 'Content-Type: application/json' -d "$BAD" $ORDERS/orders | sort | uniq -c
oc logs deploy/order-service --since=1m | grep -c 'retrying'
oc rsh deploy/order-service curl -s localhost:8080/metrics | grep '^circuit_breaker_state'
```

Expected: twenty `422`, zero `retrying` lines, circuit at `0`. Removing `errorFilter` from the code, the
same test would open the circuit: twenty bad orders would switch the catalog off for everyone.

### 6.4 The same trace id in three services

```bash
curl -s -X POST -H Content-Type:application/json -d "$NEW" $ORDERS/orders     # note the id
oc logs deploy/order-service --since=1m | grep 'order created'                 # note trace_id
oc logs deploy/catalog-service --since=1m | grep <trace_id>

curl -s -X POST $ORDERS/orders/<id>/confirm
oc logs deploy/order-service --since=1m | grep 'order confirmed'               # another trace_id
oc logs deploy/notification-service --since=1m | grep <trace_id of the confirmation>
```

Expected: the creation shows the same trace id in order-service and catalog-service; the confirmation in
order-service and notification-service, even though the email left later, from the relay, through Kafka.
The two operations are two requests, hence two traces.

### 6.5 RED metrics and cardinality

```bash
oc rsh deploy/order-service curl -s localhost:8080/metrics | grep -E '^http_server_duration_seconds_count|^dependency_calls_total'
```

Expected: rows with `route="/orders"` and `route="/orders/:id/confirm"`, never with an order id; counts for
`status` 201, 422 and 503 that match the previous tests.

### 6.6 Database down: out of the load balancer, no restart

```bash
oc scale deploy/order-db --replicas=0
oc get pods -l deployment=order-service -w        # after about 30 s READY 0/1, RESTARTS unchanged
oc scale deploy/order-db --replicas=1
```

Expected: the order-service Pod becomes not ready but is not restarted; when order-db is back, it becomes
ready again by itself. With the database in the liveness probe, the `RESTARTS` counter would grow.

### 6.7 Scaling under load

Already done in Step 5. Note when replicas rose to 2 and 3, the highest CPU seen in `TARGETS` and how long
they took to go back to 1.

### 6.8 Change a password without a new build

The Red Hat PostgreSQL image resets the user's password at every start, reading it from the environment:
changing the Secret and restarting database and service is enough.

```bash
oc get builds | wc -l                                         # note the number
oc patch secret catalog-db -p "{\"stringData\":{\"database-password\":\"$(openssl rand -hex 12)\"}}"
oc rollout restart deploy/catalog-db && oc rollout status deploy/catalog-db
oc rollout restart deploy/catalog-service && oc rollout status deploy/catalog-service
oc rsh deploy/catalog-service curl -s localhost:8080/health/ready
oc get builds | wc -l                                         # same number as before
```

Expected: everything ready again, no new build. Between the two restarts catalog-service stays not ready
for a few seconds, because it still uses the old password: in production you rotate with two alternating
users to avoid that.

### 6.9 A changed ConfigMap is not enough

```bash
oc patch configmap order-config -p '{"data":{"CB_RESET_MS":"30000"}}'
oc rsh deploy/order-service printenv CB_RESET_MS             # still 15000
oc rollout restart deploy/order-service && oc rollout status deploy/order-service
oc rsh deploy/order-service printenv CB_RESET_MS             # 30000
```

Expected: the value changes only after the rollout, and from then on `Retry-After` is 30. Then set it back
to 15000.

### 6.10 Check questions

Answer in two or three lines each in `docs/verifications-lab3.md`:

1. With two replicas of order-service there are two independent circuits towards the catalog. What changes
   in test 6.1, and why is that acceptable?
2. When the 2.5-second budget expires, what happens to the attempts towards the catalog still in flight?
   Who pays for them?
3. If catalog-service could scale to 10 replicas, which component would become the bottleneck?
4. Which SLI would you propose for the e-shop, and with which of the exposed metrics would you compute it?

## Step 7 — Optional: network policies

This step is required only as a **design**: the map of who may talk to whom goes in
`docs/resilience-lab3.md`. Applying it to the cluster is optional, and possible only if in Step 0
`oc auth can-i create networkpolicies` answered `yes`.

### First, look at what is already there

```bash
oc get networkpolicy
oc get networkpolicy -o yaml | grep -A6 'ingress:'
oc get pods --show-labels          # the labels the rules will rely on
```

Network policies add up: a Pod receives everything at least one policy allows. If the namespace already has a
policy allowing all traffic from the same namespace, your restrictive rules will have no effect. If you cannot
remove it, the test on the cluster cannot be demonstrated: stop at the design, or use OpenShift Local.

### The map

| Who receives | From whom | Port |
| --- | --- | --- |
| order-service | the OpenShift router (the Route) | 8080 |
| catalog-service | order-service | 8080 |
| kafka | order-service (relay), notification-service | 9092 |
| order-db, catalog-db, notification-db | only the owning service | 5432 |
| all the services | platform monitoring, for `/metrics` | 8080 |
| everything else | nobody | — |

### The rules

All of them are in [`k8s/networkpolicy.yaml`](k8s/networkpolicy.yaml). The first three:

```yaml
# 1. no ingress, for every Pod in the namespace
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: { name: default-deny }
spec:
  podSelector: {}
  policyTypes: [ Ingress ]
---
# 2. the router reaches order-service
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: { name: order-from-router }
spec:
  podSelector: { matchLabels: { deployment: order-service } }
  ingress:
  - from:
    - namespaceSelector: { matchLabels: { policy-group.network.openshift.io/ingress: "" } }
    ports: [ { port: 8080 } ]
---
# 3. catalog-service only from order-service
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: { name: catalog-from-order }
spec:
  podSelector: { matchLabels: { deployment: catalog-service } }
  ingress:
  - from:
    - podSelector: { matchLabels: { deployment: order-service } }
    ports: [ { port: 8080 } ]
```

For monitoring the namespace selector is `network.openshift.io/policy-group: monitoring`. Watch out for the
`load` Pod of Step 5: with these rules it can no longer reach catalog-service, so do the load test before,
or add a temporary rule for the label `run: load`.

### The test

```bash
oc apply -f $LAB/k8s/networkpolicy.yaml
oc rsh deploy/order-service curl -s -o /dev/null -w '%{http_code}\n' http://catalog-service:8080/products/1          # 200
oc rsh deploy/notification-service curl -s -m 3 -o /dev/null -w '%{http_code}\n' http://catalog-service:8080/products/1   # 000 after 3 s
oc rsh deploy/notification-service curl -s -m 3 telnet://order-db:5432 ; echo $?                                    # not 0
```

Expected: order-service reaches the catalog, notification-service does not, and nobody reaches a database that
is not its own. Then test the e-shop again from scratch, as in Step 5 of Lab 2: create, confirm, get the
notification.

## Step 8 — Save to Git and wrap up

ConfigMaps, HPA and network policies are already hand-written in `k8s/`; export the Deployments again because
`envFrom`, probes and resources changed (or compare with the reference service manifests in [`k8s/`](k8s/)).

```bash
for s in order-service catalog-service notification-service; do
  oc get deploy $s -o yaml > k8s/$s-deploy.yaml
done
grep -l 'kind: Secret' k8s/*.yaml k8s/config/*.yaml    # must list only k8s/secrets.example.yaml
git add . && git commit -m "Lab 3: resilience, observability, configuration, HPA" && git tag lab-3 && git push --tags
```

### Deliverables

| Item | Where |
| --- | --- |
| Code of the three services: tracing, logs, metrics, probes; in order-service the catalog client | `order-service/`, `catalog-service/`, `notification-service/` |
| ConfigMaps, HPA, network policies, exported Deployments | `k8s/` |
| Secrets with placeholder values, no real value | `k8s/secrets.example.yaml` |
| The dependency table (timeouts, retries, fallback per dependency), with reasons, and the network policy map | `docs/resilience-lab3.md` |
| Permissions found in Step 0, outcome of verifications 6.1–6.9, answers to 6.10 | `docs/verifications-lab3.md` |

## Completion checklist

- [ ] One ConfigMap per service wired with `envFrom`, no duplicate variable written by hand in the Deployment
- [ ] Database and email key Secrets out of Git, `k8s/secrets.example.yaml` with placeholder values
- [ ] Catalog client with per-attempt timeout, jittered retry only on transient errors, circuit breaker with `errorFilter` and a 503 fallback with `Retry-After`
- [ ] Injectable delay in catalog-service, off by default
- [ ] JSON logs on stdout with `trace_id` in all three services, context propagated through the outbox and Kafka
- [ ] `/metrics` with the RED histogram, normalised `route`, circuit state
- [ ] Liveness on the process only, readiness on the database
- [ ] Requests set and HPA on catalog-service from 1 to 3 replicas
- [ ] Verifications 6.1–6.9 passed and recorded, questions 6.10 answered
- [ ] Dependency table and network policy map in `docs/`
- [ ] Everything in Git, tag `lab-3`, no Secret in any commit

## Common problems

| Symptom | Likely cause | What to do |
| --- | --- | --- |
| A ConfigMap value has no effect | the same variable is still written by hand in the Deployment, and wins | `oc set env deploy/<service> --list` and remove the duplicate with `NAME-` |
| ConfigMap changed, the service uses the old value | variables are read at startup | `oc rollout restart deploy/<service>` |
| `oc apply` rejects the ConfigMap with a type error | a number without quotes in `data` | write `"800"`, not `800` |
| `npm install` fails with `ERESOLVE` on `@opentelemetry/api` | mixing `@opentelemetry/sdk-trace-base` 2.x with SDK 0.30x | use `@opentelemetry/sdk-trace` 3.x, as in this folder |
| The circuit does not open in test 6.1 | requests sent one at a time: the minimum volume is never reached in the 10 s window | send them in parallel with `xargs -P 20` |
| The circuit opens on bad orders | `errorFilter` missing: 404s count as failures | filter out 4xx errors |
| `curl` in 6.1 returns 302 | plain `http://` against the edge Route | use `https://` |
| `trace_id` missing in the logs | `tracing.js` loaded after pino or Express, or logs written outside a request | `node --require ./tracing.js server.js` in the `start` script |
| notification-service's trace id differs from the confirmation's | `traceparent` not saved in the outbox row or not read by the relay | `traceparent` column, relay SELECT, message header |
| `/metrics` has thousands of `http_server_duration_seconds` rows | `route` taken from `req.path`: one label value per id | use `req.baseUrl + req.route.path` |
| HPA `TARGETS` stays `<unknown>` | CPU requests not set, or metrics not collected yet | `oc set resources` and wait a minute |
| A catalog Pod stays `Pending` under load | quota exhausted | `oc describe quota`; lower the limits or `maxReplicas` |
| order-service restarts when order-db slows down | the database is in the liveness probe | move it to readiness (Step 4) |
| Network policies block nothing | an existing policy allows the whole namespace | `oc get networkpolicy -o yaml`; if you cannot remove it, stop at the design |

## References

- [opossum](https://github.com/nodeshift/opossum): options `timeout`, `errorThresholdPercentage`, `volumeThreshold`, `rollingCountTimeout`, `resetTimeout`, `capacity`, `errorFilter`, and events.
- [Exponential Backoff And Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/), AWS Architecture Blog: the full-jitter formula.
- [OpenTelemetry for Node.js](https://opentelemetry.io/docs/languages/js/getting-started/nodejs/) and the [auto-instrumentations package](https://www.npmjs.com/package/@opentelemetry/auto-instrumentations-node).
- [W3C Trace Context](https://www.w3.org/TR/trace-context/): the `traceparent` header format.
- [prom-client](https://github.com/siimon/prom-client) and [pino](https://getpino.io/).
- OpenShift documentation: [Horizontal Pod Autoscaler](https://docs.redhat.com/en/documentation/openshift_container_platform/latest/html/nodes/working-with-pods#nodes-pods-autoscaling), [Network policy](https://docs.redhat.com/en/documentation/openshift_container_platform/latest/html/network_security/network-policy).
- Microsoft Cloud Design Patterns: [Circuit Breaker](https://learn.microsoft.com/azure/architecture/patterns/circuit-breaker), [Retry](https://learn.microsoft.com/azure/architecture/patterns/retry), [Bulkhead](https://learn.microsoft.com/azure/architecture/patterns/bulkhead).

---

Previous: [Lab 2](../lab-2-async-events/) · Next: [Lab 4 — Saga, CI with Tekton, GitOps](../lab-4-saga-gitops/)
