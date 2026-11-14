// order-service: owns orders. Reads product name and price from catalog-service through a
// resilient client, confirms orders and publishes OrderConfirmed through a transactional outbox.
// Lab 3: resilience on order -> catalog, JSON logs with trace id, RED metrics, liveness/readiness.
const crypto = require('node:crypto');
const express = require('express');
const { context, propagation } = require('@opentelemetry/api');
const log = require('./logger');
const { httpMetrics, metricsEndpoint } = require('./metrics');
const catalog = require('./catalog-client');
const { pool, initSchema, loadOrder } = require('./db');
const { startRelay } = require('./relay');

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

app.post('/orders/:id/confirm', async (req, res) => {
  if (!isId(req.params.id)) return res.status(404).json({ error: 'order not found' });
  const db = await pool.connect();
  try {
    await db.query('BEGIN');
    const { rows } = await db.query(
      'SELECT id, customer_id, status FROM customer_order WHERE id = $1 FOR UPDATE', [req.params.id]);
    if (rows.length === 0) {
      await db.query('ROLLBACK');
      return res.status(404).json({ error: 'order not found' });
    }
    const order = rows[0];

    if (order.status !== 'CONFIRMED') {
      await db.query("UPDATE customer_order SET status = 'CONFIRMED' WHERE id = $1", [order.id]);
      const lines = (await db.query(
        'SELECT product_name, quantity, unit_price_cents FROM order_line WHERE order_id = $1', [order.id])).rows;
      const event = {
        specversion: '1.0',
        id: crypto.randomUUID(),
        source: '/eshop/order-service',
        type: 'com.example.eshop.order.confirmed.v1',
        time: new Date().toISOString(),
        subject: `orders/${order.id}`,
        datacontenttype: 'application/json',
        data: {
          orderId: order.id,
          customerId: order.customer_id,
          totalCents: lines.reduce((t, l) => t + l.quantity * l.unit_price_cents, 0),
          lines: lines.map((l) => ({ productName: l.product_name, quantity: l.quantity, unitPriceCents: l.unit_price_cents })),
        },
      };
      // save the trace context of this request with the event: the relay will publish it later
      const carrier = {};
      propagation.inject(context.active(), carrier);
      await db.query(
        'INSERT INTO outbox (id, topic, msg_key, payload, traceparent) VALUES ($1, $2, $3, $4, $5)',
        [event.id, 'orders.events', String(order.id), event, carrier.traceparent ?? null]);
      log.info({ order_id: order.id, event_id: event.id }, 'order confirmed');
    }
    await db.query('COMMIT');
    res.json(await loadOrder(order.id, db));
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
  catalog: catalog.config, circuit: { timeout_ms: process.env.CB_TIMEOUT_MS, reset_ms: process.env.CB_RESET_MS } } }, 'configuration');

initSchema()
  .then(() => {
    app.listen(PORT, '0.0.0.0', () => log.info({ port: PORT }, 'order-service listening'));
    startRelay(pool);
  })
  .catch((err) => {
    log.error({ error: err.message }, 'startup failed, database not reachable');
    process.exit(1);
  });
