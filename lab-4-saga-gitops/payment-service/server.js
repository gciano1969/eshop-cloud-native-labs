// payment-service: decides on payment authorizations (simulated: no real money, no card data).
// Reads commands from payment.commands, replies on payment.events through its own outbox. No Route.
const express = require('express');
const log = require('./logger');
const { httpMetrics, metricsEndpoint } = require('./metrics');
const { pool, initSchema } = require('./db');
const { consume } = require('./kafka-consumer');
const { onCommand } = require('./payments');
const { startRelay } = require('./relay');
const { LIMIT } = require('./decide');

const PORT = Number(process.env.PORT || 8080);
const app = express();
let consumerReady = false;
app.use(httpMetrics);

app.get('/health/live', (req, res) => res.json({ status: 'ok' }));
async function ready(req, res) {
  try {
    await pool.query('SELECT 1');
    if (!consumerReady) return res.status(503).json({ status: 'consumer not started' });
    res.json({ status: 'ready' });
  } catch (err) {
    log.warn({ error: err.message }, 'not ready: database not reachable');
    res.status(503).json({ status: 'not ready' });
  }
}
app.get('/health/ready', ready);
app.get('/health', ready);
app.get('/metrics', metricsEndpoint);

// For the verifications: the payment of one order.
app.get('/payments/:orderId', async (req, res) => {
  const { rows } = await pool.query(
    'SELECT order_id, amount_cents, status, reason, created_at, updated_at FROM payment WHERE order_id = $1',
    [req.params.orderId]);
  if (rows.length === 0) return res.status(404).json({ error: 'no payment for this order' });
  res.json(rows[0]);
});

app.use((err, req, res, next) => {
  log.error({ error: err.message }, 'unhandled error');
  res.status(500).json({ error: 'internal error' });
});

log.info({ config: { db_host: process.env.DB_HOST, kafka: process.env.KAFKA_BOOTSTRAP, group: process.env.KAFKA_GROUP,
  limit_cents: LIMIT, relay: process.env.RELAY_ENABLED } }, 'payment-service starting');

initSchema()
  .then(() => {
    app.listen(PORT, '0.0.0.0', () => log.info({ port: PORT }, 'payment-service listening'));
    startRelay(pool);
    return consume({ topic: 'payment.commands', groupId: process.env.KAFKA_GROUP || 'payment-service',
      dlqTopic: 'payment.dlq', pool, handler: onCommand });
  })
  .then(() => { consumerReady = true; log.info('consumer: listening on payment.commands'); })
  .catch((err) => {
    log.error({ error: err.message }, 'startup failed');
    process.exit(1);
  });
