// notification-service: turns OrderConfirmed events into (simulated) emails. No Route: internal only.
const express = require('express');
const log = require('./logger');
const { httpMetrics, metricsEndpoint } = require('./metrics');
const { pool, initSchema } = require('./db');
const { createConsumer } = require('./consumer');

const PORT = Number(process.env.PORT || 8080);
const app = express();
const consumer = createConsumer(pool);
app.use(httpMetrics);

app.get('/health/live', (req, res) => res.json({ status: 'ok' }));

// Readiness: the database answers AND the consumer has started.
async function ready(req, res) {
  try {
    await pool.query('SELECT 1');
    if (!consumer.state.ready) return res.status(503).json({ status: 'consumer not started' });
    res.json({ status: 'ready' });
  } catch (err) {
    log.warn({ error: err.message }, 'not ready: database not reachable');
    res.status(503).json({ status: 'not ready' });
  }
}
app.get('/health/ready', ready);
app.get('/health', ready);
app.get('/metrics', metricsEndpoint);

app.get('/notifications', async (req, res) => {
  if (!/^[0-9]+$/.test(String(req.query.orderId))) return res.status(400).json({ error: 'orderId is required' });
  const { rows } = await pool.query(
    'SELECT id, event_id, order_id, customer_id, body, created_at FROM notification WHERE order_id = $1 ORDER BY id',
    [req.query.orderId]);
  res.json(rows);
});

app.use((err, req, res, next) => {
  log.error({ error: err.message }, 'unhandled error');
  res.status(500).json({ error: 'internal error' });
});

log.info({ config: { db_host: process.env.DB_HOST, kafka: process.env.KAFKA_BOOTSTRAP, group: process.env.KAFKA_GROUP,
  max_attempts: process.env.MAX_ATTEMPTS, email_api_key_set: Boolean(process.env.EMAIL_API_KEY) } }, 'configuration');

initSchema()
  .then(() => {
    app.listen(PORT, '0.0.0.0', () => log.info({ port: PORT }, 'notification-service listening'));
    return consumer.start();
  })
  .catch((err) => {
    log.error({ error: err.message }, 'startup failed');
    process.exit(1);
  });
