// notification-service: turns OrderConfirmed events into (simulated) emails. No Route: internal only.
const express = require('express');
const { pool, initSchema } = require('./db');
const { createConsumer } = require('./consumer');

const PORT = Number(process.env.PORT || 8080);
const app = express();
const consumer = createConsumer(pool);

app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => console.log(`${req.method} ${req.originalUrl} ${res.statusCode} ${Date.now() - start}ms`));
  next();
});

// Readiness: the database answers AND the consumer has started.
app.get('/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    if (!consumer.state.ready) return res.status(503).json({ status: 'consumer not started' });
    res.json({ status: 'ok' });
  } catch (err) {
    res.status(503).json({ status: 'database unavailable' });
  }
});

// For the verifications: the notifications recorded for one order.
app.get('/notifications', async (req, res) => {
  if (!/^[0-9]+$/.test(String(req.query.orderId))) return res.status(400).json({ error: 'orderId is required' });
  const { rows } = await pool.query(
    'SELECT id, event_id, order_id, customer_id, body, created_at FROM notification WHERE order_id = $1 ORDER BY id',
    [req.query.orderId]);
  res.json(rows);
});

app.use((err, req, res, next) => {
  console.error(err);
  res.status(500).json({ error: 'internal error' });
});

initSchema()
  .then(() => {
    app.listen(PORT, '0.0.0.0', () => console.log(`notification-service listening on ${PORT}`));
    // If Kafka is down the process exits and OpenShift restarts it with increasing back-off:
    // the simplest way to retry. Once connected, the client handles reconnections by itself.
    return consumer.start();
  })
  .catch((err) => {
    console.error('startup failed:', err.message);
    process.exit(1);
  });
