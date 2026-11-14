// catalog-service: owns products and their current prices.
// Lab 3: JSON logs with trace id, RED metrics, separate liveness/readiness, injectable delay.
const express = require('express');
const log = require('./logger');
const { httpMetrics, metricsEndpoint } = require('./metrics');
const { pool, initSchema } = require('./db');

const PORT = Number(process.env.PORT || 8080);
const DELAY_MS = Number(process.env.CATALOG_DELAY_MS || 0);  // for the verifications: 0 = off

const app = express();
app.use(express.json());
app.use(httpMetrics);
app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => log.info({ method: req.method, path: req.originalUrl, status: res.statusCode, duration_ms: Date.now() - start }, 'request'));
  next();
});

const isId = (v) => /^[1-9][0-9]{0,9}$/.test(v);

// Liveness: the process only. Never the database.
app.get('/health/live', (req, res) => res.json({ status: 'ok' }));

// Readiness: the dependencies without which the service cannot work.
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
app.get('/health', ready);            // old name, kept as an alias while scripts still use it
app.get('/metrics', metricsEndpoint);

// Injected slowness for the resilience tests. Probes do not go through /products,
// so a slow catalog stays Ready.
if (DELAY_MS > 0) log.warn({ delay_ms: DELAY_MS }, 'injected delay active');
app.use('/products', (req, res, next) => (DELAY_MS > 0 ? setTimeout(next, DELAY_MS) : next()));

app.get('/products', async (req, res) => {
  const { rows } = await pool.query('SELECT id, name, price_cents, published FROM product ORDER BY id');
  res.json(rows);
});

app.get('/products/:id', async (req, res) => {
  if (!isId(req.params.id)) return res.status(404).json({ error: 'product not found' });
  const { rows } = await pool.query('SELECT id, name, price_cents, published FROM product WHERE id = $1', [req.params.id]);
  if (rows.length === 0) return res.status(404).json({ error: 'product not found' });
  res.json(rows[0]);
});

function validProduct(body) {
  return body && typeof body.name === 'string' && body.name.trim() !== ''
    && Number.isInteger(body.price_cents) && body.price_cents > 0
    && (body.published === undefined || typeof body.published === 'boolean');
}

app.post('/products', async (req, res) => {
  if (!validProduct(req.body)) {
    return res.status(400).json({ error: 'name (text) and price_cents (integer > 0) are required' });
  }
  const { rows } = await pool.query(
    'INSERT INTO product (name, price_cents, published) VALUES ($1, $2, $3) RETURNING id, name, price_cents, published',
    [req.body.name.trim(), req.body.price_cents, req.body.published ?? false]);
  log.info({ product_id: rows[0].id }, 'product created');
  res.status(201).json(rows[0]);
});

app.put('/products/:id', async (req, res) => {
  if (!isId(req.params.id)) return res.status(404).json({ error: 'product not found' });
  if (!validProduct(req.body)) {
    return res.status(400).json({ error: 'name (text) and price_cents (integer > 0) are required' });
  }
  const { rows } = await pool.query(
    'UPDATE product SET name = $2, price_cents = $3, published = $4 WHERE id = $1 RETURNING id, name, price_cents, published',
    [req.params.id, req.body.name.trim(), req.body.price_cents, req.body.published ?? false]);
  if (rows.length === 0) return res.status(404).json({ error: 'product not found' });
  res.json(rows[0]);
});

app.use((err, req, res, next) => {
  log.error({ error: err.message }, 'unhandled error');
  res.status(500).json({ error: 'internal error' });
});

// Configuration is read ONCE at startup and logged, without secrets.
log.info({ config: { db_host: process.env.DB_HOST, delay_ms: DELAY_MS, log_level: process.env.LOG_LEVEL } }, 'configuration');

initSchema()
  .then(() => app.listen(PORT, '0.0.0.0', () => log.info({ port: PORT }, 'catalog-service listening')))
  .catch((err) => {
    log.error({ error: err.message }, 'startup failed, database not reachable');
    process.exit(1);
  });
