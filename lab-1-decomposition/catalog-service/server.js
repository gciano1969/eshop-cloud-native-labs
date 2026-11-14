// catalog-service: owns products and their current prices.
const express = require('express');
const { pool, initSchema } = require('./db');

const PORT = Number(process.env.PORT || 8080);
const app = express();
app.use(express.json());

// One log line per request: method, path, status, duration.
app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => console.log(`${req.method} ${req.originalUrl} ${res.statusCode} ${Date.now() - start}ms`));
  next();
});

const isId = (v) => /^[1-9][0-9]{0,9}$/.test(v);

// Readiness: 200 only when the database answers.
app.get('/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ status: 'ok' });
  } catch (err) {
    res.status(503).json({ status: 'database unavailable' });
  }
});

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
  res.status(201).json(rows[0]);
});

// Used in the lab to change a price and show that existing orders keep their copy.
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
  console.error(err);
  res.status(500).json({ error: 'internal error' });
});

initSchema()
  .then(() => app.listen(PORT, '0.0.0.0', () => console.log(`catalog-service listening on ${PORT}`)))
  .catch((err) => {
    console.error('startup failed, database not reachable:', err.message);
    process.exit(1); // OpenShift restarts the Pod with increasing back-off
  });
