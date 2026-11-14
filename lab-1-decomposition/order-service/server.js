// order-service: owns orders. Reads product name and price from catalog-service over REST.
const express = require('express');
const { pool, initSchema, loadOrder } = require('./db');

const PORT = Number(process.env.PORT || 8080);
const CATALOG_URL = process.env.CATALOG_URL;                 // e.g. http://catalog-service:8080
const CATALOG_TIMEOUT_MS = Number(process.env.CATALOG_TIMEOUT_MS || 2000);

if (!CATALOG_URL) {
  console.error('CATALOG_URL is not set');
  process.exit(1);
}

const app = express();
app.use(express.json());

app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => console.log(`${req.method} ${req.originalUrl} ${res.statusCode} ${Date.now() - start}ms`));
  next();
});

const isId = (v) => /^[1-9][0-9]{0,9}$/.test(String(v));

app.get('/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ status: 'ok' });
  } catch (err) {
    res.status(503).json({ status: 'database unavailable' });
  }
});

class CatalogError extends Error {
  constructor(status, message) { super(message); this.status = status; }
}

// One call to the catalog, never waiting longer than CATALOG_TIMEOUT_MS.
async function fetchProduct(id) {
  let res;
  try {
    res = await fetch(`${CATALOG_URL}/products/${id}`, { signal: AbortSignal.timeout(CATALOG_TIMEOUT_MS) });
  } catch (err) {
    throw new CatalogError(503, 'catalog-service is not responding');
  }
  if (res.status === 404) throw new CatalogError(422, `product ${id} does not exist`);
  if (!res.ok) throw new CatalogError(503, `catalog-service answered ${res.status}`);
  return res.json();
}

function validOrder(body) {
  return body && typeof body.customer_id === 'string' && body.customer_id.trim() !== ''
    && Array.isArray(body.lines) && body.lines.length > 0
    && body.lines.every((l) => l && isId(l.product_id) && Number.isInteger(l.quantity) && l.quantity > 0);
}

app.post('/orders', async (req, res) => {
  // 1. validate the request
  if (!validOrder(req.body)) {
    return res.status(400).json({ error: 'customer_id and at least one line with product_id and quantity > 0 are required' });
  }

  // 2-3. read every product from the catalog
  const lines = [];
  try {
    for (const line of req.body.lines) {
      const p = await fetchProduct(line.product_id);
      lines.push({ product_id: p.id, product_name: p.name, unit_price_cents: p.price_cents, quantity: line.quantity });
    }
  } catch (err) {
    if (err instanceof CatalogError) return res.status(err.status).json({ error: err.message });
    throw err;
  }

  // 4. one transaction: the order and its lines are one aggregate. Name and price are COPIED.
  const db = await pool.connect();
  let orderId;
  try {
    await db.query('BEGIN');
    const { rows } = await db.query(
      'INSERT INTO customer_order (customer_id) VALUES ($1) RETURNING id', [req.body.customer_id.trim()]);
    orderId = rows[0].id;
    for (const l of lines) {
      await db.query(
        `INSERT INTO order_line (order_id, product_id, product_name, unit_price_cents, quantity)
         VALUES ($1, $2, $3, $4, $5)`,
        [orderId, l.product_id, l.product_name, l.unit_price_cents, l.quantity]);
    }
    await db.query('COMMIT');
  } catch (err) {
    await db.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    db.release();
  }

  // 5. 201 with the order and the computed total
  res.status(201).json(await loadOrder(orderId));
});

app.get('/orders/:id', async (req, res) => {
  if (!isId(req.params.id)) return res.status(404).json({ error: 'order not found' });
  const order = await loadOrder(req.params.id);
  if (!order) return res.status(404).json({ error: 'order not found' });
  res.json(order);
});

app.use((err, req, res, next) => {
  console.error(err);
  res.status(500).json({ error: 'internal error' });
});

initSchema()
  .then(() => app.listen(PORT, '0.0.0.0', () => console.log(`order-service listening on ${PORT}, catalog at ${CATALOG_URL}`)))
  .catch((err) => {
    console.error('startup failed, database not reachable:', err.message);
    process.exit(1);
  });
