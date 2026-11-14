// Database access for order-service. It only knows its own database: order-db.
const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.DB_HOST || 'localhost',
  port: Number(process.env.DB_PORT || 5432),
  database: process.env.DB_DATABASE_NAME,
  user: process.env.DB_DATABASE_USER,
  password: process.env.DB_DATABASE_PASSWORD,
  max: Number(process.env.DB_POOL_MAX || 5),
  connectionTimeoutMillis: 2000,
});

const SCHEMA = `
CREATE TABLE IF NOT EXISTS customer_order (
  id          SERIAL PRIMARY KEY,
  customer_id TEXT NOT NULL,                       -- id only: the customer lives in another context
  status      TEXT NOT NULL DEFAULT 'PLACED',
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS order_line (
  order_id         INTEGER NOT NULL REFERENCES customer_order(id),
  product_id       INTEGER NOT NULL,               -- identity reference, no foreign key to the catalog
  product_name     TEXT NOT NULL,                  -- copied at purchase time
  unit_price_cents INTEGER NOT NULL,               -- copied at purchase time
  quantity         INTEGER NOT NULL CHECK (quantity > 0)
);`;

async function initSchema() {
  await pool.query(SCHEMA);
}

// Reads an order with its lines and total, or null.
async function loadOrder(id, db = pool) {
  const { rows } = await db.query(
    'SELECT id, customer_id, status, created_at FROM customer_order WHERE id = $1', [id]);
  if (rows.length === 0) return null;
  const lines = (await db.query(
    `SELECT product_id, product_name, unit_price_cents, quantity
       FROM order_line WHERE order_id = $1 ORDER BY product_id`, [id])).rows;
  return {
    ...rows[0],
    lines,
    total_cents: lines.reduce((t, l) => t + l.unit_price_cents * l.quantity, 0),
  };
}

module.exports = { pool, initSchema, loadOrder };
