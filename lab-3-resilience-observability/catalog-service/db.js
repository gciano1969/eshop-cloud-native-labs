// Database access for catalog-service. Every connection setting comes from the environment.
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
CREATE TABLE IF NOT EXISTS product (
  id          SERIAL PRIMARY KEY,
  name        TEXT NOT NULL,
  price_cents INTEGER NOT NULL CHECK (price_cents > 0),
  published   BOOLEAN NOT NULL DEFAULT false
);`;

async function initSchema() {
  await pool.query(SCHEMA);
}

module.exports = { pool, initSchema };
