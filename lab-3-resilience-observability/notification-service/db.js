// Database access for notification-service: its own database, notification-db.
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
-- events already processed: the idempotency key
CREATE TABLE IF NOT EXISTS processed_event (
  event_id     UUID PRIMARY KEY,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS notification (
  id          SERIAL PRIMARY KEY,
  event_id    UUID NOT NULL,
  order_id    INTEGER NOT NULL,          -- id only: the order lives in order-service
  customer_id TEXT NOT NULL,
  body        TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);`;

async function initSchema() {
  await pool.query(SCHEMA);
}

module.exports = { pool, initSchema };
