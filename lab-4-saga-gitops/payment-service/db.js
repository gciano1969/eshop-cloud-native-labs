// Database access for payment-service: its own database, payment-db.
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
CREATE TABLE IF NOT EXISTS payment (
  order_id     TEXT PRIMARY KEY,                -- one payment per order: the idempotency key
  amount_cents INTEGER NOT NULL,
  status       TEXT NOT NULL CHECK (status IN ('AUTHORIZED', 'REJECTED', 'VOIDED')),
  reason       TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS outbox (
  id           UUID PRIMARY KEY,
  topic        TEXT NOT NULL,
  msg_key      TEXT NOT NULL,
  payload      JSONB NOT NULL,
  traceparent  TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox (created_at) WHERE published_at IS NULL;`;

async function initSchema() {
  await pool.query(SCHEMA);
}

module.exports = { pool, initSchema };
