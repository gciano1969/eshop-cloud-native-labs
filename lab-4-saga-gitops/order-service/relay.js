// Outbox relay: publishes the pending outbox rows to Kafka, in the same process as the service.
// Lab 4: the same file runs in order-service and payment-service; each row says its own topic.
// At-least-once: a row can be published twice, always with the same event id.
// Lab 3: structured logs, the traceparent header on every message, the outbox_pending gauge.
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;
const log = require('./logger');
const { client } = require('./metrics');

const pending = new client.Gauge({ name: 'outbox_pending', help: 'Outbox rows not yet published' });

function startRelay(pool, { intervalMs = 1000, batchSize = 20 } = {}) {
  const kafka = new Kafka({ kafkaJS: { clientId: process.env.OTEL_SERVICE_NAME, brokers: [process.env.KAFKA_BOOTSTRAP] } });
  const producer = kafka.producer({ kafkaJS: { idempotent: true, acks: -1 } });
  let connected = false;
  let running = false;

  async function connectProducer() {
    try {
      await producer.connect();
      connected = true;
      log.info('relay: connected to Kafka');
    } catch (err) {
      log.warn({ error: err.message }, 'relay: Kafka not reachable, retrying in 5 s');
      setTimeout(connectProducer, 5000);
    }
  }

  async function relayOnce() {
    if (running) return;
    running = true;
    let db;
    try {
      db = await pool.connect();
      pending.set(Number((await db.query('SELECT count(*) FROM outbox WHERE published_at IS NULL')).rows[0].count));
      if (!connected || process.env.RELAY_ENABLED === 'false') return;
      await db.query('BEGIN');
      const { rows } = await db.query(
        `SELECT id, topic, msg_key, payload, traceparent FROM outbox
          WHERE published_at IS NULL ORDER BY created_at LIMIT $1
          FOR UPDATE SKIP LOCKED`, [batchSize]);
      if (rows.length > 0) {
        await Promise.all(rows.map((r) => producer.send({
          topic: r.topic,
          messages: [{
            key: r.msg_key,
            value: JSON.stringify(r.payload),
            headers: {
              'content-type': 'application/cloudevents+json',
              ...(r.traceparent && { traceparent: r.traceparent }),   // W3C trace context across Kafka
            },
          }],
        })));
        await db.query('UPDATE outbox SET published_at = now() WHERE id = ANY($1)', [rows.map((r) => r.id)]);
        log.info({ count: rows.length }, 'relay: events published');
      }
      await db.query('COMMIT');
    } catch (err) {
      if (db) await db.query('ROLLBACK').catch(() => {});
      log.warn({ error: err.message }, 'relay: publish failed, retrying on the next round');
    } finally {
      if (db) db.release();
      running = false;
    }
  }

  if (process.env.RELAY_ENABLED === 'false') log.warn('relay: disabled (RELAY_ENABLED=false)');
  connectProducer();
  setInterval(relayOnce, intervalMs).unref();
}

module.exports = { startRelay };
