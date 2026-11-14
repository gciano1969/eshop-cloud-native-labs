// Outbox relay: publishes the pending outbox rows to Kafka, in the same process as order-service.
// Delivery is at-least-once: a row can be published twice (crash between Kafka ack and COMMIT),
// always with the same event id. Consumers must be idempotent.
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

function startRelay(pool, { intervalMs = 1000, batchSize = 20 } = {}) {
  const kafka = new Kafka({ kafkaJS: { clientId: 'order-service', brokers: [process.env.KAFKA_BOOTSTRAP] } });
  // idempotent: an internal producer retry does not duplicate the message in the partition
  // acks -1: Kafka confirms only after writing the message
  const producer = kafka.producer({ kafkaJS: { idempotent: true, acks: -1 } });
  let connected = false;
  let running = false;

  async function connectProducer() {
    try {
      await producer.connect();
      connected = true;
      console.log('relay: connected to Kafka');
    } catch (err) {
      console.log('relay: Kafka not reachable, retrying in 5 s');
      setTimeout(connectProducer, 5000);
    }
  }

  async function relayOnce() {
    if (running || !connected || process.env.RELAY_ENABLED === 'false') return;
    running = true;
    let db;
    try {
      db = await pool.connect();
      await db.query('BEGIN');
      const { rows } = await db.query(
        `SELECT id, topic, msg_key, payload FROM outbox
          WHERE published_at IS NULL ORDER BY created_at LIMIT $1
          FOR UPDATE SKIP LOCKED`, [batchSize]);            // two replicas never take the same rows
      if (rows.length > 0) {
        // sends start in outbox order; wait for Kafka to acknowledge all of them
        await Promise.all(rows.map((r) => producer.send({
          topic: r.topic,
          messages: [{
            key: r.msg_key,
            value: JSON.stringify(r.payload),
            headers: { 'content-type': 'application/cloudevents+json' },
          }],
        })));
        await db.query('UPDATE outbox SET published_at = now() WHERE id = ANY($1)', [rows.map((r) => r.id)]);
        console.log(`relay: published ${rows.length} events`);
      }
      await db.query('COMMIT');
    } catch (err) {
      if (db) await db.query('ROLLBACK').catch(() => {});
      console.log('relay: publish failed, retrying on the next round:', err.message);
    } finally {
      if (db) db.release();
      running = false;
    }
  }

  if (process.env.RELAY_ENABLED === 'false') console.log('relay: disabled (RELAY_ENABLED=false)');
  connectProducer();
  setInterval(relayOnce, intervalMs).unref();
}

module.exports = { startRelay };
