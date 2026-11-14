// Idempotent consumer of orders.events. Offsets are committed by hand, only after the database COMMIT.
// Lab 3: the trace context arrives in the message headers; retries use exponential backoff with jitter.
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;
const { context, propagation, trace } = require('@opentelemetry/api');
const log = require('./logger');
const { client } = require('./metrics');

const ORDER_CONFIRMED = 'com.example.eshop.order.confirmed.v1';
const num = (name, def) => Number(process.env[name] ?? def);
const cfg = { maxAttempts: num('MAX_ATTEMPTS', 3), baseMs: num('RETRY_BASE_MS', 200), capMs: num('RETRY_CAP_MS', 5000) };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const notifications = new client.Counter({ name: 'notifications_total', help: 'Messages handled, by outcome', labelNames: ['outcome'] });

function createConsumer(pool) {
  const kafka = new Kafka({ kafkaJS: { clientId: 'notification-service', brokers: [process.env.KAFKA_BOOTSTRAP] } });
  const consumer = kafka.consumer({ kafkaJS: {
    groupId: process.env.KAFKA_GROUP || 'notification-service',
    fromBeginning: true,
    autoCommit: false,
  } });
  const dlq = kafka.producer({ kafkaJS: { acks: -1 } });
  const tracer = trace.getTracer('notification-service');
  const state = { ready: false };

  async function process_({ topic, partition, message }) {
    const next = [{ topic, partition, offset: (BigInt(message.offset) + 1n).toString() }];

    let event;
    try {
      event = JSON.parse(message.value.toString());
      if (event.type !== ORDER_CONFIRMED || !event.id || !event.data?.orderId) {
        throw new Error('unrecognised event');
      }
    } catch (err) {
      log.warn({ topic, partition, offset: message.offset, error: err.message }, 'invalid message, sending it to the DLQ');
      await dlq.send({ topic: 'notification.dlq', messages: [{
        key: message.key, value: message.value,
        headers: { 'x-error': err.message, 'x-origin': `${topic}/${partition}/${message.offset}` },
      }] });
      notifications.inc({ outcome: 'dlq' });
      return consumer.commitOffsets(next);
    }

    for (let attempt = 1; ; attempt++) {
      const db = await pool.connect();
      try {
        await db.query('BEGIN');
        const first = await db.query(
          'INSERT INTO processed_event (event_id) VALUES ($1) ON CONFLICT DO NOTHING', [event.id]);
        if (first.rowCount === 0) {
          await db.query('ROLLBACK');
          log.info({ event_id: event.id, order_id: event.data.orderId }, 'duplicate discarded');
          notifications.inc({ outcome: 'duplicate' });
          return consumer.commitOffsets(next);
        }
        const d = event.data;
        const body = `Your order ${d.orderId} is confirmed. Total: EUR ${(d.totalCents / 100).toFixed(2)}.`;
        await db.query(
          'INSERT INTO notification (event_id, order_id, customer_id, body) VALUES ($1, $2, $3, $4)',
          [event.id, d.orderId, d.customerId, body]);
        await db.query('COMMIT');
        // The (fake) provider would be called here with EMAIL_API_KEY: never log the key itself.
        log.info({ event_id: event.id, order_id: d.orderId, customer_id: d.customerId, body }, 'EMAIL sent');
        notifications.inc({ outcome: 'sent' });
        return consumer.commitOffsets(next);
      } catch (err) {
        await db.query('ROLLBACK').catch(() => {});
        if (attempt >= cfg.maxAttempts) {
          log.error({ error: err.message }, 'persistent error, exiting so that the Pod restarts');
          process.exit(1);
        }
        const wait = Math.random() * Math.min(cfg.capMs, cfg.baseMs * 2 ** attempt);
        log.warn({ attempt, wait_ms: Math.round(wait), error: err.message }, 'transient error, retrying');
        await sleep(wait);
      } finally {
        db.release();
      }
    }
  }

  // Resume the producer's trace from the headers and open a child span around the processing.
  async function handle(payload) {
    const headers = Object.fromEntries(Object.entries(payload.message.headers || {}).map(([k, v]) => [k, v?.toString()]));
    const parent = propagation.extract(context.active(), headers);
    return tracer.startActiveSpan('orders.events process', {}, parent, async (span) => {
      try { return await process_(payload); } finally { span.end(); }
    });
  }

  async function start() {
    await dlq.connect();
    await consumer.connect();
    await consumer.subscribe({ topics: ['orders.events'] });
    state.ready = true;
    await consumer.run({ eachMessage: handle });
    log.info('consumer: listening on orders.events');
  }

  return { start, handle, state };
}

module.exports = { createConsumer };
