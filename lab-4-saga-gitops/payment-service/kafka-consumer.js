// Generic idempotent-consumer loop, used by order-service (saga replies) and payment-service (commands).
// One message = one database transaction; the offset is committed only after the COMMIT.
// The trace context arrives in the headers. Unreadable messages and unknown types go to the DLQ;
// transient errors are retried with exponential backoff and jitter; persistent ones restart the Pod.
// Handlers signal "unknown message type" by throwing a SyntaxError.
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;
const { context, propagation, trace } = require('@opentelemetry/api');
const log = require('./logger');

const num = (name, def) => Number(process.env[name] ?? def);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function consume({ topic, groupId, dlqTopic, pool, handler }) {
  const kafka = new Kafka({ kafkaJS: { clientId: process.env.OTEL_SERVICE_NAME, brokers: [process.env.KAFKA_BOOTSTRAP] } });
  const consumer = kafka.consumer({ kafkaJS: { groupId, fromBeginning: true, autoCommit: false } });
  const dlq = kafka.producer({ kafkaJS: { acks: -1 } });
  await Promise.all([consumer.connect(), dlq.connect()]);
  await consumer.subscribe({ topics: [topic] });
  const tracer = trace.getTracer(groupId);

  await consumer.run({
    eachMessage: async ({ topic: t, partition, message }) => {
      const next = [{ topic: t, partition, offset: (BigInt(message.offset) + 1n).toString() }];
      const headers = Object.fromEntries(Object.entries(message.headers || {}).map(([k, v]) => [k, v?.toString()]));
      const parent = propagation.extract(context.active(), headers);

      await tracer.startActiveSpan(`${t} process`, {}, parent, async (span) => {
        const toDlq = async (err) => {
          log.error({ topic: t, partition, offset: message.offset, error: err.message }, 'invalid message, sending it to the DLQ');
          await dlq.send({ topic: dlqTopic, messages: [{ key: message.key, value: message.value,
            headers: { 'x-error': err.message, 'x-origin': `${t}/${partition}/${message.offset}` } }] });
          await consumer.commitOffsets(next);
        };
        try {
          let msg;
          try {
            msg = JSON.parse(message.value.toString());
            if (!msg.type || !msg.data?.orderId) throw new SyntaxError('incomplete CloudEvents envelope');
          } catch (err) { return await toDlq(err); }

          const carrier = {};
          propagation.inject(context.active(), carrier);       // for the messages the handler puts in the outbox

          for (let attempt = 1; ; attempt++) {
            const db = await pool.connect();
            try {
              await db.query('BEGIN');
              const outcome = await handler(db, msg, carrier.traceparent);
              await db.query('COMMIT');
              log.info({ type: msg.type, order_id: msg.data.orderId, outcome }, 'message processed');
              break;
            } catch (err) {
              await db.query('ROLLBACK').catch(() => {});
              if (err instanceof SyntaxError) return await toDlq(err);          // unknown type: never retried
              if (attempt >= num('MAX_ATTEMPTS', 3)) {
                log.error({ error: err.message }, 'persistent error: exiting so that the Pod restarts');
                process.exit(1);
              }
              const wait = Math.random() * Math.min(num('RETRY_CAP_MS', 5000), num('RETRY_BASE_MS', 200) * 2 ** attempt);
              log.warn({ attempt, wait_ms: Math.round(wait), error: err.message }, 'transient error, retrying');
              await sleep(wait);
            } finally {
              db.release();
            }
          }
          await consumer.commitOffsets(next);
        } finally {
          span.end();
        }
      });
    },
  });
  return consumer;
}

module.exports = { consume };
