// Idempotent consumer of orders.events. Offsets are committed by hand, only after the database COMMIT.
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

const ORDER_CONFIRMED = 'com.example.eshop.order.confirmed.v1';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function createConsumer(pool) {
  const kafka = new Kafka({ kafkaJS: { clientId: 'notification-service', brokers: [process.env.KAFKA_BOOTSTRAP] } });
  const consumer = kafka.consumer({ kafkaJS: {
    groupId: process.env.KAFKA_GROUP || 'notification-service',
    fromBeginning: true,      // a new group starts from the first available event
    autoCommit: false,        // the offset is committed only after the database commit
  } });
  const dlq = kafka.producer({ kafkaJS: { acks: -1 } });
  const state = { ready: false };

  async function handle({ topic, partition, message }) {
    const next = [{ topic, partition, offset: (BigInt(message.offset) + 1n).toString() }];

    let event;
    try {
      event = JSON.parse(message.value.toString());
      if (event.type !== ORDER_CONFIRMED || !event.id || !event.data?.orderId) {
        throw new Error('unrecognised event');
      }
    } catch (err) {
      // permanent error: the message goes to the dead letter topic and the partition moves on
      console.log('invalid message, sending it to the DLQ:', err.message);
      await dlq.send({ topic: 'notification.dlq', messages: [{
        key: message.key, value: message.value,
        headers: { 'x-error': err.message, 'x-origin': `${topic}/${partition}/${message.offset}` },
      }] });
      return consumer.commitOffsets(next);
    }

    for (let attempt = 1; ; attempt++) {
      const db = await pool.connect();
      try {
        await db.query('BEGIN');
        const first = await db.query(
          'INSERT INTO processed_event (event_id) VALUES ($1) ON CONFLICT DO NOTHING', [event.id]);
        if (first.rowCount === 0) {                   // already seen: duplicate
          await db.query('ROLLBACK');
          console.log(`duplicate discarded: ${event.id}`);
          return consumer.commitOffsets(next);
        }
        const d = event.data;
        const body = `Your order ${d.orderId} is confirmed. Total: EUR ${(d.totalCents / 100).toFixed(2)}.`;
        await db.query(
          'INSERT INTO notification (event_id, order_id, customer_id, body) VALUES ($1, $2, $3, $4)',
          [event.id, d.orderId, d.customerId, body]);
        await db.query('COMMIT');                     // registry + notification: together or not at all
        console.log(`EMAIL -> customer ${d.customerId}: ${body}`);
        return consumer.commitOffsets(next);          // offset committed only after the database commit
      } catch (err) {
        await db.query('ROLLBACK').catch(() => {});
        if (attempt === 3) {                          // an error that does not go away: start over
          console.error('persistent error, restarting the service:', err.message);
          process.exit(1);
        }
        console.log(`transient error (attempt ${attempt}), retrying in 2 s:`, err.message);
        await sleep(2000);
      } finally {
        db.release();
      }
    }
  }

  async function start() {
    await dlq.connect();
    await consumer.connect();
    await consumer.subscribe({ topics: ['orders.events'] });
    state.ready = true;
    await consumer.run({ eachMessage: handle });
    console.log('consumer: listening on orders.events');
  }

  return { start, handle, state };
}

module.exports = { createConsumer };
