// Handling of the payment commands. Each command runs in one transaction (opened by kafka-consumer.js):
// the payment row and the reply in the outbox are written together.
const crypto = require('node:crypto');
const { decide } = require('./decide');

const T = {
  AUTHORIZE: 'com.example.eshop.payment.authorize.v1',
  VOID: 'com.example.eshop.payment.void.v1',
  AUTHORIZED: 'com.example.eshop.payment.authorized.v1',
  REJECTED: 'com.example.eshop.payment.rejected.v1',
  VOIDED: 'com.example.eshop.payment.voided.v1',
};

const cloudEvent = (type, orderId, data) => ({
  specversion: '1.0', id: crypto.randomUUID(), source: '/eshop/payment-service', type,
  time: new Date().toISOString(), subject: `orders/${orderId}`, datacontenttype: 'application/json', data,
});
const reply = (db, ev, traceparent) => db.query(
  'INSERT INTO outbox (id, topic, msg_key, payload, traceparent) VALUES ($1, $2, $3, $4, $5)',
  [ev.id, 'payment.events', String(ev.data.orderId), JSON.stringify(ev), traceparent ?? null]);

async function onCommand(db, cmd, traceparent) {
  const orderId = String(cmd.data.orderId);
  switch (cmd.type) {
    case T.AUTHORIZE: {
      const { ok, reason } = decide(cmd.data.amountCents);
      // one payment per order: the order id is the idempotency key
      const r = await db.query(
        `INSERT INTO payment (order_id, amount_cents, status, reason) VALUES ($1, $2, $3, $4)
           ON CONFLICT (order_id) DO NOTHING`,
        [orderId, cmd.data.amountCents, ok ? 'AUTHORIZED' : 'REJECTED', reason]);
      if (r.rowCount === 0) return 'duplicate';            // already decided: the reply is already in the outbox
      await reply(db, cloudEvent(ok ? T.AUTHORIZED : T.REJECTED, orderId,
        { orderId, amountCents: cmd.data.amountCents, reason }), traceparent);
      return ok ? 'AUTHORIZED' : 'REJECTED';
    }
    case T.VOID: {
      const r = await db.query(
        `UPDATE payment SET status = 'VOIDED', reason = $2, updated_at = now()
          WHERE order_id = $1 AND status = 'AUTHORIZED'`,
        [orderId, cmd.data.reason]);
      if (r.rowCount === 0) return 'nothing to void';
      await reply(db, cloudEvent(T.VOIDED, orderId, { orderId }), traceparent);
      return 'VOIDED';
    }
    default:
      throw new SyntaxError(`unrecognised command: ${cmd.type}`);   // -> dead letter topic
  }
}

module.exports = { onCommand, T };
