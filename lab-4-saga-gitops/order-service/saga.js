// The orchestrator of the order saga, inside order-service.
// PLACED -> PAYMENT_PENDING -> CONFIRMED (pivot) | CANCELLED (compensation: rejection or timeout)
// Every transition is conditional on the current status, so duplicates and late messages change nothing.
const crypto = require('node:crypto');

const SAGA_TIMEOUT_S = Number(process.env.SAGA_TIMEOUT_S || 30);
const T = {
  AUTHORIZE: 'com.example.eshop.payment.authorize.v1',
  VOID: 'com.example.eshop.payment.void.v1',
  AUTHORIZED: 'com.example.eshop.payment.authorized.v1',
  REJECTED: 'com.example.eshop.payment.rejected.v1',
  VOIDED: 'com.example.eshop.payment.voided.v1',
  ORDER_CONFIRMED: 'com.example.eshop.order.confirmed.v1',
  ORDER_CANCELLED: 'com.example.eshop.order.cancelled.v1',
};

const cloudEvent = (type, orderId, data) => ({
  specversion: '1.0', id: crypto.randomUUID(), source: '/eshop/order-service', type,
  time: new Date().toISOString(), subject: `orders/${orderId}`, datacontenttype: 'application/json', data,
});

async function toOutbox(db, topic, event, traceparent) {
  await db.query(
    'INSERT INTO outbox (id, topic, msg_key, payload, traceparent) VALUES ($1, $2, $3, $4, $5)',
    [event.id, topic, String(event.data.orderId), JSON.stringify(event), traceparent ?? null]);
}

// 1 · start, inside POST /orders/:id/confirm (one transaction)
async function startSaga(db, orderId, traceparent) {
  const { rows } = await db.query('SELECT id, customer_id, status FROM customer_order WHERE id = $1 FOR UPDATE', [orderId]);
  if (rows.length === 0) return { http: 404 };
  if (rows[0].status !== 'PLACED') return { http: 200, status: rows[0].status };      // already started: idempotent
  const { rows: [t] } = await db.query(
    'SELECT COALESCE(SUM(quantity * unit_price_cents), 0)::int AS total FROM order_line WHERE order_id = $1', [orderId]);
  await db.query(
    `UPDATE customer_order SET status = 'PAYMENT_PENDING', saga_deadline = now() + make_interval(secs => $2)
      WHERE id = $1`, [orderId, SAGA_TIMEOUT_S]);
  await toOutbox(db, 'payment.commands', cloudEvent(T.AUTHORIZE, orderId,
    { orderId: String(orderId), customerId: rows[0].customer_id, amountCents: t.total }), traceparent);
  return { http: 202, status: 'PAYMENT_PENDING' };
}

async function cancel(db, orderId, reason, traceparent) {
  const r = await db.query(
    `UPDATE customer_order SET status = 'CANCELLED', cancel_reason = $2
      WHERE id = $1 AND status = 'PAYMENT_PENDING' RETURNING customer_id`, [orderId, reason]);
  if (r.rowCount === 0) return false;
  await toOutbox(db, 'orders.events', cloudEvent(T.ORDER_CANCELLED, orderId,
    { orderId: String(orderId), customerId: r.rows[0].customer_id, reason }), traceparent);
  return true;
}

// 2 · replies from payment-service (one transaction per message, opened by kafka-consumer.js)
async function onPaymentEvent(db, ev, traceparent) {
  const orderId = String(ev.data.orderId);
  switch (ev.type) {
    case T.AUTHORIZED: {
      const r = await db.query(
        `UPDATE customer_order SET status = 'CONFIRMED'
          WHERE id = $1 AND status = 'PAYMENT_PENDING' RETURNING customer_id`, [orderId]);
      if (r.rowCount === 1) {
        const { rows: lines } = await db.query(
          'SELECT product_name, quantity, unit_price_cents FROM order_line WHERE order_id = $1', [orderId]);
        await toOutbox(db, 'orders.events', cloudEvent(T.ORDER_CONFIRMED, orderId, {
          orderId, customerId: r.rows[0].customer_id,
          totalCents: lines.reduce((t, l) => t + l.quantity * l.unit_price_cents, 0),
          lines: lines.map((l) => ({ productName: l.product_name, quantity: l.quantity, unitPriceCents: l.unit_price_cents })),
        }), traceparent);
        return 'confirmed';
      }
      const { rows } = await db.query('SELECT status FROM customer_order WHERE id = $1', [orderId]);
      if (rows[0]?.status === 'CANCELLED') {           // late reply: the order is already cancelled
        await toOutbox(db, 'payment.commands', cloudEvent(T.VOID, orderId,
          { orderId, reason: 'authorization arrived after the cancellation' }), traceparent);
        return 'void requested';
      }
      return 'duplicate';
    }
    case T.REJECTED:
      return (await cancel(db, orderId, `payment rejected: ${ev.data.reason}`, traceparent)) ? 'cancelled' : 'duplicate';
    case T.VOIDED:
      return 'void confirmed';
    default:
      throw new SyntaxError(`unrecognised event: ${ev.type}`);   // -> dead letter topic
  }
}

// 3 · timeout: sagas waiting beyond the deadline are cancelled (one transaction per round).
// FOR UPDATE SKIP LOCKED: with two replicas each takes different orders, none is cancelled twice.
async function expireSagas(db) {
  const { rows } = await db.query(
    `SELECT id FROM customer_order WHERE status = 'PAYMENT_PENDING' AND saga_deadline < now()
      ORDER BY saga_deadline LIMIT 20 FOR UPDATE SKIP LOCKED`);
  for (const r of rows) await cancel(db, r.id, 'no reply from the payment within the time limit');
  return rows.length;
}

module.exports = { startSaga, onPaymentEvent, expireSagas, T };
