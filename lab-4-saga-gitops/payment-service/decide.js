// The payment decision: a pure function, separate from database and Kafka,
// so that the pipeline can test it with nothing else running.
const LIMIT = Number(process.env.PAYMENT_LIMIT_CENTS || 50000);   // above EUR 500: rejected

function decide(amountCents) {
  if (!Number.isInteger(amountCents) || amountCents <= 0) return { ok: false, reason: 'invalid amount' };
  if (amountCents > LIMIT) return { ok: false, reason: `amount above the limit of EUR ${LIMIT / 100}` };
  return { ok: true, reason: null };
}

module.exports = { decide, LIMIT };
