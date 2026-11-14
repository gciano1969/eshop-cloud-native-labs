// npm test = node --test. Runs in the Tekton pipeline before any image is built.
const { test } = require('node:test');
const assert = require('node:assert');

process.env.PAYMENT_LIMIT_CENTS = '50000';
const { decide } = require('../decide');

test('within the limit: authorized', () => assert.deepStrictEqual(decide(4900), { ok: true, reason: null }));
test('exactly at the limit: authorized', () => assert.strictEqual(decide(50000).ok, true));
test('above the limit: rejected with the reason', () => assert.match(decide(60000).reason, /EUR 500/));
test('invalid amount: rejected', () => assert.strictEqual(decide(-1).ok, false));
