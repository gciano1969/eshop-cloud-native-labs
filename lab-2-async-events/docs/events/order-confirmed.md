# Event contract — OrderConfirmed

| Field | Value |
| --- | --- |
| CloudEvents `type` | `com.example.eshop.order.confirmed.v1` |
| Producer | order-service, through its transactional outbox |
| Consumers today | notification-service (consumer group `notification-service`) |
| Topic | `orders.events` (3 partitions, 7-day retention) |
| Message key | the order id, as a string — all events of one order land in one partition, in order |
| Header | `content-type: application/cloudevents+json` |
| Delivery | at least once: consumers must be idempotent on the event `id` |

## When it is emitted

Once per order, in the same database transaction that moves the order from `PLACED` to
`CONFIRMED` (`POST /orders/{id}/confirm`). Confirming an already confirmed order emits nothing.

## Envelope and payload

```json
{
  "specversion": "1.0",
  "id": "3f9c2b1e-6d0a-4c7e-9a51-2b8f7c1d4e60",
  "source": "/eshop/order-service",
  "type": "com.example.eshop.order.confirmed.v1",
  "time": "2026-10-01T09:14:03Z",
  "subject": "orders/1042",
  "datacontenttype": "application/json",
  "data": {
    "orderId": 1042,
    "customerId": "C-001",
    "totalCents": 4900,
    "lines": [ { "productName": "Mug", "quantity": 2, "unitPriceCents": 1200 } ]
  }
}
```

| `data` field | Type | Meaning |
| --- | --- | --- |
| `orderId` | integer | Order identifier in order-service |
| `customerId` | string | Customer identifier only — no personal data in the event |
| `totalCents` | integer | Order total in euro cents, computed from the copied prices |
| `lines[].productName` | string | Product name as copied at purchase time |
| `lines[].quantity` | integer | Quantity (> 0) |
| `lines[].unitPriceCents` | integer | Unit price as copied at purchase time |

## Rules

- `id` is generated **once**, when the event enters the outbox, and never changes: a republished
  event carries the same `id`. It is the key consumers use to discard duplicates.
- The payload carries what the known consumer needs (Event-Carried State Transfer), but no
  personal data: no names, no email addresses.
- A breaking change gets a new `type` (`…confirmed.v2`), published alongside `v1` until every
  consumer has moved.
