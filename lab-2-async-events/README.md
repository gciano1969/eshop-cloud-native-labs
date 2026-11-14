# Lab 2 — Asynchronous events with Kafka

**Concepts:** events vs commands, CloudEvents, partitions and keys, transactional outbox,
at-least-once delivery, idempotent consumer, manual offset commit, dead letter topic.

## Goal

Lab 2 adds the e-shop's first asynchronous message: when an order is confirmed, order-service
publishes the **OrderConfirmed** event and a new service, **notification-service**, receives it and
sends the confirmation email (simulated).

Four new pieces:

- a single-node **Apache Kafka broker** (KRaft mode) in your namespace, with a persistent volume;
- in order-service, an idempotent **order confirmation** and an **outbox table** written in the same
  transaction, plus a relay that publishes it on the `orders.events` topic with the order id as key;
- **notification-service**, with its own database, reading the topic as an **idempotent consumer**;
- a **dead letter topic** for messages that cannot be processed.

The lab is complete when:

- confirming an order produces one and only one notification, even if the event arrives twice;
- with notification-service down, orders still get confirmed, and the notification arrives when it comes back;
- with Kafka down, orders still get confirmed, and the event leaves when Kafka comes back;
- re-reading the topic from the beginning, notification-service sends no extra notification;
- a malformed message ends up in the dead letter topic without blocking the others;
- everything needed to recreate the environment (code, YAML, C4 diagram, decisions) is in Git.

Estimated time: 5–7 hours. The examples use Node.js with Express, as in Lab 1, and Confluent's
official Kafka client, [`@confluentinc/kafka-javascript`](https://github.com/confluentinc/confluent-kafka-javascript),
through its KafkaJS-compatible API.

```text
lab-2-async-events/
├── catalog-service/        # unchanged from Lab 1
├── order-service/          # + confirm route, outbox table, relay.js
├── notification-service/   # new: idempotent consumer, its own database
├── k8s/
│   ├── kafka.yaml          # new: ConfigMap, PVC, Deployment, Service for Kafka
│   ├── kafka-topics.sh     # new: creates the topics
│   └── ...                 # services, Secrets example
└── docs/events/order-confirmed.md   # the event contract
```

## Prerequisites, Sandbox constraints and ground rules

Start from a working Lab 1: catalog-service and order-service running, each with its own
PostgreSQL, code and manifests in Git. If the Sandbox has expired, reactivate it and recreate
everything from the repository first.

| Constraint | Consequence for this lab |
| --- | --- |
| One namespace (`<username>-dev`) | Kafka, three services and three databases share the namespace. In production Kafka would be a dedicated cluster, managed for example with Streams for Apache Kafka. |
| No cluster-admin rights | No operator (neither Strimzi nor Streams for Apache Kafka): Kafka is a plain hand-written Deployment, one node, replication 1, no authentication. |
| Pods run with an arbitrary UID (SCC `restricted-v2`) | The start script of the `apache/kafka` image wants to write to a folder the arbitrary user cannot write. Kafka is started with our own command and the configuration in a ConfigMap (Step 2). |
| Quota of about 3 cores, 14 GB RAM, 40 GB storage | Kafka requests 768 MiB and stays under 1 GiB; the new database 512 MiB and a 1 GiB volume. Plenty of room left. |
| Pods stopped after 12 consecutive hours | Kafka needs a persistent volume: topics and consumer group offsets survive the restart. |
| Expires after 30 days | Everything recreatable from Git, Kafka manifest and topic commands included. |

Ground rules, the same as Lab 1:

- no real data, no real email addresses, no company code or credentials;
- broker and database passwords generated for the exercise and never reused;
- Secrets **excluded** from commits.

## Step 0 — Restart from Lab 1

1. **Log in and check the state.** After `oc login`, scale stopped replicas back to 1 and check that
   everything answers.

```bash
oc project                                           # <username>-dev
oc get deploy -l app.kubernetes.io/part-of=eshop     # 4 Deployments
oc scale deploy -l app.kubernetes.io/part-of=eshop --replicas=1
oc describe quota                                    # note what is left
```

2. **Baseline test.** Create an order as in Step 5 of Lab 1 and check it answers 201. If it does not,
   fix Lab 1 first.
3. **Point the builds at this lab.** The services built from your fork now take their code from the
   `lab-2-async-events` folder:

```bash
REPO=https://github.com/<your-github-user>/eshop-cloud-native-labs
LAB=lab-2-async-events
for s in catalog-service order-service; do
  oc patch bc/$s --type=merge -p "{\"spec\":{\"source\":{\"contextDir\":\"$LAB/$s\"}}}"
done
```

If you write the code yourself instead, work on a `lab-2` branch of your own repository and merge it
when you are done: Lab 1 stays recoverable as it was.

## Step 1 — Design: event, contract, flow

Before any code, fix three things on paper: when the event is born, what it looks like, how it travels.

### When the event is born

In Lab 1 an order is born with status `PLACED`. The "Confirmed" hotspot of the Event Storming told us
that *placed* and *confirmed* are two different facts. Until a payment service exists, an endpoint
simulates the confirmation:

| Method and path | Effect | Response |
| --- | --- | --- |
| `POST /orders/{id}/confirm` | status from `PLACED` to `CONFIRMED`, event in the outbox | 200 with the confirmed order |
| same call on an order already `CONFIRMED` | **no effect**, no new event | 200 with the order as it is |
| non-existent order | none | 404 |

The second row makes confirmation idempotent by construction: it is a state transition, not an
action to repeat.

### The shape of the event

A CloudEvents envelope, with the data the known consumer needs (Event-Carried State Transfer, but
no personal data):

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

The `id` is generated **once**, when the event enters the outbox, and never changes: even if the
event is published twice, the `id` is the same. It is the key the consumer uses to recognise
duplicates.

Write the contract in `docs/events/order-confirmed.md`: fields, types, meaning, who produces it, who
consumes it today. The reference version is [here](docs/events/order-confirmed.md). You can also
write it in AsyncAPI.

### How it travels

| Element | Name | Choice and reason |
| --- | --- | --- |
| Topic | `orders.events` | 3 partitions: order-service's domain events, today only OrderConfirmed |
| Message key | order id, as a string | every event of the same order lands in the same partition and stays in order |
| Consumer group | `notification-service` | the service instances share the partitions; other services will use their own group |
| Dead letter topic | `notification.dlq` | 1 partition, owned by notification-service: the messages it cannot process |
| Retention | 7 days | events stay readable after being consumed: you can replay |

Rule: topics are created **beforehand**, with an explicit command, never on the fly by the clients
(auto-creation is off). The topic exists regardless of who listens: a new consumer can also read
events published before it existed. That is the main difference from a queue.

```text
 POST /orders/{id}/confirm
          │
          ▼
 ┌────────────────────┐   one transaction   ┌──────────────────┐
 │   order-service    │────────────────────►│ order-db         │
 │                    │                     │  customer_order  │
 │  relay (1 s loop) ◄├─────────────────────┤  outbox          │
 └─────────┬──────────┘  pending rows       └──────────────────┘
           │ key = order id
           ▼
 ┌────────────────────┐        ┌─────────────────────────┐      ┌──────────────────┐
 │ Kafka              │───────►│ notification-service    │─────►│ notification-db  │
 │  orders.events (3) │        │  group notification-svc │      │  processed_event │
 │  notification.dlq  │◄───────┤  (invalid messages)     │      │  notification    │
 └────────────────────┘        └─────────────────────────┘      └──────────────────┘
```

The only place where the order and the event touch is order-db; from there on, every hop is a
message that may be repeated, and notification-service tolerates that.

### Updated C4 diagram

Update `docs/c4-container.drawio`: add the broker, notification-service and notification-db. Solid
arrows for synchronous calls, dashed for messages, with the **topic name and the message key** on the
arrow. Export the PNG again.

## Step 2 — Kafka in the namespace

A single Kafka process acts as broker and controller (KRaft mode, no ZooKeeper), with the official
image `apache/kafka:4.3.1`. The image's start script wants to write to `/opt/kafka/config`, which
belongs to a fixed user; OpenShift instead assigns an arbitrary UID, and the script would fail. So:

- the configuration sits in a **ConfigMap**, mounted at `/config`;
- Kafka starts with **its own command**, which formats the storage on first run and then starts the server;
- the application logs go to `/tmp` (`LOG_DIR`), the data to the volume.

No Secret: Kafka listens in clear text and without authentication, reachable only inside the
namespace. Production would need TLS and SASL, or mTLS from a service mesh.

### The manifest: [`k8s/kafka.yaml`](k8s/kafka.yaml)

Four choices to understand, not just copy:

- **`advertised.listeners=PLAINTEXT://kafka:9092`.** It is the address Kafka gives clients after the
  first contact: it must be the Service name. With a wrong address the first connection succeeds and
  all the following ones fail.
- **`log.dirs` in a subfolder of the volume.** The root of a volume may contain `lost+found`, which
  Kafka would take for an invalid partition and refuse to start.
- **Format only the first time, with a fixed `CLUSTER_ID`.** On restart the data on the volume carry
  that id: if it changed, Kafka would not recognise them.
- **`strategy: Recreate`.** During an update the old Pod stops before the new one starts: two
  processes on the same volume would corrupt the data.

### Apply and create the topics

```bash
oc apply -f $LAB/k8s/kafka.yaml        # from your local clone of the repository
oc rollout status deploy/kafka
oc logs deploy/kafka | grep -i 'started'      # "Kafka Server started"
```

Topics are created with the script already in the image. The commands are in
[`k8s/kafka-topics.sh`](k8s/kafka-topics.sh):

```bash
KT='oc rsh deploy/kafka /opt/kafka/bin'
$KT/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic orders.events --partitions 3 --replication-factor 1
$KT/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic notification.dlq --partitions 1 --replication-factor 1
$KT/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders.events
```

Expected: `orders.events` with 3 partitions, all with `Leader: 1`.

## Step 3 — The outbox in order-service

order-service never talks to Kafka during a request. Confirmation writes the order and the event in
the same transaction; a relay, in the same process, publishes afterwards.

### Outbox table

Added to the tables created at startup ([`order-service/db.js`](order-service/db.js)):

```sql
CREATE TABLE IF NOT EXISTS outbox (
  id           UUID PRIMARY KEY,          -- = the CloudEvents id
  topic        TEXT NOT NULL,
  msg_key      TEXT NOT NULL,             -- Kafka key: the order id
  payload      JSONB NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ                -- NULL = not published yet
);
CREATE INDEX IF NOT EXISTS outbox_pending
  ON outbox (created_at) WHERE published_at IS NULL;
```

### Confirmation: one transaction, two writes

The route is in [`order-service/server.js`](order-service/server.js). Its core:

```js
await db.query('BEGIN');
// FOR UPDATE: two concurrent confirmations of the same order queue up
const { rows } = await db.query(
  'SELECT id, customer_id, status FROM customer_order WHERE id = $1 FOR UPDATE', [req.params.id]);
// ... 404 if missing
if (order.status !== 'CONFIRMED') {          // already confirmed: no effect
  await db.query("UPDATE customer_order SET status = 'CONFIRMED' WHERE id = $1", [order.id]);
  const event = { specversion: '1.0', id: crypto.randomUUID(), /* ... */ };
  await db.query('INSERT INTO outbox (id, topic, msg_key, payload) VALUES ($1, $2, $3, $4)',
    [event.id, 'orders.events', String(order.id), event]);
}
await db.query('COMMIT');
res.json(await loadOrder(order.id, db));     // same connection: never hold one and wait for another
```

Note that the request code contains **no Kafka**: if Kafka is down, the confirmation still succeeds.
Note also the last line: the order is read back on the **same** connection. Asking the pool for a
second connection while holding the first one starves the pool under load (we hit exactly this while
testing the reference solution).

### The relay

[`order-service/relay.js`](order-service/relay.js) runs in the same process. Install the client with
`npm install @confluentinc/kafka-javascript`, then:

```js
const producer = kafka.producer({ kafkaJS: { idempotent: true, acks: -1 } });
// every second:
await db.query('BEGIN');
const { rows } = await db.query(
  `SELECT id, topic, msg_key, payload FROM outbox
    WHERE published_at IS NULL ORDER BY created_at LIMIT 20
    FOR UPDATE SKIP LOCKED`);                         // two replicas never take the same rows
await Promise.all(rows.map((r) => producer.send({ topic: r.topic,
  messages: [{ key: r.msg_key, value: JSON.stringify(r.payload),
               headers: { 'content-type': 'application/cloudevents+json' } }] })));
await db.query('UPDATE outbox SET published_at = now() WHERE id = ANY($1)', [rows.map((r) => r.id)]);
await db.query('COMMIT');
```

Four points to be able to explain:

- **Why at least once.** If the Pod dies after Kafka's acknowledgement and before the `COMMIT`, the
  rows stay pending and are sent again on restart, with the same event `id`. That is why Step 4 needs
  an idempotent consumer. The idempotent producer is not enough: it avoids duplicates from its own
  internal retries, not from a process that restarts.
- **Why wait for the acknowledgement.** `send` resolves only once Kafka has written the message
  (`acks: -1`). Marking the row as published before that would risk losing the event.
- **Why the key.** With the key equal to the order id, all the events of that order go to the same
  partition and are read in the order they were written.
- **`RELAY_ENABLED`.** Only for the verifications: with `false` the relay stops, simulating a crash
  between commit and publication.

## Step 4 — notification-service

A new, small service with its own database. It reads `orders.events` in the consumer group
`notification-service`, records the notification and "sends" the email by writing a log line. No
real email provider.

### Its database

```bash
oc new-app postgresql-persistent \
  -p DATABASE_SERVICE_NAME=notification-db \
  -p POSTGRESQL_DATABASE=notifications \
  -p POSTGRESQL_USER=notifications \
  -p POSTGRESQL_PASSWORD="$(openssl rand -hex 12)" \
  -p VOLUME_CAPACITY=1Gi \
  -p MEMORY_LIMIT=512Mi \
  -l app.kubernetes.io/part-of=eshop
```

```sql
-- registry of the events already processed: the idempotency key
CREATE TABLE IF NOT EXISTS processed_event (
  event_id     UUID PRIMARY KEY,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS notification (
  id          SERIAL PRIMARY KEY,
  event_id    UUID NOT NULL,
  order_id    INTEGER NOT NULL,     -- id only: the order lives in order-service
  customer_id TEXT NOT NULL,
  body        TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### HTTP contract

| Method and path | Purpose | Response |
| --- | --- | --- |
| `GET /health` | readiness probe | 200 if the DB answers and the consumer has started, 503 otherwise |
| `GET /notifications?orderId={id}` | verifications | 200, the notifications recorded for that order |

### The consumer

Offsets are committed **by hand**, after processing: with auto-commit Kafka could mark as read a
message not yet processed. See [`notification-service/consumer.js`](notification-service/consumer.js):

```js
const consumer = kafka.consumer({ kafkaJS: {
  groupId: 'notification-service',
  fromBeginning: true,      // a new group starts from the first available event
  autoCommit: false,        // the offset is committed only after the database commit
} });
```

If startup fails (for example Kafka is down) the process exits and OpenShift restarts it with
increasing back-off: the simplest way to retry. Once connected, the client handles reconnections.

### The idempotent consumer

```js
await db.query('BEGIN');
const first = await db.query(
  'INSERT INTO processed_event (event_id) VALUES ($1) ON CONFLICT DO NOTHING', [event.id]);
if (first.rowCount === 0) {                   // already seen: duplicate
  await db.query('ROLLBACK');
  return consumer.commitOffsets(next);
}
await db.query('INSERT INTO notification (...) VALUES (...)', [...]);
await db.query('COMMIT');                     // registry + notification: together or not at all
console.log(`EMAIL -> customer ${d.customerId}: ${body}`);
return consumer.commitOffsets(next);          // offset committed only after the commit
```

The rules that make this code correct:

- **The event id enters the registry in the same commit as the notification.** Recorded in a separate
  transaction, the window where the notification exists and the registry does not would come back.
- **The offset is committed after the commit.** If the Pod dies between commit and offset, on restart
  the message is read again, and the registry recognises it as a duplicate.
- **A poison message must not stop the partition.** In Kafka the offset moves per partition, not per
  message: a message that always fails would block all those after it. So it is moved to the dead
  letter topic and skipped.

The three attempts with a fixed wait and the process exit are a crude retry: in Lab 3 they become
exponential backoff with jitter and a configurable limit.

## Step 5 — Deploy on OpenShift and test end to end

The topics already exist from Step 2, so the order of the deployments does not matter: even if
order-service publishes before notification-service exists, the events stay in the topic and the new
group will read them from the beginning.

### order-service

```bash
oc set env deploy/order-service KAFKA_BOOTSTRAP=kafka:9092 RELAY_ENABLED=true
oc start-build order-service --follow
oc logs deploy/order-service | grep relay          # "relay: connected to Kafka"
```

### notification-service

```bash
oc new-app nodejs:22-ubi9~$REPO \
  --context-dir=$LAB/notification-service \
  --name=notification-service \
  -l app.kubernetes.io/part-of=eshop

# ITS database, and Kafka
oc set env deploy/notification-service --from=secret/notification-db --prefix=DB_
oc set env deploy/notification-service DB_HOST=notification-db DB_PORT=5432 \
  KAFKA_BOOTSTRAP=kafka:9092 KAFKA_GROUP=notification-service

oc set resources deploy/notification-service \
  --requests=cpu=50m,memory=128Mi --limits=cpu=250m,memory=256Mi
oc set probe deploy/notification-service --readiness \
  --get-url=http://:8080/health --initial-delay-seconds=10
oc set probe deploy/notification-service --liveness --open-tcp=8080
```

notification-service has **no** Route: nobody outside the cluster needs it.

Check that the group is registered and has no backlog:

```bash
oc rsh deploy/kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group notification-service
```

Expected: one row for each of the 3 partitions of `orders.events`, with `LAG` 0.

### End-to-end test

```bash
ORDERS=https://$(oc get route order-service -o jsonpath='{.spec.host}')

# 1. a new order (note the id returned)
curl -s -X POST $ORDERS/orders -H 'Content-Type: application/json' \
  -d '{"customer_id":"C-001","lines":[{"product_id":1,"quantity":2},{"product_id":2,"quantity":1}]}'
ID=<returned id>

# 2. the confirmation
curl -s -X POST $ORDERS/orders/$ID/confirm

# 3. the event left the outbox
oc rsh deploy/order-db psql -U orders -d orders \
  -c 'select id, msg_key, published_at from outbox order by created_at desc limit 5'

# 4. the event is in the topic, with its key and its partition
oc rsh deploy/kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders.events --from-beginning \
  --property print.key=true --property print.partition=true --timeout-ms 5000

# 5. the notification arrived
oc logs deploy/notification-service | grep EMAIL
oc rsh deploy/notification-service curl -s "http://localhost:8080/notifications?orderId=$ID"
```

| Test | Expected result |
| --- | --- |
| confirm a `PLACED` order | 200, status `CONFIRMED`, one outbox row with `published_at` set within a few seconds |
| read the topic | the event with key equal to the order id; events with the same key always in the same partition |
| notification-service logs | a line `EMAIL -> customer C-001: Your order … is confirmed. Total: EUR 49.00.` |
| `GET /notifications?orderId=$ID` | exactly one notification |
| confirm a non-existent order | 404, no outbox row |

Reading the topic with the console does not disturb notification-service: the console uses its own
temporary group, with its own offsets. That is independent consumption.

## Step 6 — Verify the concepts

Each test ties a command to a concept. Write in `docs/verifications-lab2.md` the command, what you
observed and what it proves. Shortcuts used below:

```bash
KT='oc rsh deploy/kafka /opt/kafka/bin'
LAG="$KT/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group notification-service"
ODB='oc rsh deploy/order-db psql -U orders -d orders -c'
```

### 6.1 Confirmation is idempotent

```bash
curl -s -X POST $ORDERS/orders/$ID/confirm          # second confirmation of the same order
$ODB "select count(*) from outbox where msg_key = '$ID'"
```

Expected: 200 and a count of **1**. Repeat on a new order with two confirmations in parallel
(`curl … & curl … & wait`): still 1, thanks to `FOR UPDATE`.

### 6.2 A stopped consumer does not stop orders

```bash
oc scale deploy/notification-service --replicas=0
# create and confirm three orders: every confirmation answers 200
$LAG                                                 # the LAG column adds up to 3
oc scale deploy/notification-service --replicas=1
$LAG                                                 # after a few seconds: LAG 0 everywhere
oc logs deploy/notification-service | grep EMAIL    # the three emails
```

Compare with verification 6.5 of Lab 1: there a stopped catalog-service blocked orders. Here a
stopped notification-service blocks nothing. `LAG` is the group's backlog, measured in messages.

### 6.3 A duplicate event does not produce a second email

```bash
# put the already delivered event of order $ID back into publication
$ODB "update outbox set published_at = null where msg_key = '$ID'"
oc logs deploy/notification-service --since=1m | grep -E 'duplicate|EMAIL'
```

Expected: a line `duplicate discarded: <event id>` and **no** new `EMAIL` line. The topic now holds
two copies of the same event, with the same `id`; `GET /notifications?orderId=$ID` still returns one
notification.

### 6.4 Re-reading the whole topic produces no email

The test you cannot do with a queue: move the group back to the start of the topic.

```bash
oc scale deploy/notification-service --replicas=0       # the group must be idle
$KT/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group notification-service \
  --topic orders.events --reset-offsets --to-earliest --execute
oc scale deploy/notification-service --replicas=1
oc logs deploy/notification-service | grep -c 'duplicate discarded'
oc logs deploy/notification-service | grep -c EMAIL
```

Expected: as many `duplicate discarded` as there are events in the topic, **zero** `EMAIL`. It is the
same situation as a new service reading the history, or a recovery after a failure: idempotency makes
it harmless.

### 6.5 Kafka down does not stop orders

```bash
oc scale deploy/kafka --replicas=0
# confirm a new order: it answers 200
$ODB 'select id, published_at from outbox where published_at is null'   # one pending row
oc logs deploy/order-service --since=2m | grep relay                  # publish failed
oc scale deploy/kafka --replicas=1
# within a minute or two: row published, email in the logs
```

While Kafka is down, notification-service may restart a few times: it cannot find Kafka, exits,
OpenShift restarts it. That is the intended behaviour, and it takes nothing away from orders.

### 6.6 Kafka restarts without losing messages

```bash
oc scale deploy/notification-service --replicas=0
# confirm an order: the message stays in the topic, LAG 1
oc rollout restart deploy/kafka && oc rollout status deploy/kafka
$LAG                                                 # LAG still 1: message and group offset are safe
oc scale deploy/notification-service --replicas=1
```

Two ingredients: the persistent volume and the fixed `CLUSTER_ID`. Try removing the volume (using an
`emptyDir`) and repeat: note what happens to the topics.

### 6.7 Crash between commit and publication

```bash
oc set env deploy/order-service RELAY_ENABLED=false    # the relay stops (and the Pod restarts)
# confirm an order: 200, no email
$ODB 'select id, published_at from outbox where published_at is null'
oc set env deploy/order-service RELAY_ENABLED=true
# the event leaves, the email arrives
```

This is the dual-write problem solved: the confirmed order and its event can no longer drift apart.

### 6.8 A malformed message ends up in the dead letter topic

```bash
echo 'this is not JSON' | oc exec -i deploy/kafka -- \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders.events
$KT/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic notification.dlq \
  --from-beginning --property print.headers=true --timeout-ms 5000
```

Expected: in the logs `invalid message, sending it to the DLQ`, and the message in
`notification.dlq` with the headers `x-error` and `x-origin`. Right after, confirm a normal order: it
is processed and `LAG` goes back to 0. The poison message did not block its partition.

### 6.9 Check questions

Answer in two or three lines each in `docs/verifications-lab2.md`:

1. The email is "sent" after the commit. If the Pod dies between the commit and the sending, what
   happens? Is the guarantee for the email at-most-once or at-least-once? How would you make it
   at-least-once without duplicates?
2. In verification 6.4 the malformed message of 6.8, if already present, ends up in the dead letter
   topic again. What does that tell you about the dead letter topic itself?
3. With the key equal to the order id, which events are guaranteed to be in order and which are not?
   Which key would be wrong, and why?
4. If notification-service needed the customer's email address, where would you get it? Weigh at
   least two options and what each one implies.

## Step 7 — Save to Git and wrap up

As in Lab 1, whatever is not in Git is lost with the Sandbox. Export the manifests of the resources
created from the command line, without the Secrets (or compare them with the reference files in
[`k8s/`](k8s/)):

```bash
oc get deploy,svc,bc,is -l app=notification-service -o yaml > k8s/notification-service.yaml
oc get deploy,svc,bc,is -l app=order-service -o yaml > k8s/order-service.yaml
# k8s/kafka.yaml and k8s/kafka-topics.sh are already hand-written
git add . && git commit -m "Lab 2: Kafka, outbox, notification-service" && git tag lab-2 && git push --tags
```

The exported manifests contain status fields and metadata generated by the cluster. That is fine for
the deliverable; clean them up if you like.

### Deliverables

| Item | Where |
| --- | --- |
| order-service code (confirm, outbox, relay) and notification-service | `order-service/`, `notification-service/` |
| Kafka manifest, topic commands, service manifests, no Secrets | `k8s/` |
| OrderConfirmed event contract | `docs/events/order-confirmed.md` |
| Updated C4 diagram (source and PNG) | `docs/c4-container.drawio`, `.png` |
| Outcome of verifications 6.1–6.8 and answers to 6.9 | `docs/verifications-lab2.md` |
| Classification of the e-shop interactions (sync call, command, event), with reasons | `docs/interactions.md` |

## Completion checklist

- [ ] Kafka running with a persistent volume, fixed `CLUSTER_ID` and `log.dirs` in a subfolder
- [ ] Topics `orders.events` (3 partitions) and `notification.dlq` created by script, auto-creation off
- [ ] `POST /orders/{id}/confirm` idempotent, also with two parallel calls
- [ ] Event in the outbox in the same transaction as the confirmation, with a stable `id` and key = order id
- [ ] Relay with idempotent producer, `acks: -1` and `FOR UPDATE SKIP LOCKED`
- [ ] notification-service with its own database, no Route, offsets committed by hand after the commit
- [ ] Invalid messages moved to `notification.dlq` without blocking the partition
- [ ] Verifications 6.1–6.8 passed and recorded, questions 6.9 answered
- [ ] Event contract and updated C4 diagram in `docs/`
- [ ] Everything in Git, tag `lab-2`, no Secret in any commit

## Common problems

| Symptom | Likely cause | What to do |
| --- | --- | --- |
| Kafka Pod in `ImagePullBackOff` with `toomanyrequests` | Docker Hub anonymous pull limit, shared by the whole cluster | retry later, or import the image once into the internal registry: `oc import-image kafka:4.3.1 --from=docker.io/apache/kafka:4.3.1 --confirm` and use `image-registry.openshift-image-registry.svc:5000/<namespace>/kafka:4.3.1` |
| Kafka crashes with `cannot create directory '/opt/kafka/logs'` or permission denied on `/opt/kafka/config` | `LOG_DIR` missing, or the image's own start script is in use | check `command`, `args` and `env` in the manifest |
| Kafka does not start and mentions `lost+found` | `log.dirs` points at the root of the volume | use the subfolder `/var/lib/kafka/data/logs` |
| Kafka does not start after a restart, mentioning the cluster id | `CLUSTER_ID` changed from the one in the data on the volume | put the original value back, or delete and recreate the PVC (topics are lost) |
| Kafka `OOMKilled` | heap or memory limit too low | `-Xmx` to 768m and limit to 1536Mi, staying within the quota |
| Clients connect and then fail on an unknown host | `advertised.listeners` different from the Service name | it must be `PLAINTEXT://kafka:9092` |
| The relay logs `Unknown topic or partition` | topic not created (auto-creation is off) | run `k8s/kafka-topics.sh` |
| A service build fails on `@confluentinc/kafka-javascript` | Node.js version without prebuilt binaries | `oc get is nodejs -n openshift` and pick a 20 or 22 tag, e.g. `nodejs:22-ubi9~…` |
| `LAG` does not go down and the logs repeat the same error | persistent database error: the service exits and restarts | check notification-db; a format error should instead end up in `notification.dlq` |
| Duplicate emails in 6.3 or 6.4 | registry written in a transaction separate from the notification, or event `id` regenerated by the relay | one commit for registry and notification; `id` generated only at confirmation |
| Outbox rows that stay `NULL` | `RELAY_ENABLED=false` left on, or relay not connected | `oc set env deploy/order-service --list`, `relay:` log lines |

## References

- [Apache Kafka — KRaft operations](https://kafka.apache.org/documentation/#kraft): dynamic quorum and formatting with `--standalone`.
- [Dockerfile of the `apache/kafka` image](https://github.com/apache/kafka/blob/trunk/docker/jvm/Dockerfile) and its [`configure` script](https://github.com/apache/kafka/blob/trunk/docker/resources/common-scripts/configure): fixed user and writable `/opt/kafka/config`, the reason for our own start command.
- [`apache/kafka` image tags on Docker Hub](https://hub.docker.com/r/apache/kafka/tags).
- [Confluent JavaScript client](https://github.com/confluentinc/confluent-kafka-javascript) and its [migration guide from KafkaJS](https://github.com/confluentinc/confluent-kafka-javascript/blob/master/MIGRATION.md).
- [CloudEvents specification](https://cloudevents.io/).
- microservices.io: [Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html) and [Idempotent Consumer](https://microservices.io/patterns/communication-style/idempotent-consumer.html).

---

Previous: [Lab 1](../lab-1-decomposition/) · Next: [Lab 3 — Resilience, observability, configuration, scaling](../lab-3-resilience-observability/)
