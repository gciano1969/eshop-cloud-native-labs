# Lab 4 — Orchestrated saga, CI with Tekton, minimal GitOps

**Concepts:** distributed transactions without 2PC, orchestrated saga, pivot and compensation,
saga timeout, late replies, idempotent commands; CI pipeline with Tekton; Kustomize base and overlays;
pull-based reconciliation (GitOps) and drift correction.

## Goal

Lab 4 turns the order confirmation into an **orchestrated saga**: order-service asks a new service,
**payment-service**, to authorise the payment, and confirms or cancels the order based on the reply,
with no shared transaction between the two databases. In parallel, payment-service's release goes
through a **Tekton pipeline** and a Git repository that the cluster applies by itself.

Five new pieces:

- **payment-service**, with its own database: it receives the `AuthorizePayment` command, decides, and
  replies with an event; an amount limit in configuration simulates the rejection;
- the **saga orchestrator** in order-service: statuses `PAYMENT_PENDING`, `CONFIRMED`, `CANCELLED`,
  command in the outbox together with the status change, saga timeout, void of late replies;
- the **OrderCancelled** event, which notification-service turns into an email to the customer;
- a **Tekton pipeline** that tests and builds payment-service;
- a **minimal reconciler**: a CronJob that every 5 minutes applies to the namespace what is on the
  `main` branch.

The lab is complete when:

- an order within the amount limit becomes `CONFIRMED` and the customer gets the confirmation email;
- an order above the limit becomes `CANCELLED`, with the reason, and the customer gets the cancellation email;
- with payment-service down the order waits until the timeout and is then cancelled; when payment-service
  comes back, the late authorisation is voided;
- a duplicate command or event produces neither a second payment nor a second transition;
- a Tekton pipeline completes and produces an image tagged with the commit;
- a manual change to payment-service is undone by the reconciler within 5 minutes;
- code, manifests, pipeline, decisions and verifications are in Git, without Secrets.

Estimated time: 6–8 hours. Same stack as before: Node.js, Express and the Confluent Kafka client. The
saga, the payment logic and the dead letter handling in this folder were run and checked locally
(results quoted below); the `oc` commands and the pipeline must be tried on your Sandbox.

```text
lab-4-saga-gitops/
├── order-service/          # + saga.js (orchestrator), kafka-consumer.js, saga timeout check
├── payment-service/        # new: decide.js, payments.js, kafka-consumer.js, relay.js, tests
├── notification-service/   # + OrderCancelled
├── catalog-service/        # unchanged from Lab 3
├── .tekton/
│   ├── payment-ci.yaml       # the Pipeline
│   └── payment-ci-run.yaml   # an example PipelineRun
└── k8s/
    ├── base/                 # payment-service and its ConfigMap
    ├── overlays/sandbox/     # what the reconciler applies
    ├── payment-build.yaml    # ImageStream + BuildConfig of payment-service
    ├── reconciler.yaml       # the CronJob
    └── kafka-topics.sh       # + payment.commands, payment.events, payment.dlq, orders.dlq
```

## Prerequisites, Sandbox constraints and ground rules

Start from a working Lab 3: three services with their databases, Kafka, ConfigMaps and Secrets, JSON
logs, metrics and probes, everything in Git.

| Constraint | Consequence for this lab |
| --- | --- |
| No cluster-admin rights | No OpenShift GitOps or Argo CD: the minimal reconciler of Step 6 replaces them. OpenShift Pipelines, instead, is already installed in the Sandbox. |
| Quota of about 3 cores and 14 GB RAM | One more service and one more database, plus the PipelineRun and CronJob Pods. Before starting: bring catalog-service back to one replica and check `oc describe quota`. |
| Limited storage | Each PipelineRun asks for a temporary 1 GiB volume, deleted with the PipelineRun: keep only the latest runs. |
| Pods stopped after 12 consecutive hours | No impact on the saga: state is in the database, messages in Kafka. |
| Expires after 30 days | Everything recreatable from Git, pipeline in `.tekton/` and overlays in `k8s/` included. |
| Public GitHub repository | The reconciler clones without credentials. A private repository would need a token in a Secret: an extension. |

Ground rules, the same as the previous labs:

- no real data: no cards, no IBANs, no real names; payment-service simulates, it does not charge;
- passwords and keys generated for the exercise and never reused;
- Secrets **excluded** from commits;
- suspend the reconciler's CronJob when you stop working
  (`oc patch cronjob reconciler -p '{"spec":{"suspend":true}}'`), so it does not use quota.

## Step 0 — Restart from Lab 3

1. **Log in and check the state.** Scale stopped replicas back to 1, catalog-service included.

```bash
oc project                                                  # <username>-dev
oc get deploy -l app.kubernetes.io/part-of=eshop           # 7 Deployments
oc scale deploy -l app.kubernetes.io/part-of=eshop --replicas=1
oc describe quota                                           # note free CPU and memory
```

2. **Lab 3 test.** Create and confirm an order, check the email in notification-service's logs and the
   closed circuit on `/metrics`. If something is wrong, fix Lab 3 first.
3. **Tools and permissions to check.** Answers go in `docs/verifications-lab4.md`.

```bash
oc api-resources --api-group=tekton.dev                       # Pipeline, PipelineRun, Task: Pipelines is there
oc get serviceaccount pipeline                                # created by OpenShift Pipelines in the namespace
oc auth can-i create pipelineruns.tekton.dev                  # yes
oc auth can-i create cronjobs                                 # yes
oc auth can-i create rolebindings                             # yes: needed in Step 6
oc get is -n openshift cli                                    # image with oc, used by the reconciler
```

4. **Point the builds at this lab.**

```bash
REPO=https://github.com/<your-github-user>/eshop-cloud-native-labs
LAB=lab-4-saga-gitops
for s in catalog-service order-service notification-service; do
  oc patch bc/$s --type=merge -p "{\"spec\":{\"source\":{\"contextDir\":\"$LAB/$s\"}}}"
done
```

From Step 6 the reconciler reads the `main` branch of your fork: manifest changes reach the cluster
only once they are on `main`.

## Step 1 — Design: statuses, messages, contracts

Before any code, fix three things: the order statuses, the messages between the services, and the
business rule about a payment that never answers. Start from the saga step table of your design
exercise, saved in `docs/saga-lab4.md`.

```text
                POST /orders/{id}/confirm  → 202
 PLACED ─────────────────────────────────────────► PAYMENT_PENDING
                    AuthorizePayment in the outbox      │
                                                         ├── PaymentAuthorized ─► CONFIRMED   (pivot)
                                                         ├── PaymentRejected ───► CANCELLED   (compensation)
                                                         └── no reply in time ──► CANCELLED   (compensation)
                                                               └─ a late PaymentAuthorized ⇒ VoidPayment
```

The command leaves only from `PAYMENT_PENDING`; from there a final state is always reached, by reply or
by timeout.

### Order statuses

| Status | Meaning | Set by |
| --- | --- | --- |
| `PLACED` | order created, not yet confirmed by the customer | `POST /orders` (Lab 1) |
| `PAYMENT_PENDING` | saga started, payment command sent | `POST /orders/:id/confirm` |
| `CONFIRMED` | payment authorised | consumer of `payment.events` |
| `CANCELLED` | payment rejected, or no reply within the timeout | consumer of `payment.events` or timeout check |

The biggest change compared to Lab 2: `POST /orders/:id/confirm` no longer confirms the order, it starts
the saga, and answers **202 Accepted** instead of 200. The customer reads "order received, we will let you
know when it is confirmed".

### The messages

All in a CloudEvents envelope, with the message key equal to the order id: the messages of one saga sit in
the same partition and arrive in order.

| Topic | Message (`type`) | From → to | Data |
| --- | --- | --- | --- |
| `payment.commands` | `com.example.eshop.payment.authorize.v1` | order → payment | `orderId`, `customerId`, `amountCents` |
| `payment.commands` | `com.example.eshop.payment.void.v1` | order → payment | `orderId`, `reason` |
| `payment.events` | `com.example.eshop.payment.authorized.v1` | payment → order | `orderId`, `amountCents` |
| `payment.events` | `com.example.eshop.payment.rejected.v1` | payment → order | `orderId`, `amountCents`, `reason` |
| `payment.events` | `com.example.eshop.payment.voided.v1` | payment → order | `orderId` |
| `orders.events` | `com.example.eshop.order.confirmed.v1` | order → notification | as in Lab 2 |
| `orders.events` | `com.example.eshop.order.cancelled.v1` | order → notification | `orderId`, `customerId`, `reason` |

Commands on a Kafka topic behave like a queue because only one consumer group, payment-service's, reads
`payment.commands`.

### The timeout rule

If payment-service does not reply within `SAGA_TIMEOUT_S` seconds (30 in the Sandbox), the order is
**cancelled**. It is a business rule, the same as the "payment with no answer" hotspot of the Event
Storming, and here we decide it this way: better a cancelled order and a notified customer than an order
left hanging. If the authorisation then arrives late, the orchestrator sends `VoidPayment`.

### The topics

Run the updated [`k8s/kafka-topics.sh`](k8s/kafka-topics.sh) again (it skips the topics that already exist):

```bash
bash $LAB/k8s/kafka-topics.sh
```

It adds `payment.commands`, `payment.events` (3 partitions each), `payment.dlq` and `orders.dlq`
(1 partition each). `orders.dlq` receives the payment replies that order-service's saga consumer cannot
process; without it the consumer would fail the first time it needs it.

Update the C4 diagram in `docs/` too: payment-service, payment-db and the new topics.

## Step 2 — payment-service

A small service, with its own database and no Route: it reads the commands from `payment.commands`,
decides, and puts the reply in its own outbox. It reuses three pieces already written: the Lab 2 outbox
relay and the Lab 3 `tracing.js`, `logger.js` and `metrics.js` modules.

### Its database

```bash
oc new-app postgresql-persistent \
  -p DATABASE_SERVICE_NAME=payment-db \
  -p POSTGRESQL_DATABASE=payments \
  -p POSTGRESQL_USER=payments \
  -p POSTGRESQL_PASSWORD="$(openssl rand -hex 12)" \
  -p VOLUME_CAPACITY=1Gi \
  -p MEMORY_LIMIT=512Mi \
  -l app.kubernetes.io/part-of=eshop
```

```sql
CREATE TABLE IF NOT EXISTS payment (
  order_id     TEXT PRIMARY KEY,                -- one payment per order: the idempotency key
  amount_cents INTEGER NOT NULL,
  status       TEXT NOT NULL CHECK (status IN ('AUTHORIZED', 'REJECTED', 'VOIDED')),
  reason       TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- plus an outbox table identical to order-service's (with traceparent)
```

### The decision

A pure function, separate from database and Kafka, so that the pipeline of Step 5 can test it with
nothing else running: [`payment-service/decide.js`](payment-service/decide.js).

```js
const LIMIT = Number(process.env.PAYMENT_LIMIT_CENTS || 50000);   // above EUR 500: rejected

function decide(amountCents) {
  if (!Number.isInteger(amountCents) || amountCents <= 0) return { ok: false, reason: 'invalid amount' };
  if (amountCents > LIMIT) return { ok: false, reason: `amount above the limit of EUR ${LIMIT / 100}` };
  return { ok: true, reason: null };
}
```

Its tests, [`payment-service/test/decide.test.js`](payment-service/test/decide.test.js), run with
`npm test` (`node --test`): within the limit, exactly at the limit, above the limit, invalid amount.

### Handling the commands

Each command is processed in one transaction: the `payment` row and the reply in the outbox are written
together. The `INSERT … ON CONFLICT DO NOTHING` on the order id makes the authorisation idempotent: a
repeated command inserts nothing, and the original reply is already in the outbox. See
[`payment-service/payments.js`](payment-service/payments.js):

```js
case T.AUTHORIZE: {
  const { ok, reason } = decide(cmd.data.amountCents);
  const r = await db.query(
    `INSERT INTO payment (order_id, amount_cents, status, reason) VALUES ($1, $2, $3, $4)
       ON CONFLICT (order_id) DO NOTHING`,
    [orderId, cmd.data.amountCents, ok ? 'AUTHORIZED' : 'REJECTED', reason]);
  if (r.rowCount === 0) return 'duplicate';           // already decided: the reply is already in the outbox
  await reply(db, cloudEvent(ok ? T.AUTHORIZED : T.REJECTED, orderId, { orderId, amountCents, reason }), traceparent);
  return ok ? 'AUTHORIZED' : 'REJECTED';
}
case T.VOID: {
  const r = await db.query(
    `UPDATE payment SET status = 'VOIDED', reason = $2, updated_at = now()
      WHERE order_id = $1 AND status = 'AUTHORIZED'`, [orderId, cmd.data.reason]);
  if (r.rowCount === 0) return 'nothing to void';
  await reply(db, cloudEvent(T.VOIDED, orderId, { orderId }), traceparent);
  return 'VOIDED';
}
default:
  throw new SyntaxError(`unrecognised command: ${cmd.type}`);   // -> dead letter topic
```

### The consumer

The same pattern as notification-service in Lab 2, written once as a shared module
([`kafka-consumer.js`](payment-service/kafka-consumer.js)), also used by order-service in Step 3: one
message, one transaction, offset committed only after the commit; the tracing context from the headers as
in Lab 3; unreadable messages or unknown types to the dead letter topic, transient errors retried with
jitter, persistent errors restart the Pod.

In payment-service's [`server.js`](payment-service/server.js): the Lab 3 probes and `/metrics`, a check
route `GET /payments/:orderId` returning the `payment` row, the Lab 2 relay, and the consumer start:

```js
consume({ topic: 'payment.commands', groupId: process.env.KAFKA_GROUP, dlqTopic: 'payment.dlq', pool, handler: onCommand })
```

### Configuration and deployment

Its ConfigMap is [`k8s/base/payment-config.yaml`](k8s/base/payment-config.yaml)
(`PAYMENT_LIMIT_CENTS: "50000"`, Kafka group, retry settings). The first deployment is still done by hand,
like the services of the previous labs; from Step 6 the reconciler keeps it in line.

```bash
oc apply -f $LAB/k8s/base/payment-config.yaml
oc new-app nodejs:22-ubi9~$REPO \
  --context-dir=$LAB/payment-service --name=payment-service -l app.kubernetes.io/part-of=eshop
oc set env deploy/payment-service --from=secret/payment-db --prefix=DB_
oc patch deploy/payment-service --type=json -p '[{"op":"add","path":"/spec/template/spec/containers/0/envFrom","value":[{"configMapRef":{"name":"payment-config"}}]}]'
oc set resources deploy/payment-service --requests=cpu=50m,memory=128Mi --limits=cpu=300m,memory=256Mi
```

Probes as in Step 4 of Lab 3. Check: `oc logs deploy/payment-service | grep consumer` and, in the consumer
group, no backlog:

```bash
oc rsh deploy/kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group payment-service
```

## Step 3 — The orchestrator in order-service

Order confirmation stops being a single step. `POST /orders/:id/confirm` starts the saga and answers
**202 Accepted**: the order moves to `PAYMENT_PENDING` and the outcome will arrive by event. Three pieces,
all in order-service: the start, the reaction to payment-service's replies, the timeout.

### The schema

```sql
ALTER TABLE customer_order ADD COLUMN IF NOT EXISTS saga_deadline TIMESTAMPTZ;   -- by when the reply must arrive
ALTER TABLE customer_order ADD COLUMN IF NOT EXISTS cancel_reason TEXT;
CREATE INDEX IF NOT EXISTS customer_order_saga ON customer_order (saga_deadline) WHERE status = 'PAYMENT_PENDING';
```

If in Lab 2 you added a `CHECK` constraint on the `status` values, widen it to `PAYMENT_PENDING` and
`CANCELLED` (`\d customer_order` in `psql` shows the name of the constraint to replace).

| Status | Set by | From here to |
| --- | --- | --- |
| `PLACED` | order creation | `PAYMENT_PENDING` |
| `PAYMENT_PENDING` | saga start | `CONFIRMED` or `CANCELLED` |
| `CONFIRMED` | "authorized" reply (the pivot) | — |
| `CANCELLED` | rejection or timeout (the compensation) | — |

### The saga code

[`order-service/saga.js`](order-service/saga.js) holds the three pieces. The heart of it:

```js
// 1 · start, inside POST /orders/:id/confirm (one transaction)
async function startSaga(db, orderId, traceparent) {
  const { rows } = await db.query('SELECT id, customer_id, status FROM customer_order WHERE id = $1 FOR UPDATE', [orderId]);
  if (rows.length === 0) return { http: 404 };
  if (rows[0].status !== 'PLACED') return { http: 200, status: rows[0].status };        // already started: idempotent
  // total of the lines, status to PAYMENT_PENDING with the deadline, AuthorizePayment in the outbox
  return { http: 202, status: 'PAYMENT_PENDING' };
}

// 2 · replies from payment-service (one transaction per message)
case T.AUTHORIZED: {
  const r = await db.query(`UPDATE customer_order SET status = 'CONFIRMED'
                             WHERE id = $1 AND status = 'PAYMENT_PENDING' RETURNING customer_id`, [orderId]);
  if (r.rowCount === 1) { /* OrderConfirmed in the outbox */ return 'confirmed'; }
  if (currentStatus === 'CANCELLED') { /* VoidPayment in the outbox */ return 'void requested'; }   // late reply
  return 'duplicate';
}

// 3 · timeout: sagas waiting beyond the deadline are cancelled
SELECT id FROM customer_order WHERE status = 'PAYMENT_PENDING' AND saga_deadline < now()
 ORDER BY saga_deadline LIMIT 20 FOR UPDATE SKIP LOCKED
```

Three things to notice, because they are the check questions of Step 7:

- **Every transition is conditional** (`WHERE status = 'PAYMENT_PENDING'`). A duplicate or late event does
  not find the row in the expected status and changes nothing.
- **Confirmation is the pivot.** After `CONFIRMED` the saga does not go back; an authorisation arriving when
  the order is already `CANCELLED` does not reopen it, but asks payment-service to void.
- **The timeout uses `FOR UPDATE SKIP LOCKED`.** With two order-service replicas each takes different
  orders, and none is cancelled twice.

### Wire it into the service

In the confirm route, instead of the old direct update to `CONFIRMED`
([`order-service/server.js`](order-service/server.js)):

```js
app.post('/orders/:id/confirm', async (req, res) => {
  const db = await pool.connect();
  try {
    await db.query('BEGIN');
    const carrier = {};
    propagation.inject(context.active(), carrier);       // the trace of the HTTP request follows the command
    const r = await startSaga(db, req.params.id, carrier.traceparent);
    await db.query('COMMIT');
    if (r.http === 404) return res.status(404).json({ error: 'order not found' });
    res.status(r.http).json({ id: Number(req.params.id), status: r.status });
  } catch (err) { await db.query('ROLLBACK').catch(() => {}); throw err; }
  finally { db.release(); }
});
```

And at startup, the consumer of the replies and the timeout check:

```js
consume({ topic: 'payment.events', groupId: 'order-service-saga', dlqTopic: 'orders.dlq', pool, handler: onPaymentEvent });
setInterval(/* BEGIN; expireSagas(db); COMMIT */, 5000);
```

The Lab 2 outbox relay already reads each row's `topic` column: it publishes to `payment.commands` with no
change. The `order-service-saga` group is new, so it starts from the beginning of `payment.events`;
messages already processed do no harm, because every transition is conditional.

In order-service's ConfigMap add:

```yaml
SAGA_TIMEOUT_S: "30"    # maximum wait for the payment reply
```

then `oc apply -f $LAB/k8s/config/order-config.yaml && oc start-build order-service`.

## Step 4 — notification-service also announces cancellations

A second event type now flows on `orders.events`, `com.example.eshop.order.cancelled.v1`. The Lab 2 consumer
accepts only `confirmed.v1` and sends everything else to the dead letter topic: without this step, every
cancelled order would end up in `notification.dlq`. It is the classic case of a consumer that does not know a
new event yet; here we update it in the same release.

Two changes to [`notification-service/consumer.js`](notification-service/consumer.js). The envelope check
accepts the known types:

```js
const KNOWN = new Set([ORDER_CONFIRMED, ORDER_CANCELLED]);
if (!KNOWN.has(event.type) || !event.id || !event.data?.orderId) {
  throw new Error('unrecognised event');
}
```

The email text depends on the type:

```js
const body = event.type === ORDER_CANCELLED
  ? `Your order ${d.orderId} has been cancelled: ${d.reason}.`
  : `Your order ${d.orderId} is confirmed. Total: EUR ${(d.totalCents / 100).toFixed(2)}.`;
```

The rest does not change: event registry, one notification per event, offset after the commit. Rebuild and
check the log:

```bash
oc start-build notification-service --follow
oc logs deploy/notification-service | grep EMAIL
```

If some cancellation already ended up in `notification.dlq` meanwhile, put it back on `orders.events`: the
consumer now understands it, and the registry discards those already processed. Truly malformed messages
(verification 6.8 of Lab 2) go back to the dead letter topic, once.

```bash
oc rsh deploy/kafka bash -c '
B=/opt/kafka/bin; S="--bootstrap-server localhost:9092"
$B/kafka-console-consumer.sh $S --topic notification.dlq --from-beginning --timeout-ms 5000 \
  --property print.key=true --property key.separator="|" \
| $B/kafka-console-producer.sh $S --topic orders.events --property parse.key=true --property key.separator="|"'
```

## Step 5 — The payment-service pipeline with Tekton

OpenShift Pipelines is already installed in the Sandbox: no operator needed, just resources in your own
namespace. The pipeline does three things in order: takes the code at a precise commit, runs the tests,
builds the image and tags it with the commit SHA. It does not deploy: that is the job of the reconciler of
Step 6, which reads the version from Git.

```text
clone (commit X) ──► npm ci + npm test ──► oc start-build --commit=X ──► tag payment-service:<sha7>
```

First, one change to the deployment created in Step 2: `oc new-app` put a trigger on the Deployment that
updates the image at every build. Remove it, otherwise the version running would be decided by the latest
build and not by Git.

```bash
oc set triggers deploy/payment-service --remove-all
```

### The pipeline

The `git-clone` task comes from the operator's catalog through the *cluster resolver*; the other two are
written inside the pipeline itself. The images are Red Hat's public ones, which run with the Sandbox's
arbitrary UID. See [`.tekton/payment-ci.yaml`](.tekton/payment-ci.yaml); the test step works in
`$(workspaces.source.path)/lab-4-saga-gitops/payment-service`.

```bash
oc apply -f $LAB/.tekton/payment-ci.yaml
oc create -f $LAB/.tekton/payment-ci-run.yaml          # set repo-url to your fork first
tkn pipelinerun logs -f --last          # or in the console: Pipelines → PipelineRuns
oc get istag | grep payment-service     # the tag with the SHA appears
```

If you do not have `tkn` on your machine, the logs are in the web console. The volume requested by
`volumeClaimTemplate` is born with the run and disappears when you delete it: keep the namespace clean with
`oc delete pipelinerun -l tekton.dev/pipeline=payment-ci` now and then, because the Sandbox volume quota is
small. If `build-image` fails with `forbidden`, give the pipeline account the right to start builds:
`oc policy add-role-to-user edit -z pipeline`.

### Break the tests on purpose

In `decide.test.js` change the expectation of the "exactly at the limit" case to `false`, commit and push,
start a new run. The `test` task fails and `build-image` does not start: no new image, and the running
service does not change. Put the test back before going on.

If the `git-clone` task is not found, check its name in the operator's catalog with
`tkn task list -n openshift-pipelines` (or, without `tkn`, `oc get task -n openshift-pipelines`) and the
parameter names with `oc get task git-clone -n openshift-pipelines -o yaml`.

## Step 6 — Kustomize and a minimal reconciler

The Sandbox has no OpenShift GitOps (Argo CD) and you cannot install it: it needs an operator. We build the
essential part by hand, to see what it does: **a process inside the cluster that reads Git and applies what
it finds**, at regular intervals. That is the third and fourth OpenGitOps principles (state *pulled* by the
cluster, *continuously reconciled*) stripped to the bone.

### The manifest layout

```text
k8s/
├── base/                         # what payment-service is made of, no environment details
│   ├── kustomization.yaml
│   ├── payment-config.yaml       # from Step 2
│   ├── payment-deployment.yaml
│   └── payment-service.yaml
└── overlays/
    └── sandbox/
        └── kustomization.yaml    # which image and which tag: the line that changes at every release
```

The base Deployment uses the same selector `oc new-app` created (`deployment: payment-service`): a
Deployment's selector cannot be changed, so `oc apply` updates the existing one instead of failing. The
`payment-db` Secret stays out of Git, as in every lab: the manifest only names it.

[`k8s/overlays/sandbox/kustomization.yaml`](k8s/overlays/sandbox/kustomization.yaml):

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../base
images:
  - name: payment-service
    newName: image-registry.openshift-image-registry.svc:5000/<your-namespace>/payment-service
    newTag: "<sha7>"          # the tag produced by the pipeline of Step 5
```

Before handing it to the reconciler, see what it produces and what it would change:

```bash
oc kustomize $LAB/k8s/overlays/sandbox | less
oc diff -k $LAB/k8s/overlays/sandbox          # no output = the cluster already matches Git
```

### The reconciler

A CronJob every 5 minutes: [`k8s/reconciler.yaml`](k8s/reconciler.yaml). The first container clones the
repository with `git` (the Red Hat Node.js image has it); the second applies the overlay with `oc`. It runs
with its own ServiceAccount, which has the `edit` role **only in your namespace**.

```bash
oc apply -f $LAB/k8s/reconciler.yaml                          # set GIT_URL to your fork first
oc policy add-role-to-user edit -z reconciler
oc create job --from=cronjob/reconciler reconcile-now          # one round right away
oc logs job/reconcile-now -c git
oc logs job/reconcile-now -c apply
```

In the `apply` log each resource shows up as `configured` (it changed) or `unchanged` (it already matched Git).

### Releasing, from now on

1. Change payment-service's code, commit and push.
2. Run the pipeline: tests, image, tag `payment-service:<sha7>`.
3. Write that tag in `k8s/overlays/sandbox/kustomization.yaml`, commit and push.
4. Within 5 minutes the reconciler applies it: new rollout.

Rolling back is a `git revert` of the commit in point 3. Nobody runs `oc` by hand on the service.

### What is missing compared to Argo CD

Say it plainly, because the difference is part of the lesson:

| Argo CD | Our reconciler |
| --- | --- |
| removes from the cluster what was removed from Git (*prune*) | no: a resource deleted from Git stays in the cluster until you delete it |
| shows sync and health status of every resource | only the log of the last Job |
| reacts to a repository webhook | polls Git every 5 minutes |
| handles many repositories, environments and permissions | one repository, one overlay, the `edit` role in one namespace |

Watch out for the Sandbox's idling too: when the Sandbox scales idle Deployments to zero, the reconciler
brings them back to the replicas written in Git at the next round. It is the correct behaviour of a
reconciler, but it uses quota: suspend it when you are not working with
`oc patch cronjob/reconciler -p '{"spec":{"suspend":true}}'`.

## Step 7 — Verify the concepts

Each test ties a command to a concept. Write in `docs/verifications-lab4.md` the command, what you observed
and what it proves.

### Before the tests

If you applied the Lab 3 network policies, payment-service is a new Pod nobody has allowed yet: without two
extra rules it reaches neither its database nor Kafka. They are at the end of
[`k8s/networkpolicy.yaml`](k8s/networkpolicy.yaml) (`payment-db-from-payment`, `kafka-from-payment`). Check the
real labels with `oc get pods --show-labels` before applying. Then the shortcuts used below:

```bash
ORDERS=https://$(oc get route order-service -o jsonpath='{.spec.host}')
NEW='{"customer_id":"C-001","lines":[{"product_id":1,"quantity":1}]}'
BIG='{"customer_id":"C-002","lines":[{"product_id":1,"quantity":50}]}'   # 50 × EUR 12 = EUR 600: above the limit
KT="oc rsh deploy/kafka /opt/kafka/bin"
new_order() { curl -s -X POST -H 'Content-Type: application/json' -d "$1" $ORDERS/orders | grep -o '"id":[0-9]*' | head -1 | cut -d: -f2; }
status()    { curl -s $ORDERS/orders/$1; echo; }
payment()   { oc rsh deploy/payment-service curl -s localhost:8080/payments/$1; echo; }
```

Check the total of `BIG` against your own catalog prices: it must exceed EUR 500.

### 7.1 The happy path: asynchronous confirmation

```bash
ID=$(new_order "$NEW"); echo $ID
curl -s -i -X POST $ORDERS/orders/$ID/confirm | grep -E '^HTTP|status'
sleep 5; status $ID; payment $ID
oc logs deploy/notification-service --since=1m | grep EMAIL
```

Expected: confirmation answers `202` with `PAYMENT_PENDING`; a few seconds later the order is `CONFIRMED`, the
payment `AUTHORIZED`, and there is a confirmation email. Take the `trace_id` from order-service's `saga started`
log line and search for it in the other two services: it is the same, across two topics and three outboxes.

```bash
TID=$(oc logs deploy/order-service --since=2m | grep 'saga started' | grep "\"order_id\":$ID" | grep -o '"trace_id":"[0-9a-f]*"' | head -1 | cut -d'"' -f4)
for s in order-service payment-service notification-service; do echo "== $s"; oc logs deploy/$s --since=2m | grep -c "$TID"; done
```

Local run of the reference code: 3 lines in order-service, 1 in payment-service, 1 in notification-service,
all with the same trace id.

### 7.2 Payment rejected: the compensation

```bash
ID=$(new_order "$BIG"); curl -s -X POST $ORDERS/orders/$ID/confirm; echo
sleep 5; status $ID; payment $ID
oc logs deploy/notification-service --since=1m | grep EMAIL
```

Expected: payment `REJECTED`, order `CANCELLED` with `cancel_reason` "payment rejected: amount above the limit of
EUR 500", cancellation email. No step was "undone": the cancellation is a new transition, recorded and notified.

### 7.3 No reply: timeout and late reply

The most instructive case. First suspend the reconciler, otherwise it would bring payment-service back to one
replica during the test: it is in charge, as it should be.

```bash
oc patch cronjob/reconciler -p '{"spec":{"suspend":true}}'
oc scale deploy/payment-service --replicas=0
ID=$(new_order "$NEW"); curl -s -X POST $ORDERS/orders/$ID/confirm; echo
sleep 40; status $ID                           # CANCELLED, "no reply from the payment within the time limit"
oc scale deploy/payment-service --replicas=1
sleep 20; status $ID; payment $ID
oc logs deploy/order-service --since=1m | grep -E 'void|cancelled'
oc patch cronjob/reconciler -p '{"spec":{"suspend":false}}'
```

Expected: after 30–35 seconds the order is `CANCELLED` by timeout. When payment-service restarts it finds the
command still in the topic and authorises it; order-service receives an authorisation for an order already
cancelled, does not reopen it and asks for a void. In the end: order `CANCELLED`, payment `VOIDED`. The final
state is consistent, even though the two services never agreed in real time.

Local run of the reference code: outcomes `cancelled` (timeout), `void requested`, `void confirmed`; payment
`VOIDED` with reason "authorization arrived after the cancellation".

### 7.4 Duplicates: no double effect

```bash
curl -s -X POST $ORDERS/orders/$ID/confirm; echo     # the 7.3 order again: 200 with the current status, no new command

# re-read every payment command from the beginning
oc scale deploy/payment-service --replicas=0
$KT/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group payment-service \
  --reset-offsets --to-earliest --topic payment.commands --execute
oc scale deploy/payment-service --replicas=1
sleep 15; oc logs deploy/payment-service | grep -c '"outcome":"duplicate"'
```

Expected: one `duplicate` for every authorisation already decided, a `nothing to void` for the void already
applied, no new row in `payment`, no new reply. Remember to suspend the reconciler here too, if the test takes
more than a few minutes.

### 7.5 Messages that cannot be processed

```bash
oc rsh deploy/kafka bash -c '
B=/opt/kafka/bin; S="--bootstrap-server localhost:9092"
printf "%s\n" "k1|not json" "k2|{\"type\":\"com.example.eshop.payment.unknown.v1\",\"data\":{\"orderId\":\"1\"}}" \
| $B/kafka-console-producer.sh $S --topic payment.commands --property parse.key=true --property key.separator="|"
sleep 5
$B/kafka-console-consumer.sh $S --topic payment.dlq --from-beginning --timeout-ms 5000 --property print.headers=true'
```

Expected: both in `payment.dlq`, with the header `x-error` ("Unexpected token…" for the first,
"unrecognised command" for the second); payment-service keeps processing the other commands.

### 7.6 A release from the pipeline to Git to the cluster

Make a visible change to payment-service (for example the startup log message), commit and push. Run the
pipeline, write the new tag in the overlay, commit and push. Then:

```bash
oc create job --from=cronjob/reconciler reconcile-now-2
oc logs job/reconcile-now-2 -c apply
oc get deploy payment-service -o jsonpath='{.spec.template.spec.containers[0].image}'; echo
oc rollout status deploy/payment-service
```

Expected: the Deployment's image ends with the new SHA. Now `git revert` the overlay commit, push, run another
round: you are back to the previous image. A rollback is a commit.

### 7.7 Drift corrects itself

```bash
oc scale deploy/payment-service --replicas=0
oc patch configmap payment-config -p '{"data":{"PAYMENT_LIMIT_CENTS":"100"}}'
oc create job --from=cronjob/reconciler reconcile-now-3
sleep 30; oc get deploy payment-service; oc get configmap payment-config -o jsonpath='{.data.PAYMENT_LIMIT_CENTS}'; echo
```

Expected: one replica and limit `50000`, as in Git. Notice one detail: had the Pod stayed up, the ConfigMap
change would not have reached it anyway, because `envFrom` is read only at startup. The reconciler corrects
the cluster, not processes already running.

### 7.8 Check questions

Answer in writing in `docs/verifications-lab4.md`:

1. Why does `POST /orders/:id/confirm` answer 202 and not 200? What must the client do to learn the outcome?
2. What is the pivot of this saga? What would happen if after the confirmation there were a step that can
   fail (for example shipping)?
3. In test 7.3, for a few seconds the order was cancelled and the payment authorised. Is that a bug? Which
   mechanism resolves it, and why is it not a rollback?
4. Why does the timeout cancellation use `FOR UPDATE SKIP LOCKED` and not a plain `UPDATE`?
5. The pipeline does not deploy. What do you gain by separating "building an image" from "deciding which
   image runs"?
6. Name two things Argo CD would do and your reconciler does not. In which of today's tests would you have
   noticed?

## Step 8 — Save to Git and wrap up

In this lab Git is no longer just where you save your work: it is the source the cluster takes its
configuration from. Whatever is not in Git, the reconciler does not know about, and it is lost with the Sandbox.

```bash
git add $LAB
git status                                   # no Secret, no .env file
grep -rl 'kind: Secret' $LAB/k8s $LAB/.tekton  # must list only k8s/secrets.example.yaml
git commit -m "Lab 4: payment saga, Tekton pipeline, reconciler"
git tag lab-4 && git push && git push --tags
```

Remember to commit `payment-service/package-lock.json` too: without it `npm ci` fails in the pipeline.

### Deliverables

| Item | Where |
| --- | --- |
| payment-service: `decide.js`, `payments.js`, `kafka-consumer.js`, `server.js`, tests | `payment-service/` |
| order-service: `saga.js`, `kafka-consumer.js`, confirm route, timeout check, `ALTER TABLE` | `order-service/` |
| notification-service with the cancellation event | `notification-service/` |
| Kustomize base and overlay, `reconciler.yaml`, updated network policies | `k8s/` |
| Pipeline and PipelineRun | `.tekton/` |
| Updated C4 diagram: payment-service, payment-db, the new topics | `docs/c4-container.drawio` + PNG |
| Order state diagram and sequence diagrams of the three outcomes (confirmed, rejected, expired) | `docs/saga-lab4.md` |
| Verifications 7.1–7.7 and answers to 7.8 | `docs/verifications-lab4.md` |

When you are done, suspend the reconciler and scale down what you do not need: the Sandbox quota is shared by
all the Pods of the namespace.

```bash
oc patch cronjob/reconciler -p '{"spec":{"suspend":true}}'
oc delete pipelinerun -l tekton.dev/pipeline=payment-ci
```

## Completion checklist

- [ ] payment-db created, `payment` and `outbox` schema applied
- [ ] payment-service `npm test` green locally and in the pipeline
- [ ] payment-service running, consumer group `payment-service` with no backlog
- [ ] Confirmation answers 202; the order reaches `CONFIRMED` (7.1)
- [ ] Order above the limit: `CANCELLED` with reason and cancellation email (7.2)
- [ ] Timeout and late reply: order `CANCELLED`, payment `VOIDED` (7.3)
- [ ] Duplicates without effect (7.4), unreadable messages in `payment.dlq` (7.5)
- [ ] Image trigger removed from payment-service's Deployment
- [ ] Green pipeline producing `payment-service:<sha7>`; with a broken test it produces nothing
- [ ] Reconciler active: release and rollback with a commit (7.6), drift corrected (7.7)
- [ ] No Secret in Git, no real data in the Sandbox
- [ ] Everything in Git, tag `lab-4`

## Common problems

| Symptom | Likely cause | What to do |
| --- | --- | --- |
| The order stays `PAYMENT_PENDING` and is always cancelled by timeout | payment-service is not reading, or its replies do not leave | `kafka-consumer-groups.sh --describe --group payment-service`; look for `message processed` in payment-service's log; check its relay is on (`RELAY_ENABLED`) |
| The saga consumer of order-service crashes on its first bad message | topic `orders.dlq` missing | run `k8s/kafka-topics.sh` again |
| Cancellations end up in `notification.dlq` | notification-service not updated (Step 4) | rebuild it and replay the DLQ as in Step 4 |
| `ERROR: new row for relation "customer_order" violates check constraint` | a `CHECK` constraint on the statuses from Lab 2 | widen it to `PAYMENT_PENDING` and `CANCELLED` (Step 3) |
| payment-service cannot reach payment-db or Kafka | Lab 3 network policies without the new rules | the two rules in "Before the tests" |
| The pipeline cannot find `git-clone` | task or parameter name different in your operator version | `oc get task -n openshift-pipelines`, then the parameters with `-o yaml` |
| `npm ci` fails in the pipeline | `package-lock.json` missing from the repository | `npm install` locally, commit the lock file |
| The `build-image` task fails with `forbidden` | the `pipeline` ServiceAccount cannot start builds | `oc policy add-role-to-user edit -z pipeline` |
| The PipelineRun stays `Pending` | volume quota full | `oc delete pipelinerun -l tekton.dev/pipeline=payment-ci`, then `oc get pvc` |
| The reconciler fails with `forbidden` | `edit` role not given to its ServiceAccount | `oc policy add-role-to-user edit -z reconciler` |
| `oc apply` in the reconciler: `field is immutable` on the selector | the Deployment was not created with `oc new-app` | delete the Deployment and let the reconciler recreate it |
| After a release the Pod does not change image | the image trigger is still on the Deployment, or the tag in the overlay does not exist | `oc set triggers deploy/payment-service --remove-all`; `oc get istag \| grep payment-service` |
| A manual test is undone after a few minutes | the reconciler brings the cluster back to Git | that is correct: suspend it during the test |

## References

- Chris Richardson, *Microservices Patterns*, chapter 4 (sagas), and microservices.io: [Saga](https://microservices.io/patterns/data/saga.html), [Transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html), [Idempotent consumer](https://microservices.io/patterns/communication-style/idempotent-consumer.html).
- [OpenGitOps Principles v1.0.0](https://opengitops.dev/).
- Red Hat OpenShift Pipelines documentation: *Creating CI/CD solutions for applications using OpenShift Pipelines* and *Specifying remote pipelines and tasks using resolvers* (cluster resolver).
- [Kustomize: images](https://kubectl.docs.kubernetes.io/references/kustomize/kustomization/images/) and overlays.
- [CloudEvents 1.0](https://cloudevents.io/).

---

Previous: [Lab 3](../lab-3-resilience-observability/) · Back to the [overview](../README.md)
