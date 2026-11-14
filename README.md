# E-Shop Cloud Native Labs

Four hands-on labs that take a small e-shop from a monolith to an event-driven, observable,
self-healing microservice system on **Red Hat OpenShift** — entirely on the free
[Developer Sandbox](https://developers.redhat.com/developer-sandbox), with no cluster-admin rights
and no operators of your own.

Each lab has a step-by-step guide and a complete, runnable reference solution in Node.js.

| Lab | You build | Key concepts |
| --- | --- | --- |
| [1 — Decomposition](lab-1-decomposition/) | `catalog-service` and `order-service`, one PostgreSQL each, REST between them | bounded contexts, database per service, service discovery, probes, independent release |
| [2 — Asynchronous events](lab-2-async-events/) | single-node Kafka, transactional outbox, `notification-service` | CloudEvents, keys and partitions, at-least-once, idempotent consumer, dead letter topic |
| [3 — Resilience and observability](lab-3-resilience-observability/) | resilient catalog client, JSON logs with trace id, RED metrics, HPA | timeout, retry with jitter, circuit breaker, bulkhead, W3C trace context across Kafka, liveness vs readiness |
| [4 — Saga and GitOps](lab-4-saga-gitops/) | `payment-service`, orchestrated saga, Tekton pipeline, a minimal reconciler | pivot and compensation, saga timeout, late replies, CI/CD separation, Kustomize, drift correction |

Each lab builds on the previous one. Every lab folder is a full snapshot of the system **at the end
of that lab**, so you can start from any of them.

## The case

A deliberately small e-shop: products with a price, orders with lines, an email when an order is
confirmed, a payment that can be authorised or rejected. Small enough to build in a few hours per
lab, rich enough to hit the real problems: who owns the price, what happens when a dependency is slow,
how an event survives a crash, how two databases agree without a shared transaction.

```text
                         ┌──────────────────┐
  customer ── Route ───► │  order-service   │──REST──► catalog-service ──► catalog-db
                         │  saga · outbox   │
                         └──┬───────────┬───┘
                            │           │ order-db
              payment.commands    orders.events
                            ▼           ▼
                  payment-service   notification-service
                    payment-db        notification-db
                            │
                     payment.events ──► back to order-service
```

## Prerequisites

- A [Red Hat Developer Sandbox](https://developers.redhat.com/developer-sandbox) account (free, 30 days,
  renewable).
- The `oc` CLI, Git and a GitHub account. `tkn` (Tekton CLI) is optional for Lab 4.
- Node.js 22 if you want to run the services locally.
- Basic familiarity with containers, Kubernetes objects and SQL.

## How to use this repository

1. **Fork it** to your GitHub account and keep the fork public: OpenShift builds straight from it, and the
   Lab 4 reconciler clones it without credentials.
2. Replace `<your-github-user>` in the manifests you apply (`grep -rl '<your-github-user>' .`).
3. Open the guide of [Lab 1](lab-1-decomposition/) and follow it step by step.

You can use the labs in two ways:

- **Build it yourself** (recommended for learning): write the code following the guide, in your own
  folders, and use the reference solution only to compare.
- **Deploy the reference**: point the OpenShift builds at the lab folder
  (`--context-dir=lab-N-…/<service>`) and focus on the operational steps and the verifications.

Moving from one lab to the next on a running environment only needs the builds re-pointed, for example:

```bash
oc patch bc/order-service --type=merge \
  -p '{"spec":{"source":{"contextDir":"lab-2-async-events/order-service"}}}'
oc start-build order-service
```

Each guide follows the same structure: goal and expected result, Sandbox constraints, numbered steps,
verifications that tie a command to a concept, check questions, deliverables, a completion checklist and a
table of common problems.

## Repository layout

```text
.
├── lab-1-decomposition/
│   ├── README.md                 # the guide
│   ├── catalog-service/          # each service: package.json, server.js, db.js, ...
│   ├── order-service/
│   ├── k8s/                      # reference manifests; Secrets only as examples with placeholders
│   └── docs/
├── lab-2-async-events/           # + notification-service, k8s/kafka.yaml, docs/events/
├── lab-3-resilience-observability/  # + k8s/config/, HPA, network policies, ServiceMonitor
└── lab-4-saga-gitops/            # + payment-service, .tekton/, k8s/base, k8s/overlays, reconciler
```

## Running a service locally

Each service reads its configuration from environment variables and creates its tables at startup.
For example, catalog-service against a local PostgreSQL:

```bash
podman run -d --name catalog-db -e POSTGRESQL_USER=catalog -e POSTGRESQL_PASSWORD=catalog \
  -e POSTGRESQL_DATABASE=catalog -p 5432:5432 registry.redhat.io/rhel9/postgresql-16
cd lab-1-decomposition/catalog-service
npm install
DB_HOST=localhost DB_PORT=5432 DB_DATABASE_NAME=catalog DB_DATABASE_USER=catalog \
  DB_DATABASE_PASSWORD=catalog PORT=8080 npm start
```

From Lab 2 on, services also need `KAFKA_BOOTSTRAP`; from Lab 3 on, `npm start` loads OpenTelemetry
through `node --require ./tracing.js`. payment-service has unit tests: `cd lab-4-saga-gitops/payment-service && npm test`.

## What has been tested, and how

The reference code was run locally before publication:

- every service started against real PostgreSQL wire protocol (an in-memory PostgreSQL), driven over HTTP
  with the requests of each guide;
- Kafka was replaced, **for the tests only**, by an in-memory stand-in implementing the client calls the
  services use (produce, consume by group, manual offset commit, offset reset). The Kafka client calls were
  checked against the type definitions of `@confluentinc/kafka-javascript` 1.11;
- verified behaviours include: copied prices, 422/400/503 responses, outbox idempotency under parallel
  confirmations, duplicate and replay handling, recovery after broker and relay outages, dead letter routing,
  the circuit breaker opening (503 in 2.5 s, then 1.7 ms) and closing, no retries on 404, trace ids carried
  across HTTP and Kafka, the saga's confirm / reject / timeout-then-void paths, and the payment unit tests.

What is **not** covered by those runs and must be tried on a real cluster: the `oc` commands, the Kafka
broker manifest, the Tekton pipeline, the Kustomize overlay rendering and the reconciler CronJob. The guides
flag the points most likely to differ between Sandbox versions (task names in OpenShift Pipelines, existing
network policies, monitoring permissions).

## Ground rules for every lab

- No real data, no real email addresses, no company code or credentials in the Sandbox.
- Passwords and keys generated for the exercise and never reused.
- Secrets never committed: each `k8s/` folder holds only `secrets.example.yaml` with placeholders.

## Licence

- Code (everything under the service folders, `k8s/` and `.tekton/`): [MIT](LICENSE).
- Guides (`README.md` files and `docs/`): [Creative Commons Attribution 4.0](LICENSE-docs).

© 2026 Giuseppe Ciano
