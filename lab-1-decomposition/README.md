# Lab 1 — From monolith to two microservices

**Concepts:** bounded contexts, service granularity, database per service, synchronous REST,
server-side service discovery, readiness probes, independent deployability.

## Goal and expected result

At the end of this lab the e-shop runs on OpenShift as **two independent microservices**:
`catalog-service` and `order-service`. Each has its own PostgreSQL database, and they talk over REST.

The lab is complete when:

- an order created through order-service carries the name and price read from catalog-service;
- each service can be released, scaled or stopped without touching the other;
- no service knows the other service's database credentials;
- everything needed to recreate the environment (code, YAML manifests, C4 diagram) is in Git.

Estimated time: 4–6 hours, split between design (about 1 hour) and implementation.
Any language works; the examples use Node.js with Express. The reference solution is in this folder:

```text
lab-1-decomposition/
├── catalog-service/      # products and current prices
├── order-service/        # orders, with name and price copied at purchase time
├── k8s/                  # reference manifests (Secrets excluded: only an example with placeholders)
└── docs/openapi.yaml     # the REST contract of both services
```

## Developer Sandbox constraints and ground rules

The [Red Hat Developer Sandbox](https://developers.redhat.com/developer-sandbox) is a free
OpenShift environment with a few limits that shape how you work. Knowing them up front saves time.

| Constraint | Consequence for this lab |
| --- | --- |
| One namespace (`<username>-dev`) | Both services and both databases share a namespace. Isolation comes from separate Deployments, Services and Secrets. In production they would be different namespaces. |
| Quota of about 3 cores, 14 GB RAM, 40 GB storage | Always set small requests and limits. A 1 GiB database volume is enough. |
| Pods stopped after 12 consecutive hours | Use *persistent* PostgreSQL so data survives. When you come back, scale replicas back to 1. |
| Expires after 30 days | Everything must be recreatable from Git: code and YAML manifests. |
| No cluster-admin rights | No operators and no cluster-wide resources. This lab does not need them. |

Ground rules:

- no real data, no company source code, no company credentials in the environment;
- database passwords generated for the exercise and never reused elsewhere;
- a personal Git repository, with Secrets **excluded** from every commit.

## Step 0 — Prepare the environment

You need the `oc` CLI, Git and a GitHub (or GitLab) account. The web console is fine for looking
around, but the guide uses the CLI.

1. **Install `oc`.** In the Sandbox console: **?** icon top right → *Command Line Tools* → download
   the build for your system. Check with `oc version`.
2. **Log in.** In the console: your user name top right → *Copy login command* → *DevSandbox* →
   *Display Token*. Copy and run the `oc login --token=... --server=...` command.
3. **Check the project and the quota.**

```bash
oc project                          # must show <username>-dev
oc describe quota                   # note the CPU, memory and storage limits
oc get templates -n openshift | grep postgresql
```

4. **Fork this repository** on GitHub. The commands below build straight from your fork, so set two
   variables you will reuse:

```bash
REPO=https://github.com/<your-github-user>/eshop-cloud-native-labs
LAB=lab-1-decomposition
```

If you prefer to write the code yourself, create your own folders with the same layout and use the
code here only to compare. Either way, keep `.gitignore` excluding `k8s/*secret*.yaml` (except the
example file) and `.env`.

## Step 1 — Design (about 1 hour, before any code)

Code comes after boundaries. This step produces the diagram that drives every other step.

1. **Quick Event Storming (20 min).** On a board (Miro or paper) lay out the e-shop events in time
   order: *Product created*, *Price updated*, *Order placed*, *Stock reserved*, *Payment
   authorized*, *Order confirmed* and the others. For each one note the command and the actor. Mark
   the hotspots in red.
2. **Group the events.** Events that change together and share the same language form a bounded
   context. This lab needs only **Catalog** and **Orders**; draw the other contexts but do not
   build them.
3. **A card per service.** For catalog-service and order-service write: the responsibility in one
   sentence without the word "and", the data it owns, its aggregates.
4. **Apply the four granularity tests:** owner, explanation, rewrite, calls.
5. **Choose the interactions.** In this lab the only interaction is synchronous REST: order-service
   asks catalog-service for product data. Mark as "future" what will become an event (for example
   *Price updated*).
6. **Draw the C4 Container diagram** in draw.io: the two services, the two databases, the Route,
   the user. On each arrow: protocol and reason. Save it as `docs/c4-container.drawio` and export a PNG.

Decision to record on the diagram: order-service **copies the price** of the product into the order
line at purchase time. That resolves the "Price" hotspot: the catalog price changes, the order price
does not.

Output of this step: `docs/c4-container.png` and `docs/boundaries.md` (half a page: one paragraph
per boundary, with the reason it exists).

## Step 2 — One database per service

Each service gets its own PostgreSQL with its own user and password, from the
`postgresql-persistent` template already in the cluster.

```bash
# Catalog database
oc new-app postgresql-persistent \
  -p DATABASE_SERVICE_NAME=catalog-db \
  -p POSTGRESQL_DATABASE=catalog \
  -p POSTGRESQL_USER=catalog \
  -p POSTGRESQL_PASSWORD="$(openssl rand -hex 12)" \
  -p VOLUME_CAPACITY=1Gi \
  -p MEMORY_LIMIT=512Mi \
  -l app.kubernetes.io/part-of=eshop

# Orders database
oc new-app postgresql-persistent \
  -p DATABASE_SERVICE_NAME=order-db \
  -p POSTGRESQL_DATABASE=orders \
  -p POSTGRESQL_USER=orders \
  -p POSTGRESQL_PASSWORD="$(openssl rand -hex 12)" \
  -p VOLUME_CAPACITY=1Gi \
  -p MEMORY_LIMIT=512Mi \
  -l app.kubernetes.io/part-of=eshop
```

For each database the template creates a Deployment, a Service, a PVC and a Secret with the same
name (`catalog-db`, `order-db`). The Secret holds the keys `database-name`, `database-user` and
`database-password`.

Check:

```bash
oc get pods,svc,pvc,secret -l app.kubernetes.io/part-of=eshop
oc rsh deploy/catalog-db psql -U catalog -d catalog -c 'select 1'
```

If the template is missing or rejected by the quota, deploy PostgreSQL with a hand-written
Deployment on the `registry.redhat.io/rhel9/postgresql-16` image, using the same Secret keys.

## Step 3 — Build the two services and their REST contract

Keep the services small: a few routes and one table each. If you start from a monolith, move the
catalog code and the order code into the two folders and remove every cross access to data.

### REST contract

| Service | Method and path | Purpose | Response |
| --- | --- | --- | --- |
| catalog-service | `GET /health` | readiness probe | 200 if the DB answers, 503 otherwise |
| catalog-service | `GET /products` | list products | 200, JSON array |
| catalog-service | `GET /products/{id}` | product detail | 200 or 404 |
| catalog-service | `POST /products` | create a product | 201 with the product created |
| catalog-service | `PUT /products/{id}` | replace a product (used to change a price) | 200 or 404 |
| order-service | `GET /health` | readiness probe | 200 if the DB answers, 503 otherwise |
| order-service | `POST /orders` | place an order | 201; 400 if invalid; 422 if a product does not exist; 503 if the catalog does not answer |
| order-service | `GET /orders/{id}` | order detail | 200 or 404 |

The same contract is in [`docs/openapi.yaml`](docs/openapi.yaml). Keep it: in Lab 2 it becomes
the baseline for versioning.

### Data schema

```sql
-- catalog-db
CREATE TABLE product (
  id          SERIAL PRIMARY KEY,
  name        TEXT NOT NULL,
  price_cents INTEGER NOT NULL CHECK (price_cents > 0),
  published   BOOLEAN NOT NULL DEFAULT false
);

-- order-db
CREATE TABLE customer_order (
  id          SERIAL PRIMARY KEY,
  customer_id TEXT NOT NULL,                  -- id only: the customer lives in another context
  status      TEXT NOT NULL DEFAULT 'PLACED',
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE order_line (
  order_id         INTEGER NOT NULL REFERENCES customer_order(id),
  product_id       INTEGER NOT NULL,          -- identity reference, no foreign key to the catalog
  product_name     TEXT NOT NULL,             -- copied at purchase time
  unit_price_cents INTEGER NOT NULL,          -- copied at purchase time
  quantity         INTEGER NOT NULL CHECK (quantity > 0)
);
```

An order and its lines are one aggregate: they are written in the same transaction. No table in
order-db points to catalog-db.

### Implementation rules

- **Configuration only from environment variables:** host, port, database name, user, password and
  `CATALOG_URL`. No address written in the code.
- **Port 8080**, listening on `0.0.0.0`.
- **Tables created at startup** (`CREATE TABLE IF NOT EXISTS`) or by an SQL script: in this lab the
  simplest option is fine.
- **Timeout on the catalog call:** 2 seconds. If it expires or the catalog is unreachable,
  `POST /orders` answers 503 with a clear message. Never wait indefinitely.
- **Logs on stdout**, one line per request (method, path, status, duration).

Flow of `POST /orders` in order-service (see [`order-service/server.js`](order-service/server.js)):

1. validate the request body (at least one line, quantity > 0);
2. for each line call `GET {CATALOG_URL}/products/{id}` with the timeout;
3. if a product answers 404 → 422; if the catalog does not answer → 503;
4. in one transaction on order-db insert the order and its lines, **copying** name and price;
5. answer 201 with the order and the computed total.

Before deploying, try it locally with two PostgreSQL containers (Podman or Docker) and the same
environment variables (`DB_HOST`, `DB_PORT`, `DB_DATABASE_NAME`, `DB_DATABASE_USER`,
`DB_DATABASE_PASSWORD`, plus `CATALOG_URL` for order-service), then `npm install && npm start`.

## Step 4 — Deploy on OpenShift and configure

The quickest way is an **S2I** build straight from the Git repository: OpenShift fetches the code,
builds the image and creates the Deployment and the Service. The repository must be public; a
private one needs a source secret first.

### catalog-service

```bash
# Build and deploy from the repository (folder lab-1-decomposition/catalog-service)
oc new-app nodejs:22-ubi9~$REPO \
  --context-dir=$LAB/catalog-service \
  --name=catalog-service \
  -l app.kubernetes.io/part-of=eshop

oc logs -f bc/catalog-service          # follow the build until the image is pushed

# Credentials of catalog-db ONLY, plus host and port
oc set env deploy/catalog-service --from=secret/catalog-db --prefix=DB_
oc set env deploy/catalog-service DB_HOST=catalog-db DB_PORT=5432

# Small resources, within the quota
oc set resources deploy/catalog-service \
  --requests=cpu=50m,memory=128Mi --limits=cpu=250m,memory=256Mi

# Probes: readiness on the health endpoint, liveness on the port
oc set probe deploy/catalog-service --readiness \
  --get-url=http://:8080/health --initial-delay-seconds=5
oc set probe deploy/catalog-service --liveness --open-tcp=8080
```

The `DB_` prefix produces `DB_DATABASE_NAME`, `DB_DATABASE_USER` and `DB_DATABASE_PASSWORD`: the
code reads exactly these names.

### order-service

Same commands with a different name, folder and database. In addition, set the catalog address:

```bash
oc new-app nodejs:22-ubi9~$REPO \
  --context-dir=$LAB/order-service \
  --name=order-service \
  -l app.kubernetes.io/part-of=eshop

oc set env deploy/order-service --from=secret/order-db --prefix=DB_
oc set env deploy/order-service DB_HOST=order-db DB_PORT=5432 \
  CATALOG_URL=http://catalog-service:8080
# then resources and probes as for catalog-service
```

`CATALOG_URL` uses the **Service name**, not an IP: that is server-side service discovery. The full
name would be `catalog-service.<namespace>.svc.cluster.local`, but within the same namespace the
short one is enough.

The folder [`k8s/`](k8s/) holds the same objects as declarative manifests (ImageStream, BuildConfig,
Deployment, Service, Route) for comparison and for Step 7.

### Check

```bash
oc get deploy,pods,svc -l app.kubernetes.io/part-of=eshop
```

You should see four available Deployments (2 services + 2 databases), each with 1/1 Pod ready. In
the console, the *Topology* view groups them under the `eshop` application.

## Step 5 — Exposure and end-to-end test

Expose **only order-service** with a Route: it is the customer's entry point. catalog-service stays
internal to the cluster, as it would behind an API gateway. Products are loaded from inside.

```bash
oc create route edge order-service --service=order-service --insecure-policy=Redirect
ORDERS=https://$(oc get route order-service -o jsonpath='{.spec.host}')
```

**1. Load two products into the catalog** (from inside, through an order-service Pod):

```bash
oc rsh deploy/order-service curl -s -X POST http://catalog-service:8080/products \
  -H 'Content-Type: application/json' \
  -d '{"name":"Mug","price_cents":1200,"published":true}'

oc rsh deploy/order-service curl -s -X POST http://catalog-service:8080/products \
  -H 'Content-Type: application/json' \
  -d '{"name":"T-shirt","price_cents":2500,"published":true}'
```

If the image has no `curl`, run `oc port-forward svc/catalog-service 8080:8080` in a second
terminal and call `http://localhost:8080`.

**2. Place an order from outside:**

```bash
curl -s -X POST $ORDERS/orders -H 'Content-Type: application/json' \
  -d '{"customer_id":"C-001","lines":[{"product_id":1,"quantity":2},{"product_id":2,"quantity":1}]}'
```

Expected: 201, with name and price of each product copied into the lines and a total of 4900.

**3. Error cases:**

| Test | Expected response |
| --- | --- |
| non-existent `product_id` (e.g. 99) | 422 with a message naming the product |
| line with `quantity` 0 | 400 |
| `GET $ORDERS/orders/<id>` of the order created | 200 with the saved lines |

**4. The copied price:** change the price of the Mug in the catalog, then read the order again. The
price in the order **must not change**.

```bash
oc rsh deploy/order-service curl -s -X PUT http://catalog-service:8080/products/1 \
  -H 'Content-Type: application/json' -d '{"name":"Mug","price_cents":1500,"published":true}'
curl -s $ORDERS/orders/1
```

## Step 6 — Verify the concepts

Each test ties a command to a concept. Write in `docs/verifications.md` the command, what you
observed and what it proves.

### 6.1 Service discovery: stable name, dynamic endpoints

```bash
oc get endpointslices -l kubernetes.io/service-name=catalog-service        # 1 address
oc scale deploy/catalog-service --replicas=3
oc get endpointslices -l kubernetes.io/service-name=catalog-service -w     # 3 addresses, as the Pods become ready
oc get pods -l deployment=catalog-service -o wide                          # compare the IPs with the endpoints
```

The Endpoints API is deprecated in recent Kubernetes versions; EndpointSlices replace it. What to
observe: order-service keeps calling `catalog-service` without knowing anything about the IPs.

### 6.2 Pods that are not ready leave the load balancer

Make `/health` answer 503 in one Pod only (for example by stopping catalog-db for a few seconds).
With the `-w` command above you see the address leave and come back. Then scale back to 1 replica.

### 6.3 Independent release

```bash
oc rollout restart deploy/catalog-service
oc rollout status deploy/catalog-service
oc get pods -l deployment=order-service           # Pod age unchanged
```

Then make a visible change to the catalog (for example an extra field in the response), commit,
push and run `oc start-build catalog-service`. order-service is neither rebuilt nor restarted. If it
had to be, the boundary is wrong.

### 6.4 No shared database

```bash
oc set env deploy/order-service --list | grep DB_
```

Only the credentials and host of `order-db` must appear. No reference to `catalog-db`.

### 6.5 Partial failure

```bash
oc scale deploy/catalog-service --replicas=0
curl -s -o /dev/null -w '%{http_code} %{time_total}s\n' -X POST $ORDERS/orders \
  -H 'Content-Type: application/json' -d '{"customer_id":"C-001","lines":[{"product_id":1,"quantity":1}]}'
curl -s $ORDERS/orders/1                           # reading existing orders still works
oc scale deploy/catalog-service --replicas=1
```

Expected: 503 within about 2 seconds, not a 30-second wait or more. Orders already saved stay
readable, because order-service owns its data. This is the starting point for the retries and
circuit breakers of [Lab 3](../lab-3-resilience-observability/).

### 6.6 Check question

Answer in two lines in `docs/verifications.md`: *can I release catalog-service alone, without
warning whoever maintains order-service?* If the answer is no, say what prevents it.

## Step 7 — Save to Git and wrap up

The Sandbox expires: whatever is not in Git is lost. The goal is to be able to recreate everything
in an empty namespace with `oc apply -f k8s/`.

1. **Export the resources**, one file per kind (or compare with the reference files in [`k8s/`](k8s/)):

```bash
for r in deployment service route buildconfig imagestream pvc; do
  oc get $r -l app.kubernetes.io/part-of=eshop -o yaml > k8s/$r.yaml
done
```

2. **Clean the files:** remove `status`, `metadata.uid`, `resourceVersion`, `creationTimestamp`,
   `managedFields` and generated annotations. Alternatively use the `kubectl neat` plugin.
3. **Secrets out of Git.** Commit only an example with placeholder values
   ([`k8s/secrets.example.yaml`](k8s/secrets.example.yaml)); the real Secrets stay in the cluster.
4. **Recreation test (optional but recommended):** delete order-service with
   `oc delete all -l app=order-service` and recreate it from `k8s/`. It must work again with the
   same commands as Step 5.
5. **Commit and tag:** `git tag lab-1` and push.

### Deliverables

- the repository link, with the tag `lab-1`;
- `docs/c4-container.png` and `docs/boundaries.md` (Step 1);
- the minimal OpenAPI contract (Step 3);
- `docs/verifications.md` with the outcome of the Step 6 tests;
- a screenshot of the *Topology* view with the four components.

## Completion checklist

- [ ] C4 diagram and boundary notes in `docs/`
- [ ] Two separate databases, each with its own Secret and PVC
- [ ] catalog-service and order-service running, with readiness probe and resources set
- [ ] order-service reaches the catalog through `CATALOG_URL=http://catalog-service:8080`
- [ ] Only order-service exposed through a Route
- [ ] Order created from outside with copied price; the price does not change when the catalog changes
- [ ] Verifications 6.1–6.6 run and recorded
- [ ] Clean manifests in `k8s/`, Secrets excluded, tag `lab-1` in Git

## Common problems

| Symptom | Likely cause | What to do |
| --- | --- | --- |
| Pod `Pending` or `exceeded quota` error | requests too high or too many replicas | `oc describe quota`; reduce requests and replicas |
| S2I build fails | `package.json` not in the given folder, or no `start` script | check `--context-dir` and `oc logs bc/<name>` |
| Pod `Running` but `0/1 Ready` | `/health` answers an error, often DB credentials | `oc logs deploy/<name>`; `oc set env deploy/<name> --list` |
| order-service: `ENOTFOUND catalog-service` | Service name different from the one in `CATALOG_URL` | `oc get svc` and fix the variable |
| Route answers 503 "Application is not available" | no ready Pod behind the Service | check readiness and `oc get endpointslices -l kubernetes.io/service-name=order-service` |
| Everything is stopped when you come back | the Sandbox stopped the Pods after 12 hours | `oc scale deploy -l app.kubernetes.io/part-of=eshop --replicas=1` |
| Login expired | the CLI token has a limited lifetime | repeat *Copy login command* |

## References

- [Developer Sandbox](https://developers.redhat.com/developer-sandbox) and its [FAQ](https://developers.redhat.com/developer-sandbox/FAQ)
- [Deploy a database to OpenShift](https://developers.redhat.com/learning/learn:openshift:how-deploy-full-stack-javascript-applications-openshift/resource/resources:deploy-database-openshift) — learning path with PostgreSQL on the Sandbox
- Kubernetes: [Service](https://kubernetes.io/docs/concepts/services-networking/service/), [EndpointSlices](https://kubernetes.io/docs/concepts/services-networking/endpoint-slices/), [DNS for Services and Pods](https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/), [Liveness, Readiness and Startup Probes](https://kubernetes.io/docs/concepts/configuration/liveness-readiness-startup-probes/)
- [C4 model](https://c4model.com/) and [draw.io](https://app.diagrams.net/)
- microservices.io: [Database per service](https://microservices.io/patterns/data/database-per-service.html) and [Server-side service discovery](https://microservices.io/patterns/server-side-discovery.html)

---

Next: [Lab 2 — Asynchronous events with Kafka](../lab-2-async-events/)
