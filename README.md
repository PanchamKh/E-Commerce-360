# E-Commerce 360° — End-to-End Streaming Analytics Platform

**Streaming Data Analytics · Final Project · Section SDA-2**

Four e-commerce systems, built separately, now running as **one live platform**:

| | System | Role in the platform |
|---|---|---|
| **CLK** | Website Clickstream | Shopper traffic → checkout intent |
| **OMS** | Order Management | Orchestrates the order end to end |
| **PMS** | Payment Management | Collects the money, flags fraud |
| **IMS** | Inventory Management | Moves the stock — and can stop an order |

**9 services · 14 Kafka topics · 13 MongoDB collections · 33 dashboard charts**

---

## Why this is one platform, not four demos

The systems share a single master catalog (`shared/catalog.py`), so the SKU a
shopper clicks is the same SKU the OMS orders, the PMS charges for and the IMS
depletes. They then drive each other over Kafka:

```
  CLK ──checkout-requests-topic──▶ OMS          shopper intent becomes an order
  OMS ──payment-requests-topic───▶ PMS          order asks for money
  PMS ──payment-results-topic────▶ OMS          money decides confirm vs cancel
  OMS ──orders / fulfillment─────▶ IMS          confirmed order reserves & issues stock
  IMS ──stock-alerts-topic───────▶ OMS          out of stock → delist SKU, cancel orders
  IMS ──BACK_IN_STOCK────────────▶ OMS          replenished → relist SKU
```

That last pair is the point of the whole project. When the IMS declares a SKU
unsellable, the OMS **delists it and cancels the orders it cannot ship**; when a
purchase order lands, the OMS relists it. No single system can make that
decision, and no batch report can make it in time.

Full diagram and message contracts: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
Findings from a live run: [`docs/ANALYSIS.md`](docs/ANALYSIS.md).

---

## Repository layout

```
shared/          config (all 14 topics, thresholds) · catalog (master data)
                 bus (Kafka | in-memory) · store (MongoDB | in-memory)
                 service (poll loop, scheduler, error isolation)
clickstream/     producer.py  consumer.py        CLK
oms/             producer.py  consumer.py        OMS
pms/             producer.py  consumer.py        PMS
ims/             producer.py  consumer.py        IMS
analytics/       consumer.py                     cross-system 360° consumer
charts/          build_charts.py → Ecommerce360_Dashboard.charts
sample_data/     13 collections exported from a real run (JSON Lines)
docs/            ARCHITECTURE.md · ANALYSIS.md · SUBMISSION.md
run_all.py       runs all 9 services (live or offline)
docker-compose.yml · create_topics.sh / .bat · requirements.txt · .env.example
```

---

## Quick start

### Option A — offline (no Docker, no database, 30 seconds)

Proves the pipeline logic end to end using an in-process bus and an in-memory
store. Useful for marking, and for regenerating `sample_data/`.

```bash
pip install -r requirements.txt
python run_all.py --offline --duration 120 --rate 3 --speed 8 --export sample_data
```

### Option B — the real thing (Kafka + MongoDB Atlas)

**1. Start Kafka**
```bash
docker compose up -d
docker ps                      # confirm e360-kafka is healthy
./create_topics.sh e360-kafka  # Linux/macOS
create_topics.bat e360-kafka   # Windows
```

**2. Python environment**
```bash
python -m venv venv
venv\Scripts\activate          # Windows
source venv/bin/activate       # macOS/Linux
pip install -r requirements.txt
```

**3. Point at MongoDB Atlas** — Atlas Charts cannot read a local MongoDB, so use
an Atlas cluster. In Atlas: **Network Access → 0.0.0.0/0**, then
**Connect → Drivers** for the URI.

```bash
set MONGO_URI=mongodb+srv://user:pass@cluster0.xxxxx.mongodb.net/   # Windows
export MONGO_URI="mongodb+srv://user:pass@cluster0.xxxxx.mongodb.net/"
```

**4. Run the platform**
```bash
python run_all.py --rate 3 --speed 8
```

Run it for **8–10 minutes** before screenshotting: the per-minute charts need
several buckets, and the stock-out → cancellation loop needs time for a hot SKU
to deplete faster than its purchase order can replenish it.

### Option C — nine terminals (for the live demo)

Shows that the services are genuinely independent. Start consumers first.

```bash
python -m analytics.consumer        # terminal 1
python -m clickstream.consumer      # 2
python -m oms.consumer              # 3
python -m pms.consumer              # 4
python -m ims.consumer              # 5
python -m oms.producer   --speed 8  # 6
python -m pms.producer   --speed 8  # 7
python -m ims.producer   --speed 8  # 8
python -m clickstream.producer --rate 3 --speed 8   # 9  (start last)
```

Kill terminal 7 (the payment gateway) mid-demo: orders pile up at PLACED, the
other systems keep running, and when you restart it the backlog drains from the
committed offset. That is the decoupling argument, demonstrated rather than
claimed.

### Verify from the Kafka CLI
```bash
docker exec -it e360-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic checkout-requests-topic --from-beginning
```

---

## The dashboard

```bash
python charts/build_charts.py      # regenerates the .charts file
```

Import `charts/Ecommerce360_Dashboard.charts` in **Atlas Charts → Dashboards →
Add Dashboard ▾ → Import Dashboard**, then map the 10 data sources to the
`ecommerce360` database. Turn on **⋯ → Auto-refresh → 1 minute**.

33 charts in 7 numbered sections, ordered on first principles — a derived rate
never appears before the base data it is computed from:

| Section | Contents |
|---|---|
| KPIs | events, sessions, orders, conversion %, revenue retained / lost, loss rate, open alerts |
| 1 · Pipeline health | throughput per minute per system, volume per topic, orders vs revenue per minute |
| 2 · Funnel (CLK) | funnel stages, conversion by device and channel, cart outcomes, abandoned value, search gaps |
| 3 · Revenue outcome | where every order ends up, **lost revenue by root cause**, category scorecard, loss by warehouse |
| 4 · Payments (PMS) | attempts by method, failure rate by gateway, failure reasons, **fraud detection vs ground truth** |
| 5 · Inventory (IMS) | stock ledger, stock-out exposure, capital tied up, supplier reliability |
| 6 · Latency | **order latency decomposed across all four systems**, delivery time by city |
| 7 · Alerts | alerts by type and severity, alert pressure per minute per system |

---

## Command reference

| Flag | Applies to | Meaning |
|---|---|---|
| `--rate N` | clickstream, run_all | new shopper sessions per tick |
| `--interval S` | clickstream, run_all | seconds between ticks |
| `--speed N` | all services | lifecycle multiplier; 8 = journeys complete 8× faster |
| `--offline` | run_all | in-memory bus and store, no Docker |
| `--duration S` | run_all | stop automatically after S seconds |
| `--export DIR` | run_all (offline) | dump every collection to JSON Lines |
| `--bus kafka\|memory` | every service | which transport to use |
| `--store mongodb\|memory` | every consumer | where to persist |
| `--from-beginning` | every consumer | replay topics from offset 0 |
| `--verbose` | every consumer | print a line per persisted message |

`--speed` compresses simulated time. Latency figures on the dashboard are in
**seconds of simulated time**; their *ratios* are what carry meaning.

---

## Design decisions worth defending in Q&A

**Message keys.** Order-lifecycle events are keyed on `order_id`, stock events on
`sku:warehouse`, clickstream on `session_id`. With `hash(key) % partitions`, all
events for one entity land in one partition and stay ordered — `ORDER_PLACED`
can never be processed after `ORDER_SHIPPED`.

**One consumer group per job.** Members of a group *share* partitions. Five
consumers with different subscriptions therefore need five group IDs, or each
would see only a slice of its own stream.

**Raw + derived collections.** `pipeline_events` keeps every message untouched;
`journey_orders` is the derived join. The raw log means a new metric can be
backfilled without re-running the producers.

**Durations computed at write time.** Latencies are stored as plain numbers, so
the dashboard needs no date parsing — fewer moving parts in the layer most
likely to break during a demo.

**Failures are isolated.** A bad message increments an error counter and the
service keeps consuming. One malformed event must not take a system down.

**Ground truth is kept separate from detection.** The PMS producer tags synthetic
fraud; the consumer's rules never read that tag. That is what makes chart 4.4 a
measurement of recall rather than a count of alerts.

---

## Security note

Never commit a MongoDB URI. `MONGO_URI` is read from the environment and `.env`
is git-ignored; `.env.example` shows the shape without the secret.
