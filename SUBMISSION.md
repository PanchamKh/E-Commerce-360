# Final Project — Submission Answers

**Issue title:** `[PROJECT][SDA-2] <Team Name> — Final Project`

Fill in the team name, member names and IDs, and the repository URL before
submitting. Everything else below is ready to paste into the matching field.

---

## Section
`SDA-2`

## Team Name
_(e.g. DataFlow Analytics — replace and use the same name in the issue title)_

## Team Members (Full Name + Student ID)

```markdown
1. Pancham Khullar — 341211
2. _(name)_ — _(ID)_
3. _(name)_ — _(ID)_
```

## GitHub Repository Link
_(public repo containing this project)_

## Industry / Domain
E-commerce — online retail marketplace operations

---

## Problem Statement (25%)

```markdown
**Business problem.** An e-commerce platform loses revenue in four places at
once, and each loss is owned by a different system. A shopper abandons a cart
(clickstream). An order is taken that cannot be shipped (order management). A
payment fails or is fraudulent (payments). A SKU runs out while demand is still
arriving (inventory). Each system can see its own failure, but none can see the
*cause* of another's — and in practice no one sees the total until the next
morning's report, by which point every decision that could have prevented the
loss has already expired.

Our team built these four systems separately in Assignments 1–3. Separately,
each produces a defensible dashboard and a wrong conclusion. The inventory
dashboard reports a stock-out; it cannot show the revenue that stock-out
cancelled. The payments dashboard reports a 25% failure rate; it cannot say
whether that is fraud retries or a gateway outage, which demand opposite
responses. The final project merges all four into one streaming platform so
that the loss can be attributed to the system responsible for it, in real time.

**Why streaming rather than batch.** Every decision in this problem has a
window measured in minutes:

- Stock must be decremented and the SKU delisted the moment it hits zero.
  Overnight, the platform has already accepted orders it cannot ship, and the
  only remaining decision is how many customers to cancel on.
- A fraud pattern — three failures then a success on one order — must be held
  before settlement. After settlement it is a chargeback, not a decision.
- A cart abandonment is detected by the ABSENCE of an event. Batch processing
  cannot express this at all: there is no record to join on, only a gap where
  a purchase should have been. A streaming watcher ages idle carts out and
  triggers retargeting while the shopper is still in the market.
- During a festive sale, order volume rises several times over baseline within
  minutes. The ingestion layer has to absorb that spike without data loss,
  which is what a partitioned, replicated log is for.

**Impact if not solved in real time.** In one recorded three-minute run of our
platform (sample_data/, reproducible with the command in the README), 119
orders were created and ₹64,582 of revenue was lost — 25.1% of GMV. Split by
root cause: ₹24,471 to returns, ₹23,014 to payment failures, ₹17,096 to
stock-out cancellations. A further ₹2,50,803 sat in abandoned carts, more than
the ₹1,92,653 actually retained. A daily report would have shown one aggregate
"cancellation rate" and none of this attribution — and would have arrived
after every one of those decisions had expired.

**Our solution.** Four independent systems — Website Clickstream, Order
Management, Payment Management, Inventory Management — publishing to 14 Kafka
topics and reacting to each other's events, with a cross-system consumer that
joins them into one order journey document and a 33-chart Atlas Charts
dashboard that attributes every rupee of lost revenue to the system responsible
for it.
```

## Identification of Data (20%)

```markdown
All four systems share one master catalog (`shared/catalog.py`): 18 products
across 6 categories, 4 warehouses, 180 customers across 10 Indian cities. The
SKU a shopper clicks is the same SKU the OMS orders, the PMS charges for and
the IMS depletes — this shared reference data is what makes the four systems
one business rather than four simulators.

**Source 1 — Website Clickstream System (CLK)**
Fields: event_id, event_type, event_timestamp (ISO-8601 IST), minute_ist,
session_id, customer_id, device, channel, city, customer_segment, page_type,
page_ref, search_term, results_count, click_target, product_id, sku,
product_name, category, quantity, unit_price, cart_value, order_value,
payment_method.
Volume: ~2,900 events per 3 minutes — 61% of all platform traffic, the highest
volume and the smallest payload. One order generates dozens of page views.

**Source 2 — Order Management System (OMS)**
Fields: order_id, session_id, customer_id, customer_segment, city, device,
channel, sku, product_name, category, quantity, unit_price, subtotal, discount,
shipping_charges, order_value, payment_method, warehouse_id, order_status,
fulfillment_status, tracking_id, courier, cancellation_reason,
cancellation_cause, return_reason, refund_amount.
Volume: ~800 events per 3 minutes; 8–10 events per order across its lifecycle.

**Source 3 — Payment Management System (PMS)**
Fields: transaction_id, payment_id, order_id, customer_id, amount, currency,
payment_method, payment_gateway, transaction_status, attempt_number,
failure_reason, device, city, customer_segment, is_fraud (ground truth),
fraud_type (ground truth).
Volume: ~265 events per 3 minutes — lowest volume, highest value per message.
1.2 attempts per order on average, rising to 3–4 on fraud patterns.

**Source 4 — Inventory Management System (IMS)**
Fields: sku, product_id, product_name, category, warehouse_id, quantity_change,
stock_on_hand, stock_reserved, stock_on_order, available, unit_cost, po_id,
supplier_id, supplier_name, quantity_ordered, quantity_received, fill_rate_pct,
transfer_id, reason, alert_type, severity.
Volume: ~770 events per 3 minutes across 72 live SKU/warehouse positions.

**Volume justification.** At 3 sessions/second the platform produced 4,741
events in 3 minutes (~26 events/sec), with each message 0.4–1.5 KB. Scaled to a
mid-sized Indian platform at 50,000 orders/day, the same event ratios give
roughly 2 million events/day, or 0.6–2 GB/day — comfortably within a
three-broker Kafka cluster, and the reason we partition the clickstream topics
more heavily than the payment topics.

**Kafka topics (14), grouped by owning system:**

CLK — page-views-topic (every page landing) · search-queries-topic (terms and
result counts, including zero-result searches) · click-events-topic (product,
banner and filter clicks) · cart-events-topic (add and remove) ·
checkout-requests-topic (**checkout intent — the handoff to the OMS**)

OMS — orders-topic (ORDER_PLACED, ORDER_CONFIRMED) · fulfillment-topic
(PROCESSING → PACKED → SHIPPED → DELIVERED) · order-cancellations-topic
(cancellations with root cause, and returns) · payment-requests-topic
(**the OMS asking the PMS to collect**)

PMS — payment-transactions-topic (every individual attempt with ground-truth
fraud tag) · payment-results-topic (**final outcome back to the OMS**)

IMS — stock-movements-topic (every physical movement with stock level after) ·
purchase-orders-topic (PO_RAISED → APPROVED → GOODS_RECEIVED → CLOSED with
supplier fill rate) · stock-alerts-topic (**LOW_STOCK, OUT_OF_STOCK,
BACK_IN_STOCK — the IMS telling the OMS what it can and cannot sell**)

The four bold topics are the integration points. Messages are keyed on
order_id, sku:warehouse or session_id so that all events for one entity land in
the same partition and are consumed in order.
```

## Technical Working — Architecture (15%)

```markdown
**Producers (4 services).**
- `clickstream/producer.py` — Web/App Tracker. Simulates live shopper sessions
  as a state machine (land → browse → search → click → cart → checkout) spread
  over real elapsed time. Publishes the 5 CLK topics.
- `oms/producer.py` — OMS Order Service, the orchestrator. CONSUMES
  checkout-requests (CLK), payment-results (PMS) and stock-alerts (IMS);
  PUBLISHES orders, fulfillment, order-cancellations and payment-requests.
- `pms/producer.py` — Payment Gateway Service. CONSUMES payment-requests;
  PUBLISHES payment-transactions and payment-results. Models retries, declines
  and four fraud scenarios with ground-truth labels.
- `ims/producer.py` — WMS / Warehouse Service. CONSUMES orders, fulfillment and
  cancellations; PUBLISHES stock-movements, purchase-orders and stock-alerts.
  Holds the only live stock ledger, raises purchase orders at the reorder
  point, and generates independent activity (cycle counts, damage write-offs,
  inter-warehouse transfers).

**Consumers (5 services, one Kafka group each).**
- `clickstream/consumer.py` (`clk-session-funnel-group`) — session funnel,
  zero-result search tracking, and a background watcher that ages idle carts
  into "abandoned".
- `oms/consumer.py` (`oms-order-fulfillment-group`) — order state and returns,
  recording the CAUSE of every cancellation.
- `pms/consumer.py` (`pms-transaction-fraud-group`) — transactions, per-order
  rollup, and four detection rules that never read the ground-truth tag.
- `ims/consumer.py` (`ims-stock-position-group`) — stock positions including
  the lowest level ever reached, movements, and PO fill rates.
- `analytics/consumer.py` (`analytics-360-group`) — subscribes to **all 14
  topics**; writes `pipeline_events` (every message) and `journey_orders` (one
  document per order joining all four systems, with latencies computed across
  system boundaries and revenue attributed to a root cause).

**Storage.** MongoDB Atlas, database `ecommerce360`, 13 collections. Raw
(`pipeline_events`) and derived (`journey_orders`) are kept separate so a new
metric can be backfilled without re-running the producers. All five consumers
write to a shared `alerts` collection tagged by source system.

**Dashboard.** MongoDB Atlas Charts — `charts/Ecommerce360_Dashboard.charts`,
33 charts over 10 data sources, auto-refresh 1 minute, generated
programmatically by `charts/build_charts.py`.

**Infrastructure.** `docker-compose.yml` (Kafka, ZooKeeper, MongoDB),
`create_topics.sh`/`.bat` for all 14 topics, `run_all.py` to launch all nine
services, `requirements.txt`, `.env.example`.

**Engineering decisions we can defend:**
- The message bus and the store are abstractions with a real and an in-memory
  implementation behind one interface, so `python run_all.py --offline` runs
  the entire platform with no Docker and no database. That is how the pipeline
  was tested, and it caught two real bugs before the demo.
- One consumer group per job, because group members share partitions — five
  consumers with different subscriptions in one group would each see only a
  slice of their own stream.
- A bad message increments an error counter and the service keeps consuming.
  One malformed event must not take a system down.
- Latencies are computed at write time and stored as numbers, so the dashboard
  needs no date parsing.
```

## Charts & Visualisations (30%)

```markdown
**Tool:** MongoDB Atlas Charts · 33 charts · 10 data sources · auto-refresh 1
minute · 4 dashboard-level filters (Category, City, Warehouse, Device) that
slice every chart at once.

Charts are ordered on first principles: a derived rate never appears before the
base data it is computed from, and every chart description states BASE or
DERIVED with its formula.

**Headline KPIs.** Events Ingested (4,741 across all 14 topics) · Shopper
Sessions (543) · Orders Created (119) · Checkout Conversion 21.9% (derived from
the two before it) · Revenue Retained ₹1,92,653 · Revenue Lost ₹64,582 · Order
Loss Rate % · Open Alerts.

**1 · Pipeline health — is the stream actually live?**
1.1 Heatmap, events per minute by system — proves all four systems are live at
once and shows their relative volume. → decision: which topic to partition first.
1.2 Bar, message volume by Kafka topic — a silent topic means a producer is down.
1.3 Combo, orders vs retained revenue per minute — steady bars with a falling
line means volume is fine and something downstream is eating the revenue.

**2 · The funnel (CLK)**
2.1 Bar, session funnel by furthest stage reached — the gap between "added to
cart" and "checked out" is recoverable demand.
2.2 Data table, conversion by device (derived %) — a mobile checkout rate far
below desktop is a UX defect, not a traffic problem.
2.3 Bar, checkout conversion by acquisition channel — where ad spend should go.
2.4 Donut, cart outcomes — "abandoned" is assigned by the absence of an event.
2.5 Bar, abandoned cart value by category (₹2,50,803 total) — the retargeting
list, ranked by money.
2.6 Bar, zero-result searches — demand with no catalog cover; invisible to every
system downstream of checkout.

**3 · Revenue outcome — the cross-system view**
3.1 Donut, where every order ends up — only "kept" if all four systems succeeded.
3.2 Bar, **lost revenue by root cause** — returns ₹24,471, payment failure
₹23,014, stock-out ₹17,096. Each points at a different team. This chart exists
only because the systems are joined.
3.3 Data table, category scorecard — orders, value, retained, lost, loss rate %.
3.4 Bar, order loss rate by warehouse (derived) — normalises for warehouse size.

**4 · Payments (PMS)**
4.1 Stacked bar, attempts by method and outcome.
4.2 Bar, failure rate by gateway (derived) — one gateway spiking while others
hold steady is an outage, and the fix is to reroute, not to block customers.
4.3 Bar, failure reasons — separates customer-side from infrastructure causes.
4.4 Data table, **fraud detection vs ground truth** — the producer labels
synthetic fraud, the rules never see the label, so this measures RECALL per
fraud type and the false-positive rate. In the recorded run: 6 of 6 caught, 0
false positives. Counting alerts alone would measure neither.

**5 · Inventory (IMS)**
5.1 Data table, stock ledger by SKU — units out and in (base), then on-hand and
the LOWEST available level ever reached (derived). Current stock alone lies: a
SKU at 30 units today may have hit zero an hour ago and cost you orders.
5.2 Bar, stock-out exposure % by SKU — the reorder list, ranked.
5.3 Bar, capital tied up in stock by category — working-capital view.
5.4 Data table, supplier reliability — a supplier at 80% fill rate silently
makes every reorder 20% smaller, which is how a stock-out survives a reorder.

**6 · Latency across system boundaries**
6.1 Data table, **order latency decomposed by warehouse** — Browse→Checkout
(CLK) 4.0s, Checkout→Paid (OMS→PMS) 1.1s, Paid→Shipped (PMS→IMS→OMS) 2.5s,
Shipped→Delivered (OMS) 1.0s. Over half the post-checkout time is picking and
packing — that names the shift to staff. No single system can produce this.
6.2 Bar, delivery time by city.

**7 · Unified alerts**
7.1 Stacked bar, alerts by type and severity — every rule from every system.
7.2 Heatmap, alert pressure per minute by system — a burst in one row localises
the incident and tells the on-call which system to open first.

_(Attach: full dashboard screenshot, plus the terminal showing all nine services
running and the cross-system stock-out cancellation firing.)_
```

## Discussion — likely Q&A (10%)

```markdown
**"Why did you use separate consumer groups instead of one?"** Members of a
Kafka group share the partitions of the topics they subscribe to. Five
consumers with different subscriptions and different jobs in one group would
each receive only part of its own stream. One group per job is the only correct
arrangement.

**"What happens if a service dies mid-demo?"** We demonstrate it: kill the
payment gateway and orders pile up at PLACED while every other system keeps
running. Restart it and the backlog drains from the committed offset with
nothing lost. That is the decoupling argument shown rather than claimed.

**"Is any of this real data?"** No — it is simulated, and we say so. What is
real is the pipeline: the topics, the partitioning, the consumer groups, the
offset behaviour and the cross-system joins are exactly what a production
platform would use. The fraud rate (6%) and the simulated clock (`--speed 8`)
are deliberately unrealistic to make the behaviour demonstrable in a
ten-minute window; ratios carry meaning, absolute durations do not.

**"How do you know your fraud rules work?"** Ground truth is generated by the
producer and never read by the consumer's rules, so chart 4.4 measures recall
and false positives rather than counting alerts. That separation was a design
decision, not an afterthought.

**"What would you do next?"** Three things. Replace the in-consumer rules with
Kafka Streams so windowed aggregations are handled by the framework rather than
by our own state. Add a schema registry — right now a producer changing a field
name breaks a consumer silently. And move the alerting off the dashboard into a
notification service, because a manager should not have to be looking at a
screen for a stock-out to reach them.
```

## Submission Checklist

- [ ] Section selected and issue title updated to `[PROJECT][SDA-2]`
- [ ] Team name and all member names + IDs filled in
- [ ] Repository is public and contains README, source, sample data
- [ ] Dashboard screenshot attached
- [ ] Terminal screenshot attached (all nine services running)
- [ ] `MONGO_URI` is read from the environment — no credential committed
