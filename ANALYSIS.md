# Analysis — findings from a recorded run

Every number below comes from one continuous run of the platform, exported to
`sample_data/`. Reproduce it with:

```bash
python run_all.py --offline --duration 180 --rate 3 --speed 8 --export sample_data
```

**Run profile:** ~3 minutes wall clock · `--speed 8` · 3 new sessions/second
· 4,741 events across all 14 topics · 543 sessions · 119 orders.

Event volume by system — CLK 2,902 · OMS 802 · IMS 772 · PMS 265. The shape is
right: one order generates dozens of page views but only a few transactions,
which is exactly why the systems need different partition counts and why
clickstream cannot be run through the same consumer as payments.

---

## Finding 1 — Revenue leakage has three different owners

| Outcome | Orders | Revenue |
|---|---|---|
| Delivered (kept) | 93 | ₹1,92,653 |
| Returned | 14 | ₹24,471 lost |
| Cancelled — payment failed | 10 | ₹23,014 lost |
| Cancelled — out of stock | 2 | ₹17,096 lost |
| **Total lost** | **26** | **₹64,582 (25.1% of GMV)** |

A single-system dashboard reports one aggregate "cancellation rate". The joined
view splits it three ways, and each points at a different team: returns are a
product-quality problem, payment cancellations belong to the PMS and the
gateway, stock-out cancellations belong to procurement.

The stock-out row matters most per order. Two orders lost ₹17,096 — an average
of ₹8,548 each, well above the ₹2,301 average for payment cancellations, because
the SKUs that run out are the fast-moving, high-value ones. Ranking by order
count alone would have put this last; ranking by value puts it in contention.

## Finding 2 — Fraud detection measured, not asserted

The PMS producer tags synthetic fraud with a ground-truth `fraud_type`. The
consumer's rules never read that tag.

| | Count |
|---|---|
| Orders that were actually fraud | 6 |
| Orders flagged by the rules | 6 |
| True positives | 6 |
| False positives | 0 |
| **Recall** | **100%** |
| **Precision** | **100%** |

Chart 4.4 breaks recall down by fraud type, which is the useful view: a rule set
can catch every high-value case and miss every card-testing case while still
reporting a good headline number. Overall payment failure rate was 25.3% (29
failed + 8 declined of 146 attempts), inflated by the seeded retry patterns —
worth stating rather than presenting as a real-world figure.

## Finding 3 — Abandoned carts dwarf completed revenue

104 carts abandoned, worth ₹2,50,803 — more than the ₹1,92,653 actually
retained. Conversion from session to checkout was 21.9%.

An abandoned cart is detected by the *absence* of an event: a background watcher
in the clickstream consumer ages idle carts out. Batch processing cannot express
that at all, because there is no record to join on — only a gap. This is the
single strongest argument in the project for streaming over batch.

## Finding 4 — Latency decomposes across system boundaries

Average per stage, in seconds of simulated time:

| Stage | Systems involved | Seconds | Share |
|---|---|---|---|
| Browse → Checkout | CLK | 4.0 | shopper-controlled |
| Checkout → Paid | OMS → PMS | 1.1 | 24% |
| Paid → Shipped | PMS → IMS → OMS | 2.5 | 56% |
| Shipped → Delivered | OMS | 1.0 | 22% |
| **Checkout → Delivered** | all four | **4.5** | 100% |

Over half the post-checkout time is Paid→Shipped — warehouse picking and
packing, not payment or transit. A manager watching only end-to-end time would
see "4.5 seconds" and have nowhere to act. The decomposition names the shift to
staff. Ratios are the meaningful part; absolute values are compressed by
`--speed 8`.

## Finding 5 — The reorder point failed, and the dashboard shows why

One SKU/warehouse position hit zero available stock, triggering `OUT_OF_STOCK`,
a delisting, and two cancelled orders — despite a reorder alert having fired
earlier at the low-stock threshold.

The reason is visible in chart 5.4: suppliers short-shipped on 7 purchase
orders. A supplier filling 80% of a reorder silently makes the replenishment 20%
smaller than planned, so cover runs out before the next PO lands. Raising the
reorder point without fixing supplier fill rate would not have prevented this —
which is a conclusion that needs the IMS purchase-order stream and the stock
ledger side by side.

## Finding 6 — Demand with no catalog cover

Five distinct search terms returned zero results repeatedly. Each is a shopper
who intended to spend and found nothing. Unlike every other loss in this
analysis, it never enters the order data at all — it is invisible to any system
downstream of checkout, and only the clickstream stream can see it.

---

## What a batch report could not have produced

| Insight | Why batch fails |
|---|---|
| Cancel orders for a SKU the moment it goes out of stock | The decision has a window of minutes; overnight, the orders are already taken |
| Cart abandonment | Triggered by the absence of an event — nothing to join on |
| Fraud pattern before settlement | Retries happen within seconds; by morning the money has moved |
| Lost revenue split by root cause | Requires four systems joined at the order level, live |
| Latency decomposed by stage | Needs timestamps from four systems on the same order |
| Gateway outage vs fraud | Same headline failure rate, opposite responses; only the live per-gateway split distinguishes them |

## Honest limits

- **Simulated clock.** `--speed 8` compresses lifecycles; treat durations as
  relative, not as real delivery times.
- **Seeded fraud.** 6% of checkouts are deliberately fraudulent, far above a
  real platform. The detection *method* generalises; the rate does not.
- **Short window.** 3 minutes gives 3–4 minute buckets on the time-series
  charts. Run 8–10 minutes before screenshotting for the presentation.
- **Single broker.** Replication factor 1 locally. The production design in the
  architecture doc specifies 3.
