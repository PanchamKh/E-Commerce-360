# Architecture

## 1. The platform at a glance

```
┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│ 1. CLICKSTREAM│  │ 2. ORDER MGMT │  │ 3. PAYMENT    │  │ 4. INVENTORY  │
│    (CLK)      │  │    (OMS)      │  │    (PMS)      │  │    (IMS)      │
│               │  │               │  │               │  │               │
│ Web/App       │  │ Order Service │  │ Gateway       │  │ WMS / Warehouse│
│ Tracker       │  │ (orchestrator)│  │ Service       │  │ Service        │
└──────┬────────┘  └──────┬────────┘  └──────┬────────┘  └──────┬────────┘
       │                  │                  │                  │
       ▼                  ▼                  ▼                  ▼
╔══════════════════════ APACHE KAFKA — 14 topics ══════════════════════════╗
║ page-views · search-queries · click-events · cart-events                 ║
║ checkout-requests │ orders · fulfillment · order-cancellations           ║
║ payment-requests │ payment-transactions · payment-results                ║
║ stock-movements · purchase-orders · stock-alerts                         ║
╚══════════════════════════════════════════════════════════════════════════╝
       │                  │                  │             │            │
       ▼                  ▼                  ▼             ▼            ▼
┌────────────┐   ┌────────────┐   ┌────────────┐   ┌────────────┐  ┌──────────┐
│ CLK        │   │ OMS        │   │ PMS        │   │ IMS        │  │ANALYTICS │
│ Consumer   │   │ Consumer   │   │ Consumer   │   │ Consumer   │  │Consumer  │
│ (funnel)   │   │ (orders)   │   │ (fraud)    │   │ (stock)    │  │(all 14)  │
└─────┬──────┘   └─────┬──────┘   └─────┬──────┘   └─────┬──────┘  └────┬─────┘
      └────────────────┴────────────────┴────────────────┴──────────────┘
                                   ▼
                 MongoDB Atlas — ecommerce360 (13 collections)
                                   ▼
                 Atlas Charts — 33 charts, auto-refresh 1 min
```

## 2. The integration loop

What makes this one platform rather than four:

```
shopper adds to cart
   └─▶ CLK  checkout-requests-topic
          └─▶ OMS  creates order · ORDER_PLACED
                 ├─ is the SKU delisted by IMS?  ── yes ─▶ cancel: CANCELLED_STOCKOUT
                 └─ no ─▶ payment-requests-topic
                            └─▶ PMS  attempts (retries, fraud patterns)
                                   └─▶ payment-results-topic
                                          ├─ FAILED ─▶ OMS cancels: CANCELLED_PAYMENT
                                          └─ AUTHORISED ─▶ OMS  ORDER_CONFIRMED
                                                 └─▶ IMS reserves stock
                                                        └─▶ OMS PACKED
                                                               └─▶ IMS issues stock
                                                                      ├─ available ≤ 0
                                                                      │    └─▶ stock-alerts: OUT_OF_STOCK
                                                                      │           └─▶ OMS delists SKU
                                                                      └─ available ≤ reorder point
                                                                           └─▶ purchase-orders: PO_RAISED
                                                                                  └─(lead time)─▶ GOODS_RECEIVED
                                                                                         └─▶ BACK_IN_STOCK
                                                                                                └─▶ OMS relists SKU
```

## 3. Topic contracts

| Topic | Producer | Consumers | Key | Carries |
|---|---|---|---|---|
| `page-views-topic` | CLK tracker | CLK, ANL | `session_id` | page type, referrer, category |
| `search-queries-topic` | CLK tracker | CLK, ANL | `session_id` | search term, `results_count` |
| `click-events-topic` | CLK tracker | CLK, ANL | `session_id` | click target, product |
| `cart-events-topic` | CLK tracker | CLK, ANL | `session_id` | add/remove, qty, cart value |
| `checkout-requests-topic` | CLK tracker | **OMS**, CLK, ANL | `session_id` | checkout intent: SKU, qty, order value, method |
| `orders-topic` | OMS service | **IMS**, OMS, ANL | `order_id` | ORDER_PLACED, ORDER_CONFIRMED |
| `fulfillment-topic` | OMS service | **IMS**, OMS, ANL | `order_id` | PROCESSING → PACKED → SHIPPED → DELIVERED |
| `order-cancellations-topic` | OMS service | **IMS**, OMS, ANL | `order_id` | ORDER_CANCELLED (with cause), RETURN_RAISED |
| `payment-requests-topic` | OMS service | **PMS**, ANL | `order_id` | amount, method, customer context |
| `payment-transactions-topic` | PMS gateway | PMS, ANL | `order_id` | every attempt + ground-truth fraud tag |
| `payment-results-topic` | PMS gateway | **OMS**, PMS, ANL | `order_id` | AUTHORISED / FAILED, attempt count |
| `stock-movements-topic` | IMS warehouse | IMS, ANL | `sku:warehouse` | every physical movement, stock after |
| `purchase-orders-topic` | IMS warehouse | IMS, ANL | `po_id` | PO_RAISED → APPROVED → GOODS_RECEIVED → CLOSED |
| `stock-alerts-topic` | IMS warehouse | **OMS**, IMS, ANL | `sku:warehouse` | LOW_STOCK, OUT_OF_STOCK, BACK_IN_STOCK, DEFECT_CLUSTER |

Bold = a **cross-system** consumer: one system acting on another's events.

Every message carries `event_id`, `event_type`, `event_timestamp` (ISO-8601
IST), `minute_ist` (pre-computed bucket) and `source_system`.

## 4. Consumer groups

| Consumer | Group ID | Topics | Writes |
|---|---|---|---|
| Session & Funnel | `clk-session-funnel-group` | 5 CLK topics | `clk_sessions`, `clk_carts`, `clk_search_gaps` |
| Order & Fulfillment | `oms-order-fulfillment-group` | 3 OMS topics | `oms_orders`, `oms_returns` |
| Transaction & Fraud | `pms-transaction-fraud-group` | 2 PMS topics | `pms_transactions`, `pms_payments` |
| Stock Position | `ims-stock-position-group` | 3 IMS topics | `ims_stock`, `ims_movements`, `ims_purchase_orders` |
| Cross-System Analytics | `analytics-360-group` | **all 14** | `pipeline_events`, `journey_orders` |

All five write to the shared `alerts` collection, tagged by `source_system`.

## 5. The journey document

`journey_orders` is the collection the headline charts read. One document per
order, assembled from events produced by four different systems:

```json
{
  "_id": "ORD-2026-700042",
  "session_id": "SESS-a1b2c3d4e5f6",        // CLK
  "customer_id": "CUST-10077",
  "city": "Delhi NCR", "device": "Mobile", "channel": "Paid Ads",
  "sku": "SKU-2001-WHT-STD", "category": "Electronics",
  "warehouse_id": "WH-DEL-01", "quantity": 2,
  "order_value": 6298.20,                    // OMS
  "payment_attempts": 3, "payment_failures": 2,
  "payment_result": "AUTHORISED",            // PMS
  "payment_gateway": "Razorpay",
  "is_fraud_ground_truth": true,
  "fraud_type_ground_truth": "FAILED_THEN_SUCCESS",
  "stock_reserved": true, "stock_issued": true,   // IMS
  "outcome": "DELIVERED",
  "retained_revenue": 6298.20, "lost_revenue": 0.0,
  "secs_browse_to_checkout": 4.1,            // derived across systems
  "secs_checkout_to_paid": 1.1,
  "secs_paid_to_shipped": 2.5,
  "secs_shipped_to_delivered": 1.0,
  "secs_total": 4.5
}
```

Durations are computed in Python at write time and stored as plain numbers, so
the dashboard needs no date parsing.

## 6. Alert rules

| Alert | System | Trigger | Severity |
|---|---|---|---|
| `SEARCH_GAP_DETECTED` | CLK | same term returns 0 results 3× | MEDIUM |
| `CART_ABANDONED` | CLK | cart idle past timeout (absence of an event) | LOW |
| `REPEAT_ABANDONER` | CLK | same customer abandons 3 carts | HIGH |
| `HIGH_VALUE_ORDER` | OMS | order above ₹15,000 | LOW |
| `ORDER_CANCELLED_STOCKOUT` | OMS | order cancelled because IMS delisted the SKU | HIGH |
| `RETURN_DAMAGED` | OMS | return reason is damage or defect | MEDIUM |
| `RULE_HIGH_VALUE` | PMS | single attempt above the fraud threshold | HIGH |
| `RULE_REPEATED_FAILURE` | PMS | 3+ failed attempts on one order | HIGH |
| `RULE_CARD_TESTING` | PMS | success after 2+ failures | HIGH |
| `RULE_METHOD_CYCLING` | PMS | 3+ payment methods on one order | MEDIUM |
| `LOW_STOCK` | IMS | available ≤ 8 (fires once on the way down) | MEDIUM |
| `OUT_OF_STOCK` | IMS | available ≤ 0 | HIGH |
| `BACK_IN_STOCK` | IMS | replenished above the low threshold | LOW |
| `DEFECT_CLUSTER` | IMS | same SKU returned damaged 3× | HIGH |
| `SUPPLIER_SHORT_SHIP` | IMS | PO fill rate below 100% | MEDIUM |

## 7. Engineering choices

**Bus and store are abstractions.** `shared/bus.py` and `shared/store.py` each
have a real implementation and an in-memory one behind the same interface. The
services are written against the interface, so the identical code path runs
with or without Docker. Offline mode is how the pipeline was tested — and it
caught two real bugs: an upsert key-lookup fault, and a service loop where one
malformed message silently killed a consumer thread.

**The scheduler.** `Service.later(seconds, callback)` is what makes order
lifecycles progress over real elapsed time instead of instantly. At any moment
hundreds of orders sit at different stages, which is what a real platform looks
like — and what makes the per-minute charts meaningful.

**Procurement lead time is long on purpose.** A purchase order takes far longer
than a checkout. That gap is precisely why a stock-out cannot be fixed
reactively, and why the reorder alert has to fire before stock reaches zero.
