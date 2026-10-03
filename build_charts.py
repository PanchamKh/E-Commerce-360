"""
charts/build_charts.py
----------------------
Generates Ecommerce360_Dashboard.charts — a MongoDB Atlas Charts import file.

Design rules
------------
1. FIRST PRINCIPLES. A derived metric never appears before the base data it is
   computed from. Titles are numbered; each description says BASE or DERIVED and
   gives the formula.
2. CROSS-SYSTEM. The headline charts read `journey_orders`, the collection that
   stitches all four systems into one document per order. Single-system charts
   exist to explain what the cross-system charts show.
3. CATEGORY CHANNELS CARRY NO NUMBER FORMATTING. Atlas Charts renders NaN if a
   text channel is given a numeric format — the labels below are checked
   against CATEGORY_LABELS to prevent it.

Run:  python charts/build_charts.py
"""

import json
import os
import uuid

DEPLOYMENT = "Cluster0"          # remapped during import
DB = "ecommerce360"

DS = {
    "events":    ("data-source-1",  "pipeline_events"),
    "journey":   ("data-source-2",  "journey_orders"),
    "alerts":    ("data-source-3",  "alerts"),
    "sessions":  ("data-source-4",  "clk_sessions"),
    "carts":     ("data-source-5",  "clk_carts"),
    "searches":  ("data-source-6",  "clk_search_gaps"),
    "txns":      ("data-source-7",  "pms_transactions"),
    "payments":  ("data-source-8",  "pms_payments"),
    "stock":     ("data-source-9",  "ims_stock"),
    "pos":       ("data-source-10", "ims_purchase_orders"),
}
ALIAS = {
    "events":   "360: Pipeline Events (all 14 topics)",
    "journey":  "360: Order Journey (CLK→OMS→PMS→IMS)",
    "alerts":   "360: Unified Alerts",
    "sessions": "CLK: Sessions & Funnel",
    "carts":    "CLK: Cart Status",
    "searches": "CLK: Zero-Result Searches",
    "txns":     "PMS: Transactions",
    "payments": "PMS: Payments per Order",
    "stock":    "IMS: Stock Positions",
    "pos":      "IMS: Purchase Orders",
}

PALETTE = ["#1392AB", "#16CC62", "#E6B219", "#E6196E", "#196EE6", "#E65D19",
           "#19C3E6", "#116149", "#80340E", "#ABABAB", "#73E0A1", "#8CB6F2"]


# --------------------------------------------------------------------------
# Calculated fields
# --------------------------------------------------------------------------
CALC = {
    "funnel_stage_label": {"$switch": {"branches": [
        {"case": {"$eq": ["$funnel_stage", "view"]},     "then": "1. Viewed"},
        {"case": {"$eq": ["$funnel_stage", "search"]},   "then": "2. Searched"},
        {"case": {"$eq": ["$funnel_stage", "click"]},    "then": "3. Clicked product"},
        {"case": {"$eq": ["$funnel_stage", "cart"]},     "then": "4. Added to cart"},
        {"case": {"$eq": ["$funnel_stage", "checkout"]}, "then": "5. Checked out"},
    ], "default": "Other"}},

    "pct_reached_click":    {"$multiply": [{"$ifNull": ["$reached_click", 0]}, 100]},
    "pct_reached_cart":     {"$multiply": [{"$ifNull": ["$reached_cart", 0]}, 100]},
    "pct_reached_checkout": {"$multiply": [{"$ifNull": ["$reached_checkout", 0]}, 100]},

    "outcome_label": {"$switch": {"branches": [
        {"case": {"$eq": ["$outcome", "DELIVERED"]},          "then": "Delivered (revenue kept)"},
        {"case": {"$eq": ["$outcome", "RETURNED"]},           "then": "Returned (lost after delivery)"},
        {"case": {"$eq": ["$outcome", "CANCELLED_PAYMENT"]},  "then": "Cancelled — payment failed"},
        {"case": {"$eq": ["$outcome", "CANCELLED_STOCKOUT"]}, "then": "Cancelled — out of stock"},
    ], "default": "In progress"}},

    "lost_flag_pct": {"$cond": [{"$gt": [{"$ifNull": ["$lost_revenue", 0]}, 0]}, 100, 0]},

    "txn_failed_pct": {"$cond": [
        {"$in": ["$transaction_status", ["FAILED", "DECLINED"]]}, 100, 0]},

    "flagged_pct": {"$cond": [{"$eq": [{"$ifNull": ["$flagged", False]}, True]}, 100, 0]},

    "fraud_type_label": {"$ifNull": ["$fraud_type_ground_truth", "Not fraud (normal)"]},

    "stockout_flag_pct": {"$cond": [
        {"$lte": [{"$ifNull": ["$lowest_available", 999]}, 0]}, 100, 0]},

    "short_ship_pct": {"$subtract": [100, {"$ifNull": ["$fill_rate_pct", 100]}]},
}


def calc(name):
    expr = CALC[name]
    return {"fieldPath": name,
            "rawExpression": json.dumps(expr, indent=2),
            "derivedMQL": json.dumps(expr, separators=(",", ":"))}


# --------------------------------------------------------------------------
# Channel helpers
# --------------------------------------------------------------------------
CATEGORY_LABELS = {
    "Minute (IST)", "System", "Topic", "Event type", "Funnel stage", "Device",
    "Channel", "City", "Category", "SKU", "Warehouse", "Outcome", "Cart status",
    "Search term", "Payment method", "Gateway", "Failure reason", "Fraud type",
    "Alert type", "Severity", "Supplier", "Segment", "Product",
}


def cat(field, limit=None):
    c = {"field": field, "type": "nominal", "inferredType": "String",
         "transformedType": "String", "channelType": "category"}
    if limit:
        c.update({"isLimiting": True, "limitingSize": str(limit)})
    return c


def agg(field, aggregate, inferred="Number"):
    return {"field": field, "type": "quantitative", "inferredType": inferred,
            "channelType": "aggregation", "aggregate": aggregate}


def count():
    return agg("_id", "count", inferred="String")


def label(text, decimals=None):
    ch = {"labelOverride": {"enabled": True, "value": text}}
    if text in CATEGORY_LABELS:
        return ch
    ch["numberFormatting"] = {"enabled": True, "value": "Default"}
    if decimals is not None:
        ch["numberFormatting"] = {"enabled": True, "value": "Custom"}
        ch["numberDecimals"] = {"enabled": True, "value": str(decimals)}
    return ch


def f_in(field, values):
    return {"fieldPath": field, "disabled": False, "type": "String",
            "settings": {"allOthers": False, "values": values}}


def f_not_in(field, values):
    return {"fieldPath": field, "disabled": False, "type": "String",
            "settings": {"allOthers": True, "values": values}}


def f_min(field, value="0"):
    return {"fieldPath": field, "disabled": False, "type": "Number",
            "settings": {"min": {"enabled": True, "inclusive": True, "value": value},
                         "max": {"enabled": False, "inclusive": True, "value": ""}}}


ICON = {"Number": "number", "Donut": "donut", "Grouped Bar": "bar-grouped",
        "Stacked Bar": "bar-stacked", "Grouped Combo": "combo-grouped",
        "Data Table": "data-table", "Heatmap": "heatmap"}

BAR_OPTS = {"dataValueLabels": {"enabled": True, "value": None},
            "colorDiscrete": {"enabled": True, "value": PALETTE}}
STACK_OPTS = {"colorDiscrete": {"enabled": True, "value": PALETTE}}
TABLE_OPTS = {"totalsColumn": {"enabled": False, "value": None}}


def continuous(channel, reverse=False):
    return {"type": "continuous",
            "conditions": [{"target": {"type": "CHANNEL", "value": channel},
                            "operator": "", "value": ""}],
            "applyToEntireRow": False,
            "styling": {"backgroundColor": "mdbredwhitegreen", "reverse": reverse}}


def red_when(channel, operator, value):
    return {"type": "discrete",
            "conditions": [{"target": {"value": channel, "type": "CHANNEL"},
                            "operator": operator, "value": value}],
            "applyToEntireRow": False,
            "styling": {"color": "rgb(193, 4, 34)",
                        "backgroundColor": "rgba(0, 0, 0, 0)",
                        "fontStyle": "normal", "fontWeight": "bold",
                        "textDecoration": "initial"}}


# --------------------------------------------------------------------------
ITEMS, LAYOUT = {}, []


def add(title, description, ds, chart_type, channels, *, calcs=(), filters=(),
        channel_custom=None, options=None, axes=None, conditional=None,
        meta=None, pos=(0, 0, 4, 2)):
    iid = f"item-{len(ITEMS) + 1}"
    ITEMS[iid] = {
        "title": title, "description": description,
        "dashboardId": "dashboard-1", "dataSourceId": DS[ds][0],
        "iconValue": ICON[chart_type], "itemType": "chart",
        "filters": list(filters), "missedFields": [], "lookupFields": [],
        "convertedFields": [], "calculatedFields": [calc(c) for c in calcs],
        "channels": channels, "reductions": {},
        "customisations": {"options": options or {}, "axes": axes or {},
                           "channels": channel_custom or {},
                           "conditionalFormatting": conditional or []},
        "chartType": chart_type, "meta": meta or {}, "sample": False,
        "query": None, "queryId": None, "interactiveFiltering": "highlight",
        "embedding": {"id": str(uuid.uuid4())},
    }
    x, y, w, h = pos
    LAYOUT.append({"w": w, "h": h, "x": x, "y": y, "i": iid})


def kpi(title, description, ds, channel, *, calcs=(), filters=(), decimals=0, pos=(0, 0)):
    add(title, description, ds, "Number", {"value": channel},
        calcs=calcs, filters=filters,
        channel_custom={"value": label(title, decimals)},
        pos=(pos[0], pos[1], 2, 1))


HAS_VALUE = f_min("order_value", "0")


# ==========================================================================
# ROW 0–1 · HEADLINE KPIs
# ==========================================================================
kpi("Events Ingested (all systems)",
    "BASE. Every message the analytics consumer read from all 14 Kafka topics across CLK, OMS, PMS and IMS.",
    "events", count(), pos=(0, 0))

kpi("Shopper Sessions",
    "BASE. Distinct sessions seen by the clickstream system. Top of the funnel — the denominator for conversion.",
    "sessions", count(), pos=(2, 0))

kpi("Orders Created",
    "BASE. Orders the OMS created from checkout intent. One document per order in journey_orders.",
    "journey", count(), pos=(4, 0))

kpi("Checkout Conversion (%)",
    "DERIVED = sessions that reached checkout ÷ all sessions × 100. Built from the two KPIs to the left.",
    "sessions", agg("pct_reached_checkout", "mean"), calcs=["pct_reached_checkout"],
    decimals=1, pos=(6, 0))

kpi("Revenue Retained (₹)",
    "BASE. Order value of orders that were actually delivered and kept.",
    "journey", agg("retained_revenue", "sum"), pos=(0, 1))

kpi("Revenue Lost (₹)",
    "BASE. Order value lost to payment failure, stock-out cancellation or post-delivery return. Attribution in 3.2.",
    "journey", agg("lost_revenue", "sum"), pos=(2, 1))

kpi("Order Loss Rate (%)",
    "DERIVED = orders with any lost revenue ÷ all orders × 100.",
    "journey", agg("lost_flag_pct", "mean"), calcs=["lost_flag_pct"],
    decimals=1, pos=(4, 1))

kpi("Open Alerts (all systems)",
    "BASE. Unresolved alerts from every system in one stream. Breakdown in 7.1.",
    "alerts", count(), filters=[f_in("status", ["open"])], pos=(6, 1))


# ==========================================================================
# SECTION 1 · PIPELINE HEALTH — is the stream actually live?
# ==========================================================================
add("1.1 Stream Throughput — Events per Minute by System",
    "BASE. Each cell = messages one system produced in that minute (IST). Proves all four systems are "
    "live simultaneously, and shows their relative volume: clickstream is always the loudest, payments "
    "the quietest, because one order generates many page views but only a few transactions.",
    "events", "Heatmap",
    {"x": cat("minute_ist"), "y": cat("source_system"), "color": count()},
    axes={"x": {"categoryLabelAngle": {"enabled": True, "value": "diagonal"}}},
    channel_custom={"x": label("Minute (IST)"), "y": label("System"),
                    "color": label("Events")},
    pos=(0, 2, 8, 2))

add("1.2 Message Volume by Kafka Topic",
    "BASE. Total messages carried by each of the 14 topics. A topic with zero traffic means a producer "
    "is down; a topic dominating the chart is the one to partition first if throughput becomes a problem.",
    "events", "Grouped Bar",
    {"y": cat("topic", limit=14), "x": count()},
    options=BAR_OPTS,
    channel_custom={"y": label("Topic"), "x": label("Messages")},
    pos=(0, 4, 4, 2))

add("1.3 Orders and Retained Revenue per Minute",
    "BASE → DERIVED. Bars = orders created per minute. Line = revenue retained per minute. When the bars "
    "hold steady but the line falls, volume is fine and something downstream — payments or stock — is "
    "eating the revenue.",
    "journey", "Grouped Combo",
    {"x": cat("minute_ist"), "y": count(), "ylines": agg("retained_revenue", "sum")},
    options={"dataMarkers": {"enabled": True, "value": None}},
    axes={"x": {"categoryLabelAngle": {"enabled": True, "value": "diagonal"}},
          "ylines2": {"labelOverride": {"enabled": True, "value": "Retained ₹"}}},
    channel_custom={"x": label("Minute (IST)"), "y": label("Orders"),
                    "ylines": {**label("Retained ₹", 0),
                               "plotOnSecondaryAxis": {"enabled": True, "value": None}}},
    pos=(4, 4, 4, 2))


# ==========================================================================
# SECTION 2 · THE FUNNEL (CLK → OMS)
# ==========================================================================
add("2.1 Session Funnel — Furthest Stage Reached",
    "BASE. Each session counted once, at the deepest stage it reached. The drop between adjacent bars is "
    "where shoppers are lost; the gap between 'Added to cart' and 'Checked out' is recoverable demand.",
    "sessions", "Grouped Bar",
    {"y": cat("funnel_stage_label"), "x": count()},
    calcs=["funnel_stage_label"], options=BAR_OPTS,
    channel_custom={"y": label("Funnel stage"), "x": label("Sessions")},
    pos=(0, 6, 4, 2))

add("2.2 Conversion by Device (%)",
    "DERIVED from 2.1 = share of that device's sessions reaching each stage × 100. Absolute counts hide "
    "device problems; rates expose them — a mobile checkout rate far below desktop is a UX defect, not "
    "a traffic problem.",
    "sessions", "Data Table",
    {"group": cat("device"), "value": count(),
     "value_series_0": agg("pct_reached_click", "mean"),
     "value_series_1": agg("pct_reached_cart", "mean"),
     "value_series_2": agg("pct_reached_checkout", "mean")},
    calcs=["pct_reached_click", "pct_reached_cart", "pct_reached_checkout"],
    options=TABLE_OPTS,
    channel_custom={"group": label("Device"), "value": label("Sessions"),
                    "value_series_0": label("Reached click %", 1),
                    "value_series_1": label("Reached cart %", 1),
                    "value_series_2": label("Checked out %", 1)},
    conditional=[continuous("value_series_2")],
    meta={"sortModel": [{"colId": "value", "sort": "desc"}]},
    pos=(4, 6, 4, 2))

add("2.3 Checkout Conversion by Acquisition Channel (%)",
    "DERIVED = sessions from that channel reaching checkout ÷ sessions from that channel × 100. Spend "
    "should follow this chart, not raw traffic.",
    "sessions", "Grouped Bar",
    {"y": cat("channel"), "x": agg("pct_reached_checkout", "mean")},
    calcs=["pct_reached_checkout"], options=BAR_OPTS,
    channel_custom={"y": label("Channel"), "x": label("Checkout rate %", 1)},
    pos=(0, 8, 4, 2))

add("2.4 Cart Outcomes",
    "BASE. Every cart ends in one of four states. 'Abandoned' is assigned by the consumer's background "
    "watcher when a cart sits idle past the timeout — a decision only a streaming system can make, "
    "because it is triggered by the absence of an event.",
    "carts", "Donut",
    {"label": cat("status"), "value": count()},
    options=STACK_OPTS,
    channel_custom={"label": label("Cart status"), "value": label("Carts")},
    pos=(4, 8, 4, 2))

add("2.5 Abandoned Cart Value by Category (₹)",
    "DERIVED from 2.4 = cart value of abandoned carts only. This is live recoverable revenue — the list "
    "a retargeting campaign should be built from within minutes, not tomorrow.",
    "carts", "Grouped Bar",
    {"y": cat("category"), "x": agg("cart_value", "sum")},
    filters=[f_in("status", ["abandoned"])], options=BAR_OPTS,
    channel_custom={"y": label("Category"), "x": label("Abandoned value ₹", 0)},
    pos=(0, 10, 4, 2))

add("2.6 Demand With No Catalog Cover",
    "BASE. Search terms that returned zero results, counted. Each one is a customer who wanted to spend "
    "money and found nothing — the cheapest assortment signal in the business.",
    "searches", "Grouped Bar",
    {"y": cat("search_term", limit=12), "x": agg("zero_result_count", "sum")},
    options=BAR_OPTS,
    channel_custom={"y": label("Search term"), "x": label("Zero-result searches")},
    pos=(4, 10, 4, 2))


# ==========================================================================
# SECTION 3 · REVENUE OUTCOME — the cross-system view
# ==========================================================================
add("3.1 Where Every Order Ends Up",
    "BASE. Outcome of each order, from the journey_orders collection that joins all four systems. An "
    "order is only 'kept' if the clickstream produced it, the OMS accepted it, the PMS collected the "
    "money, the IMS shipped the stock and it was not returned.",
    "journey", "Donut",
    {"label": cat("outcome_label"), "value": agg("order_value", "sum")},
    calcs=["outcome_label"], options=STACK_OPTS,
    channel_custom={"label": label("Outcome"), "value": label("Order value ₹", 0)},
    pos=(0, 12, 4, 2))

add("3.2 Lost Revenue by Root Cause — Which System Is Responsible",
    "DERIVED from 3.1 = lost revenue grouped by the cause recorded at cancellation. This is the chart "
    "that only exists because the systems are joined: payment failure is a PMS problem, stock-out is an "
    "IMS problem, returns are a product-quality problem. Each points at a different team.",
    "journey", "Grouped Bar",
    {"y": cat("outcome_label"), "x": agg("lost_revenue", "sum")},
    calcs=["outcome_label"], filters=[f_min("lost_revenue", "1")],
    options=BAR_OPTS,
    channel_custom={"y": label("Outcome"), "x": label("Lost revenue ₹", 0)},
    pos=(4, 12, 4, 2))

add("3.3 Category Scorecard — Volume, Value and Leakage",
    "BASE → DERIVED. Orders and order value per category (base), then revenue retained, revenue lost and "
    "loss rate % (derived). Sorted by lost revenue: the top row is where the money is going.",
    "journey", "Data Table",
    {"group": cat("category"), "value": count(),
     "value_series_0": agg("order_value", "sum"),
     "value_series_1": agg("retained_revenue", "sum"),
     "value_series_2": agg("lost_revenue", "sum"),
     "value_series_3": agg("lost_flag_pct", "mean")},
    calcs=["lost_flag_pct"], options=TABLE_OPTS,
    channel_custom={"group": label("Category"), "value": label("Orders"),
                    "value_series_0": label("Order value ₹", 0),
                    "value_series_1": label("Retained ₹", 0),
                    "value_series_2": label("Lost ₹", 0),
                    "value_series_3": label("Loss rate %", 1)},
    conditional=[continuous("value_series_2", reverse=True)],
    meta={"sortModel": [{"colId": "value_series_2", "sort": "desc"}]},
    pos=(0, 14, 4, 2))

add("3.4 Order Loss Rate by Fulfilment Warehouse (%)",
    "DERIVED = orders with lost revenue ÷ orders routed to that warehouse × 100. Normalises for warehouse "
    "size, so a small warehouse with a stock problem is not hidden by a large healthy one.",
    "journey", "Grouped Bar",
    {"y": cat("warehouse_id"), "x": agg("lost_flag_pct", "mean")},
    calcs=["lost_flag_pct"], options=BAR_OPTS,
    channel_custom={"y": label("Warehouse"), "x": label("Loss rate %", 1)},
    pos=(4, 14, 4, 2))


# ==========================================================================
# SECTION 4 · PAYMENTS (PMS)
# ==========================================================================
add("4.1 Payment Attempts by Method and Outcome",
    "BASE. Every gateway attempt, split by result. An order that fails twice and succeeds contributes "
    "three attempts here — which is why attempt counts exceed order counts.",
    "txns", "Stacked Bar",
    {"y": cat("payment_method"), "x": count(), "color": cat("transaction_status")},
    options=STACK_OPTS,
    channel_custom={"y": label("Payment method"), "x": label("Attempts"),
                    "color": label("Outcome")},
    pos=(0, 16, 4, 2))

add("4.2 Failure Rate by Gateway (%)",
    "DERIVED from 4.1 = failed or declined attempts ÷ all attempts for that gateway × 100. One gateway "
    "spiking while the others hold steady is an outage, not fraud — and the fix is to reroute traffic, "
    "not to block customers.",
    "txns", "Grouped Bar",
    {"y": cat("payment_gateway"), "x": agg("txn_failed_pct", "mean")},
    calcs=["txn_failed_pct"], options=BAR_OPTS,
    channel_custom={"y": label("Gateway"), "x": label("Failure rate %", 1)},
    pos=(4, 16, 4, 2))

add("4.3 Why Payments Fail",
    "BASE. Declared failure reason on every failed or declined attempt. Explains the rates in 4.2 and "
    "separates customer-side causes (insufficient funds) from infrastructure ones (gateway timeout).",
    "txns", "Grouped Bar",
    {"y": cat("failure_reason"), "x": count()},
    filters=[f_in("transaction_status", ["FAILED", "DECLINED"])],
    options=BAR_OPTS,
    channel_custom={"y": label("Failure reason"), "x": label("Failed attempts")},
    pos=(0, 18, 4, 2))

add("4.4 Fraud Detection Quality — Rules vs Ground Truth",
    "DERIVED. The producer tags every synthetic fraud transaction with its true type; the consumer's "
    "rules never see that tag. Grouping flagged % by true type measures RECALL per fraud pattern, and "
    "the 'Not fraud' row is the false-positive rate. Counting alerts alone would tell you neither.",
    "payments", "Data Table",
    {"group": cat("fraud_type_label"), "value": count(),
     "value_series_0": agg("flagged_pct", "mean"),
     "value_series_1": agg("amount", "sum")},
    calcs=["fraud_type_label", "flagged_pct"], options=TABLE_OPTS,
    channel_custom={"group": label("Fraud type"), "value": label("Orders"),
                    "value_series_0": label("Caught by rules %", 1),
                    "value_series_1": label("Value at risk ₹", 0)},
    conditional=[continuous("value_series_0")],
    meta={"sortModel": [{"colId": "value", "sort": "desc"}]},
    pos=(4, 18, 4, 2))


# ==========================================================================
# SECTION 5 · INVENTORY (IMS)
# ==========================================================================
add("5.1 Stock Ledger by SKU",
    "BASE → DERIVED. Units issued and received per SKU (base), then current on-hand and the LOWEST "
    "available level ever reached (derived). Current stock alone lies — a SKU sitting at 30 units today "
    "may have hit zero an hour ago and cost you orders. Sorted by that low-water mark.",
    "stock", "Data Table",
    {"group": cat("sku", limit=20), "value": agg("units_out", "sum"),
     "value_series_0": agg("units_in", "sum"),
     "value_series_1": agg("on_hand", "sum"),
     "value_series_2": agg("lowest_available", "min"),
     "value_series_3": agg("stockout_episodes", "sum")},
    options=TABLE_OPTS,
    channel_custom={"group": label("SKU"), "value": label("Units out", 0),
                    "value_series_0": label("Units in", 0),
                    "value_series_1": label("On hand now", 0),
                    "value_series_2": label("Lowest available", 0),
                    "value_series_3": label("Stock-out episodes", 0)},
    conditional=[continuous("value_series_2"),
                 red_when("value_series_3", "NUMBER_GREATER_THAN", "0")],
    meta={"sortModel": [{"colId": "value_series_2", "sort": "asc"}]},
    pos=(0, 20, 4, 2))

add("5.2 Stock-Out Exposure by SKU (%)",
    "DERIVED from 5.1 = share of a SKU's warehouse positions that reached zero available stock. 100% "
    "means the item was unsellable everywhere at once — the reorder list, ranked.",
    "stock", "Grouped Bar",
    {"y": cat("sku", limit=15), "x": agg("stockout_flag_pct", "mean")},
    calcs=["stockout_flag_pct"], options=BAR_OPTS,
    channel_custom={"y": label("SKU"), "x": label("Positions out of stock %", 0)},
    conditional=[red_when("x", "NUMBER_GREATER_THAN", "0")],
    pos=(4, 20, 4, 2))

add("5.3 Capital Tied Up in Stock by Category (₹)",
    "BASE. On-hand units × unit cost, summed. Read against 3.3: a category holding heavy stock while "
    "losing revenue to returns is working capital in the wrong place.",
    "stock", "Grouped Bar",
    {"y": cat("category"), "x": agg("stock_value", "sum")},
    options=BAR_OPTS,
    channel_custom={"y": label("Category"), "x": label("Stock value ₹", 0)},
    pos=(0, 22, 4, 2))

add("5.4 Supplier Reliability",
    "BASE → DERIVED. Purchase orders per supplier and units ordered (base), then average fill rate and "
    "total short-shipped units (derived). A supplier at 80% fill rate silently makes every reorder 20% "
    "smaller than planned — which is how a stock-out survives a reorder.",
    "pos", "Data Table",
    {"group": cat("supplier_name"), "value": count(),
     "value_series_0": agg("quantity_ordered", "sum"),
     "value_series_1": agg("fill_rate_pct", "mean"),
     "value_series_2": agg("short_shipped_units", "sum")},
    options=TABLE_OPTS,
    channel_custom={"group": label("Supplier"), "value": label("POs"),
                    "value_series_0": label("Units ordered", 0),
                    "value_series_1": label("Avg fill rate %", 1),
                    "value_series_2": label("Short-shipped units", 0)},
    conditional=[continuous("value_series_1")],
    meta={"sortModel": [{"colId": "value_series_2", "sort": "desc"}]},
    pos=(4, 22, 4, 2))


# ==========================================================================
# SECTION 6 · END-TO-END LATENCY (only computable across systems)
# ==========================================================================
add("6.1 Order Latency Decomposed by Warehouse (seconds)",
    "DERIVED from timestamps in four different systems: Browse→Checkout (CLK), Checkout→Paid (OMS→PMS), "
    "Paid→Shipped (PMS→IMS→OMS), Shipped→Delivered (OMS). The four stages sum to the total, so the "
    "slowest column is the bottleneck to fix. No single system can produce this table.",
    "journey", "Data Table",
    {"group": cat("warehouse_id"), "value": count(),
     "value_series_0": agg("secs_browse_to_checkout", "mean"),
     "value_series_1": agg("secs_checkout_to_paid", "mean"),
     "value_series_2": agg("secs_paid_to_shipped", "mean"),
     "value_series_3": agg("secs_shipped_to_delivered", "mean"),
     "value_series_4": agg("secs_total", "mean")},
    options=TABLE_OPTS,
    channel_custom={"group": label("Warehouse"), "value": label("Orders"),
                    "value_series_0": label("Browse→Checkout", 1),
                    "value_series_1": label("Checkout→Paid", 1),
                    "value_series_2": label("Paid→Shipped", 1),
                    "value_series_3": label("Shipped→Delivered", 1),
                    "value_series_4": label("Total", 1)},
    conditional=[continuous("value_series_4", reverse=True),
                 continuous("value_series_2", reverse=True)],
    meta={"sortModel": [{"colId": "value_series_4", "sort": "desc"}]},
    pos=(0, 24, 4, 2))

add("6.2 Average Order-to-Delivery Time by City (seconds)",
    "DERIVED = mean of the end-to-end duration in 6.1, grouped by destination city. Read with 3.4: a "
    "city that is both slow and lossy is a routing problem, not a demand problem.",
    "journey", "Grouped Bar",
    {"y": cat("city"), "x": agg("secs_total", "mean")},
    options=BAR_OPTS,
    channel_custom={"y": label("City"), "x": label("Seconds", 1)},
    pos=(4, 24, 4, 2))


# ==========================================================================
# SECTION 7 · UNIFIED ALERTS
# ==========================================================================
add("7.1 Alerts by Type and Severity",
    "BASE. Every rule that fired in any system, in one stream. These are the items a manager acts on now: "
    "stock-outs, fraud patterns, abandoned carts, search gaps and supplier short-ships.",
    "alerts", "Stacked Bar",
    {"y": cat("alert_type", limit=15), "x": count(), "color": cat("severity")},
    options=STACK_OPTS,
    channel_custom={"y": label("Alert type"), "x": label("Alerts"),
                    "color": label("Severity")},
    pos=(0, 26, 4, 2))

add("7.2 Alert Pressure per Minute by System",
    "DERIVED from 7.1 and 1.1 = alerts per minute per system. A burst in one row while the others stay "
    "quiet localises the incident immediately — and tells the on-call which system to open first.",
    "alerts", "Heatmap",
    {"x": cat("minute_ist"), "y": cat("source_system"), "color": count()},
    axes={"x": {"categoryLabelAngle": {"enabled": True, "value": "diagonal"}}},
    channel_custom={"x": label("Minute (IST)"), "y": label("System"),
                    "color": label("Alerts")},
    pos=(4, 26, 4, 2))


# ==========================================================================
# Assemble
# ==========================================================================
def string_filter(name, links):
    return {"disabled": False, "type": "String",
            "settings": {"allOthers": True, "values": []}, "name": name,
            "linkedFields": [{"dataSourceId": DS[d][0], "fieldPath": f}
                             for d, f in links]}


export = {
    "exportVersion": 10,
    "dashboards": {"dashboard-1": {
        "title": "E-Commerce 360° — Real-Time Operations (CLK · OMS · PMS · IMS)",
        "layout": LAYOUT,
        "description": (
            "One dashboard over four streaming systems: Website Clickstream, Order Management, "
            "Payment Management and Inventory Management, joined through the journey_orders "
            "collection. Charts are ordered from first principles — base counts first, derived "
            "rates only after the data they are computed from. Numbered titles give the reading order."),
        "embedding": {"anonymousAuthEnabled": False},
        "filters": [
            string_filter("Category", [("journey", "category"), ("events", "category"),
                                       ("carts", "category"), ("stock", "category")]),
            string_filter("City", [("journey", "city"), ("events", "city"),
                                   ("sessions", "city")]),
            string_filter("Warehouse", [("journey", "warehouse_id"),
                                        ("events", "warehouse_id"),
                                        ("stock", "warehouse_id")]),
            string_filter("Device", [("sessions", "device"), ("journey", "device")]),
        ],
    }},
    "items": ITEMS,
    "dataSources": {DS[k][0]: {"alias": ALIAS[k], "collection": DS[k][1],
                               "database": DB, "deployment": DEPLOYMENT,
                               "sourceType": "cluster"} for k in DS},
    "queries": {},
}

out = os.path.join(os.path.dirname(__file__), "Ecommerce360_Dashboard.charts")
with open(out, "w", encoding="utf-8") as fh:
    json.dump(export, fh, ensure_ascii=False, separators=(",", ":"))

print(f"✅ {out}")
print(f"   {len(ITEMS)} charts, {len(DS)} data sources, "
      f"{len(export['dashboards']['dashboard-1']['filters'])} dashboard filters")
