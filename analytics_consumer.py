"""
analytics/consumer.py
---------------------
SYSTEM 5 — Cross-system analytics consumer.

The four domain consumers each see only their own slice. This one subscribes to
ALL fourteen topics and produces the two collections that make the dashboard a
360° view rather than four dashboards stapled together:

    pipeline_events   every message from every topic, tagged with its source
                      system and a pre-computed minute bucket. Powers throughput,
                      latency and volume analysis across the whole platform.

    journey_orders    ONE document per order that stitches the four systems
                      together — the session that produced it (CLK), the order
                      itself (OMS), what the gateway did (PMS), and whether
                      stock was reserved and issued (IMS) — plus the derived
                      fields no single system could compute:

                          secs_browse_to_checkout   CLK  → OMS
                          secs_checkout_to_paid     OMS  → PMS
                          secs_paid_to_shipped      PMS  → IMS/OMS
                          secs_shipped_to_delivered OMS
                          secs_total                end to end
                          outcome                   DELIVERED / CANCELLED_PAYMENT /
                                                    CANCELLED_STOCKOUT / RETURNED /
                                                    IN_PROGRESS
                          lost_revenue              order value when not retained

Durations are computed in Python and stored as plain numbers, so the dashboard
needs no date parsing or calculated fields for them.

Run:  python -m analytics.consumer
"""

import argparse
from datetime import datetime

from shared.bus import make_bus
from shared.config import ALL_TOPICS, C, T, TOPIC_SYSTEM
from shared.consumer_base import PersistingConsumer, utcnow
from shared.store import make_store


def parse(ts):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except ValueError:
        return None


def secs(a, b):
    ta, tb = parse(a), parse(b)
    if ta and tb:
        return round((tb - ta).total_seconds(), 2)
    return None


class AnalyticsConsumer(PersistingConsumer):
    system = "ANL"
    label = "Cross-System Analytics Consumer"
    subscribes = ALL_TOPICS
    group_id = "analytics-360-group"

    def __init__(self, bus, store, **kw):
        super().__init__(bus, store, **kw)
        self.events = store.collection(C.PIPELINE_EVENTS)
        self.journey = store.collection(C.JOURNEY)
        self.session_start = {}      # session_id -> first event timestamp

    # ----------------------------------------------------------------------
    def on_message(self, topic, key, event):
        self.log_event(topic, event)
        self.update_journey(topic, event)

    # -- 1. unified pipeline log -------------------------------------------
    def log_event(self, topic, event):
        self.events.insert_one({
            "event_id": event.get("event_id"),
            "event_type": event.get("event_type"),
            "topic": topic,
            "source_system": TOPIC_SYSTEM.get(topic, event.get("source_system", "?")),
            "event_timestamp": event.get("event_timestamp"),
            "minute_ist": event.get("minute_ist"),
            "order_id": event.get("order_id"),
            "session_id": event.get("session_id"),
            "customer_id": event.get("customer_id"),
            "sku": event.get("sku"),
            "category": event.get("category"),
            "city": event.get("city"),
            "warehouse_id": event.get("warehouse_id"),
            "device": event.get("device"),
            "channel": event.get("channel"),
            "customer_segment": event.get("customer_segment"),
            "order_value": event.get("order_value"),
            "amount": event.get("amount"),
            "quantity": event.get("quantity"),
            "_ingested_at": utcnow(),
        })

    # -- 2. the order journey ----------------------------------------------
    def update_journey(self, topic, event):
        et = event.get("event_type")
        sid = event.get("session_id")

        # remember when each session first appeared, so browse→checkout is real
        if sid and sid not in self.session_start and event.get("event_timestamp"):
            self.session_start[sid] = event["event_timestamp"]

        oid = event.get("order_id")
        if not oid:
            return

        set_fields, inc_fields = {"order_id": oid, "last_updated": utcnow()}, {}

        # ---- CLK + OMS: the order is born -------------------------------
        if et == "ORDER_PLACED":
            started = self.session_start.get(sid)
            set_fields.update({
                "session_id": sid,
                "customer_id": event.get("customer_id"),
                "customer_segment": event.get("customer_segment"),
                "city": event.get("city"),
                "device": event.get("device"),
                "channel": event.get("channel"),
                "sku": event.get("sku"),
                "product_name": event.get("product_name"),
                "category": event.get("category"),
                "warehouse_id": event.get("warehouse_id"),
                "quantity": event.get("quantity"),
                "order_value": event.get("order_value"),
                "discount": event.get("discount"),
                "payment_method": event.get("payment_method"),
                "placed_at": event.get("event_timestamp"),
                "minute_ist": event.get("minute_ist"),
                "session_started_at": started,
                "secs_browse_to_checkout": secs(started, event.get("event_timestamp")),
                "outcome": "IN_PROGRESS",
                "stock_reserved": False,
                "stock_issued": False,
                "lost_revenue": 0.0,
                "retained_revenue": 0.0,
            })

        # ---- PMS: attempts and settlement --------------------------------
        elif topic == T.PAYMENT_TRANSACTIONS:
            inc_fields["payment_attempts"] = 1
            if event.get("transaction_status") in ("FAILED", "DECLINED"):
                inc_fields["payment_failures"] = 1
            set_fields.update({
                "payment_gateway": event.get("payment_gateway"),
                "is_fraud_ground_truth": bool(event.get("is_fraud")),
                "fraud_type_ground_truth": event.get("fraud_type"),
            })

        elif topic == T.PAYMENT_RESULTS:
            row = self.journey.find_one({"_id": oid}) or {}
            set_fields.update({
                "payment_result": event.get("result"),
                "paid_at": event.get("event_timestamp"),
                "secs_checkout_to_paid": secs(row.get("placed_at"),
                                              event.get("event_timestamp")),
            })

        # ---- IMS: did stock actually move? -------------------------------
        elif et == "STOCK_RESERVED":
            set_fields["stock_reserved"] = True
        elif et == "STOCK_ISSUED":
            set_fields["stock_issued"] = True

        # ---- OMS: fulfilment and final outcome ---------------------------
        elif et == "ORDER_CONFIRMED":
            set_fields["confirmed_at"] = event.get("event_timestamp")

        elif et == "ORDER_SHIPPED":
            row = self.journey.find_one({"_id": oid}) or {}
            set_fields.update({
                "shipped_at": event.get("event_timestamp"),
                "courier": event.get("courier"),
                "secs_paid_to_shipped": secs(row.get("paid_at"),
                                             event.get("event_timestamp")),
            })

        elif et == "ORDER_DELIVERED":
            row = self.journey.find_one({"_id": oid}) or {}
            delivered = event.get("event_timestamp")
            set_fields.update({
                "delivered_at": delivered,
                "outcome": "DELIVERED",
                "secs_shipped_to_delivered": secs(row.get("shipped_at"), delivered),
                "secs_total": secs(row.get("placed_at"), delivered),
                "retained_revenue": row.get("order_value", 0.0),
                "lost_revenue": 0.0,
            })

        elif et == "ORDER_CANCELLED":
            row = self.journey.find_one({"_id": oid}) or {}
            cause = event.get("cancellation_cause", "UNKNOWN")
            set_fields.update({
                "outcome": ("CANCELLED_STOCKOUT" if cause == "STOCK_OUT"
                            else "CANCELLED_PAYMENT"),
                "cancellation_cause": cause,
                "cancellation_reason": event.get("cancellation_reason"),
                "cancelled_at": event.get("event_timestamp"),
                "lost_revenue": row.get("order_value", 0.0),
                "retained_revenue": 0.0,
            })

        elif et == "RETURN_RAISED":
            row = self.journey.find_one({"_id": oid}) or {}
            set_fields.update({
                "outcome": "RETURNED",
                "return_reason": event.get("return_reason"),
                "returned_at": event.get("event_timestamp"),
                "lost_revenue": row.get("order_value", 0.0),
                "retained_revenue": 0.0,
            })

        update = {"$set": set_fields}
        if inc_fields:
            update["$inc"] = inc_fields
        self.journey.update_one({"_id": oid}, update, upsert=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bus", choices=["kafka", "memory"], default="kafka")
    ap.add_argument("--store", choices=["mongodb", "memory"], default="mongodb")
    ap.add_argument("--from-beginning", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    bus = make_bus(args.bus, client_id="analytics-consumer")
    store = make_store(args.store)
    svc = AnalyticsConsumer(bus, store, from_beginning=args.from_beginning)
    svc.verbose = args.verbose
    svc.run()


if __name__ == "__main__":
    main()
