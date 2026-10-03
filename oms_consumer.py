"""
oms/consumer.py
---------------
SYSTEM 2 — Order Management System (consumer side).

Maintains one document per order in `oms_orders`, updated in place as the order
moves through its lifecycle, plus a `oms_returns` record for every cancellation
and return.

Because the OMS is the orchestrator, this is also where the cancellation CAUSE
is recorded — payment failure versus stock-out — which is what lets the
dashboard attribute lost revenue to the system responsible for it.

Alerts raised: HIGH_VALUE_ORDER, ORDER_CANCELLED_STOCKOUT, RETURN_DAMAGED.

Run:  python -m oms.consumer
"""

import argparse

from shared.bus import make_bus
from shared.config import C, T, HIGH_VALUE_ORDER
from shared.consumer_base import PersistingConsumer, utcnow
from shared.store import make_store


class OMSConsumer(PersistingConsumer):
    system = "OMS"
    label = "Order & Fulfillment Consumer"
    subscribes = [T.ORDERS, T.FULFILLMENT, T.ORDER_CANCELLATIONS]
    group_id = "oms-order-fulfillment-group"

    def __init__(self, bus, store, **kw):
        super().__init__(bus, store, **kw)
        self.orders = store.collection(C.OMS_ORDERS)
        self.returns = store.collection(C.OMS_RETURNS)

    def on_message(self, topic, key, event):
        self.trace(topic, event)
        et = event["event_type"]
        oid = event.get("order_id")
        if not oid:
            return

        update = {"last_event": et, "last_updated": utcnow(),
                  "last_minute_ist": event.get("minute_ist")}

        if et == "ORDER_PLACED":
            update.update({
                "order_id": oid,
                "session_id": event.get("session_id"),
                "customer_id": event.get("customer_id"),
                "customer_segment": event.get("customer_segment"),
                "city": event.get("city"),
                "device": event.get("device"),
                "channel": event.get("channel"),
                "sku": event.get("sku"),
                "product_name": event.get("product_name"),
                "category": event.get("category"),
                "quantity": event.get("quantity"),
                "unit_price": event.get("unit_price"),
                "subtotal": event.get("subtotal"),
                "discount": event.get("discount"),
                "shipping_charges": event.get("shipping_charges"),
                "order_value": event.get("order_value"),
                "payment_method": event.get("payment_method"),
                "warehouse_id": event.get("warehouse_id"),
                "order_status": "PLACED",
                "placed_at": event.get("event_timestamp"),
                "placed_minute_ist": event.get("minute_ist"),
            })
            if event.get("order_value", 0) > HIGH_VALUE_ORDER:
                self.alert("HIGH_VALUE_ORDER", "LOW", event,
                           f"{oid} at ₹{event['order_value']:,.0f} exceeds the "
                           f"₹{HIGH_VALUE_ORDER:,.0f} manual-review threshold",
                           order_value=event.get("order_value"))

        elif et == "ORDER_CONFIRMED":
            update.update({"order_status": "CONFIRMED",
                           "confirmed_at": event.get("event_timestamp"),
                           "payment_attempts": event.get("payment_attempts"),
                           "transaction_id": event.get("transaction_id")})

        elif et in ("ORDER_PROCESSING", "ORDER_PACKED",
                    "ORDER_SHIPPED", "ORDER_DELIVERED"):
            stage = event["fulfillment_status"]
            update["order_status"] = stage
            if stage == "SHIPPED":
                update.update({"shipped_at": event.get("event_timestamp"),
                               "tracking_id": event.get("tracking_id"),
                               "courier": event.get("courier")})
            if stage == "DELIVERED":
                update["delivered_at"] = event.get("event_timestamp")

        elif et == "ORDER_CANCELLED":
            cause = event.get("cancellation_cause", "UNKNOWN")
            update.update({"order_status": "CANCELLED",
                           "cancellation_reason": event.get("cancellation_reason"),
                           "cancellation_cause": cause,
                           "cancelled_at": event.get("event_timestamp")})
            self.returns.insert_one({**event, "record_type": "CANCELLATION",
                                     "_saved_at": utcnow()})
            if cause == "STOCK_OUT":
                self.alert("ORDER_CANCELLED_STOCKOUT", "HIGH", event,
                           f"{oid} (₹{event.get('order_value', 0):,.0f}) cancelled "
                           f"because {event.get('sku')} ran out — demand existed, "
                           f"stock did not",
                           order_value=event.get("order_value"))

        elif et == "RETURN_RAISED":
            reason = event.get("return_reason")
            update.update({"order_status": "RETURN_RAISED",
                           "return_reason": reason,
                           "returned_at": event.get("event_timestamp")})
            self.returns.insert_one({**event, "record_type": "RETURN",
                                     "_saved_at": utcnow()})
            if reason in ("Product damaged in transit", "Defective unit"):
                self.alert("RETURN_DAMAGED", "MEDIUM", event,
                           f"{event.get('sku')} returned as '{reason}' — check the "
                           f"batch before it ships again")

        self.orders.update_one({"_id": oid}, {"$set": update}, upsert=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bus", choices=["kafka", "memory"], default="kafka")
    ap.add_argument("--store", choices=["mongodb", "memory"], default="mongodb")
    ap.add_argument("--from-beginning", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    bus = make_bus(args.bus, client_id="oms-consumer")
    store = make_store(args.store)
    svc = OMSConsumer(bus, store, from_beginning=args.from_beginning)
    svc.verbose = args.verbose
    svc.run()


if __name__ == "__main__":
    main()
