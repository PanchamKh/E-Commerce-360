"""
oms/producer.py
---------------
SYSTEM 2 — Order Management System (order service).

This is the orchestrator of the whole platform. It owns no shopper traffic and
no payment logic; it reacts to what the other systems tell it:

  CONSUMES  checkout-requests-topic  (from CLK) → creates the order
            payment-results-topic    (from PMS) → confirms or cancels it
            stock-alerts-topic       (from IMS) → cancels orders it cannot ship

  PUBLISHES orders-topic             ORDER_PLACED / ORDER_CONFIRMED
            payment-requests-topic   asks PMS to collect money
            fulfillment-topic        PROCESSING → PACKED → SHIPPED → DELIVERED
            order-cancellations-topic ORDER_CANCELLED / RETURN_RAISED

The cross-system cancellation is the important bit: when the IMS declares a SKU
out of stock, this service cancels the orders for that SKU that have not shipped
yet — a decision no single system could make on its own.

Run:  python -m oms.producer --speed 4
"""

import argparse
import random
import uuid

from shared import catalog as cat
from shared.bus import make_bus
from shared.config import T
from shared.service import Service, say


class OMSOrderService(Service):
    system = "OMS"
    label = "OMS Order Service"
    subscribes = [T.CHECKOUT_REQUESTS, T.PAYMENT_RESULTS, T.STOCK_ALERTS]
    group_id = "oms-order-service"

    def __init__(self, bus, **kw):
        super().__init__(bus, **kw)
        self.orders = {}            # order_id -> order state
        self.open_by_sku = {}       # sku -> set(order_id) not yet shipped
        self.blocked_skus = set()   # SKUs the IMS has declared out of stock
        self.counter = 0
        self.rejected_demand = 0

    def on_start(self):
        say(self.system, f"{self.label} started — listening for checkout requests")

    # -- helpers -----------------------------------------------------------
    def _event(self, order, event_type, **fields):
        ev = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_timestamp": cat.ts(),
            "minute_ist": cat.minute_bucket(),
            "order_id": order["order_id"],
            "session_id": order["session_id"],
            "customer_id": order["customer_id"],
            "sku": order["sku"],
            "category": order["category"],
            "city": order["city"],
            "warehouse_id": order["warehouse_id"],
            "quantity": order["quantity"],
            "order_value": order["order_value"],
            "source_system": "OMS",
        }
        ev.update(fields)
        return ev

    def _track(self, order):
        self.open_by_sku.setdefault(order["sku"], set()).add(order["order_id"])

    def _untrack(self, order):
        self.open_by_sku.get(order["sku"], set()).discard(order["order_id"])

    # -- inbound -----------------------------------------------------------
    def on_message(self, topic, key, event):
        if topic == T.CHECKOUT_REQUESTS:
            self.create_order(event)
        elif topic == T.PAYMENT_RESULTS:
            self.handle_payment_result(event)
        elif topic == T.STOCK_ALERTS:
            self.handle_stock_alert(event)

    # -- 1. checkout request → order --------------------------------------
    def create_order(self, req):
        self.counter += 1
        order_id = f"ORD-2026-{700000 + self.counter}"
        order = {
            "order_id": order_id,
            "session_id": req["session_id"],
            "customer_id": req["customer_id"],
            "customer_segment": req.get("customer_segment"),
            "city": req["city"],
            "device": req.get("device"),
            "channel": req.get("channel"),
            "sku": req["sku"],
            "product_name": req["product_name"],
            "category": req["category"],
            "quantity": req["quantity"],
            "unit_price": req["unit_price"],
            "subtotal": req["subtotal"],
            "discount": req["discount"],
            "shipping_charges": req["shipping_charges"],
            "order_value": req["order_value"],
            "payment_method": req["payment_method"],
            "warehouse_id": cat.warehouse_for(req["city"]),
            "status": "PLACED",
        }
        self.orders[order_id] = order
        self._track(order)

        self.emit(T.ORDERS, order_id, self._event(
            order, "ORDER_PLACED",
            order_status="PLACED",
            product_name=order["product_name"],
            customer_segment=order["customer_segment"],
            device=order["device"], channel=order["channel"],
            unit_price=order["unit_price"], subtotal=order["subtotal"],
            discount=order["discount"], shipping_charges=order["shipping_charges"],
            payment_method=order["payment_method"]))

        # STOCK GATE: the IMS has delisted this SKU. The demand is real and we
        # record it, but the order cannot be honoured — so it is cancelled
        # immediately instead of taking the customer's money for something that
        # cannot ship. This is the cross-system decision no single system could
        # make on its own.
        if order["sku"] in self.blocked_skus:
            self.rejected_demand += 1
            self.cancel(order, "Item went out of stock", "STOCK_OUT")
            return

        # ask the payment system to collect
        self.emit(T.PAYMENT_REQUESTS, order_id, self._event(
            order, "PAYMENT_REQUESTED",
            amount=order["order_value"],
            payment_method=order["payment_method"],
            customer_segment=order["customer_segment"],
            device=order["device"]))

    # -- 2. payment result → confirm or cancel ----------------------------
    def handle_payment_result(self, res):
        order = self.orders.get(res["order_id"])
        if not order or order["status"] != "PLACED":
            return

        if res["result"] == "AUTHORISED":
            order["status"] = "CONFIRMED"
            order["payment_attempts"] = res.get("attempts", 1)
            self.emit(T.ORDERS, order["order_id"], self._event(
                order, "ORDER_CONFIRMED",
                order_status="CONFIRMED",
                transaction_id=res.get("transaction_id"),
                payment_attempts=res.get("attempts", 1),
                payment_method=order["payment_method"]))
            self.later(random.uniform(3, 7), lambda: self.stage(order, "PROCESSING"))
        else:
            self.cancel(order, "Payment could not be completed", "PAYMENT_FAILED")

    # -- 3. stock alert → cancel what cannot ship -------------------------
    def handle_stock_alert(self, alert):
        kind = alert.get("alert_type")
        sku = alert.get("sku")

        if kind == "BACK_IN_STOCK":
            self.blocked_skus.discard(sku)          # relist it
            return
        if kind != "OUT_OF_STOCK":
            return

        self.blocked_skus.add(sku)                  # delist it
        blocked = [self.orders[o] for o in list(self.open_by_sku.get(sku, set()))
                   if self.orders.get(o, {}).get("status") in ("PLACED", "CONFIRMED", "PROCESSING")]
        for order in blocked[:2]:      # cancel the oldest couple, not the whole book
            self.cancel(order, "Item went out of stock", "STOCK_OUT")

    # -- fulfilment --------------------------------------------------------
    STAGES = ["PROCESSING", "PACKED", "SHIPPED", "DELIVERED"]

    def stage(self, order, stage):
        if order["status"] in ("CANCELLED",):
            return
        order["status"] = stage
        extra = {}
        if stage == "SHIPPED":
            order["tracking_id"] = f"TRK-{uuid.uuid4().hex[:10].upper()}"
            extra = {"tracking_id": order["tracking_id"],
                     "courier": random.choice(["Delhivery", "Blue Dart", "Ekart", "XpressBees"])}
        self.emit(T.FULFILLMENT, order["order_id"], self._event(
            order, f"ORDER_{stage}", fulfillment_status=stage, **extra))

        idx = self.STAGES.index(stage)
        if idx + 1 < len(self.STAGES):
            self.later(random.uniform(4, 10),
                       lambda: self.stage(order, self.STAGES[idx + 1]))
        else:
            self._untrack(order)
            if random.random() < 0.09:      # ~9% of delivered orders come back
                self.later(random.uniform(6, 14), lambda: self.raise_return(order))

    # -- cancellations and returns ----------------------------------------
    def cancel(self, order, reason, cause):
        if order["status"] == "CANCELLED":
            return
        order["status"] = "CANCELLED"
        self._untrack(order)
        self.emit(T.ORDER_CANCELLATIONS, order["order_id"], self._event(
            order, "ORDER_CANCELLED",
            cancellation_reason=reason, cancellation_cause=cause,
            refund_amount=order["order_value"], refund_status="INITIATED"))
        say(self.system, f"   ✖ {order['order_id']} cancelled — {reason}")

    def raise_return(self, order):
        order["status"] = "RETURN_RAISED"
        self.emit(T.ORDER_CANCELLATIONS, order["order_id"], self._event(
            order, "RETURN_RAISED",
            return_reason=random.choice(cat.RETURN_REASONS),
            refund_amount=order["order_value"], refund_status="PENDING",
            product_name=order["product_name"]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--speed", type=float, default=1.0)
    ap.add_argument("--bus", choices=["kafka", "memory"], default="kafka")
    args = ap.parse_args()

    bus = make_bus(args.bus, client_id="oms-order-service")
    svc = OMSOrderService(bus, speed=args.speed)
    svc.run()
    print("\n" + svc.summary())


if __name__ == "__main__":
    main()
