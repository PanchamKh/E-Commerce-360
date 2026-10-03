"""
ims/producer.py
---------------
SYSTEM 4 — Inventory Management System (WMS / warehouse service).

Holds the only live stock ledger in the platform and reacts to what the OMS
does with orders. This is where the loop closes: stock depletes because real
orders were confirmed, and the OMS cancels orders because this system said the
SKU is gone.

  CONSUMES  orders-topic              ORDER_CONFIRMED  → reserve stock
            fulfillment-topic         ORDER_PACKED     → issue stock (physical pick)
            order-cancellations-topic → release reservation / restock returns

  PUBLISHES stock-movements-topic     every physical movement, with stock_after
            purchase-orders-topic     PO_RAISED → PO_APPROVED → GOODS_RECEIVED
            stock-alerts-topic        LOW_STOCK / OUT_OF_STOCK / DEFECT_CLUSTER

It also generates activity that depends on nothing else — cycle counts and
damage write-offs — because a warehouse does those on its own schedule.

Run:  python -m ims.producer --speed 4
"""

import argparse
import random
import uuid
from collections import defaultdict

from shared import catalog as cat
from shared.bus import make_bus
from shared.config import (T, LOW_STOCK_THRESHOLD, REORDER_POINT,
                           REORDER_QUANTITY, DEFECT_CLUSTER_THRESHOLD)
from shared.service import Service, alert_line, say


class WarehouseService(Service):
    system = "IMS"
    label = "WMS / Warehouse Service"
    subscribes = [T.ORDERS, T.FULFILLMENT, T.ORDER_CANCELLATIONS]
    group_id = "ims-warehouse-service"

    def __init__(self, bus, **kw):
        super().__init__(bus, **kw)
        # live ledger: (sku, warehouse) -> on_hand / reserved / on_order
        self.stock = {}
        for sku, info in cat.BY_SKU.items():
            for wh in cat.WAREHOUSES:
                self.stock[(sku, wh)] = {"on_hand": info["opening_stock"],
                                         "reserved": 0, "on_order": 0}
        self.open_pos = {}
        self.po_counter = 0
        self.defect_counts = defaultdict(int)
        self.alerted_out = set()
        self.alerted_low = set()

    def on_start(self):
        say(self.system, f"{self.label} started — {len(self.stock)} SKU/warehouse "
                         f"positions under management")
        self.later(random.uniform(4, 8), self.housekeeping)

    # -- helpers -----------------------------------------------------------
    def _movement(self, sku, wh, change, movement_type, **fields):
        pos = self.stock[(sku, wh)]
        info = cat.sku_info(sku)
        ev = {
            "event_id": str(uuid.uuid4()),
            "event_type": movement_type,
            "event_timestamp": cat.ts(),
            "minute_ist": cat.minute_bucket(),
            "sku": sku,
            "product_id": info["product_id"],
            "product_name": info["product_name"],
            "category": info["category"],
            "warehouse_id": wh,
            "quantity_change": change,
            "stock_on_hand": pos["on_hand"],
            "stock_reserved": pos["reserved"],
            "stock_on_order": pos["on_order"],
            "available": pos["on_hand"] - pos["reserved"],
            "unit_cost": round(info["unit_price"] * 0.62, 2),
            "source_system": "IMS",
        }
        ev.update(fields)
        self.emit(T.STOCK_MOVEMENTS, f"{sku}:{wh}", ev)
        self.check_thresholds(sku, wh)
        return ev

    def _alert(self, alert_type, severity, sku, wh, detail, **fields):
        self.alerts_raised += 1
        # the IMS consumer persists and announces this alert; here we only
        # note that it went onto the topic, so the console shows it once
        say(self.system, f"   → stock-alerts-topic: {alert_type} {sku}@{wh}")
        self.emit(T.STOCK_ALERTS, f"{sku}:{wh}", {
            "event_id": str(uuid.uuid4()),
            "event_type": "STOCK_ALERT",
            "event_timestamp": cat.ts(),
            "minute_ist": cat.minute_bucket(),
            "alert_type": alert_type,
            "severity": severity,
            "sku": sku,
            "category": cat.sku_info(sku)["category"],
            "warehouse_id": wh,
            "detail": detail,
            "source_system": "IMS",
            **fields,
        })

    def check_thresholds(self, sku, wh):
        pos = self.stock[(sku, wh)]
        available = pos["on_hand"] - pos["reserved"]
        key = (sku, wh)

        if available <= 0 and key not in self.alerted_out:
            self.alerted_out.add(key)
            self._alert("OUT_OF_STOCK", "HIGH", sku, wh,
                        f"{sku} at {wh} has no sellable stock — delist and "
                        f"cancel unshipped orders", available=available)
        elif 0 < available <= LOW_STOCK_THRESHOLD and key not in self.alerted_low:
            # fire once on the way down, not on every movement below the line
            self.alerted_low.add(key)
            self._alert("LOW_STOCK", "MEDIUM", sku, wh,
                        f"{sku} at {wh} down to {available} units — "
                        f"reorder cover is {REORDER_QUANTITY} units away",
                        available=available)
        elif available > LOW_STOCK_THRESHOLD and key in self.alerted_out:
            self.alerted_out.discard(key)
            self.alerted_low.discard(key)
            self._alert("BACK_IN_STOCK", "LOW", sku, wh,
                        f"{sku} at {wh} replenished to {available} units — "
                        f"safe to relist", available=available)

        if available <= REORDER_POINT and pos["on_order"] == 0:
            self.raise_po(sku, wh)

    # -- inbound from OMS --------------------------------------------------
    def on_message(self, topic, key, event):
        et = event.get("event_type")
        sku, wh, qty = event.get("sku"), event.get("warehouse_id"), event.get("quantity", 1)
        if not sku or (sku, wh) not in self.stock:
            return

        if et == "ORDER_CONFIRMED":
            self.stock[(sku, wh)]["reserved"] += qty
            self._movement(sku, wh, 0, "STOCK_RESERVED",
                           order_id=event.get("order_id"), quantity=qty)

        elif et == "ORDER_PACKED":
            pos = self.stock[(sku, wh)]
            pos["reserved"] = max(0, pos["reserved"] - qty)
            pos["on_hand"] = max(0, pos["on_hand"] - qty)
            self._movement(sku, wh, -qty, "STOCK_ISSUED",
                           order_id=event.get("order_id"), quantity=qty)

        elif et == "ORDER_CANCELLED":
            pos = self.stock[(sku, wh)]
            if pos["reserved"] >= qty:              # never shipped: release hold
                pos["reserved"] -= qty
                self._movement(sku, wh, 0, "RESERVATION_RELEASED",
                               order_id=event.get("order_id"), quantity=qty)
            else:                                    # already picked: back to shelf
                pos["on_hand"] += qty
                self._movement(sku, wh, qty, "STOCK_RESTOCKED_CANCEL",
                               order_id=event.get("order_id"), quantity=qty)

        elif et == "RETURN_RAISED":
            reason = event.get("return_reason", "")
            damaged = reason in ("Product damaged in transit", "Defective unit")
            if damaged:
                self._movement(sku, wh, 0, "RETURN_QUARANTINED",
                               order_id=event.get("order_id"), quantity=qty,
                               reason=reason)
                self.defect_counts[(sku, reason)] += 1
                n = self.defect_counts[(sku, reason)]
                if n >= DEFECT_CLUSTER_THRESHOLD:
                    self._alert("DEFECT_CLUSTER", "HIGH", sku, wh,
                                f"{sku} returned {n}× for '{reason}' — quarantine "
                                f"batch and halt sales pending QC", defect_count=n)
            else:
                self.stock[(sku, wh)]["on_hand"] += qty
                self._movement(sku, wh, qty, "STOCK_RESTOCKED_RETURN",
                               order_id=event.get("order_id"), quantity=qty,
                               reason=reason)

    # -- procurement -------------------------------------------------------
    def raise_po(self, sku, wh):
        self.po_counter += 1
        po_id = f"PO-{5000 + self.po_counter}"
        supplier_id, supplier_name = random.choice(cat.SUPPLIERS)
        info = cat.sku_info(sku)
        po = {"po_id": po_id, "sku": sku, "warehouse_id": wh,
              "quantity": REORDER_QUANTITY, "supplier_id": supplier_id,
              "supplier_name": supplier_name,
              "unit_cost": round(info["unit_price"] * 0.62, 2)}
        self.open_pos[po_id] = po
        self.stock[(sku, wh)]["on_order"] += REORDER_QUANTITY
        self.po_event(po, "PO_RAISED")
        # Procurement lead time is long relative to a checkout — that gap is
        # exactly why a stock-out cannot be fixed reactively, and why the
        # reorder alert has to fire before stock reaches zero.
        self.later(random.uniform(8, 16), lambda: self.po_event(po, "PO_APPROVED"))
        self.later(random.uniform(55, 110), lambda: self.goods_received(po))

    def po_event(self, po, status, **fields):
        info = cat.sku_info(po["sku"])
        self.emit(T.PURCHASE_ORDERS, po["po_id"], {
            "event_id": str(uuid.uuid4()),
            "event_type": status,
            "event_timestamp": cat.ts(),
            "minute_ist": cat.minute_bucket(),
            "po_id": po["po_id"], "sku": po["sku"],
            "category": info["category"],
            "warehouse_id": po["warehouse_id"],
            "supplier_id": po["supplier_id"], "supplier_name": po["supplier_name"],
            "quantity": po["quantity"], "unit_cost": po["unit_cost"],
            "po_value": round(po["quantity"] * po["unit_cost"], 2),
            "status": status,
            "source_system": "IMS",
            **fields,
        })

    def goods_received(self, po):
        sku, wh = po["sku"], po["warehouse_id"]
        # suppliers sometimes short-ship — that is what supplier fill rate measures
        received = po["quantity"] if random.random() < 0.8 else \
            int(po["quantity"] * random.uniform(0.6, 0.95))
        fill_rate = round(received / po["quantity"] * 100, 1)

        self.stock[(sku, wh)]["on_order"] = max(
            0, self.stock[(sku, wh)]["on_order"] - po["quantity"])
        self.stock[(sku, wh)]["on_hand"] += received

        self.po_event(po, "GOODS_RECEIVED", quantity_received=received,
                      fill_rate_pct=fill_rate)
        self._movement(sku, wh, received, "GOODS_RECEIPT",
                       po_id=po["po_id"], supplier_id=po["supplier_id"],
                       quantity=received, fill_rate_pct=fill_rate)
        self.po_event(po, "PO_CLOSED", quantity_received=received,
                      fill_rate_pct=fill_rate)
        self.open_pos.pop(po["po_id"], None)

    # -- independent warehouse activity ------------------------------------
    def housekeeping(self):
        roll = random.random()
        sku = random.choice(cat.SKUS)
        wh = random.choice(cat.WAREHOUSES)
        pos = self.stock[(sku, wh)]

        if roll < 0.5 and pos["on_hand"] > 3:          # cycle count correction
            delta = random.choice([-2, -1, 1])
            pos["on_hand"] = max(0, pos["on_hand"] + delta)
            self._movement(sku, wh, delta, "CYCLE_COUNT_ADJUSTMENT",
                           reason="Cycle count correction")
        elif roll < 0.75 and pos["on_hand"] > 2:       # damage write-off
            delta = -random.randint(1, 2)
            pos["on_hand"] = max(0, pos["on_hand"] + delta)
            self._movement(sku, wh, delta, "DAMAGE_WRITE_OFF",
                           reason=random.choice(cat.ADJUSTMENT_REASONS))
        else:                                           # inter-warehouse transfer
            other = random.choice([w for w in cat.WAREHOUSES if w != wh])
            qty = min(5, pos["on_hand"])
            if qty > 0:
                pos["on_hand"] -= qty
                self.stock[(sku, other)]["on_hand"] += qty
                transfer_id = f"TRF-{uuid.uuid4().hex[:8].upper()}"
                self._movement(sku, wh, -qty, "TRANSFER_OUT",
                               transfer_id=transfer_id, destination=other, quantity=qty)
                self._movement(sku, other, qty, "TRANSFER_IN",
                               transfer_id=transfer_id, origin=wh, quantity=qty)

        self.later(random.uniform(5, 11), self.housekeeping)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--speed", type=float, default=1.0)
    ap.add_argument("--bus", choices=["kafka", "memory"], default="kafka")
    args = ap.parse_args()

    bus = make_bus(args.bus, client_id="ims-warehouse-service")
    svc = WarehouseService(bus, speed=args.speed)
    svc.run()
    print("\n" + svc.summary())


if __name__ == "__main__":
    main()
