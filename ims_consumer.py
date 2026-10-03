"""
ims/consumer.py
---------------
SYSTEM 4 — Inventory Management System (consumer side).

Maintains the analytical view of stock:

    ims_stock            one document per SKU + warehouse: current on-hand,
                         reserved, available, plus the LOWEST available level
                         ever reached and the number of stock-out episodes
    ims_movements        every physical movement, for the audit trail
    ims_purchase_orders  one document per PO, from raised to closed, with the
                         supplier fill rate once goods are received

The "lowest available ever reached" field is the important one — a snapshot of
current stock hides the fact that a SKU hit zero an hour ago and cost you
orders. That number is what the dashboard ranks reorder priority on.

Run:  python -m ims.consumer
"""

import argparse

from shared.bus import make_bus
from shared.config import C, T
from shared.consumer_base import PersistingConsumer, utcnow
from shared.store import make_store

OUTBOUND = {"STOCK_ISSUED", "DAMAGE_WRITE_OFF", "TRANSFER_OUT"}
INBOUND = {"GOODS_RECEIPT", "STOCK_RESTOCKED_CANCEL",
           "STOCK_RESTOCKED_RETURN", "TRANSFER_IN"}


class IMSConsumer(PersistingConsumer):
    system = "IMS"
    label = "Stock Position Consumer"
    subscribes = [T.STOCK_MOVEMENTS, T.PURCHASE_ORDERS, T.STOCK_ALERTS]
    group_id = "ims-stock-position-group"

    def __init__(self, bus, store, **kw):
        super().__init__(bus, store, **kw)
        self.stock = store.collection(C.IMS_STOCK)
        self.movements = store.collection(C.IMS_MOVEMENTS)
        self.pos = store.collection(C.IMS_PURCHASE_ORDERS)

    # ----------------------------------------------------------------------
    def on_message(self, topic, key, event):
        self.trace(topic, event)
        if topic == T.STOCK_MOVEMENTS:
            self.handle_movement(event)
        elif topic == T.PURCHASE_ORDERS:
            self.handle_po(event)
        else:
            self.handle_stock_alert(event)

    # -- movements ---------------------------------------------------------
    def handle_movement(self, ev):
        self.movements.insert_one({**ev, "_saved_at": utcnow()})

        et = ev["event_type"]
        change = ev.get("quantity_change", 0)
        available = ev.get("available", 0)
        key = f"{ev['sku']}|{ev['warehouse_id']}"

        self.stock.update_one(
            {"_id": key},
            {"$set": {"sku": ev["sku"],
                      "product_name": ev.get("product_name"),
                      "category": ev.get("category"),
                      "warehouse_id": ev["warehouse_id"],
                      "on_hand": ev.get("stock_on_hand"),
                      "reserved": ev.get("stock_reserved"),
                      "on_order": ev.get("stock_on_order"),
                      "available": available,
                      "unit_cost": ev.get("unit_cost"),
                      "stock_value": round((ev.get("stock_on_hand") or 0)
                                           * (ev.get("unit_cost") or 0), 2),
                      "last_movement": et,
                      "last_updated": utcnow(),
                      "minute_ist": ev.get("minute_ist")},
             "$inc": {"movements": 1,
                      "units_out": abs(change) if et in OUTBOUND else 0,
                      "units_in": abs(change) if et in INBOUND else 0,
                      "stockout_episodes": int(available <= 0 and et in OUTBOUND),
                      "damage_units": abs(change) if et == "DAMAGE_WRITE_OFF" else 0},
             "$setOnInsert": {"lowest_available": available}},
            upsert=True)

        # track the worst point this SKU reached, not just where it is now
        row = self.stock.find_one({"_id": key})
        if row is not None and available < row.get("lowest_available", available):
            self.stock.update_one({"_id": key},
                                  {"$set": {"lowest_available": available}})

    # -- purchase orders ----------------------------------------------------
    def handle_po(self, ev):
        update = {"po_id": ev["po_id"], "sku": ev["sku"],
                  "category": ev.get("category"),
                  "warehouse_id": ev["warehouse_id"],
                  "supplier_id": ev["supplier_id"],
                  "supplier_name": ev.get("supplier_name"),
                  "quantity_ordered": ev["quantity"],
                  "unit_cost": ev["unit_cost"],
                  "po_value": ev.get("po_value"),
                  "status": ev["status"],
                  "last_updated": utcnow(),
                  "minute_ist": ev.get("minute_ist")}

        if ev["status"] == "PO_RAISED":
            update["raised_at"] = ev["event_timestamp"]
        if ev["status"] == "GOODS_RECEIVED":
            update.update({"received_at": ev["event_timestamp"],
                           "quantity_received": ev.get("quantity_received"),
                           "fill_rate_pct": ev.get("fill_rate_pct"),
                           "short_shipped_units": ev["quantity"] - ev.get("quantity_received", 0)})
            if ev.get("fill_rate_pct", 100) < 100:
                self.alert("SUPPLIER_SHORT_SHIP", "MEDIUM", ev,
                           f"{ev.get('supplier_name')} filled only "
                           f"{ev.get('fill_rate_pct')}% of {ev['po_id']} "
                           f"({ev['sku']}) — reorder cover is smaller than planned",
                           supplier_id=ev["supplier_id"],
                           fill_rate_pct=ev.get("fill_rate_pct"))

        self.pos.update_one({"_id": ev["po_id"]}, {"$set": update}, upsert=True)

    # -- alerts published by the warehouse service --------------------------
    def handle_stock_alert(self, ev):
        self.alert(ev["alert_type"], ev.get("severity", "MEDIUM"), ev,
                   ev.get("detail", ""), available=ev.get("available"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bus", choices=["kafka", "memory"], default="kafka")
    ap.add_argument("--store", choices=["mongodb", "memory"], default="mongodb")
    ap.add_argument("--from-beginning", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    bus = make_bus(args.bus, client_id="ims-consumer")
    store = make_store(args.store)
    svc = IMSConsumer(bus, store, from_beginning=args.from_beginning)
    svc.verbose = args.verbose
    svc.run()


if __name__ == "__main__":
    main()
