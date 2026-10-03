"""
clickstream/consumer.py
-----------------------
SYSTEM 1 — Website Clickstream System (consumer side).

Subscribes to all five clickstream topics and maintains:

    clk_sessions      one document per session, with the furthest funnel stage
                      reached (view → search → click → cart → checkout) and
                      sticky boolean flags for each step
    clk_search_gaps   running count of search terms that returned zero results
    clk_carts         cart state per session: active / emptied / checked_out /
                      abandoned (a background watcher ages idle carts out)

Alerts raised: SEARCH_GAP_DETECTED, CART_ABANDONED, REPEAT_ABANDONER.

Run:  python -m clickstream.consumer
"""

import argparse
from collections import defaultdict
from datetime import timedelta

from shared.bus import make_bus
from shared.config import (C, T, ZERO_RESULT_ALERT_THRESHOLD,
                           CART_ABANDON_SECONDS, REPEAT_ABANDONER_THRESHOLD)
from shared.consumer_base import PersistingConsumer, utcnow
from shared.store import make_store

STAGE_ORDER = ["view", "search", "click", "cart", "checkout"]
STAGE_MAP = {
    "page_view": "view", "search_query": "search", "click": "click",
    "add_to_cart": "cart", "remove_from_cart": "cart",
    "checkout_requested": "checkout",
}


class ClickstreamConsumer(PersistingConsumer):
    system = "CLK"
    label = "Session & Funnel Consumer"
    subscribes = [T.PAGE_VIEWS, T.SEARCH_QUERIES, T.CLICK_EVENTS,
                  T.CART_EVENTS, T.CHECKOUT_REQUESTS]
    group_id = "clk-session-funnel-group"

    def __init__(self, bus, store, **kw):
        super().__init__(bus, store, **kw)
        self.sessions = store.collection(C.CLK_SESSIONS)
        self.search_gaps = store.collection(C.CLK_SEARCH_GAPS)
        self.carts = store.collection(C.CLK_CARTS)
        self.abandon_counts = defaultdict(int)
        self._next_sweep = 0

    # ----------------------------------------------------------------------
    def on_message(self, topic, key, event):
        self.trace(topic, event)
        self.update_session(event)
        et = event["event_type"]
        if et == "search_query":
            self.check_search(event)
        elif et in ("add_to_cart", "remove_from_cart", "checkout_requested"):
            self.update_cart(event)

    # -- funnel ------------------------------------------------------------
    def update_session(self, event):
        sid = event["session_id"]
        et = event["event_type"]
        stage = STAGE_MAP.get(et, "view")

        existing = self.sessions.find_one({"_id": sid})
        if existing and STAGE_ORDER.index(existing.get("funnel_stage", "view")) > \
                STAGE_ORDER.index(stage):
            stage = existing["funnel_stage"]          # never move the funnel backwards

        self.sessions.update_one(
            {"_id": sid},
            {"$set": {"session_id": sid,
                      "customer_id": event.get("customer_id"),
                      "city": event.get("city"),
                      "device": event.get("device"),
                      "channel": event.get("channel"),
                      "customer_segment": event.get("customer_segment"),
                      "funnel_stage": stage,
                      "last_seen_at": utcnow(),
                      "last_minute_ist": event.get("minute_ist")},
             "$inc": {"event_count": 1},
             "$max": {"reached_view": 1,
                      "reached_search": int(et == "search_query"),
                      "reached_click": int(et == "click"),
                      "reached_cart": int(et == "add_to_cart"),
                      "reached_checkout": int(et == "checkout_requested")},
             "$setOnInsert": {"first_seen_at": utcnow(),
                              "first_minute_ist": event.get("minute_ist")}},
            upsert=True)

    # -- search gaps -------------------------------------------------------
    def check_search(self, event):
        if event.get("results_count", 1) != 0:
            return
        term = event["search_term"]
        self.search_gaps.update_one(
            {"_id": term},
            {"$inc": {"zero_result_count": 1},
             "$set": {"search_term": term, "last_seen_at": utcnow()}},
            upsert=True)
        row = self.search_gaps.find_one({"_id": term})
        if row and row["zero_result_count"] == ZERO_RESULT_ALERT_THRESHOLD:
            self.alert("SEARCH_GAP_DETECTED", "MEDIUM", event,
                       f"\"{term}\" returned zero results "
                       f"{row['zero_result_count']}× — demand with no catalog cover",
                       search_term=term)

    # -- carts -------------------------------------------------------------
    def update_cart(self, event):
        sid = event["session_id"]
        et = event["event_type"]
        if et == "add_to_cart":
            self.carts.update_one(
                {"_id": sid},
                {"$set": {"session_id": sid,
                          "customer_id": event.get("customer_id"),
                          "sku": event.get("sku"),
                          "product_name": event.get("product_name"),
                          "category": event.get("category"),
                          "quantity": event.get("quantity"),
                          "cart_value": event.get("cart_value"),
                          "city": event.get("city"),
                          "device": event.get("device"),
                          "status": "active_cart",
                          "updated_at": utcnow(),
                          "minute_ist": event.get("minute_ist")}},
                upsert=True)
        elif et == "remove_from_cart":
            self.carts.update_one({"_id": sid},
                                  {"$set": {"status": "emptied", "updated_at": utcnow()}})
        elif et == "checkout_requested":
            self.carts.update_one({"_id": sid},
                                  {"$set": {"status": "checked_out",
                                            "updated_at": utcnow()}}, upsert=True)

    # -- abandonment watcher ------------------------------------------------
    def on_tick(self):
        import time
        if time.time() < self._next_sweep:
            return
        self._next_sweep = time.time() + 5
        cutoff = utcnow() - timedelta(seconds=CART_ABANDON_SECONDS / self.speed)
        for cart in self.carts.find({"status": "active_cart"}):
            updated = cart.get("updated_at")
            if not updated or updated >= cutoff:
                continue
            self.carts.update_one({"_id": cart["_id"]},
                                  {"$set": {"status": "abandoned",
                                            "abandoned_at": utcnow()}})
            uid = cart.get("customer_id")
            self.abandon_counts[uid] += 1
            self.alert("CART_ABANDONED", "LOW", cart,
                       f"cart worth ₹{cart.get('cart_value', 0):,.0f} "
                       f"({cart.get('product_name')}) idle — trigger retargeting",
                       cart_value=cart.get("cart_value"),
                       discount_suggestion_pct=10)
            if self.abandon_counts[uid] == REPEAT_ABANDONER_THRESHOLD:
                self.alert("REPEAT_ABANDONER", "HIGH", cart,
                           f"{uid} abandoned {self.abandon_counts[uid]} carts — "
                           f"price sensitivity, not intent; escalate the offer")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bus", choices=["kafka", "memory"], default="kafka")
    ap.add_argument("--store", choices=["mongodb", "memory"], default="mongodb")
    ap.add_argument("--from-beginning", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    bus = make_bus(args.bus, client_id="clk-consumer")
    store = make_store(args.store)
    svc = ClickstreamConsumer(bus, store, from_beginning=args.from_beginning)
    svc.verbose = args.verbose
    svc.run()


if __name__ == "__main__":
    main()
