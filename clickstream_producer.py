"""
clickstream/producer.py
-----------------------
SYSTEM 1 — Website Clickstream System (producer side).

Simulates live shopper traffic on the storefront. Each session is a small state
machine: land → browse → (maybe search) → click a product → (maybe add to cart)
→ (maybe check out). Steps are spread over real elapsed time, so at any moment
hundreds of sessions are at different stages.

Publishes to:
    page-views-topic        every page a session lands on
    search-queries-topic    search terms and their result counts
    click-events-topic      product / banner / filter clicks
    cart-events-topic       add_to_cart and remove_from_cart
    checkout-requests-topic CHECKOUT INTENT — this is the handoff to the OMS

The last topic is the integration point: this system does not create orders, it
only says "a shopper wants to buy". The OMS decides what happens next.

Run:  python -m clickstream.producer --rate 2 --speed 4
"""

import argparse
import random
import uuid

from shared import catalog as cat
from shared.bus import make_bus
from shared.config import T
from shared.service import Service, say

PAGE_TYPES = ["home", "category", "product", "search_results", "cart", "checkout"]


class ClickstreamProducer(Service):
    system = "CLK"
    label = "Web/App Tracker Producer"

    def __init__(self, bus, sessions_per_tick=2, **kw):
        super().__init__(bus, **kw)
        self.customers = cat.Customers()
        self.sessions_per_tick = sessions_per_tick
        self.session_count = 0

    def on_start(self):
        say(self.system, f"{self.label} started — simulating live storefront traffic")

    # -- event envelope ----------------------------------------------------
    def _event(self, session, event_type, **fields):
        ev = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_timestamp": cat.ts(),
            "minute_ist": cat.minute_bucket(),
            "session_id": session["session_id"],
            "customer_id": session["customer_id"],
            "device": session["device"],
            "channel": session["channel"],
            "city": session["city"],
            "customer_segment": session["segment"],
            "source_system": "CLK",
        }
        ev.update(fields)
        return ev

    # -- session lifecycle -------------------------------------------------
    def new_session(self):
        person = self.customers.pick()
        self.session_count += 1
        session = {
            "session_id": f"SESS-{uuid.uuid4().hex[:12]}",
            "customer_id": person["customer_id"],
            "city": person["city"],
            "segment": person["segment"],
            "device": person["device"],
            "channel": person["channel"],
            "sku": cat.pick_sku(),
            "depth": 0,
        }
        self.page_view(session, "home")
        self.later(random.uniform(1.5, 4), lambda: self.step_browse(session))

    def page_view(self, session, page_type, ref=None):
        sku = session["sku"]
        info = cat.sku_info(sku)
        self.emit(T.PAGE_VIEWS, session["session_id"],
                  self._event(session, "page_view",
                              page_type=page_type,
                              page_ref=ref or info["category"],
                              category=info["category"]))

    def step_browse(self, session):
        """Search or go straight to a category page."""
        if random.random() < 0.45:
            self.do_search(session)
        else:
            self.page_view(session, "category")
        self.later(random.uniform(1.5, 4), lambda: self.step_product(session))

    def do_search(self, session):
        # 18% of searches use a term the catalog cannot satisfy — this is what
        # the consumer's SEARCH_GAP_DETECTED rule looks for.
        if random.random() < 0.18:
            term, results = random.choice(cat.NO_RESULT_TERMS), 0
        else:
            term, results = random.choice(cat.SEARCH_TERMS), random.randint(3, 40)
        self.emit(T.SEARCH_QUERIES, session["session_id"],
                  self._event(session, "search_query",
                              search_term=term, results_count=results))
        self.page_view(session, "search_results", ref=term)

    def step_product(self, session):
        info = cat.sku_info(session["sku"])
        self.emit(T.CLICK_EVENTS, session["session_id"],
                  self._event(session, "click",
                              click_target=random.choice(
                                  ["product_card", "banner", "filter", "recommendation"]),
                              product_id=info["product_id"], sku=info["sku"],
                              product_name=info["product_name"],
                              category=info["category"]))
        self.page_view(session, "product", ref=info["product_name"])
        # ~38% of product views become a cart add
        if random.random() < 0.38:
            self.later(random.uniform(1, 3), lambda: self.step_add_to_cart(session))

    def step_add_to_cart(self, session):
        info = cat.sku_info(session["sku"])
        qty = random.choices([1, 2, 3], weights=[72, 21, 7])[0]
        session["quantity"] = qty
        self.emit(T.CART_EVENTS, session["session_id"],
                  self._event(session, "add_to_cart",
                              product_id=info["product_id"], sku=info["sku"],
                              product_name=info["product_name"],
                              category=info["category"],
                              quantity=qty, unit_price=info["unit_price"],
                              cart_value=round(info["unit_price"] * qty, 2)))
        roll = random.random()
        if roll < 0.10:                       # removed from cart
            self.later(random.uniform(2, 5), lambda: self.step_remove(session))
        elif roll < 0.62:                     # proceeds to checkout
            self.later(random.uniform(2, 6), lambda: self.step_checkout(session))
        # the rest simply go quiet — the consumer's watcher calls those abandoned

    def step_remove(self, session):
        info = cat.sku_info(session["sku"])
        self.emit(T.CART_EVENTS, session["session_id"],
                  self._event(session, "remove_from_cart",
                              product_id=info["product_id"], sku=info["sku"],
                              product_name=info["product_name"],
                              category=info["category"],
                              quantity=session.get("quantity", 1),
                              unit_price=info["unit_price"]))

    def step_checkout(self, session):
        """The handoff to the OMS. No order exists yet — this is intent only."""
        info = cat.sku_info(session["sku"])
        qty = session.get("quantity", 1)
        subtotal = round(info["unit_price"] * qty, 2)
        discount = round(subtotal * random.choice([0, 0, 0, 0.05, 0.10, 0.15]), 2)
        shipping = 0.0 if subtotal > 999 else 49.0
        order_value = round(subtotal - discount + shipping, 2)

        self.page_view(session, "checkout")
        self.emit(T.CHECKOUT_REQUESTS, session["session_id"],
                  self._event(session, "checkout_requested",
                              product_id=info["product_id"], sku=info["sku"],
                              product_name=info["product_name"],
                              category=info["category"],
                              quantity=qty, unit_price=info["unit_price"],
                              subtotal=subtotal, discount=discount,
                              shipping_charges=shipping,
                              order_value=order_value,
                              payment_method=random.choice(cat.PAYMENT_METHODS)))

    def on_tick(self):
        pass

    def generate(self):
        for _ in range(self.sessions_per_tick):
            self.new_session()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=2, help="new sessions per tick")
    ap.add_argument("--interval", type=float, default=1.0, help="seconds per tick")
    ap.add_argument("--speed", type=float, default=1.0, help="lifecycle speed multiplier")
    ap.add_argument("--bus", choices=["kafka", "memory"], default="kafka")
    args = ap.parse_args()

    import time
    bus = make_bus(args.bus, client_id="clickstream-producer")
    svc = ClickstreamProducer(bus, sessions_per_tick=args.rate, speed=args.speed)
    svc.on_start()
    try:
        while True:
            svc.generate()
            svc._run_due()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        say("CLK", "stopping…")
    finally:
        svc.stop()
        print("\n" + svc.summary())


if __name__ == "__main__":
    main()
