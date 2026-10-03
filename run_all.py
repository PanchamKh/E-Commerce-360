"""
run_all.py
----------
Runs the whole platform.

Two modes:

  LIVE (default)   every service runs in its own thread against real Kafka and
                   real MongoDB. This is what the demo uses. You can equally run
                   the nine services in nine terminals — see the README — but one
                   command is easier to show.

      python run_all.py --rate 2 --speed 5

  OFFLINE          the same nine services, but wired to an in-process message bus
                   and an in-memory store. No Docker, no MongoDB, no network.
                   Used to prove the pipeline logic works, and to regenerate the
                   sample data committed to the repo.

      python run_all.py --offline --duration 90 --export sample_data

Ctrl+C prints a per-service summary and the collection counts.
"""

import argparse
import threading
import time

from analytics.consumer import AnalyticsConsumer
from clickstream.consumer import ClickstreamConsumer
from clickstream.producer import ClickstreamProducer
from ims.consumer import IMSConsumer
from ims.producer import WarehouseService
from oms.consumer import OMSConsumer
from oms.producer import OMSOrderService
from pms.consumer import PMSConsumer
from pms.producer import PaymentGatewayService
from shared.bus import make_bus
from shared.config import ALL_TOPICS, C
from shared.service import say
from shared.store import make_store

COLLECTIONS = [C.PIPELINE_EVENTS, C.JOURNEY, C.ALERTS,
               C.CLK_SESSIONS, C.CLK_CARTS, C.CLK_SEARCH_GAPS,
               C.OMS_ORDERS, C.OMS_RETURNS,
               C.PMS_TRANSACTIONS, C.PMS_PAYMENTS,
               C.IMS_STOCK, C.IMS_MOVEMENTS, C.IMS_PURCHASE_ORDERS]

BANNER = r"""
╔══════════════════════════════════════════════════════════════════════════╗
║   E-COMMERCE 360° — END-TO-END STREAMING ANALYTICS PLATFORM              ║
║                                                                          ║
║   CLK  Website Clickstream  →  checkout intent                           ║
║   OMS  Order Management     →  orchestrates the order                    ║
║   PMS  Payment Management   →  collects the money                        ║
║   IMS  Inventory Management →  moves the stock, and can stop the order   ║
╚══════════════════════════════════════════════════════════════════════════╝
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=2,
                    help="new shopper sessions per tick")
    ap.add_argument("--interval", type=float, default=1.0,
                    help="seconds between session batches")
    ap.add_argument("--speed", type=float, default=4.0,
                    help="lifecycle speed multiplier (higher = faster demo)")
    ap.add_argument("--offline", action="store_true",
                    help="in-memory bus and store; no Kafka or MongoDB needed")
    ap.add_argument("--duration", type=float, default=0,
                    help="stop automatically after N seconds (0 = run until Ctrl+C)")
    ap.add_argument("--export", type=str, default=None,
                    help="offline only: dump every collection to this directory")
    ap.add_argument("--verbose", action="store_true",
                    help="print a line for every persisted message")
    args = ap.parse_args()

    bus_mode = "memory" if args.offline else "kafka"
    store_mode = "memory" if args.offline else "mongodb"

    print(BANNER)
    if args.offline:
        print("🧪 OFFLINE MODE — in-process bus, in-memory store, no Docker required\n")

    bus = make_bus(bus_mode, client_id="platform")
    store = make_store(store_mode)

    # Consumers are created FIRST so their subscriptions exist before any
    # producer publishes — otherwise the first events would be missed.
    consumers = [
        AnalyticsConsumer(bus, store, speed=args.speed),
        ClickstreamConsumer(bus, store, speed=args.speed),
        OMSConsumer(bus, store, speed=args.speed),
        PMSConsumer(bus, store, speed=args.speed),
        IMSConsumer(bus, store, speed=args.speed),
    ]
    for c in consumers:
        c.verbose = args.verbose

    reactive = [
        OMSOrderService(bus, speed=args.speed),
        PaymentGatewayService(bus, speed=args.speed),
        WarehouseService(bus, speed=args.speed),
    ]
    clickstream = ClickstreamProducer(bus, sessions_per_tick=args.rate, speed=args.speed)

    services = consumers + reactive + [clickstream]
    threads = []
    for svc in consumers + reactive:
        t = threading.Thread(target=svc.run, name=svc.label, daemon=True)
        t.start()
        threads.append(t)

    clickstream.on_start()
    time.sleep(0.4)
    print("\n" + "─" * 100)
    say("ANL", f"platform live — {len(ALL_TOPICS)} topics, 9 services. Ctrl+C to stop.\n")

    started = time.time()
    try:
        while True:
            clickstream.generate()
            clickstream._run_due()
            if args.duration and time.time() - started > args.duration:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass

    # let in-flight lifecycles drain so orders reach a terminal state
    say("ANL", "draining in-flight orders…")
    drain_until = time.time() + 12
    while time.time() < drain_until:
        clickstream._run_due()
        time.sleep(0.2)

    for svc in services:
        svc.stop()
    time.sleep(0.5)

    elapsed = time.time() - started
    print("\n" + "═" * 100)
    print(f"📊 RUN SUMMARY — {elapsed:.0f}s elapsed\n")
    total_pub = sum(s.published for s in services)
    total_con = sum(s.consumed for s in services)
    total_alerts = sum(s.alerts_raised for s in services)
    for svc in services:
        if svc.published or svc.consumed:
            print(f"   {svc.system}  {svc.label:<34} "
                  f"published {svc.published:>6}  consumed {svc.consumed:>6}  "
                  f"alerts {svc.alerts_raised:>4}")
    print(f"\n   TOTAL published {total_pub}, consumed {total_con}, "
          f"alerts {total_alerts}")

    print("\n📦 Collections:")
    for name, n in store.counts(COLLECTIONS).items():
        print(f"   {name:<22} {n:>7} documents")

    if args.export and hasattr(store, "dump"):
        written = store.dump(args.export)
        print(f"\n💾 Sample data written to {args.export}/")
        for name, n in sorted(written.items()):
            print(f"   {name}.jsonl  ({n} docs)")

    bus.close()
    store.close()
    print("\n✅ Shutdown clean.")


if __name__ == "__main__":
    main()
