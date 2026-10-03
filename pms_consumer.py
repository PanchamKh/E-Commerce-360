"""
pms/consumer.py
---------------
SYSTEM 3 — Payment Management System (consumer side).

Stores every attempt in `pms_transactions` and rolls them up into one document
per order in `pms_payments` (attempts, final status, total value).

The fraud rules here are DETECTION rules — they look only at the observable
behaviour of the stream. Each transaction also carries a ground-truth
`is_fraud` flag from the producer, which the transaction is never allowed to
influence. Keeping the two separate is what lets the dashboard measure
precision and recall instead of just counting alerts.

Detection rules:
    RULE_HIGH_VALUE        single attempt above the high-value threshold
    RULE_REPEATED_FAILURE  3+ failed attempts on one order
    RULE_CARD_TESTING      failure(s) followed by a success on the same order
    RULE_METHOD_CYCLING    attempts on one order use 3+ different methods

Run:  python -m pms.consumer
"""

import argparse
from collections import defaultdict

from shared.bus import make_bus
from shared.config import C, T, HIGH_VALUE_FRAUD_MIN
from shared.consumer_base import PersistingConsumer, utcnow
from shared.store import make_store

REPEATED_FAILURE_THRESHOLD = 3
METHOD_CYCLING_THRESHOLD = 3


class PMSConsumer(PersistingConsumer):
    system = "PMS"
    label = "Transaction & Fraud Consumer"
    subscribes = [T.PAYMENT_TRANSACTIONS, T.PAYMENT_RESULTS]
    group_id = "pms-transaction-fraud-group"

    def __init__(self, bus, store, **kw):
        super().__init__(bus, store, **kw)
        self.transactions = store.collection(C.PMS_TRANSACTIONS)
        self.payments = store.collection(C.PMS_PAYMENTS)
        self.failures = defaultdict(int)
        self.methods = defaultdict(set)
        self.fired = defaultdict(set)         # order_id -> {rules already fired}

    # ----------------------------------------------------------------------
    def on_message(self, topic, key, event):
        self.trace(topic, event)
        if topic == T.PAYMENT_TRANSACTIONS:
            self.handle_transaction(event)
        else:
            self.handle_result(event)

    # -- every attempt -----------------------------------------------------
    def handle_transaction(self, txn):
        oid = txn["order_id"]
        status = txn["transaction_status"]

        self.transactions.insert_one({**txn, "_saved_at": utcnow()})

        self.payments.update_one(
            {"_id": oid},
            {"$set": {"order_id": oid,
                      "customer_id": txn.get("customer_id"),
                      "session_id": txn.get("session_id"),
                      "amount": txn.get("amount"),
                      "payment_gateway": txn.get("payment_gateway"),
                      "payment_method": txn.get("payment_method"),
                      "city": txn.get("city"),
                      "device": txn.get("device"),
                      "customer_segment": txn.get("customer_segment"),
                      "last_status": status,
                      "is_fraud_ground_truth": bool(txn.get("is_fraud")),
                      "fraud_type_ground_truth": txn.get("fraud_type"),
                      "last_updated": utcnow(),
                      "minute_ist": txn.get("minute_ist")},
             "$inc": {"attempts": 1,
                      "failed_attempts": int(status in ("FAILED", "DECLINED"))},
             "$setOnInsert": {"first_seen_minute_ist": txn.get("minute_ist")}},
            upsert=True)

        self.methods[oid].add(txn.get("payment_method"))
        if status in ("FAILED", "DECLINED"):
            self.failures[oid] += 1

        self.apply_rules(txn, status)

    # -- detection rules ---------------------------------------------------
    def apply_rules(self, txn, status):
        oid = txn["order_id"]
        fired = self.fired[oid]

        def fire(rule, severity, detail):
            if rule in fired:
                return
            fired.add(rule)
            self.payments.update_one(
                {"_id": oid},
                {"$set": {"flagged": True, "flagged_rule": rule},
                 "$push": {"rules_fired": rule}})
            self.alert(rule, severity, txn, detail,
                       amount=txn.get("amount"),
                       payment_method=txn.get("payment_method"),
                       payment_gateway=txn.get("payment_gateway"),
                       # ground truth travels with the alert so the dashboard can
                       # compare what we caught against what was actually fraud
                       is_fraud_ground_truth=bool(txn.get("is_fraud")),
                       fraud_type_ground_truth=txn.get("fraud_type"))

        if txn.get("amount", 0) >= HIGH_VALUE_FRAUD_MIN:
            fire("RULE_HIGH_VALUE", "HIGH",
                 f"{oid} attempted ₹{txn['amount']:,.0f} — far above normal basket "
                 f"size; hold for manual review")

        if self.failures[oid] >= REPEATED_FAILURE_THRESHOLD:
            fire("RULE_REPEATED_FAILURE", "HIGH",
                 f"{oid} has {self.failures[oid]} failed attempts "
                 f"({txn.get('failure_reason')}) — block before fulfilment")

        if status == "SUCCESS" and self.failures[oid] >= 2:
            fire("RULE_CARD_TESTING", "HIGH",
                 f"{oid} succeeded after {self.failures[oid]} failures — "
                 f"classic card-testing pattern")

        if len(self.methods[oid]) >= METHOD_CYCLING_THRESHOLD:
            fire("RULE_METHOD_CYCLING", "MEDIUM",
                 f"{oid} cycled through {len(self.methods[oid])} payment methods "
                 f"in one order")

    # -- final outcome -----------------------------------------------------
    def handle_result(self, res):
        self.payments.update_one(
            {"_id": res["order_id"]},
            {"$set": {"final_result": res["result"],
                      "final_attempts": res.get("attempts"),
                      "settled_minute_ist": res.get("minute_ist"),
                      "settled_at": res.get("event_timestamp"),
                      "authorised_value": res["amount"] if res["result"] == "AUTHORISED" else 0.0,
                      "lost_value": res["amount"] if res["result"] != "AUTHORISED" else 0.0}},
            upsert=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bus", choices=["kafka", "memory"], default="kafka")
    ap.add_argument("--store", choices=["mongodb", "memory"], default="mongodb")
    ap.add_argument("--from-beginning", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    bus = make_bus(args.bus, client_id="pms-consumer")
    store = make_store(args.store)
    svc = PMSConsumer(bus, store, from_beginning=args.from_beginning)
    svc.verbose = args.verbose
    svc.run()


if __name__ == "__main__":
    main()
