"""
pms/producer.py
---------------
SYSTEM 3 — Payment Management System (gateway service).

Reacts to payment requests raised by the OMS and behaves like a real payment
gateway: an attempt can succeed, fail and be retried, or be declined outright.
A small share of requests are seeded as fraud scenarios so the fraud rules in
the consumer have something real to catch.

  CONSUMES  payment-requests-topic   (from OMS)
  PUBLISHES payment-transactions-topic  every individual attempt
            payment-results-topic       the final outcome per order (back to OMS)

Fraud scenarios (each carries ground-truth is_fraud / fraud_type so the
dashboard can measure detection accuracy, not just alert volume):
    HIGH_VALUE_TRANSACTION     unusually large amount on one attempt
    REPEATED_PAYMENT_FAILURES  3+ consecutive failures on the same order
    FAILED_THEN_SUCCESS        fails twice then succeeds — card testing pattern
    MULTIPLE_PAYMENT_METHODS   attempts cycle through different methods

Run:  python -m pms.producer --speed 4
"""

import argparse
import random
import uuid

from shared import catalog as cat
from shared.bus import make_bus
from shared.config import T, FRAUD_RATE, HIGH_VALUE_FRAUD_MIN, MAX_PAYMENT_ATTEMPTS
from shared.service import Service, say

FRAUD_TYPES = ["HIGH_VALUE_TRANSACTION", "REPEATED_PAYMENT_FAILURES",
               "FAILED_THEN_SUCCESS", "MULTIPLE_PAYMENT_METHODS"]


class PaymentGatewayService(Service):
    system = "PMS"
    label = "Payment Gateway Service"
    subscribes = [T.PAYMENT_REQUESTS]
    group_id = "pms-gateway-service"

    def __init__(self, bus, **kw):
        super().__init__(bus, **kw)
        self.in_flight = {}

    def on_start(self):
        say(self.system, f"{self.label} started — awaiting payment requests")

    def on_message(self, topic, key, event):
        if topic == T.PAYMENT_REQUESTS:
            self.begin(event)

    # -- decide the shape of this payment up front -------------------------
    def begin(self, req):
        is_fraud = random.random() < FRAUD_RATE
        fraud_type = random.choice(FRAUD_TYPES) if is_fraud else None

        amount = req["amount"]
        if fraud_type == "HIGH_VALUE_TRANSACTION":
            amount = round(random.uniform(HIGH_VALUE_FRAUD_MIN, HIGH_VALUE_FRAUD_MIN * 3), 2)

        if fraud_type == "REPEATED_PAYMENT_FAILURES":
            plan = ["FAILED"] * random.randint(3, 4)          # never succeeds
        elif fraud_type == "FAILED_THEN_SUCCESS":
            plan = ["FAILED", "FAILED", "SUCCESS"]
        elif fraud_type == "MULTIPLE_PAYMENT_METHODS":
            plan = ["FAILED", "FAILED", "SUCCESS"]
        elif random.random() < 0.16:                           # ordinary one-off failure
            plan = ["FAILED", "SUCCESS"]
        elif random.random() < 0.05:
            plan = ["DECLINED"]                                # hard decline
        else:
            plan = ["SUCCESS"]

        payment = {
            "payment_id": f"PAY-{uuid.uuid4().hex[:12].upper()}",
            "order_id": req["order_id"],
            "session_id": req.get("session_id"),
            "customer_id": req["customer_id"],
            "amount": amount,
            "method": req["payment_method"],
            "gateway": random.choice(cat.PAYMENT_GATEWAYS),
            "city": req.get("city"),
            "device": req.get("device"),
            "segment": req.get("customer_segment"),
            "plan": plan,
            "attempt": 0,
            "is_fraud": is_fraud,
            "fraud_type": fraud_type,
        }
        self.in_flight[payment["payment_id"]] = payment
        # a gateway round-trip is not instant — this is what makes
        # checkout→paid latency a real number on the dashboard
        self.later(random.uniform(1.5, 4.0), lambda: self.attempt(payment))

    # -- one attempt -------------------------------------------------------
    def attempt(self, payment):
        idx = payment["attempt"]
        outcome = payment["plan"][idx]
        payment["attempt"] += 1

        method = payment["method"]
        if payment["fraud_type"] == "MULTIPLE_PAYMENT_METHODS":
            method = cat.PAYMENT_METHODS[idx % len(cat.PAYMENT_METHODS)]

        txn = {
            "event_id": str(uuid.uuid4()),
            "event_type": f"PAYMENT_{outcome}",
            "event_timestamp": cat.ts(),
            "minute_ist": cat.minute_bucket(),
            "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
            "payment_id": payment["payment_id"],
            "order_id": payment["order_id"],
            "session_id": payment["session_id"],
            "customer_id": payment["customer_id"],
            "amount": payment["amount"],
            "currency": "INR",
            "payment_method": method,
            "payment_gateway": payment["gateway"],
            "city": payment["city"],
            "device": payment["device"],
            "customer_segment": payment["segment"],
            "transaction_status": outcome,
            "attempt_number": payment["attempt"],
            "failure_reason": random.choice(cat.FAILURE_REASONS)
                              if outcome in ("FAILED", "DECLINED") else None,
            # ground truth for measuring detection quality
            "is_fraud": payment["is_fraud"],
            "fraud_type": payment["fraud_type"],
            "source_system": "PMS",
        }
        self.emit(T.PAYMENT_TRANSACTIONS, payment["order_id"], txn)

        more_attempts_planned = payment["attempt"] < len(payment["plan"])
        if outcome == "SUCCESS":
            self.finish(payment, "AUTHORISED", method)
        elif more_attempts_planned and payment["attempt"] < MAX_PAYMENT_ATTEMPTS + 1:
            self.later(random.uniform(2, 5), lambda: self.attempt(payment))
        else:
            self.finish(payment, "FAILED", method)

    def finish(self, payment, result, method):
        self.in_flight.pop(payment["payment_id"], None)
        self.emit(T.PAYMENT_RESULTS, payment["order_id"], {
            "event_id": str(uuid.uuid4()),
            "event_type": f"PAYMENT_RESULT_{result}",
            "event_timestamp": cat.ts(),
            "minute_ist": cat.minute_bucket(),
            "order_id": payment["order_id"],
            "payment_id": payment["payment_id"],
            "customer_id": payment["customer_id"],
            "amount": payment["amount"],
            "payment_method": method,
            "payment_gateway": payment["gateway"],
            "result": result,
            "attempts": payment["attempt"],
            "is_fraud": payment["is_fraud"],
            "fraud_type": payment["fraud_type"],
            "source_system": "PMS",
        })


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--speed", type=float, default=1.0)
    ap.add_argument("--bus", choices=["kafka", "memory"], default="kafka")
    args = ap.parse_args()

    bus = make_bus(args.bus, client_id="pms-gateway-service")
    svc = PaymentGatewayService(bus, speed=args.speed)
    svc.run()
    print("\n" + svc.summary())


if __name__ == "__main__":
    main()
