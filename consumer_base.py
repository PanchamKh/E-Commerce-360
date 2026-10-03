"""
shared/consumer_base.py
-----------------------
Shared behaviour for the five consumer processes: a MongoDB handle, the unified
`alerts` collection every system writes to, and a consistent console line per
message so the live demo is readable.

Each consumer has its own Kafka group_id. Members of one group SHARE the
partitions of the topics they subscribe to — so two consumers with different
jobs must be in different groups, otherwise each would see only part of its
own stream.
"""

import uuid
from datetime import datetime, timezone

from shared.config import C
from shared.service import Service, alert_line, say


def utcnow():
    return datetime.now(timezone.utc)


class PersistingConsumer(Service):
    """A Service that writes what it consumes into MongoDB."""

    def __init__(self, bus, store, **kw):
        super().__init__(bus, store=store, **kw)
        self.alerts = store.collection(C.ALERTS)
        self.verbose = kw.pop("verbose", False)

    def on_start(self):
        say(self.system, f"{self.label} subscribed → {', '.join(self.subscribes)} "
                         f"[group: {self.group_id}]")

    def alert(self, alert_type, severity, event, detail, **extra):
        """Every system writes alerts to one collection, tagged by system."""
        self.alerts.insert_one({
            "_id": f"{alert_type}-{uuid.uuid4().hex[:12]}",
            "alert_type": alert_type,
            "severity": severity,
            "source_system": self.system,
            "raised_by": self.label,
            "detail": detail,
            "order_id": event.get("order_id"),
            "session_id": event.get("session_id"),
            "customer_id": event.get("customer_id"),
            "sku": event.get("sku"),
            "category": event.get("category"),
            "warehouse_id": event.get("warehouse_id"),
            "city": event.get("city"),
            "minute_ist": event.get("minute_ist"),
            "event_timestamp": event.get("event_timestamp"),
            "alert_at": utcnow(),
            "status": "open",
            **extra,
        })
        self.alerts_raised += 1
        alert_line(self.system, alert_type, detail, severity)

    def trace(self, topic, event):
        if self.verbose:
            say(self.system, f"   💾 {topic:<26} {event.get('event_type', '')}")

    def on_stop(self):
        say(self.system, f"{self.label} stopped — consumed {self.consumed} events, "
                         f"raised {self.alerts_raised} alerts")
