"""
shared/service.py
-----------------
Base class for the eight long-running processes. It gives every service the
same three things:

  1. a poll loop over its subscribed topics,
  2. a scheduler, so a service can say "emit this event in 4 seconds" — which is
     how order lifecycles progress in real elapsed time rather than instantly,
  3. consistent, colour-coded console output for the live demo.

A service is either a PRODUCER (publishes business events, may also react to
upstream topics) or a CONSUMER (reads and persists to MongoDB). Both use this
class, because in an event-driven system the distinction is only about intent.
"""

import heapq
import itertools
import threading
import time
from datetime import timedelta

from shared.catalog import now_ist

_print_lock = threading.Lock()
_counter = itertools.count()

SYSTEM_TAG = {
    "CLK": "\033[95m[CLK]\033[0m",   # magenta
    "OMS": "\033[94m[OMS]\033[0m",   # blue
    "PMS": "\033[93m[PMS]\033[0m",   # yellow
    "IMS": "\033[92m[IMS]\033[0m",   # green
    "ANL": "\033[96m[ANL]\033[0m",   # cyan
}


def say(system, message):
    with _print_lock:
        print(f"{SYSTEM_TAG.get(system, '[---]')} {message}", flush=True)


def alert_line(system, alert_type, detail, severity="MEDIUM"):
    icon = "🚨" if severity == "HIGH" else "⚠️ "
    say(system, f"   {icon} {alert_type}: {detail}")


class Service:
    system = "---"          # CLK / OMS / PMS / IMS / ANL
    label = "service"
    subscribes = []         # topics this service reacts to
    group_id = None         # required when subscribes is non-empty

    def __init__(self, bus, store=None, speed=1.0, from_beginning=False):
        self.bus = bus
        self.store = store
        self.speed = max(speed, 0.01)
        self.published = 0
        self.consumed = 0
        self.alerts_raised = 0
        self.errors = 0
        self.by_topic = {}
        self._schedule = []      # heap of (due_ts, seq, callback)
        self._running = True
        self._sub = None
        if self.subscribes:
            self._sub = bus.subscribe(self.subscribes, self.group_id or self.label,
                                      from_beginning=from_beginning)

    # -- publishing --------------------------------------------------------
    def emit(self, topic, key, event):
        self.bus.publish(topic, key, event)
        self.published += 1
        self.by_topic[topic] = self.by_topic.get(topic, 0) + 1
        return event

    # -- scheduling --------------------------------------------------------
    def later(self, seconds, callback):
        """Run callback after `seconds` of simulated time (divided by --speed)."""
        due = time.time() + (seconds / self.speed)
        heapq.heappush(self._schedule, (due, next(_counter), callback))

    def _run_due(self):
        now = time.time()
        while self._schedule and self._schedule[0][0] <= now:
            _, _, callback = heapq.heappop(self._schedule)
            callback()

    def pending(self):
        return len(self._schedule)

    # -- hooks subclasses override ----------------------------------------
    def on_start(self):
        pass

    def on_message(self, topic, key, event):
        pass

    def on_tick(self):
        """Called every loop even when no messages arrived."""
        pass

    def on_stop(self):
        pass

    # -- main loop ---------------------------------------------------------
    def run(self):
        self.on_start()
        try:
            while self._running:
                if self._sub:
                    for topic, key, event in self._sub.poll(timeout=0.2):
                        self.consumed += 1
                        # One bad message must never kill a service: log it,
                        # count it, keep consuming. This is the difference
                        # between a demo and something that survives a demo.
                        try:
                            self.on_message(topic, key, event)
                        except Exception as exc:
                            self.errors += 1
                            say(self.system,
                                f"   ⚠️  error handling {topic}/"
                                f"{event.get('event_type')}: {exc!r}")
                else:
                    time.sleep(0.05)
                self._run_due()
                self.on_tick()
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    def stop(self):
        if not self._running:
            return
        self._running = False
        try:
            self.on_stop()
        finally:
            if self._sub:
                self._sub.close()
            self.bus.flush()

    def summary(self):
        lines = [f"{self.label}: published {self.published}, consumed {self.consumed}"]
        for t, n in sorted(self.by_topic.items()):
            lines.append(f"      {t:<26} {n:>6}")
        return "\n".join(lines)
