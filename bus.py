"""
shared/bus.py
-------------
Every service talks to Kafka through this thin layer instead of importing
kafka-python directly. Two implementations share one interface:

    KafkaBus   real Apache Kafka (what the live demo uses)
    MemoryBus  in-process queues, so the whole pipeline can be run and tested
               on a machine with no Docker — used by `python run_all.py --offline`

Interface:
    bus.publish(topic, key, value)
    sub = bus.subscribe(topics, group_id)
    for topic, key, value in sub.poll(timeout=0.5): ...
"""

import json
import queue
import sys
import threading
from collections import defaultdict

from shared.config import KAFKA_BROKER


# ==========================================================================
# Real Kafka
# ==========================================================================
class KafkaSubscription:
    def __init__(self, consumer):
        self._consumer = consumer

    def poll(self, timeout=0.5, max_records=200):
        batch = self._consumer.poll(timeout_ms=int(timeout * 1000),
                                    max_records=max_records)
        out = []
        for tp, messages in batch.items():
            for m in messages:
                out.append((tp.topic, m.key, m.value))
        return out

    def close(self):
        self._consumer.close()


class KafkaBus:
    name = "kafka"

    def __init__(self, broker=KAFKA_BROKER, client_id="service"):
        from kafka import KafkaProducer, errors
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: (k or "").encode("utf-8"),
                acks="all", retries=5, linger_ms=10, client_id=client_id,
            )
        except errors.NoBrokersAvailable:
            print(f"❌ Kafka not reachable at {broker}. Start it with: docker compose up -d")
            sys.exit(1)
        self._broker = broker

    def publish(self, topic, key, value):
        self._producer.send(topic, key=key, value=value)

    def subscribe(self, topics, group_id, from_beginning=False):
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=self._broker,
            group_id=group_id,
            auto_offset_reset="earliest" if from_beginning else "latest",
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
        )
        return KafkaSubscription(consumer)

    def flush(self):
        self._producer.flush()

    def close(self):
        self._producer.flush()
        self._producer.close()


# ==========================================================================
# In-memory bus (offline mode)
# ==========================================================================
class MemorySubscription:
    def __init__(self, q):
        self._q = q

    def poll(self, timeout=0.5, max_records=200):
        out = []
        try:
            out.append(self._q.get(timeout=timeout))
        except queue.Empty:
            return out
        while len(out) < max_records:
            try:
                out.append(self._q.get_nowait())
            except queue.Empty:
                break
        return out

    def close(self):
        pass


class MemoryBus:
    """Fan-out queues: every distinct group_id gets its own copy of a message."""
    name = "memory"

    def __init__(self):
        self._subs = defaultdict(list)          # topic -> [queue]
        self._lock = threading.Lock()
        self.published = 0

    def publish(self, topic, key, value):
        with self._lock:
            targets = list(self._subs.get(topic, []))
            self.published += 1
        for q in targets:
            q.put((topic, key, json.loads(json.dumps(value))))   # copy, like the wire

    def subscribe(self, topics, group_id, from_beginning=False):
        q = queue.Queue()
        with self._lock:
            for t in topics:
                self._subs[t].append(q)
        return MemorySubscription(q)

    def flush(self):
        pass

    def close(self):
        pass


def make_bus(mode, client_id="service"):
    return MemoryBus() if mode == "memory" else KafkaBus(client_id=client_id)
