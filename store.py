"""
shared/store.py
---------------
Consumers write through this layer. Two implementations:

    MongoStore   real MongoDB / MongoDB Atlas (what Atlas Charts reads)
    MemoryStore  dict-backed, supports the same update operators, and can dump
                 every collection to sample_data/*.jsonl — used by offline mode
                 so the pipeline can be validated without a database.

Only the operators the consumers actually use are implemented:
$set, $setOnInsert, $inc, $max, $push.
"""

import json
import os
import threading
from datetime import date, datetime

from shared.config import DB_NAME, MONGO_URI


# ==========================================================================
class MongoStore:
    name = "mongodb"

    def __init__(self, uri=MONGO_URI, db_name=DB_NAME):
        from pymongo import MongoClient
        self._client = MongoClient(uri, serverSelectionTimeoutMS=8000)
        self._client.admin.command("ping")
        self._db = self._client[db_name]
        self.db_name = db_name
        target = "Atlas" if "mongodb+srv" in uri else "local MongoDB"
        print(f"✅ Connected to {target} → database '{db_name}'")

    def collection(self, name):
        return self._db[name]

    def counts(self, names):
        return {n: self._db[n].count_documents({}) for n in names}

    def close(self):
        self._client.close()


# ==========================================================================
class _MemoryCollection:
    def __init__(self, name):
        self.name = name
        self._docs = {}
        self._auto = 0
        self._lock = threading.Lock()

    # -- helpers ----------------------------------------------------------
    def _key(self, filt):
        if "_id" in filt:
            return filt["_id"] if filt["_id"] in self._docs else None
        for doc_id, doc in self._docs.items():
            if all(doc.get(k) == v for k, v in filt.items()):
                return doc_id
        return None

    def _apply(self, doc, update, inserted):
        for k, v in update.get("$set", {}).items():
            doc[k] = v
        if inserted:
            for k, v in update.get("$setOnInsert", {}).items():
                doc.setdefault(k, v)
        for k, v in update.get("$inc", {}).items():
            doc[k] = doc.get(k, 0) + v
        for k, v in update.get("$max", {}).items():
            doc[k] = max(doc.get(k, v), v)
        for k, v in update.get("$push", {}).items():
            doc.setdefault(k, []).append(v)

    # -- API --------------------------------------------------------------
    def insert_one(self, doc):
        with self._lock:
            self._auto += 1
            doc = dict(doc)
            doc.setdefault("_id", f"{self.name}-{self._auto}")
            self._docs[doc["_id"]] = doc
        return type("R", (), {"inserted_id": doc["_id"]})()

    def update_one(self, filt, update, upsert=False):
        with self._lock:
            key = self._key(filt)
            if key is None:
                if not upsert:
                    return
                key = filt.get("_id", f"{self.name}-auto-{len(self._docs) + 1}")
                doc = {"_id": key}
                doc.update({k: v for k, v in filt.items() if k != "_id"})
                self._docs[key] = doc
                self._apply(doc, update, inserted=True)
            else:
                self._apply(self._docs[key], update, inserted=False)

    def find_one(self, filt, projection=None):
        with self._lock:
            key = self._key(filt)
            return dict(self._docs[key]) if key is not None else None

    def find(self, filt=None):
        filt = filt or {}
        with self._lock:
            return [dict(d) for d in self._docs.values()
                    if all(d.get(k) == v for k, v in filt.items()
                           if not isinstance(v, dict))]

    def count_documents(self, filt=None):
        return len(self.find(filt or {}))

    def docs(self):
        return list(self._docs.values())


class MemoryStore:
    name = "memory"
    db_name = "ecommerce360 (in-memory)"

    def __init__(self):
        self._cols = {}
        self._lock = threading.Lock()

    def collection(self, name):
        with self._lock:
            if name not in self._cols:
                self._cols[name] = _MemoryCollection(name)
            return self._cols[name]

    def counts(self, names):
        return {n: self.collection(n).count_documents({}) for n in names}

    def dump(self, out_dir):
        """Write every collection to <out_dir>/<collection>.jsonl."""
        os.makedirs(out_dir, exist_ok=True)
        written = {}

        def enc(o):
            if isinstance(o, (datetime, date)):
                return o.isoformat()
            return str(o)

        for name, col in self._cols.items():
            path = os.path.join(out_dir, f"{name}.jsonl")
            with open(path, "w", encoding="utf-8") as fh:
                for doc in col.docs():
                    fh.write(json.dumps(doc, default=enc) + "\n")
            written[name] = col.count_documents({})
        return written

    def close(self):
        pass


def make_store(mode, uri=MONGO_URI):
    return MemoryStore() if mode == "memory" else MongoStore(uri=uri)
