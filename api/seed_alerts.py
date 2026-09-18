"""Load 500k synthetic alerts into coldchain.alerts_bench for the Task 9 index exercise.

Real alerts arrive a few per minute, far too few for an index to make a visible
difference. This is plumbing, not the lesson; the lesson is reading explain() output.

    docker compose --profile api run --rm alerts-api python seed_alerts.py
"""
from __future__ import annotations

import os
import random
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient

N = 500_000
BATCH = 10_000
DEVICES = 100
DAYS = 30

# A separate bench collection: the real `alerts` collection is never touched.
coll = MongoClient(os.environ.get("MONGO_URI", "mongodb://mongo:27017")).coldchain.alerts_bench
coll.drop()
now = datetime.now(timezone.utc)

for start in range(0, N, BATCH):
    docs = []
    for i in range(start, min(start + BATCH, N)):
        device_id = f"freezer-{random.randrange(DEVICES):02d}"
        kind = random.choice(["breach", "silent"])
        doc = {
            "_id": f"{kind}:{device_id}:{i}",
            "type": kind,
            "device_id": device_id,
            "event_ts": now - timedelta(seconds=random.randrange(DAYS * 86400)),
            "ingested_at": now,
        }
        if kind == "breach":
            doc |= {"seq": i, "temp_c": round(random.uniform(-17.0, -12.0), 2)}
        docs.append(doc)
    coll.insert_many(docs, ordered=False)
    print(f"inserted {start + len(docs)}/{N}")

print(f"done: {coll.estimated_document_count()} docs, indexes={list(coll.index_information())}")
