"""Build the training table from Iceberg history (batch).  SKELETON — you complete it.

Runs in LAKEHOUSE mode (needs minio + iceberg-rest up). Mirrors sink/ env config:
CATALOG_URI, S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY.

Goal: one row per (device, time-window) holding its features plus a LABEL —
"did this device breach within the next H minutes?". Kafka is the source of truth;
this only reads the history Iceberg already stores. See sink/query_table.py for the
PyIceberg load pattern you can reuse.
"""
from __future__ import annotations

# TODO(you): load coldchain.readings via PyIceberg (reuse sink/query_table.py's catalog setup).
# TODO(you): per device, order by event time; build rolling features with ml/features.py.
# TODO(you): define the label — breach within horizon H (this is a modeling decision, not plumbing).
# TODO(you): write out a parquet/csv that train.py reads. Record row counts + class balance in LOG.md.

if __name__ == "__main__":
    raise SystemExit("extract.py is a skeleton — implement the TODOs (see ml/README.md).")
