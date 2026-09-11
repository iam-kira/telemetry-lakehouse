"""Kafka -> Iceberg sink (micro-batching, PyIceberg).

Consumes the `telemetry` topic and appends rows to the Iceberg table
`coldchain.telemetry`. Batches by size or time so we write a few sizeable data
files instead of thousands of tiny ones (small-files is the classic lakehouse
foot-gun). Offsets are committed only after a successful append, so a crash
re-reads the batch rather than losing it — at-least-once, matching the rest of the
pipeline. Duplicates that this can produce are deduped at query time on
(device_id, seq).

Why PyIceberg instead of the Kafka Connect Iceberg sink: it is far lighter (no
extra JVM worker) and it keeps every write visible in Python, which is the point of
this layer. The Connect sink is the production swap-in; see docs/04-iceberg.md.

Env: KAFKA_BOOTSTRAP, KAFKA_TOPIC, CATALOG_URI, S3_ENDPOINT, AWS creds, BATCH_SIZE,
FLUSH_SECONDS.
"""
from __future__ import annotations

import json
import os
import time

import pyarrow as pa
from confluent_kafka import Consumer
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "telemetry")
CATALOG_URI = os.environ.get("CATALOG_URI", "http://iceberg-rest:8181")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))
FLUSH_SECONDS = float(os.environ.get("FLUSH_SECONDS", "5"))

NAMESPACE = "coldchain"
TABLE = "telemetry"

SCHEMA = Schema(
    NestedField(1, "device_id", StringType(), required=False),
    NestedField(2, "ts", DoubleType(), required=False),          # sensor epoch seconds
    NestedField(3, "temp_c", DoubleType(), required=False),
    NestedField(4, "door_open", BooleanType(), required=False),
    NestedField(5, "seq", LongType(), required=False),
    NestedField(6, "ingest_ts", TimestamptzType(), required=False),  # when the sink wrote it
)


def load_catalog() -> RestCatalog:
    return RestCatalog("rest", **{
        "uri": CATALOG_URI,
        "s3.endpoint": S3_ENDPOINT,
        "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "admin"),
        "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "password"),
        "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
    })


def ensure_table(catalog: RestCatalog):
    try:
        catalog.create_namespace(NAMESPACE)
        print(f"created namespace {NAMESPACE}")
    except NamespaceAlreadyExistsError:
        pass
    try:
        return catalog.load_table(f"{NAMESPACE}.{TABLE}")
    except NoSuchTableError:
        tbl = catalog.create_table(f"{NAMESPACE}.{TABLE}", schema=SCHEMA)
        print(f"created table {NAMESPACE}.{TABLE}")
        return tbl


def flush(table, rows: list[dict]) -> None:
    if not rows:
        return
    now_us = int(time.time() * 1_000_000)
    df = pa.table({
        "device_id": pa.array([r.get("device_id") for r in rows], pa.string()),
        "ts": pa.array([r.get("ts") for r in rows], pa.float64()),
        "temp_c": pa.array([r.get("temp_c") for r in rows], pa.float64()),
        "door_open": pa.array([r.get("door_open") for r in rows], pa.bool_()),
        "seq": pa.array([r.get("seq") for r in rows], pa.int64()),
        "ingest_ts": pa.array([now_us] * len(rows), pa.timestamp("us", tz="UTC")),
    })
    # cast to exactly what the Iceberg table expects (large_string vs string, etc.)
    df = df.cast(table.schema().as_arrow())
    table.append(df)
    print(f"appended {len(rows)} rows; snapshot={table.current_snapshot().snapshot_id}")


def main() -> None:
    catalog = load_catalog()
    table = ensure_table(catalog)
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "iceberg-sink",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([KAFKA_TOPIC])
    print(f"sink: consuming {KAFKA_TOPIC} -> {NAMESPACE}.{TABLE} (batch={BATCH_SIZE}, flush={FLUSH_SECONDS}s)")

    batch: list[dict] = []
    last_flush = time.time()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None and msg.error() is None:
                batch.append(json.loads(msg.value()))
            due = (len(batch) >= BATCH_SIZE) or (batch and time.time() - last_flush >= FLUSH_SECONDS)
            if due:
                table = catalog.load_table(f"{NAMESPACE}.{TABLE}")  # refresh metadata
                flush(table, batch)
                consumer.commit(asynchronous=False)
                batch.clear()
                last_flush = time.time()
    except KeyboardInterrupt:
        if batch:
            flush(catalog.load_table(f"{NAMESPACE}.{TABLE}"), batch)
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
