"""CDC -> Iceberg registry-history sink.

Consumes the Debezium topic `cdc.public.devices` and appends one row per change to
the Iceberg table `coldchain.device_changes`. This is an append-only history: every
insert/update/delete to the registry becomes a dated row, so we can reconstruct what
the registry said at any past moment — the key to attributing a breach to the site a
device was at *at the time*.

Debezium envelope (schemas disabled): {before, after, op, ts_ms, source}.
op: r=snapshot, c=create, u=update, d=delete.
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
from pyiceberg.types import DoubleType, LongType, NestedField, StringType, TimestamptzType

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
CDC_TOPIC = os.environ.get("CDC_TOPIC", "cdc.public.devices")
CATALOG_URI = os.environ.get("CATALOG_URI", "http://iceberg-rest:8181")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "20"))
FLUSH_SECONDS = float(os.environ.get("FLUSH_SECONDS", "3"))

NAMESPACE, TABLE = "coldchain", "device_changes"

SCHEMA = Schema(
    NestedField(1, "device_id", StringType(), required=False),
    NestedField(2, "site_id", StringType(), required=False),
    NestedField(3, "customer", StringType(), required=False),
    NestedField(4, "threshold_c", DoubleType(), required=False),
    NestedField(5, "op", StringType(), required=False),        # r/c/u/d
    NestedField(6, "event_ms", LongType(), required=False),    # change time, epoch ms
    NestedField(7, "valid_from", TimestamptzType(), required=False),
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
    except NamespaceAlreadyExistsError:
        pass
    try:
        return catalog.load_table(f"{NAMESPACE}.{TABLE}")
    except NoSuchTableError:
        print(f"created table {NAMESPACE}.{TABLE}")
        return catalog.create_table(f"{NAMESPACE}.{TABLE}", schema=SCHEMA)


def to_row(evt: dict) -> dict | None:
    op = evt.get("op")
    state = evt.get("before") if op == "d" else evt.get("after")
    if not state:
        return None
    return {
        "device_id": state.get("device_id"),
        "site_id": state.get("site_id"),
        "customer": state.get("customer"),
        "threshold_c": state.get("threshold_c"),
        "op": op,
        "event_ms": evt.get("ts_ms"),
    }


def flush(table, rows: list[dict]) -> None:
    if not rows:
        return
    df = pa.table({
        "device_id": pa.array([r["device_id"] for r in rows], pa.string()),
        "site_id": pa.array([r["site_id"] for r in rows], pa.string()),
        "customer": pa.array([r["customer"] for r in rows], pa.string()),
        "threshold_c": pa.array([r["threshold_c"] for r in rows], pa.float64()),
        "op": pa.array([r["op"] for r in rows], pa.string()),
        "event_ms": pa.array([r["event_ms"] for r in rows], pa.int64()),
        "valid_from": pa.array(
            [int((r["event_ms"] or int(time.time() * 1000)) * 1000) for r in rows],
            pa.timestamp("us", tz="UTC"),
        ),
    })
    df = df.cast(table.schema().as_arrow())
    table.append(df)
    print(f"appended {len(rows)} change(s); snapshot={table.current_snapshot().snapshot_id}")


def main() -> None:
    catalog = load_catalog()
    ensure_table(catalog)
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "registry-sink",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([CDC_TOPIC])
    print(f"registry-sink: {CDC_TOPIC} -> {NAMESPACE}.{TABLE}")

    batch: list[dict] = []
    last = time.time()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None and msg.error() is None and msg.value():
                row = to_row(json.loads(msg.value()))
                if row:
                    batch.append(row)
            if batch and (len(batch) >= BATCH_SIZE or time.time() - last >= FLUSH_SECONDS):
                flush(catalog.load_table(f"{NAMESPACE}.{TABLE}"), batch)
                consumer.commit(asynchronous=False)
                batch.clear()
                last = time.time()
    except KeyboardInterrupt:
        if batch:
            flush(catalog.load_table(f"{NAMESPACE}.{TABLE}"), batch)
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
