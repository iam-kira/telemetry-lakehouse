"""Inspect the Iceberg table: schema, snapshots, row count, time travel.

Run against the live catalog (same network as the stack):

    docker run --rm --network telemetry-lakehouse_default \
      -e CATALOG_URI=http://iceberg-rest:8181 -e S3_ENDPOINT=http://minio:9000 \
      -e AWS_ACCESS_KEY_ID=admin -e AWS_SECRET_ACCESS_KEY=password \
      -v "$PWD/sink:/app" telemetry-lakehouse-sink python -u query_table.py
"""
from __future__ import annotations

import os

from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog("rest", **{
    "uri": os.environ.get("CATALOG_URI", "http://iceberg-rest:8181"),
    "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://minio:9000"),
    "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "admin"),
    "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "password"),
    "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
})

table = catalog.load_table("coldchain.telemetry")

print("=== schema ===")
print(table.schema())

print("\n=== snapshots (each append = one snapshot) ===")
snaps = list(table.snapshots())
for s in snaps:
    print(f"  id={s.snapshot_id} at={s.timestamp_ms} op={s.summary.operation} "
          f"added_records={s.summary.additional_properties.get('added-records')}")

current = table.scan().to_arrow()
print(f"\n=== current row count: {current.num_rows} ===")
sample = current.slice(0, 3).to_pydict()
for i in range(min(3, current.num_rows)):
    print("  " + ", ".join(f"{k}={sample[k][i]}" for k in ("device_id", "temp_c", "seq", "door_open")))

# Time travel: read the table as of the FIRST snapshot and compare.
if len(snaps) >= 2:
    first_id = snaps[0].snapshot_id
    old = table.scan(snapshot_id=first_id).to_arrow()
    print(f"\n=== time travel: as of first snapshot {first_id}: {old.num_rows} rows ===")
    print(f"    now: {current.num_rows} rows  ->  the table remembers its own history")

# The business-flavoured query: warmest reading per device (breach hunting).
warm = current.group_by("device_id").aggregate([("temp_c", "max")])
print("\n=== warmest reading per device (breach candidates) ===")
wd = warm.to_pydict()
for dev, t in sorted(zip(wd["device_id"], wd["temp_c_max"])):
    print(f"  {dev}: max temp_c = {round(t, 2)}")
