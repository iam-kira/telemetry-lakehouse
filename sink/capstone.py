"""The capstone query: breach attribution with a temporal join.

For every telemetry reading warmer than the device's threshold, attribute the
breach to the site + customer that owned the device **at the reading's timestamp**
— not whoever owns it now. That's a DuckDB ASOF JOIN against the registry history
in coldchain.device_changes.

Loads both Iceberg tables to Arrow (PyIceberg), runs the SQL in DuckDB.
"""
from __future__ import annotations

import os

import duckdb
from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog("rest", **{
    "uri": os.environ.get("CATALOG_URI", "http://iceberg-rest:8181"),
    "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://minio:9000"),
    "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "admin"),
    "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "password"),
    "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
})

telemetry = catalog.load_table("coldchain.telemetry").scan().to_arrow()
changes = catalog.load_table("coldchain.device_changes").scan().to_arrow()

con = duckdb.connect()
con.register("telemetry", telemetry)
con.register("device_changes", changes)

print("=== registry history (coldchain.device_changes) ===")
con.sql("""
    SELECT device_id, op, site_id, customer, threshold_c, valid_from
    FROM device_changes ORDER BY device_id, valid_from
""").show()

# ASOF JOIN: for each reading, match the most recent registry state whose
# valid_from is <= the reading time. That is the "at the time" attribution.
print("\n=== breaches, attributed to the site/customer at the time ===")
result = con.sql("""
    WITH reads AS (
        SELECT device_id, temp_c, door_open,
               to_timestamp(ts) AS read_ts
        FROM telemetry
    )
    SELECT r.device_id,
           d.customer,
           d.site_id,
           ROUND(r.temp_c, 2)      AS temp_c,
           d.threshold_c,
           r.door_open
    FROM reads r
    ASOF JOIN device_changes d
      ON r.device_id = d.device_id
     AND r.read_ts >= d.valid_from
    WHERE r.temp_c > d.threshold_c
    ORDER BY r.device_id, temp_c DESC
""")
rows = result.fetchall()
if not rows:
    print("(no breaches in current data — every reading is below its device threshold)")
else:
    for row in rows:
        print(f"  device={row[0]} customer={row[1]} site={row[2]} temp={row[3]} threshold={row[4]} door_open={row[5]}")

print("\n=== breach count by customer ===")
by_cust = con.sql("""
    WITH reads AS (SELECT device_id, temp_c, to_timestamp(ts) AS read_ts FROM telemetry)
    SELECT d.customer, COUNT(*) AS breaches
    FROM reads r
    ASOF JOIN device_changes d
      ON r.device_id = d.device_id AND r.read_ts >= d.valid_from
    WHERE r.temp_c > d.threshold_c
    GROUP BY d.customer ORDER BY breaches DESC
""")
by_rows = by_cust.fetchall()
print("\n".join(f"  {c}: {n} breaches" for c, n in by_rows) if by_rows else "(none)")
