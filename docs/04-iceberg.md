# Layer 4 — Iceberg lakehouse

Kafka is a log; it forgets on a retention timer. The lakehouse is where telemetry
lands to stay: **Apache Iceberg** tables backed by **MinIO** (S3), tracked by an
**Iceberg REST catalog**. Iceberg gives a pile of Parquet files the behaviour of a
real table — schema, atomic commits, snapshots, and time travel.

## The pieces

| Service | Role |
|---------|------|
| `minio` | S3-compatible object store — holds every data + metadata file |
| `minio-init` | one-shot: creates the `warehouse` bucket, then exits |
| `iceberg-rest` | REST catalog — the source of truth for "what is the current table" |
| `sink` | Kafka consumer that micro-batches rows and appends them to Iceberg (PyIceberg) |

### Why PyIceberg, not the Kafka Connect Iceberg sink

The spec's production shape is the Iceberg **Kafka Connect** sink. This build uses a
small **PyIceberg** consumer instead — one fewer JVM (matters on 16 GB), and every
write is visible in ~80 lines of Python rather than hidden in connector config. The
Connect sink is the documented swap-in; the table format on disk is identical either
way, so nothing downstream changes.

## What "a table" actually is on disk

After a few appends, MinIO holds:

```
coldchain/telemetry/
  metadata/00000-*.metadata.json   table v0 (empty, just created)
  metadata/00001-*.metadata.json   table v1  ─┐ one metadata.json per commit;
  metadata/00002-*.metadata.json   table v2   │ the catalog points at the latest
  metadata/00003-*.metadata.json   table v3  ─┘
  metadata/snap-<id>-*.avro        manifest list — which manifests are in a snapshot
  metadata/<uuid>-m0.avro          manifest — which data files, with row counts + stats
  data/00000-0-*.parquet           the actual rows (one file per append here)
```

A **snapshot** is a consistent view of the table at a point in time: a metadata.json
names a current snapshot, which points at a manifest list, which points at manifests,
which point at Parquet data files. A commit is atomic because it's a single swap of
the catalog's pointer to a new metadata.json — readers never see a half-written table.

## Snapshots and time travel

Every append is a new snapshot. Because old snapshots and their data files are kept,
you can read the table *as it was*:

```python
current = table.scan().to_arrow()                      # 42 rows
first   = table.scan(snapshot_id=snaps[0].snapshot_id).to_arrow()  # 12 rows
```

Observed:

```
snapshots: 3599800648852862058 (+12), 1155254468835081537 (+1), 8167128934158848968 (+29)
current row count: 42
time travel: as of first snapshot -> 12 rows;  now -> 42 rows
```

This is the capability the whole project exists to demonstrate. Once the device
registry has history (Layer 5, via CDC), "which customer owned this device **at the
time** of the breach" becomes a time-travel join instead of a wrong answer.

## Inspect it yourself

```bash
docker run --rm --network telemetry-lakehouse_default \
  -e CATALOG_URI=http://iceberg-rest:8181 -e S3_ENDPOINT=http://minio:9000 \
  -e AWS_ACCESS_KEY_ID=admin -e AWS_SECRET_ACCESS_KEY=password \
  -v "$PWD/sink:/app" telemetry-lakehouse-sink python -u query_table.py
```

## Micro-batching (the small-files trap)

The sink flushes on **50 rows OR 5 seconds**, whichever comes first — so one snapshot
above holds 29 rows, another just 1 (the timer caught a lull). Appending per-message
instead would spray thousands of tiny Parquet files and wreck read performance; this
is the single most common lakehouse mistake, avoided on purpose.

## Persist the catalog, not just the data (a bug we hit)

The first build ran the REST catalog with its **default in-memory** backend. Data
files lived safely in MinIO, but on the first full restart the catalog came up blank:
`coldchain.telemetry` reported **0 rows**, every data file orphaned. The lesson: an
Iceberg lakehouse has *two* durable things — the data files **and** the catalog that
names the current metadata pointer. Lose the catalog and the data is unreachable.

Fix: give the JDBC catalog a real database on a volume.

```yaml
iceberg-rest:
  user: root                                    # write the file on the mounted volume
  environment:
    CATALOG_URI: jdbc:sqlite:/persist/catalog.db  # not the default in-memory sqlite
  volumes:
    - iceberg-catalog:/persist
```

Recovery was painless because Kafka still had the telemetry (7-day retention): reset
the sink's consumer group to earliest and it **replayed the log** back into the fresh
persistent catalog — 122 rows restored, no data generated. That replayability is
exactly why the durable log sits in the middle of the pipeline.
