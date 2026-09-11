# Layer 6 — SQL query layer (DuckDB + Dremio)

The lakehouse needs a SQL front door. Two are wired up, and this layer is an honest
account of what each one does and doesn't do on a 16 GB OSS stack.

## DuckDB — the working SQL engine (headless)

`sink/capstone.py` and `sink/query_table.py` load the Iceberg tables through
PyIceberg and run SQL in **DuckDB**, including the breach-attribution ASOF JOIN. This
is fully working today and is how the project answers its question:

```bash
docker run --rm --network telemetry-lakehouse_default \
  -e CATALOG_URI=http://iceberg-rest:8181 -e S3_ENDPOINT=http://minio:9000 \
  -e AWS_ACCESS_KEY_ID=admin -e AWS_SECRET_ACCESS_KEY=password \
  -v "$PWD/sink:/app" telemetry-lakehouse-sink python -u capstone.py
```

DuckDB reads the REST-catalog-managed tables correctly because PyIceberg resolves the
current snapshot through the catalog and hands DuckDB the exact data files.

## Dremio — the UI, with one real limitation

Dremio runs at <http://localhost:9047> (admin / dremio123, bootstrapped by
`scripts/dremio_setup.py`) and a MinIO S3 source named `lake` is attached, so you can
browse the `warehouse` bucket and query any Parquet/CSV there.

**What it can't do on OSS 25.2:** query our Iceberg tables *as Iceberg tables*. Two
things block it, both discovered by trying:

1. As a **filesystem** S3 source, Dremio expects a hadoop-style pointer
   (`version-hint.text`) to find the current metadata. PyIceberg + a REST catalog
   don't write one — the catalog *is* the pointer — so Dremio reports
   *"Failed to get iceberg metadata"*.
2. Dremio's dedicated **Iceberg REST Catalog** source (type `RESTCATALOG`, field
   `restEndpointUri`) is recognised but gated: OSS 25.2 returns
   *"Iceberg Catalog Source is not supported."* It's an enterprise / newer-version
   feature.

This is a genuine ecosystem edge, not a misconfiguration: a REST-catalog-managed
Iceberg table needs a **catalog-aware** reader. DuckDB via PyIceberg is one; Dremio
Enterprise (or Spark, Trino, Snowflake) are others.

### Making Dremio query the lakehouse (the real fix)

Swap the standalone REST fixture for **Nessie** as the catalog. Nessie speaks the
Iceberg REST API (so the PyIceberg sinks keep working, pointed at
`http://nessie:19120/iceberg/main`) *and* Dremio has native Nessie source support in
OSS. That's the documented upgrade path; it's left as the next iteration rather than
done here, because the DuckDB layer already answers the business question and swapping
the catalog is a pipeline-wide change worth doing deliberately. Nessie also brings
git-like branching of data — a natural fit for the roadmap's git thread.

## Reflections (why Dremio, once it can read the tables)

Dremio's headline feature is the **reflection**: a materialized, auto-maintained copy
of a query that it transparently substitutes into future queries. A breach dashboard
re-running every few seconds turns from a full scan into a lookup. That's the
query-acceleration half of the lakehouse — worth returning to once the Nessie swap
lets Dremio see the tables.
