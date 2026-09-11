# Layer 5 — Change data capture (Debezium)

Telemetry is a firehose; the **registry** is slow-moving reference data: which
freezer is at which site, for which customer, with what threshold. The naive way to
join them is to look up the registry as it is *now* — but devices get reassigned, and
a breach from last week belongs to whoever owned the device *then*. CDC captures every
registry change as it happens, so we can reconstruct the past.

## The pieces

| Service | Role |
|---------|------|
| `postgres` | The registry. `wal_level=logical` exposes the write-ahead log to Debezium |
| `connect` | Kafka Connect running the Debezium Postgres connector |
| `registry-sink` | Lands each CDC event into the Iceberg table `coldchain.device_changes` |

Two Postgres settings make CDC correct:

- **`wal_level=logical`** — without it Debezium can't read row changes at all.
- **`REPLICA IDENTITY FULL`** on the table — makes Postgres log the *before* image on
  update/delete, so a change event carries both old and new rows.

## The change event

Register the connector (`connect/devices-connector.json`) against Connect's REST API,
and Debezium first **snapshots** the table (op `r`), then streams every change. An
`UPDATE` produces:

```
op=u
before: site=site-B customer=Globex Cold threshold=-12.0
after:  site=site-C customer=Initech     threshold=-18.0
```

`op` values: `r` snapshot, `c` insert, `u` update, `d` delete. The `registry-sink`
appends one dated row per event to `coldchain.device_changes`, building an
append-only history.

## The payoff: attribution "at the time"

With telemetry and registry history both in Iceberg, the capstone query
(`sink/capstone.py`) is a DuckDB **ASOF JOIN** — for each reading, match the most
recent registry state whose `valid_from` is at or before the reading's timestamp:

```sql
SELECT r.device_id, d.customer, d.site_id, r.temp_c, d.threshold_c
FROM reads r
ASOF JOIN device_changes d
  ON r.device_id = d.device_id AND r.read_ts >= d.valid_from
WHERE r.temp_c > d.threshold_c
```

Observed result: freezer-02's breaches attribute to **Initech** (its owner at read
time), and the tightened −18 threshold is applied — not the −12 it had under Globex.

```
breach count by customer:
  Initech:    17
  Acme Foods:  7
```

> The demo data is temporally compressed — all readings arrive after the
> reassignment, so they all attribute to Initech. The mechanism is what matters: had
> some readings predated the change, the same query would have attributed *those* to
> Globex, automatically. That correctness is the entire reason CDC + a dated history
> table are in this design instead of a plain lookup.

## Why the sink is PyIceberg, not Connect

The Debezium *source* runs in Kafka Connect (that's where Debezium lives). The
*sink* to Iceberg is the same lightweight PyIceberg pattern as telemetry — one fewer
JVM, and the transform from CDC envelope to history row is plain, readable Python.
