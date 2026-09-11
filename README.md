# telemetry-lakehouse

Phase 1 of the [infra learning roadmap](../learning-roadmap/docs/superpowers/specs/2026-09-11-infra-learning-roadmap-design.md).

**Domain:** cold-chain monitoring. Freezer sensors publish temperature and door
events over MQTT; a device registry in Postgres records which device sits at
which site for which customer. The system answers one question:

> Which customers had a temperature breach this week, attributed to where the
> device was **at the time** of the breach?

That "at the time" clause is why this needs CDC (Debezium) and Iceberg time travel,
not just a dashboard.

## Pipeline (built over 6 weeks)

```
sensor sim --MQTT--> Mosquitto --bridge--> Kafka --sink--> Iceberg --> Dremio
                                            ^              (MinIO)
Postgres registry --WAL--> Debezium --------+
```

## Status

| Week | Topic | State |
|------|-------|-------|
| 1 | Docker + MQTT | plan written — see `docs/plans/2026-09-11-phase1-week1.md` |
| 2 | Kafka | not started |
| 3 | MQTT→Kafka bridge | not started |
| 4 | Iceberg | not started |
| 5 | Debezium CDC | not started |
| 6 | Dremio + consolidation | not started |

## Where am I?

Read `LOG.md` — it always states the last thing done and the next command to run.
