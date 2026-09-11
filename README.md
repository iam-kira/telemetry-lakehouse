# telemetry-lakehouse

A working **cold-chain monitoring lakehouse**, built from scratch one layer at a
time. Freezer sensors stream temperature and door-open events over MQTT; a change
-data-capture stream keeps a device registry in sync; everything lands in Apache
Iceberg tables on object storage and is queryable with SQL through Dremio.

It is deliberately end-to-end and deliberately small — sized to run on a single
16 GB laptop — so every moving part is visible and every guarantee is one you can
break and watch fail.

> Phase 1 of a longer [infrastructure roadmap](../learning-roadmap/docs/superpowers/specs/2026-09-11-infra-learning-roadmap-design.md):
> this repo (the data pipeline), then self-hosting it, then Kubernetes + Helm.

## The question it answers

> **Which customers had a temperature breach this week, attributed to the site
> the device was at _at the time_ of the breach?**

That "at the time" is the whole point. Devices get reassigned between sites and
customers; a naive join against the current registry would mis-attribute a breach
that happened last Tuesday to wherever the device sits today. Answering it
correctly needs **change-data-capture** (to record registry history) and
**Iceberg time travel** (to query the world as it was) — which is why those
technologies are in the stack rather than bolted on for show.

## Architecture

```
                          sensors/<id>/telemetry
  sim/sensor.py  ──MQTT──▶  Mosquitto  ──▶  bridge  ──▶  Kafka ─┐
  (cold-chain fleet)                     (mqtt→kafka)          │
                                                               ▼
  Postgres registry ──WAL──▶ Debezium ──▶ Kafka ──▶ Iceberg sink ──▶ Iceberg tables
  (device→site→customer)                        (Kafka Connect)      on MinIO (S3)
                                                               │
                                                               ▼
                                                            Dremio  ◀── SQL
```

Single Kafka broker (KRaft, no ZooKeeper). One Kafka Connect worker hosts **both**
the Debezium source and the Iceberg sink — no Spark, no Flink — so the whole stack
fits in memory.

## Status

| Week | Layer | State |
|------|-------|-------|
| 1 | Docker fundamentals + **MQTT** (Mosquitto, sensor sim, QoS) | ✅ done — [docs/01-mqtt.md](docs/01-mqtt.md) |
| 2 | **Kafka** (KRaft single broker) | ✅ done — [docs/02-kafka.md](docs/02-kafka.md) |
| 3 | **MQTT→Kafka bridge** | ✅ done — [docs/03-bridge.md](docs/03-bridge.md) |
| 4 | **Iceberg** + MinIO + REST catalog | ✅ done — [docs/04-iceberg.md](docs/04-iceberg.md) |
| 5 | **Debezium** CDC from Postgres | ✅ done — [docs/05-cdc.md](docs/05-cdc.md) |
| 6 | **Dremio** query layer | 🚧 in progress |

## Quickstart

Requires Docker Desktop (WSL2 backend) and Python 3.12+.

```bash
# 1. start the broker
docker compose up -d mosquitto

# 2. python deps
py -m venv .venv && .venv/Scripts/python -m pip install -r requirements.txt

# 3. stream cold-chain telemetry
.venv/Scripts/python sim/sensor.py --devices 3 --interval 1

# 4. in another terminal, watch it
.venv/Scripts/python mqtt/sub.py --topic 'sensors/#'
```

## Layout

```
docker-compose.yml     the stack, brought up one service at a time
mqtt/                  broker config + observer (sub.py)
sim/                   cold-chain sensor simulator (sensor.py)
bridge/                MQTT→Kafka bridge            (week 3)
docs/                  per-layer write-ups
docs/plans/            the week-by-week build plan
LOG.md                 append-only journal; read the last entry to resume
```

## How this repo is built

Each layer ends at a **checkpoint** — a command whose output proves it works — and
each is stress-tested by breaking it on purpose (killing the broker mid-publish,
etc.) and recording the result. `LOG.md` is the running record and the
resume-from-cold pointer.
