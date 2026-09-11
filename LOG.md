# LOG

Append-only. Newest at the bottom. Each entry: date · what got done · (weekend) prediction vs actual · **next command to run**.

This file is how you resume cold. If you sit down and don't know where you were, read the last entry.

---

## 2026-09-11 — Phase 1 kicked off

- Roadmap spec approved; `telemetry-lakehouse` repo created.
- Week-1 teaching plan written: `docs/plans/2026-09-11-phase1-week1.md`.
- **Blocker:** Docker not yet installed.

**Next command to run:**

```
winget install -e --id Docker.DockerDesktop
```

Then Task 0 in the week-1 plan (`docker run --rm hello-world`).

## 2026-09-11 — Docker installed (the hard way)

Docker Desktop 4.90 crash-looped on startup: `rename <socket> <socket>.stale: The file cannot be accessed by the system`. Cause: orphaned AF_UNIX socket files in a broken state (one, `docker-secrets-engine\engine.sock`, dated back to March) that no user-space tool could delete. Fix: killed all Docker procs + `wsl --shutdown`, then **renamed the parent directories aside** (`Docker\run`, `docker-secrets-engine`) so Docker recreated them clean. Engine came up: server 29.7.2, 16 CPUs, ~7.5 GB VM RAM. Compose v5.5.1.

**Task 0 done** — `docker run --rm hello-world` prints the greeting.

**Next command to run:** Task 1, Step 1 — `docker run -it --name box1 ubuntu:24.04 bash`

## 2026-09-11 — Tasks 1 & 2 done

- **Task 1** (image vs container): file in a container's writable layer survived stop→start but died on `docker rm`. Bonus lesson: `docker` is not available *inside* a container — proved isolation by accident.
- **Task 2** (layers + cache): first build 21.0s, rebuild after editing app.txt 3.6s. Two `RUN` layers `CACHED`, only `COPY` re-ran. `docker history` timestamps show reused vs rebuilt layers; `<missing>` = unnamed intermediate layers, not an error.

**Next command to run:** Task 3, Step 1 — `docker volume create mydata`

## 2026-09-11 — Task 3 done

Volume `mydata`: wrote proof.txt via a `--rm` container (deleted on exit), read `survives` back from two separate fresh containers. Volume outlives any container; contrast Task 1 where `rm` killed the file. This is the storage model for Kafka/Postgres/MinIO.

**Next command to run:** Task 4, Step 1 — `docker network create labnet`

## 2026-09-11 — Task 4 done — WEEKNIGHT BLOCK COMPLETE (Tasks 1–4)

Networks + DNS: on `labnet`, `ping server` resolved to 172.18.0.2, 0% loss. Off the network, `ping: bad address 'server'`. User-defined bridge = DNS by container name. This is why Compose services reach each other as `kafka:9092`, `postgres:5432`.

Docker fundamentals done: image vs container, layers/cache, volumes, networking. Ready for the weekend MQTT project.

**Next command to run:** Task 5, Step 1 — create `mqtt/mosquitto.conf`, then run the broker.

## 2026-09-11 — Week 1 COMPLETE (weekend MQTT project executed)

Built the real MQTT layer instead of the toy pub/sub: `docker-compose.yml` (Mosquitto 2.0.22, persistence on), `sim/sensor.py` (cold-chain simulator: N freezers, random-walk temp, door-open warming events, QoS-configurable), `mqtt/sub.py` (observer with optional persistent session).

**Prediction (written before running):** persistent subscriber offline → QoS1 messages delivered on reconnect, QoS0 lost.
**Actual:** exactly that. QoS1: session_present=True, 5/5 received. QoS0: session_present=True, 0/5 received. Broker persistence + clean_session=False is what makes at-least-once survive a disconnect.

Smoke test: sim published 6 → observer received 6 with correct JSON. All verified.

**Next command to run:** Week 2 — add Kafka (KRaft, single broker) to compose, create a topic, produce/consume.

## 2026-09-11 — Week 2 done (Kafka)

Added single-broker KRaft Kafka to compose (dual listeners: kafka:9092 internal, localhost:29092 host). Two config traps hit and fixed: apache/kafka rejects 0.0.0.0 in advertised listeners (use empty host `:9092`); Git Bash mangles `/opt/...` paths (MSYS_NO_PATHCONV=1) and `docker exec` needs `-i` for piped stdin. Verified: created 3-partition topic, produced 3, consumed 3 from beginning. docs/02-kafka.md written.

**Next command to run:** Week 3 — build the MQTT→Kafka bridge container, run sensor sim, consume the `telemetry` topic.

## 2026-09-11 — Week 3 done (MQTT→Kafka bridge)

Built `bridge/` as a containerized service (paho-mqtt + confluent-kafka): subscribes sensors/# at QoS1, produces to `telemetry` keyed by device_id, acks=all + idempotence. Verified 12 readings from 3 devices flow host-sim → mosquitto → bridge → kafka. Consumer output proved key→partition ordering: each device's seqs grouped and in order within a partition. docs/03-bridge.md written.

**Next command to run:** Week 4 — MinIO + Iceberg REST catalog, land `telemetry` into an Iceberg table, inspect snapshots/time-travel.

## 2026-09-11 — Week 4 done (Iceberg lakehouse)

Added MinIO (S3) + bucket-init + Iceberg REST catalog (apache/iceberg-rest-fixture 1.9.2) + a PyIceberg micro-batching sink (`sink/`). Full pipeline verified end-to-end: 42 readings flowed sensor→mqtt→bridge→kafka→sink→Iceberg on MinIO. 3 snapshots; time travel confirmed (first snapshot=12 rows, current=42). Physical layout in MinIO inspected: metadata.json v0–v3, snap-*.avro manifest lists, *-m0.avro manifests, data/*.parquet. Chose PyIceberg over the Kafka Connect Iceberg sink (lighter, transparent) — noted as swap-in. `sink/query_table.py` inspects schema/snapshots/time-travel. docs/04-iceberg.md written.

Gotchas: never name a script `inspect.py` (shadows stdlib, breaks pydantic import); image tags must be real (iceberg-rest-fixture 1.9.2, not guessed 1.7.1).

**Next command to run:** Week 5 — Postgres registry + Debezium CDC → device history in Iceberg.

## 2026-09-11 — Week 5 done (Debezium CDC + capstone query)

Added Postgres (wal_level=logical, REPLICA IDENTITY FULL) with a seeded `devices` registry, Kafka Connect + Debezium Postgres connector (connect/devices-connector.json), and `registry-sink` landing CDC into coldchain.device_changes. Verified: 3 snapshot events + 1 UPDATE (freezer-02 site-B/Globex/-12 → site-C/Initech/-18) captured with full before/after. Capstone query (sink/capstone.py, DuckDB ASOF JOIN) attributes breaches to the owner at reading time: Initech 17, Acme Foods 7. This is the whole project working end to end. docs/05-cdc.md written.

**Next command to run:** Week 6 — Dremio query UI over the Iceberg catalog (has a web-UI setup step).
