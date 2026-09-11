# Layer 3 — MQTT → Kafka bridge

The seam between two worlds: MQTT (lightweight, device-friendly, lossy by choice)
and Kafka (durable, replayable, the system of record for the pipeline). The bridge
subscribes to `sensors/#` and republishes each message to the Kafka `telemetry`
topic.

## The one real decision: the key

```python
device_id = msg.topic.split("/")[1]
producer.produce("telemetry", key=device_id, value=msg.payload)
```

Kafka assigns a partition by hashing the key. Keying by `device_id` means:

- **Every reading from one freezer lands on the same partition** and therefore stays
  in order — essential, because a temperature series read out of order is a lie.
- **Different freezers spread across partitions**, so consumers parallelise.

You can watch it happen. Publish 12 readings from 3 devices and consume with keys:

```bash
.venv/Scripts/python sim/sensor.py --devices 3 --count 12 --qos 1
docker exec tl-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic telemetry --from-beginning \
  --max-messages 12 --property print.key=true --property key.separator=' | '
```

The consumer drains one partition at a time, so the output groups by device:

```
freezer-02 | {... "seq": 2}
freezer-02 | {... "seq": 5}      <- freezer-02's readings, in order, one partition
freezer-02 | {... "seq": 8}
freezer-02 | {... "seq": 11}
freezer-00 | {... "seq": 0}
freezer-01 | {... "seq": 1}      <- freezer-00 and -01 share another partition,
freezer-00 | {... "seq": 3}         each still in its own order
...
```

## Delivery guarantees

Both links are **at-least-once**:

- MQTT in at **QoS 1** — the broker retries until the bridge acks.
- Kafka out with **`acks=all` + `enable.idempotence`** — the producer waits for the
  broker to persist, and its own retries don't create duplicates.

End to end a duplicate is still possible (e.g. the bridge crashes after producing
but before the MQTT ack). Rather than pretend otherwise, downstream dedupes on
`(device_id, seq)`. Naming the guarantee and handling it beats assuming
exactly-once you don't have.

## Why a service, not a script

The bridge runs as a container on the compose network, reaching `mosquitto:1883`
and `kafka:9092` by name (Layer-1 DNS lesson, cashed in). It restarts with the
stack and has no host dependencies — the same shape every other pipeline service
takes.
