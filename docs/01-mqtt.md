# Layer 1 — MQTT ingestion

The edge of the pipeline: freezer sensors publish to an MQTT broker, and anything
downstream subscribes. Publishers and subscribers never talk directly — the broker
decouples them, which is what lets a sensor keep publishing when the consumer is
down, and vice versa.

## Components

| File | Role |
|------|------|
| `mqtt/mosquitto.conf` | Broker config — listener on 1883, anonymous access, **persistence on** |
| `sim/sensor.py` | Cold-chain sensor fleet — N freezers, random-walk temperature, door-open warming events |
| `mqtt/sub.py` | Observer — subscribes and prints; `--persistent` keeps a broker-side session |

## Message shape

Topic `sensors/<device_id>/telemetry`, JSON payload:

```json
{"device_id": "freezer-00", "ts": 1789139265.15, "temp_c": -18.13, "door_open": false, "seq": 0}
```

Freezers hold a −18 °C setpoint. A door-open event warms the device by a few
degrees for several readings before it recovers — the transient breach signal the
final query hunts for.

## QoS is a durability contract, not a quality knob

MQTT quality-of-service picks how hard the broker works to deliver a message:

- **QoS 0 — at most once.** Fire and forget. No ack, no retry, no queue.
- **QoS 1 — at least once.** Retried until acknowledged; duplicates possible.
- **QoS 2 — exactly once.** A four-step handshake; no duplicates, most overhead.

### The experiment

A subscriber with a **persistent session** (`clean_session=False`) asks the broker
to hold its messages while it is offline — but the broker only queues **QoS 1/2**,
never QoS 0. To see it: register the session, take the subscriber offline, publish
while it's gone, reconnect.

| QoS | 5 messages published while offline | On reconnect (`session_present=True`) |
|-----|-----------------------------------|----------------------------------------|
| 1   | queued by broker                  | **5 / 5 received** |
| 0   | never queued                      | **0 / 5 received** |

Reproduce it:

```bash
# QoS 1 — register session, go offline
.venv/Scripts/python -u mqtt/sub.py --qos 1 --persistent --client-id cc-q1   # Ctrl-C after "connected"
.venv/Scripts/python sim/sensor.py --devices 1 --count 5 --qos 1             # publish while offline
.venv/Scripts/python -u mqtt/sub.py --qos 1 --persistent --client-id cc-q1   # reconnect → 5 queued msgs
```

Swap `1` for `0` and the reconnect delivers nothing.

### Why it matters here

A freezer that drops off Wi-Fi for 30 seconds must not lose the readings that
prove a breach. So telemetry is published at **QoS 1**: at-least-once. The cost is
possible duplicates, which the pipeline handles downstream by keying on
`(device_id, seq)` — the honest tradeoff, made explicit rather than hidden.

Two things `persistence true` in the broker config buys us: the queued messages
survive not just a subscriber restart but a **broker** restart too. Kill the
`tl-mosquitto` container mid-publish and the QoS 1 client reconnects and re-sends
its unacked messages — the same at-least-once guarantee, one layer down.
