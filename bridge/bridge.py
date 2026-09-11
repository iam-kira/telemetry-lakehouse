"""MQTT -> Kafka bridge.

Subscribes to the sensor topic on MQTT and republishes every message to a Kafka
topic, keyed by device_id so a device's readings keep their order (same key ->
same partition). This is the seam between the edge protocol (MQTT, lightweight,
lossy-by-choice) and the durable log (Kafka, replayable).

Delivery: MQTT in at QoS 1 (at-least-once) and Kafka out with acks=all. Both links
are at-least-once, so a duplicate is possible end to end; downstream dedupes on
(device_id, seq). Honest tradeoff, not hidden.

Config via env: MQTT_HOST, MQTT_PORT, KAFKA_BOOTSTRAP, KAFKA_TOPIC.
"""
from __future__ import annotations

import os
import sys

import paho.mqtt.client as mqtt
from confluent_kafka import Producer

MQTT_HOST = os.environ.get("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "telemetry")

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "acks": "all",             # wait for the broker to persist before considering it sent
    "enable.idempotence": True,  # no duplicate on producer-side retry
    "linger.ms": 20,           # small batching window for throughput
})

_delivered = 0
_failed = 0


def on_delivery(err, msg):
    global _delivered, _failed
    if err is not None:
        _failed += 1
        print(f"DELIVERY FAILED: {err}", file=sys.stderr)
    else:
        _delivered += 1


def on_connect(client, userdata, flags, reason_code, properties):
    print(f"bridge connected to MQTT rc={reason_code}; subscribing sensors/#")
    client.subscribe("sensors/#", qos=1)


def on_message(client, userdata, msg):
    # topic is sensors/<device_id>/telemetry — key by device_id for per-device ordering
    parts = msg.topic.split("/")
    device_id = parts[1] if len(parts) > 1 else "unknown"
    producer.produce(KAFKA_TOPIC, key=device_id, value=msg.payload, on_delivery=on_delivery)
    producer.poll(0)  # serve delivery callbacks without blocking
    if (_delivered + _failed) % 20 == 0 and _delivered:
        print(f"bridged: delivered={_delivered} failed={_failed}")


def main() -> None:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="mqtt-kafka-bridge")
    client.on_connect = on_connect
    client.on_message = on_message
    print(f"bridge: MQTT {MQTT_HOST}:{MQTT_PORT} -> Kafka {KAFKA_BOOTSTRAP} topic={KAFKA_TOPIC}")
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nflushing producer...")
    finally:
        producer.flush(10)
        print(f"final: delivered={_delivered} failed={_failed}")


if __name__ == "__main__":
    main()
