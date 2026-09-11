"""Cold-chain sensor simulator.

Publishes temperature + door-open telemetry for a fleet of freezer devices to
MQTT, one JSON message per reading. This is the source of the whole Phase 1
pipeline: sensor -> MQTT -> bridge -> Kafka -> Iceberg -> Dremio.

Topic:   sensors/<device_id>/telemetry
Payload: {"device_id","ts","temp_c","door_open","seq"}

It is also the tool used for the Week 1 QoS experiments: --count and --qos let
you send a fixed number of messages at a chosen QoS while a subscriber is
offline, to see which guarantees survive.

Examples:
    # stream telemetry from 3 devices, one reading/sec each, forever
    python sim/sensor.py --devices 3 --interval 1

    # send exactly 5 messages at QoS 1 (for the offline-delivery experiment)
    python sim/sensor.py --devices 1 --count 5 --qos 1
"""
from __future__ import annotations

import argparse
import json
import random
import time

import paho.mqtt.client as mqtt

# Each freezer holds a setpoint; readings random-walk around it. A door-open
# event nudges the temperature up for a few readings, the way a real freezer warms
# when someone opens it — this is the signal the "threshold breach" query hunts for.
SETPOINT_C = -18.0
WALK_STEP = 0.3
DOOR_OPEN_PROB = 0.05
DOOR_WARMING_C = 4.0


def build_client(client_id: str, host: str, port: int) -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    client.connect(host, port, keepalive=60)
    client.loop_start()
    return client


def run(args: argparse.Namespace) -> None:
    client = build_client(f"sensor-sim-{random.randint(1000, 9999)}", args.host, args.port)

    # per-device state: current temp and how many more readings stay "warm"
    temps = {d: SETPOINT_C for d in range(args.devices)}
    warming = {d: 0 for d in range(args.devices)}

    seq = 0
    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            for d in range(args.devices):
                door_open = random.random() < DOOR_OPEN_PROB
                if door_open:
                    warming[d] = 5  # stay warm for ~5 readings after a door event
                # random walk, plus a warming offset while the door effect lingers
                temps[d] += random.uniform(-WALK_STEP, WALK_STEP)
                target = SETPOINT_C + (DOOR_WARMING_C if warming[d] > 0 else 0.0)
                temps[d] += (target - temps[d]) * 0.3
                warming[d] = max(0, warming[d] - 1)

                device_id = f"freezer-{d:02d}"
                payload = {
                    "device_id": device_id,
                    "ts": time.time(),
                    "temp_c": round(temps[d], 2),
                    "door_open": door_open,
                    "seq": seq,
                }
                info = client.publish(
                    f"sensors/{device_id}/telemetry",
                    json.dumps(payload),
                    qos=args.qos,
                )
                info.wait_for_publish()
                seq += 1
                sent += 1
                print(f"sent {device_id} temp={payload['temp_c']:>6}C door={door_open} qos={args.qos}")
                if args.count and sent >= args.count:
                    break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nstopping")
    finally:
        client.loop_stop()
        client.disconnect()
        print(f"published {sent} messages")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Cold-chain MQTT sensor simulator")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=1883)
    p.add_argument("--devices", type=int, default=3, help="number of freezer devices")
    p.add_argument("--interval", type=float, default=1.0, help="seconds between reading rounds")
    p.add_argument("--count", type=int, default=0, help="total messages to send (0 = forever)")
    p.add_argument("--qos", type=int, default=1, choices=[0, 1, 2])
    return p.parse_args()


if __name__ == "__main__":
    run(parse_args())
