"""MQTT observer — subscribe to a topic and print what arrives.

Used to watch the sensor stream and to run the Week 1 QoS experiments. With
--persistent the subscriber keeps its broker-side session (clean_session=False),
so QoS 1/2 messages published while it is offline are queued and delivered on
reconnect — the mechanism that makes "at least once" mean something.

Examples:
    # watch everything
    python mqtt/sub.py --topic 'sensors/#'

    # register a persistent session at QoS 1, then Ctrl-C; publish while offline;
    # rerun to receive the queued messages
    python mqtt/sub.py --topic 'sensors/#' --qos 1 --persistent
"""
from __future__ import annotations

import argparse

import paho.mqtt.client as mqtt


def on_connect(client, userdata, flags, reason_code, properties):
    topic = userdata["topic"]
    qos = userdata["qos"]
    print(f"connected rc={reason_code} session_present={flags.session_present}; "
          f"subscribing {topic!r} at QoS {qos}")
    client.subscribe(topic, qos=qos)


def on_message(client, userdata, msg):
    userdata["received"] += 1
    print(f"[{userdata['received']:>3}] {msg.topic} qos={msg.qos} {msg.payload.decode()}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MQTT observer")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=1883)
    p.add_argument("--topic", default="sensors/#")
    p.add_argument("--qos", type=int, default=0, choices=[0, 1, 2])
    p.add_argument("--persistent", action="store_true",
                   help="keep broker-side session so offline QoS1/2 msgs are queued")
    p.add_argument("--client-id", default="observer-1",
                   help="stable id is required for a persistent session to be found again")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    userdata = {"topic": args.topic, "qos": args.qos, "received": 0}
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=args.client_id,
        clean_session=False if args.persistent else None,
    )
    client.user_data_set(userdata)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(args.host, args.port, keepalive=60)
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print(f"\nstopping; received {userdata['received']} messages")


if __name__ == "__main__":
    main()
