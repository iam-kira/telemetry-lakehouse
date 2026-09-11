# Layer 2 — Kafka

MQTT is great at the edge but it is not a store. Kafka is the durable, replayable
log in the middle of the pipeline: messages are appended to **topics**, retained
for a configured time, and any number of consumers read them independently at
their own offset. If the Iceberg sink is down for an hour, the data waits in Kafka;
when it comes back it reads from where it left off.

## What's running

A **single broker in KRaft mode** — Kafka's own Raft implementation for metadata,
so there is no separate ZooKeeper. One process is both controller and broker. This
is a deliberate, memory-driven simplification (a real cluster is 3+ brokers); the
concepts — partitions, offsets, consumer groups — are identical.

### Listeners

Kafka tells clients where to reconnect via *advertised listeners*, so a single
address doesn't work for both in-container and host clients. Two listeners:

| Listener | Advertised as | Used by |
|----------|---------------|---------|
| INTERNAL | `kafka:9092` | other containers (bridge, Kafka Connect) |
| EXTERNAL | `localhost:29092` | tools on the host (dev scripts, CLI) |

A gotcha worth remembering: the `apache/kafka` image rejects `0.0.0.0` in a
listener's advertised address — bind with an empty host (`INTERNAL://:9092`) and
advertise a routable name.

## Verify it

```bash
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: stop it rewriting /opt/... paths
K="docker exec tl-kafka /opt/kafka/bin"

$K/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic demo --partitions 3 --replication-factor 1
printf 'a\nb\nc\n' | docker exec -i tl-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic demo
$K/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic demo --from-beginning --max-messages 3
```

Two Windows-specific traps this shook out, both worth internalizing:

- **Path mangling** — Git Bash converts a leading `/opt/...` into a Windows path
  before `docker exec` sees it. `MSYS_NO_PATHCONV=1` disables that.
- **stdin** — `docker exec` needs `-i` to attach a pipe; without it the console
  producer reads nothing and the topic stays empty.

## Partitions and keys (why this matters for the pipeline)

A topic is split into partitions; ordering is guaranteed only *within* a partition.
The producer chooses a partition by hashing the message **key**. The bridge (next
layer) keys telemetry by `device_id`, so every reading from one freezer lands on
the same partition and stays in order — while different freezers spread across
partitions for parallelism. Key choice is a real design decision: key by something
too coarse and one partition gets hot; too fine and you lose useful ordering.
