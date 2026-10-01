# Kafka gateway demo

`demo.sh` runs a live demo of the Kafka gateway in seven steps, in about five minutes. Stock Kafka clients create a
topic, produce and consume through the gateway. The Iggy CLI then reads the same records, and it writes a message that
a Kafka consumer reads.

The demo needs the Fetch handler from PR #4336. Until that PR merges, run the demo from the `kafka-fetch` branch.

## Requirements

- Linux, because the Kafka clients run in containers on the host network.
- Docker. If `apache/kafka:3.9.0` or `edenhill/kcat:1.7.1` is missing, the script pulls it.
- Free ports 8090 for Iggy and 9093 for the gateway. `DEMO_IGGY_PORT` and `DEMO_KAFKA_PORT` change them.
- Release builds of the server, the CLI and the gateway.

## Before the meeting

1. Build the binaries from the repository root:

   ```bash
   cargo build --release --bin iggy-server --bin iggy --bin iggy-gateway-kafka
   ```

2. Run the rehearsal. It runs every step without pauses, compares each output with the expected one, and then stops
   the stack.

   ```bash
   gateways/kafka/demo/demo.sh check
   ```

   The last line must be `Rehearsal passed: every step produced the expected output.`

3. Open two terminals. Put the main terminal at the top of the shared screen, and the second terminal below it, about
   12 lines high.

4. Set a font size that the audience can read. The main terminal must stay at least 180 columns wide, because step 5
   prints a wide table. If it is narrower, `run` shows a warning on the title card.

## Run the demo

1. In the main terminal, start the walkthrough:

   ```bash
   gateways/kafka/demo/demo.sh run
   ```

2. When the title card appears, start the live Kafka consumer in the second terminal:

   ```bash
   gateways/kafka/demo/demo.sh tail
   ```

3. Press Enter to go forward. Each step waits for Enter before it starts, and again before each command runs. The
   output of a command stays on the screen until the next Enter.

4. After the questions, stop everything and delete the demo data:

   ```bash
   gateways/kafka/demo/demo.sh down
   ```

## Talk track

### Title card

The gateway lets an application with a Kafka client keep its data in Iggy, with no code change. The application only
points its bootstrap server at the gateway. Phase 1 is the end-to-end flow: create a topic, produce and consume.
Consumer groups, security and performance come after it.

### Step 1: Start Iggy and the Kafka gateway

Iggy starts as usual. The gateway is a separate binary. It speaks the Kafka wire protocol to clients, and it connects to
Iggy as an ordinary Iggy client.

Point at `Iggy bridge connected` and at `kafka listener bound on 127.0.0.1:9093`.

### Step 2: Create a topic with kafka-topics.sh

`kafka-topics.sh` is the admin tool that ships with Kafka. The gateway turns its CreateTopics request into an Iggy
stream named `kafka` with a topic named `orders`.

Point at the `Partitions count` row of the Iggy table. It shows 3, the count that `kafka-topics.sh` asked for.

### Step 3: Produce with two different Kafka clients

Two client families cover most of the Kafka ecosystem. One is the Java client. The other is librdkafka, which the
Confluent clients for Python, Go, .NET and JavaScript wrap. The Java console producer is idempotent by default, so it
also gets a producer id from the gateway.

Point at the input lines. Each record has a key, such as `order-1004`, and a `source` header.

### Step 4: Consume with kcat

kcat reads every partition from the beginning. It stops at the end of each partition, which it learns from the high
watermark that Fetch returns.

Point at the partition column. The Java client hashed both of its keys to partition 0, and kcat sent `order-1006` to
partition 2. The client picks the partition, the same as with a Kafka broker.

### Step 5: Read the same records with the Iggy CLI

There is one copy of the data. Each Kafka record is a native Iggy message, so Iggy tools, SDKs and connectors can use
it.

Point at three things. The Offset column shows 0 and 1, the same offsets that kcat printed. The Payload column holds
the Kafka value. The `kafka.key` and `kafka.h.source` headers hold the Kafka key and header.

### Step 6: Write with the Iggy CLI

This is the other direction. A service that uses Iggy directly writes a message, and a Kafka application reads it.

Point at the second terminal. The message appears there at once, with no key and with the header `source=iggy-cli`.

### Step 7: Read it with the Java consumer, from a chosen offset

The Java consumer assigns itself partition 0 and starts at offset 1. A consumer that keeps track of its own offsets
works today.

Point at offset 1, which the Java producer wrote, and offset 2, which the Iggy CLI wrote. The key of the Iggy message is
`null`, and its header arrives as a Kafka header.

### Closing card

This completes the end-to-end part of Phase 1. Next are consumer group offsets (#3542), so that `--group` consumers
work. After that come the Docker Compose quick start and the CI end-to-end job (#3539).

## Questions to expect

Can a consumer group read the topic? Not yet. Group membership works: join, sync, heartbeat and leave. Offset commit
and offset fetch are #3542. Until then, a consumer with `--group` stops with
`The node does not support OFFSET_FETCH`.

How fast is it? Nobody measured it yet, and the throughput work did not start. Today one Iggy client carries every
Produce request, one request at a time. Do not quote a number.

What are the delivery guarantees? At least once. An idempotent producer starts, but a retry after a timeout can write
a record twice. The gateway refuses transactions on purpose.

Is it secure? SASL/PLAIN sign-in can be turned on, and the user names and passwords are Iggy users. The Kafka listener
has no TLS yet, so PLAIN is for trusted networks only.

Which Kafka clients work? The gateway accepts Produce from version 3 and Fetch from version 4, which Kafka 0.11 and
newer clients use. This demo shows the Java client 3.9 and librdkafka.

Do topics delete old data? No. A topic that the gateway creates has no message expiry. That is the Iggy default, not
the Kafka default of seven days.

Can several gateways serve one Iggy cluster? For produce and fetch, yes, with a different `IGGY_KAFKA_INSTANCE_ID` on
each gateway. All consumers of one group must use the same gateway, because group membership is in the memory of one
gateway. `docs/CONSUMER_GROUPS.md` has the reason.

## Do not run these live

| Command | What happens |
| ------- | ------------ |
| `kafka-console-consumer.sh` without `--partition` | It joins a group, then stops with `The node does not support OFFSET_FETCH`. |
| `kafka-topics.sh --describe` | It needs DescribeConfigs, which the gateway does not support. `kcat -b 127.0.0.1:9093 -L -t orders` shows the partitions. |
| `kafka-topics.sh --delete` | It stops with `The node does not support DELETE_TOPICS`. |
| `kafka-consumer-groups.sh` | It stops with `The node does not support LIST_GROUPS`. |
| `kafka-producer-perf-test.sh` | A local run reached about 1,800 records per second with 200-byte records, because one Iggy client carries every Produce request. |

## If something goes wrong

- If a step fails, the script prints the step number and the paths of both logs. Fix the cause, then run
  `demo.sh run N` to repeat step N.
- If you press Ctrl-C, Iggy and the gateway keep running. Run `demo.sh run N` to continue at step N.
- If the script reports that a port is in use, stop the program on that port. You can also set `DEMO_IGGY_PORT` and
  `DEMO_KAFKA_PORT`.
- If the script warns that a binary is older than the last commit, rebuild the binaries.
- To start again with empty data, run `demo.sh run`. Step 1 always deletes the old demo data first.

## Commands

| Command | What it does |
| ------- | ------------ |
| `run [STEP]` | Runs the walkthrough. Step 1 starts a fresh stack. A later step continues on the running stack. |
| `tail` | Follows every partition of the topic with kcat. After a gateway restart, it starts again. |
| `check` | Runs all steps without pauses, compares the outputs, and stops the stack. |
| `up` | Starts a fresh iggy-server and gateway, without the walkthrough. |
| `status` | Shows what runs, and where the logs are. |
| `down` | Stops everything and deletes the demo data. |

## What the script runs

The Kafka tools run from the official images, with `docker run --network host`. On the screen, the script shows each
command without the `docker run` part.

iggy-server and the gateway run from `target/release` with an empty environment, so variables from your shell do not
reach them. The screen shows their main settings only. The script also sets the data directory, turns off the HTTP,
QUIC and WebSocket listeners, and limits the server to four shards.

The script keeps the data, the logs and the process ids in `${TMPDIR:-/tmp}/iggy-kafka-demo`. The logs are
`iggy-server.log` and `gateway.log`. `down` deletes the directory.
