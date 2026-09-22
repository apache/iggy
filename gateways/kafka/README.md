# Kafka gateway (`iggy-gateway-kafka`)

Foundation layer for [apache/iggy#3421](https://github.com/apache/iggy/issues/3421): a TCP listener on the Kafka wire port that decodes requests, validates scoped API keys and versions, and returns stub responses.

> **Stub warning:** When you set `IGGY_KAFKA_BRIDGE_ENABLED=true`, Produce writes to Iggy and
> ListOffsets answers `EARLIEST`/`LATEST` from real partition state. No other API stores or reads
> real data. Produce and ListOffsets without a bridge, and Fetch with or without one, answer
> retriable `NOT_LEADER_OR_FOLLOWER` (6). Clients then keep their data and do not trust a fake
> success. CreateTopics answers `NOT_CONTROLLER` (41) and creates nothing. Metadata reports every
> topic as unknown, so a real client cannot reach Produce or ListOffsets yet. See
> [docs/SCOPE.md](docs/SCOPE.md).

## Run

```bash
cargo run -p iggy-gateway-kafka
```

Default bind: `127.0.0.1:9093`. Environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `IGGY_KAFKA_BIND_ADDR` | `127.0.0.1:9093` | TCP address to listen on |
| `IGGY_KAFKA_ADVERTISED_HOST` | bind IP | Hostname/IP clients use to reach this broker (required when binding to `0.0.0.0`/`::`) |
| `IGGY_KAFKA_ADVERTISED_PORT` | bind port | Port advertised in Metadata responses |
| `IGGY_KAFKA_MAX_CONNECTIONS` | `1024` | Maximum concurrent connections before new ones are rejected |
| `IGGY_KAFKA_MAX_FRAME_SIZE` | `8388608` | Maximum accepted request frame size in bytes |
| `IGGY_KAFKA_IDLE_TIMEOUT_SECS` | `600` | Seconds a connection may sit idle before the next frame's length prefix arrives |
| `IGGY_KAFKA_READ_TIMEOUT_SECS` | `15` | Seconds allowed to read a frame body once its length prefix arrives |
| `IGGY_KAFKA_WRITE_TIMEOUT_SECS` | `10` | Seconds allowed to write a response frame |
| `IGGY_KAFKA_SHUTDOWN_DRAIN_TIMEOUT_SECS` | `25` | Seconds graceful shutdown waits for in-flight connections before abandoning them |
| `IGGY_KAFKA_BRIDGE_ENABLED` | `false` | Connect the Iggy bridge at startup. While false every API answers with its stub, and the `IGGY_KAFKA_IGGY_*` variables below are read by nothing. A failed connection is fatal, not a downgrade to stubs. |

## Test

```bash
cargo test -p iggy-gateway-kafka
```

See [docs/TEST_SUITE.md](docs/TEST_SUITE.md) for the full suite catalog (`cargo test -p iggy-gateway-kafka -- --list` for the exact current test names - the count has drifted out of sync with the actual suites before, so it isn't pinned here).

Some `api_handler_tests`, `server_e2e_tests`, and `version_firewall_tests` cases require wire fixtures under `tools/kafka-tool/kafka_messages/` (gitignored locally; CI generates them via `scripts/ci-wire-fixtures.sh`):

```bash
./gateways/kafka/scripts/ci-wire-fixtures.sh generate
cargo test -p iggy-gateway-kafka
./gateways/kafka/scripts/ci-wire-fixtures.sh cleanup   # optional
```

Or generate only the keys the tests need:

```bash
for key in 0 1 2 19; do
  cargo run -p kafka-message-gen -- generate \
    --output gateways/kafka/tools/kafka-tool/kafka_messages \
    --api-key "$key"
done
```

## Manual testing

Before check-in, run the procedure in [docs/MANUAL_TESTING.md](docs/MANUAL_TESTING.md) (smoke, version firewall, kcat, adversarial cases).

## Scoped APIs

See [docs/SCOPE.md](docs/SCOPE.md) for [#3421](https://github.com/apache/iggy/issues/3421) deliverables, supported API key/version table, and post-foundation TODO backlog.

## Design decisions

- [docs/BRIDGE_MAPPING.md](docs/BRIDGE_MAPPING.md) — how a Kafka record becomes an Iggy message, and back
- [docs/IDEMPOTENCE.md](docs/IDEMPOTENCE.md) — InitProducerId, and why delivery is at-least-once
- [docs/OFFSET_STORAGE.md](docs/OFFSET_STORAGE.md) — where Kafka consumer group offsets live

### Delivery guarantees

Delivery through this gateway is **at-least-once**, and stays at-least-once across a gateway
restart. Transactions are not supported, and will not be. An idempotent Kafka producer is given
a producer id so that it starts, but its retries are not deduplicated: a retry after a network
timeout writes the record twice, and both copies reach the stream with their own offsets.

Iggy deduplicates writes on its own partition plane, and that does not close this gap, because it
guards the hop from the gateway to Iggy rather than the hop from the producer to the gateway.
[docs/IDEMPOTENCE.md](docs/IDEMPOTENCE.md) has the detail and what closing it needs.

## Iggy bridge ([#3533](https://github.com/apache/iggy/issues/3533))

`src/bridge/` is the SDK integration layer: connects to Iggy, maps Kafka topics to Iggy
streams/topics, provisions them on demand, and looks up high watermarks (one or many partitions of
a topic per call) for `ListOffsets`.
ListOffsets ([#3537](https://github.com/apache/iggy/issues/3537)) and Produce
([#3535](https://github.com/apache/iggy/issues/3535), see below) call it. Fetch does not call it
yet ([#3536](https://github.com/apache/iggy/issues/3536)).
Tested by `bridge`'s own unit tests, `tests/bridge_iggy_integration_tests.rs`,
`tests/list_offsets_real_bridge_tests.rs` and `tests/produce_real_bridge_tests.rs`. The last three
start a real `iggy-server`.

### Produce ([#3535](https://github.com/apache/iggy/issues/3535))

Each partition entry in a Produce request becomes one Iggy `send_messages` call. Each entry
answers for itself. A partition that fails carries its own error code, and the partitions that
worked keep their offsets.

| Field | What the gateway does |
| ------- | ----------------------- |
| Partition | Uses the index the client sent. Both systems number partitions from 0. |
| Base offset | Takes it from the send confirmation. Sends `-1` when the server confirms the write but names no offset. |
| `log_start_offset` | Always `-1`. The real value costs one extra call per request, and no producer reads it yet. |
| `acks` | Writes the same way for `0`, `1` and `-1`. Durability is the Iggy topic's setting. Any other value answers `INVALID_REQUIRED_ACKS` (21). |
| Topics | Creates none. A missing topic answers `UNKNOWN_TOPIC_OR_PARTITION` (3). Metadata creates topics. |
| `timeout_ms` | Honors it, up to 20 seconds. Partitions past the deadline answer `REQUEST_TIMED_OUT` (7). |
| Compression | Reads gzip, snappy, lz4 and zstd. |

`acks=0` still writes the records. It answers nothing, because the wire protocol forbids a reply.

Bad records get one of two codes. A record Iggy cannot hold answers `MESSAGE_TOO_LARGE` (10). A
record this gateway cannot map answers `INVALID_RECORD` (87). A transactional or control batch
answers `UNSUPPORTED_VERSION` (35), which is the code that stops a transactional producer. None of
the three is retriable, because the same bytes never succeed on a second try.

All batches in one request share one decompression budget, set to `max_frame_size`. A compressed
request therefore cannot produce more bytes than the same client could have sent uncompressed. A
request past the budget answers `MESSAGE_TOO_LARGE` (10).

[docs/BRIDGE_MAPPING.md](docs/BRIDGE_MAPPING.md) describes what a record becomes once it is stored.

### Connection config

| Variable | Default | Description |
| --- | --- | --- |
| `IGGY_KAFKA_IGGY_ADDR` | `127.0.0.1:8090` | Address of the Iggy server to bridge to |
| `IGGY_KAFKA_IGGY_USERNAME` | `iggy` | Iggy username |
| `IGGY_KAFKA_IGGY_PASSWORD` | none - **required** | Iggy password. No default: `iggy-server` only uses the well-known `iggy`/`iggy` root credentials when started with `--with-default-root-credentials` (dev-only); otherwise it generates a random password, so a hardcoded default here could never be right and would invite running as root unnoticed |
| `IGGY_KAFKA_IGGY_STREAM` | `kafka` | Default Iggy stream for a Kafka topic with no explicit mapping override |
| `IGGY_KAFKA_TOPIC_MAP_PATH` | unset | Path to a topic-mapping TOML file (see below); omit to use only the default rule |

The initial connect retries a fixed, bounded number of times (`RECONNECTION_RETRIES = 3`, not the
Iggy SDK client's own default of unlimited retries, one dial per second, forever), and the whole
attempt - retries included - is capped at `REQUEST_TIMEOUT` (15s) wall-clock, so `IggyBridge::connect`
fails in bounded time whether the address refuses the connection or silently drops it, instead of
blocking the calling task indefinitely. Every other bridge call (`ensure_stream_and_topic`,
`high_watermark(s)`, `close`) carries the same `REQUEST_TIMEOUT` for the same reason: the SDK
reconnects internally, mid-call, on a transport error, through the same undead-lined dial path - a
bridge call made well after the initial connect can still hit this if Iggy becomes unreachable
later.

This timeout only bounds the *caller's* wait, not the SDK's own work: the SDK writes and reads on
a detached background task specifically so that dropping the awaiting future - what this timeout
does on expiry - cannot abort it mid-flight. A timed-out call can leave that task holding the
shared client's connection lock for up to another 30s (the SDK's own reply deadline), queuing
every other bridge call behind it. `IggyBridge` holds one `IggyClient` with no pooling (see
Concurrency ceiling below) and no semaphore bounding concurrent bridge calls. ListOffsets
(`#3537`) and Produce (`#3535`) call it from real handlers, Produce on every write, so this is no
longer a hypothetical. CreateTopics (`#3538`), Metadata (`#3534`) and Fetch (`#3536`) add more
concurrent callers when they land. See `IggyBridge`'s own doc comment (its rustdoc is private, so
this isn't a followable link outside the crate - read the source at `src/bridge/iggy_bridge.rs`).

### Topic mapping

Default rule, no config file needed: a Kafka topic `orders` maps to Iggy stream
`IGGY_KAFKA_IGGY_STREAM` (default `kafka`), topic `orders` - the Kafka topic name carries over
unchanged. Override specific topics with a TOML file:

```toml
default_stream = "kafka"

[topics.orders]
stream = "billing"
topic = "orders_v2"

# A Kafka topic name containing dots needs the key quoted, or TOML parses it as nested
# tables ([topics.org] containing [apache] containing [kafka]) instead of one topic named
# "org.apache.kafka.events".
[topics."org.apache.kafka.events"]
stream = "billing"
topic = "kafka_events"
```

Point `IGGY_KAFKA_TOPIC_MAP_PATH` at the file to load it; topics not listed under `[topics.*]`
still fall back to the default rule.

`default_stream` is required in a map file - it has no `#[serde(default)]`, unlike `topics` -
so an override-only file with no `default_stream` key fails to load rather than falling back to
`kafka`. When both `IGGY_KAFKA_TOPIC_MAP_PATH` and `IGGY_KAFKA_IGGY_STREAM` are set, the file's own
`default_stream` always wins and the env var is ignored entirely: a TOML file is a complete mapping
document, not an overlay on top of the env var.

### Provisioning and idempotency

`ensure_stream_and_topic(kafka_topic, partition_count)` creates the mapped Iggy stream and topic
if either is missing. Idempotent when repeated with the *same* `partition_count`: a no-op if both
already exist with that count, and a `NameAlreadyExists` race against a concurrent caller creating
the same stream/topic is treated as success, not an error - the goal is "it exists," not "this
call created it." A *different* `partition_count` against an already-existing topic returns
`BridgeError::PartitionCountMismatch` rather than silently keeping the old count or growing it -
two concurrent callers requesting different counts for the same topic must not both see success.

Topics created this way have **no message expiry** - Iggy's own server default, not Kafka's 7-day
default. Nothing is bounding retention until it's configured explicitly (Iggy's own topic options,
outside this bridge today); repointing a Kafka app that assumes bounded retention onto this bridge
will accumulate data indefinitely unless you set that up yourself.

They also use Iggy's default **durability**, `Durability::Replicated` - quorum commit without an
additional stable-storage barrier, with the disk write itself threshold-gated (flushed at 1024
messages or 1 MiB of unflushed data, whichever comes first). Kafka's own defaults take the same
posture, so this isn't a wrong choice, but on a single node both can lose an acked write to a power
cut before that threshold is reached - worth knowing rather than discovering later.

### Concurrency ceiling

One `IggyBridge` (one `IggyClient`) is meant to serve every Kafka connection this gateway handles,
and the Iggy SDK's TCP transport is lockstep - one request in flight per client, its stream mutex
held across write, flush, and read. Every concurrent Kafka connection ends up serialized behind
whichever single Iggy request is in flight; the Kafka side's own connection limit
(`IGGY_KAFKA_MAX_CONNECTIONS`) does nothing to relieve this. No connection pooling exists yet, and
Produce puts this on a hot path as of `#3535` - it is a known gap, not a design decision to rely
on.

Message order does not depend on the pool. The connection loop reads one frame, runs the handler,
then writes the answer. One Kafka connection therefore never has two requests in flight, and a
producer's writes to a partition stay in the order it sent them. The single client costs
throughput instead. A producer set up for several requests in flight gains nothing from them, and
every Kafka connection waits behind the Iggy call that is running.

### Error mapping

`BridgeError::to_kafka_error_code()` maps Iggy failures to Kafka wire error codes:

- Stream, topic or partition not found → `UNKNOWN_TOPIC_OR_PARTITION` (3). This includes the
  generic `ResourceNotFound` that a partition request returns when the server cannot resolve it
- A rejected *permission* (`Unauthorized`) → `TOPIC_AUTHORIZATION_FAILED` (29) - a real,
  fixable-by-the-Kafka-operator ACL problem
- A rejected *login* (the bridge's own `IGGY_KAFKA_IGGY_USERNAME`/`_PASSWORD` are wrong) →
  `UNKNOWN_SERVER_ERROR` (-1), deliberately **not** 29 - the Kafka client can't fix a bridge-side
  credential misconfiguration, and blaming its own ACLs for one is worse than an unexplained
  fatal error
- Connection-shaped failures → `NOT_LEADER_OR_FOLLOWER` (6, the same retriable code the
  foundation's own stubs send, so a client backs off and retries)
- An Iggy commit whose outcome is genuinely unknown (`TransientNotCommitted`) →
  `REQUEST_TIMED_OUT` (7) - retriable in real Kafka too, chosen because it's what a real broker
  sends for the same shape of failure, not to make a client stop retrying
- A bridge-side call timeout (`BridgeError::Timeout`) → `REQUEST_TIMED_OUT` (7), the same code and
  the same reasoning as `TransientNotCommitted` above - the SDK's write/read run on a task this
  timeout cannot abort, so the outcome is unknown, not known-safe-to-retry (see Connection config's
  timeout caveat above)
- An invalid Kafka-side topic name (empty, whitespace-padded, oversized, illegal characters) →
  `INVALID_TOPIC_EXCEPTION` (17), checked before any Iggy call is made
- `PartitionCountMismatch` → `TOPIC_ALREADY_EXISTS` (36, not `INVALID_PARTITIONS` - that code's
  own text is "below 1", a different condition)
- Too many partitions requested (`TooManyPartitions`) → `INVALID_PARTITIONS` (37), reachable
  through `ensure_topic`'s `partition_count` argument once it exceeds the server's cap
- Anything else → `UNKNOWN_SERVER_ERROR` (-1)

### Server limits the gateway inherits

These are Iggy server limits, not gateway settings. A Kafka client cannot act on any of them, so
an operator has to.

| Limit | Default | Where |
| ------- | --------- | ------- |
| Consumer offset keys per partition, per consumer kind | 4096, ceiling 262144 | `partition.consumer_offsets_max` |
| One user header name, and one header value | 255 bytes | fixed, `user_headers.rs` |
| All user headers of one message | 100 KB | fixed, `MAX_USER_HEADERS_SIZE` |
| Message payload | 64 MB | fixed, `MAX_PAYLOAD_SIZE` |

Only the first is configurable. A Kafka consumer group commits one offset key per partition it
holds, so `partition.consumer_offsets_max` is what bounds the number of groups that can commit
against one partition. Passing it returns `TooManyConsumerOffsets` (3024), which reaches the
client as `UNKNOWN_SERVER_ERROR` because Kafka has no code for the condition. The gateway logs
the real Iggy error, so the server log is where an operator diagnoses it.

The other three decide when a Kafka record goes into the envelope instead of being stored
natively. See [docs/OFFSET_STORAGE.md](docs/OFFSET_STORAGE.md) and
[docs/BRIDGE_MAPPING.md](docs/BRIDGE_MAPPING.md).

## Wire fixture tool

See [tools/kafka-tool/README.md](tools/kafka-tool/README.md).
