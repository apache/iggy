# Kafka gateway (`iggy-gateway-kafka`)

Foundation layer for [apache/iggy#3421](https://github.com/apache/iggy/issues/3421): a TCP listener on the Kafka wire port that decodes requests and validates scoped API keys and versions. With a bridge, Produce, Fetch, ListOffsets, Metadata and CreateTopics use Iggy. InitProducerId and consumer group coordination work with or without one.

> **Stub warning:** with `IGGY_KAFKA_BRIDGE_ENABLED=true`, Produce, Fetch, ListOffsets, Metadata and
> CreateTopics use Iggy. With the bridge off (the default), those five are stubs: Produce, Fetch and
> ListOffsets answer retriable `NOT_LEADER_OR_FOLLOWER` (6), CreateTopics answers `NOT_CONTROLLER`
> (41), and Metadata reports every requested topic unknown. **CreateTopics runs as the bridge's own
> Iggy user**: with the bridge on and `IGGY_KAFKA_SASL_ENABLED` off (the default), any client that
> can reach this port can create topics (up to 1000 partitions each). Every client reads and writes
> as the bridge's Iggy user. See [docs/SCOPE.md](docs/SCOPE.md).
>
> InitProducerId does real work too, with or without the bridge: it allocates a producer id, so a stock idempotent producer starts instead of failing at startup.
>
> Consumer group coordination is not a stub either: `FindCoordinator`, `JoinGroup`, `Heartbeat`,
> `LeaveGroup` and `SyncGroup` are real, with real membership, rebalances, graceful leave and
> session expiry ([docs/CONSUMER_GROUPS.md](docs/CONSUMER_GROUPS.md)). With the bridge off,
> Metadata reports every topic unknown, so a consumer joins a group and is assigned 0 partitions.
> Offset commit and fetch are not implemented yet, so a consumer must use `assign()` with explicit
> start offsets and `enable.auto.commit=false`. The Java client needs no `group.id`. librdkafka
> refuses `assign()` without one, so set any value there.

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
| `IGGY_KAFKA_INSTANCE_ID` | `0` | This gateway's number among the gateways fronting one Iggy cluster. It is the high half of every producer id `InitProducerId` hands out, and Kafka requires those to be cluster-unique, so give every gateway its own value. A single gateway can leave it at `0`. |
| `IGGY_KAFKA_BRIDGE_ENABLED` | `false` | Connect the Iggy bridge at startup. While false every API answers with its stub, and the `IGGY_KAFKA_IGGY_*` variables below are read by nothing. A failed connection is fatal, not a downgrade to stubs. |
| `IGGY_KAFKA_SASL_ENABLED` | `false` | Require SASL/PLAIN authentication before serving any other API (`true` or `false`, nothing else) |
| `IGGY_KAFKA_PRE_AUTH_TIMEOUT_SECS` | `15` | Seconds an unauthenticated connection may sit between frames. Waiting for an authentication slot and the verification itself each get this budget, the verification's starting once it holds a slot. Separate from the 10-minute idle timeout that applies once authenticated |
| `IGGY_KAFKA_MAX_CONCURRENT_AUTHENTICATIONS` | `4` | Credential verifications the gateway runs at once, across all connections. Each costs a password hash on an Iggy shard thread. This bounds the gateway's side only: a check that times out frees its slot while its hash keeps running inside Iggy. Size it below the Iggy node's shard count |

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
for key in 0 1 2 10 11 12 13 14 19 22; do
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
- [docs/CONSUMER_GROUPS.md](docs/CONSUMER_GROUPS.md) — group membership, rebalances, and why one gateway per bootstrap endpoint
- [docs/AUTHENTICATION.md](docs/AUTHENTICATION.md) — how a Kafka client authenticates, and why PLAIN only
- [docs/ACL_MAPPING.md](docs/ACL_MAPPING.md) — how Iggy permissions are described as Kafka ACLs

### Delivery guarantees

Delivery through this gateway is **at-least-once**, and stays at-least-once across a gateway
restart. Transactions are not supported, and will not be. An idempotent Kafka producer is given
a producer id so that it starts, but its retries are not deduplicated: Produce stores idempotent
batches but ignores their producer id, epoch and sequence, so a retry after a network timeout
writes the record twice, and both copies reach the stream with their own offsets.

Iggy deduplicates writes on its own partition plane, and that does not close this gap, because it
guards the hop from the gateway to Iggy rather than the hop from the producer to the gateway.
[docs/IDEMPOTENCE.md](docs/IDEMPOTENCE.md) has the detail and what closing it needs.

Transactions are **not supported**, and will not be. AddPartitionsToTxn (24), AddOffsetsToTxn
(25), EndTxn (26) and TxnOffsetCommit (28) are never advertised, so a conforming client never
sends one; `InitProducerId` with a `transactional_id` answers `UNSUPPORTED_VERSION` (35); and a
Produce request carrying a `transactional_id` gets `UNSUPPORTED_VERSION` (35) on every partition
rather than having its records stored as if they were ordinary ones. None of those closes the
connection. `docs/SCOPE.md`'s Transactions section has the ordering and the reasoning.

## Authentication ([#3549](https://github.com/apache/iggy/issues/3549))

Off by default. With `IGGY_KAFKA_SASL_ENABLED=true` the gateway requires SASL/PLAIN before it serves
any other API, and it verifies the credentials by logging into Iggy with them.

The username and password a Kafka client sends are an **Iggy** username and password. There is no
mapping table and no credential store in the gateway: create an Iggy user for each Kafka principal
and point the client at it. Credentials are verified against `IGGY_KAFKA_IGGY_ADDR`.

```bash
IGGY_KAFKA_SASL_ENABLED=true IGGY_KAFKA_IGGY_ADDR=127.0.0.1:8090 cargo run -p iggy-gateway-kafka
```

Transport security to Iggy is configured separately from the Kafka side, because the two protect
different hops. These variables cover only the connection the credential check makes, and are
refused while SASL is off. The bridge's own client connects without TLS at any setting, and that
connection carries `IGGY_KAFKA_IGGY_PASSWORD` in the clear:

| Variable | Default | Description |
| --- | --- | --- |
| `IGGY_KAFKA_IGGY_TLS_ENABLED` | `false` | Encrypt the credential check's connection to Iggy (`true` or `false`, nothing else). The bridge connection is not covered. Required if the Iggy server only accepts TLS, otherwise every verification fails as unreachable |
| `IGGY_KAFKA_IGGY_TLS_DOMAIN` | derived from the address | Name checked against the Iggy server certificate |
| `IGGY_KAFKA_IGGY_TLS_CA_FILE` | SDK bundled roots | PEM roots to trust. Note the SDK does not use the system trust store |

Client side, for example with `kcat`:

```bash
kcat -b 127.0.0.1:9093 -X security.protocol=SASL_PLAINTEXT -X sasl.mechanisms=PLAIN \
     -X sasl.username=alice -X sasl.password=s3cret -L
```

Four things to know before switching it on:

- **PLAIN sends the password in the clear**, on the Kafka hop and on the Iggy hop. The gateway
  listener has no TLS yet, so this is only safe on a trusted network until that lands. SCRAM cannot
  be offered at all, because Iggy stores one Argon2 hash per user and SCRAM needs PBKDF2-derived
  keys that cannot come from it.
- **Enabling it breaks every existing client at once.** The two SASL keys appear in the
  `ApiVersions` advertisement only while it is on, and unauthenticated clients are refused.
- **Every connection costs a login**, meaning one password hash on an Iggy shard thread and one
  replicated registration. Verification is deliberately not cached, since caching it per username
  would let a second connection present any password. Connection churn is therefore server load.
  `IGGY_KAFKA_MAX_CONCURRENT_AUTHENTICATIONS` bounds the checks the gateway runs at once, and a peer
  whose login was rejected is refused for a delay that doubles per rejection, from 0.5s up to 30s.
- **Authentication and an ACL view, not enforcement.** The gateway verifies the credentials and
  can describe what Iggy grants the principal, but no operation checks those grants. Produce and
  Fetch act as the bridge's Iggy user for every client, so any client that signs in reads and
  writes what that user can.

### ACLs

`DescribeAcls` renders the authenticated principal's Iggy permissions as Kafka ACL bindings, so
`kafka-acls.sh --list` works against the gateway. It is read only: `CreateAcls` and `DeleteAcls`
are not implemented and not advertised.

`--list` here is a snapshot of the caller's own grants, not the broker-wide dump it is against a
Kafka cluster. A principal sees its own permissions and nobody else's, because the gateway holds no
administrative credentials, so filtering on another `User:` returns an empty listing whatever that
user holds, and root's listing is root's grants rather than a catalog of every binding. Only global permissions are rendered, as wildcard bindings, and the view is a snapshot
taken when the connection authenticated, so a permission changed afterwards is invisible until the
client reconnects. [docs/ACL_MAPPING.md](docs/ACL_MAPPING.md) has the mapping table and what is
deliberately left out.

Full reasoning, including what was rejected and why, is in
[docs/AUTHENTICATION.md](docs/AUTHENTICATION.md).

## Iggy bridge ([#3533](https://github.com/apache/iggy/issues/3533))

`src/bridge/` is the SDK integration layer: connects to Iggy, maps Kafka topics to Iggy
streams/topics, provisions them on demand, looks up high watermarks (one or many partitions of
a topic per call) for `ListOffsets`, and probes and polls partitions for Fetch.
Produce ([#3535](https://github.com/apache/iggy/issues/3535)), Fetch ([#3536](https://github.com/apache/iggy/issues/3536)), ListOffsets
([#3537](https://github.com/apache/iggy/issues/3537)), Metadata ([#3534](https://github.com/apache/iggy/issues/3534)) and CreateTopics
([#3538](https://github.com/apache/iggy/issues/3538)) call it.
Tested by `bridge`'s own unit tests, `tests/bridge_iggy_integration_tests.rs` and the
`tests/*_real_bridge_tests.rs` suites. All but the unit tests start a real `iggy-server`.

### Produce ([#3535](https://github.com/apache/iggy/issues/3535))

One partition, one Iggy send. Each partition answers for itself.

| Field | Gateway |
| ----- | ------- |
| Partition | Index as sent. Both count from 0. |
| Base offset | From the send confirmation. `-1` if none. |
| `log_start_offset` | Always `-1`. |
| `acks` | `0`, `1`, `-1` write the same. Other values: 21, before any per-partition check. |
| `acks=0` | Writes, answers nothing. Any failed partition closes the connection. |
| Topics | Creates none. Missing: 3. Bad name: 17. |
| `timeout_ms` | Honored, max 20 s. Past it: 7. |
| Compression | gzip, snappy, lz4. zstd from v7, else 76. |
| Producer id, epoch, sequence | Ignored, so a retry writes twice. |
| Timestamps over about 71 min apart | Clamped into one send. `kafka.ts` keeps the real one. |
| Several batches in one partition | 87, as Kafka. |
| Bytes after the batch | 87. |
| More records than the batch declares | 87, as Kafka. |
| Same partition twice in one request | Written twice. Kafka keeps the last. |
| Repeated header name in a record | 87. |
| Produce v0-2, `acks=0` | Closes the connection. |
| Undecodable request | Closes the connection. |

| Code | When | Client |
| ---- | ---- | ------ |
| 10 | Record, send or partition too large, even alone | Java splits multi-record batches. Else fails. |
| 87 | Record the gateway cannot map. Reason in `error_message` from v8. | Fails. |
| 35 | Transactional or control batch | Fails. |
| 6 | Request budget ran out. Nothing written. | Retries. |
| 7 | Deadline passed, or connection lost mid-send. May be written. | Retries. Can duplicate. |

Partition cap: `max_frame_size` decompressed bytes, `max_frame_size / 64` record slots, 3 headers
per slot. Past it: 10.

Request budget: 8 partition caps, refused partitions included. Past it: 6 for the rest, not
decoded. 4 requests decode at once.

[docs/BRIDGE_MAPPING.md](docs/BRIDGE_MAPPING.md) describes what a record becomes once it is stored.

### Fetch ([#3536](https://github.com/apache/iggy/issues/3536))

Per request: one probe per topic, shared with other Fetches, then polls of each partition with
records, in request order. One uncompressed batch per partition.

| Field | Gateway |
| ----- | ------- |
| Partition, offset | As sent. Both systems count from 0. |
| Iggy replica | The one on the node each bridge client is on. Every client signs in at the metadata leader, and a view change or a refused call can move it. So in a cluster, a probe and a poll can read nodes that are not at the same offset. |
| Offset below the oldest kept one | Reads from the oldest kept one. If none is kept, no records until the next write, because a cluster replica that lost records after a crash reads the same. Kafka answers `1` here, so `auto.offset.reset=none` gets no error. |
| Purge | Iggy restarts the partition at offset 0, and Fetch cannot see it. Seek consumers to 0 after a purge. Otherwise writes past a consumer's old offset make it skip the records below. |
| `high_watermark` | One past the last committed offset. |
| `last_stable_offset` | Same as `high_watermark`. No transactions. |
| `log_start_offset` | Always `-1`. |
| Sessions | None opened. `session_id` is always `0`, so every request is a full one. |
| `max_wait_ms` | Honored, max 18 s. |
| `min_bytes` | Honored, as Kafka does: the request waits until the records read reach it. New bytes are estimated from Iggy's average message size. |
| `max_bytes` | Honored, max `IGGY_KAFKA_MAX_FRAME_SIZE`. |
| `partition_max_bytes` | Honored. |
| First partition with records | At least one record, even past every byte limit (KIP-74). |
| Records per partition | At most 1000 per request, polled up to 16 at a time. |
| `isolation_level`, replica id, epochs, rack, forgotten topics | Ignored. |

| Code | When | Java consumer |
| ---- | ---- | ------------- |
| `OFFSET_OUT_OF_RANGE` (1) | Offset below 0. Or an offset still past `high_watermark` after 30 s of 6 on the same connection, counted from its first 6 there. A read there in range starts the 30 s over. A node that lags clears within them. A partition with no offset past 0 never answers 1, because Iggy reports that while it loads, fences or rebuilds one. | Resets its offset (`auto.offset.reset`). |
| `UNKNOWN_TOPIC_OR_PARTITION` (3) | Topic or partition not in Iggy, or a name Kafka refuses | Refreshes metadata, retries. |
| `NOT_LEADER_OR_FOLLOWER` (6) | Iggy unreachable, too slow, not signed in, loading the partition, returning no records where it reports some, or an offset past the end (see 1). Alone, it goes out after `max_wait_ms`. | Refreshes metadata, retries. |
| `TOPIC_AUTHORIZATION_FAILED` (29) | The bridge's Iggy user lacks permission. Fetch reads as that user, not as the client. | Fails. |
| `UNKNOWN_SERVER_ERROR` (-1) | Anything else, such as a stored message the gateway cannot map. Alone, it goes out after `max_wait_ms`. Each connection reads the partition again at most once per `max_wait_ms`. | Retries the same offset. |
| `FETCH_SESSION_ID_NOT_FOUND` (70) | Top level. The request continues a session. | Sends a full request. |

The Java consumer throws on any other partition code, so Fetch sends none.

Limits:

- 20 s per request, wait included, so a waiting Fetch fits the 25 s shutdown drain. Partitions
  not read by then answer `0` with no records.
- 4 requests read and encode at once. A waiting request, the probes that decide empty polls, or a
  response on a slow socket hold no slot. So response memory grows with connections: `max_bytes`
  each, or one larger record.
- A message the gateway cannot map stops the consumer at its offset. Records before it are served.
  The log shows it once per connection at `warn`.
- Each slot polls on its own Iggy client, which connects at the slot's first poll. A poll ends at
  the request deadline, so a partition that Iggy refuses holds up only its own slot. The SDK can
  hold that connection for up to 30 s more, so the slot drops the client and connects a new one.
- Iggy polls by count, not bytes. One poll can load 16 of the largest messages: about 1 GB, and
  about 4 GB over the 4 slots. It also costs polls: 1000 small records take 63. When Iggy polls
  take a byte cap, both limits go away.
- Probes run on one more Iggy client of their own, so they never wait behind a Produce. A request
  waits at most 2 s for its probes, then reads with what it has.
- A request loads at most 100 new topic probes per round, the oldest first. Other topics use an
  older probe. A waiting request probes 100 topics per round, in turn.
- An empty poll waits for a probe of its topic that starts after it. A read takes one such probe
  per topic, after its last empty poll there, for at most 100 topics. The other empty polls answer
  no records.
- A partition with no room left in `max_bytes` gets no poll, unless the request still owes its
  first record.
- A probe that fails with 6 or -1 leaves the last good probe in use for up to 30 s, and the polls
  decide. That probe never proves a consumer caught up, so an offset at its end gets a poll too.
- A partition that Iggy reports as loading answers 6 for 30 s, counted for the whole gateway.
  After that, Fetch polls it and the log warns once. Counters that drift read the same way.
  `ListOffsets` answers 6 for it all along, and warns too.
- A Produce through the gateway wakes a waiting Fetch of that topic at once. That includes a
  Produce that ends during the Fetch's first read. A native Iggy write wakes it at the next probe,
  within about 200 ms.

### Connection config

| Variable | Default | Description |
| --- | --- | --- |
| `IGGY_KAFKA_IGGY_ADDR` | `127.0.0.1:8090` | Address of the Iggy server to bridge to |
| `IGGY_KAFKA_IGGY_USERNAME` | `iggy` | Iggy username |
| `IGGY_KAFKA_IGGY_PASSWORD` | none - **required** | Iggy password. No default: `iggy-server` only uses the well-known `iggy`/`iggy` root credentials when started with `--with-default-root-credentials` (dev-only); otherwise it generates a random password, so a hardcoded default here could never be right and would invite running as root unnoticed |
| `IGGY_KAFKA_IGGY_STREAM` | `kafka` | Default Iggy stream for a Kafka topic with no explicit mapping override |
| `IGGY_KAFKA_TOPIC_MAP_PATH` | unset | Path to a topic-mapping TOML file (see below); omit to use only the default rule |
| `IGGY_KAFKA_IGGY_MAX_MESSAGE_SIZE` | `64MiB` | Iggy's `message_bus.max_message_size`. Set both together. Larger partitions answer 10 |

The initial connect retries a fixed, bounded number of times (`RECONNECTION_RETRIES = 3`, not the
Iggy SDK client's own default of unlimited retries, one dial per second, forever), and the whole
attempt - retries included - is capped at `REQUEST_TIMEOUT` (15s) wall-clock, so `IggyBridge::connect`
fails in bounded time whether the address refuses the connection or silently drops it, instead of
blocking the calling task indefinitely. Every other bridge call (`ensure_stream_and_topic`,
`high_watermark(s)`, `probe`, `close`) carries the same `REQUEST_TIMEOUT` for the same
reason: the SDK reconnects internally, mid-call, on a transport error, through the same
undead-lined dial path - a bridge call made well after the initial connect can still hit this if
Iggy becomes unreachable later.

`send_records` uses the Produce deadline instead:

- One send runs in the SDK at a time.
- A send past its deadline keeps that slot until it ends, up to 45 s. Other sends answer 7 meanwhile.
- ListOffsets needs no slot, but it waits behind a stuck send in the SDK. Past `REQUEST_TIMEOUT` it answers 7.

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

- One `IggyClient` serves Produce, Metadata and CreateTopics for every Kafka connection, one Iggy
  request at a time. Fetch polls use 4 more, one per read slot. Topic probes use 1 more.
- `IGGY_KAFKA_MAX_CONNECTIONS` does not change that. A Produce client pool is a TODO in
  [docs/SCOPE.md](docs/SCOPE.md).
- Order: set `max.in.flight.requests.per.connection=1`, or `retries=0`. Otherwise a retried batch
  can land after later ones.

Fetches share topic probes: one `get_topic` per topic at a time, for all consumers together, on
the probe client. A caught-up consumer of a busy topic can start one per Fetch. A waiting Fetch
costs about 10 `get_topic` calls per second per topic. `ListOffsets` probes on that client too.

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
- Connection-shaped failures → `NOT_LEADER_OR_FOLLOWER` (6). On a send, `Disconnected`,
  `EmptyResponse`, `TcpError` and `StaleClient` → 7 instead: the write may have landed
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

Fetch folds these into the codes a consumer handles: 7 → 6, 17 → 3, and any other code → -1.

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
