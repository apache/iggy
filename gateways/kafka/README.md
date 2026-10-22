# Kafka gateway (`iggy-gateway-kafka`)

Foundation layer for [apache/iggy#3421](https://github.com/apache/iggy/issues/3421): a TCP listener on the Kafka wire port that decodes requests and validates scoped API keys and versions. With a bridge, Produce, Fetch, ListOffsets, Metadata, CreateTopics, DeleteTopics, CreatePartitions, DescribeConfigs, AlterConfigs and IncrementalAlterConfigs use Iggy. InitProducerId and consumer group coordination work with or without one.

> **Stub warning:** with `IGGY_KAFKA_BRIDGE_ENABLED=true`, Produce, Fetch, ListOffsets, Metadata,
> CreateTopics, DeleteTopics, CreatePartitions, DescribeConfigs, AlterConfigs and
> IncrementalAlterConfigs use Iggy. With the bridge off (the default), those ten are stubs:
> Produce, Fetch and ListOffsets answer retriable `NOT_LEADER_OR_FOLLOWER` (6), CreateTopics,
> DeleteTopics, CreatePartitions, DescribeConfigs, AlterConfigs and IncrementalAlterConfigs answer
> `NOT_CONTROLLER` (41), and Metadata reports every requested topic unknown. **CreateTopics,
> DeleteTopics, CreatePartitions and both alter APIs run as the bridge's own Iggy user**: with the
> bridge on and `IGGY_KAFKA_SASL_ENABLED` off (the default), any client that can reach this port
> can create topics (up to 1000 partitions each), grow them by up to 1000 partitions per topic per
> request (100 topics per request), delete topics and their data, or shorten a topic's
> `retention.ms` low enough that the cleaner drops sealed segments, deleting data. With
> `IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED=true` and no listener that names principals, any client
> that reaches this port can also create topics just by naming them in a `Metadata` request. When
> the connection has a principal (SASL, or a client certificate mapped to an Iggy user),
> `DeleteTopics`, `CreatePartitions`, `AlterConfigs`, `IncrementalAlterConfigs` and `Metadata`
> auto-creation additionally require it to hold a global `manage_topics` or `manage_streams`;
> without it every resource of a `DeleteTopics`, `CreatePartitions`, `AlterConfigs` or
> `IncrementalAlterConfigs` request, and every missing topic a `Metadata` request would create,
> fails `TOPIC_AUTHORIZATION_FAILED` (29) and nothing is written, created or deleted.
> `CreateTopics` has no such gate yet - SASL on does not protect it. Every client reads and writes
> as the bridge's Iggy user. Iggy's `metadata.partitions_max` defaults to 0 (unlimited), so set it
> before exposing the gateway. See [docs/SCOPE.md](docs/SCOPE.md).
>
> InitProducerId does real work too, with or without the bridge: it allocates a producer id, so a stock idempotent producer starts instead of failing at startup.
>
> Consumer group coordination is not a stub either: `FindCoordinator`, `JoinGroup`, `Heartbeat`,
> `LeaveGroup`, `SyncGroup`, `DescribeGroups` and `ListGroups` are real, with real membership,
> rebalances, graceful leave, session expiry and admin views of that membership
> ([docs/CONSUMER_GROUPS.md](docs/CONSUMER_GROUPS.md)). `kafka-consumer-groups.sh --list`,
> `--describe --members` and `--describe --state` use those views. The default `--describe`
> calls OffsetFetch (API key 9), which requires the bridge and otherwise answers
> `COORDINATOR_LOAD_IN_PROGRESS` (14). With the bridge off, Metadata reports every topic unknown,
> so a consumer joins a group and is assigned 0 partitions.
> With the bridge on, OffsetCommit and OffsetFetch keep each group's offsets in Iggy, so a group
> resumes from its last commit, also after a gateway restart
> ([docs/OFFSET_STORAGE.md](docs/OFFSET_STORAGE.md)). Every Iggy server must run a build with the
> external group consumer kind ([#4392](https://github.com/apache/iggy/pull/4392)) before the first
> commit. In a partly upgraded cluster, one commit can make a partition stop committing. In a
> cluster, a node that has not applied a group's first commit answers "no offset", so a consumer on
> `auto.offset.reset=latest` can skip records until
> [#4409](https://github.com/apache/iggy/issues/4409).

## Run

```bash
cargo run -p iggy-gateway-kafka
```

Default bind: a `PLAINTEXT` listener on `127.0.0.1:9093`. Environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `IGGY_KAFKA_LISTENERS` | unset | Kafka-style listener list, `PROTOCOL://ip:port` entries separated by commas, one per security protocol: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`. An IPv6 address is bracketed: `SSL://[::1]:9094`. Takes precedence over `IGGY_KAFKA_BIND_ADDR`, which the image bakes in and which is ignored with a warning. Refused beside `IGGY_KAFKA_ADVERTISED_HOST`, `IGGY_KAFKA_ADVERTISED_PORT` (advertise through `IGGY_KAFKA_ADVERTISED_LISTENERS` instead) and `IGGY_KAFKA_SASL_ENABLED` (the listener protocols say where SASL runs). See [Listeners and TLS](#listeners-and-tls-4510) |
| `IGGY_KAFKA_ADVERTISED_LISTENERS` | bind address of each listener | `PROTOCOL://host:port` per listener, the address clients are pointed back at. An IPv6 host is bracketed: `SSL://[2001:db8::1]:9094`. Required for any listener bound to `0.0.0.0`/`::` |
| `IGGY_KAFKA_BIND_ADDR` | `127.0.0.1:9093` | Single-listener form: TCP address to listen on |
| `IGGY_KAFKA_ADVERTISED_HOST` | bind IP | Single-listener form: hostname/IP clients use to reach this broker (required when binding to `0.0.0.0`/`::`) |
| `IGGY_KAFKA_ADVERTISED_PORT` | bind port | Single-listener form: port advertised in Metadata responses |
| `IGGY_KAFKA_TLS_CERT_FILE` | unset | PEM certificate chain for every `SSL` and `SASL_SSL` listener, leaf first. Required when one exists, refused when none does |
| `IGGY_KAFKA_TLS_KEY_FILE` | unset | PEM private key for the certificate (PKCS#8, PKCS#1 or SEC1) |
| `IGGY_KAFKA_TLS_CLIENT_AUTH` | `none` | Kafka's `ssl.client.auth`: `none`, `requested` or `required`. Applies to `SSL` listeners, where a verified client certificate names the Iggy user. A `SASL_SSL` listener never asks for a client certificate, Kafka's default there. Refused without an `SSL` listener |
| `IGGY_KAFKA_TLS_CLIENT_CA_FILE` | unset | PEM roots client certificates must chain to. Required with `requested` or `required`, refused with `none` |
| `IGGY_KAFKA_TLS_PRINCIPAL_MAPPING_RULES` | `DEFAULT` | Kafka's `ssl.principal.mapping.rules`, turning a client certificate's subject into an Iggy username. Only with client authentication on an `SSL` listener |
| `IGGY_KAFKA_TLS_RELOAD_INTERVAL_SECS` | `60` | How often the certificate, key and client CA files are re-read; a change goes live for new connections without a restart. `0` never re-reads them |
| `IGGY_KAFKA_MAX_CONNECTIONS` | `1024` | Maximum concurrent connections per listener before new ones are rejected. Each listener has its own allowance, so idle clients on one cannot lock clients out of another. Open connections can therefore reach the number of listeners times this value; size the file-descriptor limit (`ulimit -n`) for that |
| `IGGY_KAFKA_MAX_FRAME_SIZE` | `8388608` | Maximum accepted request frame size in bytes |
| `IGGY_KAFKA_IDLE_TIMEOUT_SECS` | `600` | Seconds a connection may sit idle before the next frame's length prefix arrives |
| `IGGY_KAFKA_READ_TIMEOUT_SECS` | `15` | Seconds allowed to read a frame body once its length prefix arrives |
| `IGGY_KAFKA_WRITE_TIMEOUT_SECS` | `10` | Seconds allowed to write a response frame |
| `IGGY_KAFKA_SHUTDOWN_DRAIN_TIMEOUT_SECS` | `25` | Seconds graceful shutdown waits for in-flight connections before abandoning them. The bridge then signs its Iggy clients out, 5 s at most |
| `IGGY_KAFKA_INSTANCE_ID` | `0` | This gateway's number among the gateways fronting one Iggy cluster. It is the high half of every producer id `InitProducerId` hands out, and Kafka requires those to be cluster-unique, so give every gateway its own value. A single gateway can leave it at `0`. |
| `IGGY_KAFKA_BRIDGE_ENABLED` | `false` | Connect the Iggy bridge at startup. While false every API answers with its stub, and the `IGGY_KAFKA_IGGY_*` variables below are read by nothing. A failed connection is fatal, not a downgrade to stubs. |
| `IGGY_KAFKA_SASL_ENABLED` | `false` | Single-listener form: require SASL/PLAIN before serving any other API (`true` or `false`, nothing else). With `IGGY_KAFKA_LISTENERS`, a `SASL_PLAINTEXT` or `SASL_SSL` listener says the same |
| `IGGY_KAFKA_PRE_AUTH_TIMEOUT_SECS` | `15` | Seconds an unauthenticated connection may sit between frames. Waiting for an authentication slot and the verification itself each get this budget, the verification's starting once it holds a slot. Separate from the 10-minute idle timeout that applies once authenticated |
| `IGGY_KAFKA_MAX_CONCURRENT_AUTHENTICATIONS` | `4` | Credential verifications the gateway runs at once, across all connections. Each costs a password hash on an Iggy shard thread. This bounds the gateway's side only: a check that times out frees its slot while its hash keeps running inside Iggy. Size it below the Iggy node's shard count |
| `IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED` | `false` | Kafka's `auto.create.topics.enable`: a `Metadata` lookup of a missing topic creates it ([Auto topic creation](#auto-topic-creation)). Needs `IGGY_KAFKA_BRIDGE_ENABLED=true`. Off by default, unlike Kafka |
| `IGGY_KAFKA_AUTO_CREATE_TOPICS_PARTITIONS` | `1` | Partitions of an auto-created topic, at most 1000. Only read while auto-creation is on, and refused otherwise |

## Docker image

CI publishes [`apache/iggy-kafka-gateway`](https://hub.docker.com/r/apache/iggy-kafka-gateway)
for `linux/amd64` and `linux/arm64`, using the same publishing rules as the Iggy server and
connectors runtime:

| Tag | Publishing rule |
| --- | --- |
| `edge` | Rolling build refreshed by successful publishing after affected changes merge to `master`. It can lag after a failed publish run. |
| `<version>` | Version from `gateways/kafka/Cargo.toml`. Stable releases are published manually; new prereleases can be published automatically. |
| `latest` | Updated only by a manual stable release. |

CI treats only versions containing `-edge` or `-rc` as prereleases, such as `0.2.0-edge.1`
or `0.2.0-rc.1`.

```bash
docker pull apache/iggy-kafka-gateway:edge
```

With the image's default listener, set `IGGY_KAFKA_ADVERTISED_HOST` to a host reachable by
Kafka clients; see [Run](#run). Produce and Fetch need `IGGY_KAFKA_BRIDGE_ENABLED=true`
and the Iggy connection settings. The [Compose quick start](#quick-start-docker-compose)
sets both the advertised address and the bridge configuration.

## Quick start (Docker Compose)

The fastest way to see a Kafka client talk to Iggy: two containers, no local Rust toolchain to
build them. (Reading the record back below uses `docker compose exec`, not a local build either.)

```text
                    Kafka wire (TCP 9093)              Iggy wire (TCP 8090)
  kcat / librdkafka  ─────────────────────▶  gateway  ─────────────────────▶  iggy-server
  (your Kafka client)                    iggy-gateway-kafka                  (real broker)
```

```bash
cd gateways/kafka
docker compose up --build
```

By default, `iggy-server` pulls the prebuilt `apache/iggy:edge` image,
so only `iggy-gateway-kafka` compiles from source. Its Dockerfile mounts a cargo registry/target
cache, so a first run costs one real compile and every run after reuses it, even across a source
change elsewhere in the workspace. Testing a local `iggy-server` change: comment out `image:` and
uncomment `build:` in `docker-compose.yml` first. That build mounts cargo registry, git, and
target caches, so the first run is still a full release build of several minutes and later runs
reuse those mounts.

To use the prebuilt gateway too, comment out its `build:` block in
[`docker-compose.yml`](docker-compose.yml) and uncomment its `image:` and `pull_policy:` lines.
The [`pull_policy: always`](https://docs.docker.com/reference/compose-file/services/#pull_policy)
setting checks for a newer rolling image each time. From `gateways/kafka`, start both images
without compiling:

```bash
docker compose up --no-build
```

Before testing local gateway changes or running [manual testing](docs/MANUAL_TESTING.md)
Category I, restore the `build:` block and comment out `image:` and `pull_policy:`.

This starts `iggy-server` (the real Iggy broker) and `iggy-gateway-kafka` (bridge enabled,
pointed at that server) on a shared Docker network, with the gateway waiting for the server's
health check before it starts. From another terminal:

```bash
# Broker discovery works with just kcat - it has no create-topic mode of its own, though.
kcat -b 127.0.0.1:9093 -L
```

A full create-topic-then-produce flow needs a client that can send `CreateTopics`, unless
`IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED=true` (see [Auto topic creation](#auto-topic-creation)). With Python's `kafka-python` (illustrative - not exercised by an
automated test, so adjust if the library's API has moved since this was written):

```python
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer

admin = KafkaAdminClient(bootstrap_servers="127.0.0.1:9093")
admin.create_topics([NewTopic(name="orders", num_partitions=1, replication_factor=1)])

producer = KafkaProducer(bootstrap_servers="127.0.0.1:9093")
producer.send("orders", b"hello from kafka-python").get(timeout=5)
```

Read the record back with a real Kafka consumer - Fetch is wired to Iggy with the bridge on (see
the stub warning above), so `kcat` reads it the same way it would against a real broker:

```bash
kcat -b 127.0.0.1:9093 -C -t orders -o beginning -e
```

`-e` exits once `kcat` catches up rather than waiting for more. No consumer group needed for a
one-shot read; see [docs/CONSUMER_GROUPS.md](docs/CONSUMER_GROUPS.md) for group-based consuming.
Polling Iggy directly still works too, if you want to see the record from the other side:

```bash
docker compose exec iggy-server /usr/local/bin/iggy --username iggy \
  --password iggy-gateway-quickstart message poll kafka orders 0 --offset 0 --message-count 1
```

**Ports**: gateway `9093` (Kafka wire, the one a client dials), Iggy `8090` (TCP, `iggy` CLI/SDK),
`3000`/`8080`/`8092` (Iggy HTTP/QUIC/WebSocket, unused by the gateway itself but published for
direct Iggy inspection).

**Config**: the compose file sets fixed dev-only root credentials (`IGGY_ROOT_PASSWORD`) so the
gateway's `IGGY_KAFKA_IGGY_PASSWORD` is known ahead of time - never reuse them outside a local
stack. See `gateways/kafka/docker-compose.yml` and the environment variable tables below for
every other knob.

**Limitations** (see the stub warning above for the full list): single gateway, single Iggy node,
SASL off by default. This is a development quick start, not a production deployment shape.

This quick start's topics use Iggy's default `Durability::Replicated` - a Produce ack is a
quorum commit, not an fsync. A `docker kill` or host power loss can lose acked records despite
the named Docker volume; the volume protects a clean `docker compose down`/`up`, not a crash.
Neither this quick start nor `phase1_e2e_tests.rs` exercises crash durability - the automated
test and the manual read-back above both only prove a produced record is readable from the
still-live process that wrote it. `docs/MANUAL_TESTING.md`'s Category I covers a *restart* (a
graceful `docker compose restart`, which flushes on the way down) as a smoke test for "does the
volume actually persist across a container restart" - it is not a crash test and does not
contradict this limitation.

**Success criteria** (tracked for [#3539](https://github.com/apache/iggy/issues/3539)): a new
contributor should reach a successful produce (and an Iggy-side read-back confirming it landed)
within 15 minutes of `git clone`, and every Phase 1 API key (CreateTopics, Metadata, Produce,
ListOffsets) has an automated test exercising the real compiled binaries end to end -
`tests/phase1_e2e_tests.rs` is that test, spawning real `iggy-server` and `iggy-gateway-kafka`
processes (not `KafkaGateway::run` in-process, as every other suite in this crate does) and
driving CreateTopics → Metadata → Produce → ListOffsets over real TCP, then reading the produced
records back through the Iggy SDK.

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
- [docs/CONFIGS.md](docs/CONFIGS.md) — DescribeConfigs, AlterConfigs and IncrementalAlterConfigs for topic `retention.ms`

### Delivery guarantees

Delivery through this gateway is **at-least-once**, and stays at-least-once across a gateway
restart. Transactions are not supported, and will not be. An idempotent Kafka producer is given
a producer id so that it starts, but its retries are not deduplicated: Produce uses the producer
id only to pick the producer's Iggy session, and ignores the epoch and sequence, so a retry after a
network timeout writes the record twice, and both copies reach the stream with their own offsets.

Iggy deduplicates writes on its own partition plane, and that does not close this gap, because it
guards the hop from the gateway to Iggy rather than the hop from the producer to the gateway.
[docs/IDEMPOTENCE.md](docs/IDEMPOTENCE.md) has the detail and what closing it needs.

Transactions are **not supported**, and will not be. AddPartitionsToTxn (24), AddOffsetsToTxn
(25), EndTxn (26) and TxnOffsetCommit (28) are never advertised, so a conforming client never
sends one; `InitProducerId` with a `transactional_id` answers `UNSUPPORTED_VERSION` (35); and a
Produce request carrying a `transactional_id` gets `UNSUPPORTED_VERSION` (35) on every partition
rather than having its records stored as if they were ordinary ones. None of those closes the
connection. `docs/SCOPE.md`'s Transactions section has the ordering and the reasoning.

## Consumer groups ([#3544](https://github.com/apache/iggy/issues/3544))

A stock consumer with a `group.id` joins, takes the partitions its own assignor hands it, commits,
and resumes from its commit. With the bridge on, commits live in Iggy, so a group also resumes after
a gateway restart. `tests/kafka_client_e2e_tests.rs` checks both with the Java consumer, and a
member that leaves hands its partition to the other at its commit.

Limits:

- One gateway per bootstrap endpoint. Group membership is gateway memory
  ([docs/CONSUMER_GROUPS.md](docs/CONSUMER_GROUPS.md)).
- Classic protocol only. A consumer with `group.protocol=consumer` (KIP-848) fails.
- No DeleteGroups or OffsetDelete, and offsets never expire. An operator deletes a group's offsets
  in Iggy ([docs/OFFSET_STORAGE.md](docs/OFFSET_STORAGE.md#limits)).
- Delivery is at-least-once. A consumer that stops before it commits reads those records again.

## Listeners and TLS ([#4510](https://github.com/apache/iggy/issues/4510))

The gateway binds one listener per Kafka security protocol, named the way Kafka's own `listeners`
and `advertised.listeners` are. A client is pointed back, in Metadata and FindCoordinator, at the
listener it connected through and no other, so a client on the TLS port never learns the plaintext
one.

```bash
IGGY_KAFKA_LISTENERS=PLAINTEXT://127.0.0.1:9093,SSL://0.0.0.0:9094,SASL_SSL://0.0.0.0:9095 \
  IGGY_KAFKA_ADVERTISED_LISTENERS=SSL://kafka.example.com:9094,SASL_SSL://kafka.example.com:9095 \
  IGGY_KAFKA_TLS_CERT_FILE=/etc/iggy/kafka.pem IGGY_KAFKA_TLS_KEY_FILE=/etc/iggy/kafka.key \
  cargo run -p iggy-gateway-kafka
```

Each security protocol can be bound once, since the protocol is also the listener's name.

TLS is rustls. One certificate serves every `SSL` and `SASL_SSL` listener, and the files are
re-read every `IGGY_KAFKA_TLS_RELOAD_INTERVAL_SECS`: a rotated certificate takes effect for new
connections without a restart, a key that does not match leaves the previous material serving and
logs an error. Rotate by writing the new files elsewhere and renaming them into place: a chain
written in place can be read half written, and if what was read still parses and matches the key,
the truncated chain serves until the next interval. There is no self-signed mode; mint a
certificate (`docs/MANUAL_TESTING.md` has an `openssl` recipe) and give the Kafka client the CA.

The certificate's subject alternative names must cover every host in
`IGGY_KAFKA_ADVERTISED_LISTENERS`: a client verifies the name of each broker address Metadata
hands it, so a host missing from the certificate fails the client's next connection, not the
first. Client certificates are checked against the CA only; there is no CRL or OCSP revocation
checking, so revoke access by deleting or deactivating the Iggy user a certificate maps to. That
applies to new connections only: an open connection keeps the principal its handshake resolved,
and the permissions that came with it, until it closes.

```bash
kcat -b kafka.example.com:9094 -X security.protocol=SSL -X ssl.ca.location=ca.pem -L
kcat -b kafka.example.com:9095 -X security.protocol=SASL_SSL -X ssl.ca.location=ca.pem \
     -X sasl.mechanisms=PLAIN -X sasl.username=alice -X sasl.password=s3cret -L
```

### Client certificates (mTLS)

`IGGY_KAFKA_TLS_CLIENT_AUTH=required` (or `requested`) makes every `SSL` listener verify client
certificates against `IGGY_KAFKA_TLS_CLIENT_CA_FILE`, and the certificate names the principal
there. Its subject, rendered the way Java does (`CN=alice,OU=eng,O=Example`), goes through
`IGGY_KAFKA_TLS_PRINCIPAL_MAPPING_RULES`, and the result must be the name of an active Iggy user.
The gateway looks the user up through the bridge's own session, so the bridge is required and its
Iggy user needs `read_users`. A certificate whose subject no rule maps, or that names no such user,
is closed after the handshake, with the subject or name in the gateway's log at `warn`.

A `SASL_SSL` listener never asks for a client certificate, which is Kafka's default for that
protocol: SASL names the principal there.

With `requested`, a client that presents no certificate is Kafka's `ANONYMOUS` principal, which
holds no permissions. Once any listener names principals (a SASL listener, or an `SSL` listener
with client authentication), every connection that proved no identity is `ANONYMOUS`, on a
`PLAINTEXT` listener beside it too. The requests the gateway checks against a principal,
`DeleteTopics`, `AlterConfigs` and the auto-creation a `Metadata` request asks for, need
`manage_topics` and are refused for it; everything else is served as the bridge's user. A gateway whose listeners name nobody serves every request as the
bridge's user.

The rules are Kafka's syntax, tried in order: `RULE:pattern/replacement/[LU]` applies when the
pattern matches the whole subject, `DEFAULT` keeps the subject as is. Iggy accepts any username of
3 to 50 characters, but a raw subject is usually longer than that, and is rarely the name the
operator created, so a deployment that maps identities sets at least one rule. Patterns are a
Java-compatible subset: `\d \s \w` match ASCII only and `.` stops at Java's line terminators, as on
a broker, while `(?i)`, the `u`, `U`, `m`, `x` and `R` flags, `\p{..}`, POSIX classes, `\b`, `\v`,
possessive quantifiers and the `--` and `~~` class operations are refused at startup
([docs/AUTHENTICATION.md](docs/AUTHENTICATION.md) says why). A replacement that
names a group the pattern does not have is refused at startup. That is stricter than Kafka, which
writes such a `$N` literally: `RULE:^CN=(.*)$/$2/` maps to `$2` on a broker. The reverse case,
`/$1\$2/`, makes Kafka throw while the gateway maps `alice` to `alice$2`, which is harmless because
that Iggy user must still exist:

```bash
IGGY_KAFKA_TLS_PRINCIPAL_MAPPING_RULES='RULE:^CN=([^,]+),OU=services.*$/svc-$1/L,RULE:^CN=([^,]+).*$/$1/'
```

Acting as the mapped user on the data plane is
[#4518](https://github.com/apache/iggy/issues/4518); today the identity gates admission and
answers `DescribeAcls`, as a SASL identity does.

## Authentication ([#3549](https://github.com/apache/iggy/issues/3549))

Off by default. With a `SASL_PLAINTEXT` or `SASL_SSL` listener (or `IGGY_KAFKA_SASL_ENABLED=true`
in the single-listener form) the gateway requires SASL/PLAIN before it serves any other API on
that listener, and it verifies the credentials by logging into Iggy with them.

The username and password a Kafka client sends are an **Iggy** username and password. There is no
mapping table and no credential store in the gateway: create an Iggy user for each Kafka principal
and point the client at it. Credentials are verified against `IGGY_KAFKA_IGGY_ADDR`.

```bash
IGGY_KAFKA_SASL_ENABLED=true IGGY_KAFKA_IGGY_ADDR=127.0.0.1:8090 cargo run -p iggy-gateway-kafka
```

Transport security to Iggy is configured separately from the Kafka side, because the two protect
different hops. These variables cover every connection the gateway makes to Iggy, the credential
check's and the bridge's alike, and are refused while neither SASL nor the bridge is on:

| Variable | Default | Description |
| --- | --- | --- |
| `IGGY_KAFKA_IGGY_TLS_ENABLED` | `false` | Encrypt the gateway's connections to Iggy (`true` or `false`, nothing else). Required if the Iggy server only accepts TLS, otherwise the bridge fails to connect and every verification fails as unreachable |
| `IGGY_KAFKA_IGGY_TLS_DOMAIN` | derived from the address | Name checked against the Iggy server certificate |
| `IGGY_KAFKA_IGGY_TLS_CA_FILE` | SDK bundled roots | PEM roots to trust. Note the SDK does not use the system trust store |

Client side, for example with `kcat`:

```bash
kcat -b 127.0.0.1:9093 -X security.protocol=SASL_PLAINTEXT -X sasl.mechanisms=PLAIN \
     -X sasl.username=alice -X sasl.password=s3cret -L
```

Four things to know before switching it on:

- **PLAIN sends the password in the clear** unless the hop is encrypted: a `SASL_SSL` listener on
  the Kafka side, `IGGY_KAFKA_IGGY_TLS_ENABLED=true` on the Iggy side. A `SASL_PLAINTEXT` listener
  is only safe on a trusted network, and the gateway says so at startup. SCRAM cannot be offered at
  all, because Iggy stores one Argon2 hash per user and SCRAM needs PBKDF2-derived keys that cannot
  come from it.
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
streams/topics, provisions them on demand, looks up high watermarks (one or many partitions of a
topic per call) and records by time for `ListOffsets`, probes and polls partitions for Fetch, and
stores and reads group offsets for OffsetCommit and OffsetFetch.
Produce ([#3535](https://github.com/apache/iggy/issues/3535)), Fetch ([#3536](https://github.com/apache/iggy/issues/3536)), ListOffsets
([#3537](https://github.com/apache/iggy/issues/3537)), Metadata ([#3534](https://github.com/apache/iggy/issues/3534)), CreateTopics
([#3538](https://github.com/apache/iggy/issues/3538)), DescribeConfigs, AlterConfigs, IncrementalAlterConfigs,
OffsetCommit and OffsetFetch ([#3542](https://github.com/apache/iggy/issues/3542)) call it.
Topic configuration behavior is in [docs/CONFIGS.md](docs/CONFIGS.md).
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
| Producer id | Picks the producer's Iggy session. See [Concurrency ceiling](#concurrency-ceiling). |
| Producer epoch, sequence | Ignored, so a retry writes twice. |
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
| 7 | Deadline passed: in the send, waiting for decode memory, or behind the producer's own earlier send. Or connection lost mid-send. May be written. | Retries. Can duplicate. |

Partition cap: `max_frame_size` decompressed bytes, `max_frame_size / 64` record slots, 3 headers
per slot. Past it: 10.

Request budget: 8 partition caps, refused partitions included. Past it: 6 for the rest, not
decoded.

Decode memory: 5 partition caps across all requests, counted in bytes. A request holds what its
largest partition may decode: its bytes or 64 per record slot, whichever is more, or a whole cap
when compressed. One holding 64 KiB or more first takes one of 4 large slots, and holds no memory
while it waits for one. So large requests hold 4 caps at most, and leave 1 to the small ones.

A request waits for its producer's turn first, and holds no slot and no memory while it does.
Past its deadline waiting for any of the three: 7. A send that outlives its request keeps its slot
and its memory until it ends, 45 s at most.

[docs/BRIDGE_MAPPING.md](docs/BRIDGE_MAPPING.md) describes what a record becomes once it is stored.

### Fetch ([#3536](https://github.com/apache/iggy/issues/3536))

Per request: one probe per topic, shared with other Fetches, then polls of each partition with
records, in request order. One uncompressed batch per partition.

| Field | Gateway |
| ----- | ------- |
| Partition, offset | As sent. Both systems count from 0. |
| Iggy replica | The one on the node each bridge client is on. Every client signs in at the metadata leader, and a view change or a refused call can move it. So in a cluster, a probe and a poll can read nodes that are not at the same offset. |
| Offset below the oldest kept one | Reads from the oldest kept one. If none is kept, no records until the next write, because a cluster replica that lost records after a crash reads the same. Kafka answers `1` here, so `auto.offset.reset=none` gets no error. |
| Recreated topic | Iggy starts each partition at offset 0 again, and Fetch cannot see it. Seek consumers to 0 after you recreate a topic. Otherwise writes past a consumer's old offset make it skip the records below. |
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
| `IGGY_KAFKA_IGGY_PASSWORD` | none - **required** | Iggy password. No default: `iggy-server` only uses the well-known `iggy`/`iggy` root credentials when started with `--with-default-root-credentials` (dev-only). Otherwise the operator sets the root password, so a hardcoded default here could never be right and would invite running as root unnoticed |
| `IGGY_KAFKA_IGGY_STREAM` | `kafka` | Default Iggy stream for a Kafka topic with no explicit mapping override |
| `IGGY_KAFKA_TOPIC_MAP_PATH` | unset | Path to a topic-mapping TOML file (see below); omit to use only the default rule |
| `IGGY_KAFKA_IGGY_MAX_MESSAGE_SIZE` | `64MiB` | Iggy's `message_bus.max_message_size`. Set both together. Larger partitions answer 10 |
| `IGGY_KAFKA_PRODUCE_SESSIONS` | `256` | Producers that get an Iggy client of their own. The rest share one. `0`: all share one |
| `IGGY_KAFKA_PRODUCE_SESSION_IDLE_SECS` | `600` | An unused Produce client closes after this long |

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

- One request of a producer runs at a time, on that producer's client.
- A send past its deadline keeps the producer's turn until it ends. That producer's requests answer
  7 meanwhile. Other producers send on.
- After 45 s the gateway gives up on the send. It then closes that connection before the turn
  frees, so the send cannot land after a later one. The SDK ends its write within 30 s more,
  unless Iggy stopped reading: the producer then waits until that connection breaks.
- No other API waits behind a stuck send. Each uses another client.

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
default. `AlterConfigs` or `IncrementalAlterConfigs` can set that expiry afterwards through `retention.ms`
([docs/CONFIGS.md](docs/CONFIGS.md)). Until a client does, messages never expire, so a Kafka app
that assumes the 7-day default will accumulate data on this bridge.

They also use Iggy's default **durability**, `Durability::Replicated` - quorum commit without an
additional stable-storage barrier, with the disk write itself threshold-gated (flushed at 1024
messages or 1 MiB of unflushed data, whichever comes first). Kafka's own defaults take the same
posture, so this isn't a wrong choice, but on a single node both can lose an acked write to a power
cut before that threshold is reached - worth knowing rather than discovering later.

The bridge also keeps the Kafka cluster id in Iggy, in topic `cluster_id` of stream
`<default stream>.meta`, created on the first start with durability `persisted`, no expiry and no
size cap. Leave that stream alone: deleting it gives the next start a new id, and a topic mapping
override that targets it is refused at startup. On the start that stores the id, the bridge's Iggy
user needs to create whichever of that stream and topic is missing, then `send_messages` and
`poll_messages` on the stream. Every later start only polls. A user without `manage_streams` can
still run the gateway once an operator creates the stream and the topic (one partition,
durability `persisted`) and grants it `poll_messages` and `send_messages` on the stream: the
bridge creates only what it finds missing. See [docs/SCOPE.md](docs/SCOPE.md#cluster-id).

### Auto topic creation

With `IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED=true`, the gateway behaves like a Kafka broker with
`auto.create.topics.enable=true`. Producers, Connect source tasks, Debezium and dead-letter
topics that rely on creation by first use then work without a `CreateTopics` call.

- A `Metadata` request that names a missing topic and allows auto-creation (always below v4, the
  request's `allow_auto_topic_creation` from v4) starts creating it with
  `IGGY_KAFKA_AUTO_CREATE_TOPICS_PARTITIONS` partitions and answers `UNKNOWN_TOPIC_OR_PARTITION`
  (3), as Kafka does. The client finds the topic on its next refresh.
- A `Metadata` request for all topics never creates anything, and neither does Produce or Fetch.
- An invalid topic name is answered `INVALID_TOPIC_EXCEPTION` (17). Kafka's internal topics,
  `__consumer_offsets`, `__transaction_state` and `__share_group_state`, are answered
  `UNKNOWN_TOPIC_OR_PARTITION` and never created. Any other name starting with `__`, such as a
  Debezium heartbeat topic, is created like any other.
- When the connection has a principal (a SASL login, a client certificate mapped to an Iggy user,
  or `ANONYMOUS` beside either), it needs a global `manage_topics` or `manage_streams`. Only global
  grants count: `manage_topics` granted on one stream does not, even where Iggy would let the
  principal create the topic. A principal without it is answered `TOPIC_AUTHORIZATION_FAILED` (29)
  for each missing topic it names, and nothing is created; topics that exist are answered as usual.
  Kafka clients treat 29 as fatal, so a consumer whose principal may not create topics should set
  `allow.auto.create.topics=false`, which keeps its lookups of a missing topic answered
  `UNKNOWN_TOPIC_OR_PARTITION` (3). A principal whose permissions could not be read is answered 3
  and nothing is created, since 29 is fatal to Java producers, consumers and Streams while Produce
  and Fetch still work for it. An explicit `CreateTopics` is not checked against `manage_topics`
  today, so a principal refused auto-creation can still create the same topic with `CreateTopics`.
- A topic already being created is not started twice. At most 100 creations run at once; a name
  past that is started by a later refresh. A creation Iggy refuses, for example at
  `metadata.partitions_max`, is not retried for 1 second. One Iggy refuses for the bridge user's
  permissions is not retried for a minute: Iggy refuses those only after journaling the create, so
  each attempt costs a replicated metadata write. A name waiting out either still counts toward the
  100. Its first failure is logged at `warn`, and repeats within a minute at `debug`.
- Creations run as the bridge's Iggy user, like `CreateTopics`, on an Iggy client of their own.
  Both create the topic first, and the backing Iggy stream, which the topic mapping names and the
  client never does, only when Iggy reports it missing. The bridge user needs `manage_topics` on
  that stream, or globally, to create a topic in a stream that exists, and the global
  `manage_streams` to create a missing stream. A principal with `manage_topics` but no stream
  rights still gets a new stream when the mapping names one that does not exist yet.
- Like every topic the gateway creates, an auto-created topic has no message expiry (see
  [Provisioning and idempotency](#provisioning-and-idempotency)): its messages are kept until a
  client sets `retention.ms` with `AlterConfigs`.
- Shutdown cancels the creations still running before the bridge signs out, and drops their
  client if any was running, so the sign-out does not wait behind it. An embedder that builds its
  own `GatewayState` calls `shutdown_auto_topic_creation` before closing the bridge.
- `IGGY_KAFKA_AUTO_CREATE_TOPICS_PARTITIONS` applies to auto-creation only. A `CreateTopics` with
  `num_partitions=-1` still creates 1 partition.

As in Kafka, a producer or consumer still running recreates a topic right after `DeleteTopics`
removes it, on its next `Metadata` refresh. A Java consumer with `allow.auto.create.topics=true`
creates whatever topic it subscribes to, including one whose name is a typo.

With auto-creation off, the default, nothing changes: a missing topic is answered
`UNKNOWN_TOPIC_OR_PARTITION` and a client waits until something else creates it.

### Concurrency ceiling

- Each producer sends on an `IggyClient` of its own, one request at a time, so its batches land
  in order. Different producers send at once.
- A producer is its producer id, if this gateway gave that id out since it started, plus its user
  when SASL is on. Every other producer is its Kafka connection: no id, an id from before a
  gateway restart, or an id from another gateway.
- Up to `IGGY_KAFKA_PRODUCE_SESSIONS` (256) of these clients are open. A full pool closes its
  least recently used free client for a new producer, if that client sat unused for 30 s.
  Otherwise the producer shares 1 more client, one request at a time, and moves to its own at a
  later request, once there is room and none of its requests runs.
- `IGGY_KAFKA_MAX_CONNECTIONS` (1024) allows more producers than sessions, so raise the session
  limit to give each its own.
- Each client costs Iggy a connection, a slot of `metadata.clients_table_max` (8192), and a slot
  of `partition.dedup_clients_max` (4096) in each partition it writes.
- A client whose producer is a connection closes, signed out, when that connection ends. Any
  other closes after `IGGY_KAFKA_PRODUCE_SESSION_IDLE_SECS` (600) unused, checked every 30 s.
- 4 clients sign in at once, 80 a second at most. More wait, up to their request's deadline.
- `IGGY_KAFKA_PRODUCE_SESSIONS=0`: all producers share one client, one request at a time.
- Metadata, CreateTopics, DeleteTopics, CreatePartitions, DescribeConfigs, AlterConfigs and
  IncrementalAlterConfigs share 1 more client, one Iggy request at a time. Fetch polls use 4
  more, one per read slot, and a `ListOffsets` lookup by time borrows one of those slots for its
  request. Topic probes use 1
  more. With auto topic creation on, creations use 1 more, opened at the first one.
  Offset calls use 8 more: a commit client and a read client per offset slot.
- CreateTopics, DeleteTopics and CreatePartitions writes also take one gateway-wide lock, so a
  CreatePartitions can read a topic's count and add to it with nothing in between. A
  CreatePartitions whose write outcome is unknown keeps the lock while it re-reads the topic, for
  at most 50 s after its request arrived: the 30 s request ceiling plus 20 s from the write. Admin
  calls behind it wait up to their own deadline, then answer retriable `REQUEST_TIMED_OUT` (7).
  The Java admin client does not retry that code by itself: `kafka-topics.sh` and
  `Admin.createPartitions` report a `TimeoutException`, and the caller must retry.
- OffsetCommit and OffsetFetch cost one Iggy call per partition, spread over the offset slots. In a
  cluster, an offset commit client can also open one connection per node for stores.
- Order: set `max.in.flight.requests.per.connection=1`, or `retries=0`. Otherwise a retried batch
  can land after later ones.
- Order after a 7: the send can still run. A producer with a producer id waits behind it on any
  connection. A producer that is its connection does not once it reconnects, so its retry or its
  next batch can land first. To keep that order use an idempotent producer, the Java default, or
  `IGGY_KAFKA_PRODUCE_SESSIONS=0`.

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
- A create past the node's partition cap (`PartitionsLimitReached`, 2022) → `POLICY_VIOLATION`
  (44). See `metadata.partitions_max` below
- Anything else → `UNKNOWN_SERVER_ERROR` (-1)

Fetch folds these into the codes a consumer handles: 7 → 6, 17 → 3, and any other code → -1.
OffsetCommit and OffsetFetch fold 6, 7 and `RequestTooOld` into `COORDINATOR_LOAD_IN_PROGRESS`
(14), which the Java client retries in both (`to_offset_error_code`). ListOffsets answers
`HistoryUnavailable`, `LifecycleBusy` and `ClientShutdown` with 6 and `RequestTooOld` with 7
instead of -1, because the Java `AdminClient` fails a partition on any non-retriable code
(`to_list_offsets_error_code`). `HistoryUnavailable` refuses a read routed to an older
incarnation of the partition, as after a delete and recreate; the retry probes again and answers 3
if the topic is gone. ListOffsets reads do not get `RequestTooOld` today, so that arm is defensive.

### Server limits the gateway inherits

These are Iggy server limits, not gateway settings. A Kafka client cannot act on any of them, so
an operator has to.

| Limit | Default | Where |
| ------- | --------- | ------- |
| Consumer offset keys per partition, per consumer kind | 4096, ceiling 262144 | `partition.consumer_offsets_max` |
| Partitions of one node, all streams and topics | 0, no cap | `metadata.partitions_max` |
| One user header name, and one header value | 255 bytes | fixed, `user_headers.rs` |
| All user headers of one message | 100 KB | fixed, `MAX_USER_HEADERS_SIZE` |
| Message payload | 64 MB | fixed, `MAX_PAYLOAD_SIZE` |

Only the first two are configurable. A Kafka consumer group commits one offset key per partition
it holds, so `partition.consumer_offsets_max` is what bounds the number of groups that can commit
against one partition. Passing it returns `TooManyConsumerOffsets` (3024), which reaches the
client as `UNKNOWN_SERVER_ERROR` because Kafka has no code for the condition. The gateway logs
the real Iggy error, so the server log is where an operator diagnoses it.

A `CreateTopics` or `CreatePartitions` that would take the node past `metadata.partitions_max`
returns `PartitionsLimitReached` (2022), which reaches the client as `POLICY_VIOLATION` (44). The
cap counts committed partitions only, so creates that run at the same time can go past it. A
`CreatePartitions` on a topic created with a fixed partition count (`PartitionResizeDisabled`,
2023) reaches the client as `POLICY_VIOLATION` too, `validate_only` included. Topics the gateway
creates are resizable.

The other three decide when a Kafka record goes into the envelope instead of being stored
natively. See [docs/OFFSET_STORAGE.md](docs/OFFSET_STORAGE.md) and
[docs/BRIDGE_MAPPING.md](docs/BRIDGE_MAPPING.md).

## Wire fixture tool

See [tools/kafka-tool/README.md](tools/kafka-tool/README.md).
