# Kafka gateway — manual testing procedure

Manual validation for [apache/iggy#3421](https://github.com/apache/iggy/issues/3421) foundation: TCP listener, wire decode, version firewall, stub responses. **No Iggy backend** — success means correct Kafka wire behavior, not message persistence.

See also: [SCOPE.md](SCOPE.md) (supported API keys), [TEST_SUITE.md](TEST_SUITE.md) (automated coverage).

---

## 1. Environment setup

### Requirements

| Tool | Purpose | Install |
| ------ | --------- | --------- |
| Rust toolchain | Build gateway + kafka-tool | [rustup.rs](https://rustup.rs) |
| `kafka-message-gen` | Generate/send wire fixtures | `cargo build -p kafka-message-gen` |
| `kcat` (optional) | Real Kafka client smoke test | `brew install kcat` / `apt install kafkacat` |
| `nc` / `netcat` (optional) | Raw byte injection | Usually preinstalled |
| `xxd` or `hexdump` (optional) | Inspect binary responses | Usually preinstalled |

### Build and start gateway

```bash
# From iggy workspace root (or iggy-gateway-kafka subdir)
cargo build -p iggy-gateway-kafka

# Terminal 1 — start listener (default 127.0.0.1:9093)
RUST_LOG=info cargo run -p iggy-gateway-kafka
```

Expected log:

```text
kafka listener bound on 127.0.0.1:9093
```

### Generate wire fixtures

```bash
# Terminal 2
# Keys 0/1/2/19/22 match ci-wire-fixtures.sh's FIXTURE_API_KEYS: the only keys any test
# actually loads a .bin fixture for. Omit key 22 and the six InitProducerId cases skip
# silently, which reads as a pass. Metadata (3) and ApiVersions (18) requests are built
# synthetically in-test instead, so fixtures for those keys are generated but unused -
# `generate` still accepts them if you want them for manual `send`/`verify` below.
cargo run -p kafka-message-gen -- generate \
  --output gateways/kafka/tools/kafka-tool/kafka_messages \
  --api-key 0 --api-key 1 --api-key 2 --api-key 19 --api-key 22
```

---

## 2. Pre-flight automated check

Run before manual testing to catch regressions:

```bash
cargo test -p iggy-gateway-kafka
```

All tests must pass. If the fixture-backed suites (`api_handler_tests`, `server_e2e_tests`,
`version_firewall_tests`) skip instead of running, regenerate fixtures (step above).

---

## 3. Manual test cases

### Category S — SASL/PLAIN

Run against a real Iggy server, because the automated suite drives a stub verifier and cannot
catch a client-compatibility problem. One already slipped through that way: advertising
`SaslHandshake` from v1 rather than v0 passed every automated test and made librdkafka report
"SASL Handshake not supported by broker" before it sent anything.

No host installs needed, both clients run from containers with `--network host`:

```bash
# Terminal 1 - Iggy, with known development credentials
IGGY_PATH=/tmp/iggy-sasl-data ./target/debug/iggy-server --fresh --with-default-root-credentials

# Terminal 2 - gateway with SASL on
IGGY_KAFKA_SASL_ENABLED=true IGGY_KAFKA_IGGY_ADDR=127.0.0.1:8090 \
  IGGY_KAFKA_BIND_ADDR=127.0.0.1:9095 RUST_LOG=debug cargo run -p iggy-gateway-kafka
```

`KC` below is shorthand for:

```bash
docker run --rm --network host edenhill/kcat:1.7.1 -b 127.0.0.1:9095 \
  -X security.protocol=SASL_PLAINTEXT -X sasl.mechanisms=PLAIN
```

| ID | Test | Command | Pass criteria | Last run |
| ---- | ------ | --------- | --------------- | ---------- |
| S1 | Valid credentials | `KC -X sasl.username=iggy -X sasl.password=iggy -L` | Metadata returned, no auth error | Pass |
| S2 | Wrong password | same with `sasl.password=WRONG` | Client reports "Authentication failed", connection drops | Pass |
| S3 | Unknown mechanism | `-X sasl.mechanisms=SCRAM-SHA-256` | Client reports the mechanism is unsupported **and names PLAIN** | Pass |
| S4 | No credentials | `docker run --rm --network host edenhill/kcat:1.7.1 -b 127.0.0.1:9095 -L` | No metadata served; client suggests SASL may be required | Pass |
| S5 | Java client | see below | Authenticates, and lists `SaslHandshake(17): 0 to 1 [usable: 1]` and `SaslAuthenticate(36): 0 to 2 [usable: 2]` | Pass |
| S6 | Feature off | restart without `IGGY_KAFKA_SASL_ENABLED`, rerun S1 | Client reports no broker SASL support; plaintext `-L` still served | Pass |

S5 is the case that matters most, because the Java client negotiates `SaslAuthenticate` **v2**, the
compact framing no other case here exercises, and it sends `ApiVersions` both before and after
authenticating:

```bash
cat > /tmp/client.properties <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="iggy" password="iggy";
EOF
docker run --rm --network host -v /tmp/client.properties:/tmp/client.properties:ro \
  apache/kafka:3.9.0 /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server 127.0.0.1:9095 --command-config /tmp/client.properties
```

### Category T — ACL view

Needs the same running stack as category S, plus two extra Iggy users so the view has something to
distinguish. Create them over the HTTP API, logging in as root first:

```bash
TOKEN=$(curl -s -X POST http://127.0.0.1:3000/users/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"iggy","password":"iggy"}' | jq -r .access_token.token)

curl -s -X POST http://127.0.0.1:3000/users -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"username":"consumer-only","password":"s3cretpass",
  "status":"active","permissions":{"global":{"read_topics":true,"poll_messages":true,
  "manage_servers":false,"read_servers":false,"manage_users":false,"read_users":false,
  "manage_streams":false,"read_streams":false,"manage_topics":false,"send_messages":false},
  "streams":null}}'

curl -s -X POST http://127.0.0.1:3000/users -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username":"no-grants","password":"s3cretpass","status":"active","permissions":null}'
```

Then list each principal's ACLs with the real admin client, swapping the username and password in
`client.properties`:

```bash
docker run --rm --network host -v /tmp/client.properties:/tmp/client.properties:ro \
  apache/kafka:3.9.0 /opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server 127.0.0.1:9095 --command-config /tmp/client.properties --list
```

| ID | Principal | Pass criteria | Last run |
| ---- | ----------- | --------------- | ---------- |
| T1 | `iggy` (root) | `CLUSTER kafka-cluster` with DESCRIBE and no other cluster operation; `TOPIC *` with all six operations; `GROUP *` with READ | Pass |
| T2 | `consumer-only` | `TOPIC *` with DESCRIBE and READ only, `GROUP *` with READ, **no** WRITE and **no** CLUSTER | Pass |
| T3 | `no-grants` | No output at all, and no error | Pass |

T2 is the one that matters. It proves the mapping distinguishes principals rather than echoing a
fixed set, and that the derived group binding appears for a principal Iggy would admit to a group.
T3 proves an empty view is a successful answer rather than a failure, which is a distinction an
admin tool prints very differently.

T1 asserts the absence of `CLUSTER ALTER` rather than its presence. Iggy reads `manage_servers` in
one rule, as an alias for `read_servers`, so rendering an alter grant from it would advertise an
ability with nowhere to be used; [`ACL_MAPPING.md`](ACL_MAPPING.md) carries the argument.

This procedure is now automated, in `gateways/kafka/tests/kafka_client_e2e_tests.rs`, driving the
same `kafka-acls.sh` image against the same stack. The automated version runs two principals this
table does not: one holding only `poll_messages` and one holding only `send_messages`, which are
what separate the group binding's real source from polling. Run the manual procedure when changing
the mapping by hand; otherwise the test is the faster check and the one CI enforces.

#### Login cost

Every authenticated connection costs one Iggy login. Measured against a debug build of both
binaries on one host, so treat these as an upper bound rather than a benchmark:

| Concurrent logins | Median | p95 | Rate |
| ------------------- | -------- | ----- | ------ |
| 1 | 10.6 ms | 12.3 ms | 92/s |
| 8 | 20.6 ms | 29.2 ms | 371/s |
| 32 | 75.2 ms | 107.9 ms | 412/s |

Latency grows with concurrency while throughput flattens, which is what a server-side Argon2 on
shard threads with no blocking pool predicts. No failures at any level. Re-measure before quoting
these anywhere: a release build will be substantially faster.

### Category A — Smoke tests (must pass before check-in)

The gateway binds `:9093` by default; `kafka-message-gen`'s own default is `:9092` (a real Kafka
broker's usual port). Every command below passes `--host 127.0.0.1:9093` explicitly - omitting it
connection-refuses before any assertion runs.

Every case below describes a gateway started **without** a bridge (`IGGY_KAFKA_BRIDGE_ENABLED`
unset). With a bridge, A5 writes one record to `test-topic`: `ec=0` if it exists, `ec=3` if not.
`tests/produce_real_bridge_tests.rs` covers the bridge path.

| ID | Test | Steps | Expected result | Pass criteria |
| ---- | ------ | ------- | ----------------- | --------------- |
| A1 | Gateway starts | Run `iggy-gateway-kafka` | Binds to `:9093`, no panic | Log shows bind address |
| A2 | ApiVersions v1 | `cargo run -p kafka-message-gen -- send --host 127.0.0.1:9093 --api-key 18 --version 1` | Response received | `ec=0`, non-zero byte count |
| A3 | ApiVersions v3 (flexible) | Same with `--version 3` | Response received | `ec=0` |
| A4 | Metadata v0 | `send --host 127.0.0.1:9093 --api-key 3 --version 0` | Stub broker in response | Topic entries show `ec=3` (UNKNOWN_TOPIC_OR_PARTITION, stub) |
| A5 | Produce v3 | `send --host 127.0.0.1:9093 --api-key 0 --version 3` | Decode + stub retriable error, no bridge | `ec=6` (NOT_LEADER_OR_FOLLOWER) per partition |
| A6 | Fetch v4 | `send --host 127.0.0.1:9093 --api-key 1 --version 4` | Decode + stub response | Top-level `ec=0`; per-partition `ec=6` (NOT_LEADER_OR_FOLLOWER) |
| A7 | ListOffsets v1 | `send --host 127.0.0.1:9093 --api-key 2 --version 1` | Decode + stub offsets | Per-partition `ec=6` (NOT_LEADER_OR_FOLLOWER) - no top-level error field on this response |
| A8 | CreateTopics v2 | `send --host 127.0.0.1:9093 --api-key 19 --version 2` | Decode + stub non-creation ack | `ec=41` (NOT_CONTROLLER) per topic |
| A9 | InitProducerId v4 | `send --host 127.0.0.1:9093 --api-key 22 --version 4` | Producer id allocated | `ec=0`, `producer_id >= 0`, `producer_epoch=0`; a second send returns a different `producer_id` |
| A10 | Verify all scoped keys | `cargo run -p kafka-message-gen -- verify --host 127.0.0.1:9093 --api-key 0 --api-key 1 --api-key 2 --api-key 3 --api-key 18 --api-key 19 --api-key 22` | Exit code 0 | No timeouts or I/O errors (`verify` already knows each stub's expected non-zero code - see `is_acceptable_verify_error` in `kafka-tool/src/response.rs`) |

### Category B — Version firewall (boundary validation)

For each API key, test **min−1**, **min**, **max**, **max+1** using `kafka-message-gen send --host 127.0.0.1:9093` with `--version N`.

| API key | Name | Min | Max | Test versions |
| --------- | ------ | ----- | ----- | --------------- |
| 18 | ApiVersions | 0 | 3 | −1, 0, 3, 4 |
| 3 | Metadata | 0 | 9 | −1, 0, 9, 10 |
| 0 | Produce | 3 | 9 | 2, 3, 9, 10 |
| 1 | Fetch | 4 | 12 | 3, 4, 12, 13 |
| 2 | ListOffsets | 1 | 6 | 0, 1, 6, 7 |
| 19 | CreateTopics | 2 | 5 | 1, 2, 5, 6 |
| 8 | OffsetCommit | 2 | 9 | 1, 2, 9, 10 |
| 9 | OffsetFetch | 1 | 9 | 0, 1, 9, 10 |
| 10 | FindCoordinator | 0 | 4 | −1, 0, 4, 5 |
| 11 | JoinGroup | 0 | 9 | −1, 0, 9, 10 |
| 12 | Heartbeat | 0 | 4 | −1, 0, 4, 5 |
| 13 | LeaveGroup | 0 | 5 | −1, 0, 5, 6 |
| 14 | SyncGroup | 0 | 5 | −1, 0, 5, 6 |
| 22 | InitProducerId | 0 | 5 | −1, 0, 5, 6 |
| 32 | DescribeConfigs | 1 | 4 | 0, 1, 4, 5 |
| 33 | AlterConfigs | 0 | 2 | −1, 0, 2, 3 |

| ID | Test | Expected for in-range | Expected for out-of-range |
| ---- | ------ | ---------------------- | --------------------------- |
| B1 | ApiVersions negotiation | `error_code=0`; body lists 14 API keys with correct min/max | KIP-511 exception: still answers, `error_code=35` (UNSUPPORTED_VERSION), v0 response header regardless of the request's own encoding |
| B2 | Metadata out-of-range | N/A | **Connection closes**, no response sent - Metadata has no top-level error field to carry a version-correct error in |
| B3 | Produce/Fetch/ListOffsets/CreateTopics/InitProducerId/OffsetCommit/OffsetFetch  out-of-range | N/A | **Connection closes** for both above-max and below-min - `kafka_protocol`'s schema floor for each of these five messages equals `SUPPORTED_RANGES`' own min, so there is no encodable error response below min either (see `SCOPE.md`'s Governance model) |
| B4 | ApiVersions lists only scoped keys | Decode response | Contains keys 0,1,2,3,8,9,10,11,12,13,14,18,19,22,32,33 only, and no transaction keys (24, 25, 26, 28) |

An out-of-range version only ever produces `error_code=35` on ApiVersions (B1); every other API
key's out-of-range case closes the connection - see B2/B3. InitProducerId and Produce also send
35 in range, for a transactional request - see Category H.

**Validation tip:** Use `--hex` when generating to inspect request bytes:

```bash
cargo run -p kafka-message-gen -- generate --api-key 18 --version 3 --hex
```

### Category C — Unsupported API keys

An unlisted API key closes the connection: no api-specific response schema exists for it, so any
body the gateway could send would be misparsed by the client against the schema it expected.
There is no error-code response to inspect here - the pass criterion is a clean close, not a
decoded `ec`.

| ID | API key | Name | Steps | Expected |
| ---- | --------- | ------ | ------- | ---------- |
| C1 | 15 | DescribeGroups | `send --host 127.0.0.1:9093 --api-key 15` | Connection closes, no response bytes |
| C2 | 10 | FindCoordinator | `send --host 127.0.0.1:9093 --api-key 10` | Connection closes |
| C3 | 17 | SaslHandshake | `send --host 127.0.0.1:9093 --api-key 17` | Connection closes |
| C4 | 20 | DeleteTopics | `send --host 127.0.0.1:9093 --api-key 20` | Connection closes |

Reconnect and send A2 in a **new** session after each of C1-C4 to confirm the *gateway* (not just
that one connection) is still serving other clients.

### Category D — Flexible vs legacy wire encoding

Run every `send` below with `--host 127.0.0.1:9093`. This category validates that the wire
encoding round-trips at the legacy/flexible boundary - not the stub error code, which is
per-API and constant across both rows (see Category A: `ec=6` for Produce with no bridge, `ec=41`
for CreateTopics, and so on).

| ID | API key | Version | Encoding | Validation |
| ---- | --------- | --------- | ---------- | ------------ |
| D1 | Produce | 8 | Legacy (i32 arrays) | `send` succeeds, response decodes, `ec=6` (NOT_LEADER_OR_FOLLOWER, same as A5) |
| D2 | Produce | 9 | Flexible (compact + tagged fields) | `send` succeeds, response decodes, `ec=6` |
| D3 | Fetch | 11 | Legacy | `send` succeeds |
| D4 | Fetch | 12 | Flexible | `send` succeeds |
| D5 | Metadata | 8 | Legacy | `send` succeeds |
| D6 | Metadata | 9 | Flexible | `send` succeeds |
| D7 | ListOffsets | 5 | Legacy | `send` succeeds |
| D8 | ListOffsets | 6 | Flexible | `send` succeeds |
| D9 | CreateTopics | 4 | Legacy | `send` succeeds |
| D10 | CreateTopics | 5 | Flexible | `send` succeeds |

### Category E — Metadata stub semantics

| ID | Test | Steps | Expected |
| ---- | ------ | ------- | ---------- |
| E1 | Broker advertise address | Start gateway on `127.0.0.1:9093`; Metadata v0 | Broker host=`127.0.0.1`, port=`9093` |
| E2 | Wildcard bind + advertised host | `IGGY_KAFKA_BIND_ADDR=0.0.0.0:19093` + `IGGY_KAFKA_ADVERTISED_HOST=kafka.internal`, restart | Metadata broker host/port match advertised values |
| E3 | Unknown topic stub | Metadata with topic name `my-topic` | Topic error `3` (UNKNOWN_TOPIC_OR_PARTITION), name **echoes the requested name** `my-topic` - the response must not substitute a placeholder like `unknown-topic` |
| E4 | Multiple topics | Metadata request listing 3 topics | 3 topic entries, each with error 3 |

### Category F — TCP / connection behavior

| ID | Test | Steps | Expected |
| ---- | ------ | ------- | ---------- |
| F1 | Correlation ID echoed | Send ApiVersions with known correlation_id; decode response header | Response correlation_id matches request |
| F2 | Sequential requests | Send ApiVersions then Metadata on same TCP connection | Both get valid responses |
| F3 | Client disconnect | Connect, send partial frame, close | Gateway logs clean disconnect, no panic |
| F4 | Invalid frame length 0 | `printf '\x00\x00\x00\x00' \| nc 127.0.0.1 9093` | Connection closed, gateway continues serving others |
| F5 | Oversized frame | Send 4-byte length > 8 MiB | Connection rejected/closed, no OOM |
| F6 | Graceful shutdown | Ctrl+C on gateway | Log "shutdown requested", in-flight requests drain |

### Category G — Real Kafka client (kcat, Java console consumer)

Requires `kcat` installed. SASL is off by default (`IGGY_KAFKA_SASL_ENABLED=false` - see Category S
for the SASL-on flow) and the gateway does not implement full broker semantics — expect limited
success. This category predates the bridge landing: G2/G3's "fails at metadata" framing assumes an unbridged gateway, and no longer holds with `IGGY_KAFKA_BRIDGE_ENABLED=true` against a topic that was actually created first. See Category I for the bridged, docker-compose flow.

| ID | Test | Command | Expected (foundation) |
| ---- | ------ | --------- | --------------------- |
| G1 | Broker metadata | `kcat -b 127.0.0.1:9093 -L` | ApiVersions + Metadata handshake; broker appears in metadata |
| G2 | Produce (fails at metadata) | `echo "hello" \| kcat -b 127.0.0.1:9093 -t test -P` | Fails before it sends a Produce request. Metadata reports every topic as unknown, so the client finds no leader. [#3534](https://github.com/apache/iggy/issues/3534) unblocks this, not the Produce handler |
| G3 | Consumer group rebalance | `kcat -b 127.0.0.1:9093 -G g1 test` in two terminals | Both complete JoinGroup and SyncGroup and are assigned 0 partitions, because the Metadata stub reports `test` as unknown and the assignor has nothing to hand out. Record the exact librdkafka log lines |
| G4 | Ungraceful consumer exit | `kill -9` one of G3's kcats | Within `session.timeout.ms` the survivor logs a rebalance, rejoins and is again assigned 0 partitions |
| G5 | Java console consumer | `kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9093 --group g2 --topic test` | Exercises JoinGroup v9, SyncGroup v5, Heartbeat v4, FindCoordinator v4. `group.protocol` defaults to `classic` in 4.x; `--consumer-property group.protocol=consumer` sends ConsumerGroupHeartbeat (68) instead and the connection closes |
| G6 | Graceful kcat exit | G3's two kcats, then Ctrl-C one (kcat closes its consumer, which sends LeaveGroup v0/v1) | The survivor rebalances within one heartbeat interval, not `session.timeout.ms`. Join, leave and sync are real, but the assignment is empty: the Metadata stub reports the topic unknown, so the assignor has no partitions to hand out |
| G7 | Graceful Java exit | G5 in two terminals, then Ctrl-C one | Same as G6 |
| G8 | Static member exit | G7 with `--consumer-property group.instance.id=x` on the one stopped | No LeaveGroup is sent; the survivor waits out the session timeout before rebalancing |
| G9 | Group resumes from its commit | Bridge on and topic `orders` made as in Category I. Run `kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9093 --group g9 --topic orders --from-beginning`, stop it, produce one record, run it again | The second run prints only the new record. `--from-beginning` applies only to a group with no committed offset |
| G10 | Resume after a gateway restart | G9 with a gateway restart before the second run | Same as G9, because the offsets live in Iggy ([#3542](https://github.com/apache/iggy/issues/3542)) |
| G11 | Two members share a topic | Bridge on and a topic with 2 partitions. Run `kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9093 --group g11 --topic <topic> --property print.partition=true --property print.offset=true` in two terminals, produce to both partitions, stop one, produce again | Each prints one partition. After the stop, the other prints the new records of both partitions, and none twice |

Record kcat version and exact error strings in your test log. G1 passing is the minimum bar for client compatibility smoke.

### Category H — Adversarial / negative input

| ID | Test | Steps | Expected |
| ---- | ------ | ------- | ---------- |
| H1 | Truncated Produce body | Send valid header + incomplete body | No response. Connection closed. **No panic** |
| H2 | Random bytes | `dd if=/dev/urandom bs=64 count=1 \| nc 127.0.0.1 9093` | Connection closed or protocol error; gateway stays up |
| H3 | Empty body after header | ApiVersions with valid header, empty body | `ec=0` (ApiVersions accepts empty body) |
| H4 | Transactional InitProducerId | Send key 22 v4 with a non-null `transactional_id` | `ec=35` (UNSUPPORTED_VERSION); connection stays open (send A2 next to confirm) |
| H5 | Transactional Produce | Send key 0 v3 with a non-null `transactional_id` and `acks=1` | `ec=35` per partition, **not** `ec=6`; connection stays open. With `acks=0`: no response, and the gateway closes the connection |
| H6 | Transaction API keys | `send --host 127.0.0.1:9093 --api-key 24` (also 25, 26, 28) | Connection closes, no response bytes - they are never advertised |

### Category I — Docker Compose quick start ([#3539](https://github.com/apache/iggy/issues/3539))

Exercises the real compiled binaries the way an operator actually runs them - not `cargo run`
against a locally-built binary, and not `KafkaGateway::run` in-process the way every automated
suite except `tests/phase1_e2e_tests.rs` does.

| ID | Test | Steps | Expected |
| ---- | ------ | ------- | ---------- |
| I1 | Stack comes up | `cd gateways/kafka && docker compose up --build` | Both containers start; gateway does not exit (it would if the bridge connect failed before the server's health check passed) |
| I2 | Broker discovery | `kcat -b 127.0.0.1:9093 -L` | Broker listed, reachable |
| I3 | Create + produce | Run the `kafka-python` snippet from README.md's Quick start | `create_topics` and `send(...).get()` both return without raising |
| I4 | Read-back via Kafka Fetch | `kcat -b 127.0.0.1:9093 -C -t orders -o beginning -e` | The produced record's payload is printed - validates the actual Kafka Fetch path, not just Iggy's own side |
| I5 | Volume persistence across a restart | `docker compose restart iggy-server`, wait for its health check to go healthy again, re-run I4's poll command | The same record is still readable after the container restarts against the same named volume |
| I6 | Teardown | `docker compose down -v` | Both containers stop; volume removed |

I5 is a graceful restart (`docker compose restart` sends SIGTERM, which flushes on the way down) -
it checks that the named volume actually persists data across a container restart, not that a
produced record survives an unclean crash. It has no automated equivalent yet.
`tests/phase1_e2e_tests.rs` and every step above I5 only prove read-your-own-write on a live
process (see README.md's Limitations, which also covers why this quick start's default topic
durability is not crash-safe). Treat an I5 failure as a real regression, not a flake.

Record wall-clock time from `git clone` to I3 passing - README.md's Quick start states a 15-minute
target for a new contributor ([#3539](https://github.com/apache/iggy/issues/3539) acceptance
criteria).

---

## 4. Validation reference

### Kafka error codes used in #3421

| Code | Name | When returned |
| ------ | ------ | --------------- |
| -1 | UNKNOWN_SERVER_ERROR | Produce with a bridge: Iggy error with no closer code, or bad bridge login |
| 0 | NONE | Fetch top-level error field only (`ec=0` there does not mean per-partition success - see A6) |
| 3 | UNKNOWN_TOPIC_OR_PARTITION | Metadata stub, per topic. Produce with a bridge: missing topic or partition |
| 6 | NOT_LEADER_OR_FOLLOWER | Produce/Fetch/ListOffsets stub (not stored). Produce with a bridge: Iggy unreachable, or the request budget ran out. Fetch with a bridge: Iggy unreachable, too slow, loading the partition, or an offset past the end in its first 30 s on the connection |
| 7 | REQUEST_TIMED_OUT | Produce with a bridge: deadline passed, or connection lost mid-send (may be stored) |
| 10 | MESSAGE_TOO_LARGE | Produce with a bridge: record, send or partition too large, even alone |
| 17 | INVALID_TOPIC_EXCEPTION | Produce with a bridge: bad topic name |
| 21 | INVALID_REQUIRED_ACKS | Produce with a bridge: `acks` is not 0, 1 or -1 |
| 29 | TOPIC_AUTHORIZATION_FAILED | Produce with a bridge: Iggy user lacks permission |
| 35 | UNSUPPORTED_VERSION | Out of range: **ApiVersions only** (KIP-511 exception); every other API key's out-of-range version closes the connection instead - see Category B. In range: InitProducerId with a `transactional_id`; Produce with a `transactional_id`, on every partition with or without a bridge (an `acks=0` one closes instead); and, with a bridge, a transactional or control batch - transactions are not supported, see `SCOPE.md` |
| 37 | INVALID_PARTITIONS | CreateTopics: partition count `0` or `< -1` (or any non-positive on v2–v3) |
| 38 | INVALID_REPLICATION_FACTOR | CreateTopics: replication factor `0` or `< -1` (or any non-positive on v2–v3) |
| 41 | NOT_CONTROLLER | CreateTopics stub (topic not created) |
| 42 | INVALID_REQUEST | Fetch/ListOffsets/CreateTopics/ApiVersions decode failure. Not Produce: it closes the connection (H1) |
| 76 | UNSUPPORTED_COMPRESSION_TYPE | Produce with a bridge: zstd before v7 |
| 87 | INVALID_RECORD | Produce with a bridge: a record batch this gateway cannot map, including a missing, empty or second one, or bytes after it |

A malformed request header (before any API-specific body is even reached) has no parsed header to
build a version-correct response against, so it closes the connection rather than returning any
`ec`.

### Response header rules

Header version selection now delegates entirely to `kafka_protocol::messages::ApiKey` (see
`src/protocol/header.rs`) - there is no lookup table in this codebase to consult.

| API key | Request flexible? | Response header version |
| --------- | -------------------- | ------------------------- |
| 18 ApiVersions | v3+ | Always v0 (correlation_id only) - KIP-511: a client probing an unknown server must be able to parse the discovery response before it knows the server supports flexible encoding |
| 3 Metadata | v9+ | v1 (correlation_id + tagged fields) |
| 0 Produce | v9+ | v1 |
| 1 Fetch | v12+ | v1 |
| 2 ListOffsets | v6+ | v1 |
| 19 CreateTopics | v5+ | v1 |
| 22 InitProducerId | v2+ | v1 |

### Frame layout (for manual hex inspection)

```text
Request frame:
  [length: i32 BE]
  [api_key: i16][api_version: i16][correlation_id: i32]
  [client_id: NULLABLE_STRING or COMPACT_NULLABLE_STRING]
  [tagged_fields: 0x00]          ← flexible requests only
  [request body]

Response frame:
  [length: i32 BE]
  [correlation_id: i32]
  [tagged_fields: 0x00]          ← flexible responses only (not ApiVersions)
  [response body]
```

### Raw netcat smoke test

```bash
# ApiVersions v3 — after generating fixtures
cat gateways/kafka/tools/kafka-tool/kafka_messages/018_ApiVersions_v3.bin \
  | nc -w 2 127.0.0.1 9093 | xxd | head -20
```

First bytes after length prefix should include your correlation_id from the fixture.

---

## 5. Manual test execution checklist

Copy this checklist into your PR or test log:

```text
Date: ___________
Tester: ___________
Gateway commit: ___________
kcat version (if used): ___________

[ ] A1–A10 Smoke tests
[ ] B1–B4  Version firewall (every listed key × 4 boundary versions)
[ ] C1–C4  Unsupported API keys
[ ] D1–D10 Flexible vs legacy encoding
[ ] E1–E4  Metadata stub semantics
[ ] F1–F6  TCP / connection behavior
[ ] G1–G10 kcat / Java client (record errors for G2/G3)
[ ] H1–H6  Adversarial input
[ ] I1–I6  Docker Compose quick start (record wall-clock time to I3; I5 is restart durability)

Automated regression:
[ ] cargo test -p iggy-gateway-kafka — all passed (see `TEST_SUITE.md` for why this checklist
    does not pin a hard count: it has drifted out of sync with the actual suites before)
[ ] cargo clippy -p iggy-gateway-kafka — clean / warnings noted

Notes / failures:
_________________________________
```

---

## 6. Troubleshooting

| Symptom | Likely cause | Fix |
| --------- | -------------- | ----- |
| `Connection refused` on 9093 | Gateway not running, or you connected to the default `9092` instead | Start `iggy-gateway-kafka`; pass `--host 127.0.0.1:9093` to every `kafka-message-gen` command |
| `api_handler_tests`/`server_e2e_tests`/`version_firewall_tests` skip silently | Missing fixtures | Run `kafka-message-gen generate` (set `KAFKA_FIXTURES_REQUIRED=1` to turn skips into failures) |
| Connection closes with no response for an in-range version | Not a bug: only ApiVersions ever returns `ec=35` - every other API key's out-of-range case closes instead (see Category B) | Confirm the version really is in-range: check `SCOPE.md`'s `SUPPORTED_RANGES` table and `api.rs` |
| kcat hangs | Timeout waiting for data | Set `-m 1000`; check gateway logs |
| Buffer underflow on Metadata v9+ | Flexible decode mismatch | File issue; check `api.rs`'s `decode_metadata_topics` / `bounds_guard.rs`'s `validate_metadata_shape` |
| Port already in use | Another process on 9093 | `lsof -i :9093` / change bind port |

---

## 7. What manual testing does NOT cover (deferred)

These are documented as TODO in [SCOPE.md](SCOPE.md) — do not fail #3421 validation for these:

- Message persistence to Iggy
- SASL authentication
- Accurate partition leadership / ISR
- Transactional produce
- Admin group views (join/sync/heartbeat/leave are covered by G3-G8, offset commit and fetch by G9-G10)
