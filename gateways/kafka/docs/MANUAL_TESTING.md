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
# Keys 0/1/2/19 match ci-wire-fixtures.sh: the only keys any test actually loads a .bin
# fixture for. Metadata (3) and ApiVersions (18) requests are built synthetically in-test
# instead, so fixtures for those keys are generated but unused - `generate` still accepts
# them if you want them for manual `send`/`verify` below.
cargo run -p kafka-message-gen -- generate \
  --output gateways/kafka/tools/kafka-tool/kafka_messages \
  --api-key 0 --api-key 1 --api-key 2 --api-key 19
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

### Category A — Smoke tests (must pass before check-in)

The gateway binds `:9093` by default; `kafka-message-gen`'s own default is `:9092` (a real Kafka
broker's usual port). Every command below passes `--host 127.0.0.1:9093` explicitly - omitting it
connection-refuses before any assertion runs.

| ID | Test | Steps | Expected result | Pass criteria |
| ---- | ------ | ------- | ----------------- | --------------- |
| A1 | Gateway starts | Run `iggy-gateway-kafka` | Binds to `:9093`, no panic | Log shows bind address |
| A2 | ApiVersions v1 | `cargo run -p kafka-message-gen -- send --host 127.0.0.1:9093 --api-key 18 --version 1` | Response received | `ec=0`, non-zero byte count |
| A3 | ApiVersions v3 (flexible) | Same with `--version 3` | Response received | `ec=0` |
| A4 | Metadata v0 | `send --host 127.0.0.1:9093 --api-key 3 --version 0` | Stub broker in response | Topic entries show `ec=3` (UNKNOWN_TOPIC_OR_PARTITION, stub) |
| A5 | Produce v3 | `send --host 127.0.0.1:9093 --api-key 0 --version 3` | Decode + stub retriable error | `ec=6` (NOT_LEADER_OR_FOLLOWER) per partition |
| A6 | Fetch v4 | `send --host 127.0.0.1:9093 --api-key 1 --version 4` | Decode + stub response | Top-level `ec=0`; per-partition `ec=6` (NOT_LEADER_OR_FOLLOWER) |
| A7 | ListOffsets v1 | `send --host 127.0.0.1:9093 --api-key 2 --version 1` | Decode + stub offsets | Per-partition `ec=6` (NOT_LEADER_OR_FOLLOWER) - no top-level error field on this response |
| A8 | CreateTopics v2 | `send --host 127.0.0.1:9093 --api-key 19 --version 2` | Decode + stub non-creation ack | `ec=41` (NOT_CONTROLLER) per topic |
| A9 | Verify all scoped keys | `cargo run -p kafka-message-gen -- verify --host 127.0.0.1:9093 --api-key 0 --api-key 1 --api-key 2 --api-key 3 --api-key 18 --api-key 19` | Exit code 0 | No timeouts or I/O errors (`verify` already knows each stub's expected non-zero code - see `is_acceptable_verify_error` in `kafka-tool/src/response.rs`) |

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

| ID | Test | Expected for in-range | Expected for out-of-range |
| ---- | ------ | ---------------------- | --------------------------- |
| B1 | ApiVersions negotiation | `error_code=0`; body lists 6 API keys with correct min/max | KIP-511 exception: still answers, `error_code=35` (UNSUPPORTED_VERSION), v0 response header regardless of the request's own encoding |
| B2 | Metadata out-of-range | N/A | **Connection closes**, no response sent - Metadata has no top-level error field to carry a version-correct error in |
| B3 | Produce/Fetch/ListOffsets/CreateTopics out-of-range | N/A | **Connection closes** for both above-max and below-min - `kafka_protocol`'s schema floor for each of these four messages equals `SUPPORTED_RANGES`' own min, so there is no encodable error response below min either (see `SCOPE.md`'s Governance model) |
| B4 | ApiVersions lists only scoped keys | Decode response | Contains keys 0,1,2,3,18,19 only — no consumer-group keys |

Only ApiVersions (B1) ever returns `error_code=35` on this gateway. Every other API key's
out-of-range case closes the connection - see B2/B3.

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
| C1 | 8 | OffsetCommit | `send --host 127.0.0.1:9093 --api-key 8 --version 2` | Connection closes, no response bytes |
| C2 | 10 | FindCoordinator | `send --host 127.0.0.1:9093 --api-key 10` | Connection closes |
| C3 | 17 | SaslHandshake | `send --host 127.0.0.1:9093 --api-key 17` | Connection closes |
| C4 | 20 | DeleteTopics | `send --host 127.0.0.1:9093 --api-key 20` | Connection closes |

Reconnect and send A2 in a **new** session after each of C1-C4 to confirm the *gateway* (not just
that one connection) is still serving other clients.

### Category D — Flexible vs legacy wire encoding

Run every `send` below with `--host 127.0.0.1:9093`. This category validates that the wire
encoding round-trips at the legacy/flexible boundary - not the stub error code, which is
per-API and constant across both rows (see Category A: `ec=6` for Produce, `ec=41` for
CreateTopics, and so on).

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

### Category G — Real Kafka client (kcat)

Requires `kcat` installed. Gateway does **not** implement SASL or full broker semantics — expect limited success.

| ID | Test | Command | Expected (foundation) |
| ---- | ------ | --------- | --------------------- |
| G1 | Broker metadata | `kcat -b 127.0.0.1:9093 -L` | ApiVersions + Metadata handshake; broker appears in metadata |
| G2 | Produce (likely fails later) | `echo "hello" \| kcat -b 127.0.0.1:9093 -t test -P` | May fail at coordinator/group stage — document actual error |
| G3 | Consumer (likely fails later) | `kcat -b 127.0.0.1:9093 -t test -C -o beginning` | May fail without consumer groups — document actual error |

Record kcat version and exact error strings in your test log. G1 passing is the minimum bar for client compatibility smoke.

### Category H — Adversarial / negative input

| ID | Test | Steps | Expected |
| ---- | ------ | ------- | ---------- |
| H1 | Truncated Produce body | Send valid header + incomplete body | **No response at all** - `kafka_protocol` decodes the whole request in one shot, so a failure anywhere leaves `acks` unknowable; answering risks desyncing an `acks=0` fire-and-forget client's correlation stream, so every Produce decode failure stays silent. Connection stays open (send A2 next to confirm); **no panic** |
| H2 | Random bytes | `dd if=/dev/urandom bs=64 count=1 \| nc 127.0.0.1 9093` | Connection closed or protocol error; gateway stays up |
| H3 | Empty body after header | ApiVersions with valid header, empty body | `ec=0` (ApiVersions accepts empty body) |

---

## 4. Validation reference

### Kafka error codes used in #3421

| Code | Name | When returned |
| ------ | ------ | --------------- |
| 0 | NONE | Fetch top-level error field only (`ec=0` there does not mean per-partition success - see A6) |
| 6 | NOT_LEADER_OR_FOLLOWER | Produce/Fetch/ListOffsets stub, per partition (retriable; payload not persisted) |
| 3 | UNKNOWN_TOPIC_OR_PARTITION | Metadata stub, per topic |
| 35 | UNSUPPORTED_VERSION | **ApiVersions only** (KIP-511 exception). Every other API key's out-of-range version closes the connection instead - see Category B |
| 37 | INVALID_PARTITIONS | CreateTopics: partition count `0` or `< -1` (or any non-positive on v2–v3) |
| 38 | INVALID_REPLICATION_FACTOR | CreateTopics: replication factor `0` or `< -1` (or any non-positive on v2–v3) |
| 41 | NOT_CONTROLLER | CreateTopics stub (topic not created) |
| 42 | INVALID_REQUEST | Fetch/ListOffsets/CreateTopics/ApiVersions decode failure. **Not** Produce - a Produce decode failure always stays silent (`NoResponse`), never `ec=42` - see H1 |

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

[ ] A1–A9  Smoke tests
[ ] B1–B4  Version firewall (all 6 keys × 4 boundary versions)
[ ] C1–C4  Unsupported API keys
[ ] D1–D10 Flexible vs legacy encoding
[ ] E1–E4  Metadata stub semantics
[ ] F1–F6  TCP / connection behavior
[ ] G1–G3  kcat client (record errors for G2/G3)
[ ] H1–H3  Adversarial input

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
- Consumer group join/sync/heartbeat
- SASL authentication
- Accurate partition leadership / ISR
- Transactional produce
- Real offset commit semantics
