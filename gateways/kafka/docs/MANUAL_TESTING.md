# Kafka gateway — manual testing procedure

Manual validation for [apache/iggy#3421](https://github.com/apache/iggy/issues/3421) foundation: TCP listener, wire decode, version firewall, stub responses. **No Iggy backend** — success means correct Kafka wire behavior, not message persistence.

See also: [SCOPE.md](SCOPE.md) (supported API keys), [TEST_SUITE.md](TEST_SUITE.md) (automated coverage).

---

## 1. Environment setup

### Requirements

| Tool | Purpose | Install |
|------|---------|---------|
| Rust toolchain | Build gateway + kafka-tool | [rustup.rs](https://rustup.rs) |
| `kafka-message-gen` | Generate/send wire fixtures | `cargo build -p kafka-message-gen` |
| `kcat` (optional) | Real Kafka client smoke test | `brew install kcat` / `apt install kafkacat` |
| `nc` / `netcat` (optional) | Raw byte injection | Usually preinstalled |
| `xxd` or `hexdump` (optional) | Inspect binary responses | Usually preinstalled |

### Build and start gateway

```bash
# From iggy workspace root
cargo build -p iggy_gateway_kafka --bin iggy-kafka-gateway

# Terminal 1 — start listener (default 127.0.0.1:9093)
RUST_LOG=info cargo run -p iggy_gateway_kafka --bin iggy-kafka-gateway
```

Expected log:

```
kafka listener bound on 127.0.0.1:9093
```

### Generate wire fixtures

```bash
# Terminal 2
cargo run -p kafka-message-gen -- generate \
  --output gateways/kafka/tools/kafka-tool/kafka_messages \
  --api-key 0 --api-key 1 --api-key 2 --api-key 3 --api-key 18 --api-key 19
```

---

## 2. Pre-flight automated check

Run before manual testing to catch regressions:

```bash
cargo test -p iggy_gateway_kafka
```

All tests must pass. If `decode_validation_tests` fail, regenerate fixtures (step above).

---

## 3. Manual test cases

### Category A — Smoke tests (must pass before check-in)

| ID | Test | Steps | Expected result | Pass criteria |
|----|------|-------|-----------------|---------------|
| A1 | Gateway starts | Run `iggy-kafka-gateway` | Binds to `:9093`, no panic | Log shows bind address |
| A2 | ApiVersions v1 | `cargo run -p kafka-message-gen -- send --host 127.0.0.1:9093 --api-key 18 --version 1` | Response received | `ec=0`, non-zero byte count |
| A3 | ApiVersions v3 (flexible) | Same with `--version 3` | Response received | `ec=0` |
| A4 | Metadata v0 | `send --api-key 3 --version 0` | Stub broker in response | `ec=0` or topic error 3 (stub) |
| A5 | Produce v3 | `send --api-key 0 --version 3` | Decode + stub ack | `ec=0` |
| A6 | Fetch v4 | `send --api-key 1 --version 4` | Decode + stub response | `ec=0` |
| A7 | ListOffsets v1 | `send --api-key 2 --version 1` | Decode + stub offsets | `ec=0` |
| A8 | CreateTopics v2 | `send --api-key 19 --version 2` | Decode + stub ack | `ec=0` |
| A9 | Verify all scoped keys | `cargo run -p kafka-message-gen -- verify --host 127.0.0.1:9093 --api-key 0 --api-key 1 --api-key 2 --api-key 3 --api-key 18 --api-key 19` | Exit code 0 | No timeouts or I/O errors |

### Category B — Version firewall (boundary validation)

For each API key, test **min−1**, **min**, **max**, **max+1** using `kafka-message-gen send` with `--version N`.

| API key | Name | Min | Max | Test versions |
|---------|------|-----|-----|---------------|
| 18 | ApiVersions | 0 | 3 | −1, 0, 3, 4 |
| 3 | Metadata | 0 | 9 | −1, 0, 9, 10 |
| 0 | Produce | 3 | 9 | 2, 3, 9, 10 |
| 1 | Fetch | 4 | 12 | 3, 4, 12, 13 |
| 2 | ListOffsets | 1 | 6 | 0, 1, 6, 7 |
| 19 | CreateTopics | 2 | 5 | 1, 2, 5, 6 |

| ID | Test | Expected for in-range | Expected for out-of-range |
|----|------|----------------------|---------------------------|
| B1 | ApiVersions negotiation | `error_code=0`; body lists 6 API keys with correct min/max | `error_code=35` (UNSUPPORTED_VERSION) |
| B2 | Metadata out-of-range | N/A | Topic entries show `error_code=35` |
| B3 | Produce/Fetch/ListOffsets/CreateTopics out-of-range | N/A | Version-aware response with `error_code=35` (top-level or per-topic/partition) |
| B4 | ApiVersions lists only scoped keys | Decode response | Contains keys 0,1,2,3,18,19 only — no consumer-group keys |

**Validation tip:** Use `--hex` when generating to inspect request bytes:

```bash
cargo run -p kafka-message-gen -- generate --api-key 18 --version 3 --hex
```

### Category C — Unsupported API keys

| ID | API key | Name | Steps | Expected |
|----|---------|------|-------|----------|
| C1 | 8 | OffsetCommit | `send --api-key 8 --version 2` | `ec=35`, connection stays open |
| C2 | 10 | FindCoordinator | `send --api-key 10` | `ec=35` |
| C3 | 17 | SaslHandshake | `send --api-key 17` | `ec=35` |
| C4 | 20 | DeleteTopics | `send --api-key 20` | `ec=35` |

Follow C1 with A2 on the **same** `nc` session to confirm the connection is not dropped.

### Category D — Flexible vs legacy wire encoding

| ID | API key | Version | Encoding | Validation |
|----|---------|---------|----------|------------|
| D1 | Produce | 8 | Legacy (i32 arrays) | `send` succeeds, `ec=0` |
| D2 | Produce | 9 | Flexible (compact + tagged fields) | `send` succeeds, `ec=0` |
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
|----|------|-------|----------|
| E1 | Broker advertise address | Start gateway on `127.0.0.1:9093`; Metadata v0 | Broker host=`127.0.0.1`, port=`9093` |
| E2 | Wildcard bind + advertised host | `KAFKA_BIND_ADDR=0.0.0.0:19093` + `KAFKA_ADVERTISED_HOST=kafka.internal`, restart | Metadata broker host/port match advertised values |
| E3 | Unknown topic stub | Metadata with topic name `my-topic` | Topic error `3` (UNKNOWN_TOPIC_OR_PARTITION), name `unknown-topic` |
| E4 | Multiple topics | Metadata request listing 3 topics | 3 topic entries, each with error 3 |

### Category F — TCP / connection behavior

| ID | Test | Steps | Expected |
|----|------|-------|----------|
| F1 | Correlation ID echoed | Send ApiVersions with known correlation_id; decode response header | Response correlation_id matches request |
| F2 | Sequential requests | Send ApiVersions then Metadata on same TCP connection | Both get valid responses |
| F3 | Client disconnect | Connect, send partial frame, close | Gateway logs clean disconnect, no panic |
| F4 | Invalid frame length 0 | `printf '\x00\x00\x00\x00' \| nc 127.0.0.1 9093` | Connection closed, gateway continues serving others |
| F5 | Oversized frame | Send 4-byte length > 8 MiB | Connection rejected/closed, no OOM |
| F6 | Graceful shutdown | Ctrl+C on gateway | Log "shutdown requested", in-flight requests drain |

### Category G — Real Kafka client (kcat)

Requires `kcat` installed. Gateway does **not** implement SASL or full broker semantics — expect limited success.

| ID | Test | Command | Expected (foundation) |
|----|------|---------|---------------------|
| G1 | Broker metadata | `kcat -b 127.0.0.1:9093 -L` | ApiVersions + Metadata handshake; broker appears in metadata |
| G2 | Produce (likely fails later) | `echo "hello" \| kcat -b 127.0.0.1:9093 -t test -P` | May fail at coordinator/group stage — document actual error |
| G3 | Consumer (likely fails later) | `kcat -b 127.0.0.1:9093 -t test -C -o beginning` | May fail without consumer groups — document actual error |

Record kcat version and exact error strings in your test log. G1 passing is the minimum bar for client compatibility smoke.

### Category H — Adversarial / negative input

| ID | Test | Steps | Expected |
|----|------|-------|----------|
| H1 | Truncated Produce body | Send valid header + incomplete body | `error_code=42` (INVALID_REQUEST) or connection error; **no panic** |
| H2 | Random bytes | `dd if=/dev/urandom bs=64 count=1 \| nc 127.0.0.1 9093` | Connection closed or protocol error; gateway stays up |
| H3 | Empty body after header | ApiVersions with valid header, empty body | `ec=0` (ApiVersions accepts empty body) |

---

## 4. Validation reference

### Kafka error codes used in #3421

| Code | Name | When returned |
|------|------|---------------|
| 0 | NONE | Successful stub response |
| 42 | INVALID_REQUEST | Produce/Fetch/ListOffsets/CreateTopics decode failure; unsupported request header |
| 3 | UNKNOWN_TOPIC_OR_PARTITION | Metadata stub per-topic error |
| 35 | UNSUPPORTED_VERSION | Out-of-range version or unlisted API key |
| 42 | INVALID_REQUEST | Unsupported request header version |

### Response header rules

| API key | Request flexible? | Response header version |
|---------|--------------------|-------------------------|
| 18 ApiVersions | v3+ | Always v0 (correlation_id only) |
| 3 Metadata | v9+ | v1 (correlation_id + tagged fields) |
| 0 Produce | v9+ | v1 |
| 1 Fetch | v12+ | v1 |
| Others | Per SCOPE.md | See `header.rs` lookup table |

### Frame layout (for manual hex inspection)

```
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

```
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
[ ] cargo test -p iggy_gateway_kafka — ___/103 passed
[ ] cargo clippy -p iggy_gateway_kafka — clean / warnings noted

Notes / failures:
_________________________________
```

---

## 6. Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `Connection refused` on 9093 | Gateway not running | Start `iggy-kafka-gateway` |
| `decode_validation_tests` panic | Missing fixtures | Run `kafka-message-gen generate` |
| `ec=35` for in-range version | Version not in `SUPPORTED_RANGES` | Check `SCOPE.md` and `api.rs` |
| kcat hangs | Timeout waiting for data | Set `-m 1000`; check gateway logs |
| Buffer underflow on Metadata v9+ | Flexible decode mismatch | File issue; check `api.rs` metadata encoder |
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
