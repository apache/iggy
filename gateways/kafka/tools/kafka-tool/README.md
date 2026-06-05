# kafka-message-gen

A Rust CLI tool that generates correct, fully-framed Kafka binary wire protocol messages for **every API key** and **every supported version** — built for testing Kafka-compatible server implementations such as [Apache Iggy](https://iggy.apache.org)'s Kafka compatibility listener.

## What It Does

Each output `.bin` file is a complete, TCP-ready Kafka request:

```
[total_length: i32][api_key: i16][api_version: i16]
[correlation_id: i32][client_id: NULLABLE_STRING]
[tagged_fields: 0x00]  ← only for flexible versions
[payload: bytes]       ← API-specific encoded body
```

The tool covers **65 API keys** and **~280 versioned messages**, sourced directly from the official [Apache Kafka JSON schema files](https://github.com/apache/kafka/tree/trunk/clients/src/main/resources/common/message) (Kafka 4.1.0).

---

## Why This Tool Exists

When implementing a Kafka protocol compatibility layer (e.g. inside Apache Iggy), you need to:

1. Verify your server correctly **parses** every API key at every version
2. Verify your server sends **valid responses** back (correct correlation ID, error codes)
3. Do this without spinning up a full Kafka client or running JVM-based tests

This tool solves all three: generate the binary messages once, then `cat` them directly to your server's port 9092 and inspect the response.

---

## Dependency on `kafka-protocol`

This tool is built on the [`kafka-protocol`](https://crates.io/crates/kafka-protocol) Rust crate, which is itself **code-generated from Kafka's official JSON schema files**. This ensures byte-perfect correctness — the same schemas that generate Kafka's own Java serialization code generate the Rust structs used here.

---

## Installation

```bash
cd gateways/kafka/tools/kafka-tool
cargo build --release
# Binary is at: ./target/release/kafka-message-gen
```

**Requirements:** Rust 1.75+ (MSRV follows `kafka-protocol` crate)

---

## Usage

### List all API keys and version ranges

```bash
cargo run -- list
```

Output:
```
Key    Name                                       MinVer     MaxVer     Count
──────────────────────────────────────────────────────────────────────────────
0      Produce                                    3          13         11
1      Fetch                                      4          18         15
2      ListOffsets                                1          11         11
3      Metadata                                   0          13         14
8      OffsetCommit                               2          10         9
...
──────────────────────────────────────────────────────────────────────────────
Total: 65 API keys  |  ~280 versioned messages
```

---

### Generate all binary messages

```bash
cargo run -- generate --output ./kafka_messages/
```

Creates one `.bin` file per API key × version:

```
kafka_messages/
  000_Produce_v3.bin
  000_Produce_v4.bin
  ...
  000_Produce_v13.bin
  001_Fetch_v4.bin
  ...
  003_Metadata_v0.bin
  003_Metadata_v13.bin
  018_ApiVersions_v0.bin
  018_ApiVersions_v3.bin
  ...
```

#### Options

| Flag | Description | Default |
|------|-------------|---------|
| `--output` | Output directory | `kafka_messages/` |
| `--api-key N` | Generate only for API key N | all |
| `--version N` | Generate only for version N | all |
| `--hex` | Print hex dump to stdout | off |

```bash
# Generate only Metadata messages
cargo run -- generate --api-key 3

# Generate only ApiVersions v3 with hex dump
cargo run -- generate --api-key 18 --version 3 --hex
```

---

### Send messages to a live server

```bash
# Start Iggy with Kafka compat listener on port 9092, then:
cargo run -- send --host 127.0.0.1:9092
```

Output (one line per API key × version):
```
✓ ApiVersions v3 → 32 bytes  ec=0
✓ Metadata v12 → 148 bytes  ec=0
⚠ Produce v9 → 24 bytes  ec=3        ← ec=3 = UnknownTopicOrPartition (expected)
✓ Fetch v12 → 36 bytes  ec=0
...
Result: 243 OK  37 failed
```

#### Options

| Flag | Description | Default |
|------|-------------|---------|
| `--host` | Server address | `127.0.0.1:9092` |
| `--api-key N` | Test only API key N | all |
| `--version N` | Test only version N | all |
| `--timeout-ms N` | Per-request timeout | `5000` |

---

### Verify compatibility (CI-friendly)

```bash
cargo run -- verify --host 127.0.0.1:9092
```

Exits with code **0** if all messages get a response, **1** if any fail (timeout or IO error). Useful in CI pipelines testing a Kafka-compatible server implementation.

---

### Quick raw test with netcat

No Rust needed for a quick smoke test:

```bash
# Generate first
cargo run -- generate

# Send ApiVersions v3 directly via netcat and inspect response
cat kafka_messages/018_ApiVersions_v3.bin | nc 127.0.0.1 9092 | xxd | head

# Send and decode with Wireshark (capture on loopback, filter: kafka)
```

---

## Supported API Keys (Kafka 4.1.0)

| Key | Name | Versions | Phase 1 Priority |
|-----|------|----------|-----------------|
| 0   | Produce | v3–v13 | ✅ Critical |
| 1   | Fetch | v4–v18 | ✅ Critical |
| 2   | ListOffsets | v1–v11 | ✅ Critical |
| 3   | Metadata | v0–v13 | ✅ Critical |
| 8   | OffsetCommit | v2–v10 | ✅ Critical |
| 9   | OffsetFetch | v1–v10 | ✅ Critical |
| 10  | FindCoordinator | v0–v6 | ✅ Critical |
| 11  | JoinGroup | v0–v9 | ✅ Critical |
| 12  | Heartbeat | v0–v4 | ✅ Critical |
| 13  | LeaveGroup | v0–v5 | ✅ Critical |
| 14  | SyncGroup | v0–v5 | ✅ Critical |
| 15  | DescribeGroups | v0–v6 | 🟡 Important |
| 16  | ListGroups | v0–v5 | 🟡 Important |
| 17  | SaslHandshake | v0–v1 | 🟡 Important |
| 18  | ApiVersions | v0–v5 | ✅ Critical |
| 19  | CreateTopics | v2–v7 | ✅ Critical |
| 20  | DeleteTopics | v1–v6 | 🟡 Important |
| 21  | DeleteRecords | v0–v2 | 🔵 Phase 2 |
| 22  | InitProducerId | v0–v6 | 🔵 Phase 2 |
| 24  | AddPartitionsToTxn | v0–v5 | 🔵 Phase 2 |
| 25  | AddOffsetsToTxn | v0–v4 | 🔵 Phase 2 |
| 26  | EndTxn | v0–v5 | 🔵 Phase 2 |
| 28  | TxnOffsetCommit | v0–v5 | 🔵 Phase 2 |
| 29–31 | ACL APIs | v1–v3 | 🔵 Phase 2 |
| 32  | DescribeConfigs | v1–v4 | 🟡 Important |
| 36  | SaslAuthenticate | v0–v2 | 🟡 Important |
| ... | 40+ more | various | 🔵 Phase 3 |

---

## Project Structure

```
tools/kafka-tool/
├── Cargo.toml        ← package manifest and dependencies
├── src/
│   └── main.rs       ← complete CLI implementation
└── README.md         ← this file
```

---

## How It Works

### Protocol Source

All API schemas come from the official Kafka repository:
`apache/kafka/trunk/clients/src/main/resources/common/message/*.json`

The `kafka-protocol` crate processes these JSON files and generates Rust structs with `encode()` and `decode()` methods. This guarantees byte-level compatibility with what official Kafka clients send.

### Flexible vs Legacy Encoding

Kafka introduced "flexible" encoding (compact ULEB128 strings/arrays) starting at different versions per API. The tool automatically detects whether a version uses flexible or legacy encoding and sets the request header format accordingly (header v1 for legacy, header v2 for flexible with tagged fields section).

### API Key Coverage

- **Explicit builders (23 API keys):** Produce, Fetch, ListOffsets, Metadata, OffsetCommit, OffsetFetch, FindCoordinator, JoinGroup, Heartbeat, LeaveGroup, SyncGroup, DescribeGroups, ListGroups, SaslHandshake, ApiVersions, CreateTopics, DeleteTopics, DeleteRecords, InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit, DescribeConfigs, SaslAuthenticate
- **Header-framing test (42 API keys):** All remaining API keys are framed correctly with an empty payload — useful for testing that your server returns a proper error response rather than crashing

---

## Contributing

The tool is intentionally simple. To add an explicit builder for a new API key:

1. Find the JSON schema in `apache/kafka/.../message/YourRequest.json`
2. Add a new match arm in `build_payload()` in `src/main.rs`
3. Use the kafka-protocol crate's generated struct (e.g. `YourRequest::default().with_field(value)`)
4. Open a PR

---

## License

Apache License 2.0 — same as Apache Kafka and Apache Iggy.
