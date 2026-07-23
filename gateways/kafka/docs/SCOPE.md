# Kafka gateway scope — [apache/iggy#3421](https://github.com/apache/iggy/issues/3421)

## Issue #3421 — in scope (this iteration)

Foundation layer only: a TCP listener on the Kafka wire port that decodes requests, validates scoped API keys and versions, validates request wire formats, and returns stub responses. **No Iggy backend integration.**

**Stub semantics (important):** Produce discards the payload and answers with retriable `NOT_LEADER_OR_FOLLOWER` (6). CreateTopics validates the request but answers with `NOT_CONTROLLER` (41) so clients do not believe topics were created. Do not treat `ec=0` stub success as durable storage — that arrives in the Iggy bridge phase.

| Deliverable | Status | Location |
| ------------- | -------- | ---------- |
| TCP listener on `127.0.0.1:9093` (configurable) | Done | `src/server.rs`, `src/main.rs` |
| Length-prefixed frame read/write with `max_frame_size` cap | Done | `src/server.rs` |
| Request header v1/v2 auto-detection | Done | `src/protocol/header.rs` |
| Version negotiation firewall (`SUPPORTED_RANGES`) | Done | `src/protocol/api.rs` |
| Request decode + stub encode for 6 API keys | Done | `src/protocol/requests.rs`, `responses.rs`, `api.rs` |
| Produce hot path: RecordBatch as opaque `Bytes` | Done | `src/protocol/requests.rs` |
| Graceful errors (`UNSUPPORTED_VERSION`, corrupt decode, invalid header) | Done | `src/protocol/api.rs`, `src/server.rs` |
| Adversarial decode safety tests | Done | `tests/decode_safety_tests.rs` |
| Regression test suite (103 tests) | Done | `tests/` — catalog in [`TEST_SUITE.md`](TEST_SUITE.md) |
| Manual testing procedure | Done | [`MANUAL_TESTING.md`](MANUAL_TESTING.md) |
| Wire fixture tool for manual/integration testing | Done | `tools/kafka-tool/` |

Source of truth for supported ranges: `SUPPORTED_RANGES` in [`src/protocol/api.rs`](../src/protocol/api.rs).

### Governance model

Expand `SUPPORTED_RANGES` only after a key/version pair is manually tested. ApiVersions advertises exactly what the firewall allows; out-of-range requests receive `UNSUPPORTED_VERSION` (35) without dropping the connection.

---

## Supported API keys and versions

| API key | Name | Min version | Max version | Valid versions | Behavior |
| --------- | ------ | ------------- | ------------- | ---------------- | ---------- |
| 18 | ApiVersions | 0 | 3 | 0, 1, 2, 3 | Advertise supported ranges; flexible encoding at v3+ |
| 3 | Metadata | 0 | 9 | 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 | Decode topic list count; stub broker from `ServerConfig.bind_addr`; flexible encoding at v9+ |
| 0 | Produce | 3 | 9 | 3, 4, 5, 6, 7, 8, 9 | Decode request; stub returns `NOT_LEADER_OR_FOLLOWER` (6) |
| 1 | Fetch | 4 | 12 | 4, 5, 6, 7, 8, 9, 10, 11, 12 | Decode request; stub response |
| 2 | ListOffsets | 1 | 6 | 1, 2, 3, 4, 5, 6 | Decode request; stub response |
| 19 | CreateTopics | 2 | 5 | 2, 3, 4, 5 | Decode request; stub returns `NOT_CONTROLLER` (41); `-1` partitions/RF = broker default on v4+ |

A request is accepted when `min_version ≤ api_version ≤ max_version` for that API key. Any other version for a listed key, or any unlisted API key, receives `UNSUPPORTED_VERSION` (35).

### Valid versions reference (by API key)

Use this table when configuring clients or generating wire fixtures with `kafka-message-gen`.

| API key | Name | Valid versions (inclusive range) | Flexible wire encoding from |
| --------- | ------ | ---------------------------------- | ---------------------------- |
| 0 | Produce | 3–9 | v9 |
| 1 | Fetch | 4–12 | v12 |
| 2 | ListOffsets | 1–6 | v6 |
| 3 | Metadata | 0–9 | v9 |
| 18 | ApiVersions | 0–3 | v3 |
| 19 | CreateTopics | 2–5 | v5 |

---

## Unsupported API keys (foundation)

All API keys not listed above receive an error-only response with `UNSUPPORTED_VERSION` (35). Examples not in this foundation scope:

| API key | Name | Notes |
| --------- | ------ | ------- |
| 8 | OffsetCommit | Consumer group — later issue |
| 9 | OffsetFetch | Consumer group — later issue |
| 10 | FindCoordinator | Consumer group — later issue |
| 11–16 | JoinGroup, Heartbeat, LeaveGroup, SyncGroup, DescribeGroups, ListGroups | Consumer group — later issue |
| 17 | SaslHandshake | Auth — later issue |
| 20+ | DeleteTopics, InitProducerId, transactions, ACLs, etc. | Later issues |

Full reference for future phases: [`kafka_api_keys_reference.md`](kafka_api_keys_reference.md).

---

## Architecture (three layers)

| Layer | #3421 | Description |
| ------- | ------- | ------------- |
| **1 — Wire framing** | In scope | `server.rs`, `codec.rs`, `header.rs` — keep custom, zero-copy frame I/O |
| **2 — Request/response codecs** | Partial | Custom minimal-parse codecs for 6 hot-path keys; stub responses only |
| **3 — Iggy bridge** | Out of scope | Produce/Fetch → Iggy SDK; deferred to a follow-on issue |

---

## TODO — post-#3421 (architecture review backlog)

Items from the [hybrid architecture review](https://github.com/apache/iggy/discussions/3252) and maintainer feedback. **Not part of #3421.**

### Phase 2 — Iggy bridge (new issue)

- [ ] Add `bridge/` module (`iggy_bridge`): Produce → `send_messages`, Fetch → `poll_messages`
- [ ] Document partition mapping in `docs/BRIDGE_MAPPING.md`:
  - Iggy partitions are **0-based** (same as Kafka) — direct `partition_id` mapping, no offset conversion
  - Iggy **consumer groups exist** — map Kafka group APIs to Iggy consumer group APIs
  - Use `Partitioning::balanced()` only when Kafka sends `partition == -1`; otherwise use request partition ID
- [ ] Idempotent `ensure_stream_and_topic()` (create-if-not-exists)
- [ ] Real Metadata topology (brokers, partitions, leaders) backed by Iggy state

### Phase 2 — Selective `kafka-protocol` crate (feature-gated)

- [ ] Add optional `kafka-protocol-cold` feature to `iggy_gateway_kafka` — **not** wholesale replace of `requests.rs`/`responses.rs`
- [ ] Use crate for: RecordBatch decode (compression, CRC), consumer-group API keys (8–14, 10), complex Metadata/FindCoordinator responses
- [ ] **Keep custom codecs** for Produce/Fetch hot paths (opaque RecordBatch bytes)

### Phase 3 — Consumer groups (~7 API keys)

- [ ] OffsetCommit (8), OffsetFetch (9), FindCoordinator (10)
- [ ] JoinGroup (11), Heartbeat (12), LeaveGroup (13), SyncGroup (14)
- [ ] DescribeGroups (15), ListGroups (16) as needed by target clients

### Phase 3+ — Auth, admin, tuning

- [ ] SASL (17, 36) if required by deployment
- [ ] Tune `max_frame_size` per workload (Kafka defaults: ~1 MiB produce, ~50 MiB fetch; current default 8 MiB)
- [ ] Target **~15–20 API keys** total for a functional bridge — not all 74+ admin keys

### Open questions (ask maintainers before Phase 2)

- [ ] Repo placement: `gateways/kafka/` in [apache/iggy](https://github.com/apache/iggy) vs separate proxy repo (affects workspace deps and CI)
- [ ] Confirm bridge dependency strategy with spetz/hubcio ([Discussion #3081](https://github.com/apache/iggy/discussions/3081), [#3252](https://github.com/apache/iggy/discussions/3252))
