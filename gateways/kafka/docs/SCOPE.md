# Kafka API scope — issue #3421 foundation

This gateway iteration implements **wire validation and stub responses only** (no Iggy backend, no real broker semantics).

Source of truth in code: `SUPPORTED_RANGES` in [`src/protocol/api.rs`](../src/protocol/api.rs).

## Supported API keys and versions

| API key | Name | Min version | Max version | Valid versions | Behavior |
|---------|------|-------------|-------------|----------------|----------|
| 18 | ApiVersions | 0 | 3 | 0, 1, 2, 3 | Advertise supported ranges; flexible encoding at v3+ |
| 3 | Metadata | 0 | 9 | 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 | Decode request; stub broker from `ServerConfig.bind_addr`; flexible encoding at v9+ |
| 0 | Produce | 3 | 9 | 3, 4, 5, 6, 7, 8, 9 | Decode request; stub response |
| 1 | Fetch | 4 | 12 | 4, 5, 6, 7, 8, 9, 10, 11, 12 | Decode request; stub response |
| 2 | ListOffsets | 1 | 6 | 1, 2, 3, 4, 5, 6 | Decode request; stub response |
| 19 | CreateTopics | 2 | 5 | 2, 3, 4, 5 | Decode request; stub response |

A request is accepted when `min_version ≤ api_version ≤ max_version` for that API key. Any other version for a listed key, or any unlisted API key, receives `UNSUPPORTED_VERSION` (35).

## Valid versions reference (by API key)

Use this table when configuring clients or generating wire fixtures with `kafka-message-gen`.

| API key | Name | Valid versions (inclusive range) | Flexible wire encoding from |
|---------|------|----------------------------------|----------------------------|
| 0 | Produce | 3–9 | v9 |
| 1 | Fetch | 4–12 | v12 |
| 2 | ListOffsets | 1–6 | v6 |
| 3 | Metadata | 0–9 | v9 |
| 18 | ApiVersions | 0–3 | v3 |
| 19 | CreateTopics | 2–5 | v5 |

## Unsupported API keys

All API keys not listed above receive an error-only response with `UNSUPPORTED_VERSION` (35). Examples not in this foundation scope:

| API key | Name | Notes |
|---------|------|-------|
| 8 | OffsetCommit | Consumer group — later issue |
| 9 | OffsetFetch | Consumer group — later issue |
| 10 | FindCoordinator | Consumer group — later issue |
| 11–16 | JoinGroup, Heartbeat, LeaveGroup, SyncGroup, DescribeGroups, ListGroups | Consumer group — later issue |
| 17 | SaslHandshake | Auth — later issue |
| 20+ | DeleteTopics, InitProducerId, transactions, ACLs, etc. | Later issues |

## Out of scope (later issues)

- `IggyBridge` / produce-fetch against Iggy streams
- Consumer group APIs (8–14), SASL (17, 36), transactions
- Accurate metadata topology and record batch semantics
