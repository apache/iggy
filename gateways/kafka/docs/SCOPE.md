# Kafka API scope — issue #3421 foundation

This gateway iteration implements **wire validation and stub responses only** (no Iggy backend, no real broker semantics).

## Supported API keys and versions

| API key | Name | Min version | Max version | Behavior |
|---------|------|-------------|-------------|----------|
| 18 | ApiVersions | 0 | 3 | Advertise supported ranges; flexible encoding at v3+ |
| 3 | Metadata | 0 | 9 | Decode request; stub broker `127.0.0.1:9093` |
| 0 | Produce | 3 | 9 | Decode request; stub response |
| 1 | Fetch | 4 | 12 | Decode request; stub response |
| 2 | ListOffsets | 1 | 6 | Decode request; stub response |
| 19 | CreateTopics | 2 | 5 | Decode request; stub response |

## Unsupported API keys

All other API keys receive an error-only response with `UNSUPPORTED_VERSION` (35).

## Out of scope (later issues)

- `IggyBridge` / produce-fetch against Iggy streams
- Consumer group APIs (8–14), SASL (17, 36), transactions
- Accurate metadata topology and record batch semantics
