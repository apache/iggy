# Kafka gateway — automated regression test suite

Regression tests live under [`tests/`](../tests/). Run from the workspace root:

```bash
cargo test -p iggy-gateway-kafka
```

**Current count:** 103 tests across 12 suites (as of #3421 foundation).

## Prerequisites

### Wire fixtures (required for `decode_validation_tests` and some handler tests)

```bash
cargo run -p kafka-message-gen -- generate \
  --output gateways/kafka/tools/kafka-tool/kafka_messages \
  --api-key 0 --api-key 1 --api-key 2 --api-key 19
```

Fixtures are gitignored under `tools/kafka-tool/kafka_messages/`. Tests that need them skip gracefully when a fixture file is missing (`handler_regression_tests`) or panic with a clear path (`decode_validation_tests`).

---

## Test file catalog

| File | Suite focus | Test count (approx.) | Depends on fixtures |
| ------ | ------------- | ---------------------- | --------------------- |
| [`codec_tests.rs`](../tests/codec_tests.rs) | Primitive encode/decode round-trips, varint, compact strings, tagged fields | 9 | No |
| [`decode_safety_tests.rs`](../tests/decode_safety_tests.rs) | Adversarial wire input — malformed lengths, truncated bodies | 6 | No |
| [`header_tests.rs`](../tests/header_tests.rs) | Request/response header v1/v2, version lookup table | 10 | No |
| [`api_handler_tests.rs`](../tests/api_handler_tests.rs) | ApiVersions, Metadata stub, unsupported key/version | 7 | No |
| [`golden_wire_fixtures_tests.rs`](../tests/golden_wire_fixtures_tests.rs) | Byte-exact golden responses (ApiVersions v1, Metadata v0) | 2 | No |
| [`decode_validation_tests.rs`](../tests/decode_validation_tests.rs) | kafka-tool fixture decode + response structure per version | 14 | **Yes** |
| [`version_firewall_tests.rs`](../tests/version_firewall_tests.rs) | Version boundary matrix, unsupported keys, corrupt bodies | 17 | Partial |
| [`metadata_regression_tests.rs`](../tests/metadata_regression_tests.rs) | Metadata v0–v9, topic counts, broker advertise | 7 | No |
| [`broker_advertise_tests.rs`](../tests/broker_advertise_tests.rs) | `BrokerAdvertise::from_server_config` parsing | 5 | No |
| [`handler_regression_tests.rs`](../tests/handler_regression_tests.rs) | Every scoped key×version via `handle_request`, stub error codes | 5 | Partial |
| [`server_integration_tests.rs`](../tests/server_integration_tests.rs) | `read_frame` / `write_frame` unit-level I/O | 4 | No |
| [`server_e2e_tests.rs`](../tests/server_e2e_tests.rs) | Full `KafkaServer` TCP round-trips | 8 | Partial |
| [`common/mod.rs`](../tests/common/mod.rs) | Shared helpers (not a test binary) | — | — |

---

## Coverage matrix by API key

### ApiVersions (key 18, v0–v3)

| Scenario | Test file | Test name |
| ---------- | ----------- | ----------- |
| Non-flexible response (v1) | `api_handler_tests` | `api_versions_v1_response_non_flexible_format` |
| Flexible response (v3) | `api_handler_tests` | `api_versions_v3_response_flexible_format` |
| Golden byte fixture (v1) | `golden_wire_fixtures_tests` | `golden_apiversions_v1_response_fixture` |
| Exact advertised ranges (v1, v3) | `version_firewall_tests` | `apiversions_advertises_exact_supported_ranges_*` |
| All versions return `error_code=0` | `version_firewall_tests` | `apiversions_all_versions_return_success` |
| Out-of-range version | `version_firewall_tests` | `apiversions_out_of_range_returns_unsupported_in_body` |
| E2E correlation ID preserved | `server_e2e_tests` | `e2e_apiversions_v1_*`, `e2e_apiversions_v3_*` |

### Metadata (key 3, v0–v9)

| Scenario | Test file | Test name |
| ---------- | ----------- | ----------- |
| Stub broker (default 127.0.0.1:9093) | `api_handler_tests`, `metadata_regression_tests` | `metadata_response_has_broker_*`, `metadata_v0_empty_*` |
| Unsupported version → topic error 35 | `api_handler_tests`, `version_firewall_tests` | `unsupported_version_returns_protocol_error`, `metadata_*_version_returns_topic_error` |
| Golden byte fixture (v0, 1 topic) | `golden_wire_fixtures_tests` | `golden_metadata_v0_single_topic_response_fixture` |
| v1 controller_id, v2 cluster_id | `metadata_regression_tests` | `metadata_v1_*`, `metadata_v2_*` |
| v9 flexible encoding | `metadata_regression_tests` | `metadata_v9_flexible_encoding` |
| Custom broker advertise | `metadata_regression_tests`, `broker_advertise_tests` | `metadata_uses_custom_*`, `metadata_reflects_parsed_*` |
| E2E round-trip | `server_e2e_tests` | `e2e_metadata_v0_returns_stub_broker` |

### Produce (key 0, v3–v9)

| Scenario | Test file | Test name |
| ---------- | ----------- | ----------- |
| Decode all versions (fixture) | `decode_validation_tests` | `produce_all_supported_versions_decode` |
| Response encode all versions | `decode_validation_tests` | `produce_response_encodes_for_all_supported_versions` |
| v3 field layout | `decode_validation_tests` | `produce_response_v3_roundtrip` |
| v8 record_errors array | `decode_validation_tests` | `produce_response_v8_includes_record_errors` |
| Unsupported v2 → error 35 | `version_firewall_tests` | `produce_unsupported_version_returns_error_only` |
| Corrupt body → error 42 | `version_firewall_tests` | `corrupt_produce_body_returns_invalid_request_error` |
| Stub partition error 0 | `handler_regression_tests` | `produce_stub_response_has_zero_error_per_partition` |
| E2E round-trip | `server_e2e_tests` | `e2e_produce_v3_round_trip_with_fixture` |

### Fetch (key 1, v4–v12)

| Scenario | Test file | Test name |
| ---------- | ----------- | ----------- |
| Decode all versions | `decode_validation_tests` | `fetch_all_supported_versions_decode` |
| Response encode all versions | `decode_validation_tests` | `fetch_response_encodes_for_all_supported_versions` |
| v7 session_id / error_code layout | `decode_validation_tests` | `fetch_response_v7_roundtrip` |
| Unsupported v3 | `version_firewall_tests` | `fetch_unsupported_version_returns_error_only` |
| Corrupt body → error 42 | `version_firewall_tests` | `corrupt_fetch_body_returns_invalid_request_error` |
| Stub partition error 0 | `handler_regression_tests` | `fetch_stub_response_has_zero_partition_error` |

### ListOffsets (key 2, v1–v6)

| Scenario | Test file | Test name |
| ---------- | ----------- | ----------- |
| Decode all versions | `decode_validation_tests` | `list_offsets_all_supported_versions_decode` |
| v1 no leader_epoch | `decode_validation_tests` | `list_offsets_response_v1_no_leader_epoch` |
| v4 has leader_epoch | `decode_validation_tests` | `list_offsets_response_v4_has_leader_epoch` |
| Unsupported v0 | `version_firewall_tests` | `list_offsets_unsupported_version_returns_error_only` |
| Stub error 0 | `handler_regression_tests` | `list_offsets_stub_response_has_zero_error` |

### CreateTopics (key 19, v2–v5)

| Scenario | Test file | Test name |
| ---------- | ----------- | ----------- |
| Decode all versions | `decode_validation_tests` | `create_topics_all_supported_versions_decode` |
| v2 roundtrip | `decode_validation_tests` | `create_topics_response_v2_roundtrip` |
| v5 flexible roundtrip | `decode_validation_tests` | `create_topics_response_v5_roundtrip` |
| Unsupported v1 | `version_firewall_tests` | `create_topics_unsupported_version_returns_error_only` |
| Stub error 0 | `handler_regression_tests` | `create_topics_stub_response_has_zero_error` |

---

## Cross-cutting scenarios

| Scenario | Test file | Test name |
| ---------- | ----------- | ----------- |
| Version firewall min/max boundaries | `version_firewall_tests` | `is_supported_version_matches_scope_table` |
| Unknown API keys (8, 9, 10, 17, 20, 999) | `version_firewall_tests`, `api_handler_tests` | `unsupported_api_keys_*`, `unknown_api_key_*` |
| Negative i32 array length | `decode_safety_tests` | `negative_i32_array_length_returns_error_not_panic` |
| Oversized collection count | `decode_safety_tests` | `i32_array_length_above_max_returns_collection_too_large` |
| Compact array varint=0 (null array) | `decode_safety_tests` | `compact_array_varint_zero_decodes_as_empty_without_panic` |
| Malformed varint at shift 63 | `decode_safety_tests` | `varint_terminal_byte_with_extra_bits_at_shift_63_is_rejected` |
| Invalid frame length (0) | `server_integration_tests` | `read_frame_rejects_invalid_lengths` |
| Frame exceeds max_frame_size | `server_integration_tests`, `server_e2e_tests` | `read_frame_rejects_invalid_lengths`, `e2e_oversized_frame_is_rejected` |
| Sequential requests on one TCP connection | `server_e2e_tests` | `e2e_sequential_requests_on_one_connection` |
| Connection survives unsupported API key | `server_e2e_tests` | `e2e_unsupported_api_key_returns_error_without_disconnect` |
| Negative frame length closes connection | `server_e2e_tests` | `e2e_negative_frame_length_closes_connection` |

---

## CI recommendation

```bash
# 1. Generate fixtures
cargo run -p kafka-message-gen -- generate \
  --output gateways/kafka/tools/kafka-tool/kafka_messages \
  --api-key 0 --api-key 1 --api-key 2 --api-key 19

# 2. Run regression suite
cargo test -p iggy-gateway-kafka

# 3. Optional lint gate
cargo clippy -p iggy-gateway-kafka -- -D warnings
```

---

## Adding new tests

1. **New API key or version range** — update `SUPPORTED_RANGES` in `api.rs`, `SCOPE.md`, and add rows to the coverage matrix above.
2. **New decode path** — add fixture via `kafka-message-gen`, extend `decode_validation_tests.rs`.
3. **New error path** — add to `version_firewall_tests.rs` or `decode_safety_tests.rs`.
4. **New TCP behavior** — add to `server_e2e_tests.rs` using helpers in `tests/common/mod.rs`.
