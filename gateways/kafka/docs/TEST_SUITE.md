# Kafka gateway — automated regression test suite

Regression tests live under [`tests/`](../tests/). Run from the workspace root:

```bash
cargo test -p iggy-gateway-kafka
```

## Prerequisites

### Wire fixtures (required for `decode_validation_tests` and some handler tests)

```bash
./gateways/kafka/scripts/ci-wire-fixtures.sh generate
```

Fixtures are gitignored under `tools/kafka-tool/kafka_messages/`. CI runs the same script
before `rust-gateway` test jobs and removes the directory afterward. Every fixture-dependent
suite goes through `tests/common/fixtures.rs::load_fixture_body_or_skip`, which skips with a
regeneration hint when a fixture is missing, and panics instead when `KAFKA_FIXTURES_REQUIRED=1`
is set (CI sets this) so a broken generation step can't leave a suite green with zero assertions.

---

## Test files

An exact per-file test count and a full test-name-to-scenario matrix used to live here; both
drifted out of sync with the actual suites more than once as tests were added and consolidated.
Rather than re-derive a snapshot that will drift again, this only lists what each file is for —
`cargo test -p iggy-gateway-kafka -- --list` gives the exact current test names.

| File | Suite focus | Depends on fixtures |
| ------ | ------------- | --------------------- |
| [`codec_tests.rs`](../tests/codec_tests.rs) | Primitive encode/decode round-trips, varint, compact strings, tagged fields | No |
| [`decode_safety_tests.rs`](../tests/decode_safety_tests.rs) | Adversarial wire input — malformed lengths, truncated bodies, oversized declared counts | No |
| [`header_tests.rs`](../tests/header_tests.rs) | Request/response header v1/v2, flexible-version lookup table | No |
| [`api_handler_tests.rs`](../tests/api_handler_tests.rs) | ApiVersions, Metadata stub, unsupported key/version, `handle_request` dispatch | Partial |
| [`response_negative_tests.rs`](../tests/response_negative_tests.rs) | Error-response encoding and validation for each API | No |
| [`golden_wire_fixtures_tests.rs`](../tests/golden_wire_fixtures_tests.rs) | Byte-exact golden responses (ApiVersions v1, Metadata v0) | No |
| [`decode_validation_tests.rs`](../tests/decode_validation_tests.rs) | kafka-tool fixture decode + response structure per version | **Yes** |
| [`version_firewall_tests.rs`](../tests/version_firewall_tests.rs) | Version boundary matrix, unsupported keys, corrupt bodies | Partial |
| [`broker_advertise_tests.rs`](../tests/broker_advertise_tests.rs) | `BrokerAdvertise::from_server_config` parsing | No |
| [`server_integration_tests.rs`](../tests/server_integration_tests.rs) | `read_frame` / `write_frame` unit-level I/O | No |
| [`server_e2e_tests.rs`](../tests/server_e2e_tests.rs) | Full `KafkaServer` TCP round-trips | Partial |
| [`listener_robustness_tests.rs`](../tests/listener_robustness_tests.rs) | TCP listener robustness — framing, pipelining, concurrency, connection limits | No |

`tests/common/` holds shared helpers (`fixtures.rs`, `scope.rs`, `server.rs`, `tcp.rs`, `wire.rs`),
compiled per test binary via `#[path]`, not a test binary itself.

---

## Adding new tests

1. **New API key or version range** — update `SUPPORTED_RANGES` in `api.rs` and `SCOPE.md`.
2. **New decode path** — add a fixture via `kafka-message-gen`, extend `decode_validation_tests.rs`.
3. **New error path** — add to `version_firewall_tests.rs`, `decode_safety_tests.rs`, or
   `response_negative_tests.rs`.
4. **New TCP behavior** — add to `server_e2e_tests.rs` or `listener_robustness_tests.rs` using
   the helpers under `tests/common/`.
