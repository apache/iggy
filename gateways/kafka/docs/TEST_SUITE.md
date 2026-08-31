# Kafka gateway — automated regression test suite

Regression tests live under [`tests/`](../tests/). Run from the workspace root:

```bash
cargo test -p iggy-gateway-kafka
```

## Prerequisites

### Wire fixtures (required for some `api_handler_tests`, `server_e2e_tests`, and `version_firewall_tests` cases)

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

Primitive encode/decode and adversarial wire-input coverage (varint, compact strings, tagged
fields, malformed lengths, oversized declared counts) moved with the codec itself: `kafka_protocol`
covers spec-correct decode/encode, and `src/protocol/bounds_guard.rs` carries its own inline
`#[cfg(test)]` unit tests for the DoS-bound pre-checks it adds on top. Neither has a corresponding
file under `tests/` anymore.

| File | Suite focus | Depends on fixtures |
| ------ | ------------- | --------------------- |
| [`header_tests.rs`](../tests/header_tests.rs) | Request/response header v1/v2 delegation to `kafka_protocol::messages::ApiKey` | No |
| [`api_handler_tests.rs`](../tests/api_handler_tests.rs) | ApiVersions, Metadata stub, unsupported key/version, `handle_request` dispatch | Partial |
| [`response_negative_tests.rs`](../tests/response_negative_tests.rs) | Error-response encoding and validation for each API | No |
| [`golden_wire_fixtures_tests.rs`](../tests/golden_wire_fixtures_tests.rs) | Byte-exact golden responses (ApiVersions v1, Metadata v0) | No |
| [`fixtures_canary_tests.rs`](../tests/fixtures_canary_tests.rs) | Fails loudly if `KAFKA_FIXTURES_REQUIRED=1` and no `.bin` fixtures exist, so a broken generation step can't leave the fixture-backed suites green-but-empty | Canary only |
| [`version_firewall_tests.rs`](../tests/version_firewall_tests.rs) | Version boundary matrix, unsupported keys, corrupt bodies | Partial |
| [`broker_advertise_tests.rs`](../tests/broker_advertise_tests.rs) | `BrokerAdvertise::from_server_config` parsing | No |
| [`server_integration_tests.rs`](../tests/server_integration_tests.rs) | `read_frame` unit-level I/O | No |
| [`server_e2e_tests.rs`](../tests/server_e2e_tests.rs) | Full `KafkaGateway` TCP round-trips | Partial |
| [`listener_robustness_tests.rs`](../tests/listener_robustness_tests.rs) | TCP listener robustness — framing, pipelining, concurrency, connection limits | No |

`tests/common/` holds shared helpers (`codec.rs`, `fixtures.rs`, `scope.rs`, `server.rs`,
`tcp.rs`, `wire.rs`), compiled per test binary via `#[path]`, not a test binary itself. `codec.rs`
is test-only primitive encode/decode scaffolding for hand-building legacy/adversarial wire shapes
`kafka_protocol`'s spec-correct encoder cannot produce - it is not the gateway's production codec.

---

## Adding new tests

1. **New API key or version range** — update `SUPPORTED_RANGES` in `api.rs`, `SCOPE.md`, and add
   a matching `validate_*_shape` guard function in `bounds_guard.rs` (see its module doc for why).
2. **New decode path** — add a fixture via `kafka-message-gen`, extend `api_handler_tests.rs` or
   `version_firewall_tests.rs`.
3. **New error path** — add to `version_firewall_tests.rs` or `response_negative_tests.rs`.
4. **New TCP behavior** — add to `server_e2e_tests.rs` or `listener_robustness_tests.rs` using
   the helpers under `tests/common/`.
