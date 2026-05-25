---
name: connector-testing
description: Write tests for Apache Iggy connectors - sinks, sources, transforms, SDK, runtime. Covers BDD unit-test naming, the canonical source state round-trip tests, and the real-infra integration test layout under `core/integration/tests/connectors/` with `testcontainers-modules` + `#[iggy_harness]`. Load when writing or reviewing tests in `core/connectors/` or `core/integration/tests/connectors/`.
---

# Testing Apache Iggy Connectors

There are **two distinct test layers** in this codebase. Use both - they catch different bugs.

| Layer | Location | What it covers | When required |
| ------- | ---------- | ---------------- | --------------- |
| **Unit tests** | `#[cfg(test)] mod tests` at EOF of `<plugin>/src/lib.rs` | Config parsing, defaults, fallbacks, payload conversion, query/document building, state ser/de | Every plugin |
| **Integration tests** | `core/integration/tests/connectors/<name>/` (uses real backend via Docker) | End-to-end: runtime + plugin + real Postgres/ES/Iceberg/Mongo/etc. + Iggy server | Every plugin that integrates with an external system |

## Unit tests

### Naming - BDD `given_when_should`

Match the convention used in the codebase (`sources/random_source/src/lib.rs::tests`, `sinks/postgres_sink/src/lib.rs::tests`):

```rust
#[test]
fn given_persisted_state_should_restore_messages_produced() { ... }

#[test]
fn given_no_state_should_start_fresh() { ... }

#[test]
fn given_invalid_duration_should_fall_back_to_default() { ... }

#[test]
fn given_payload_json_should_serialize_correctly() { ... }
```

You may **omit one part** when the test is small (`given_X_should_Y` instead of full `given_X_when_Y_should_Z`), but **stay consistent within a file**. Imperative `test_foo` / `does_bar` names are not the convention here.

### `test_config()` helper

Every plugin's tests start with a small helper returning a tuned-down version of the production config. New tests extend it; don't construct from scratch each time.

```rust
fn test_config() -> RandomSourceConfig {
    RandomSourceConfig {
        interval: Some("100ms".to_string()),
        max_count: Some(100),
        messages_range: Some((5, 10)),
        payload_size: Some(50),
    }
}
```

### Async testing pattern

The trait methods are `async`. For unit tests, build a runtime locally - don't add `#[tokio::test]` in `src/lib.rs` (it pulls test macros into prod build):

```rust
#[test]
fn given_X_should_Y() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        // await calls here
    });
}
```

### Mandatory: four canonical source state tests

Every source must have these. Copy from `sources/random_source/src/lib.rs::tests`:

```rust
#[test]
fn given_persisted_state_should_restore_<field>() {
    let state = State { /* fields */ };
    let serialized = rmp_serde::to_vec(&state).expect("Failed to serialize state");
    let connector_state = ConnectorState(serialized);

    let source = MySource::new(1, test_config(), Some(connector_state));

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let restored = source.state.lock().await;
        assert_eq!(restored.<field>, /* expected */);
    });
}

#[test]
fn given_no_state_should_start_fresh() {
    let source = MySource::new(1, test_config(), None);
    /* assert default state */
}

#[test]
fn given_invalid_state_should_start_fresh() {
    let invalid = ConnectorState(b"not valid msgpack".to_vec());
    let source = MySource::new(1, test_config(), Some(invalid));
    /* assert default state */
}

#[test]
fn state_should_be_serializable_and_deserializable() {
    let original = State { /* fields */ };
    let bytes = rmp_serde::to_vec(&original).unwrap();
    let restored: State = rmp_serde::from_slice(&bytes).unwrap();
    assert_eq!(original.<field>, restored.<field>);
}
```

State is the only thing that survives a plugin restart. Silent corruption here means lost data on the next deploy.

### Sink unit tests

Cover pure per-message logic:

- Config defaults + optional fields + invalid values → fallback.
- Payload conversion for each `Payload` variant the sink claims to handle.
- Header encoding (especially base64 for binary headers).
- Query/document building (e.g., `postgres_sink::build_batch_insert_query`).
- Transient vs permanent error classification (e.g., `postgres_sink::is_transient_error`).

### Transform tests

Four branches must be covered (see `connector-transform`):

1. Happy path: matching input → expected output.
2. Filter path: input that should be dropped returns `Ok(None)`.
3. Pass-through path: non-JSON payload (or unmatched condition) returns `Ok(Some(unchanged))`.
4. Invalid config: `new()` returns `Err(Error::InvalidConfigValue(_))`.

### What NOT to mock

- The underlying client lib (sqlx, reqwest, mongodb). Tests would exercise the mock, not the code.
- The Iggy server.
- The runtime.

If you need a fake backend, use `wiremock` (a real local HTTP server) - see `core/integration/tests/connectors/http/`.

## Integration tests (real infra via Docker)

The canonical pattern lives in `core/integration/tests/connectors/`. Plugins that integrate with external services (Postgres, Elasticsearch, Iceberg, MongoDB, InfluxDB, Quickwit, HTTP/wiremock) have a paired fixture + test file there.

### Layout

```text
core/integration/tests/connectors/
├── mod.rs                        Common helpers (TestMessage, create_test_messages)
├── fixtures/                     One subdir per backend
│   ├── postgres/
│   │   ├── container.rs          PostgresContainer (testcontainers wrapper), env-var consts
│   │   ├── sink.rs               PostgresSinkFixture, PostgresSinkByteaFixture, PostgresSinkJsonFixture
│   │   ├── source.rs             PostgresSourceJsonFixture, PostgresSourceJsonbFixture, ...
│   │   └── mod.rs                re-exports
│   ├── elasticsearch/  iceberg/  mongodb/  influxdb/  quickwit/  http/  delta/
│   └── wiremock.rs               HTTP mocking for http_sink tests
├── postgres/
│   ├── postgres_sink.rs          The actual #[iggy_harness] tests
│   ├── postgres_source.rs
│   ├── restart.rs                Restart + state-survival tests
│   ├── sink.toml                 Runtime config used by sink tests
│   ├── source.toml               Runtime config used by source tests
│   └── mod.rs                    Shared constants (TEST_MESSAGE_COUNT, POLL_ATTEMPTS, ...)
├── delta/ elasticsearch/ iceberg/ mongodb/ influxdb/ random/ quickwit/ http/ stdout/
├── http_config_provider/             HTTP config provider end-to-end
├── runtime/                      Runtime-only tests (error isolation, missing plugin, ...)
└── api/                          HTTP control plane tests
```

### The `#[iggy_harness]` proc macro

Each integration test is annotated with this macro from the `integration` crate. It boots an in-process Iggy server (and optionally the connectors runtime) for the duration of the test, runs the seeds, then injects a `&TestHarness` and your fixture.

```rust
use integration::harness::seeds;
use integration::iggy_harness;

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/sink.toml")),
    seed = seeds::connector_stream
)]
async fn json_messages_sink_stores_as_bytea(
    harness: &TestHarness,
    fixture: PostgresSinkFixture,
) {
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.wait_for_table(&pool, "iggy_messages").await;
    // ... send messages, assert on rows
}
```

Key pieces:

- `server(connectors_runtime(config_path = ...))` boots the connectors runtime against the given TOML.
- `seed = seeds::connector_stream` runs the named seed to create the test stream/topic.
- `fixture: PostgresSinkFixture` - the test harness calls `PostgresSinkFixture::setup()` (which spins up the testcontainer) and injects the result.
- `harness: &TestHarness` (defined in `core/integration/src/harness/orchestrator/harness.rs`) exposes `.root_client().await`, `.connectors_runtime().expect("...").http_url()`, etc. - `connectors_runtime()` returns `Option`, so `.expect()` first.

### Fixture pattern (`TestFixture` trait)

Each fixture implements `integration::harness::TestFixture`. Pattern:

```rust
// core/integration/tests/connectors/fixtures/<backend>/container.rs

use testcontainers_modules::{
    <backend_module>,
    testcontainers::{ContainerAsync, runners::AsyncRunner},
};

pub struct MyContainer {
    container: ContainerAsync<<backend_module>::Image>,
    pub connection_string: String,
}

impl MyContainer {
    pub async fn start() -> Result<Self, TestBinaryError> {
        let container = <backend_module>::Image::default().start().await?;
        let port = container.get_host_port_ipv4(DEFAULT_PORT).await?;
        Ok(Self {
            container,
            connection_string: format!("...://localhost:{port}"),
        })
    }
}
```

The fixture itself wraps the container and implements `TestFixture::setup()` to spin it up and inject env vars that the runtime picks up.

### Env-var injection via `ConfigEnv`

The runtime's config structs derive `ConfigEnv` (`configs_derive::ConfigEnv`, applied in `runtime/src/configs/connectors.rs` to `SinkConfig`, `SourceConfig`, `StreamConsumerConfig`, `StreamProducerConfig`). Fields are addressable by env var with a path-built prefix. Three concrete forms observed in `core/integration/tests/connectors/fixtures/postgres/container.rs`:

```rust
// Plugin-config fields (your sink/source's own config struct):
//   IGGY_CONNECTORS_<TYPE>_<KEY>_PLUGIN_CONFIG_<FIELD>
pub const ENV_SINK_CONNECTION_STRING: &str =
    "IGGY_CONNECTORS_SINK_POSTGRES_PLUGIN_CONFIG_CONNECTION_STRING";

// Indexed nested fields (per-stream entries are a Vec):
//   IGGY_CONNECTORS_<TYPE>_<KEY>_STREAMS_<INDEX>_<FIELD>
pub const ENV_SINK_STREAMS_0_STREAM: &str =
    "IGGY_CONNECTORS_SINK_POSTGRES_STREAMS_0_STREAM";

// Top-level scalar fields on SinkConfig/SourceConfig itself:
//   IGGY_CONNECTORS_<TYPE>_<KEY>_<FIELD>
pub const ENV_SINK_PATH: &str =
    "IGGY_CONNECTORS_SINK_POSTGRES_PATH";
```

`<TYPE>` is `SINK` or `SOURCE`; `<KEY>` is the connector key (TOML key field, uppercased). `PLUGIN_CONFIG` is the literal segment used when reaching into your plugin's nested config. Fields marked `#[config_env(skip)]` (e.g., `transforms`, `plugin_config` as a whole) are NOT env-addressable; primitive leaves marked `#[config_env(leaf)]` are.

The fixture sets these in `setup()` before the runtime reads its config. This is how a dynamic container port reaches a static TOML.

### Test naming for integration tests

Integration tests use a **descriptive declarative** style, not the unit-test `given_should` form. Match the codebase:

```rust
async fn json_messages_sink_stores_as_bytea(...) { ... }
async fn binary_messages_sink_stores_as_bytea(...) { ... }
async fn json_messages_sink_stores_as_jsonb(...) { ... }
async fn delete_after_read_source_removes_rows_after_producing(...) { ... }
async fn processed_column_source_marks_rows_after_producing(...) { ... }
async fn state_persists_across_connector_restart(...) { ... }
async fn restart_sink_connector_continues_processing(...) { ... }
```

Pattern: `<subject>_<action>_<observation>`. Reads like a sentence describing what the test proves.

### Polling vs sleeping

Integration tests poll until an expected condition - never `sleep(big_duration)`. Constants `POLL_ATTEMPTS` and `POLL_INTERVAL_MS` defined per-backend `mod.rs`. Fixture helpers like `PostgresSinkFixture::fetch_rows_as` and `PostgresSinkFixture::wait_for_table` encapsulate the pattern.

### When to add an integration test

- New sink/source plugin → at minimum one happy-path test per supported `Schema` and per supported plugin mode. Concrete coverage in `core/integration/tests/connectors/postgres/`: 3 sink tests (`json_messages_sink_stores_as_bytea`, `binary_messages_sink_stores_as_bytea`, `json_messages_sink_stores_as_jsonb`) backed by 3 sink fixtures, plus 5 source fixtures (`Json`, `Jsonb`, `Bytea`, `Delete`, `Mark`) and a dedicated `restart.rs` for state-survival.
- Bug fix in a plugin → regression test reproducing the bug.
- Behavior that crosses the FFI boundary (state restart, transform chain, schema decode) → integration test, not unit test.

### When NOT to add an integration test

- Pure logic (query builder, payload converter) - that's a unit test.
- Config validation - unit test.
- Code paths already covered by an equivalent test for another backend, when the new backend has identical behavior.

## SDK + runtime tests

- `core/connectors/sdk/tests/` - cross-module SDK concerns. In tree: `protobuf_integration.rs`, `flatbuffer_integration.rs` (round-trip schema decoders/encoders).
- Runtime tests live under `core/integration/tests/connectors/runtime/` and `core/integration/tests/connectors/api/`. They exercise the runtime alone (no plugin-specific backend) - error isolation, missing plugin, invalid config, HTTP API contract.

## CI / verification flow

Before pushing:

```text
cargo fmt --all
cargo sort --no-format --workspace
cargo clippy -p <crate> --all-targets -- -D warnings
cargo test -p <crate>
```

For integration tests, the relevant package is `integration`. There is **no `--test connectors` target** - the `integration` crate has no `[[test]]` entries and `tests/` is structured as a single binary entry through `tests/mod.rs` with `connectors/`, `server/`, `cli/`, etc. as nested modules. Run via name filter:

```text
cargo test -p integration -- connectors::postgres::postgres_sink
cargo test -p integration -- connectors::postgres::                   # all postgres tests
cargo test -p integration -- connectors::                              # all connector tests
```

Integration tests assume Docker is available locally and pull images from a public registry on first run.

## Common pitfalls

1. **Skipping integration tests for a real-infra plugin.** Unit tests don't catch FFI/lifecycle/encoding regressions.
2. **Forgetting the four canonical source state tests.** This is the most common review comment.
3. **Mocking the client library** instead of using `testcontainers` - test exercises the mock.
4. **`#[tokio::test]` in `src/lib.rs`** - pulls tokio test macros into prod build. Use `Runtime::new()` instead. (`#[tokio::test]` is fine in `tests/` directories, which compile separately.)
5. **`unwrap()` in test assertions** - prefer `assert_eq!` / `expect("descriptive")` so failures tell you which step blew up.
6. **Tests depending on system time** - inject the clock or use `tokio::time::pause()`.
7. **Hidden coupling via shared globals** (static `AtomicU64`, env vars) - tests must be independent and parallelizable.
8. **Asserting on `Display` of an error** - use `matches!(err, Error::Variant(_))`. `assert_eq!(err.to_string(), ...)` breaks on message tweaks.
9. **Sleeping a fixed duration** instead of polling - causes flakes when CI is slow.

## Examples worth copying

| Concern | Where |
| --------- | ------- |
| Source state round-trip | `sources/random_source/src/lib.rs::tests` |
| Config + query-building unit tests | `sinks/postgres_sink/src/lib.rs::tests` |
| Real-infra sink integration tests | `core/integration/tests/connectors/postgres/postgres_sink.rs` |
| Real-infra source integration tests | `core/integration/tests/connectors/postgres/postgres_source.rs` |
| Restart + state survival | `core/integration/tests/connectors/postgres/restart.rs` |
| Wiremock-driven HTTP sink test | `core/integration/tests/connectors/http/http_sink.rs` |
| Testcontainer wrapper | `core/integration/tests/connectors/fixtures/postgres/container.rs` |
| Fixture impl `TestFixture` | `core/integration/tests/connectors/fixtures/postgres/sink.rs` |
| Runtime-only error-isolation | `core/integration/tests/connectors/runtime/error_isolation.rs` |
| Transform tests | `sdk/src/transforms/add_fields.rs::tests`, `filter_fields.rs::tests` |
