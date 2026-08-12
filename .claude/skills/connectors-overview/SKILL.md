---
name: connectors-overview
description: Entry point and index for the Apache Iggy Connectors subsystem (`core/connectors/`). Load this first when working anywhere under `core/connectors/` - runtime, SDK, sinks, sources, or transforms. Routes to the focused per-area skills.
---

# Apache Iggy Connectors - Overview

Repo-wide rules (Apache headers, fmt/sort/clippy order, idiomatic Rust traits, imports at top, no `cargo install`, LLM-slop tells) live in [AGENTS.md](../../../AGENTS.md). This file owns **connector-specific** rules and routes to the per-area skills.

## Contents

- [STOP and ask the user before](#stop-and-ask-the-user-before)
- [What sinks and sources are](#what-sinks-and-sources-are)
- [Which skill to load](#which-skill-to-load)
- [Stick to conventions](#stick-to-conventions)
- [Connector-wide rules](#connector-wide-rules)
- [Benchmark mode (per-batch timing observability)](#benchmark-mode-per-batch-timing-observability)
- [Logging format](#logging-format)
- [Exemplars](#exemplars)
- [Concrete efficiency patterns](#concrete-efficiency-patterns)
- [Drop accounting](#drop-accounting)
- [Common review smells](#common-review-smells)
- [File map](#file-map)
- [Cite by symbol, not line number](#cite-by-symbol-not-line-number)

## STOP and ask the user before

- Bumping `iggy_connector_sdk` MAJOR version or changing any FFI signature in `sdk/src/{sink,source}.rs` - breaks every pre-built plugin `.so`.
- Changing the runtime's wire conventions (postcard FFI payload structs, default consumer group naming, plugin path resolution).
- Modifying `runtime/src/state.rs` save protocol (atomic rename + fsync ordering) - corruption risk.
- Renaming or repurposing a `Schema` variant - decoders/encoders pinned to wire bytes.
- Promoting a transient `Error` variant to `Permanent*` or vice versa - affects retry behavior across every plugin.

## What sinks and sources are

A **sink** is a plugin that **consumes** messages from Apache Iggy streams and writes them to an external system (Postgres, Elasticsearch, Iceberg, HTTP endpoint, Mongo, object store, stdout, ...). Implements `iggy_connector_sdk::Sink`.

A **source** is a plugin that **produces** messages from an external system into Apache Iggy streams (poll a DB table, scroll an ES index, generate random data, ...). Implements `iggy_connector_sdk::Source`.

Both compile as `cdylib` shared libraries (`.so`/`.dylib`/`.dll`) loaded by the **connectors runtime** at startup via `dlopen`. The runtime drives lifecycle (open/handle/consume/close), bridges Apache Iggy ⇄ plugin via a small FFI, applies optional transforms, and persists source state.

```text
                      ┌─ optional transforms ─┐
External  ──poll──▶  SOURCE  ──FFI──▶  RUNTIME  ──encode──▶  Apache Iggy stream
   system    plugin            ▲                    ▲
                               │                    │
                       state save (msgpack)         │
                                                    │
                       ┌─ optional transforms ─┐    │
Apache Iggy stream  ──decode──▶  RUNTIME  ──FFI──▶  SINK  ──write──▶ External
                                              plugin                   system
```

Headers set on the source side ride through transforms (which may modify, drop, or pass them) and arrive at the sink with `BTreeMap<HeaderKey, HeaderValue>` preserved deterministically.

## Which skill to load

| Task                                                | Skill                 |
| --------------------------------------------------- | --------------------- |
| Write a new sink plugin                             | `connector-sink`      |
| Write a new source plugin                           | `connector-source`    |
| Add schema / decoder / encoder / SDK trait surface  | `connector-sdk`       |
| Change runtime internals (FFI, manager, state, ...) | `connector-runtime`   |
| Add a transform (field-level or format conversion)  | `connector-transform` |
| Write unit / integration tests for any of the above | `connector-testing`   |

## Stick to conventions

The connectors codebase is intentionally repetitive across plugins. Cross-plugin consistency makes review, LLM-assisted contribution, and onboarding tractable.

1. **Find the closest existing plugin by shape** (DB-write -> `postgres_sink`, HTTP -> `http_sink`, polling source -> `postgres_source`).
2. **Copy its file layout, naming, log format, error mapping, test structure** verbatim. Only the backend-specific code (the client call) should differ.
3. **Don't reinvent existing patterns.** The SDK's `retry.rs` covers HTTP retry + circuit breaker. Existing plugins cover batching, idempotency, header encoding, secret handling.
4. **Don't refactor unrelated code in the same PR.** Match conventions. propose convention changes separately.

## Connector-wide rules

### Hot loops

- **Zero clone.** Use `&self`, `&str`, `&[T]`, `&Payload`. For `Payload::Json`, `try_to_bytes(&self)` serializes single-pass without cloning the `OwnedValue` tree.
- **Move-out without cloning** via `std::mem::replace` / `std::mem::take`.
- **`Vec::with_capacity(n)`** when building per-batch buffers. reallocation in hot path is wasted work.

### Async + concurrency

- **`async fn` only.** Never `.block_on()` inside a `Sink`/`Source` impl - the runtime drives the executor.
- **Hold locks briefly.** Pattern: lock, clone/read what's needed, drop guard, then do I/O. Never hold a `Mutex` across an external `.await`.
- **Atomic counters** (`AtomicU64`) preferred over `Mutex<u64>` for hot-path metrics.
- **No `tokio::spawn` from plugin code.** Runtime owns lifecycle. orphans survive `close()`.

### Logging

- `tracing` only. `info!` for lifecycle, `debug!` per-batch detail, `warn!` recoverable, `error!` failures. Include `connector ID: {self.id}` and the connector name on every line.
- **`verbose: bool`** on `SinkConfig`/`SourceConfig` (TOML top-level). Runtime upgrades its own per-batch `debug!` to `info!` when set. Plugins should mirror with a `verbose_logging: Option<bool>` field inside `plugin_config` (convention: `postgres_sink::PostgresSinkConfig::verbose_logging`). Default both to `false`.
- **Never log secrets** (connection strings, API keys, tokens). Redact URLs at the log site. Credential fields must use `SecretString` (see below).

### Secrets

Any credential-bearing field (connection strings, API keys, bearer tokens, AWS keys) must be `SecretString` from the `secrecy` crate, with the workspace serde wrapper applied so `Debug` and serialization both redact. Runtime exposes plugin configs over the `/stats` HTTP surface via serialization - plain `String` leaks the secret to anyone who can hit the endpoint. Plain `String` for a credential is a review-blocker. Pattern (from `sinks/postgres_sink/src/lib.rs::PostgresSinkConfig`):

```rust
use secrecy::{ExposeSecret, SecretString};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MyConfig {
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_secret")]
    pub connection_string: SecretString,
}

// At call site:
let pool = PgPoolOptions::new()
    .connect(self.config.connection_string.expose_secret())
    .await?;
```

In-tree uses: `sinks/{postgres,mongodb,elasticsearch,influxdb,delta}_sink`, `sources/{postgres,elasticsearch,influxdb}_source`.

### Errors

- **Use `Error::PermanentHttpError` / `Error::SchemaMismatch` / `Error::CatalogCommitError` for non-retryable failures.** Returning a transient variant for bad data trips circuit breakers and hammers the backend.
- **No `unwrap()` / `expect()`** on Results from external I/O outside tests.

### Forward-compat config

New TOML fields use `#[serde(default)]` or `Option<T>`. Adding a field must not break existing configs.

## Benchmark mode (per-batch timing observability)

Every sink and source supports an opt-in `benchmark: bool` flag (`SinkConfig`/`SourceConfig`, also env `IGGY_CONNECTORS_<TYPE>_<KEY>_BENCHMARK=true`). Two independent surfaces:

1. **Tracing events** (only when flag is on) - one structured `info!` per batch under target `iggy_connectors::benchmark` with per-stage microsecond timings:
   - Sink: `connector_type, connector_key, stream, topic, partition_id, current_offset, batch_size, processed_count, decode_us, prepare_us, ffi_us, total_us`
   - Source: `connector_type, connector_key, stream, topic, batch_size, sent_count, decode_us, prepare_us, iggy_send_us, state_saved, state_save_us, total_us` (`state_save_us` is 0 when `state_saved` is false - emitted as flat number + bool, not an Option, so the JSON layer keeps it numeric)
   - Filter live: `RUST_LOG=iggy_connectors::benchmark=info`
   - Also emitted on startup: a one-shot `"Benchmark mode enabled for ..."` info log per connector.

2. **Prometheus histograms** (always on, regardless of flag):
   - Metric: `iggy_connector_stage_duration_seconds{connector_key, connector_type, stage}` (both labels snake_case)
   - `stage` (snake_case): `decode`, `prepare`, `total` on both sides; `ffi` (sink only); `iggy_send`, `state_save` (source only). `decode` and `prepare` mean the same thing on both sides - decode is schema decoding, prepare is transform + encode/serialize.
   - Buckets: see `runtime/src/metrics.rs::STAGE_BUCKETS_SECONDS`
   - Example: `histogram_quantile(0.95, sum(rate(iggy_connector_stage_duration_seconds_bucket{stage="ffi"}[5m])) by (le, connector_key))`

The flag only gates the verbose text event. the histogram observation is always on. Per-stage call path: `Instant::now()` + SipHash over `StageLabels` + parking_lot `RwLock::read` + `HashMap` probe + linear scan inside `Histogram::observe`. Benchmark on target hardware if you suspect the overhead matters at your batch rate.

`iggy_connector_messages_filtered_total{connector_key, connector_type}` counts intentional drops by transforms returning `Ok(None)`, distinct from `errors_total` for unexpected drops. Both also exposed in `/stats` JSON (`ConnectorStats.messages_filtered`, `.errors`).

Implementation: `runtime/src/benchmark.rs` (text-event emitters +
`as_micros`), `runtime/src/metrics.rs::{Stage, SinkLabels, SourceLabels,
observe_stage_with_labels}` (pre-built label cache avoids per-batch
`String` clones. `&str`-based wrappers are `#[cfg(test)]` only). Tests:
`runtime/src/metrics.rs::tests::given_stage_histogram_*` (unit),
`core/integration/tests/connectors/runtime/benchmark.rs` (integration:
text emission, env override, disabled-flag, histograms via `/metrics`,
JSON log format, parser unit tests).

## Logging format

`runtime/src/configs/runtime.rs::LoggingConfig` exposes `format = "text" | "json"` (text default), env `IGGY_CONNECTORS_LOGGING_FORMAT=json`. `runtime/src/log.rs::init_logging` matches on `(telemetry.enabled, format)` to install the right `fmt::layer()`/`fmt::layer().json()` combined with the OpenTelemetry layer when telemetry is on. JSON applies to the stdout layer only - OTel export pipeline unaffected by this flag.

## Exemplars

| Purpose                                                   | Plugin                                                                                  |
| --------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| Simplest sink (read first)                                | `sinks/stdout_sink/`                                                                    |
| Real-infra sink + integration tests                       | `sinks/postgres_sink/` + `integration/tests/connectors/postgres/postgres_sink.rs`       |
| Feature-rich sink config (validation, batch modes, retry) | `sinks/http_sink/`                                                                      |
| Atomic counters on hot path                               | `sinks/mongodb_sink/`                                                                   |
| Simplest source (4 canonical state tests)                 | `sources/random_source/`                                                                |
| Real-infra source                                         | `sources/postgres_source/` + `integration/tests/connectors/postgres/postgres_source.rs` |

Read the relevant exemplar end-to-end before writing or modifying a connector.

## Concrete efficiency patterns

Each implemented in at least one in-tree plugin or runtime path.

| Pattern                                                                 | Where                                                                              | When                                              |
| ----------------------------------------------------------------------- | ---------------------------------------------------------------------------------- | ------------------------------------------------- |
| `std::mem::replace(&mut message.payload, Payload::Raw(vec![]))`         | `sinks/http_sink/src/lib.rs` (`send_individual`, `send_ndjson`, `send_json_array`) | Take payload from `&mut ConsumedMessage` no clone |
| `std::mem::take(&mut batch)`                                            | `runtime/src/sink.rs::consume_messages`, `manager/{sink,source}.rs`                | Drain a `Vec` field with one move                 |
| `std::mem::swap`                                                        | `sinks/http_sink` (retry_delay / max_retry_delay validation)                       | Fix config-field ordering in `new()`              |
| `Vec::with_capacity(messages.len())`                                    | Every batching sink                                                                | Pre-size per-batch buffers                        |
| `bytes::Bytes::from(body)`                                              | `sinks/{http,influxdb}_sink`                                                       | Cheap ref-counted body sharing across retries     |
| `AtomicU64` counters                                                    | `sinks/mongodb_sink`                                                               | Lock-free per-message metric updates              |
| `Payload::try_to_bytes(&self)`                                          | `sdk/src/lib.rs::Payload::try_to_bytes`                                            | Serialize `Payload::Json` no-clone                |
| `simd_json::to_owned_value(&mut bytes)`                                 | `sdk/src/decoders/json.rs::JsonStreamDecoder::decode`                              | In-place JSON parse                               |
| Brief-lock fetch                                                        | `sources/postgres_source/src/lib.rs::poll_tables`                                  | Lock read -> drop -> I/O -> lock write            |
| `is_transient_error(&e)` SQLSTATE mapping                               | `sinks/postgres_sink/src/lib.rs`                                                   | Driver errors -> retry vs `PermanentHttpError`    |
| Two-constructor pattern (`new` lenient + `try_new` strict + `Default`)  | `sdk/src/decoders/avro.rs::AvroStreamDecoder`                                      | Stateful decoders/encoders where schema can fail  |
| Duplicate-ID FFI guard                                                  | `sdk/src/sink.rs::sink_connector!`, `sdk/src/source.rs::source_connector!`         | Prevents silent data loss on reopen-without-close |
| `restart_guard.try_lock()`                                              | `runtime/src/manager/{sink,source}.rs::restart_connector`                          | No thundering-herd restarts                       |
| `tokio::time::timeout(..., handle).await` + `handle.abort()` on timeout | `runtime/src/manager/source.rs::stop_connector`                                    | Bounded shutdown + leak prevention                |
| `flume::unbounded()` channel                                            | `runtime/src/source.rs::spawn_source_handler` / `source_forwarding_loop`           | MPSC handoff from SDK async task to runtime loop  |
| `tokio::sync::watch::channel(())`                                       | `sdk/src/{sink,source}.rs`, `runtime/src/sink.rs`, `runtime/src/manager/*`         | One-shot shutdown broadcast                       |
| `dashmap::DashMap`                                                      | `runtime/src/manager/sink.rs`, `source.rs::SOURCE_SENDERS`, SDK `INSTANCES`        | Lock-free concurrent keyed access                 |
| `secrecy::SecretString` + `iggy_common::serde_secret::serialize_secret` | `sinks/postgres_sink::PostgresSinkConfig::connection_string`                       | Auto-redact on Debug/Display + serialization      |

## Drop accounting

- **Filter drops** (transform returning `Ok(None)`) bump `messages_filtered`.
- **Every non-filter drop** bumps `errors`. Inside the per-batch loops the count is accumulated in a local and flushed once after the loop via `inc_errors_by_with_labels` (filters likewise via `inc_messages_filtered_with_labels`) - one `Family` lookup per batch, not per message. One-shot drops outside a loop call `inc_errors_with_labels` directly. Hot-path calls always use the pre-built `SinkLabels`/`SourceLabels` cache. `&str`-based wrappers on `Metrics` are `#[cfg(test)]`-only.

## Common review smells

- `.clone()` on `Payload::Json` or `OwnedValue` - use `try_to_bytes(&self)`.
- `&mut self` on `Sink::consume` or `Source::poll` impls - won't compile, flag any creative workaround.
- `std::sync::Mutex` held across `.await` - swap for `tokio::sync::Mutex`.
- Missing `[lib] crate-type = ["cdylib", "lib"]` in plugin `Cargo.toml`.
- Source plugin without the four canonical state tests (see `connector-testing`).
- New silent message drop without a metric increment.
- Wrapping `format!()` around args passed to `error!`/`warn!`/`info!`/`debug!` - eager `format!` allocates even when level filters the line out. Pass args directly: `error!("foo: {x}")` or `error!(error = %x, "foo")`.
- Logging a connection string, API key, or token.
- Plain `String` for a credential field - use `SecretString`.
- `tokio::spawn` inside plugin code - runtime owns lifecycle.
- `std::time::SystemTime::now()` in transforms - non-deterministic, breaks tests.

## File map

```text
core/connectors/
├── README.md
├── runtime/
│   ├── src/
│   │   ├── main.rs              FFI structs (SinkApi, SourceApi), plugin path resolution
│   │   ├── manager/{sink,source}.rs  Lifecycle, restart_guard, status transitions
│   │   ├── configs/             Local + HTTP config providers, ConfigEnv derive
│   │   ├── {sink,source,stream,transform,state,context}.rs
│   │   ├── benchmark.rs, log.rs, metrics.rs
│   │   └── api/                 HTTP control plane (/sinks, /sources, /stats, /metrics)
│   └── example_config/          Reference TOML
├── sdk/
│   └── src/
│       ├── lib.rs               Sink/Source traits, Payload, Schema, Error
│       ├── {sink,source}.rs     FFI containers + sink_connector!/source_connector! macros
│       ├── decoders/encoders/   Per-schema (json/raw/text/proto/flatbuffer/avro)
│       ├── transforms/          add/delete/update/filter fields, format conversions
│       └── retry.rs             CircuitBreaker, HttpRetryMiddleware
├── sinks/<name>_sink/
└── sources/<name>_source/

core/integration/tests/connectors/    # Real-infra integration tests
├── fixtures/                          # testcontainers-modules wrappers per backend
├── postgres/, elasticsearch/, mongodb/, iceberg/, influxdb/, quickwit/, http/, delta/
├── runtime/                           # error_isolation, benchmark
└── api/                               # HTTP API endpoint tests
```

## Cite by symbol, not line number

Skills cite files by path + symbol (`PostgresContainer::start` in `core/integration/tests/connectors/fixtures/postgres/container.rs`). Line numbers go stale on the first refactor. symbol names survive. Grep the symbol if you need the exact spot.

---

Discussion / help: see [AGENTS.md](../../../AGENTS.md#discussion-and-support).
