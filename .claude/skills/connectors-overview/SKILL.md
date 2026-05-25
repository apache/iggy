---
name: connectors-overview
description: Entry point and index for the Apache Iggy Connectors subsystem (`core/connectors/`). Load this first when working anywhere under `core/connectors/` - runtime, SDK, sinks, sources, or transforms. Routes to the focused per-area skills.
---

# Apache Iggy Connectors - Overview

## What sinks and sources are

A **sink** is a plugin that **consumes** messages from one or more Apache Iggy streams and writes them to an external system (Postgres, Elasticsearch, Iceberg, HTTP endpoint, Mongo, object store, stdout, ...). It implements `iggy_connector_sdk::Sink`.

A **source** is a plugin that **produces** messages from an external system into Apache Iggy streams (poll a DB table, scroll an ES index, generate random data, ...). It implements `iggy_connector_sdk::Source`.

Both are compiled as `cdylib` shared libraries (`.so`/`.dylib`/`.dll`) and loaded by the **connectors runtime** at startup via `dlopen`. The runtime drives lifecycle (open/handle/consume/close), bridges Iggy ⇄ plugin via a small FFI, applies optional transforms in between, and persists source state.

### End-to-end flow

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

A message header set on the source side rides through transforms (which may modify, drop, or pass it through) and arrives at the sink with `BTreeMap<HeaderKey, HeaderValue>` headers preserved deterministically.

## Subsystem layout

| Area | Path | Role |
| ------ | ------ | ------ |
| Runtime | `core/connectors/runtime/` | Host process: load plugins, drive lifecycle, bridge streams ⇄ plugins, expose `/stats`, `/metrics`, control HTTP API |
| SDK | `core/connectors/sdk/` | Stable contract: `Sink`/`Source`/`StreamDecoder`/`StreamEncoder`/`Transform` traits, `Payload`/`Schema`, FFI macros, retry helpers |
| Sinks | `core/connectors/sinks/<name>_sink/` | One crate per sink plugin |
| Sources | `core/connectors/sources/<name>_source/` | One crate per source plugin |
| Integration tests | `core/integration/tests/connectors/` | Real-infra tests using `testcontainers-modules` + `#[iggy_harness]` macro |

## Which skill to load

- Writing **a new sink plugin** → `connector-sink`
- Writing **a new source plugin** → `connector-source`
- Adding **schemas, decoders, encoders, traits in the SDK** → `connector-sdk`
- Changing **runtime internals** (FFI, plugin manager, state storage, configs, transforms wiring) → `connector-runtime`
- Adding **a new transform** (add/delete/update fields, format conversion) → `connector-transform`
- Writing **unit OR integration tests** for any of the above → `connector-testing`

## Stick to the existing conventions

The connectors codebase is intentionally repetitive across plugins. Cross-plugin consistency makes review, LLM-assisted contribution, and onboarding tractable. Before writing anything new:

1. **Find the closest existing plugin** by shape (DB-write sink → `postgres_sink`; HTTP sink → `http_sink`; polling source → `postgres_source`; etc.).
2. **Copy its file layout, naming, log format, error mapping, test structure** verbatim. Only the backend-specific code (the client call) should differ.
3. **If you find yourself reinventing a pattern** (writing your own retry loop, your own batch-mode enum, your own state struct), stop and check whether the SDK or another plugin already has it. The SDK's `retry.rs` covers HTTP retry + circuit breaker. Existing plugins have the batching, idempotency, header-encoding patterns.
4. **Don't refactor unrelated code in the same PR.** Match conventions; propose convention changes separately.

## Universal rules (every connectors PR)

These apply everywhere - violate them and the PR will be flagged in review.

### Style + structure

1. **Apache 2.0 license header** at the top of every new `.rs` and `Cargo.toml`. Copy from any existing connector file. `.rs` uses `/* ... */`, `Cargo.toml` uses `# ...`.
2. **Avoid LLM-slop tells: em dashes (—), gratuitous semicolons, hedging narrative ("note that...", "it's important to..."), trailing summaries.** Em dashes and semicolons aren't banned outright (sometimes the right tool), but in this codebase they're almost always a sign of generated prose. Default to hyphens, commas, or rewriting the sentence. Same rule for code, comments, commit messages, PR descriptions.
3. **Imports at the top of the file.** No inline `use` inside functions or blocks. Group: std, external crates, then `crate::`.
4. **No banner comments** (`// === Section ===`). Use functions, types, and naming for structure.
5. **Code reads top to bottom like a book.** Public consts and public functions first; helpers and internals below; deeper-level helpers below those. A reader scrolling top-down should hit each function before they hit any function it calls. Don't make readers jump around the file.
6. **Precise variable names.** No `b`, `p`, `t`, `m`. Use `batch`, `payload`, `timestamp`, `message`. Match literal API field names as log labels (`offset=`, `current_offset=`) - don't invent.
7. **Comments explain WHY**, not WHAT. Default to no comment. Add one only when a hidden constraint, invariant, or workaround would surprise a reader.

### Performance

1. **Zero clone in hot loops.** Per-message work runs millions of times. Use `&self`, `&str`, `&[T]`, `&Payload`. For `Payload::Json`, the SDK provides `try_to_bytes(&self)` which does a single serialization pass (no `OwnedValue` clone). To move a field out of a struct without cloning, use `std::mem::replace` / `std::mem::take`.
2. **`Vec::with_capacity(n)` when building per-batch vectors** - reallocation in hot path is wasted work.
3. **Avoid `String::from_utf8(bytes)` on every message** when `std::str::from_utf8(&bytes)` (borrowed) works.

### Async + concurrency

1. **`async fn` only.** Never `.block_on()` inside a `Sink`/`Source` impl - the runtime drives the executor.
2. **`tokio::sync::Mutex` (not `std::sync::Mutex`)** wherever a lock is held across `.await`.
3. **Hold locks briefly.** Pattern: lock, read/clone what you need, drop the guard, then do I/O. Never hold a `Mutex` across an external I/O await.
4. **Atomic counters** (`AtomicU64`) preferred over `Mutex<u64>` for hot-path metrics.

### Logging

1. **`tracing`, not `println!`.** `info!` for lifecycle, `debug!` per-batch detail, `warn!` for recoverable issues, `error!` for failures. Every log line includes `connector ID: {self.id}` and the connector name.
2. **The `verbose` flag.** Both `SinkConfig` and `SourceConfig` carry a `verbose: bool` field at the top level. The **runtime** uses it to upgrade certain `debug!` logs to `info!` (per-batch processing/timing). **Plugins should mirror this** by accepting a similar plugin-level toggle (e.g., `postgres_sink::PostgresSinkConfig::verbose_logging`) and emitting more detailed `info!` logs when set. Verbose is off by default.
3. **Never log secrets** (connection strings, API keys, tokens). Redact URLs at the log site. Credential-bearing config fields must use `SecretString` with the workspace serde redactor (see Exemplars and the per-area skills).

### Errors

1. **Use `Error::PermanentHttpError` / `Error::SchemaMismatch` / `Error::CatalogCommitError` for non-retryable failures.** Returning a transient variant for bad data trips circuit breakers and hammers the backend.
2. **No `unwrap()` / `expect()`** on `Result` from external I/O outside tests. Acceptable in tests, in `const` init, and where the invariant is locally provable.

### Toolchain

1. **Never run `cargo install`, `pip install`, `brew install`** etc. without explicit user authorization. Use the toolchain already configured.
2. **Verification order before declaring done:** `cargo fmt --all` → `cargo sort --no-format --workspace` → `cargo clippy --all-features --all-targets -- -D warnings` → `cargo test`. CI enforces all four.

### Idiomatic Rust

1. **Prefer standard traits over free helpers.** Parsing = `FromStr` + `.parse()`. Formatting = `Display` + `.to_string()`. Conversion = `From`/`TryFrom`. A free `fn parse_foo(s: &str) -> Result<Foo, _>` or `fn format_foo(&Foo) -> String` is a code smell - use the trait.
2. **`#[serde(default)]` or `Option<T>` for new config fields.** Preserves forward compat - existing TOML files won't break.

## Exemplars

| Purpose | Plugin | Why |
| --------- | -------- | ----- |
| Simplest sink (read this first) | `sinks/stdout_sink/` | ~100 LOC, the canonical minimal shape |
| Real-infra sink with integration tests | `sinks/postgres_sink/` + `core/integration/tests/connectors/postgres/postgres_sink.rs` | Docker testcontainers via `PostgresContainer`, idempotency via PK, `is_transient_error` mapping, both unit + integration tests |
| Most feature-rich sink config | `sinks/http_sink/` | Validation patterns, batch modes (individual / ndjson / json_array / raw), `std::mem::replace` for zero-copy payload extraction, retry middleware. No external infra in tests though - uses `wiremock` |
| `AtomicU64` counters on the hot path | `sinks/mongodb_sink/` | Lock-free metric updates instead of `Mutex<State>` |
| Simplest source | `sources/random_source/` | The four canonical state round-trip tests live here |
| Real-infra source | `sources/postgres_source/` + `core/integration/tests/connectors/postgres/postgres_source.rs` | Cursor + delete-after-read + processed-column modes, restart-survives-state integration test |

**Read the relevant exemplar end-to-end before writing or modifying a connector.** Patterns are intentionally consistent across plugins - "originality" usually means missing a convention. When in doubt, copy verbatim and adapt the backend-specific bits.

## Concrete efficiency patterns observed in the codebase

These are not theoretical - each is implemented in at least one in-tree plugin or runtime path. Match them.

| Pattern | Where to look | When to use |
| --------- | --------------- | ------------- |
| `std::mem::replace(&mut message.payload, Payload::Raw(vec![]))` | `sinks/http_sink/src/lib.rs` (3 sites inside `send_individual`, `send_ndjson`, `send_json_array`) | Taking a payload out of a `&mut ConsumedMessage` without cloning |
| `std::mem::take(&mut batch)` | `runtime/src/sink.rs::consume_messages`, `runtime/src/manager/{sink,source}.rs` (`handler_tasks`, `task_handles`) | Draining a `Vec` field with one move |
| `std::mem::swap(&mut a, &mut b)` | `sinks/http_sink/src/lib.rs` (retry_delay vs max_retry_delay validation) | Fixing config-field ordering in `new()` |
| `std::mem::replace(&mut self.config, new)` | `sdk/src/decoders/avro.rs::update_config`, `sdk/src/encoders/avro.rs::update_config` | Hot-swapping internal config without cloning the old |
| `Vec::with_capacity(messages.len())` | Every sink that builds per-batch buffers (`postgres_sink`, `mongodb_sink`, `quickwit_sink`, `elasticsearch_sink`, `http_sink`, `iceberg_sink`) | Pre-sizing batch output to exact size |
| `Vec::with_capacity(messages.len() * 2)` | `sinks/elasticsearch_sink/src/lib.rs` (bulk API needs index-line + doc-line per message) | Pre-sizing with a known multiplier |
| `String::with_capacity(messages.len() * 256)` and `Vec::with_capacity(messages.len() * 256)` | `sinks/influxdb_sink/src/lib.rs` (line protocol body), `sinks/http_sink/src/lib.rs` (ndjson body) | Heuristic byte-size pre-allocation for serialized bodies |
| `bytes::Bytes::from(body)` | `sinks/http_sink/src/lib.rs`, `sinks/influxdb_sink/src/lib.rs` | Cheap ref-counted body sharing across retry attempts - `Bytes::try_clone()` is O(1) |
| `AtomicU64` for counters | `sinks/mongodb_sink/src/lib.rs` (`messages_processed`, `insertion_errors`) | Lock-free per-message metric updates |
| `Payload::try_to_bytes(&self)` | `sdk/src/lib.rs::Payload::try_to_bytes` (the docstring spells out the optimization) | Serializing `Payload::Json` to bytes without cloning the `OwnedValue` tree |
| `simd_json::to_owned_value(&mut bytes)` | `sdk/src/decoders/json.rs::JsonStreamDecoder::decode` | In-place JSON parsing (mutates input buffer, no extra allocation) |
| Brief-lock fetch pattern | `sources/postgres_source/src/lib.rs::poll_tables` | Read cursor under `Mutex`, drop guard, do I/O, take `Mutex` again to write |
| `is_transient_error(&e)` mapping | `sinks/postgres_sink/src/lib.rs` (SQLSTATE mapping for `40001`, `40P01`, `57P01-03`, `08000/03/06`) | Classifying driver errors for retry vs `PermanentHttpError` |
| Two-constructor pattern (`new` lenient, `try_new` strict, `Default`) | `sdk/src/decoders/avro.rs::AvroStreamDecoder` | Stateful decoders/encoders where schema loading can fail |
| Duplicate-ID guard in FFI macro | `sdk/src/sink.rs::sink_connector!`, `sdk/src/source.rs::source_connector!` | Prevents silent buffered-data loss on reopen-without-close |
| `restart_guard.try_lock()` (not `lock()`) | `runtime/src/manager/sink.rs::restart_connector`, `runtime/src/manager/source.rs::restart_connector` | No thundering-herd restarts |
| `tokio::time::timeout(Duration::from_secs(5), handle).await` | `runtime/src/manager/sink.rs::stop_connector`, `runtime/src/manager/source.rs::stop_connector` | Bounded wait when joining task handles on shutdown |
| `flume::unbounded()` channel | `runtime/src/source.rs::spawn_source_handler` (registers `SOURCE_SENDERS` entry; the consumer side is `source_forwarding_loop`) | Cheap MPSC handoff from the SDK's async `handle_messages` task into the runtime's forwarding loop. Note: the FFI registration call `iggy_source_handle(id, callback)` runs in `spawn_blocking`, but the `send_callback` invocations come from the SDK's spawned async task, not from the blocking worker |
| `tokio::sync::watch::channel(())` | `sdk/src/sink.rs`, `sdk/src/source.rs`, `runtime/src/sink.rs`, `runtime/src/manager/*` | Broadcast a one-shot shutdown signal to many tasks |
| `dashmap::DashMap` (lock-free concurrent map) | `runtime/src/manager/sink.rs` (`SinkManager`), `runtime/src/source.rs` (`SOURCE_SENDERS`), SDK macro `INSTANCES` | Concurrent keyed access without a single `Mutex` |
| Connection pool tuning | `sources/postgres_source/src/lib.rs` (`PgPoolOptions::new().max_connections(...)`), `sinks/http_sink/src/lib.rs` (`pool_max_idle_per_host`, `tcp_keepalive`) | Configurable bounds on backend connections |
| `secrecy::SecretString` + `iggy_common::serde_secret::serialize_secret` | `sinks/postgres_sink/src/lib.rs::PostgresSinkConfig::connection_string` | Secrets that auto-redact on `Debug`/`Display` |

## Common review smells (across all areas)

- `.clone()` on `Payload::Json` or `OwnedValue` - use `try_to_bytes(&self)`.
- `&mut self` on `Sink::consume` or `Source::poll` impls - won't compile, but flag any creative workaround.
- `std::sync::Mutex` held across `.await` - swap for `tokio::sync::Mutex`.
- Missing `[lib] crate-type = ["cdylib", "lib"]` in plugin `Cargo.toml`.
- Missing Apache 2.0 header.
- Source plugin without the four canonical state tests (see `connector-testing`).
- New silent message drop without a metric counter increment.
- Wrapping `format!()` around args passed to `error!`/`warn!`/`info!`/`debug!` - e.g., `error!("foo: {}", format!("bar: {}", x))`. The eager `format!()` allocates even when the log level filters the line out. Pass args directly: `error!("foo: {x}")` or `error!(error = %x, "foo")`. (Building a `String` for both logging AND a state field via one `format!()` is fine - that's what the runtime does with `set_error`.)
- Logging a connection string, API key, or token.
- Plain `String` for a credential field. Use `SecretString` (`secrecy` crate) with `#[serde(serialize_with = "iggy_common::serde_secret::serialize_secret")]` so the `/stats` surface and `Debug` impls both redact. Expose with `.expose_secret()` only at the call site that needs raw bytes.
- `tokio::spawn` inside plugin code (the runtime owns lifecycle; orphaned tasks survive `close()`).
- `std::time::SystemTime::now()` in transforms (non-deterministic, breaks tests).

## Benchmark mode (per-batch timing observability)

Every sink and source supports an opt-in `benchmark = true` flag (`SinkConfig`/`SourceConfig`, also overridable via env `IGGY_CONNECTORS_<TYPE>_<KEY>_BENCHMARK=true`). Two observability surfaces:

1. **Tracing events** (only when flag is on) - one structured `info!` per batch under target `iggy_connectors::benchmark` with per-stage microsecond timings:
   - Sink: `connector_type, connector_key, stream, topic, partition_id, current_offset, batch_size, processed_count, prepare_us, ffi_us, total_us`
   - Source: `connector_type, connector_key, stream, topic, batch_size, sent_count, decode_us, prepare_us, iggy_send_us, state_save_us, total_us`
   - Filter live: `RUST_LOG=iggy_connectors::benchmark=info`

2. **Prometheus histograms** (always on, regardless of flag) - aggregated for proper p50/p95/p99 queries:
   - Metric: `iggy_connector_stage_duration_seconds{connector_key, connector_type, stage}`
   - `stage`: `Prepare`, `Ffi` (sink), `Decode`, `IggySend`, `StateSave` (source), `Total` (both)
   - Buckets: 50µs - 5s, geometric (see `runtime/src/metrics.rs::STAGE_BUCKETS_SECONDS`)
   - Query example: `histogram_quantile(0.95, sum(rate(iggy_connector_stage_duration_seconds_bucket{stage="Ffi"}[5m])) by (le, connector_key))`

The flag only gates the verbose text event; the histogram observation is always on. Cost when no observers attached: ~50 ns per batch (5 stages × `Instant::now()` + atomic bucket bump). Negligible at every realistic throughput.

Also new in this surface: `iggy_connector_messages_filtered_total{connector_key, connector_type}` counts intentional drops by transforms returning `Ok(None)`, distinct from `errors_total` for unexpected drops. Both also exposed in `/stats` JSON (`ConnectorStats.messages_filtered`, `.errors`).

Implementation: `core/connectors/runtime/src/benchmark.rs` (text-event
emitters + `as_micros`), `runtime/src/metrics.rs::{Stage, SinkLabels,
SourceLabels, observe_stage_with_labels}` (pre-built label cache avoids
per-batch `String` clones on the hot path; `&str`-based wrappers like
`observe_stage_duration` are `#[cfg(test)]` only). Tests:
`core/connectors/runtime/src/metrics.rs::tests::given_stage_histogram_*`
(unit), `core/integration/tests/connectors/runtime/benchmark.rs` (BDD-named
integration tests: text emission, env override, disabled-flag, histograms
via `/metrics` scrape, JSON log format end-to-end, parser unit tests).

## Logging format

`runtime/src/configs/runtime.rs::LoggingConfig` exposes `format = "text" | "json"` (text default), env-addressable via `IGGY_CONNECTORS_LOGGING_FORMAT=json`. `runtime/src/log.rs::init_logging` matches on `(telemetry.enabled, format)` to install the right `fmt::layer()`/`fmt::layer().json()` combined with the OpenTelemetry layer when telemetry is on. JSON applies to the stdout layer only - the OTel export pipeline is unaffected by this flag.

## File map cheat-sheet

```text
core/connectors/
├── README.md
├── runtime/
│   ├── src/
│   │   ├── main.rs              FFI structs (SinkApi, SourceApi), plugin resolution
│   │   ├── manager/{sink,source}.rs  Plugin lifecycle, restart_guard, status transitions
│   │   ├── configs/             Local + HTTP config providers, ConfigEnv derive
│   │   ├── {sink,source,stream,transform,state,context}.rs
│   │   └── api/                 HTTP control plane (/sinks, /sources, /stats, /metrics)
│   └── example_config/          Reference TOML
├── sdk/
│   └── src/
│       ├── lib.rs               Sink/Source traits, Payload, Schema, Error
│       ├── {sink,source}.rs     FFI containers + sink_connector!/source_connector! macros
│       ├── decoders/encoders/   Per-schema (json/raw/text/proto/flatbuffer/avro)
│       ├── transforms/          add_fields, delete_fields, update_fields, filter_fields,
│       │                        unwrap_envelope, proto_convert, flatbuffer_convert, avro_convert
│       └── retry.rs             CircuitBreaker, HttpRetryMiddleware
├── sinks/<name>_sink/
└── sources/<name>_source/

core/integration/tests/connectors/         # Integration tests (real infra)
├── fixtures/                               # testcontainers-modules wrappers per backend
│   ├── postgres/   (PostgresContainer, PostgresSinkFixture, PostgresSourceJsonFixture, ...)
│   ├── elasticsearch/  (Elasticsearch container + sink/source fixtures)
│   ├── mongodb/ iceberg/ influxdb/ quickwit/ http/ delta/
│   └── wiremock.rs                         # for HTTP sink tests
├── postgres/      postgres_sink.rs, postgres_source.rs, restart.rs, sink.toml, source.toml
├── elasticsearch/  ...
├── runtime/       error_isolation.rs and friends
└── api/           HTTP API endpoint tests
```

## Note on referenced files

This skill set deliberately cites files by path + symbol (`PostgresContainer::start` in `core/integration/tests/connectors/fixtures/postgres/container.rs`) rather than line numbers. Line numbers go stale on the first refactor; symbol names survive. Grep the symbol if you need the exact spot.
