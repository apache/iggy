---
name: connector-runtime
description: Modify the Apache Iggy Connectors runtime (`core/connectors/runtime/`) - the host process that loads plugins, manages lifecycle, and bridges Apache Iggy streams to plugins via FFI. Use when changing plugin loading, the FFI dispatch, the sink/source manager, state storage, config providers, the HTTP control API, metrics, or transforms wiring. NOT for plugin code.
---

# Modifying the Apache Iggy Connectors Runtime

The runtime is the host. It loads `.so`/`.dylib`/`.dll` plugins, drives their lifecycle, ferries messages between Apache Iggy and plugins, exposes a `/stats`, `/metrics`, and control HTTP API. Bugs here affect every plugin - tread carefully.

## File map

```text
runtime/src/
├── main.rs                  Entry, plugin path resolution, SourceApi/SinkApi FFI structs
├── sink.rs                  Sink lifecycle, Iggy consumer wiring, FFI consume calls
├── source.rs                Source lifecycle, flume forwarding, state save loop
├── stream.rs                Stream + consumer/producer setup
├── transform.rs             Loads transforms from config, applies them in chain
├── state.rs                 FileStateProvider, ConnectorState file I/O
├── context.rs               Per-instance context (Iggy client, metrics, config)
├── error.rs                 RuntimeError with as_code() for HTTP responses
├── log.rs                   init_logging (text/JSON x telemetry-on/off matrix) + LOG_CALLBACK
├── benchmark.rs             emit_sink_event / emit_source_event + as_micros (target = iggy_connectors::benchmark)
├── metrics.rs               Prometheus families + gauges + stage histograms + label caches
├── stats.rs                 /stats endpoint payload assembly
├── manager/
│   ├── mod.rs
│   ├── sink.rs              SinkManager, status transitions, restart guard
│   └── source.rs            SourceManager (parallel structure)
├── configs/
│   ├── mod.rs
│   ├── runtime.rs           RuntimeConfig (top-level) + LoggingConfig + LogFormat (text/json)
│   ├── connectors.rs        ConnectorConfig, SinkConfig, SourceConfig (with verbose + benchmark flags)
│   └── connectors/
│       ├── local_provider.rs  Filesystem-based config loader
│       └── http_provider.rs   HTTP-based config loader
└── api/                     HTTP control + observability endpoints
```

## Architecture invariants

Load-bearing assumptions. Breaking them silently corrupts data or hangs the process.

### Plugin instance identity

- Each plugin instance has a globally unique `plugin_id: u32`, monotonically allocated from `PLUGIN_ID: AtomicU32` in `main.rs`. Never reused for the lifetime of the process.
- The same `.so` may be loaded for many instances - each gets a fresh `plugin_id` but **shares the `Container` (dlopen handle)** via `Arc` in `manager/sink.rs` and `manager/source.rs`.
- All FFI calls (`iggy_*_open`, `iggy_*_consume`, etc.) are keyed by `plugin_id`. Mixing IDs = data routed to wrong plugin.

### FFI pointer lifetimes

- Config and state are passed as `(*const u8, usize)`. Valid for the call duration only. The plugin **must not retain the pointer** - it must copy if it needs the bytes. The SDK macros do this correctly; don't bypass them.
- `LogCallback` lifetime spans the entire process - kept in static memory in `log.rs`. Don't make it dynamic.

### Container ownership

- `dlopen2::wrapper::Container<SinkApi>` / `Container<SourceApi>` (FFI structs defined in `main.rs`) is shared via `Arc` across all instances of the same .so.
- The Arc must outlive every spawned task that calls into the plugin. The manager holds the Arc in `SinkDetails`/`SourceDetails` for the lifetime of the connector.
- Unloading a .so while a task is mid-FFI-call would segfault. The runtime does not unload .so files. Don't try to add hot reload without rearchitecting.

### Plugin manager state

- `SinkManager` and `SourceManager` use `DashMap<String, Arc<Mutex<Details>>>`. Key = connector key (TOML key field).
- Status transitions (`Starting → Running → Stopped/Error`) update running counters only on transitions to/from `Running`. See `SinkManager::update_status`.
- `restart_guard: Arc<Mutex<()>>` prevents concurrent restart of the same connector. `restart_connector()` uses `try_lock()` - if busy, returns OK without restarting. **This is intentional** to prevent thundering-herd restarts.

### Serialization split

- **FFI message payloads** (`TopicMetadata`, `MessagesMetadata`, `RawMessages`, headers): `postcard` - compact binary, stable wire format.
- **Connector configs** passed to plugin: `serde_json` - human-editable TOML in, JSON over FFI.
- **`ConnectorState`** bytes inside plugins: `rmp_serde` (MessagePack). The runtime treats the bytes opaquely.

Each is in its place for a reason: stability vs editability vs compactness. Don't mix.

## Benchmark mode

`SinkConfig`/`SourceConfig` carry a `benchmark: bool` (`#[serde(default)]`, env-addressable via `ConfigEnv` as `IGGY_CONNECTORS_<TYPE>_<KEY>_BENCHMARK=true`). Implementation lives in `runtime/src/benchmark.rs` (text event emitters + `as_micros`) and `runtime/src/metrics.rs::{Stage, SinkLabels, SourceLabels, observe_stage_with_labels, STAGE_BUCKETS_SECONDS}`.

**Two independent surfaces**:

1. **Per-batch tracing event** - target `iggy_connectors::benchmark`, gated by `benchmark = true`. One `info!` line per batch with `prepare_us/ffi_us/total_us` for sinks and `decode_us/prepare_us/iggy_send_us/state_save_us/total_us` for sources. Filter via `RUST_LOG=iggy_connectors::benchmark=info`. On startup, the sink/source loop also emits a one-shot `"Benchmark mode enabled for ..."` info log so operators can confirm the flag took effect.

2. **Stage-duration histograms** - `iggy_connector_stage_duration_seconds{connector_key, connector_type, stage}`. **Always on** regardless of the flag. Observed at the same per-stage points the text event uses. Bucket boundaries: `50µs, 100µs, 250µs, 500µs, 1ms, 5ms, 10ms, 50ms, 100ms, 250ms, 500ms, 1s, 5s`. Read via `/metrics` endpoint (requires `[http.metrics] enabled = true` in runtime config). Lets operators do `histogram_quantile(0.95, ...)` queries in Prometheus.

When wiring new stages in the runtime, mirror the existing pattern: capture
`Instant::now()` before the stage, capture `.elapsed()` after, call
`metrics.observe_stage_with_labels(&labels.stage_x, elapsed)` using the
**pre-built labels cache** (`SinkLabels` / `SourceLabels` from `metrics.rs`).
The cache is built once per consumer/producer task at spawn time and threaded
through (`Arc<SinkLabels>` for sink, `Arc<SourceLabels>` for source). This
avoids per-batch `String::to_owned` for the label keys - cost stays at ~10 ns
per stage (Instant + atomic bucket increment). Histograms are always on
regardless of the `benchmark` flag; only the verbose text event emission is
gated.

## Logging format

`LoggingConfig` in `configs/runtime.rs` exposes `format: LogFormat` (`Text` default, `Json`), env-addressable via `IGGY_CONNECTORS_LOGGING_FORMAT=json`. `log::init_logging` matches on `(telemetry.enabled, format)` and installs the correct combination of `fmt::layer()` / `fmt::layer().json()` and the OpenTelemetry layer. The OTel export pipeline is untouched by `format` - only the stdout layer flips between text and JSON.

When extending logging, preserve the 4-branch matrix - tracing-subscriber layers have different concrete types for text vs JSON and cannot be unified without boxing the entire registry.

## The `verbose` flag

`SinkConfig` and `SourceConfig` in `configs/connectors.rs` carry `verbose: bool` (default false via `#[serde(default)]`) at the top level. The runtime threads it through `spawn_consume_tasks` / source equivalent to `consume_messages`, where it gates per-batch log promotion:

```rust
if verbose {
    info!("Processing {messages_count} messages for sink connector with ID: {plugin_id}");
} else {
    debug!("Processing {messages_count} messages for sink connector with ID: {plugin_id}");
}
```

When extending the runtime with new per-batch logging, follow the same pattern - default to `debug!`, upgrade to `info!` when `verbose` is set. Don't introduce a third level. Plugin authors are encouraged to mirror this pattern internally for their own diagnostics.

## Config provider rules

### Local provider (`configs/connectors/local_provider.rs`)

- Reads `*.toml` from `config_dir`. Skips hidden files (`.`) and `Cargo.toml`.
- Filename convention: `{key}_{type}[_v{N}].toml` (e.g., `postgres_sink_v2.toml`).
- One TOML deserialized into either `SinkConfig` or `SourceConfig`. Grouped by `key`; highest `version` wins unless `.active_versions.toml` (leading dot - it's a hidden file by convention) overrides. Built by `LocalConnectorsConfigProvider::active_versions_file_path` as `{config_dir}/.active_versions.toml`.

### HTTP provider (`configs/connectors/http_provider.rs`)

- Uses `reqwest_middleware` + `RetryTransientMiddleware`.
- Retries 5xx + connection errors. **Never retries 4xx** - per the `PermanentHttpError` convention.
- Default URL templates documented in `runtime/README.md`. Templates configurable via TOML.

### Env-var overrides via `ConfigEnv` derive

`SinkConfig`, `SourceConfig`, and several inner structs derive `ConfigEnv`
(`configs_derive::ConfigEnv`). This generates env-var addressability with the
pattern `IGGY_CONNECTORS_<TYPE>_<KEY>_<FIELD>` for primitive fields. Used
heavily by integration tests to inject testcontainer ports into static TOML -
see `core/integration/tests/connectors/fixtures/postgres/container.rs` for the
env-var constants. When adding a config field, mark it `#[config_env(skip)]`
for compound types or `#[config_env(leaf)]` for primitives that should be
env-addressable.

### Versioning

- Configs carry `version: u64`. Local provider auto-increments if absent; HTTP provider trusts the server.
- Restart on version change uses `restart_guard`.

## State storage (`state.rs`)

- File path: `{state_path}/source_{connector_key}.state`.
- `save()` is crash-atomic: bytes are written to `{path}.tmp`, fsynced, then `rename`d into place. POSIX guarantees the rename, so a crash leaves either the previous good state or the new one - never a truncated file. On Unix, the parent directory is fsynced after the rename as a best-effort to make the rename itself durable. A `Mutex<()>` serializes concurrent saves on the same provider.
- Save happens after every successful Apache Iggy send. Save failure logs + continues; the next batch retries the write.
- `load()` returns `Ok(None)` for missing or empty files. The file is created lazily on first successful save.
- Sinks have no state - only sources.

## Source forwarding loop

Per polling source:

1. `iggy_source_handle(id, send_callback)` - plugin registers itself.
2. Plugin polls + invokes `send_callback(plugin_id, ptr, len)`.
3. `send_callback` runs in the SDK macro's spawned task; pushes postcard-serialized `ProducedMessages` into a `flume` channel keyed by `plugin_id` in `SOURCE_SENDERS: Lazy<DashMap<u32, SourceSenderEntry>>`. `SourceSenderEntry` wraps the sender + `Arc<Metrics>` + plugin_key so the FFI callback can bump `errors` on postcard deserialize failure or channel-closed send without needing access to the per-connector runtime context.
4. `source_forwarding_loop` pulls from the channel, deserializes, applies transforms, encodes via `StreamEncoder`, sends to Iggy producer.
5. On success, save returned `ConnectorState` via `FileStateProvider`.

Gotchas:

- `SOURCE_SENDERS` must be cleaned up on connector close or **memory leak** (channel + task).
- The runtime's `spawn_source_handler` wraps the outer
  `iggy_source_handle(id, callback)` FFI registration call in
  `tokio::task::spawn_blocking()`. That call returns quickly (the SDK macro
  internally `runtime.spawn`s an async `handle_messages` task and returns).
  **Subsequent `send_callback` invocations happen from that async task, not
  from `spawn_blocking`** - they hand the postcard bytes into a
  `flume::unbounded` channel that `source_forwarding_loop` drains. A
  long-running synchronous poll inside the plugin would block one Tokio
  worker; async polls don't.
- No timeout on the registration call - a plugin whose `iggy_source_handle` never returns stalls one blocking worker for the lifetime of the runtime.

## Sink consumption loop

Per `[[streams]]` entry per sink:

1. Build / ensure consumer group `iggy-connect-sink-{key}` (note: `connect`, not `connectors` - matches the docker image name `apache/iggy-connect`; see `runtime/src/sink.rs::default_consumer_group`). Overridable via the stream config's `consumer_group` field.
2. Spawn one task per topic in the stream config (`spawn_consume_tasks`).
3. Poll Iggy → batch messages (`batch_length` default 1000, `poll_interval` default 5ms). A `consumer.next()` returning `Err` (transport/decode failure at the Iggy client boundary) bumps `errors` and `continue`s - the offset has already auto-committed via `AutoCommit::When(AutoCommitWhen::PollingMessages)`, so the affected message is effectively dropped.
4. Decode via `Schema::decoder()`. Decode failure logs `error!` and bumps `errors`. `messages_filtered` is reserved for the transform filter contract (`Ok(None)`).
5. Apply transforms (chain). A transform returning `Ok(None)` filters the message - `messages_filtered{connector_type="Sink"}` is bumped (intentional drop, distinct from `errors`).
6. Postcard-encode batch as `RawMessages`, headers as separate postcard blob. Per-message failure (missing required field, payload conversion, header serialization) bumps `errors` and skips that message. Batch-level postcard failure (topic metadata, messages metadata, the wrapping `RawMessages`) propagates `Err` out of `process_messages` → `consume_messages` → the spawned task wrapper in `spawn_consume_tasks`, which bumps `errors` for the failed batch + records `set_error`.
7. Call `iggy_sink_consume(id, topic_meta, messages_meta, messages)`.
8. Non-zero return → log + increment errors metric.

Gotchas:

- Batch flushes on `current_offset != message_offset` (gap detected) OR batch full - causes higher latency for low-volume topics.
- Auto-commit happens on poll - idempotency at the sink protects against re-delivery on crash before plugin write.

## Error categorization

`RuntimeError` (`error.rs`) carries `as_code()` for HTTP API responses.

| Class | Fatal? | Example |
| ------- | -------- | --------- |
| Config load | yes (process exit) | Bad TOML, missing required field |
| Iggy client init | yes | Auth failure at startup |
| State dir create | yes | Permission denied |
| Plugin .so resolve | per-plugin (FailedPlugin) | Missing file |
| Plugin open FFI | per-plugin (status = Error) | Plugin returned non-zero |
| Message decode | per-message (skip) | Bad protobuf bytes |
| Iggy send | per-batch (metric) | Network blip |
| State save | per-batch (log) | Disk full |

Fatal errors propagate to `main` and exit. Per-connector and per-message errors are isolated.

## Metrics (`metrics.rs`)

All families labeled by `connector_key` + `connector_type` (the histogram adds `stage`):

- **Counters**: `iggy_connector_messages_{produced,sent,consumed,processed,filtered,errors}_total`.
  - `messages_filtered_total` - intentional drops by transforms returning `Ok(None)`.
  - `errors_total` - unexpected drops (decode/encode/build failure, missing field, etc.) AND batch-level failures (Iggy send, state save, transform-error propagation).
- **Histograms**: `iggy_connector_stage_duration_seconds{stage}` - per-batch stage timing. `stage` ∈ `{Prepare, Ffi, Decode, IggySend, StateSave, Total}`. Buckets `STAGE_BUCKETS_SECONDS` (50µs - 5s, 13 buckets). Always populated regardless of any flag; observation lives next to each stage in `source_forwarding_loop` and `consume_messages`/`process_messages`. Histograms are scrape-served at `/metrics` only when `[http.metrics] enabled = true` in the runtime TOML.
- **Gauges**: `iggy_connectors_{sources,sinks}_{total,running}`.

`/stats` JSON surface mirrors counters per-connector via `ConnectorStats` (sdk `api.rs`): `messages_filtered`, `errors`, and the kind-specific produced/sent/consumed/processed.

When adding a new metric:

- Add the family to `Metrics` struct + `init`, register with a name + help text.
- For families with new label sets (like the stage histogram), define an `EncodeLabelSet` struct + `EncodeLabelValue` enum.
- For histograms, pass a constructor `fn() -> Histogram` to `Family::new_with_constructor`.
- Add unit tests under `mod tests`; mirror the existing `given_*_when_*_should_*` BDD names.

## Hard rules

1. **Filter drops bump `messages_filtered`** (a transform returning `Ok(None)`
   is the documented filter contract). **Every non-filter drop bumps `errors`**
   via `metrics.inc_errors_with_labels(&labels.counter)` before `continue` (or
   `metrics.inc_messages_filtered_with_labels(&labels.counter, 1)` for the
   filter case). Hot-path calls always use the pre-built
   `SinkLabels`/`SourceLabels` cache; the `&str`-based wrappers
   (`increment_errors`/`increment_messages_filtered`/...) on `Metrics` are
   `#[cfg(test)]`-only and exist solely for ergonomic test code. Wired at
   every drop site:
   - `runtime/src/sink.rs::consume_messages` - `consumer.next()` Err (transport boundary).
   - `runtime/src/sink.rs::process_messages` - decode, missing required fields (id/offset/checksum/timestamp/origin_timestamp), payload conversion, header serialization (per-message; previously batch-level).
   - `runtime/src/sink.rs::spawn_consume_tasks` task wrapper - bumps once on any `consume_messages` Err (covers batch-level postcard failures propagated via `?`).
   - `runtime/src/source.rs::source_forwarding_loop` - payload decode, prepare (transform/encode) failure, Iggy send Err, state save Err.
   - `runtime/src/source.rs::process_messages` - transform encode failure, `build_iggy_message` failure.
   - `runtime/src/source.rs::handle_produced_messages` - postcard deserialize failure from plugin, `sender.send` channel-closed (via the `SourceSenderEntry.metrics` handle).

   Both counters surface in `/metrics` (`iggy_connector_messages_filtered_total`, `iggy_connector_errors_total`) and in `/stats` (`ConnectorStats.messages_filtered`, `.errors`). When adding a new code path that drops a message, mirror this pattern.
2. **Never block the executor.** All I/O is async. `spawn_blocking` only for FFI calls (already in place).
3. **Plugin ID counter is monotonic.** Don't reset, don't reuse.
4. **FFI return codes:** `0` success, non-zero failure. Stick to this.
5. **Don't add static mutable state** beyond the existing `LOG_CALLBACK`, `PLUGIN_ID`, `SOURCE_SENDERS`.
6. **Always pair `cleanup_sender(id)` with shutdown** for sources (avoid the flume leak).
7. **Restart uses `restart_guard.try_lock()`** - no thundering-herd regression.
8. **Don't add timeouts to plugin FFI calls without a kill-task strategy.** A timeout that returns from the runtime but leaves the plugin running has the worst of both worlds.

## Common pitfalls

1. **Mutating `INSTANCES` (in the SDK macro) from the runtime side** - impossible by design; only the macro touches it.
2. **Calling `iggy_*_open` with a duplicate ID** - returns `-1` by design. If you see this, the runtime forgot to call `iggy_*_close` first.
3. **Holding a `DashMap` shard guard across `.await`** - causes lockups. Pattern: `.get().clone()` out, drop the guard, await.
4. **Adding fields to `#[repr(C)]` types without an SDK version bump** - existing plugins will misalign on `postcard::from_bytes`.
5. **Changing the default consumer group name** without coordinating with operators - they have offsets stored there.

## Before declaring done

```text
cargo fmt --all
cargo sort --no-format --workspace
cargo clippy -p iggy-connectors --all-targets -- -D warnings
cargo test -p iggy-connectors

# Sanity-build all in-tree plugins:
cargo build -p iggy_connector_stdout_sink -p iggy_connector_random_source

# Run runtime-focused integration tests (filter via test path; no separate --test target):
cargo test -p integration -- connectors::runtime::

# Smoke-test with example config:
IGGY_CONNECTORS_CONFIG_PATH=core/connectors/runtime/example_config/config.toml \
  cargo run --bin iggy-connectors
```

Update `runtime/README.md` if endpoints, env vars, or config schema change.
