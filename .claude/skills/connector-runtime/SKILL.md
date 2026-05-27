---
name: connector-runtime
description: Modify the Apache Iggy Connectors runtime (`core/connectors/runtime/`) - the host process that loads plugins, manages lifecycle, and bridges Apache Iggy streams to plugins via FFI. Use when changing plugin loading, the FFI dispatch, the sink/source manager, state storage, config providers, the HTTP control API, metrics, or transforms wiring. NOT for plugin code.
---

# Modifying the Apache Iggy Connectors Runtime

Runtime is the host. Loads `.so`/`.dylib`/`.dll` plugins, drives their
lifecycle, ferries messages between Apache Iggy and plugins, exposes
`/stats`, `/metrics`, control HTTP API. Bugs here affect every plugin.

> **Universal connector rules** (benchmark, SecretString, verbose, drop accounting, exemplar patterns) live in
> [connectors-overview](../connectors-overview/SKILL.md). This skill covers runtime internals only.

## Contents

- [STOP and ask the user before](#stop-and-ask-the-user-before)
- [File map](#file-map)
- [Architecture invariants](#architecture-invariants)
- [State storage (`state.rs`)](#state-storage-staters)
- [Source forwarding loop](#source-forwarding-loop)
- [Sink consumption loop](#sink-consumption-loop)
- [Logging format](#logging-format)
- [The `verbose` flag](#the-verbose-flag)
- [Config provider rules](#config-provider-rules)
- [Error categorization](#error-categorization)
- [Metrics (`metrics.rs`)](#metrics-metricsrs)
- [Drop accounting (wired sites)](#drop-accounting-wired-sites)
- [Hard rules](#hard-rules)
- [Common pitfalls](#common-pitfalls)
- [Before declaring done](#before-declaring-done)

## STOP and ask the user before

- Changing FFI `#[repr(C)]` layouts (`SourceApi`, `SinkApi`, `RawMessages`, headers) - existing plugin `.so`s misalign.
- Modifying `state.rs` save protocol (atomic tmp + rename + fsync ordering) - corruption risk.
- Renaming the default consumer group `iggy-connect-sink-{key}` - operators hold offsets there.
- Hot-reloading or unloading a `.so` - mid-FFI tasks would segfault. Architectural change required.
- Changing the SerDe split (postcard for FFI payloads, serde_json for configs, rmp_serde for state) - downstream incompat.

## File map

```text
runtime/src/
├── main.rs                  Entry, plugin path resolution, SourceApi/SinkApi FFI structs
├── sink.rs                  Sink lifecycle, Iggy consumer wiring, FFI consume calls
├── source.rs                Source lifecycle, flume forwarding, state save loop
├── stream.rs                Stream + consumer/producer setup
├── transform.rs             Loads transforms from config, applies them in chain
├── state.rs                 FileStateProvider, atomic ConnectorState file I/O
├── context.rs               Per-instance context (Iggy client, metrics, config)
├── error.rs                 RuntimeError with as_code() for HTTP responses
├── log.rs                   init_logging (text/JSON x telemetry-on/off matrix) + LOG_CALLBACK
├── benchmark.rs             emit_sink_event / emit_source_event + as_micros
├── metrics.rs               Prometheus families + gauges + stage histograms + label caches
├── stats.rs                 /stats endpoint payload assembly
├── manager/{mod,sink,source}.rs   SinkManager / SourceManager, status, restart_guard
├── configs/
│   ├── runtime.rs           RuntimeConfig + LoggingConfig + LogFormat
│   ├── connectors.rs        ConnectorConfig, SinkConfig, SourceConfig (verbose + benchmark)
│   └── connectors/{local,http}_provider.rs
└── api/                     HTTP control + observability endpoints
```

## Architecture invariants

Load-bearing assumptions. Breaking them silently corrupts data or hangs.

### Plugin instance identity

- Each instance gets a globally unique `plugin_id: u32` from `PLUGIN_ID: AtomicU32` in `main.rs`. Never reused.
- Same `.so` may load many instances. each gets a fresh `plugin_id` but shares the `Container` (dlopen handle) via `Arc` (`manager/{sink,source}.rs`).
- All FFI calls keyed by `plugin_id`. Mixing IDs = data to the wrong plugin.

### FFI pointer lifetimes

- Config and state are `(*const u8, usize)`. Valid for the call duration only. Plugin must copy if it needs the bytes past the call. SDK macros do this correctly. don't bypass.
- `LogCallback` lifetime spans the whole process - kept static in `log.rs`. Don't make it dynamic.

### Container ownership

- `dlopen2::wrapper::Container<{Sink,Source}Api>` (defined in `main.rs`) is shared via `Arc` across all instances of the same `.so`.
- Arc must outlive every spawned task that calls into the plugin. Manager holds the Arc in `{Sink,Source}Details` for the connector lifetime.
- Unloading a `.so` while a task is mid-FFI-call would segfault. Runtime does not unload. Hot reload needs a rearchitect.

### Plugin manager state

- `SinkManager` / `SourceManager` use `DashMap<String, Arc<Mutex<Details>>>`. Key = TOML `key` field.
- Status transitions (`Starting → Running → Stopped/Error`) update running counters only on transitions to/from `Running`. See `SinkManager::update_status`.
- `restart_guard: Arc<Mutex<()>>` prevents concurrent restart of the same connector. `restart_connector()` uses `try_lock()` - returns OK without restart if busy. Intentional anti-thundering-herd.

### Serialization split

- FFI message payloads (`TopicMetadata`, `MessagesMetadata`, `RawMessages`, headers): `postcard` (compact, stable wire).
- Connector configs passed to plugin: `serde_json` (TOML in, JSON over FFI - human-editable).
- `ConnectorState` bytes inside plugins: `rmp_serde` (MessagePack - compact, opaque to runtime).

Don't mix.

## State storage (`state.rs`)

- File path: `{state_path}/source_{connector_key}.state`. Mode `0o600` on Unix.
- `save()` is crash-atomic. `rename(2)` is atomic in the namespace (no observer sees a half-renamed file). the preceding `sync_data` on the tmp file plus the post-rename parent-dir `sync_all` are what make a crash leave either the old or new content - never truncated. The parent-dir fsync failure is propagated as `CannotWriteStateFile` (not swallowed) so a lost rename surfaces instead of silently restarting the source from scratch. A `Mutex<()>` serializes concurrent saves on the same provider.
- Save after every successful Apache Iggy send. Save failure logs + continues. next batch retries.
- `load()` returns `Ok(None)` for missing or empty files. After a `NotFound` read, re-stats the parent directory: missing parent -> `Err(CannotOpenStateFile)` so a broken state path fails at init rather than masquerading as "fresh start".
- Sinks have no state - only sources.

## Source forwarding loop

1. `iggy_source_handle(id, send_callback)` - plugin registers itself.
2. Plugin polls + invokes `send_callback(plugin_id, ptr, len)`.
3. Callback runs in the SDK macro's spawned async task. Pushes postcard `ProducedMessages` into a `flume` channel keyed by `plugin_id` in `SOURCE_SENDERS: Lazy<DashMap<u32, SourceSenderEntry>>` (`pub(crate)`). `SourceSenderEntry` wraps the sender + a pre-extracted owned `Counter` (the `errors` series, `Arc<AtomicU64>` inside). The FFI callback bumps errors on deserialize or channel-closed failure with one relaxed atomic - no `Family` lookup, no `Arc<Metrics>` handle.
4. `source_forwarding_loop` pulls from the channel, deserializes, applies transforms, encodes via `StreamEncoder`, sends to Iggy producer.
5. On success, save returned `ConnectorState` via `FileStateProvider`.

**Shutdown ordering (`manager/source.rs::stop_connector`):**

1. Call `iggy_source_close` FIRST. It blocks until the plugin's polling task stops, so no new send callbacks fire after it returns.
2. `cleanup_sender(plugin_id)` NEXT - dropping the channel sender makes the forwarding task's `recv_async()` resolve with `Disconnected` and exit cleanly, instead of blocking until the abort timeout.
3. Finally await spawned handlers with `tokio::time::timeout`. On timeout, `handle.abort()` + drain - prevents leaked tasks colliding with the next `start_connector` (a late `file.save()` could otherwise race the new instance). The silent-drop branch in `handle_produced_messages` only covers the window between close and cleanup.

Gotchas:

- `SOURCE_SENDERS` must be cleaned up on connector close or memory leaks (channel + task).
- `spawn_source_handler` wraps the outer `iggy_source_handle(id, callback)` FFI call in `tokio::task::spawn_blocking()`. That call returns quickly - the SDK macro internally `runtime.spawn`s an async `handle_messages` task and returns. **`send_callback` invocations come from that async task, not from `spawn_blocking`.** A long-running synchronous poll inside the plugin would block one Tokio worker. async polls don't.
- No timeout on the registration call - a plugin whose `iggy_source_handle` never returns stalls one blocking worker for the process lifetime.

## Sink consumption loop

Per `[[streams]]` entry per sink:

1. Build / ensure consumer group `iggy-connect-sink-{key}` (`connect` matches the docker image name `apache/iggy-connect` - see `runtime/src/sink.rs::default_consumer_group`). Overridable via the stream config's `consumer_group`.
2. Spawn one task per topic (`spawn_consume_tasks`).
3. Poll Iggy → batch messages (default `batch_length = 1000`, `poll_interval = 5ms`). A `consumer.next()` `Err` (transport/decode at the Iggy client boundary) bumps `errors` and `continue`s - offset already auto-committed via `AutoCommit::When(AutoCommitWhen::PollingMessages)`, message effectively dropped.
4. Decode via `Schema::decoder()`. Decode failure logs + bumps `errors`. `messages_filtered` is reserved for the transform filter contract (`Ok(None)`).
5. Apply transforms (chain). A transform `Err` is logged, bumps `errors`, and drops only that message (drop-and-continue, mirroring the source) - one bad payload never kills the batch. `Ok(None)` filters the message - bumps `messages_filtered{connector_type="sink"}`.
6. Postcard-encode batch as `RawMessages`. Headers as a separate postcard blob. Per-message failures (missing field, payload conversion, header serialization) bump `errors` + skip. Batch-level failures (postcard serialization of metadata / `RawMessages`) propagate `Err` -> `spawn_consume_tasks` wrapper bumps `errors` + `set_error`. The total histogram and benchmark emit still fire for the failed batch before the `Err` propagates.
7. Call `iggy_sink_consume(id, topic_meta, messages_meta, messages)`.
8. Non-zero return → log + `errors` increment.

Per-message counters (decode errors, transform errors, filters, field/serialize drops) are accumulated and flushed once per batch via `inc_errors_by_with_labels` / `inc_messages_filtered_with_labels`, not one `Family` lookup per message.

Stages (both labels snake_case): `decode` (schema decode loop), `prepare` (transform + serialize = total - decode - ffi), `ffi` (plugin consume call), `total`. Same `decode` / `prepare` meaning as the source side.

Gotchas:

- Batch flushes on `current_offset != message_offset` (gap) OR batch full - higher latency for low-volume topics.
- Auto-commit happens on poll - idempotency at the sink protects against re-delivery on crash before plugin write.

## Logging format

`LoggingConfig` in `configs/runtime.rs` exposes `format: LogFormat` (`Text` default, `Json`), env-addressable via `IGGY_CONNECTORS_LOGGING_FORMAT=json`. `log::init_logging` matches on `(telemetry.enabled, format)` and installs the right `fmt::layer()`/`fmt::layer().json()` + OpenTelemetry layer. OTel pipeline untouched by `format` - only the stdout layer switches.

`fmt::layer()` and `fmt::layer().json()` are different concrete types, so the stdout layer is built once as a `Box<dyn Layer<_>>` (via `.boxed()`) by matching on `format`, then a single 2-arm branch on `telemetry.enabled` attaches the OTel layers. When extending, keep the boxed-layer + 2-arm shape rather than re-expanding into a 4-way match.

## The `verbose` flag

`SinkConfig` and `SourceConfig` in `configs/connectors.rs` carry `verbose: bool` (default false). Runtime threads it to `spawn_consume_tasks` / source equivalent to gate per-batch log promotion:

```rust
if verbose {
    info!("Processing {messages_count} messages for sink connector with ID: {plugin_id}");
} else {
    debug!("Processing {messages_count} messages for sink connector with ID: {plugin_id}");
}
```

New per-batch logging follows the same pattern. Default to `debug!`, upgrade to `info!` when `verbose`. No third level.

## Config provider rules

### Local (`configs/connectors/local_provider.rs`)

- Discovery walk reads `*.toml` from `config_dir`. Skips hidden files (`.`) and `Cargo.toml`.
- Filename: `{key}_{type}[_v{N}].toml` (e.g., `postgres_sink_v2.toml`).
- One TOML deserialized into `SinkConfig` or `SourceConfig`. Grouped by `key`. Highest `version` wins unless `.active_versions.toml` overrides.
- `.active_versions.toml` is NOT read via the discovery walk (hidden). Read via direct path through `LocalConnectorsConfigProvider::active_versions_file_path`.

### HTTP (`configs/connectors/http_provider.rs`)

- `reqwest_middleware` + `RetryTransientMiddleware`. Retries 5xx + connection errors. Never 4xx (`PermanentHttpError` convention).
- Default URL templates documented in `runtime/README.md`. Configurable via TOML.

### Env-var overrides via `ConfigEnv` derive

`SinkConfig`, `SourceConfig`, and inner structs derive `ConfigEnv` (`configs_derive::ConfigEnv`). Generates env-var addressability as `IGGY_CONNECTORS_<TYPE>_<KEY>_<FIELD>` for primitive fields. Used heavily by integration tests to inject testcontainer ports - see `core/integration/tests/connectors/fixtures/postgres/container.rs` for env-var constants. Mark new compound fields `#[config_env(skip)]`, leaf primitives `#[config_env(leaf)]`.

### Versioning

- Configs carry `version: u64`. Local provider auto-increments if absent. HTTP provider trusts the server.
- Restart on version change uses `restart_guard`.

## Error categorization

`RuntimeError` (`error.rs`) carries `as_code()` for HTTP API responses.

| Class              | Fatal?                      | Example                          |
| ------------------ | --------------------------- | -------------------------------- |
| Config load        | yes (process exit)          | Bad TOML, missing required field |
| Iggy client init   | yes                         | Auth failure at startup          |
| State dir create   | yes                         | Permission denied                |
| Plugin .so resolve | per-plugin (`FailedPlugin`) | Missing file                     |
| Plugin open FFI    | per-plugin (status = Error) | Plugin returned non-zero         |
| Message decode     | per-message (skip)          | Bad protobuf bytes               |
| Iggy send          | per-batch (metric)          | Network blip                     |
| State save         | per-batch (log)             | Disk full                        |

Fatal errors propagate to `main` and exit. Per-connector / per-message errors are isolated.

## Metrics (`metrics.rs`)

All families labeled by `connector_key` + `connector_type` (histogram adds `stage`):

- **Counters**: `iggy_connector_messages_{produced,sent,consumed,processed,filtered,errors}_total`.
  - `messages_filtered_total` - intentional drops via transform `Ok(None)`.
  - `errors_total` - unexpected drops (decode/encode/build failure, missing field, ...) + batch-level failures.
- **Histograms**: `iggy_connector_stage_duration_seconds{stage}` (snake_case stage labels - `prepare`, `ffi`, `decode`, `iggy_send`, `state_save`, `total`). Buckets `STAGE_BUCKETS_SECONDS`. Always populated regardless of any flag. Scraped at `/metrics` when `[http.metrics] enabled = true`.
- **Gauges**: `iggy_connectors_{sources,sinks}_{total,running}`.

`/stats` JSON surface mirrors counters per-connector via `ConnectorStats` (`sdk/api.rs`): `messages_filtered`, `errors`, kind-specific counters.

When adding a metric:

- Add family to `Metrics` struct + `init`, register with name + help text.
- New label sets define `EncodeLabelSet` struct + label enum (hand-impl `EncodeLabelValue` for snake_case values - the derive emits PascalCase).
- Histograms: pass `fn() -> Histogram` to `Family::new_with_constructor`.
- Add unit tests under `mod tests` with `given_*_when_*_should_*` BDD names.

## Drop accounting (wired sites)

Per-message drops in the batch loops are counted into a local `u64` and flushed once after the loop via `inc_errors_by_with_labels` / `inc_messages_filtered_with_labels` - one `Family` lookup per batch, not per message. One-shot drops outside a loop (e.g. `consumer.next()` Err) still call `inc_errors_with_labels` directly. Hot path uses pre-built `SinkLabels`/`SourceLabels`. `&str`-based wrappers are `#[cfg(test)]`-only.

- `sink.rs::consume_messages` - `consumer.next()` Err
- `sink.rs::process_messages` - decode, transform Err (drop-and-continue), missing required fields, payload conversion, header serialization (accumulated, flushed once per batch)
- `sink.rs::spawn_consume_tasks` task wrapper - bumps once on `consume_messages` Err
- `source.rs::source_forwarding_loop` - payload decode, prepare (transform/encode) failure, Iggy send Err, state save Err
- `source.rs::process_messages` - transform Err (logs + bumps `errors` + continue. does NOT propagate, so one bad payload doesn't flip the connector to permanent ERROR), transform encode failure, `build_iggy_message` failure
- `source.rs::handle_produced_messages` - postcard deserialize failure, `sender.send` channel-closed

Filter case bumps `messages_filtered` via `inc_messages_filtered_with_labels`. Adding a new drop path: mirror this pattern.

## Hard rules

1. **Never block the executor.** All I/O async. `spawn_blocking` only for FFI registration (already in place).
2. **Plugin ID counter is monotonic.** No reset, no reuse.
3. **FFI return codes:** `0` success, non-zero failure.
4. **Don't add static mutable state** beyond `LOG_CALLBACK`, `PLUGIN_ID`, `SOURCE_SENDERS`.
5. **Pair `cleanup_sender(id)` with shutdown** for sources (avoid flume leak). Order: close FFI -> cleanup sender -> drain/abort tasks.
6. **Restart uses `restart_guard.try_lock()`** - no thundering-herd regression.
7. **No timeouts on plugin FFI calls without a kill-task strategy.** A timeout that returns from the runtime but leaves the plugin running has the worst of both worlds.

## Common pitfalls

1. Mutating `INSTANCES` (in SDK macro) from runtime side - impossible by design. only the macro touches it.
2. Calling `iggy_*_open` with a duplicate ID - returns `-1`. Means runtime forgot to `iggy_*_close` first.
3. Holding a `DashMap` shard guard across `.await` - lockups. Pattern: `.get().clone()`, drop guard, await.
4. Adding fields to `#[repr(C)]` types without an SDK version bump - existing plugins misalign on `postcard::from_bytes`.
5. Changing the default consumer group name without coordinating with operators - they have offsets stored there.

## Before declaring done

```bash
cargo fmt --all
cargo sort --no-format --workspace
cargo clippy -p iggy-connectors --all-targets -- -D warnings
cargo test -p iggy-connectors

# Sanity-build in-tree plugins:
cargo build -p iggy_connector_stdout_sink -p iggy_connector_random_source

# Runtime-focused integration tests:
cargo test -p integration -- connectors::runtime::

# Smoke-test with example config:
IGGY_CONNECTORS_CONFIG_PATH=core/connectors/runtime/example_config/config.toml \
  cargo run --bin iggy-connectors
```

Update `runtime/README.md` if endpoints, env vars, or config schema change.

---

Discussion / help: see [AGENTS.md](../../../AGENTS.md#discussion-and-support).
