---
name: connector-source
description: Author a new Apache Iggy connector source plugin under `core/connectors/sources/<name>_source/`. Sources poll an external system (DB, API, queue) and produce messages into Apache Iggy streams. Load when creating, modifying, or reviewing a source crate.
---

# Writing an Apache Iggy Connector Source

A **source** is a Rust `cdylib` that implements `iggy_connector_sdk::Source` and exposes FFI symbols via the `source_connector!` macro. The runtime calls `poll()` in a loop, applies the transform chain, encodes via the configured `Schema`, sends the batch to Apache Iggy, and persists the returned `ConnectorState` to disk after every successful send.

## Reference exemplars

| Use case | Plugin |
| ---------- | -------- |
| Read this first - minimal shape with the four canonical state tests | `sources/random_source/src/lib.rs` |
| Real-world DB integration with multiple consumption modes, cursor + restart tests | `sources/postgres_source/src/lib.rs` (paired with `core/integration/tests/connectors/postgres/postgres_source.rs` and `restart.rs`) |
| External API with scroll cursor | `sources/elasticsearch_source/src/lib.rs` |
| Time-series scan | `sources/influxdb_source/src/lib.rs` |

## Cargo.toml skeleton

Identical to a sink's `Cargo.toml` (see `connector-sink` skill) - only the crate name suffix differs (`iggy_connector_<name>_source`) and the upstream client dep changes. Same `[lib] crate-type = ["cdylib", "lib"]`, same `cargo-machete` ignored list.

## src/lib.rs skeleton

Code reads top to bottom like a book. Public types first, `impl Source` afterward, helpers below.

```rust
/* Apache 2.0 header */

use async_trait::async_trait;
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source, source_connector,
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

source_connector!(MySource);

const CONNECTOR_NAME: &str = "My source";

#[derive(Debug, Serialize, Deserialize)]
pub struct MySourceConfig {
    pub endpoint: String,
    pub poll_interval: Option<String>,      // humantime, "10s" default
    pub batch_size: Option<u32>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub verbose_logging: Option<bool>,      // plugin-level verbose - mirror runtime SourceConfig.verbose
}

#[derive(Debug, Serialize, Deserialize)]
struct State {
    cursor: Option<String>,                 // resume token (WAL LSN, scroll id, timestamp, ...)
    last_offset: u64,
    messages_produced: u64,
}

#[derive(Debug)]
pub struct MySource {
    id: u32,
    config: MySourceConfig,
    poll_interval: Duration,
    verbose: bool,
    client: Option<Client>,
    state: Mutex<State>,
}

impl MySource {
    pub fn new(id: u32, config: MySourceConfig, state: Option<ConnectorState>) -> Self {
        let raw_interval = config.poll_interval.clone().unwrap_or_else(|| "10s".into());
        let poll_interval = humantime::Duration::from_str(&raw_interval)
            .map(|d| *d)
            .unwrap_or_else(|_| {
                warn!("Invalid poll_interval for {CONNECTOR_NAME} ID: {id}, defaulting to 10s");
                Duration::from_secs(10)
            });

        let verbose = config.verbose_logging.unwrap_or(false);

        let restored = state
            .and_then(|s| s.deserialize::<State>(CONNECTOR_NAME, id))
            .inspect(|s| info!(
                "Restored state for {CONNECTOR_NAME} ID: {id}, last_offset: {}, cursor: {:?}",
                s.last_offset, s.cursor
            ));

        Self {
            id,
            config,
            poll_interval,
            verbose,
            client: None,
            state: Mutex::new(restored.unwrap_or(State {
                cursor: None,
                last_offset: 0,
                messages_produced: 0,
            })),
        }
    }
}

#[async_trait]
impl Source for MySource {
    async fn open(&mut self) -> Result<(), Error> {
        let client = build_client(&self.config)
            .await
            .map_err(|e| Error::InitError(format!("client build failed: {e}")))?;
        self.client = Some(client);
        info!(
            "Opened {CONNECTOR_NAME} connector ID: {}, endpoint: {}",
            self.id, self.config.endpoint
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        sleep(self.poll_interval).await;            // sleep first - backpressure

        let cursor = { self.state.lock().await.cursor.clone() };   // brief read

        let fetched = self.fetch_since(cursor.as_deref()).await?;  // no lock held

        let mut messages = Vec::with_capacity(fetched.len());
        let mut next_cursor = None;
        for row in fetched {
            let payload = simd_json::to_vec(&row).map_err(|e|
                Error::Serialization(format!("row serialize: {e}"))
            )?;
            messages.push(ProducedMessage {
                id: Some(row.id as u128),                  // natural ID for idempotency
                checksum: None,
                timestamp: None,
                origin_timestamp: Some(row.created_at_ns),
                headers: None,
                payload,
            });
            next_cursor = Some(row.cursor_value);
        }

        if self.verbose {
            info!(
                "{CONNECTOR_NAME} ID: {} produced {} messages, next cursor: {:?}",
                self.id, messages.len(), next_cursor
            );
        }

        let persisted = {                                  // brief write
            let mut state = self.state.lock().await;
            state.messages_produced += messages.len() as u64;
            if let Some(c) = next_cursor {
                state.cursor = Some(c);
            }
            ConnectorState::serialize(&*state, CONNECTOR_NAME, self.id)
        };

        Ok(ProducedMessages {
            schema: Schema::Json,
            messages,
            state: persisted,
        })
    }

    async fn close(&mut self) -> Result<(), Error> {
        // Adapt to your client: sqlx pool has `.close().await` (see postgres_source::close);
        // elasticsearch/reqwest clients just drop.
        if let Some(client) = self.client.take() {
            let _ = client; // drop; or `client.close().await;` for sqlx pools
        }
        let state = self.state.lock().await;
        info!(
            "Closed {CONNECTOR_NAME} connector ID: {}, total produced: {}",
            self.id, state.messages_produced
        );
        Ok(())
    }
}

// Private helpers below.
async fn build_client(config: &MySourceConfig) -> Result<Client, ClientError> { /* ... */ }
```

## Hard rules

### `poll()` signature is `&self`

The macro shares the source as `Arc<T>` across the FFI callback and forwarding loop. The signature is `async fn poll(&self) -> ...` - any mutable state goes behind `tokio::sync::Mutex`. **This is the single most common new-contributor mistake.**

### Lock discipline

The fetch path holds the upstream client for the duration of I/O. **Never hold the state `Mutex` across upstream I/O.** Canonical pattern (matches `sources/postgres_source/src/lib.rs::poll_tables`):

```rust
let cursor = { self.state.lock().await.cursor.clone() };   // brief read
let rows = client.query(&sql, &[&cursor]).await?;           // no lock held
let persisted = {                                           // brief write
    let mut state = self.state.lock().await;
    state.cursor = Some(new_cursor);
    ConnectorState::serialize(&*state, CONNECTOR_NAME, self.id)
};
```

### State persistence

- `ConnectorState` is `Vec<u8>` serialized via MessagePack (`rmp_serde`). Use `ConnectorState::serialize(&state, NAME, id)` and `ConnectorState::deserialize::<State>(NAME, id)` - both return `Option<T>` and **log on failure (non-fatal)**.
- The runtime saves to `{state_path}/source_{key}.state` only after a successful Iggy send. On a crash between `poll()` returning and the runtime persisting the save, the **same cursor will be returned on the next poll** - downstream must tolerate at-least-once.
- **Always return state in every `ProducedMessages`**, including empty polls. Empty-result polls still need to advance watermarks (timestamp source) or affirm "nothing new."
- Keep `State` small - rewritten after every batch. No unbounded vecs.

### Sleep first

`poll()` must `sleep(self.poll_interval).await` before any work. Without it, an empty source spins a CPU.

### Schema selection

Match `ProducedMessages.schema` to the bytes in `messages[i].payload`:

- JSON-serialized rows → `Schema::Json`
- Already-protobuf bytes → `Schema::Proto`
- Already-avro bytes → `Schema::Avro`
- Opaque → `Schema::Raw`

### Secrets in plugin config

Any credential-bearing field (connection strings, API keys, bearer tokens, AWS keys) must be `SecretString` from the `secrecy` crate, with the workspace serde wrapper applied so `Debug` and serialization both redact. The runtime exposes plugin configs over the `/stats` HTTP surface via serialization, so a plain `String` would leak the secret to anyone who can hit the endpoint. Plain `String` for a credential is a review-blocker. Pattern (matches `sources/postgres_source/src/lib.rs::PostgresSourceConfig`):

```rust
use secrecy::{ExposeSecret, SecretString};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MySourceConfig {
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_secret")]
    pub connection_string: SecretString,
    // ...other fields
}

// At the call site (inside open()/poll()):
let pool = PgPoolOptions::new()
    .connect(self.config.connection_string.expose_secret())
    .await?;
```

In-tree uses: `sources/{postgres,elasticsearch,influxdb}_source`, `sinks/{postgres,mongodb,elasticsearch,influxdb,delta}_sink`.

### `verbose` flag (two layers)

- **Runtime-level**: `verbose: bool` on `SourceConfig` - the runtime upgrades its own per-batch `debug!` to `info!`. The source does not read it.
- **Plugin-level**: Mirror with `verbose_logging: Option<bool>` in your plugin_config. Use it to upgrade source-internal `debug!` (per-poll counts, cursor advancement) to `info!`. Default false.

### The `benchmark` flag

`SourceConfig` carries `benchmark: bool` (`#[serde(default)]`, defaults to
`false`), env-addressable via `IGGY_CONNECTORS_SOURCE_<KEY>_BENCHMARK=true`.
When `true`, the runtime emits one `info!` line per batch on tracing target
`iggy_connectors::benchmark` with stage timings in microseconds (`decode_us`,
`prepare_us`, `iggy_send_us`, `state_save_us`, `total_us`). The source itself
does not read the flag - instrumentation lives in `runtime/src/source.rs`.
The corresponding `iggy_connector_stage_duration_seconds` histogram (stages
`Decode`, `Prepare`, `IggySend`, `StateSave`, `Total`) is always populated
regardless of the flag, so Prometheus dashboards work without enabling text
events. Include `benchmark = false` in the sample TOML so operators discover
the option.

### IDs and timestamps

- `ProducedMessage.id: Option<u128>` - set when there is a natural ID (DB primary key, document id). Apache Iggy can dedupe on this.
- `origin_timestamp: Option<u64>` - source-system event time in nanoseconds. Lets downstream sinks reason about lag.
- `timestamp` and `checksum` are Iggy-side - leave `None`.

### Concurrency

- The runtime spawns ONE `poll()` task per source. No concurrent `poll()` execution.
- Don't spawn your own long-running Tokio tasks - the runtime owns the lifecycle, orphans survive `close()`.

### Errors

| Scenario | Variant |
| ---------- | --------- |
| Bad config in `new()`/`open()` | `Error::InitError` |
| Cannot reach external system at startup | `Error::InitError` or `Error::Connection` |
| Transient fetch failure (retry-worthy) | `Error::Connection` or `Error::HttpRequestFailed` |
| Permanent fetch failure (auth, schema gone) | `Error::PermanentHttpError` |
| Row failed to serialize | `Error::Serialization(...)` |
| State serialization failed | log + skip (non-fatal) |

Returning `Err` from `poll()` is **only logged** by the SDK's FFI bridge
(`sdk/src/source.rs::handle_messages`) - the loop continues and the next
`poll()` is still called. The connector status does NOT flip to `Error` from a
poll failure. Status `Error` is set by the runtime only on transform/encode
failure, Iggy send failure, or state save failure (see
`runtime/src/source.rs::source_forwarding_loop` calls to
`context.sources.set_error`). If you need a poll failure to surface as
connector unhealth, raise it through the metric counter (which `runtime`
reads) or escalate to `Error::InitError` from `open()`.

### Logging

```rust
info!("Opened <connector> connector ID: {}, endpoint: {}", self.id, ...);
info!("Restored state for <connector> ID: {id}, cursor: {:?}", ...);
debug!("Polled {} rows for <connector> ID: {}", rows.len(), self.id);
warn!("Transient fetch failure for <connector> ID: {}, will retry: {error}", self.id);
error!("Failed to <op> for <connector> ID: {}, error: {error}", self.id);
info!("Closed <connector> connector ID: {}, total produced: {}", self.id, ...);
```

Iggy consumer-loop labels use literal API names: `offset=`, `current_offset=`. Don't invent labels.

## Common pitfalls

1. **`async fn poll(&mut self)`** - won't compile. Use `&self` + `Mutex<State>`.
2. **Holding `state.lock()` across the fetch I/O** - blocks `close()`, causes shutdown timeouts.
3. **Forgetting to sleep** - 100% CPU on idle source.
4. **Returning state only on success** - state should also advance on empty polls.
5. **Unbounded data in `State`** - rewritten every batch; keep O(constant).
6. **`std::sync::Mutex`** - blocks the executor. Use `tokio::sync::Mutex`.
7. **Not setting `ProducedMessage.id`** when a stable ID exists - loses idempotency.
8. **Spawning side tasks** - the runtime owns the scheduler.

## Tests

### Unit tests

Mandatory four canonical state tests (see `connector-testing` skill for full pattern). Copy from `sources/random_source/src/lib.rs::tests`. Plus tests for config defaults, payload building, schema selection.

### Integration tests (real infra)

For any source that reads from a real external system, add tests under `core/integration/tests/connectors/<backend>/`. The pattern uses `#[iggy_harness]` plus a `TestFixture` backed by `testcontainers-modules`. Reference: `core/integration/tests/connectors/postgres/postgres_source.rs` (multiple consumption-mode tests) and `restart.rs` (state survives restart).

## Before declaring done

```text
cargo fmt --all
cargo sort --no-format --workspace
cargo clippy -p iggy_connector_<name>_source --all-targets -- -D warnings
cargo test -p iggy_connector_<name>_source

# If you added integration tests (filter by test name; the integration crate has no
# per-area --test target, all tests share one binary via tests/mod.rs):
cargo test -p integration -- connectors::<backend>::<test_name>
```

Update `core/connectors/sources/README.md` and add a sample TOML in `core/connectors/runtime/example_config/connectors/`.
