---
name: connector-sink
description: Author a new Apache Iggy connector sink plugin under `core/connectors/sinks/<name>_sink/`. Sinks consume messages from Apache Iggy streams and push them to an external system (DB, search engine, HTTP endpoint, object store). Load when creating, modifying, or reviewing a sink crate.
---

# Writing an Apache Iggy Connector Sink

A **sink** is a Rust `cdylib` that implements `iggy_connector_sdk::Sink` and exposes FFI symbols via the `sink_connector!` macro. The runtime loads it as `.so`/`.dylib`/`.dll`, polls Apache Iggy streams on its behalf, runs the transform chain, decodes via the configured `Schema`, and hands the sink batches of `ConsumedMessage`. The sink is responsible for getting them to the external system reliably and efficiently.

## Reference exemplars

| Use case | Plugin |
| ---------- | -------- |
| Read this first - minimal shape | `sinks/stdout_sink/src/lib.rs` (~100 LOC) |
| Real-world DB integration with idempotency + transient detection | `sinks/postgres_sink/src/lib.rs` (paired with full integration tests under `core/integration/tests/connectors/postgres/`) |
| Rich config validation + retry middleware + multiple batch modes | `sinks/http_sink/src/lib.rs` |
| Backend-specific idioms | `sinks/mongodb_sink/`, `elasticsearch_sink/`, `iceberg_sink/` |

Always read the relevant exemplar end-to-end before writing - the patterns are intentionally repetitive across plugins.

## Cargo.toml skeleton

```toml
# Apache 2.0 header (copy verbatim from any existing sink Cargo.toml)
[package]
name = "iggy_connector_<name>_sink"
version = "0.4.1-edge.1"          # match the version most other sinks use; check by grepping `version =` in sibling Cargo.toml files
edition = "2024"
license = "Apache-2.0"
publish = false
# ...keywords, description, repository, homepage all identical to existing sinks

[package.metadata.cargo-machete]
ignored = ["dashmap", "once_cell"]   # the sink_connector! macro uses them - machete can't see through macros

[lib]
crate-type = ["cdylib", "lib"]       # cdylib = runtime-loadable; lib = unit tests + reuse

[dependencies]
async-trait        = { workspace = true }
dashmap            = { workspace = true }
iggy_connector_sdk = { workspace = true }
once_cell          = { workspace = true }
serde              = { workspace = true }
tokio              = { workspace = true }
tracing            = { workspace = true }
# + your client crate (reqwest, sqlx, mongodb, ...)
```

Run `cargo sort --no-format --workspace` after edits.

## src/lib.rs skeleton

Code reads top to bottom like a book. Public types first, then `impl`, then private helpers. Don't make the reader jump.

```rust
/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements... */   // full Apache header

use async_trait::async_trait;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

sink_connector!(MySink);   // generates FFI symbols + version export

const CONNECTOR_NAME: &str = "My sink";

#[derive(Debug, Serialize, Deserialize)]
pub struct MySinkConfig {
    pub endpoint: String,
    pub batch_size: Option<u32>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,        // humantime, e.g. "500ms"
    pub verbose_logging: Option<bool>,      // plugin-level verbose - mirror the runtime's `verbose` flag
    // Every runtime-tunable field is Option<T>; defaults applied in new()
}

#[derive(Debug)]
pub struct MySink {
    id: u32,
    config: MySinkConfig,
    batch_size: usize,
    max_retries: u32,
    retry_delay: Duration,
    verbose: bool,
    client: Option<Client>,
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    messages_processed: u64,
    errors: u64,
}

impl MySink {
    pub fn new(id: u32, config: MySinkConfig) -> Self {
        let batch_size = config.batch_size.unwrap_or(100) as usize;
        let max_retries = config.max_retries.unwrap_or(3);
        let retry_delay = config
            .retry_delay
            .as_deref()
            .and_then(|raw| humantime::Duration::from_str(raw).ok().map(|d| *d))
            .unwrap_or_else(|| {
                warn!("Invalid retry_delay for {CONNECTOR_NAME} ID: {id}, defaulting to 500ms");
                Duration::from_millis(500)
            });
        let verbose = config.verbose_logging.unwrap_or(false);
        Self {
            id,
            config,
            batch_size,
            max_retries,
            retry_delay,
            verbose,
            client: None,
            state: Mutex::new(State { messages_processed: 0, errors: 0 }),
        }
    }
}

#[async_trait]
impl Sink for MySink {
    async fn open(&mut self) -> Result<(), Error> {
        let client = build_client(&self.config)
            .await
            .map_err(|e| Error::InitError(format!("client build failed: {e}")))?;
        client.ping()
            .await
            .map_err(|e| Error::InitError(format!("connectivity check failed: {e}")))?;
        self.client = Some(client);
        info!(
            "Opened {CONNECTOR_NAME} connector ID: {}, endpoint: {}",
            self.id, self.config.endpoint
        );
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let Some(client) = self.client.as_ref() else {
            return Err(Error::InitError("client not initialized".into()));
        };

        if self.verbose {
            info!(
                "{CONNECTOR_NAME} ID: {} consuming {} messages from stream: {}, topic: {}, offset: {}, current_offset: {}",
                self.id, messages.len(), topic_metadata.stream, topic_metadata.topic,
                messages_metadata.partition_id, messages_metadata.current_offset
            );
        } else {
            debug!(
                "{CONNECTOR_NAME} ID: {} consuming {} messages",
                self.id, messages.len()
            );
        }

        let mut last_err: Option<Error> = None;
        for batch in messages.chunks(self.batch_size) {
            match self.send_batch(client, batch).await {
                Ok(()) => { /* counter */ }
                Err(Error::PermanentHttpError(message)) => {
                    error!(
                        "{CONNECTOR_NAME} ID: {} dropping batch (permanent): {message}",
                        self.id
                    );
                }
                Err(error) => {
                    last_err = Some(error);
                }
            }
        }
        match last_err {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        // Adapt to your client: sqlx pool needs `.close().await`; mongodb/elasticsearch/reqwest just drop.
        if let Some(client) = self.client.take() {
            let _ = client; // drop; replace with `client.close().await;` if your client requires it
        }
        let state = self.state.lock().await;
        info!(
            "Closed {CONNECTOR_NAME} connector ID: {}, processed: {}, errors: {}",
            self.id, state.messages_processed, state.errors
        );
        Ok(())
    }
}

// Private helpers below this line. Each is referenced from the impl above.
async fn build_client(config: &MySinkConfig) -> Result<Client, ClientError> { /* ... */ }
```

## Hard rules

### Lifecycle

- `Sink::consume` takes **`&self`** - never `&mut self`. Mutable state goes behind `tokio::sync::Mutex` / `AtomicU64`. **Prefer atomics for counters** - canonical example: `sinks/mongodb_sink/src/lib.rs` uses `messages_processed: AtomicU64` and `insertion_errors: AtomicU64` and avoids a state lock entirely on the hot path. Use `Mutex<State>` only when multiple fields must update atomically together.
- `Sink::open` builds the client AND tests connectivity. Fail fast with `Error::InitError`.
- `Sink::close` flushes + drops resources. Take ownership of `client: Option<Client>` via `.take()`. Whether you then call an explicit close depends on the client crate: sqlx pools have `.close().await` (see `postgres_sink::close`); MongoDB, Elasticsearch, and reqwest clients have no explicit close - dropping is enough (see `mongodb_sink::close` which just calls `self.client.take()`, and `elasticsearch_sink` which assigns `self.client = None;`). Log final stats either way.

### The `verbose` flag (two layers)

- **Runtime-level**: `verbose: bool` lives on `SinkConfig` (TOML top-level field, see `runtime/src/configs/connectors.rs::SinkConfig`). The runtime promotes its own per-batch `debug!` logs to `info!` when set. The sink does not need to read this field directly - it just receives bytes.
- **Plugin-level**: Optionally expose a similar toggle inside your `plugin_config` (convention: `verbose_logging: Option<bool>` - see `sinks/postgres_sink/src/lib.rs::PostgresSinkConfig::verbose_logging`). Use it to upgrade your own per-batch `debug!` logs to `info!` so operators can opt into deep tracing without recompiling.
- Default both to `false`. Verbose logs include batch size, offset ranges, latency, header counts.

### The `benchmark` flag

`SinkConfig` carries `benchmark: bool` (`#[serde(default)]`, defaults to
`false`), env-addressable via `IGGY_CONNECTORS_SINK_<KEY>_BENCHMARK=true`.
When `true`, the runtime emits one `info!` line per batch on tracing target
`iggy_connectors::benchmark` with stage timings in microseconds (`prepare_us`,
`ffi_us`, `total_us`). The sink itself does not read the flag - instrumentation
lives in `runtime/src/sink.rs`. The corresponding
`iggy_connector_stage_duration_seconds` histogram (stages `Prepare`, `Ffi`,
`Total`) is always populated regardless of the flag, so Prometheus dashboards
work without enabling text events. Include `benchmark = false` in the sample
TOML so operators discover the option.

### Config

- Every runtime-tunable field is `Option<T>` with a default applied in `new()`. Forward-compat: adding a field doesn't break existing TOML.
- Durations: `Option<String>`, parsed via `humantime::Duration` in `new()`. Two equivalent idioms in-tree, pick one:
  - **Inline parse** (used by `sources/random_source`, `sources/postgres_source`): `use humantime::Duration as HumanDuration;` then `HumanDuration::from_str(&raw).map(|d| *d).unwrap_or_else(|_| { warn!(...); fallback })`.
  - **Local `parse_duration` helper** (used by `sinks/http_sink::parse_duration` and `sdk/src/retry.rs::parse_duration`): `fn parse_duration(input: Option<&str>, default: &str) -> Duration`. Use when several durations need the same fallback logic.
  Either way, fall back to a default on parse failure with a `warn!` - never panic. We do **not** use `humantime_serde`.
- **Secrets**: any credential-bearing field (connection strings, API keys,
  bearer tokens, AWS keys) must be `SecretString` from the `secrecy` crate,
  with the workspace serde wrapper applied on the field. The wrapper redacts
  the value during `Debug`/`Display` and during serialization (the `/stats`
  HTTP surface uses serialization), preventing accidental leaks via
  `info!("{:?}", config)` or operator dumps. Expose with `.expose_secret()`
  only at the call site where the underlying client needs the raw value.
  Plain `String` for a credential is a review-blocker. Pattern (from
  `sinks/postgres_sink/src/lib.rs::PostgresSinkConfig`):

  ```rust
  use secrecy::{ExposeSecret, SecretString};

  #[derive(Debug, Clone, Serialize, Deserialize)]
  pub struct MySinkConfig {
      #[serde(serialize_with = "iggy_common::serde_secret::serialize_secret")]
      pub connection_string: SecretString,
      // ...other fields
  }

  // At the call site:
  let pool = PgPoolOptions::new()
      .connect(self.config.connection_string.expose_secret())
      .await?;
  ```

  In-tree uses: `sinks/{postgres,mongodb,elasticsearch,influxdb,delta}_sink`, `sources/{postgres,elasticsearch,influxdb}_source`.
- **Conflict resolution**: if two config fields can be in an invalid relative ordering, fix it in `new()` and `warn!` - don't return an error. Live pattern: `http_sink` uses `std::mem::swap(&mut retry_delay, &mut max_retry_delay)` when `retry_delay > max_retry_delay` to avoid an `ExponentialBackoff` panic downstream.
- Validation order: structural validation in `new()`, connectivity validation in `open()`, **never in `consume()`**.

### Payloads (efficiency-critical)

This runs on every message. Patterns from existing plugins:

- Dispatch on `messages_metadata.schema` and the `Payload` enum variant. Handle `Json`, `Raw`, `Text` at minimum. Treat unsupported as `Error::InvalidPayloadType` or fall back to base64 (see `sinks/elasticsearch_sink/src/lib.rs`).
- **For `Payload::Json`, call `try_to_bytes(&self)`** - serializes the `OwnedValue` tree in a single pass without cloning. Documented and implemented in `sdk/src/lib.rs::Payload::try_to_bytes`. Never `payload.clone().try_into_vec()`.
- **To take a payload out of `ConsumedMessage` without cloning**, use `std::mem::replace(&mut message.payload, Payload::Raw(vec![]))`. Live pattern in `sinks/http_sink/src/lib.rs` (used inside `send_individual`, `send_ndjson`, `send_json_array`). Copy this verbatim - it's the standard.
- **Pre-allocate per-batch buffers** with `Vec::with_capacity(messages.len())` (or `messages.len() * <factor>` for serialized bodies). Used in `postgres_sink`, `mongodb_sink`, `quickwit_sink`, `elasticsearch_sink`, `http_sink`. Skipping this wastes reallocations on every batch.

### Headers

- `message.headers: Option<BTreeMap<HeaderKey, HeaderValue>>`. Check `.is_empty()` before iterating - the runtime treats empty as `None`.
- For binary header values use `v.as_raw()` + base64; for text use `v.to_string_value()`. See `http_sink::EncodedHeader`.
- `BTreeMap` for deterministic ordering. Don't collect into `HashMap`.

### Errors

Map underlying-library errors into SDK variants:

| Scenario | SDK variant | Retry? |
| ---------- | ------------- | -------- |
| Init/connectivity failure | `Error::InitError` | n/a (fails open) |
| Bad config value | `Error::InvalidConfigValue(field)` | n/a |
| Single message fails schema validation | `Error::InvalidRecordValue(reason)` | no - skip + log |
| Transient backend (5xx, conn reset, lock timeout) | `Error::HttpRequestFailed` / `Error::Connection` / `Error::CannotStoreData` | yes |
| Permanent 4xx, schema mismatch, constraint violation | `Error::PermanentHttpError` / `Error::SchemaMismatch` | **no** - retrying bad data trips circuit breakers |
| I/O failure mid-write (parquet/iceberg) | `Error::WriteFailure` | caller decides |
| Catalog commit failure (Iceberg) | `Error::CatalogCommitError` | **no - not idempotent** |

### Retry

Two valid patterns in-tree - pick based on how much control you need:

- **Use SDK helpers** for startup probes and simple request paths: `iggy_connector_sdk::retry::check_connectivity_with_retry(...)` (used in plugin `open()`), or wrap your `reqwest::Client` with `iggy_connector_sdk::retry::HttpRetryMiddleware` for the SDK's default 429/5xx/network policy.
- **Use `reqwest-middleware` + `reqwest_retry::RetryTransientMiddleware` directly** when you need custom `RetryableStrategy` (e.g., to honor `success_status_codes`, or apply per-status decisions). Canonical example: `sinks/http_sink/src/lib.rs` defines its own `HttpSinkRetryStrategy` and wires it via `RetryTransientMiddleware::new_with_policy_and_strategy(retry_policy, retry_strategy)`.
- For non-HTTP clients (sqlx, mongodb), write `is_transient_error(&e)` mapping driver-specific codes - see `sinks/postgres_sink/src/lib.rs::is_transient_error` for Postgres SQLSTATEs (`40001`, `40P01`, `57P01-03`, `08000/03/06`).
- Backoff helpers: `iggy_connector_sdk::retry::exponential_backoff(base, attempt, max)` + `jitter()`.
- Cap retries. Default cap: 3 total attempts (not 3 retries on top of 1).

### Idempotency

The runtime retries on any `Err`, so the same batch can be redelivered. The strength of the guarantee varies by sink - what's actually implemented in tree:

- **Postgres** (`postgres_sink::build_batch_insert_query`): `id DECIMAL(39, 0) PRIMARY KEY` enforces uniqueness, but the INSERT is plain `INSERT INTO ... VALUES (...)` - no `ON CONFLICT` clause. A duplicate retry triggers a `23505` constraint violation, which `is_transient_error` does NOT mark as transient, so the error propagates. Adding `ON CONFLICT (id) DO NOTHING` is a known improvement.
- **MongoDB** (`mongodb_sink`): composite `_id` from `build_composite_document_id(stream:topic:partition:message_id)`, written via `insert_many(docs).ordered(false)`. Duplicate-key errors per-document don't abort the batch - effective idempotency under retry.
- **Elasticsearch** (`elasticsearch_sink::bulk_index_documents`): bulk action specifies only `_index`, **no `_id`** - Elasticsearch auto-generates document ids, so a retry creates new docs. NOT idempotent. Plugins extending this should add `"_id"` to the bulk action header.
- **HTTP** (`http_sink`): endpoint-dependent. Document the assumption in the plugin README; the SDK does not enforce.
- **Iceberg** (`iceberg_sink`): transactional commit. `Error::CatalogCommitError` (see SDK `lib.rs::Error`) documents that the transaction is consumed on `commit()` and rebuild-then-retry can duplicate data; callers must check the catalog first.

When writing a new sink, prefer dedup-on-write using the message `id` field. Match a working in-tree example (mongo upsert-style is the cleanest).

### Batching

- Default `batch_size: 100`. Tune per-backend.
- Chunk via `messages.chunks(self.batch_size)`. Do not buffer across `consume()` calls - the runtime already batches at the consumer-poll boundary.
- For HTTP backends, support multiple batch modes when applicable (individual / ndjson / json_array / raw) - see `http_sink::BatchMode`.

### Logging

```rust
info!("Opened <connector> connector ID: {}, endpoint: <redacted>", self.id);
debug!("Processing batch of {} messages, offset: {}", batch.len(), offset);
warn!("Retry {attempt}/{max} for <connector> ID: {}, reason: {error}", self.id);
error!("Failed to <op> for <connector> ID: {}, error: {error}", self.id);
info!("Closed <connector> connector ID: {}, processed: {}", self.id, count);
```

Use literal API field names as labels (`offset=`, `current_offset=`) - don't invent.

## Common pitfalls

1. **Missing `[lib] crate-type = ["cdylib", "lib"]`** - the runtime can't dlopen a regular rlib.
2. **Cloning the whole `Payload::Json`** to serialize - use `try_to_bytes(&self)`.
3. **Holding `Mutex` across `.await`** when the locked region does I/O.
4. **`&mut self` on `consume`** - won't compile; you actually need interior mutability.
5. **Returning the first error and dropping subsequent batches** - track `last_err` and process all, then return.
6. **Forgetting to map `PermanentHttpError`** - retries hammer the backend with bad data.
7. **Mutating config in `consume()`** - config is a snapshot from `new()`. Reload requires runtime restart.
8. **Logging the connection string** - never log secrets. Sinks redact URLs at construction (`http_sink::sanitize_url_for_log` populates an `HttpSink.log_url` field that is safe to print).

## Tests

Unit tests at EOF of `src/lib.rs` (see `connector-testing` skill for the BDD naming + `test_config()` helper conventions).

Real-infra integration tests live under `core/integration/tests/connectors/<backend>/` and use the `#[iggy_harness]` macro plus a backend-specific fixture that spins up a Docker container via `testcontainers-modules`. **Plugins that integrate with external services must have an integration test.** The reference is `core/integration/tests/connectors/postgres/postgres_sink.rs` paired with `core/integration/tests/connectors/fixtures/postgres/`.

## Before declaring done

```text
cargo fmt --all
cargo sort --no-format --workspace
cargo clippy -p iggy_connector_<name>_sink --all-targets -- -D warnings
cargo test -p iggy_connector_<name>_sink

# If you added integration tests (filter by test name; the integration crate has no
# per-area --test target, all tests share one binary via tests/mod.rs):
cargo test -p integration -- connectors::<backend>::<test_name>
```

Update `core/connectors/sinks/README.md` and add a sample TOML in `core/connectors/runtime/example_config/connectors/`.
