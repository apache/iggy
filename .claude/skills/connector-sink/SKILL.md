---
name: connector-sink
description: Author a new Apache Iggy connector sink plugin under core/connectors/sinks/. Sinks consume messages from Apache Iggy streams and push them to an external system (DB, search engine, HTTP endpoint, object store). Load when creating, modifying, or reviewing a sink crate. Use for sink plugin authoring. NOT for runtime internals (see connector-runtime).
---

# Writing an Apache Iggy Connector Sink

A **sink** is a Rust `cdylib` that implements `iggy_connector_sdk::Sink`
and exposes FFI symbols via the `sink_connector!` macro. The runtime
loads it as `.so`/`.dylib`/`.dll`, polls Apache Iggy on its behalf,
runs the transform chain, decodes via the configured `Schema`, and
hands the sink batches of `ConsumedMessage`. The sink is responsible
for getting them to the external system reliably and efficiently.

> **Universal connector rules** (SecretString, benchmark, verbose flag, drop accounting, filter contract, exemplar patterns) live in
> [connectors-overview](../connectors-overview/SKILL.md). This skill
> covers only what's sink-specific.

## Contents

- [STOP and ask the user before](#stop-and-ask-the-user-before)
- [Quick reference](#quick-reference)
- [Hard rules](#hard-rules)
- [Common pitfalls](#common-pitfalls)
- [Tests](#tests)
- [Before declaring done](#before-declaring-done)

## STOP and ask the user before

- Changing the SDK trait surface (`Sink::open` / `consume` / `close`) - that's an SDK change, not a plugin one.
- Touching default `batch_size`, retry caps, or backoff defaults baked into existing in-tree sinks.
- Promoting an `Err` variant to retryable when it previously was not - retrying bad data trips circuit breakers.
- Adding a new sink that introduces a backend the runtime doesn't yet integration-test for.

## Quick reference

- Skeleton: [TEMPLATE.md](TEMPLATE.md) (load on demand when authoring).
- Exemplars: `stdout_sink` (minimal), `postgres_sink` (DB + transient detection), `http_sink` (validation, batch modes, retry middleware), `mongodb_sink` (atomic counters), `elasticsearch_sink` / `iceberg_sink` (backend-specific idioms).

## Hard rules

### Lifecycle

- `Sink::consume` takes **`&self`** - never `&mut self`. Mutable state goes behind `tokio::sync::Mutex` / `AtomicU64`. Prefer atomics for hot counters - `mongodb_sink` uses `messages_processed: AtomicU64` + `insertion_errors: AtomicU64` and avoids a state lock on the hot path entirely.
- `Sink::open` builds the client AND tests connectivity. Fail fast with `Error::InitError`.
- `Sink::close` flushes + drops resources. Take ownership via `client: Option<Client>` + `.take()`. sqlx pools have `.close().await`. reqwest/mongodb/elasticsearch clients just drop. Log final stats either way.

### Config

- Every runtime-tunable field is `Option<T>` with a default applied in `new()`. Forward-compat: adding a field doesn't break existing TOML.
- Durations: `Option<String>`, parsed via `humantime::Duration` in `new()`. Two equivalent in-tree idioms - inline `HumanDuration::from_str(&raw).map(|d| *d).unwrap_or_else(..)` (`random_source`, `postgres_source`) or a local `parse_duration` helper (`http_sink`, `sdk/src/retry.rs`). Fall back to a default on parse failure with `warn!` - never panic. We do **not** use `humantime_serde`.
- **Conflict resolution**: if two config fields can be in invalid relative ordering, fix it in `new()` + `warn!` - don't error. `http_sink` swaps `retry_delay` and `max_retry_delay` when reversed to avoid an `ExponentialBackoff` panic downstream.
- Validation order: structural in `new()`, connectivity in `open()`, **never in `consume()`**.

### Payloads (efficiency-critical)

- Dispatch on `messages_metadata.schema` and the `Payload` variant. Handle `Json`, `Raw`, `Text` at minimum. Unsupported -> `Error::InvalidPayloadType` or fall back to base64 (`elasticsearch_sink`).
- For `Payload::Json`, call `try_to_bytes(&self)` - single-pass serialization without cloning the `OwnedValue` tree (`sdk/src/lib.rs::Payload::try_to_bytes`). Never `payload.clone().try_into_vec()`.
- To take a payload out of `ConsumedMessage` without cloning: `std::mem::replace(&mut message.payload, Payload::Raw(vec![]))`. Standard pattern across `http_sink::send_individual` / `send_ndjson` / `send_json_array`.
- Pre-allocate per-batch buffers with `Vec::with_capacity(messages.len())` (or `* factor` for serialized bodies). Used across `postgres_sink`, `mongodb_sink`, `quickwit_sink`, `elasticsearch_sink`, `http_sink`.

### Headers

- `message.headers: Option<BTreeMap<HeaderKey, HeaderValue>>`. Check `.is_empty()` before iterating - runtime treats empty as `None`.
- For binary values use `v.as_raw()` + base64. for text use `v.to_string_value()`. See `http_sink::EncodedHeader`.
- `BTreeMap` for deterministic ordering. Don't collect into `HashMap`.

### Errors

| Scenario                                   | SDK variant                                                                 | Retry?                                   |
| ------------------------------------------ | --------------------------------------------------------------------------- | ---------------------------------------- |
| Init / connectivity failure                | `Error::InitError`                                                          | n/a (fails open)                         |
| Bad config value                           | `Error::InvalidConfigValue(field)`                                          | n/a                                      |
| Single-message validation failure          | `Error::InvalidRecordValue(reason)`                                         | no - skip + log                          |
| Transient backend (5xx, conn reset, ...)   | `Error::HttpRequestFailed` / `Error::Connection` / `Error::CannotStoreData` | yes                                      |
| Permanent 4xx, schema mismatch, constraint | `Error::PermanentHttpError` / `Error::SchemaMismatch`                       | **no** - retrying trips circuit breakers |
| I/O failure mid-write (parquet/iceberg)    | `Error::WriteFailure`                                                       | caller decides                           |
| Catalog commit failure (Iceberg)           | `Error::CatalogCommitError`                                                 | **no - not idempotent**                  |

### Retry

- SDK helpers cover the simple case: `iggy_connector_sdk::retry::check_connectivity_with_retry(...)` for `open()`, `HttpRetryMiddleware` for default 429/5xx/network policy on reqwest clients.
- Custom strategies: `reqwest-middleware` + `reqwest_retry::RetryTransientMiddleware::new_with_policy_and_strategy`. `http_sink` defines its own `HttpSinkRetryStrategy` (honors `success_status_codes`, per-status decisions).
- Non-HTTP clients: write `is_transient_error(&e)` mapping driver-specific codes. `postgres_sink::is_transient_error` maps SQLSTATEs `40001`, `40P01`, `57P01-03`, `08000/03/06`.
- Backoff: `iggy_connector_sdk::retry::exponential_backoff(base, attempt, max)` + `jitter()`.
- Cap retries at 3 total attempts.

### Idempotency

Runtime retries on any `Err` - same batch can be redelivered. In-tree guarantees:

- **Postgres** (`postgres_sink::build_batch_insert_query`): PK on `id`, but plain `INSERT` (no `ON CONFLICT`). Duplicate retry triggers `23505`, `is_transient_error` does NOT mark it transient -> error propagates. Adding `ON CONFLICT (id) DO NOTHING` is a known improvement.
- **MongoDB**: composite `_id` from `stream:topic:partition:message_id`, via `insert_many(docs).ordered(false)`. Per-document duplicate-key errors don't abort the batch - effective idempotency under retry.
- **Elasticsearch** (`bulk_index_documents`): bulk action specifies only `_index`, no `_id` -> ES auto-generates ids -> NOT idempotent. Extensions should add `"_id"`.
- **HTTP**: endpoint-dependent. Document in the plugin README.
- **Iceberg**: transactional commit. `Error::CatalogCommitError` documents that the txn is consumed on `commit()`. Rebuild-then-retry can duplicate data - callers must check the catalog first.

When writing a new sink, prefer dedup-on-write using the message `id` field. Mongo's upsert-style is the cleanest reference.

### Batching

- Default `batch_size: 100`. Tune per-backend.
- Chunk via `messages.chunks(self.batch_size)`. Do not buffer across `consume()` - runtime already batches at the consumer-poll boundary.
- For HTTP backends support multiple batch modes when applicable (individual / ndjson / json_array / raw). See `http_sink::BatchMode`.

### Logging

```rust
info!("Opened <connector> connector ID: {}, endpoint: <redacted>", self.id);
debug!("Processing batch of {} messages, offset: {}", batch.len(), offset);
warn!("Retry {attempt}/{max} for <connector> ID: {}, reason: {error}", self.id);
error!("Failed to <op> for <connector> ID: {}, error: {error}", self.id);
info!("Closed <connector> connector ID: {}, processed: {}", self.id, count);
```

Use literal API field names as labels (`offset=`, `current_offset=`).

## Common pitfalls

1. Missing `[lib] crate-type = ["cdylib", "lib"]` - runtime can't dlopen a regular rlib.
2. Cloning the whole `Payload::Json` to serialize - use `try_to_bytes(&self)`.
3. Holding `Mutex` across `.await` when the locked region does I/O.
4. `&mut self` on `consume` - won't compile. you need interior mutability.
5. Returning the first error and dropping subsequent batches - track `last_err`, process all, return.
6. Forgetting to map `PermanentHttpError` - retries hammer the backend with bad data.
7. Mutating config in `consume()` - config is a snapshot from `new()`. Reload requires runtime restart.
8. Logging the connection string. Redact at construction: `http_sink::sanitize_url_for_log` populates `HttpSink.log_url`.

## Tests

Unit tests at EOF of `src/lib.rs`. See [connector-testing](../connector-testing/SKILL.md) for BDD naming + `test_config()` helper.

Real-infra integration tests live under `core/integration/tests/connectors/<backend>/` using `#[iggy_harness]` + a backend fixture that spins up Docker via `testcontainers-modules`. Plugins integrating with external services must have an integration test. Reference: `core/integration/tests/connectors/postgres/postgres_sink.rs` + `fixtures/postgres/`.

## Before declaring done

```bash
cargo fmt --all
cargo sort --no-format --workspace
cargo clippy -p iggy_connector_<name>_sink --all-targets -- -D warnings
cargo test -p iggy_connector_<name>_sink

# Integration tests (filter by name; integration crate has no per-area target):
cargo test -p integration -- connectors::<backend>::<test_name>
```

Update `core/connectors/sinks/README.md` and add a sample TOML under `core/connectors/runtime/example_config/connectors/`.

---

Discussion / help: see [AGENTS.md](../../../AGENTS.md#discussion-and-support).
