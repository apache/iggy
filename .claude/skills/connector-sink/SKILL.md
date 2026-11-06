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

- Skeleton: [TEMPLATE.md](TEMPLATE.md) (steps for copying `sinks/sink_template`, with an HTTP and a non-HTTP path).
- PR pre-flight: [connector-pr-review](../connector-pr-review/SKILL.md).
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

- Dispatch on the `Payload` variant. Handle `Json`, `Raw`, `Text` and `Proto` at minimum. `Proto` holds text that is a JSON document when `proto_convert` had no descriptor or could not encode, and arbitrary text otherwise: call `Payload::json_document()` first and fall back to the text handling when it returns `None`. Unsupported -> `Error::InvalidPayloadType` or fall back to base64 (`elasticsearch_sink`).
- For `Payload::Json`, call `try_to_bytes(&self)` - single-pass serialization without cloning the `OwnedValue` tree (`sdk/src/lib.rs::Payload::try_to_bytes`). Never `payload.clone().try_into_vec()`.
- To take a payload out of `ConsumedMessage` without cloning: `std::mem::replace(&mut message.payload, Payload::Raw(vec![]))`. Standard pattern across `http_sink::send_individual` / `send_ndjson` / `send_json_array`.
- Pre-allocate per-batch buffers with `Vec::with_capacity(messages.len())` (or `* factor` for serialized bodies). Used across `postgres_sink`, `mongodb_sink`, `quickwit_sink`, `elasticsearch_sink`, `http_sink`.

### Headers

- `message.headers: Option<BTreeMap<HeaderKey, HeaderValue>>`. Check `.is_empty()` before iterating - runtime treats empty as `None`.
- For binary values use `v.as_raw()` + base64. for text use `v.to_string_value()`. See `http_sink::EncodedHeader`.
- `BTreeMap` for deterministic ordering. Don't collect into `HashMap`.

### Errors

| Scenario                                   | SDK variant                                                                 | Retry?                                                                   |
| ------------------------------------------ | --------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| Init / connectivity failure                | `Error::InitError`                                                          | n/a (fails open)                                                         |
| Bad config value                           | `Error::InvalidConfigValue(field)`                                          | n/a                                                                      |
| Single-message validation failure          | `Error::InvalidRecordValue(reason)`                                         | no - skip + log; under at-least-once, fail the call (stops the sink)     |
| Transient backend (5xx, conn reset, ...)   | `Error::HttpRequestFailed` / `Error::Connection` / `Error::CannotStoreData` (or `WriteFailure`, `Storage`, `TransientState`, `CatalogCommitError`) | yes                                                                      |
| Permanent 4xx, schema mismatch, constraint | `Error::PermanentHttpError` / `Error::SchemaMismatch`                       | **no** - retrying trips circuit breakers                                 |
| I/O failure mid-write (parquet/iceberg)    | `Error::WriteFailure`                                                       | caller decides                                                           |
| Catalog commit failure (Iceberg)           | `Error::CatalogCommitError`                                                 | not in place (txn consumed); under at-least-once, the runtime redelivers |

`Error::is_permanent()` (`sdk/src/lib.rs`) is the class the runtime acts on for an at-least-once sink: a permanent error stops the delivery, a transient one hands the messages over again. `CatalogCommitError` is transient there, because the redelivered messages rebuild the transaction, and a commit that did land shows up as a duplicate, which at-least-once allows.

### Retry

- SDK helpers cover the simple case: `iggy_connector_sdk::retry::check_connectivity_with_retry(...)` for `open()`, `HttpRetryMiddleware` for default 429/5xx/network policy on reqwest clients.
- Custom strategies: `reqwest-middleware` + `reqwest_retry::RetryTransientMiddleware::new_with_policy_and_strategy`. `http_sink` defines its own `HttpSinkRetryStrategy` (honors `success_status_codes`, per-status decisions).
- Non-HTTP clients: write `is_transient_error(&e)` mapping driver-specific codes. `postgres_sink::is_transient_error` maps SQLSTATEs `40001`, `40P01`, `57P01-03`, `08000/03/06`.
- Retry loop: new connectors use `iggy_connector_sdk::retry::retry_async(policy, context, should_retry, op)` for anything that fails as `Err`. It owns attempt counting, backoff and the per-retry log, and returns `RetryFailure { error, attempts, exhausted }`; the caller logs the terminal failure.
- Backoff only, for a loop that computes its own delay (it retries on an `Ok` response, carries a deadline, or reconnects between attempts): `retry_backoff(base, retry, max)`, where `retry` is 1-based. `exponential_backoff` is the 0-based primitive underneath, and it applies no jitter; call it directly only when the delay must be exact, as `sdk/src/source.rs::nack_retry_delay` needs for its tests.
- Cap retries at 3 total attempts per request. A whole `consume()` call also needs one time budget; see below.

### Delivery budget

By default the server stores the offset when it answers the poll and the runtime never redelivers a batch after `Err` (see `connector-runtime`), so what `consume()` does inside one call is all the safety a batch gets. `sinks/sink_template` is the reference implementation. With `[delivery] guarantee = "at_least_once"` the runtime redelivers a failed call; see At-least-once below.

- **One time budget per call** (`max_delivery_time`, template default `20s`), shared by all chunks. `consume()` is a synchronous FFI call that blocks a runtime worker thread (one per CPU core by default), and a stop waits for it (the runtime's close budget is the poll timeout plus 60 s). Keep the budget well under a minute.
- **Retry only transient errors** (`HttpRequestFailed`, `Connection`, `CannotStoreData`, `WriteFailure`, `Storage`, `TransientState`, `CatalogCommitError`; the template's `is_transient()`), with jittered backoff (`retry_backoff`). Never retry a `PermanentHttpError` or touch the breaker for it. At most once, skip that chunk; at least once, fail the call (see below). Do not retry anything unclassified: at least once it stops the call at its chunk, like a permanent error.
- **The circuit breaker paces attempts; it does not discard batches.** While it is open, wait for the cool-down rather than returning an error, because a refused batch is gone. Record one failure per failed round. The SDK breaker resets completely after its cool-down and needs `threshold` new failures to reopen, so keep the threshold small (template default 3). Keep the cool-down shorter than the budget and reject a config that breaks that in `open()`.
- **When the budget runs out, stop.** Do not attempt the remaining chunks. Count every undelivered message as failed, log the stream, topic, partition and offset range at `error!`, and return the error. The messages are still in Iggy, so at most once an operator can replay that range, and at least once the runtime hands them over again.
- **Stream settings are part of the design.** `[[streams]]` takes `batch_length`, `min_count` and `max_wait` (which replace `poll_interval`; a sink that sets it fails to start). Waiting for full batches (`min_count = batch_length`) means fewer, larger writes to the destination.

### At-least-once

A sink configured with `[delivery] guarantee = "at_least_once"` gets `enable_at_least_once()` once after `open()`. The SDK default returns `Error::Unsupported`, so the sink refuses to start until it overrides the method.

- **Accept only if a successful `consume()` proves delivery.** A sink that writes inside `consume()` keeps the default no-op `flush()`. A sink that buffers across calls implements `flush()`, which must make every accepted message of the listed partitions durable, and keeps its buffers per topic and partition because topics run concurrently.
- **Never skip a message.** The runtime stores the offset past every message of a successful call, so a skipped chunk is lost for good. Fail the call for a permanent rejection (`is_permanent()` errors stop the sink at that offset; transient ones are handed over again with backoff) and stop at an unclassified error, so no later chunk is written before the redelivery. `sinks/sink_template` does this in `deliver_messages()`.
- **Expect duplicates.** A failed call repeats its whole batch, including chunks already written, and the same stream, topic, partition and offset can arrive again.

### Idempotency

By default the runtime does not retry or redeliver a batch after `Err`: it logs and counts the error and keeps polling, and the server already stored the offset when it answered the poll (at-most-once from Iggy). At least once it does redeliver, so duplicates are certain rather than rare. Duplicates come from a plugin's own retries, from redelivery and from a failover before the stored offset replicates. In-tree guarantees:

- **Postgres** (`postgres_sink::build_batch_insert_query`): PK on `id`, but plain `INSERT` (no `ON CONFLICT`). Duplicate retry triggers `23505`, `is_transient_error` does NOT mark it transient -> error propagates. Adding `ON CONFLICT (id) DO NOTHING` is a known improvement.
- **MongoDB**: composite `_id` from `stream:topic:partition:message_id`, via `insert_many(docs).ordered(false)`. Per-document duplicate-key errors don't abort the batch - effective idempotency under retry.
- **Elasticsearch** (`bulk_index_documents`): bulk action specifies only `_index`, no `_id` -> ES auto-generates ids -> NOT idempotent. Extensions should add `"_id"`.
- **HTTP**: endpoint-dependent. Document in the plugin README.
- **Iceberg**: transactional commit. `Error::CatalogCommitError` documents that the txn is consumed on `commit()`. Rebuild-then-retry can duplicate data - callers must check the catalog first.

When writing a new sink, make the write idempotent (upsert, `ON CONFLICT`, unique index) on `stream:topic:partition:offset`. Partition plus offset names one log position for good, so a replayed message gets the same key. The parts are joined with `:`, which names may contain, so odd names can collide. Do not default to `message.id`: producers can set it, and two messages with the same id would overwrite each other. Use it only when producers guarantee unique ids. Mongo's composite `_id` is the cleanest in-tree reference for upsert-style writes, though it keys on `message_id`.

### Batching

- Default `batch_size: 100`. Tune per-backend.
- Chunk via `messages.chunks(self.batch_size)`. Do not buffer across `consume()` - runtime already batches at the consumer-poll boundary, and `max_wait` / `min_count` on the stream decide how full those batches are.
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
5. Dropping the rest of a call silently when one chunk fails. After the budget runs out, count and log every undelivered message with its offset range. At most once, permanent and unclassified errors only affect their own chunk, so keep going and return the last error. At least once, stop at a permanent or unclassified error.
6. Forgetting to map `PermanentHttpError` - retries hammer the backend with bad data.
7. Mutating config in `consume()` - config is a snapshot from `new()`. Reload requires runtime restart.
8. Logging the connection string. Redact at construction: `http_sink::sanitize_url_for_log` populates `HttpSink.log_url`.
9. Returning an error from `consume()` expecting a retry. By default the runtime logs it and moves on; the batch is not redelivered. Only a sink configured for at-least-once gets the call handed over again.
10. Refusing batches while the circuit breaker is open. By default the offset is already stored, so that discards them. Wait out the cool-down inside the budget instead.

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
