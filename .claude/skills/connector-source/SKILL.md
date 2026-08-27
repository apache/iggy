---
name: connector-source
description: Author a new Apache Iggy connector source plugin under core/connectors/sources/. Sources poll an external system (DB, API, queue) and produce messages into Apache Iggy streams. Load when creating, modifying, or reviewing a source crate. Use for source plugin authoring. NOT for runtime internals (see connector-runtime).
---

# Writing an Apache Iggy Connector Source

A **source** is a Rust `cdylib` that implements
`iggy_connector_sdk::Source` and exposes FFI symbols via the
`source_connector!` macro. The runtime calls `poll()` in a loop,
applies transforms, encodes via the configured `Schema`, sends to
Apache Iggy, and persists the returned `ConnectorState` after every
successful send.

> **Universal connector rules** (SecretString, benchmark, verbose flag, drop accounting, filter contract, exemplar patterns) live in
> [connectors-overview](../connectors-overview/SKILL.md). This skill
> covers only what's source-specific.

## Contents

- [STOP and ask the user before](#stop-and-ask-the-user-before)
- [Quick reference](#quick-reference)
- [Hard rules](#hard-rules)
- [Common pitfalls](#common-pitfalls)
- [Tests](#tests)
- [Before declaring done](#before-declaring-done)

## STOP and ask the user before

- Changing the SDK trait surface (`Source::open` / `poll` / `on_batch_result` / `close`) - that's an SDK change.
- Adding a long-running side task in the plugin - the runtime owns lifecycle, and orphans survive `close()`. Sanctioned only where the source is itself a server the runtime cannot drive, as in `http_source`'s listener, and then only with an explicit shutdown in the last `close()` that awaits its tasks before returning.
- Persisting unbounded state - `State` is rewritten every batch.
- Adding a source that requires authoritative offsets external to Apache Iggy without coordinating retention.

## Quick reference

- Skeleton: [TEMPLATE.md](TEMPLATE.md) (load on demand).
- Exemplars: `random_source` (minimal + canonical state tests), `postgres_source` (cursor / delete-after-read / processed-column modes, restart-survives-state tests), `elasticsearch_source` (scroll cursor), `influxdb_source` (time-series scan).

## Hard rules

### `poll()` signature is `&self`

The macro shares the source as `Arc<T>` across the FFI callback and forwarding loop. Signature: `async fn poll(&self) -> ...` - any mutable state behind `tokio::sync::Mutex`. **Single most common new-contributor mistake.**

### Lock discipline

Never hold the state `Mutex` across upstream I/O. Canonical pattern (matches `sources/postgres_source/src/lib.rs::poll_tables`):

```rust
let cursor = { self.state.lock().await.cursor.clone() };   // brief read
let rows = client.query(&sql, &[&cursor]).await?;           // no lock held
let persisted = {                                           // brief write
    let mut state = self.state.lock().await;
    state.cursor = Some(new_cursor);
    ConnectorState::serialize(&*state, CONNECTOR_NAME, self.id)
};
```

### Delivery acknowledgment

`on_batch_result` (added by #3855) is how a source learns what happened to the batch it just
returned. The SDK keeps exactly one batch in flight: it will not call `poll()` again until this
returns, and it stops the source after `MAX_CONSECUTIVE_NACKS` (5) consecutive NACKs, roughly 1.5s
of backoff, without calling `close()`.

- `Ack` means the runtime sent the batch **and** persisted its state. `Nack` means it did neither.
- The trait has a **default no-op**, which suits only a source with no staged cursor and no
  destructive work. If `poll()` advances a cursor, deletes rows, or drains an in-memory buffer,
  omitting this loses data silently and nothing will tell you. `random_source` and `http_source`
  implement it; the other shipped sources take the default and skip rows on a NACK.
- Stage in `poll()`, apply on `Ack`, discard or replay on `Nack`. A source whose input is pushed to
  it, rather than re-readable upstream, has to hold the batch itself: see `http_source`'s staging.
- Returning `Err` from it stops the source immediately, so it is not a retry signal.

### State persistence

- `ConnectorState` is `Vec<u8>` via MessagePack (`rmp_serde`). Use `ConnectorState::serialize(&state, NAME, id)` + `ConnectorState::deserialize::<State>(NAME, id)`. Both return `Option<T>` and log on failure (non-fatal).
- Runtime saves to `{state_path}/source_{key}.state` only after a successful Iggy send. Between `poll()` returning and the runtime persisting the save, a crash leaves the same cursor for the next poll - downstream must tolerate at-least-once.
- **Return state on a batch whose send cannot fail.** The runtime saves state only on the success branch of the Iggy send, so state riding a batch of messages is skipped whenever that send fails, while the source has already cleared whatever dirty flag it tracks.
- Timestamp sources that stage their cursor and apply it in `on_batch_result` can attach state to any batch. A source whose state is a control-plane record rather than a cursor, such as `http_source`'s endpoint registry, should ride it on an empty batch instead, which cannot fail for want of a publish.
- The runtime can still NACK an empty batch: it short-circuits the send stage when its own state storage is latched or a pending checkpoint will not resolve. Never treat a hand-off as durable; `on_batch_result` is how you learn.
- Keep `State` small - rewritten every batch. No unbounded vecs.

### Sleep first

`poll()` must `sleep(self.poll_interval).await` before any work. Without it, an empty source spins a CPU.

### Schema selection

Match `ProducedMessages.schema` to the bytes in `messages[i].payload`:

- JSON-serialized rows → `Schema::Json`
- Already-protobuf bytes → `Schema::Proto`
- Already-avro bytes → `Schema::Avro`
- Opaque → `Schema::Raw`

### IDs and timestamps

- `ProducedMessage.id: Option<u128>` - set when a natural ID exists (DB PK, document id). Apache Iggy can dedupe on this.
- `origin_timestamp: Option<u64>` - source-system event time in nanoseconds. Lets downstream sinks reason about lag.
- `timestamp` and `checksum` are Iggy-side - leave `None`.

### Concurrency

- Runtime spawns ONE `poll()` task per source. No concurrent `poll()`.
- Don't spawn your own long-running Tokio tasks: the runtime owns lifecycle. The exception is a source that listens rather than polls, which has to own its listener; `http_source` is the worked example, and it shuts its tasks down in the last `close()` rather than leaving them to outlive the connector.

### Errors

| Scenario                                    | Variant                                           |
| ------------------------------------------- | ------------------------------------------------- |
| Bad config in `new()`/`open()`              | `Error::InitError`                                |
| Cannot reach external system at startup     | `Error::InitError` or `Error::Connection`         |
| Transient fetch failure (retry-worthy)      | `Error::Connection` or `Error::HttpRequestFailed` |
| Permanent fetch failure (auth, schema gone) | `Error::PermanentHttpError`                       |
| Row failed to serialize                     | `Error::Serialization(...)`                       |
| State serialization failed                  | log + skip (non-fatal)                            |

Returning `Err` from `poll()` is only logged by the SDK's FFI bridge
(`sdk/src/source.rs::handle_messages`) - the loop continues, the next
`poll()` runs. Connector status does NOT flip to `Error` from a poll
failure. Status `Error` is set by the runtime only on transform/encode
failure, Iggy send failure, or state save failure
(`runtime/src/source.rs::source_forwarding_loop` calls to
`context.sources.set_error`). To surface a poll failure as unhealth,
raise it through the metric counter or escalate to `Error::InitError`
from `open()`.

### Logging

```rust
info!("Opened <connector> connector ID: {}, endpoint: {}", self.id, ...);
info!("Restored state for <connector> ID: {id}, cursor: {:?}", ...);
debug!("Polled {} rows for <connector> ID: {}", rows.len(), self.id);
warn!("Transient fetch failure for <connector> ID: {}, will retry: {error}", self.id);
error!("Failed to <op> for <connector> ID: {}, error: {error}", self.id);
info!("Closed <connector> connector ID: {}, total produced: {}", self.id, ...);
```

Iggy consumer-loop labels use literal API names (`offset=`, `current_offset=`).

## Common pitfalls

1. `async fn poll(&mut self)` - won't compile. Use `&self` + `Mutex<State>`.
2. Holding `state.lock()` across the fetch I/O - blocks `close()`, causes shutdown timeouts.
3. Forgetting to sleep - 100% CPU on idle source.
4. Returning state only on success - state should advance on empty polls too.
5. Unbounded data in `State` - rewritten every batch. keep O(constant).
6. `std::sync::Mutex` - blocks the executor. Use `tokio::sync::Mutex`.
7. Not setting `ProducedMessage.id` when a stable ID exists - loses idempotency.
8. Spawning side tasks - the runtime owns the scheduler.

## Tests

Mandatory four canonical source state tests (see [connector-testing](../connector-testing/SKILL.md) for the full pattern). Copy from `sources/random_source/src/lib.rs::tests`. Plus config defaults, payload building, schema selection.

Integration tests under `core/integration/tests/connectors/<backend>/` for any source backed by external infra. Use `#[iggy_harness]` + a `TestFixture` backed by `testcontainers-modules`. Reference: `core/integration/tests/connectors/postgres/postgres_source.rs` (multi-mode tests) + `restart.rs` (state survives restart).

## Before declaring done

```bash
cargo fmt --all
cargo sort --no-format --workspace
cargo clippy -p iggy_connector_<name>_source --all-targets -- -D warnings
cargo test -p iggy_connector_<name>_source

# Integration tests:
cargo test -p integration -- connectors::<backend>::<test_name>
```

Update `core/connectors/sources/README.md` and add a sample TOML under `core/connectors/runtime/example_config/connectors/`.

---

Discussion / help: see [AGENTS.md](../../../AGENTS.md#discussion-and-support).
