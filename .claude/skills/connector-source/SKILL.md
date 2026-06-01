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

- Changing the SDK trait surface (`Source::open` / `poll` / `close`) - that's an SDK change.
- Adding a long-running side task in the plugin - the runtime owns lifecycle. orphans survive `close()`.
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

### State persistence

- `ConnectorState` is `Vec<u8>` via MessagePack (`rmp_serde`). Use `ConnectorState::serialize(&state, NAME, id)` + `ConnectorState::deserialize::<State>(NAME, id)`. Both return `Option<T>` and log on failure (non-fatal).
- Runtime saves to `{state_path}/source_{key}.state` only after a successful Iggy send. Between `poll()` returning and the runtime persisting the save, a crash leaves the same cursor for the next poll - downstream must tolerate at-least-once.
- **Always return state in every `ProducedMessages`**, including empty polls. Empty results still need to advance watermarks (timestamp sources) or affirm "nothing new."
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
- Don't spawn your own long-running Tokio tasks - runtime owns lifecycle.

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
