---
name: connector-source
description: Author a new Apache Iggy connector source plugin under core/connectors/sources/. Sources poll an external system (DB, API, queue) and produce messages into Apache Iggy streams. Load when creating, modifying, or reviewing a source crate. Use for source plugin authoring. NOT for runtime internals (see connector-runtime).
---

# Writing an Apache Iggy Connector Source

A **source** is a Rust `cdylib` that implements
`iggy_connector_sdk::Source` and exposes FFI symbols via the
`source_connector!` macro. The runtime calls `poll()` in a loop,
applies transforms, encodes via the configured `Schema`, sends to
Apache Iggy, and persists the state `poll()` returned - but only after
the send succeeds. Only one batch is ever in flight: the runtime does
not call `poll()` again until it has reported `Ack` or `Nack` for the
current one via `on_batch_result()` (source batch acknowledgment, see PR #3855
for the full contract). See
[State persistence](#state-persistence-stage-in-poll-commit-in-on_batch_result)
below.

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

- Changing the SDK trait surface (`Source::open` / `poll` / `on_batch_result` / `close`) - that's an SDK change, and `poll`/`on_batch_result` are also an FFI change (`iggy_source_handle_v2`, `iggy_source_batch_result` - breaks every pre-built plugin `.so`).
- Adding a long-running side task in the plugin - the runtime owns lifecycle. orphans survive `close()`.
- Persisting unbounded state - `State` is rewritten every batch.
- Adding a source that requires authoritative offsets external to Apache Iggy without coordinating retention.

## Quick reference

- Skeleton: [TEMPLATE.md](TEMPLATE.md) (fill-in-the-blank kit — implement only `TODO(ConnectorDeveloper)`).
- PR pre-flight: [connector-pr-review](../connector-pr-review/SKILL.md).
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

### State persistence: stage in `poll()`, commit in `on_batch_result()`

Source connectors use a one-in-flight-batch ACK/NACK contract (#3855)
between the plugin and the runtime:

1. `poll()` returns messages and *candidate* state without committing
   cursor changes or destructive operations (deletes, mark-processed).
2. The runtime sends the batch to Apache Iggy and waits for the
   producer result.
3. After a successful send, the runtime persists the candidate state
   to `{state_path}/source_{key}.state`.
4. The runtime calls `on_batch_result(SourceBatchResult::Ack)`. A send
   or state-save failure calls `on_batch_result(SourceBatchResult::Nack)`
   instead.
5. `on_batch_result()` commits or discards the plugin's staged work
   before the next `poll()` starts. The SDK allows only one batch in
   flight - it will not call `poll()` again until `on_batch_result()`
   for the current batch has returned.

Canonical pattern (`sources/random_source/src/lib.rs`):

```rust
pending_state: Mutex<Option<State>>,   // staged, not yet committed

async fn poll(&self) -> Result<ProducedMessages, Error> {
    // ... fetch ...
    let candidate_state = State { cursor: next_cursor };
    *self.pending_state.lock().await = Some(candidate_state.clone());
    Ok(ProducedMessages {
        schema: Schema::Json,
        messages,
        state: Some(ConnectorState::serialize(&candidate_state, NAME, self.id)?),
    })
}

async fn on_batch_result(&self, result: SourceBatchResult) -> Result<(), Error> {
    let candidate_state = self.pending_state.lock().await.take();
    if result == SourceBatchResult::Ack
        && let Some(candidate_state) = candidate_state
    {
        *self.state.lock().await = candidate_state;
    }
    // Nack: drop candidate_state, committed self.state is untouched -
    // the same range is polled again.
    Ok(())
}
```

- `ConnectorState` is `Vec<u8>` via MessagePack (`rmp_serde`). Use `ConnectorState::serialize(&state, NAME, id)` + `ConnectorState::deserialize::<State>(NAME, id)`. Both return `Option<T>` and log on failure (non-fatal).
- **The default `on_batch_result` is a no-op.** Only override it - and only then does staging via `pending_state` matter - if `poll()` advances a cursor or performs destructive work (delete-after-read, mark-processed). A source with no staged work (e.g. a pure generator) can rely on the default.
- Returning `Err` from `on_batch_result` **stops the SDK from polling further** - a failed rollback must not be allowed to silently advance to the next batch.
- **Always return state in every `ProducedMessages`**, including empty polls that made progress. Return `state: None` for an empty poll that made *no* progress - this avoids an unnecessary state write and cannot persist state left over from a failed batch.
- Keep `State` small - rewritten every batch. No unbounded vecs.
- NACK handling must discard staged cursor changes and staged delete/mark operations so polling redelivers the batch. The SDK retries NACKed batches with capped exponential backoff and stops the source after repeated consecutive NACKs.
- Crash recovery is at-least-once at every point except after the plugin has processed the ACK (see the SDK README's crash-point table, `core/connectors/sdk/README.md#source-delivery-acknowledgment`, for the full breakdown).

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
- Only one batch is ever in flight: the SDK does not call `poll()` again until `on_batch_result()` has returned for the previous batch (up to a 30s result timeout, after which the SDK treats it as a Nack).
- Don't spawn your own long-running Tokio tasks - runtime owns lifecycle.

### Errors

| Scenario                                            | Variant                                           |
| --------------------------------------------------- | ------------------------------------------------- |
| Bad config in `new()`/`open()`                      | `Error::InitError`                                |
| Cannot reach external system at startup             | `Error::InitError` or `Error::Connection`         |
| Transient fetch failure (retry-worthy)              | `Error::Connection` or `Error::HttpRequestFailed` |
| Permanent fetch failure (auth, schema gone)         | `Error::PermanentHttpError`                       |
| Row failed to serialize                             | `Error::Serialization(...)`                       |
| State serialization failed                          | log + skip (non-fatal)                            |
| `on_batch_result()` failed to roll back staged work | `Err` - stops the SDK from polling further        |

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
4. Returning `state: None` for an empty poll that *did* make progress (e.g. advanced a watermark) - only a no-progress empty poll should return `None`.
5. Committing a cursor or destructive work (delete/mark-processed) directly in `poll()` instead of staging it and applying it in `on_batch_result()` on `Ack` - a Nack (send or state-save failure) has nothing to discard, and the batch is redelivered against already-mutated state.
6. Unbounded data in `State` - rewritten every batch. keep O(constant).
7. `std::sync::Mutex` - blocks the executor. Use `tokio::sync::Mutex`.
8. Not setting `ProducedMessage.id` when a stable ID exists - loses idempotency.
9. Spawning side tasks - the runtime owns the scheduler.

## Tests

Mandatory six canonical source tests (see [connector-testing](../connector-testing/SKILL.md) for the full pattern): the four
state tests (restore / no-state / invalid-state / round-trip) plus `given_ack_when_batch_is_staged_should_commit_candidate_state`
and `given_nack_when_batch_is_staged_should_keep_committed_state`. Copy from `sources/random_source/src/lib.rs::tests`. Plus
config defaults, payload building, schema selection. A source relying on the default no-op `on_batch_result` (no staged work)
may skip the ack/nack pair.

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
