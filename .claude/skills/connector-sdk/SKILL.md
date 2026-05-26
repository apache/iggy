---
name: connector-sdk
description: Extend or modify the Apache Iggy Connectors SDK (`core/connectors/sdk/`). Use when adding a new `Schema` variant, a new `StreamDecoder`/`StreamEncoder`, a new `Error` variant, modifying the `Sink`/`Source`/`Transform` trait surface, the FFI macros, or the `retry`/`api`/`convert` modules. NOT for writing plugins - use `connector-sink` or `connector-source` for those.
---

# Extending the Apache Iggy Connectors SDK

The SDK (`core/connectors/sdk/`) is the **stable contract** between the
runtime and every plugin. Changes ripple to every sink/source in-tree
and every third-party plugin. Treat the public surface as a versioned API.

> **Universal connector rules** (benchmark, SecretString, drop accounting, exemplar patterns) live in
> [connectors-overview](../connectors-overview/SKILL.md). This skill covers SDK-level changes only.

## Contents

- [STOP and ask the user before](#stop-and-ask-the-user-before)
- [File map](#file-map)
- [Cardinal rules](#cardinal-rules)
- [Adding a new `Schema` variant](#adding-a-new-schema-variant)
- [Adding a `StreamDecoder` / `StreamEncoder`](#adding-a-streamdecoder--streamencoder)
- [Adding a new `Error` variant](#adding-a-new-error-variant)
- [Modifying `Sink` / `Source` / `Transform` traits](#modifying-sink--source--transform-traits)
- [FFI macros (`sink_connector!`, `source_connector!`)](#ffi-macros-sink_connector-source_connector)
- [`Payload::try_to_bytes(&self)` is non-negotiable for JSON](#payloadtry_to_bytesself-is-non-negotiable-for-json)
- [Retry helpers (`retry.rs`)](#retry-helpers-retryrs)
- [`ConnectorState`](#connectorstate)
- [Tests for SDK changes](#tests-for-sdk-changes)
- [Before declaring done](#before-declaring-done)

## STOP and ask the user before

- Bumping the SDK MAJOR version or changing any `#[repr(C)]` layout - misaligns every pre-built plugin `.so`.
- Changing `Sink` / `Source` / `Transform` / `StreamDecoder` / `StreamEncoder` trait signatures - cascades through every plugin.
- Removing or renaming a `Schema` variant or `Error` variant - decoders/encoders pinned to it. downstream pattern matches break.
- Changing wire serialization (postcard / rmp_serde / serde_json) for any FFI / state / config payload.
- Changing FFI macro return codes or function names (`iggy_*_open` / `consume` / `handle` / `close`) - runtime + plugins both need lockstep update.

## File map

```text
sdk/src/
├── lib.rs              Traits (Sink, Source, StreamDecoder, StreamEncoder),
│                       Payload, Schema, ConnectorState, Error enum, FFI message structs.
├── sink.rs             SinkContainer + sink_connector! macro (FFI plumbing).
├── source.rs           SourceContainer + source_connector! macro (FFI plumbing).
├── api.rs              ConnectorStatus, ConnectorStats (feature = "api").
├── convert.rs          owned_value_to_serde_json (simd_json ⇄ serde_json bridge).
├── log.rs              CallbackLayer for tracing across FFI.
├── retry.rs            CircuitBreaker, HttpRetryMiddleware, exponential_backoff, jitter.
├── decoders/           One per schema: json, raw, text, proto, flatbuffer, avro.
├── encoders/           Mirror of decoders.
└── transforms/         add_fields, delete_fields, update_fields, filter_fields,
                        unwrap_envelope, proto_convert, flatbuffer_convert, avro_convert.
```

## Cardinal rules

1. **Apache 2.0 license header** on every new file.
2. **`Send + Sync`** on every public trait (FFI runs across thread pools).
3. **`#[repr(C)]`** on every type that crosses FFI (`Schema`, `TopicMetadata`, `MessagesMetadata`, `RawMessage`, `ProducedMessages`, `ConsumedMessage`, `DecodedMessage`, etc. in `lib.rs`). Adding fields requires bumping the SDK version - existing plugins built against the old layout will misalign on `postcard::from_bytes`.
4. **postcard** for FFI message serialization (handled by `SinkContainer::consume`, `SourceContainer`'s `handle_messages`). **MessagePack (`rmp_serde`)** for `ConnectorState`. **JSON** (`serde_json`) only for human-editable config that crosses FFI. Don't mix.
5. **`simd_json::OwnedValue`** for JSON payloads, not `serde_json::Value`. Use `convert::owned_value_to_serde_json` as a bridge when interop is required.
6. **`BTreeMap`** for headers - deterministic ordering. Never `HashMap` on the wire.
7. **No breaking changes** to `Sink`/`Source`/`StreamDecoder`/`StreamEncoder`/`Transform` trait signatures without coordinating with all in-tree plugins in the same PR.

## Adding a new `Schema` variant

The `Schema` enum (in `lib.rs`) is `#[repr(C)]` - it crosses FFI. Touch points to update **in one PR**:

1. `Schema` enum: add variant with `#[strum(to_string = "...")]` matching the `serde(rename_all="snake_case")` form.
2. `Payload` enum: add a matching variant carrying the deserialized form.
3. `Payload::try_into_vec` - consuming bytes-out path.
4. `Payload::try_to_bytes` - **borrowing** bytes-out path. For non-trivial payloads (parsed trees), implement a no-clone serialization, not a `clone() + serialize`. See the `Payload::Json` arm for the canonical optimization.
5. `Payload::Display`.
6. `Schema::try_into_payload` - bytes → `Payload`.
7. `Schema::decoder()` - factory returning `Arc<dyn StreamDecoder>`.
8. `Schema::encoder()` - factory returning `Arc<dyn StreamEncoder>`.
9. New `decoders/<name>.rs` and `encoders/<name>.rs`.
10. Update `sdk/README.md` and `core/connectors/README.md` schema list.
11. Tests: round-trip encode/decode, error paths.

Miss any of these → silent failures at runtime (typically `Error::InvalidPayloadType` or surprising decoder behavior in plugin code).

## Adding a `StreamDecoder` / `StreamEncoder`

Pattern from `decoders/json.rs::JsonStreamDecoder`:

```rust
pub struct MyStreamDecoder;        // unit struct if stateless

impl StreamDecoder for MyStreamDecoder {
    fn schema(&self) -> Schema { Schema::MyFormat }
    fn decode(&self, payload: Vec<u8>) -> Result<Payload, Error> {
        // Decode payload bytes into Payload::MyFormat(...).
        // Errors: Error::CannotDecode(Schema::MyFormat), Error::InvalidJsonPayload, etc.
    }
}
```

For **stateful** decoders (proto/avro/flatbuffer) holding schema descriptors:

- Store the schema state directly in the struct (`config`, `message_descriptor`, `schema`). The instance is built once and wrapped in `Arc<dyn>` for sharing - it must be `Send + Sync` but does not need additional locking if construction-time loading is final.
- Provide **two constructors** when schema loading can fail: lenient `new(config) -> Self` (logs + degrades) AND strict `try_new(config) -> Result<Self, Error>` (fail-fast). Plus a `Default` impl for the no-schema case. Canonical example: `sdk/src/decoders/avro.rs` exports all three (`AvroStreamDecoder::new`, `::try_new`, `Default`).
- If you genuinely need to mutate the schema after construction (e.g., a `update_config` method), use `&mut self` plus `std::mem::replace(&mut self.config, new_config)` to swap without cloning the old value. Live pattern: `sdk/src/decoders/avro.rs::AvroStreamDecoder::update_config` and `sdk/src/encoders/avro.rs::AvroStreamEncoder::update_config`.
- A fresh decoder instance is created via `Schema::decoder()` on each call (it returns `Arc<dyn StreamDecoder>`), so per-decoder caching of shared mutable state is the wrong abstraction.

## Adding a new `Error` variant

The `Error` enum (in `lib.rs`) is `Clone + PartialEq + Eq + Hash`. New variants must preserve these. Guidelines:

- **Use existing variants first.** Only add a new one if the failure mode is distinct in handling, not just description. `InvalidConfigValue(String)` covers most "bad config" cases.
- **Carry context as `String`** - don't add a struct unless multiple discrete fields are needed by callers programmatically.
- **Document retry semantics in the docstring.** Existing variants (`PermanentHttpError`, `CatalogCommitError`, `TransactionApplyError`) document retry behavior - mirror that style.
- **Place near related variants** to keep the enum readable top-to-bottom.

## Modifying `Sink` / `Source` / `Transform` traits

**Breaking changes.** Required process:

1. Document migration in the SDK changelog (and `connectors/sdk/README.md`).
2. Update every in-tree plugin in the same PR. CI builds them all.
3. Bump SDK minor version.
4. Note in PR description: dynamic plugins compiled against the old SDK version will fail to load with mismatched FFI symbols.

Non-breaking additions (new default method, new struct field with `#[serde(default)]`, new variant on a non-exhaustive enum) are preferable.

## FFI macros (`sink_connector!`, `source_connector!`)

The macros (in `sink.rs` and `source.rs`) generate:

- `iggy_<kind>_open(id, config_ptr, config_len, [state_ptr, state_len,] log_callback) -> i32`
- `iggy_sink_consume(id, ...)` / `iggy_source_handle(id, callback) -> i32`
- `iggy_<kind>_close(id) -> i32`
- `iggy_<kind>_version() -> *const c_char` (static lifetime, from `env!("CARGO_PKG_VERSION")`)

Invariants:

- **Duplicate-ID guard** returns `-1` if the caller reopens an ID without closing first. Don't remove this - it prevents silent buffered-data loss.
- `INSTANCES: Lazy<DashMap<u32, SinkContainer<$type>>>` (and `SourceContainer<$type>` mirror) is the only mutable global the macro introduces - keep it that way.
- Return codes: `0` success, `-1` invalid call, `1` open failure. Don't repurpose.

Changes to the FFI signature must update `runtime/src/main.rs::{SourceApi, SinkApi}` in the same PR.

## `Payload::try_to_bytes(&self)` is non-negotiable for JSON

Plugin authors call this on every consumed message. The implementation in `lib.rs::Payload::try_to_bytes` documents *why* it skips the deep `OwnedValue` clone - replacing `O(n) clone + O(n) serialize` with `O(n) serialize`. If you add a new variant requiring a parsed tree, do the same: serialize in place, don't clone.

## Retry helpers (`retry.rs`)

- `CircuitBreaker`: threshold + cooldown, `try_lock()` on the success path to avoid hot-path contention.
- `HttpRetryMiddleware`: integrates with `reqwest-middleware`. Retries 429 + 5xx + network errors. Honors `Retry-After`.
- `max_retries` = **total attempts** including the first try, not extra retries. Document if you change this convention.
- New helpers must take `Duration` (not `u64 millis`) on the public API. Internal computation uses `humantime` parsing of `String`.

## `ConnectorState`

- Bytes are opaque - the wrapping type does not impose schema.
- Serialization is MessagePack via `rmp_serde`. Compact, deterministic, well-supported in serde.
- `serialize`/`deserialize` helpers return `Option<T>` and log on failure. **Failures are non-fatal** - design downstream code to tolerate fresh state.
- Don't change the underlying format without coordinating with all sources.

## Tests for SDK changes

- Unit tests for any new pure function in the changed module.
- Round-trip tests for new schemas/transforms (encode → decode == identity for JSON-equivalent payloads).
- `sdk/tests/` integration tests when the change crosses module boundaries.

## Before declaring done

```text
cargo fmt --all
cargo sort --no-format --workspace
cargo clippy -p iggy_connector_sdk --all-targets --all-features -- -D warnings
cargo test -p iggy_connector_sdk --all-features

# Rebuild all plugins to catch breaking-change leaks:
cargo build -p iggy_connector_stdout_sink -p iggy_connector_random_source
# If FFI signatures changed, also build the runtime:
cargo build -p iggy-connectors
```

---

Discussion / help: see [AGENTS.md](../../../AGENTS.md#discussion-and-support).
