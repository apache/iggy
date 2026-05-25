---
name: connector-transform
description: Add a new transform under `core/connectors/sdk/src/transforms/`. Transforms mutate, filter, or convert messages between source/encode and decode/sink. Use when adding field-level transforms (add/delete/update/filter), format conversions, envelope unwrapping, or similar config-driven message manipulations for Apache Iggy connectors.
---

# Adding a New Apache Iggy Connector Transform

A transform implements `iggy_connector_sdk::transforms::Transform`. It runs after decode (for sinks) or before encode (for sources), in a chain configured per-connector. Config-driven, composable.

## File map

```text
sdk/src/transforms/
├── mod.rs                 Transform trait, TransformType enum, from_config() factory,
│                          FieldValue + ComputedValue helpers
├── add_fields.rs          AddFields - append static or computed fields to JSON
├── delete_fields.rs       DeleteFields - remove fields by key
├── update_fields.rs       UpdateFields - conditional field replacement
├── filter_fields.rs       FilterFields - keep/drop messages by key/value pattern
├── unwrap_envelope.rs     UnwrapEnvelope - promote nested field to top-level payload
├── proto_convert.rs       ProtoConvert - bidirectional Proto ⇄ Json/Text/Raw/etc.
├── flatbuffer_convert.rs  FlatBufferConvert
├── avro_convert.rs        AvroConvert
└── json/                  Shared JSON helpers (path traversal, ops)
```

## The trait

```rust
// sdk/src/transforms/mod.rs - Transform
pub trait Transform: Send + Sync {
    fn r#type(&self) -> TransformType;
    fn transform(
        &self,
        metadata: &TopicMetadata,
        message: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error>;
}
```

Semantics:

- **`&self`** - transforms are immutable post-construction. Internal state (compiled regex, parsed paths, schema descriptors) is computed in `new()` and stored as plain struct fields. No locks needed - the whole instance is shared via `Arc<dyn Transform>`.
- Takes `message` **by value** - the transform owns the `DecodedMessage` and may mutate freely without cloning.
- Returns `Ok(Some(msg))` to pass through (possibly modified), `Ok(None)` to **filter** (drop silently), `Err(e)` for actual failures.
- `&TopicMetadata` is for routing-aware logic. Most transforms ignore it.

## Steps to add a new transform

### 1. Create the file

`sdk/src/transforms/my_transform.rs`. Apache 2.0 license header. Code reads top to bottom - config struct, then transform struct, then `impl`, then helpers.

```rust
/* Apache 2.0 header */

use crate::transforms::{Transform, TransformType};
use crate::{DecodedMessage, Error, Payload, TopicMetadata};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MyTransformConfig {
    pub key: String,
    pub option: Option<bool>,
}

pub struct MyTransform {
    config: MyTransformConfig,
}

impl MyTransform {
    pub fn new(config: MyTransformConfig) -> Result<Self, Error> {
        if config.key.is_empty() {
            return Err(Error::InvalidConfigValue(
                "my_transform.key must not be empty".into(),
            ));
        }
        Ok(Self { config })
    }
}

impl Transform for MyTransform {
    fn r#type(&self) -> TransformType {
        TransformType::MyTransform
    }

    fn transform(
        &self,
        _metadata: &TopicMetadata,
        mut message: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error> {
        // Most transforms operate only on Payload::Json - non-JSON is a no-op.
        let Payload::Json(ref mut value) = message.payload else {
            return Ok(Some(message));
        };

        // Mutate `value` in place via simd_json::OwnedValue API.
        // Return Ok(None) to filter, Ok(Some(message)) to pass through.

        Ok(Some(message))
    }
}
```

### 2. Wire into `transforms/mod.rs`

Three edits, all required:

(a) Module declaration + re-export at the top:

```rust
mod my_transform;
pub use my_transform::{MyTransform, MyTransformConfig};
```

(b) Discriminant in `TransformType`:

```rust
pub enum TransformType {
    AddFields,
    // ...
    MyTransform,   // snake_case form in TOML: "my_transform"
}
```

(c) Branch in `from_config()`:

```rust
TransformType::MyTransform => {
    // For new transforms, prefer Error::InvalidConfigValue with formatted context -
    // it preserves the underlying serde_json error. Existing code uses bare
    // Error::InvalidConfig for most variants (UnwrapEnvelope is the exception).
    let cfg: MyTransformConfig = serde_json::from_value(raw.clone())
        .map_err(|error| Error::InvalidConfigValue(format!("my_transform: {error}")))?;
    Ok(Arc::new(MyTransform::new(cfg)?))
}
```

`from_config()` returns `Arc<dyn Transform>` so the same instance is shared across the pipeline.

### 3. Document

Add a TOML usage example to `sdk/README.md`:

```toml
[transforms.my_transform]
enabled = true
key = "some_field"
option = true
```

### 4. Tests

Bottom of `my_transform.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_matching_field_should_transform() { ... }

    #[test]
    fn given_non_json_payload_should_passthrough() { ... }

    #[test]
    fn given_filter_condition_should_drop_message() { /* assert Ok(None) */ }

    #[test]
    fn given_invalid_config_should_error_in_new() {
        let cfg = MyTransformConfig { key: "".into(), option: None };
        assert!(matches!(MyTransform::new(cfg), Err(Error::InvalidConfigValue(_))));
    }

    fn topic_meta() -> TopicMetadata {
        TopicMetadata { stream: "s".into(), topic: "t".into() }
    }

    fn json_message(json: serde_json::Value) -> DecodedMessage { /* ... */ }
}
```

## Rules

### Operate on JSON only (mostly)

Most field-level transforms only make sense for `Payload::Json`. Convention (see `add_fields.rs::AddFields::transform`): silently pass through non-JSON:

```rust
let Payload::Json(ref mut value) = message.payload else {
    return Ok(Some(message));
};
```

Format-conversion transforms (`proto_convert`, `avro_convert`, `flatbuffer_convert`) are the exception - they explicitly convert between variants.

### Mutate `OwnedValue` in place, don't clone-then-replace

`simd_json::OwnedValue` is mutable in place:

```rust
if let OwnedValue::Object(ref mut obj) = value {
    obj.insert(key.into(), new_value);
}
```

Do NOT clone the value, mutate the clone, then `message.payload = Payload::Json(new)`. The whole point of `&mut value` is in-place mutation.

### Filter semantics

`Ok(None)` drops the message **silently** - no error log, no metric. By design (filters succeed when they decide to drop). For visibility, log at `debug!` level.

### Config validation

Validate in `new()` and return `Err(Error::InvalidConfigValue(...))`. Don't `panic!`. Don't validate on the hot path.

For transforms with regex or compiled patterns, compile once in `new()` and store the compiled form.

### Stateless except for config

Transforms are constructed once at startup and `Arc`-cloned across tasks. Must be `Send + Sync`. No mutable runtime state - if you need accumulator-like behavior, that's not a `Transform`, that's a sink.

### Compiled state goes in the struct, not behind a lock

Things derived from config (compiled regex, parsed paths, schema descriptors) should be computed in `new()` and stored as plain struct fields. The whole instance is shared via `Arc<dyn Transform>`, so per-field locking is unnecessary. See `sdk/src/transforms/filter_fields.rs::FilterFields::new` for the canonical pattern: regexes compile in `new()`, get stored as struct fields, are then read via `&self`.

### `transform()` is sync

The trait method is sync (not `async`). Don't propose making it async - that would require pervasive change across the runtime. Schemas and other external resources should be loaded in `new()` (potentially via a fallible `try_new`), not lazily during `transform()`.

### Errors

| Scenario | Variant |
| ---------- | --------- |
| Config validation in `new()` | `Error::InvalidConfigValue("transform_name: ...")` |
| Per-message validation (bad shape) | `Error::InvalidRecordValue("...")` |
| Schema resolution failure | `Error::InvalidTransformer` |
| Format conversion failure | `Error::InvalidPayloadType` |

Returning `Err` from `transform()` stops the message and is logged. For non-fatal field-level issues (field missing when transform expects it), prefer pass-through (`Ok(Some(message))`) unless config explicitly says to fail.

## Common pitfalls

1. **Forgetting one of the three `mod.rs` edits** - module declaration, `TransformType` variant, `from_config()` branch.
2. **`#[serde(rename_all = "snake_case")]` mismatch** between enum variant name and TOML key.
3. **Cloning `OwnedValue`** when in-place mutation would work.
4. **Validating config inside `transform()`** instead of `new()`.
5. **`std::sync::Mutex` for internal state** on the hot path - precompute in `new()` and store as plain struct fields. Transforms run on every message; locks are wasted work.
6. **Returning `Err` for a filter** - filters use `Ok(None)`.

## Before declaring done

```text
cargo fmt --all
cargo sort --no-format --workspace
cargo clippy -p iggy_connector_sdk --all-targets --all-features -- -D warnings
cargo test -p iggy_connector_sdk --all-features
```

Verify by adding the transform to `core/connectors/runtime/example_config/connectors/` and running the runtime - the transform should apply to each message.

If the transform changes payload shape sufficiently to break downstream sinks (e.g., renaming primary keys), call it out in the PR description.
