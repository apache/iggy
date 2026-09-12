# Apache Iggy Connectors - SDK

SDK provides the commonly used structs and traits such as `Sink` and `Source`, along with the `sink_connector!` and `source_connector!` macros to be used when developing connectors.

The macros automatically export the connector's version (from `CARGO_PKG_VERSION`) via FFI, allowing the runtime to report per-connector version information in the `/stats` endpoint.

## Source delivery acknowledgment

Source connectors use a one-in-flight-batch contract between the plugin and the runtime:

1. `Source::poll()` returns messages and candidate state without committing cursor changes or destructive operations.
2. The runtime sends the batch to Iggy and waits for the producer result.
3. After a successful send, the runtime persists the candidate state if the batch provides it.
4. The runtime reports `SourceBatchResult::Ack` to the plugin. A send or state-save failure reports `SourceBatchResult::Nack` instead.
5. `Source::on_batch_result()` commits or discards the plugin's staged work before the next poll starts.

An empty batch follows the same handshake. A source should return `state: None` when an empty poll made no progress; this avoids an unnecessary state write and cannot persist state left over from a failed batch. Producer errors, including request timeouts, report a NACK. A successful send from the legacy Iggy server is still an ACK even though that server returns an empty confirmation list.

The crash behavior is intentionally at-least-once:

| Crash point | Recovery behavior |
| --- | --- |
| Before Iggy commits the batch | Persisted state is unchanged and the source can poll the batch again. |
| After Iggy commits but before the runtime observes success | Persisted state is unchanged, so the batch may be delivered again. |
| After send success but before state persistence | Persisted state is unchanged, so the batch may be delivered again. |
| After state persistence but before the plugin processes the ACK | The restored state records the delivered batch. Deferred source-side cleanup may still be pending. |
| After the plugin processes the ACK | The state and plugin cursor both record the delivered batch. |
| After a replicated confirmation and source cleanup, but before stable storage | Losing the replicas holding the unpersisted tail can lose the batch. Create the topic with `durability=persisted` when source cleanup requires durable quorum confirmation. |

An ACK follows Iggy's quorum confirmation. The topic's `durability` policy decides whether that confirmation also waits for stable storage on the quorum. Both policies normally write messages to disk.

Source-side ACK work should be idempotent because process termination can interrupt it. NACK handling must discard staged cursor changes and staged delete or mark operations so polling can redeliver the batch. The SDK retries NACKed batches with capped exponential backoff and stops after repeated NACKs.

The default `Source::on_batch_result()` implementation is a no-op for sources without staged work. Sources that advance cursors, delete rows, or mark rows must override it. The SDK stops polling if the handler returns an error, preventing a failed rollback from advancing to another batch.

This contract is a breaking FFI change. Source plugins must be rebuilt with the matching SDK. The runtime loads `iggy_source_handle_v2`, which supplies a batch ID to the runtime callback, and source plugins export `iggy_source_batch_result` for the corresponding ACK or NACK.

Moreover, it contains both, the `decoders` and `encoders` modules, implementing either `StreamDecoder` or `StreamEncoder` traits, which are used when consuming or producing data from/to Iggy streams.

The SDK provides decoders and encoders for JSON, raw bytes, text, Protocol Buffers, FlatBuffers, and Avro. Their supported conversions have format-specific limits; see below and the [transforms guide](https://iggy.apache.org/docs/connectors/transforms).

Last but not least, the different `transforms` are available, to transform (add, update, delete etc.) the particular fields of the data being processed via external configuration. It's as simple as adding a new transform to the `transforms` section of the particular connector configuration file:

```toml
[transforms.add_fields]
enabled = true

[[transforms.add_fields.fields]]
key = "message"
value.static = "hello"
```

## Retry helpers

`retry_async` runs an operation that fails with `Err` and retries it while `should_retry` accepts the error. It owns attempt counting, backoff and the per-retry log, and returns `RetryFailure { error, attempts, exhausted }` so the caller logs the terminal failure. `retry_backoff` computes a single delay for a loop that cannot use `retry_async`, such as `HttpRetryMiddleware`, which retries on an `Ok` response rather than an `Err`. Its `retry` argument is 1-based.

Two items changed in a way that breaks out-of-tree plugins, so those plugins must be rebuilt against the current source:

| Removed | Replacement |
| --- | --- |
| `ConnectivityConfig` | `RetryPolicy`. `max_open_retries` becomes `max_attempts`, `retry_delay` becomes `base_delay`, and `open_retry_max_delay` becomes `max_delay`. |
| `jitter` (was public) | `retry_backoff`, which applies the jitter itself. |

Both types carry `(u32, Duration, Duration)` and the two delay roles cross over, so a field-by-field rename compiles and swaps the base delay for the cap. Map the fields by name.

## Protocol Buffers Support

The SDK includes support for Protocol Buffers (protobuf) format with both encoding and decoding capabilities. Protocol Buffers provide efficient serialization and are particularly useful for high-performance data streaming scenarios.

### Configuration Example

This example uses the Random source and Stdout sink from the matching checkout. Start a server using the **[getting-started guide](https://iggy.apache.org/docs/introduction/getting-started)**, then build the plugins, runtime, and CLI from the repository root:

```bash
cargo build --release -p iggy_connector_random_source -p iggy_connector_stdout_sink -p iggy-connectors -p iggy-cli
mkdir -p connectors
```

The source's `schema = "proto"` selects the default protobuf encoder, which wraps each JSON record in a `google.protobuf.StringValue` inside `google.protobuf.Any`. The sink reads raw bytes and applies `proto_convert` to expose the Any envelope as JSON. This does not unpack a custom protobuf message schema.

**Main runtime config (connectors.toml):**

```toml
[iggy]
address = "localhost:8090"
username = "iggy"
password = "iggy"

[connectors]
config_type = "local"
config_dir = "connectors"
```

**Source connector config (connectors/protobuf_source.toml):**

```toml
type = "source"
key = "protobuf"
enabled = true
version = 0
name = "Protobuf Source"
path = "target/release/libiggy_connector_random_source"

[[streams]]
stream = "protobuf_stream"
topic = "protobuf_topic"
schema = "proto"
batch_length = 1000
linger_time = "5ms"

[plugin_config]
interval = "100ms"
messages_range = [1, 10]
payload_size = 32
max_count = 100
```

**Sink connector config (connectors/protobuf_sink.toml):**

```toml
type = "sink"
key = "protobuf"
enabled = true
version = 0
name = "Protobuf Sink"
path = "target/release/libiggy_connector_stdout_sink"

[[streams]]
stream = "protobuf_stream"
topics = ["protobuf_topic"]
schema = "raw"

[plugin_config]
print_payload = true

[transforms.proto_convert]
enabled = true
source_format = "proto"
target_format = "json"
include_paths = ["."]
preserve_unknown_fields = false

[transforms.proto_convert.conversion_options]
validate_messages = true
pretty_json = false
include_metadata = false
type_url_prefix = "type.googleapis.com"
strict_mode = false
```

Create the stream and topic, then start the runtime from the repository root:

```bash
./target/release/iggy --username iggy --password iggy stream create protobuf_stream
./target/release/iggy --username iggy --password iggy topic create protobuf_stream protobuf_topic 1 none 1d
IGGY_CONNECTORS_CONFIG_PATH=connectors.toml ./target/release/iggy-connectors
```

The source sends 100 records, then continues polling without new messages. Stdout logs message offsets and the serialized JSON envelope bytes, containing `type_url` and base64 `value`. The sink's `raw` schema also determines how the plugin receives those transformed bytes.

The format-conversion transforms define no per-key defaults. Every non-optional key shown above must be present, or the configuration fails to deserialize (`schema_path`, `message_type`, `field_mappings`, and `descriptor_set` are optional).

The two `[[streams]]` shapes differ: a source produces to a single `topic` and can tune batching via `batch_length` and `linger_time`, while a sink consumes from a list of `topics` and can additionally set `batch_length`, `poll_interval`, and `consumer_group`.

### Key Configuration Options

#### Programmatic Encoder and Decoder Configuration

These are SDK configuration fields, not Random or Stdout `plugin_config` keys. The runtime's `schema = "proto"` uses the default encoder or decoder.

- **`schema_path`**: Path to the `.proto` file containing message definitions
- **`message_type`**: Fully qualified name of the protobuf message type to use
- **`use_any_wrapper`**: Selects the Any fallback when no message descriptor is loaded; a loaded descriptor takes precedence

#### Transform Options

- **`proto_convert`**: Transform for converting between protobuf and other formats
- **`source_format`** / **`target_format`**: Formats to convert between - any schema value (`json`, `raw`, `text`, `proto`, `flat_buffer`, `avro`)
- **`preserve_unknown_fields`**: Accepted by `proto_convert`, but currently has no effect
- **`include_paths`**: Additional directories searched for imported `.proto` files
- **`field_mappings`**: Renames fields in a JSON input object before conversion (e.g., `"old_field" = "new_field"`)
- **`conversion_options`**: `pretty_json` controls JSON text output and `include_metadata` enriches supported protobuf-to-JSON paths. `validate_messages`, `type_url_prefix`, and `strict_mode` are accepted but currently have no effect

The `schema_registry_url` field is reserved and currently not implemented. The SDK never contacts a schema registry, and schemas are loaded only from `schema_path` or `descriptor_set`.

- **`unwrap_envelope`**: Extracts a nested JSON field and promotes it as the top-level payload.
  Required when a source emits envelope-wrapped records (with metadata fields alongside a nested
  data object) and the downstream sink expects flat JSON matching the target table schema.
  - **`field`**: The envelope key whose value becomes the new payload (e.g., `"data"`). Must not be empty.

```toml
[transforms.unwrap_envelope]
enabled = true
field = "data"
```

### Supported Features

- **Encoding**: A loaded message descriptor encodes matching JSON fields. The encoder supports booleans, strings, all protobuf integer types, and base64 strings for bytes or already-encoded nested messages. Float, double, and enum fields are unsupported by the encoder; nested JSON objects, repeated fields, maps, and proto2 groups are not a general-purpose schema conversion path.
- **Decoding**: A loaded descriptor extracts present fields. Integer, boolean, and string fields become JSON values; bytes become base64 and nested messages become metadata with base64 content. Missing fields are not filled with protobuf defaults, and float/double fields produce `unsupported_wire_type` placeholders. Without a descriptor, the default decoder returns an Any envelope's `type_url` and base64 `value`.
- **Transforms**: `proto_convert` supports JSON-to-protobuf schema encoding for scalar fields, including floating-point numbers and numeric enum values; bytes and already-encoded nested messages use base64 strings. It logs and omits fields it cannot encode. Its protobuf-to-JSON path exposes Any metadata or raw-data metadata rather than decoding a custom message descriptor. Converting protobuf to `flat_buffer` or `avro` rewraps bytes without transcoding them.
- **Field Mapping**: Encoder/decoder mappings use protobuf field names as keys and JSON field names as values. The encoder applies that mapping in reverse. Transform mappings rename JSON input keys directly.
- **Any Wrapper**: The default encoder puts JSON/text in a `google.protobuf.StringValue`, or binary data in a `google.protobuf.BytesValue`, inside `google.protobuf.Any`. The default decoder exposes the envelope without unpacking its inner message.

### Programmatic Usage

From the matching repository root, create an example crate and schema directory:

```bash
mkdir -p connector-sdk-example/src schemas
```

Save this as `connector-sdk-example/Cargo.toml`. The path dependency uses the SDK from the same checkout as the runtime:

```toml
[package]
name = "connector-sdk-example"
version = "0.1.0"
edition = "2024"

[dependencies]
iggy_connector_sdk = { path = "../core/connectors/sdk" }
simd-json = { version = "0.18.1", features = ["serde_impl"] }

[workspace]
```

Save this as `schemas/user.proto`:

```protobuf
syntax = "proto3";
package com.example;

message User {
  uint64 id = 1;
  string name = 2;
}
```

Each Rust example below is a complete `connector-sdk-example/src/main.rs`. Run it from the repository root so the relative schema path resolves:

```bash
cargo run --manifest-path connector-sdk-example/Cargo.toml
```

#### Dynamic Schema Loading

You can load or reload schemas programmatically:

```rust
use iggy_connector_sdk::decoders::proto::{ProtoConfig, ProtoStreamDecoder};
use iggy_connector_sdk::encoders::proto::{ProtoEncoderConfig, ProtoStreamEncoder};
use iggy_connector_sdk::{Error, Payload, StreamDecoder, StreamEncoder};
use std::path::PathBuf;

fn main() -> Result<(), Error> {
    let mut decoder = ProtoStreamDecoder::new_default();
    decoder.update_config(
        ProtoConfig {
            schema_path: Some(PathBuf::from("schemas/user.proto")),
            message_type: Some("com.example.User".to_string()),
            ..ProtoConfig::default()
        },
        true,
    )?;
    let encoder = ProtoStreamEncoder::new_with_config(ProtoEncoderConfig {
        schema_path: Some(PathBuf::from("schemas/user.proto")),
        message_type: Some("com.example.User".to_string()),
        ..ProtoEncoderConfig::default()
    });
    let encoded = encoder.encode(Payload::Json(simd_json::json!({
        "id": 1,
        "name": "Alice"
    })))?;
    println!("{}", decoder.decode(encoded)?);
    Ok(())
}
```

The encoder follows the same pattern:

```rust
use iggy_connector_sdk::encoders::proto::{ProtoEncoderConfig, ProtoStreamEncoder};
use iggy_connector_sdk::{Error, Payload, StreamEncoder};
use std::path::PathBuf;

fn main() -> Result<(), Error> {
    let mut encoder = ProtoStreamEncoder::new_with_config(ProtoEncoderConfig {
        schema_path: Some(PathBuf::from("schemas/user.proto")),
        message_type: Some("com.example.User".to_string()),
        use_any_wrapper: false,
        ..ProtoEncoderConfig::default()
    });
    encoder.load_schema()?;
    let encoded = encoder.encode(Payload::Json(simd_json::json!({
        "id": 1,
        "name": "Alice"
    })))?;
    println!("{encoded:?}");
    Ok(())
}
```

#### Creating Converters with Schema

The loaded schema is used for JSON-to-protobuf conversion. This example maps `user_id` and `full_name` to the schema's field names:

```rust
use iggy_connector_sdk::transforms::{ProtoConvert, ProtoConvertConfig, Transform};
use iggy_connector_sdk::{DecodedMessage, Error, Payload, Schema, TopicMetadata};
use std::collections::HashMap;
use std::path::PathBuf;

fn main() -> Result<(), Error> {
    let converter = ProtoConvert::new(ProtoConvertConfig {
        source_format: Schema::Json,
        target_format: Schema::Proto,
        schema_path: Some(PathBuf::from("schemas/user.proto")),
        message_type: Some("com.example.User".to_string()),
        field_mappings: Some(HashMap::from([
            ("user_id".to_string(), "id".to_string()),
            ("full_name".to_string(), "name".to_string()),
        ])),
        ..ProtoConvertConfig::default()
    });
    let metadata = TopicMetadata {
        stream: "users".to_string(),
        topic: "users".to_string(),
    };
    let message = DecodedMessage {
        id: None,
        offset: None,
        checksum: None,
        timestamp: None,
        origin_timestamp: None,
        headers: None,
        payload: Payload::Json(simd_json::json!({
            "user_id": 1,
            "full_name": "Alice"
        })),
    };
    if let Some(converted) = converter.transform(&metadata, message)? {
        println!("{:?}", converted.payload);
    }
    Ok(())
}
```

### Usage Notes

- **Automatic Loading**: Constructors attempt to load `schema_path` or `descriptor_set`; `schema_path` takes precedence when both are set. Constructors log loading errors and return an instance without a loaded schema.
- **Manual Loading**: `load_schema()` reloads the configured source. A returned error preserves an already-loaded schema. `update_config(config, true)` also restores the previous configuration on error; `false` changes the configuration while retaining the cached schema.
- **Fallbacks**: Missing files, compilation failures, absent schema sources, or an unmatched `message_type` can return `Ok(())` without an active message descriptor. Successful reloads into fallback mode clear the previous descriptor. Invalid protobuf syntax and malformed descriptor bytes return errors. Check the actual encoded/decoded result when validating a schema setup.
- **Encoding Errors**: Errors encoding a loaded message descriptor are returned to the caller. The encoder does not retry that message as Any. The decoder attempts Any after a schema decoding error.
- **Transform Configuration**: Create a new converter to change its configuration. `load_schema()` can reload its existing source. Without a descriptor, JSON-to-protobuf conversion produces JSON text in `Payload::Proto`, not a schema-encoded binary message.
- **Format Options**: Encoder `preserve_unknown_fields`, `compact_encoding`, `validate_message`, and `deterministic_encoding` are accepted but have no effect. Decoder `preserve_unknown_fields` retains unknown varints as numbers and length-delimited data as base64; fixed-width unknown fields become placeholders. It does not retain the original wire encoding. See the [Transforms page](https://iggy.apache.org/docs/connectors/transforms) for conversion-specific limits.
- Protocol Buffers provide efficient binary serialization compared to JSON
