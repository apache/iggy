# Apache Iggy Connectors - SDK

SDK provides the commonly used structs and traits such as `Sink` and `Source`, along with the `sink_connector!` and `source_connector!` macros to be used when developing connectors.

The macros automatically export the connector's version (from `CARGO_PKG_VERSION`) via FFI, allowing the runtime to report per-connector version information in the `/stats` endpoint.

## Source delivery acknowledgment

Source connectors use a one-in-flight-batch contract between the plugin and the runtime:

1. `Source::poll()` returns messages and candidate state without committing cursor changes or destructive operations.
2. The runtime sends the batch to Iggy and waits for the producer result.
3. After a successful send, the runtime persists the candidate state.
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
| After an in-memory confirmation and source cleanup, but before Iggy fsyncs | A server crash can lose the batch after source cleanup unless server-side `enforce_fsync` is enabled. |

An ACK means that Iggy confirmed the batch in memory; durability depends on the server's `enforce_fsync` setting. Source-side ACK work should be idempotent because process termination can interrupt it. NACK handling must discard staged cursor changes and staged delete or mark operations so polling can redeliver the batch. The SDK retries NACKed batches with capped exponential backoff and stops after repeated NACKs.

The default `Source::on_batch_result()` implementation is a no-op for sources without staged work. Sources that advance cursors, delete rows, or mark rows must override it. The SDK stops polling if the handler returns an error, preventing a failed rollback from advancing to another batch.

This contract is a breaking FFI change. Source plugins must be rebuilt with the matching SDK. The runtime loads `iggy_source_handle_v2`, which supplies a batch ID to the runtime callback, and source plugins export `iggy_source_batch_result` for the corresponding ACK or NACK.

Moreover, it contains both, the `decoders` and `encoders` modules, implementing either `StreamDecoder` or `StreamEncoder` traits, which are used when consuming or producing data from/to Iggy streams.

SDK is WiP, and it'd certainly benefit from having the support of multiple format schemas, such as Protobuf, Avro, Flatbuffers etc. including decoding/encoding the data between the different formats (when applicable) and supporting the data transformations whenever possible (easy for JSON, but complex for Bincode for example).

Last but not least, the different `transforms` are available, to transform (add, update, delete etc.) the particular fields of the data being processed via external configuration. It's as simple as adding a new transform to the `transforms` section of the particular connector configuration file:

```toml
[transforms.add_fields]
enabled = true

[[transforms.add_fields.fields]]
key = "message"
value.static = "hello"
```

## Protocol Buffers Support

The SDK includes support for Protocol Buffers (protobuf) format with both encoding and decoding capabilities. Protocol Buffers provide efficient serialization and are particularly useful for high-performance data streaming scenarios.

### Configuration Example

Here's a complete example configuration for using Protocol Buffers with Iggy connectors.

**Main runtime config (config.toml):**

```toml
[iggy]
address = "localhost:8090"
username = "iggy"
password = "iggy"

[connectors]
config_type = "local"
config_dir = "path/to/connectors"
```

**Source connector config (connectors/protobuf_source.toml):**

```toml
type = "source"
key = "protobuf"
enabled = true
version = 0
name = "Protobuf Source"
path = "target/release/libiggy_connector_protobuf_source"

[[streams]]
stream = "protobuf_stream"
topic = "protobuf_topic"
schema = "proto"
batch_size = 1000
send_interval = "5ms"

[plugin_config]
schema_path = "schemas/message.proto"
message_type = "com.example.Message"
use_any_wrapper = true
```

**Sink connector config (connectors/protobuf_sink.toml):**

```toml
type = "sink"
key = "protobuf"
enabled = true
version = 0
name = "Protobuf Sink"
path = "target/release/libiggy_connector_protobuf_sink"

[[streams]]
stream = "protobuf_stream"
topic = "protobuf_topic"
schema = "proto"

[[transforms]]
type = "proto_convert"
target_format = "json"
preserve_structure = true

field_mappings = { "old_field" = "new_field", "legacy_id" = "id" }

[[transforms]]
type = "proto_convert"
target_format = "proto"
preserve_structure = false
```

### Key Configuration Options

#### Source Configuration

- **`schema_path`**: Path to the `.proto` file containing message definitions
- **`message_type`**: Fully qualified name of the protobuf message type to use
- **`use_any_wrapper`**: Whether to wrap messages in `google.protobuf.Any` for type safety

#### Transform Options

- **`proto_convert`**: Transform for converting between protobuf and other formats
  - **`target_format`**: Target format for conversion (`json`, `proto`, `text`)
  - **`preserve_structure`**: Whether to preserve the original message structure during conversion
  - **`field_mappings`**: Mapping of field names for transformation (e.g., `"old_field" = "new_field"`)
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

- **Encoding**: Convert JSON, Text, and Raw data to protobuf format
- **Decoding**: Parse protobuf messages into JSON format with type information
- **Transforms**: Convert between protobuf and other formats (JSON, Text)
- **Field Mapping**: Transform field names during format conversion
- **Any Wrapper**: Support for `google.protobuf.Any` message wrapper

### Programmatic Usage

#### Dynamic Schema Loading

You can load or reload schemas programmatically:

```rust
use iggy_connector_sdk::decoders::proto::{ProtoStreamDecoder, ProtoConfig};
use std::path::PathBuf;

let mut decoder = ProtoStreamDecoder::new(ProtoConfig {
    schema_path: None,
    use_any_wrapper: true,
    ..Default::default()
});

let config_with_schema = ProtoConfig {
    schema_path: Some(PathBuf::from("schemas/user.proto")),
    message_type: Some("com.example.User".to_string()),
    ..Default::default()
};

match decoder.update_config(config_with_schema, true) {
    Ok(()) => println!("Schema loaded successfully"),
    Err(e) => eprintln!("Failed to load schema: {}", e),
}
```

#### Schema Registry Integration

```rust
use iggy_connector_sdk::encoders::proto::{ProtoStreamEncoder, ProtoEncoderConfig};

let mut encoder = ProtoStreamEncoder::new_with_config(ProtoEncoderConfig {
    schema_registry_url: Some("http://schema-registry:8081".to_string()),
    message_type: Some("com.example.Event".to_string()),
    use_any_wrapper: false,
    ..Default::default()
});

if let Err(e) = encoder.load_schema() {
    eprintln!("Schema reload failed: {}", e);
}
```

#### Creating Converters with Schema

```rust
use iggy_connector_sdk::transforms::proto_convert::{ProtoConvert, ProtoConvertConfig};
use iggy_connector_sdk::Schema;
use std::collections::HashMap;
use std::path::PathBuf;

let converter = ProtoConvert::new(ProtoConvertConfig {
    source_format: Schema::Proto,
    target_format: Schema::Json,
    schema_path: Some(PathBuf::from("schemas/user.proto")),
    message_type: Some("com.example.User".to_string()),
    field_mappings: Some(HashMap::from([
        ("user_id".to_string(), "id".to_string()),
        ("full_name".to_string(), "name".to_string()),
    ])),
    ..ProtoConvertConfig::default()
});

let mut converter_with_manual_loading = ProtoConvert::new(ProtoConvertConfig::default());
if let Err(e) = converter_with_manual_loading.load_schema() {
    eprintln!("Manual schema loading failed: {}", e);
}
```

### Usage Notes

- **Automatic Loading**: Schemas are loaded automatically when `schema_path` or `descriptor_set` is provided in config
- **Manual Loading**: Use `load_schema()` method for dynamic schema loading or reloading
- **Error Handling**: Schema loading errors are handled gracefully with fallback to Any wrapper mode
- **Immutable Design**: Converters are created with fixed configuration - create new instances for different schemas
- When `use_any_wrapper` is enabled, messages are wrapped in `google.protobuf.Any` for better type safety
- The `proto_convert` transform can be used to convert protobuf messages to JSON for easier processing
- Field mappings allow you to rename fields during format conversion
- Protocol Buffers provide efficient binary serialization compared to JSON
