# Apache Iggy Connectors - SDK

SDK provides the commonly used structs and traits such as `Sink` and `Source`, along with the `sink_connector` and `source_connector` macros to be used when developing connectors.

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
