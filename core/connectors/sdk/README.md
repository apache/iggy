# Apache Iggy Connectors - SDK

SDK provides the commonly used structs and traits such as `Sink` and `Source`, along with the `sink_connector` and `source_connector` macros to be used when developing connectors.

Moreover, it contains both, the `decoders` and `encoders` modules, implementing either `StreamDecoder` or `StreamEncoder` traits, which are used when consuming or producing data from/to Iggy streams.

SDK is WiP, and it'd certainly benefit from having the support of multiple format schemas, such as Protobuf, Avro, Flatbuffers etc. including decoding/encoding the data between the different formats (when applicable) and supporting the data transformations whenever possible (easy for JSON, but complex for Bincode for example).

Last but not least, the different `transforms` are available, to transform (add, update, delete etc.) the particular fields of the data being processed via external configuration. It's as simple as adding a new transform to the `transforms` section of the particular connector configuration:

```toml
[sources.random.transforms.add_fields]
enabled = true

[[sources.random.transforms.add_fields.fields]]
key = "message"
value.static = "hello"
```

## Protocol Buffers Support

The SDK includes support for Protocol Buffers (protobuf) format with both encoding and decoding capabilities. Protocol Buffers provide efficient serialization and are particularly useful for high-performance data streaming scenarios.

### Configuration Example

Here's a complete example configuration for using Protocol Buffers with Iggy connectors:

```toml
[iggy]
address = "localhost:8090"
username = "iggy"
password = "iggy"

[sources.protobuf_source]
enabled = true
name = "Protobuf Source"
path = "target/release/libiggy_connector_protobuf_source"

[[sources.protobuf_source.streams]]
stream = "protobuf_stream"
topic = "protobuf_topic"
schema = "proto"
batch_size = 1000
send_interval = "5ms"

[sources.protobuf_source.config]
schema_path = "schemas/message.proto"
message_type = "com.example.Message"
use_any_wrapper = true

[sinks.protobuf_sink]
enabled = true
name = "Protobuf Sink"
path = "target/release/libiggy_connector_protobuf_sink"

[[sinks.protobuf_sink.streams]]
stream = "protobuf_stream"
topic = "protobuf_topic"
schema = "proto"

[[sinks.protobuf_sink.transforms]]
type = "proto_convert"
target_format = "json"
preserve_structure = true

field_mappings = { "old_field" = "new_field", "legacy_id" = "id" }

[[sinks.protobuf_sink.transforms]]
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

### Usage Notes

- When `use_any_wrapper` is enabled, messages are wrapped in `google.protobuf.Any` for better type safety
- The `proto_convert` transform can be used to convert protobuf messages to JSON for easier processing
- Field mappings allow you to rename fields during format conversion
- Protocol Buffers provide efficient binary serialization compared to JSON
