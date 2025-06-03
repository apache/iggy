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
