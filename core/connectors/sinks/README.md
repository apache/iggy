# Apache Iggy Connectors - Sink

## Overview

Sink connectors are responsible for writing data from Iggy streams to external systems or destinations. They provide a way to integrate Apache Iggy with various data sources and destinations, enabling seamless data flow and processing.

The sink is represented by the single `Sink` trait, which defines the basic interface for all sink connectors. It provides methods for initializing the sink, writing data to external destination, and closing the sink.

```rust
#[async_trait]
pub trait Sink: Send + Sync {
    /// Invoked when the sink is initialized, allowing it to perform any necessary setup.
    async fn open(&mut self) -> Result<(), Error>;

    /// Invoked every time a batch of messages is received from the configured stream(s) and topic(s).
    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error>;

    /// Invoked when the sink is closed, allowing it to perform any necessary cleanup.
    async fn close(&mut self) -> Result<(), Error>;
}
```

## Configuration

Each sink connector is configured in its own separate configuration file within the connectors directory specified in the main runtime config.

```rust
pub struct SinkConfig {
    pub key: String,
    pub enabled: bool,
    pub version: u64,
    pub name: String,
    pub path: String,
    pub transforms: Option<TransformsConfig>,
    pub streams: Vec<StreamConsumerConfig>,
    pub plugin_config_format: Option<ConfigFormat>,
    pub plugin_config: Option<serde_json::Value>,
}
```

**Main runtime config (config.toml):**

```toml
[connectors]
config_type = "local"
config_dir = "path/to/connectors"
```

**Sink connector config (connectors/stdout.toml):**

```toml
# Type of connector (sink or source)
type = "sink"
key = "stdout" # Unique sink ID

# Required configuration for a sink connector
enabled = true
version = 0
name = "Stdout sink"
path = "target/release/libiggy_connector_stdout_sink"
plugin_config_format = "toml"

# Collection of the streams from which messages are consumed
[[streams]]
stream = "example_stream"
topics = ["example_topic"]
schema = "json"
batch_length = 100
poll_interval = "5ms"
consumer_group = "stdout_sink_connector"

# Custom configuration for the sink connector, deserialized to type T from `plugin_config` field
[plugin_config]
print_payload = true

# Optional data transformation(s) to be applied after consuming messages from the stream
[transforms.add_fields]
enabled = true

# Collection of the fields transforms to be applied after consuming messages from the stream
[[transforms.add_fields.fields]]
key = "message"
value.static = "hello"
```

### Environment Variable Overrides

Configuration properties can be overridden using environment variables. The pattern follows: `IGGY_CONNECTORS_SINK_[KEY]_[PROPERTY]`

For example, to override the `enabled` property for a sink with ID `stdout`:

```bash
IGGY_CONNECTORS_SINK_STDOUT_ENABLED=false
```

## Sample implementation

Let's implement the example sink connector, which will simply print the messages to the standard output.

Additionally, our sink connector will have its own state, which can be used e.g. to track the overall progress or store some relevant information when ingesting the data further to the external sources or tooling.

Also, when implementing the sink connector, make sure to use the `sink_connector!` macro to expose the FFI interface and allow the connector runtime to register the sink with the runtime.

And finally, each sink should have its own, custom configuration, which is passed along with the unique plugin ID via expected `new()` method.

Let's start by defining the internal state and the public sink connector along with its own configuration.

```rust
#[derive(Debug)]
struct State {
    invocations_count: usize,
}
```

```rust
#[derive(Debug)]
pub struct StdoutSink {
    id: u32,
    print_payload: bool,
    state: Mutex<State>
}
```

```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct StdoutSinkConfig {
    print_payload: Option<bool>,
}
```

```rust
impl StdoutSink {
    pub fn new(id: u32, config: StdoutSinkConfig) -> Self {
        StdoutSink {
            id,
            print_payload: config.print_payload.unwrap_or(false),
            state: Mutex::new(State { invocations_count: 0 }),
        }
    }
}
```

We can invoke the expected macro to expose the FFI interface and allow the connector runtime to register the sink within the runtime.

```rust
sink_connector!(StdoutSink);
```

At a bare minimum, we need to add the following dependencies to the `Cargo.toml` file to compile the plugin at all:

- dashmap
- once_cell
- tracing

Now, let's implement the `Sink` trait for our `StdoutSink` struct.

```rust
#[async_trait]
impl Sink for StdoutSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opened stdout sink connector with ID: {}, print payload: {}",
            self.id, self.print_payload
        );
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let mut state = self.state.lock().await;
        state.invocations_count += 1;
        let invocation = state.invocations_count;
        drop(state);

        info!(
            "Stdout sink with ID: {} received: {} messages, schema: {}, stream: {}, topic: {}, partition: {}, offset: {}, invocation: {}",
            self.id,
            messages.len(),
            messages_metadata.schema,
            topic_metadata.stream,
            topic_metadata.topic,
            messages_metadata.partition_id,
            messages_metadata.current_offset,
            invocation
        );
        if self.print_payload {
            for message in messages {
                info!(
                    "Message offset: {}, payload: {:#?}",
                    message.offset, message.payload
                );
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Stdout sink connector with ID: {} is closed.", self.id);
        Ok(())
    }
}
```

It's also important to note, that the supported format(s) might vary depending on the connector implementation. For example, you might expect `JSON` as the payload format, which can be then easily parsed and processed by upstream components such as data transforms, but at the same time, you could support the other formats and let the user decide which one to use.

For example, you can match against the `payload` enum field containing the deserialized value to process (or not) the consumed message(s).

```rust
for message in messages {
    match message.payload {
        Payload::Json(value) =>  {
            // Process JSON payload
        }
        _ => {
            warn!("Unsupported payload format: {}", messages_metadata.schema);
        }
    }
}
```

While the schema of messages (that will be consumed from the Iggy stream), cannot be controlled by the sink connector itself, the built-in configuration allows to decide what's the expected format of the messages (the particular `StreamDecoder` will be used).

Keep in mind, that it might be sometimes difficult/impossible e.g. to transform one format to another e.g. JSON to SBE or so, and in such a case, the consumed messages will be ignored.

Eventually, compile the source code and create a separate connector configuration file in the connectors directory (as specified in the main runtime `config.toml`).  Make sure that `path` points to the existing plugin.

And that's all, enjoy using the sink connector!

On a side note, if you'd like to produce the messages to the Iggy stream instead, you can implement your own **[Source connector](https://github.com/apache/iggy/tree/master/core/connectors/sources)** too :)
