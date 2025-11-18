# Apache Iggy Connectors - Source

## Overview

Source connectors are responsible for ingesting data from external sources into Apache Iggy. They provide a way to integrate Apache Iggy with various data sources, such as databases, message queues, or file systems.

The source is represented by the single `Source` trait, which defines the basic interface for all source connectors. It provides methods for initializing the source, reading data from it, and closing the source.

```rust
#[async_trait]
pub trait Source: Send + Sync {
    /// Invoked when the source is initialized, allowing it to perform any necessary setup.
    async fn open(&mut self) -> Result<(), Error>;

    /// Invoked every time a batch of messages is produced to the configured stream and topic.
    async fn poll(&self) -> Result<ProducedMessages, Error>;

    /// Invoked when the source is closed, allowing it to perform any necessary cleanup.
    async fn close(&mut self) -> Result<(), Error>;
}
```

## Configuration

Each source connector is configured in its own separate configuration file within the connectors directory specified in the main runtime config.

```rust
pub struct SourceConfig {
    pub key: String,
    pub enabled: bool,
    pub version: u64,
    pub name: String,
    pub path: String,
    pub transforms: Option<TransformsConfig>,
    pub streams: Vec<StreamProducerConfig>,
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

**Source connector config (connectors/random.toml):**

```toml
# Type of connector (sink or source)
type = "source"
key = "random" # Unique source ID

# Required configuration for a source connector
enabled = true # Toggle source on/off
version = 0
name = "Random source" # Name of the source
path = "libiggy_connector_random_source" # Path to the source connector
config_format = "toml"

# Collection of the streams to which the produced messages are sent
[[streams]]
stream = "example_stream"
topic = "example_topic"
schema = "json"
batch_length = 100
linger_time = "5ms"

# Custom configuration for the source connector, deserialized to type T from `plugin_config` field
[plugin_config]
messages_count = 10

# Optional data transformation(s) to be applied before sending messages to the stream
[transforms.add_fields]
enabled = true

# Collection of the fields transforms to be applied before sending messages to the stream
[[transforms.add_fields.fields]]
key = "message"
value.static = "hello"
```

### Environment Variable Overrides

Configuration properties can be overridden using environment variables. The pattern follows: `IGGY_CONNECTORS_SOURCE_[KEY]_[PROPERTY]`

For example, to override the `enabled` property for a source with ID `random`:

```bash
IGGY_CONNECTORS_SOURCE_RANDOM_ENABLED=false
```

## Sample implementation

Let's implement the example source connector, which will simply generate the N random messages depending on the count specified in the configuration.

Additionally, our source connector will have its own state, which can be used e.g. to track the overall progress or store some relevant information when producing the data from the actual external sources or tooling.

Keep in mind, that the produced messages will be sent further to the specified stream, however it's already the responsibility of the runtime to handle the delivery.

Also, when implementing the source connector, make sure to use the `source_connector!` macro to expose the FFI interface and allow the connector runtime to register the source with the runtime.

And finally, each source should have its own, custom configuration, which is passed along with the unique plugin ID and optional state via expected `new()` method.

Let's start by defining the internal state and the public source connector along with its own configuration.

```rust
#[derive(Debug)]
struct State {
    current_id: usize,
}
```

```rust
#[derive(Debug)]
pub struct RandomSource {
    id: u32,
    messages_count: u32,
    state: Mutex<State>
}
```

```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct RandomSourceConfig {
    messages_count: Option<u32>,
}
```

At this point, we can expose the required `new()` method, which will be used by the runtime to create a new instance of the source connector. The `id` is assigned by the runtime, and represents the unique identifier of the source connector. The `state` is an optional connector state (e.g. persisted in the local file), which will be provided by the runtime, given that the connector has persisted its own state before the runtime was restarted.

```rust
impl RandomSource {
    pub fn new(id: u32, config: RandomSourceConfig, state: Option<ConnectorState>) -> Self {
        let current_id = if let Some(state) = state {
            u64::from_le_bytes(
                state.0[0..8]
                    .try_into()
                    .inspect_err(|error| {
                        error!("Failed to convert state to current ID. {error}");
                    })
                    .unwrap_or_default(),
            )
        } else {
            0
        } as usize;

        RandomSource {
            id,
            payload_size: config.payload_size.unwrap_or(100),
            state: Mutex::new(State { current_id }),
        }
    }
}
```

We can invoke the expected macro to expose the FFI interface and allow the connector runtime to register the source within the runtime.

```rust
source_connector!(TestSource);
```

At a bare minimum, we need to add the following dependencies to the `Cargo.toml` file to compile the plugin at all:

- dashmap
- once_cell
- tracing

Before we make use of the `Source` trait, let's define the internal payload of the message that will be produced (e.g. as if it was pulled from some external database or so).

```rust
#[derive(Debug, Serialize, Deserialize)]
struct Record {
    id: u64,
    text: String,
}
```

Now, let's implement the `Source` trait for our `RandomSource` struct. We'll assume that the amount of messages (provided in the config), will be generated every 100ms to mimic the behavior of a real-world external source. On top of this, we'll also keep track of the current ID of the last message produced and return the state along with the `ProducedMessages` - the state in this case, will be just a binary encoded number, but it can be anything else, including the complex structs.

```rust
#[async_trait]
impl Source for RandomSource {
    async fn open(&mut self) -> Result<(), iggy_connector_sdk::Error> {
        info!(
            "Opened random source connector with ID: {}, messages count: {}",
            self.id, self.messages_count
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, iggy_connector_sdk::Error> {
        sleep(Duration::from_millis(100)).await;
        let mut state = self.state.lock().await;
        let current_id = state.current_id;

        let mut messages = Vec::new();
        for _ in 0..self.messages_count {
            current_id += 1;
            let record = Record {
                id: current_id,
                text: format!("Hello from random source connector: #{current_id}")
            };
            let Ok(payload) = simd_json::to_vec(&record) else {
                error!(
                    "Failed to serialize record by random source connector with ID: {}",
                    self.id
                );
                continue;
            };

            let message = ProducedMessage {
                id: None,
                headers: None,
                checksum: None,
                timestamp: None,
                origin_timestamp: None,
                payload,
            };
            messages.push(message);
        }

        state.current_id += current_id;
        info!(
            "Generated {} messages by random source connector with ID: {}"
            messages.len(),
            self.id,
        );
        Ok(ProducedMessages {
            schema: Schema::Json,
            messages,
            state: Some(ConnectorState(state.current_id.to_le_bytes().to_vec())),
        })
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Random source connector with ID: {} is closed.", self.id);
        Ok(())
    }
}
```

As you can see, the `ProducedMessage` can be customized to fit your needs, as all the fields will be directly mapped to the existing Iggy message struct.

It's also important to note, that the supported format(s) might vary depending on the connector implementation. For example, you might use `JSON` as the payload format, which can be then easily parsed and processed by downstream components such as data transforms, but at the same time, you could support the other formats and let the user decide which one to use.

While the final schema of messages (that will be appended to the Iggy stream), can be controlled with the built-in configuration (the particular `StreamEncoder` will be used), keep in mind, that it might be sometimes difficult/impossible e.g. to transform one format to another e.g. JSON to SBE or so, and in such a case, the produced messages will be ignored.

Eventually, compile the source code and create a separate connector configuration file in the connectors directory (as specified in the main runtime `config.toml`). Make sure that `path` points to the existing plugin.

And before starting the runtime, do not forget to create the specified stream and topic e.g. via Iggy CLI.

```bash
iggy --username iggy --password iggy stream create example_stream

iggy --username iggy --password iggy topic create example_stream example_topic 1 none 1d
```

And that's all, enjoy using the source connector!

On a side note, if you'd like to process the messages consumed from the Iggy stream instead, you can implement your own **[Sink connector](https://github.com/apache/iggy/tree/master/core/connectors/sinks)** too :)
