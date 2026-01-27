# An Example on Message Compression in Transit

This example illustrates how (with some minor additional implementation) the Iggy SDK can be used to compress and decompress messages in transit.

## Running the Example

Details on how to run the examples for the Rust Iggy SDK can be found in the parent folder [README.md](https://github.com/apache/iggy/tree/master/examples/rust#readme).

Run the following commands

1. Start the server

    ```bash
    cargo run --bin iggy-server -- --with-default-root-credentials
    ```

    **NOTE**: In case the server was running before, make sure to run `rm -rf local_data/` to delete server state data from prior runs.

2. Run the producer to write compressed messages to the server

    ```bash
    cargo run --example message-headers-compression-producer
    ```

3. Run the consumer to read and decompress messages from the server

    ```bash
    cargo run --example message-headers-compression-consumer
    ```

## The Codec

The **co**mpression and **dec**compression utilities are implemented in `examples/rust/src/shared/codec.rs` and used when sending messages to the server and reading them from the server.

First, define a stream and a topic name.
The producer will first initiate the stream and the topic on that stream and then write the example messages to that topic within that stream.
The consumer will use the names as identifier to read messages from that topic on that stream.

```rust
pub const STREAM_NAME: &str = "compression-stream";
pub const TOPIC_NAME: &str = "compression-topic";
```

Additionally, set a constant that defines the number of messages to be send to the server via the producer.

```rust
pub const NUM_MESSAGES: u32 = 1000;
```

### Spotlight: IggyMessage's

In order to add functionality to compress and decompress messages during transit, we need to know what a message actually is and how it is implemented.
Iggy implements two important types, that we need to know.

* [IggyMessage](https://github.com/apache/iggy/blob/e46f294b7af4f86b0d7e26d984205a019a8885f8/core/common/src/types/message/iggy_message.rs#L108)
* [ReceivedMessage](https://github.com/apache/iggy/blob/b26246252502ba6f5d6cad2895e7c468d9f959e4/core/sdk/src/clients/consumer.rs#L905)

A message send to the server needs to be of type `IggyMessage`.

```Rust
pub struct IggyMessage {
    /// Message metadata
    pub header: IggyMessageHeader,
    /// Message content
    pub payload: Bytes,
    /// Optional user-defined headers
    pub user_headers: Option<Bytes>,
}
```

The important bits in context of this example are the *payload* and the *user_headers*.
Payload is of type Bytes and corresponds to the actual message that we want to send to the server.

Let's suppose our example is an abstraction over a real world scenario, where some application sends it's application logs to the iggy-server. This application is therefore the producer.
We also have a monitoring service, that inspects the logs of our application to check for any service disruptions. So this monitoring service needs to read the logs from the iggy-server and is therefore the consumer.

Further suppose, the application logs are quite large and repetitive, since they follow a structured pattern (as logs usually do).
It may be a good idea to reduce bandwidth by trading of some idle CPU time to compress the logs before sending them to the server.
We go straight ahead, implement some compression functionalities and send the compressed messages to the server.
If the monitoring service now consumes these messages we have a problem. The logs are still compressed.
Even if we know that the messages are compressed we do not know how to decompress them because the algorithm that was used for compression is unknown.

This is where `user_headers` become handy.
The definition above tells us, that user_headers are (optional) Bytes. But thats because finally everything is serialized before sending to the server.
Looking at the implementation of `IggyMessage` we see that user_headers are a serialized `HashMap` with Iggy specific types `HeaderKey` and `HeaderValue`.
So the user_headers are basically a set of metadata defined by us, the user, using a key and a value.

Thus, for the compression scenario the user_headers can be used to signal to a consumer that a message was compressed before it was sent to the server.
The key to highlight message compression in this example is defined as:

```rust
pub const COMPRESSION_HEADER_KEY: &str = "iggy-compression";
```

By reading the user_headers and finding the "iggy-compression" key a consumer now knows, that the message was compressed. But it's still not transparent how it can be decompressed.
We can use the `HeaderValue` to store the information on how to decompress the message, e.g. using a **Codec**.

----

So the HeaderKey is used to indicate that a message was compressed before transit and the HeaderValue indicates how it was compressed.
Using this idea we can implement a Codec that is shared between the consumer and producer.

The Codec is an enum listing all the available algorithms to compress and decompress.

```rust
pub enum Codec {
    None,
    Lz4,
}
```

Further, we implement three methods

* `header_key`: Returns the HeaderKey that defines if a message was compressed or not.
The consumer uses it to look for the "iggy-compression" HeaderKey when inspecting the user_headers of a message.
* `to_header_value`: Generates a HeaderValue from the specific Codec instance.
* `from_header_value`: Resolves a HeaderValue into a Codec.
This is used in the consumer. After the "iggy-compression" HeaderKey was found in the user_headers we can obtain the HeaderValue, from which
we obtain the Codec type using this method and thereby gain access to the decompress method.

```rust
impl Codec {
    pub fn header_key() -> HeaderKey {
        HeaderKey::new(COMPRESSION_HEADER_KEY)
            .expect("COMPRESSION_HEADER_KEY is an InvalidHeaderKey.")
    }

    pub fn to_header_value(&self) -> HeaderValue {
        HeaderValue::from_str(&self.to_string()).expect("failed generating HeaderValue.")
    }

    pub fn from_header_value(value: &HeaderValue) -> Self {
        let name = value
            .as_str()
            .expect("could not convert HeaderValue into str.");
        Self::from_str(name).expect("compression algorithm not available.")
    }
```

The other two methods implement the compression and decompression logic, which is specifc to the actual Codec instance, dependent on the enum's variant.
The example Codec implements two. *None*, where data is not compressed and *Lz4* (using the lz4_flex crate).
Note, that this can be easily extended to more algorithms.
It might be reasonable to limit the number of bytes that can be decompressed to avoid large memory footprints, or even crashing the consumer.
The `decompress` method, therefore takes one more byte as defined by the `MAX_PAYLOAD_SIZE` which is [64MB](https://github.com/apache/iggy/blob/05243138255349a78bd1e086a0d7fb264682f980/core/common/src/types/message/iggy_message.rs#L46).
If the decoder read the full `MAX_PAYLOAD_SIZE` + 1 bytes, the payload exceeds the limit and the program panics.
Note, that only the Lz4 branch in the match statement applies this logic.
This is safe, because an `IggyMessage` ensures that the payload does not exceed `MAX_PAYLOAD_SIZE`, when using the builder.
A compressed message that meets the limit of `MAX_PAYLOAD_SIZE`, however, can decompress into much more bytes.
In a productive environment panics would be replaced with informative errors that can be properly handled.
You would most likely want to continue reading messages from the server, even if one of them exceeds the limit.

```rust
impl Codec {
    pub fn compress(&self, data: &[u8]) -> Vec<u8> {
        match self {
            Codec::None => Ok(data.to_vec()),
            Codec::Lz4 => {
                let mut compressed_data = Vec::new();
                let mut encoder = FrameEncoder::new(&mut compressed_data);
                encoder
                    .write_all(data)
                    .expect("Cannot write into buffer using Lz4 compression.");
                encoder.finish().expect("Cannot finish Lz4 compression.");
                Ok(compressed_data)
            }
        }
    }

    pub fn decompress(&self, data: &[u8]) -> Vec<u8> {
        match self {
            Codec::None => data.to_vec(),
            Codec::Lz4 => {
                let decoder = FrameDecoder::new(data);
                let mut decompressed_data = Vec::new();
                let bytes_read = decoder
                    .take(MAX_PAYLOAD_SIZE as u64 + 1)
                    .read_to_end(&mut decompressed_data)
                    .expect("Cannot decode payload using Lz4.");

                if bytes_read > MAX_PAYLOAD_SIZE as usize {
                    panic!("Decompressed message exceeds MAX_PAYLOAD_SIZE!")
                }
                decompressed_data
            }
        }
    }
}
```

## The producer

The example `/producer/main.rs` sets up a basic client that connects via TCP to a running iggy-server.
Since the plain server on start-up does not have any, it creates a stream and a topic to which it writes the compressed messages.

This is how the Codec described above is used to setup the user_headers entry to signal message compression.

```rust
let codec = Codec::Lz4;
let key = Codec::header_key();
let value = codec.to_header_value();
let compression_headers = HashMap::from([(key, value)]);
```

The builder interface for the IggyMessage type is used to construct a message.
Using the `user_headers` method sets the user_headers of that `IggyMessage`.
Note, that this method sets or overwrites the user_headers of that message with the provided HashMap.
Extending an existing header would require using the `user_headers_map` method and appending the returned HashMap.

```rust
let msg = IggyMessage::builder()
    .payload(compressed_bytes)
    .user_headers(compression_headers.clone())
    .build()
    .expect("IggyMessage should be buildable.");
```

Once all messages are generated they are send to the server.

## The consumer

The example `/consumer/main.rs` requires, that `../message-compression/producer/main.rs` was executed before.
It sets up a client that connects via TCP to a running iggy-server and reads messages from the same stream and topic that
was used by the producer.

The core piece of the consumer is the while loop which awaits messages from the stream and topic.
Note, the example terminates once the `NUM_MESSAGES` compressed messages were consumed.

```rust
while let Some(message) = consumer.next().await {
    // message handling
}
```

Within that loop we make use of the handle_payload_compression method.
Every `ReceivedMessage` is processed by that method.
A `ReceivedMessage` is a type that has an `IggyMessage` and two additional fields (which do not concern us here).

```rust
pub struct ReceivedMessage {
    pub message: IggyMessage,
    pub current_offset: u64,
    pub partition_id: u32,
}

```

The method decompresses the message payload.
It first checks if the "iggy-compression" key is present in the `user_headers` of the `IggyMessage` that would indicate that the message is compressed.
If that is not the case, the function returns `Ok(())` right away. In that case there is nothing to do.
For the case where the "iggy-compression" key is present in the user_headers, a Codec is setup
from the algorithm that was used to compress the message. Note, that .get_user_header takes the "iggy-compression" key and if it is present
returns the HeaderValue, which is the algorithm as we have defined above.
So at that point, codec is Codec::Lz4.
The next step then is to decompress the payload and update the payload length attribute of the `IggyMessage` metadata since it changed.
In a next and final step we update the user_headers. Since the message is now decompressed, the user_headers entry that signals compression should be removed.
If the compression key-value pair was the only user header the user_headers are set to None, otherwise we just remove the compression key-value pair from the HashMap.

```rust
fn handle_payload_compression(msg: &mut ReceivedMessage) -> Result<(), IggyError> {
    if let Ok(Some(algorithm)) = msg.message.get_user_header(&Codec::header_key()) {
        let codec = Codec::from_header_value(&algorithm);

        let decompressed_payload = codec.decompress(&msg.message.payload)?;
        msg.message.payload = Bytes::from(decompressed_payload);
        msg.message.header.payload_length = msg.message.payload.len() as u32;

        if let Ok(Some(mut headers_map)) = msg.message.user_headers_map() {
            headers_map.remove(&Codec::header_key());
            let headers_bytes = headers_map.to_bytes();
            msg.message.header.user_headers_length = headers_bytes.len() as u32;
            msg.message.user_headers = if headers_map.is_empty() {
                None
            } else {
                Some(headers_bytes)
            };
        }
    }
    Ok(())
}
```

When executing the program, the consumed and decompressed messages will be printed to console.
