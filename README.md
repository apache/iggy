# Apache Iggy (Incubating)

<div style="display: flex; flex-wrap: wrap; justify-content: center; align-items: center; text-align: center;">

  [Website](https://iggy.apache.org) | [Getting started](https://iggy.apache.org/docs/introduction/getting-started/) | [Documentation](https://iggy.apache.org/docs/) | [Blog](https://iggy.apache.org/blogs/) | [Discord](https://discord.gg/C5Sux5NcRa) | [Crates](https://crates.io/crates/iggy)

</div>
<div style="display: flex; flex-wrap: wrap; justify-content: center; align-items: center; text-align: center;">

  [![crates.io](https://img.shields.io/crates/v/iggy.svg)](https://crates.io/crates/iggy)
  [![crates.io](https://img.shields.io/crates/d/iggy.svg)](https://crates.io/crates/iggy)
  [![docs](https://docs.rs/iggy/badge.svg)](https://docs.rs/iggy)
  [![workflow](https://github.com/iggy-rs/iggy/actions/workflows/test.yml/badge.svg)](https://github.com/iggy-rs/iggy/actions/workflows/test.yml)
  [![coverage](https://coveralls.io/repos/github/iggy-rs/iggy/badge.svg?branch=master)](https://coveralls.io/github/iggy-rs/iggy?branch=master)
  [![dependency](https://deps.rs/repo/github/iggy-rs/iggy/status.svg)](https://deps.rs/repo/github/iggy-rs/iggy)
  [![x](https://img.shields.io/twitter/follow/iggy_rs_?style=social)](https://twitter.com/iggy_rs_)
   [![discord-badge](https://img.shields.io/discord/1144142576266530928)](https://discord.gg/C5Sux5NcRa)

</div>

---

<div align="center">

  ![iggy](assets/iggy_black.png)

</div>

---

**Iggy** is the persistent message streaming platform written in Rust, supporting [QUIC](https://www.chromium.org/quic/), TCP (custom binary specification) and HTTP (regular REST API) transport protocols. Currently, running as a single server, it allows creating streams, topics, partitions and segments, and send/receive messages to/from them. The **messages are stored on disk as an append-only log**, and are persisted between restarts.

The goal of the project is to make a distributed streaming platform (running as a cluster), which will be able to scale horizontally and handle **millions of messages per second** (actually, **it's already very fast**, see the benchmarks below).

Iggy provides **exceptionally high throughput and performance** while utilizing minimal computing resources.

This is **not yet another extension** running on top of the existing infrastructure, such as Kafka or SQL database.

Iggy is the persistent message streaming log **built from the ground up** using the low lvl I/O for speed and efficiency.

The name is an abbreviation for the Italian Greyhound - small yet extremely fast dogs, the best in their class. Just like mine lovely [Fabio & Cookie](https://www.instagram.com/fabio.and.cookie/) ❤️

---

## Features

- **Highly performant**, persistent append-only log for the message streaming
- **Very high throughput** for both writes and reads
- **Low latency and predictable resource usage** thanks to the Rust compiled language (no GC)
- **Users authentication and authorization** with granular permissions and PAT (Personal Access Tokens)
- Support for multiple streams, topics and partitions
- Support for **multiple transport protocols** (QUIC, TCP, HTTP)
- Fully operational RESTful API which can be optionally enabled
- Available client SDK in multiple languages
- **Works directly with the binary data** (lack of enforced schema and serialization/deserialization)
- Configurable server features (e.g. caching, segment size, data flush interval, transport protocols etc.)
- Possibility of storing the **consumer offsets** on the server
- Multiple ways of polling the messages:
  - By offset (using the indexes)
  - By timestamp (using the time indexes)
  - First/Last N messages
  - Next N messages for the specific consumer
- Possibility of **auto committing the offset** (e.g. to achieve *at-most-once* delivery)
- **Consumer groups** providing the message ordering and horizontal scaling across the connected clients
- **Message expiry** with auto deletion based on the configurable **retention policy**
- Additional features such as **server side message deduplication**
- **Multi-tenant** support via abstraction of **streams** whch group **topics**
- **TLS** support for all transport protocols (TCP, QUIC, HTTPS)
- Optional server-side as well as client-side **data encryption** using AES-256-GCM
- Optional metadata support in the form of **message headers**
- Optional **data backups & archivization** on disk and/or the **S3** compatible cloud storage (e.g. AWS S3)
- Support for **[OpenTelemetry](https://opentelemetry.io/)** logs & traces + Prometheus metrics
- Built-in **CLI** to manage the streaming server installable via `cargo install iggy-cli`
- Built-in **benchmarking app** to test the performance
- **Single binary deployment** (no external dependencies)
- Running as a single node (no cluster support yet)

---

## Roadmap

- Low level optimizations such zero-copy deserialization with [rkyv](https://github.com/rkyv/rkyv) (WiP)
- Advanced Web UI (WiP)
- Shared-nothing design and io_uring support (on experimental branch)
- Clustering & data replication (on sandbox project)
- Developer friendly SDK supporting multiple languages
- Plugins & extensions support

---

## Supported languages SDK (work in progress)

- [Rust](https://crates.io/crates/iggy)
- [C#](https://github.com/iggy-rs/iggy-dotnet-client)
- [Go](https://github.com/iggy-rs/iggy-go-client)
- [Node](https://github.com/iggy-rs/iggy-node-client)
- [Python](https://github.com/iggy-rs/iggy-python-client)
- [Java](https://github.com/iggy-rs/iggy-java-client)
- [C++](https://github.com/iggy-rs/iggy-cpp-client)
- [Elixir](https://github.com/iggy-rs/iggy-elixir-client)

---

## CLI

The brand new, rich, interactive CLI is implemented under the `cli` project, to provide the best developer experience. This is a great addition to the Web UI, especially for all the developers who prefer using the console tools.

Iggy CLI can be installed with `cargo install iggy-cli` and then simply accessed by typing `iggy` in your terminal.

![CLI](assets/cli.png)

## Web UI

There's an ongoing effort to build the administrative web UI for the server, which will allow to manage the streams, topics, partitions, messages and so on. Check the [Web UI repository](https://github.com/iggy-rs/iggy-web-ui)

![Web UI](assets/web_ui.png)

---

## Docker

You can find the `Dockerfile` and `docker-compose` in the root of the repository. To build and start the server, run: `docker compose up`.

Additionally, you can run the `CLI` which is available in the running container, by executing: `docker exec -it iggy-server /iggy`.

Keep in mind that running the container on the OS other than Linux, where the Docker is running in the VM, might result in the significant performance degradation.

The official images can be found [here](https://hub.docker.com/r/iggyrs/iggy), simply type `docker pull iggyrs/iggy`.

---

## Configuration

The default configuration can be found in `server.toml` file in `configs` directory.

The configuration file is loaded from the current working directory, but you can specify the path to the configuration file by setting `IGGY_CONFIG_PATH` environment variable, for example `export IGGY_CONFIG_PATH=configs/server.toml` (or other command depending on OS).

When config file is not found, the default values from embedded server.toml file are used.

For the detailed documentation of the configuration file, please refer to the [configuration](https://iggy.apache.org/docs//server/configuration) section.

---

## Quick start

Build the project (the longer compilation time is due to [LTO](https://doc.rust-lang.org/rustc/linker-plugin-lto.html) enabled in release [profile](https://github.com/spetz/iggy/blob/master/Cargo.toml#L2)):

`cargo build`

Run the tests:

`cargo test`

Start the server:

`cargo r --bin iggy-server`

*Please note that all commands below are using `iggy` binary, which is part of release (`cli` sub-crate).*

Create a stream with name `dev` (numerical ID will be assigned by server automatically) using default credentials and `tcp` transport (available transports: `quic`, `tcp`, `http`, default `tcp`):

`cargo r --bin iggy -- --transport tcp --username iggy --password iggy stream create dev`

List available streams:

`cargo r --bin iggy -- --username iggy --password iggy stream list`

Get `dev` stream details:

`cargo r --bin iggy -- -u iggy -p iggy stream get dev`

Create a topic named `sample` (numerical ID will be assigned by server automatically) for stream `dev`, with 2 partitions (IDs 1 and 2), disabled compression (`none`) and disabled message expiry (skipped optional parameter):

`cargo r --bin iggy -- -u iggy -p iggy topic create dev sample 2 none`

List available topics for stream `dev`:

`cargo r --bin iggy -- -u iggy -p iggy topic list dev`

Get topic details for topic `sample` in stream `dev`:

`cargo r --bin iggy -- -u iggy -p iggy topic get dev sample`

Send a message 'hello world' (message ID 1) to the stream `dev` to topic `sample` and partition 1:

`cargo r --bin iggy -- -u iggy -p iggy message send --partition-id 1 dev sample "hello world"`

Send another message 'lorem ipsum' (message ID 2) to the same stream, topic and partition:

`cargo r --bin iggy -- -u iggy -p iggy message send --partition-id 1 dev sample "lorem ipsum"`

Poll messages by a regular consumer with ID 1 from the stream `dev` for topic `sample` and partition with ID 1, starting with offset 0, messages count 2, without auto commit (storing consumer offset on server):

`cargo r --bin iggy -- -u iggy -p iggy message poll --consumer 1 --offset 0 --message-count 2 --auto-commit dev sample 1`

Finally, restart the server to see it is able to load the persisted data.

The HTTP API endpoints can be found in [server.http](https://github.com/spetz/iggy/blob/master/server/server.http) file, which can be used with [REST Client](https://marketplace.visualstudio.com/items?itemName=humao.rest-client) extension for VS Code.

To see the detailed logs from the CLI/server, run it with `RUST_LOG=trace` environment variable. See images below:

![files structure](assets/files_structure.png)*Files structure*

![server](assets/server.png)*Server*

---

## Examples

You can find the sample consumer & producer applications under `examples` directory. The purpose of these apps is to showcase the usage of the client SDK. To find out more about building the applications, please refer to the [getting started](https://iggy.apache.org/docs/introduction/getting-started) guide.

To run the example, first start the server with `cargo r --bin iggy-server` and then run the producer and consumer apps with `cargo r --example message-envelope-producer` and `cargo r --example message-envelope-consumer` respectively.

You might start multiple producers and consumers at the same time to see how the messages are being handled across multiple clients. Check the [Args](https://github.com/spetz/iggy/blob/master/examples/src/shared/args.rs) struct to see the available options, such as the transport protocol, stream, topic, partition, consumer ID, message size etc.

By default, the consumer will poll the messages using the `next` available offset with auto commit enabled, to store its offset on the server. With this approach, you can easily achieve *at-most-once* delivery.

![sample](assets/sample.png)

---

## SDK

Iggy comes with the Rust SDK, which is available on [crates.io](https://crates.io/crates/iggy).

The SDK provides both, low-level client for the specific transport, which includes the message sending and polling along with all the administrative actions such as managing the streams, topics, users etc., as well as the high-level client, which abstracts the low-level details and provides the easy-to-use API for both, message producers and consumers.

You can find the more examples, including the multi-tenant one under the `examples` directory.

```rust
// Create the Iggy client
let client = IggyClient::from_connection_string("iggy://user:secret@localhost:8090")?;

// Create a producer for the given stream and one of its topics
let mut producer = client
    .producer("dev01", "events")?
    .batch_size(1000)
    .send_interval(IggyDuration::from_str("1ms")?)
    .partitioning(Partitioning::balanced())
    .build();

producer.init().await?;

// Send some messages to the topic
let messages = vec![Message::from_str("Hello Apache Iggy")?];
producer.send(messages).await?;

// Create a consumer for the given stream and one of its topics
let mut consumer = client
    .consumer_group("my_app", "dev01", "events")?
    .auto_commit(AutoCommit::IntervalOrWhen(
        IggyDuration::from_str("1s")?,
        AutoCommitWhen::ConsumingAllMessages,
    ))
    .create_consumer_group_if_not_exists()
    .auto_join_consumer_group()
    .polling_strategy(PollingStrategy::next())
    .poll_interval(IggyDuration::from_str("1ms")?)
    .batch_size(1000)
    .build();

consumer.init().await?;

// Start consuming the messages
while let Some(message) = consumer.next().await {
    // Handle the message
}
```

---

## Benchmarks

To benchmark the project, first build the project in release mode:

```bash
cargo build --release
```

Then, run the benchmarking app with the desired options:

1. Sending (writing) benchmark

   ```bash
   cargo r --bin iggy-bench -r -- -v pinned-producer tcp
   ```

2. Polling (reading) benchmark

   ```bash
   cargo r --bin iggy-bench -r -- -v pinned-consumer tcp
   ```

3. Parallel sending and polling benchmark

   ```bash
   cargo r --bin iggy-bench -r -- -v pinned-producer-and-consumer tcp
   ```

4. Balanced sending to multiple partitions benchmark

   ```bash
   cargo r --bin iggy-bench -r -- -v balanced-producer tcp
   ```

5. Consumer group polling benchmark:

   ```bash
   cargo r --bin iggy-bench -r -- -v balanced-consumer-group tcp
   ```

6. Parallel balanced sending and polling from consumer group benchmark:

   ```bash
   cargo r --bin iggy-bench -r -- -v balanced-producer-and-consumer-group tcp
   ```

7. End to end producing and consuming benchmark (single task produces and consumes messages in sequence):

   ```bash
   cargo r --bin iggy-bench -r -- -v end-to-end-producing-consumer tcp
   ```

These benchmarks would start the server with the default configuration, create a stream, topic and partition, and then send or poll the messages. The default configuration is optimized for the best performance, so you might want to tweak it for your needs. If you need more options, please refer to `iggy-bench` subcommands `help` and `examples`.
For example, to run the benchmark for the already started server, provide the additional argument `--server-address 0.0.0.0:8090`.

Depending on the hardware, transport protocol (`quic`, `tcp` or `http`) and payload size (`messages-per-batch * message-size`) you might expect **over 5000 MB/s (e.g. 5M of 1 KB msg/sec) throughput for writes and reads**. These results have been achieved on Ryzen 9 7950X with 64 GB RAM and gen 4 NVMe SSD.

---
