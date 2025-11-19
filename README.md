# Apache Iggy (Incubating)

<div align="center">

[Website](https://iggy.apache.org) | [Getting started](https://iggy.apache.org/docs/introduction/getting-started/) | [Documentation](https://iggy.apache.org/docs/) | [Blog](https://iggy.apache.org/blogs/) | [Discord](https://discord.gg/C5Sux5NcRa) | [Crates](https://crates.io/crates/iggy)

</div>
<div style="display: flex; flex-wrap: wrap; justify-content: center; align-items: center; text-align: center;">

  [![crates.io](https://img.shields.io/crates/v/iggy.svg)](https://crates.io/crates/iggy)
  [![crates.io](https://img.shields.io/crates/d/iggy.svg)](https://crates.io/crates/iggy)
  [![coverage](https://coveralls.io/repos/github/apache/iggy/badge.svg?branch=master)](https://coveralls.io/github/apache/iggy?branch=master)
  [![dependency](https://deps.rs/repo/github/apache/iggy/status.svg)](https://deps.rs/repo/github/apache/iggy)
  [![x](https://img.shields.io/twitter/follow/ApacheIggy?style=social)](https://twitter.com/ApacheIggy)
  [![discord-badge](https://img.shields.io/discord/1144142576266530928)](https://discord.gg/C5Sux5NcRa)

</div>

---

<div align="center">

  ![iggy](assets/iggy_black.png)

</div>

---

**Iggy** is a persistent message streaming platform written in Rust, supporting QUIC, WebSocket, TCP (custom binary specification) and HTTP (regular REST API) transport protocols, **capable of processing millions of messages per second at ultra-low latency**.

Iggy provides **exceptionally high throughput and performance** while utilizing minimal computing resources.

This is **not yet another extension** running on top of existing infrastructure, such as Kafka or SQL database.

Iggy is a persistent message streaming log **built from the ground up** using low-level I/O with **thread-per-core shared nothing architecture**, `io_uring` and `compio` for maximum speed and efficiency.

The name is an abbreviation for the Italian Greyhound - small yet extremely fast dogs, the best in their class. See the lovely [Fabio & Cookie](https://www.instagram.com/fabio.and.cookie/) ❤️

---

## Features

- **Highly performant**, persistent append-only log for message streaming
- **Very high throughput** for both writes and reads
- **Low latency and predictable resource usage** thanks to the Rust compiled language (no GC) and `io_uring`.
- **User authentication and authorization** with granular permissions and Personal Access Tokens (PAT)
- Support for multiple streams, topics and partitions
- Support for **multiple transport protocols** (QUIC, WebSocket, TCP, HTTP)
- Fully operational RESTful API which can be optionally enabled
- Available client SDK in multiple languages
- **Thread per core shared nothing design** together with `io_uring` guarantee the best possible performance on modern `Linux` systems.
- **Works directly with binary data**, avoiding enforced schema and serialization/deserialization overhead
- Custom **zero-copy (de)serialization**, which greatly improves the performance and reduces memory usage.
- Configurable server features (e.g. caching, segment size, data flush interval, transport protocols etc.)
- Server-side storage of **consumer offsets**
- Multiple ways of polling the messages:
  - By offset (using the indexes)
  - By timestamp (using the time indexes)
  - First/Last N messages
  - Next N messages for the specific consumer
- Possibility of **auto committing the offset** (e.g. to achieve *at-most-once* delivery)
- **Consumer groups** providing the message ordering and horizontal scaling across the connected clients
- **Message expiry** with auto deletion based on the configurable **retention policy**
- Additional features such as **server side message deduplication**
- **Multi-tenant** support via abstraction of **streams** which group **topics**
- **TLS** support for all transport protocols (TCP, WebSocket, QUIC, HTTPS)
- **[Connectors](https://github.com/apache/iggy/tree/master/core/connectors)** - sinks, sources and data transformations based on the **custom Rust plugins**
- **[Model Context Protocol](https://github.com/apache/iggy/tree/master/core/ai/mcp)** - provide context to LLM with **MCP server**
- Optional server-side as well as client-side **data encryption** using AES-256-GCM
- Optional metadata support in the form of **message headers**
- Optional **data backups and archiving** to disk or **S3** compatible cloud storage (e.g. AWS S3)
- Support for **OpenTelemetry** logs & traces + Prometheus metrics
- Built-in **CLI** to manage the streaming server installable via `cargo install iggy-cli`
- Built-in **benchmarking app** to test the performance
- **Single binary deployment** (no external dependencies)
- Running as a single node (clustering based on Viewstamped Replication will be implemented in the near future)

![server](assets/server.png)

![files structure](assets/files_structure.png)

---

## Architecture

This is the high-level architecture of the Iggy message streaming server, where extremely high performance and ultra low and stable tail latencies are the primary goals. The server is designed to handle high throughput and very low latency (sub-millisecond tail latencies), making it suitable for real-time applications. For more details, please refer to the [documentation](https://iggy.apache.org/docs/introduction/architecture).

![server](assets/iggy_architecture.png)

---

## Version

The official releases follow the regular semver (`0.5.0`) or have `latest` tag applied (`apache/iggy:latest`).

We do also publish edge/dev/nightly releases (e.g. `0.6.0-edge.1` or `apache/iggy:edge`), for both, SDKs and the Docker images, which are typically compatible with the latest changes, but are not guaranteed to be stable, and as the name states, are not recommended for production use.

---

## Roadmap

- **Clustering** & data replication based on **[VSR](http://pmg.csail.mit.edu/papers/vr-revisited.pdf)** (on sandbox project using Raft, will be implemented after shared-nothing design is completed)

---

## Supported languages SDK

- [Rust](https://crates.io/crates/iggy)
- [C#](https://www.nuget.org/packages/Apache.Iggy/)
- [Java](https://repository.apache.org/#nexus-search;quick~iggy)
- [Python](https://pypi.org/project/apache-iggy/)
- [Node.js (TypeScript)](https://www.npmjs.com/package/apache-iggy)
- [Go](https://pkg.go.dev/github.com/apache/iggy/foreign/go)

C++ and Elixir are work in progress.

---

## CLI

The interactive CLI is implemented under the `cli` project, to provide the best developer experience. This is a great addition to the Web UI, especially for all the developers who prefer using the console tools.

Iggy CLI can be installed with `cargo install iggy-cli` and then simply accessed by typing `iggy` in your terminal.

![CLI](assets/cli.png)

## Web UI

There's a dedicated Web UI for the server, which allows managing the streams, topics, partitions, browsing the messages and so on. This is an ongoing effort to build a comprehensive dashboard for administrative purposes of the Iggy server. Check the Web UI in the `/web` directory. The [docker image for Web UI](https://hub.docker.com/r/apache/iggy-web-ui) is available, and can be fetched via `docker pull apache/iggy-web-ui`.

![Web UI](assets/web_ui.png)

---

## Connectors

The highly performant and modular **[runtime](https://github.com/apache/iggy/tree/master/core/connectors)** for statically typed, yet dynamically loaded connectors. Ingest the data from the external sources and push it further to the Iggy streams, or fetch the data from the Iggy streams and push it further to the external sources. **Create your own Rust plugins** by simply implementing either the `Source` or `Sink` trait and **build custom pipelines for the data processing**.

```toml
## Configure a sink or source connector, depending on your needs in its own config file.
type = "sink"
key = "quickwit"
enabled = true
version = 0
name = "Quickwit sink"
path = "target/release/libiggy_connector_quickwit_sink"
plugin_config_format = "yaml"

[[streams]]
stream = "qw"
topics = ["records"]
schema = "json"
batch_length = 1000
poll_interval = "5ms"
consumer_group = "qw_sink_connector"

[transforms.add_fields]
enabled = true

[[transforms.add_fields.fields]]
key = "service_name"
value.static = "qw_connector"

[[transforms.add_fields.fields]]
key = "timestamp"
value.computed = "timestamp_millis"

[transforms.delete_fields]
enabled = true
fields = ["email", "created_at"]
```

---

## Model Context Protocol

The [Model Context Protocol](https://modelcontextprotocol.io) (MCP) is an open protocol that standardizes how applications provide context to LLMs. The **[Iggy MCP Server](https://github.com/apache/iggy/tree/master/core/ai/mcp)** is an implementation of the MCP protocol for the message streaming infrastructure. It can be used to provide context to LLMs in real-time, allowing for more accurate and relevant responses.

![server](assets/iggy_mcp_server.png)

---

## Docker

The official Apache Iggy images can be found in [Docker Hub](https://hub.docker.com/r/apache/iggy), simply type `docker pull apache/iggy` to pull the image.

You can also find the images for all the different tooling such as Connectors, MCP Server etc. at [Docker Hub](https://hub.docker.com/u/apache?page=1&search=iggy).

Please note that the images tagged as `latest` are based on the official, stable releases, while the `edge` ones are updated directly from latest version of the `master` branch.

You can find the `Dockerfile` and `docker-compose` in the root of the repository. To build and start the server, run: `docker compose up`.

Additionally, you can run the `CLI` which is available in the running container, by executing: `docker exec -it iggy-server /iggy`.

Keep in mind that running the container on the OS other than Linux, where the Docker is running in the VM, might result in the performance degradation.

Also, when running the container, **make sure to include the additional capabilities**, as you can find in [docker-compose](https://github.com/apache/iggy/blob/master/docker-compose.yml) file:

```yml
cap_add:
  - SYS_NICE
security_opt:
  - seccomp:unconfined
ulimits:
  memlock:
    soft: -1
    hard: -1
```

---

## Configuration

The default configuration can be found in `server.toml` file in `configs` directory.

The configuration file is loaded from the current working directory, but you can specify the path to the configuration file by setting `IGGY_CONFIG_PATH` environment variable, for example `export IGGY_CONFIG_PATH=configs/server.toml` (or other command depending on OS).

When config file is not found, the default values from embedded `server.toml` file are used.

For the detailed documentation of the configuration file, please refer to the [configuration](https://iggy.apache.org/docs/server/configuration) section.

---

## Quick start

Build the project (the longer compilation time is due to [LTO](https://doc.rust-lang.org/rustc/linker-plugin-lto.html) enabled in release [profile](https://github.com/apache/iggy/blob/master/Cargo.toml#L2):

`cargo build`

Run the tests:

`cargo test`

Set root user credentials (OPTIONAL):

Iggy requires credentials to authenticate request to the server.
You can set the root user **before** starting the server.

(macOS/Linux)

```bash
export IGGY_ROOT_USERNAME=iggy
export IGGY_ROOT_PASSWORD=iggy
```

(Windows(Powershell))

```bash
$env:IGGY_ROOT_USERNAME = "iggy"
$env:IGGY_ROOT_PASSWORD = "iggy"
```

By default, `iggy-server` will generate a randomized root user password and print it to `stdout`, when there's
NO users created.

Start the server:

`cargo run --bin iggy-server`

All the data used by the server will be persisted under the `local_data` directory by default, unless specified differently in the configuration (see `system.path` in `server.toml`).

One can use default root credentials with optional `--with-default-root-credentials`.
This flag is equivalent to setting `IGGY_ROOT_USERNAME=iggy` and `IGGY_ROOT_PASSWORD=iggy`, plus
it should only be used for development and testing.

`cargo run --bin iggy-server -- --with-default-root-credentials`

Root credentials are only set on the first server startup when the data directory doesn't exist yet. Once the server has been started and persisted data exists, the existing root credentials will be reused, and the `--with-default-root-credentials` flag or environment variables will have no effect. To reset credentials, delete the data directory.

For configuration options and detailed help:

`cargo run --bin iggy-server -- --help`

You can also use environment variables to override any configuration setting:

- Override TCP address
   `IGGY_TCP_ADDRESS=0.0.0.0:8090 cargo run --bin iggy-server`

- Set custom data path
   `IGGY_SYSTEM_PATH=/data/iggy cargo run --bin iggy-server`

- Enable HTTP transport
   `IGGY_HTTP_ENABLED=true cargo run --bin iggy-server`

- Set custom root user credentials
   `IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy cargo run --bin iggy-server`

To quickly generate the sample data:

`cargo run --bin data-seeder-tool`

*Please note that all commands below are using `iggy` binary, which is part of release (`cli` sub-crate).*

Create a stream with name `dev` (numerical ID will be assigned by server automatically) using default credentials and `tcp` transport (available transports: `quic`, `websocket`, `tcp`, `http`, default `tcp`):

`cargo run --bin iggy -- --transport tcp --username <iggy_username> --password <iggy_password> stream create dev`

List available streams:

`cargo run --bin iggy -- --username <iggy_username> --password <iggy_password> stream list`

Get `dev` stream details:

`cargo run --bin iggy -- -u <iggy_username> -p <iggy_password> stream get dev`

Create a topic named `sample` (numerical ID will be assigned by server automatically) for stream `dev`, with 2 partitions (IDs 1 and 2), disabled compression (`none`) and disabled message expiry (skipped optional parameter):

`cargo run --bin iggy -- -u <iggy_username> -p <iggy_password> topic create dev sample 2 none`

List available topics for stream `dev`:

`cargo run --bin iggy -- -u <iggy_username> -p <iggy_password> topic list dev`

Get topic details for topic `sample` in stream `dev`:

`cargo run --bin iggy -- -u <iggy_username> -p <iggy_password> topic get dev sample`

Send a message 'hello world' (message ID 1) to the stream `dev` to topic `sample` and partition 1:

`cargo run --bin iggy -- -u <iggy_username> -p <iggy_password> message send --partition-id 1 dev sample "hello world"`

Send another message 'lorem ipsum' (message ID 2) to the same stream, topic and partition:

`cargo run --bin iggy -- -u <iggy_username> -p <iggy_password> message send --partition-id 1 dev sample "lorem ipsum"`

Poll messages by a regular consumer with ID 1 from the stream `dev` for topic `sample` and partition with ID 1, starting with offset 0, messages count 2, without auto commit (storing consumer offset on server):

`cargo run --bin iggy -- -u <iggy_username> -p <iggy_password> message poll --consumer 1 --offset 0 --message-count 2 --auto-commit dev sample 1`

Finally, restart the server to see it is able to load the persisted data.

The HTTP API endpoints can be found in [server.http](https://github.com/apache/iggy/blob/master/core/server/server.http) file, which can be used with [REST Client](https://marketplace.visualstudio.com/items?itemName=humao.rest-client) extension for VS Code.

To see the detailed logs from the CLI/server, run it with `RUST_LOG=trace` environment variable. See images below:

---

## Examples

You can find comprehensive sample applications under the `examples/rust` directory. These examples showcase various usage patterns of the Iggy client SDK, from basic operations to advanced multi-tenant scenarios.

For detailed information about available examples and how to run them, please see the [Examples README](examples/rust/README.md).

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
    .direct( // Use either direct (instant) or background message sending
        DirectConfig::builder()
            .batch_length(1000)
            .linger_time(IggyDuration::from_str("1ms")?)
            .build(),
    )
    .partitioning(Partitioning::balanced())
    .build();

producer.init().await?;

// Send some messages to the topic
let messages = vec![IggyMessage::from_str("Hello Apache Iggy")?];
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
    .batch_length(1000)
    .build();

consumer.init().await?;

// Start consuming the messages
while let Some(message) = consumer.next().await {
    // Handle the message
}
```

---

## Benchmarks

**Benchmarks should be the first-class citizens**. We believe that performance is crucial for any system, and we strive to provide the best possible performance for our users. Please check, why we believe that the **[transparent
benchmarking](https://iggy.apache.org/blogs/2025/02/17/transparent-benchmarks)** is so important.

We've also built the **[benchmarking platform](https://benchmarks.iggy.apache.org)** where anyone can upload the benchmarks and compare the results with others. Source code for the platform is available in the `core/bench/dashboard` directory.

![server](assets/benchmarking_platform.png)

For the benchmarking purposes, we've developed the dedicated **iggy-bench** tool, which is a part of the **iggy** project. It is a command-line tool that allows you to run the variety of fully customizable benchmarks.

![server](assets/bench.png)

To benchmark the project, first build the project in release mode:

```bash
cargo build --release
```

Then, run the benchmarking app with the desired options:

1. Sending (writing) benchmark

   ```bash
   cargo run --bin iggy-bench -r -- -v pinned-producer tcp
   ```

2. Polling (reading) benchmark

   ```bash
   cargo run --bin iggy-bench -r -- -v pinned-consumer tcp
   ```

3. Parallel sending and polling benchmark

   ```bash
   cargo run --bin iggy-bench -r -- -v pinned-producer-and-consumer tcp
   ```

4. Balanced sending to multiple partitions benchmark

   ```bash
   cargo run --bin iggy-bench -r -- -v balanced-producer tcp
   ```

5. Consumer group polling benchmark:

   ```bash
   cargo run --bin iggy-bench -r -- -v balanced-consumer-group tcp
   ```

6. Parallel balanced sending and polling from consumer group benchmark:

   ```bash
   cargo run --bin iggy-bench -r -- -v balanced-producer-and-consumer-group tcp
   ```

7. End to end producing and consuming benchmark (single task produces and consumes messages in sequence):

   ```bash
   cargo run --bin iggy-bench -r -- -v end-to-end-producing-consumer tcp
   ```

These benchmarks would start the server with the default configuration, create a stream, topic and partition, and then send or poll the messages. The default configuration is optimized for the best performance, so you might want to tweak it for your needs. If you need more options, please refer to `iggy-bench` subcommands `help` and `examples`.

For example, to run the benchmark for the already started server, provide the additional argument `--server-address 0.0.0.0:8090`.

 **Iggy is already capable of processing millions of messages per second at the microseconds range for p99+ latency** Depending on the hardware, transport protocol (`quic`, `websocket`, `tcp` or `http`) and payload size (`messages-per-batch * message-size`) you might expect **over 5000 MB/s (e.g. 5M of 1 KB msg/sec) throughput for writes and reads**.

Please refer to the mentioned [benchmarking platform](https://benchmarks.iggy.apache.org) where you can browse the results achieved on the different hardware configurations, using the different Iggy server versions.

---

## Contributing

Please see [Contributing](CONTRIBUTING.md)
