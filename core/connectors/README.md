# Apache Iggy Connectors

The highly performant and modular runtime for statically typed, yet dynamically loaded connectors. Ingest the data from the external sources and push it further to the Iggy streams, or fetch the data from the Iggy streams and push it further to the external sources. Create your own Rust plugins by simply implementing either the `Source` or `Sink` trait and build custom pipelines for the data processing.

The [docker image](https://hub.docker.com/r/apache/iggy-connect) is available, and can be fetched via `docker pull apache/iggy-connect:edge`.

## Features

- **High Performance**: Utilizes Rust's performance characteristics to ensure fast data ingestion and egress.
- **Low memory footprint**: Designed with memory efficiency in mind, minimizing the memory footprint of the connectors.
- **Modular Design**: Designed with modularity in mind, allowing for easy extension and customization.
- **Dynamic Loading**: Supports dynamic loading of plugins, enabling seamless integration with various data sources and sinks.
- **Statically Typed**: Ensures type safety and compile-time checks, reducing runtime errors.
- **Easy Customization**: Provides a simple interface for implementing custom connectors, making it easy to create new plugins.
- **Data transformation**: Supports data transformation with the help of existing functions.
- **Powerful configuration**: Define your sinks, sources, and transformations in the configuration file or fetch them from a remote HTTP API.
- **Flexible configuration providers**: Support for local file-based and HTTP-based configuration providers for centralized configuration management.
- **Observability**: Prometheus metrics with per-stage latency histograms, plus an opt-in per-batch tracing benchmark target.
- **Structured logging**: Selectable text or JSON log format via `[logging]` configuration.

## Quick Start

Run these commands from the root of the same Iggy source checkout used for the server and plugins. This guide targets server 0.9.0, including its edge builds.

1. Build the server, CLI, runtime and quick-start plugins:

    ```bash
    cargo build --release -p server -p iggy-cli -p iggy-connectors \
      -p iggy_connector_random_source -p iggy_connector_stdout_sink \
      -p iggy_connector_quickwit_sink
    ```

    For a debug build, omit `--release` and replace `target/release` with `target/debug` in both the commands and plugin paths. Make sure that the plugins specified in `core/connectors/runtime/example_config/connectors/` directory under `path` are available. The configuration must be provided in `toml` format.
    The example directory also enables connectors for ClickHouse, Delta Lake, Apache Doris, Apache Iceberg, and InfluxDB. Without their backing services (or their compiled plugins) these are reported with the `Error` status, but they don't block the remaining connectors. Set `enabled = false` in their files to skip them entirely.

2. Run `docker compose -f examples/rust/src/sink-data-producer/docker-compose.yml up -d`, which will start the Quickwit server to be used by an example sink connector. At this point, you can access the Quickwit UI at [http://localhost:7280](http://localhost:7280) - check this dashboard again later on, after the `events` index will be created.

3. In the terminal that will run the connectors, set the runtime configuration path:

    ```bash
    export IGGY_CONNECTORS_CONFIG_PATH=core/connectors/runtime/example_config/config.toml
    ```

4. Start the Iggy server in a separate terminal with credentials matching the sample connector configuration:

    ```bash
    IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy cargo run --bin iggy-server --release
    ```

    With the server running, create the example streams and topics using the CLI from this checkout. An existing server must have these credentials, or you must adjust the commands and connector configuration to match it.

    ```bash
    target/release/iggy --username iggy --password iggy stream create example_stream
    target/release/iggy --username iggy --password iggy topic create example_stream example_topic 1 none 1d
    target/release/iggy --username iggy --password iggy stream create qw
    target/release/iggy --username iggy --password iggy topic create qw records 1 none 1d
    ```

5. Execute `cargo run --example sink-data-producer --release`, which sends 100 batches of messages to previously created `qw` stream and `records` topic (this will be used by the Quickwit sink connector).

6. Start the connector runtime `cargo run --bin iggy-connectors --release` in the terminal configured in step 3. The Quickwit sink indexes the produced records in the `events` index. At the same time, you should see the new messages being added to the `example_stream` stream and `example_topic` topic by the Random source connector - you can [start the Iggy Web UI](https://iggy.apache.org/docs/web_ui/start) to browse the data. The messages will have applied the basic fields transformations.

## Runtime

All the connectors are implemented as Rust libraries and can be used as a part of the connector runtime. The runtime is responsible for managing the lifecycle of the connectors and providing the necessary infrastructure for the connectors to run. For more information, please refer to the **[runtime documentation](https://github.com/apache/iggy/tree/master/core/connectors/runtime)**.

## Plugin path resolution

The `path` field in connector configs points to the shared library (`.so`, `.dylib`, `.dll`). The runtime resolves it as follows:

1. **Extension** — if the path has no recognized extension, the OS-native one is appended automatically (`.so` on Linux, `.dylib` on macOS, `.dll` on Windows).

2. **Absolute paths** — used as-is.

3. **Relative paths** — searched in order, returning the first match:
   - the literal relative path (from working directory)
   - directory of the runtime binary (filename only)
   - current working directory (filename only)
   - `/usr/lib`, `/usr/lib64`, `/lib`, `/lib64`, `/usr/local/lib`, `/usr/local/lib64`

**Examples:**

```toml
# Relative — resolved against search dirs; extension appended on Linux
path = "target/release/libiggy_connector_stdout_sink"

# Absolute — used directly
path = "/opt/iggy/plugins/libiggy_connector_stdout_sink.so"
```

If the library is not found, the runtime logs all searched paths to help diagnose the issue.

## Sink

Sinks are responsible for consuming the messages from the configured stream(s) and topic(s) and sending them further to the specified destination. For example, the Quickwit sink connector is responsible for sending the messages to the Quickwit indexer.

Please refer to the **[Sink documentation](https://github.com/apache/iggy/tree/master/core/connectors/sinks)** for the details about the configuration and the sample implementation.

When implementing `Sink`, make sure to use the `sink_connector!` macro to expose the FFI interface and allow the connector runtime to register the sink with the runtime. The macro also exports the connector's version (from `Cargo.toml`) which is reported in the runtime's `/stats` endpoint.
Each sink should have its own, custom configuration, which is passed along with the unique plugin ID via expected `new()` method.

### Available Sinks

- **Doris Sink** - loads JSON messages into Apache Doris tables via the Stream Load HTTP API
- **Elasticsearch Sink** - sends messages to Elasticsearch indices
- **Iceberg Sink** - writes data to Apache Iceberg tables via REST catalog
- **Meilisearch Sink** - indexes messages in Meilisearch
- **PostgreSQL Sink** - stores messages in PostgreSQL database tables
- **Quickwit Sink** - indexes messages in Quickwit search engine
- **RabbitMQ Sink** - publishes messages to RabbitMQ exchanges via AMQP 0.9.1
- **Reshift Sink** - stores messages in Redshift warehouse tables via S3 as staging
- **S3 Sink** - writes messages to Amazon S3 and S3-compatible stores (MinIO, R2, B2, DO Spaces)
- **Stdout Sink** - prints messages to standard output (useful for debugging/development)
- **SurrealDB Sink** - writes messages into SurrealDB with deterministic record IDs for idempotent replay

## Source

Sources produce messages to an Iggy stream and topic. Configure one `[[streams]]` entry per source instance: the runtime retains only the last configured producer. For example, the Random source generates messages for that stream and topic.

Please refer to the **[Source documentation](https://github.com/apache/iggy/tree/master/core/connectors/sources)** for the details about the configuration and the sample implementation.

### Available Sources

- **Elasticsearch Source** - polls documents from Elasticsearch indices
- **PostgreSQL Source** - reads rows from PostgreSQL tables with multiple consumption strategies (delete after read, mark as processed, timestamp tracking)
- **Random Source** - generates random test messages (useful for testing/development)

## Building the connectors

New connector can be built simply by implementing either `Sink` or `Source` trait. Please check the **[sink](https://github.com/apache/iggy/tree/master/core/connectors/sinks)** or **[source](https://github.com/apache/iggy/tree/master/core/connectors/sources)** documentation, as well as the existing examples under `core/connectors/sinks` and `core/connectors/sources`.

## Transformations

Field transformations (depending on the supported payload formats) can be applied to the messages either before they are sent to the specified topic (e.g. when produced by the source connectors), or before consumed by the sink connectors. To add a transformation, implement the `Transform` trait in the SDK, add its `TransformType` variant and extend `transforms::from_config`. Each transform may have its own, custom configuration.

To find out more about the transforms, stream decoders or encoders, please refer to the **[SDK documentation](https://github.com/apache/iggy/tree/master/core/connectors/sdk)**.
