# Apache Iggy Connectors

The highly performant and modular runtime for statically typed, yet dynamically loaded connectors. Ingest the data from the external sources and push it further to the Iggy streams, or fetch the data from the Iggy streams and push it further to the external sources. Create your own Rust plugins by simply implementing either the `Source` or `Sink` trait and build custom pipelines for the data processing.

**This is still WiP, and the runtime can be started only after compilation from the source code (no installable package yet).**

The [docker image](https://hub.docker.com/r/apache/iggy-connect) is available, and can be fetched via `docker pull apache/iggy-connect`.

## Features

- **High Performance**: Utilizes Rust's performance characteristics to ensure fast data ingestion and egress.
- **Low memory footprint**: Designed with memory efficiency in mind, minimizing the memory footprint of the connectors.
- **Modular Design**: Designed with modularity in mind, allowing for easy extension and customization.
- **Dynamic Loading**: Supports dynamic loading of plugins, enabling seamless integration with various data sources and sinks.
- **Statically Typed**: Ensures type safety and compile-time checks, reducing runtime errors.
- **Easy Customization**: Provides a simple interface for implementing custom connectors, making it easy to create new plugins.
- **Data transformation**: Supports data transformation with the help of existing functions.
- **Powerful configuration**: Define your sinks, sources, and transformations in the configuration file.

## Quick Start

1. Build the project in release mode (or debug, and update the connectors paths in the config accordingly), and make sure that the plugins specified in `core/connectors/runtime/example_config/connectors/` directory under `path` are available. The configuration must be provided in `toml` format.

2. Run `docker compose up -d` from `/examples/rust/src/sink-data-producer` which will start the Quickwit server to be used by an example sink connector. At this point, you can access the Quickwit UI at [http://localhost:7280](http://localhost:7280) - check this dashboard again later on, after the `events` index will be created.

3. Set environment variable `IGGY_CONNECTORS_CONFIG_PATH=core/connectors/runtime/example_config/config.toml` (adjust the path as needed) pointing to the runtime configuration file.

4. Start the Iggy server and invoke the following commands via Iggy CLI to create the example streams and topics used by the sample connectors.

    ```bash
    iggy --username iggy --password iggy stream create example_stream
    iggy --username iggy --password iggy topic create example_stream example_topic 1 none 1d
    iggy --username iggy --password iggy stream create qw
    iggy --username iggy --password iggy topic create qw records 1 none 1d
    ```

5. Execute `cargo run --example sink-data-producer -r` which will start the example data producer application, sending the messages to previously created `qw` stream and `records` topic (this will be used by the Quickwit sink connector).

6. Start the connector runtime `cargo run --bin iggy-connectors -r` - you should be able to browse Quickwit UI with records being constantly added to the `events` index. At the same time, you should see the new messages being added to the `example` stream and `topic1` topic by the test source connector - you can use Iggy Web UI to browse the data. The messages will have applied the basic fields transformations.

## Runtime

All the connectors are implemented as Rust libraries and can be used as a part of the connector runtime. The runtime is responsible for managing the lifecycle of the connectors and providing the necessary infrastructure for the connectors to run. For more information, please refer to the **[runtime documentation](https://github.com/apache/iggy/tree/master/core/connectors/runtime)**.

## Sink

Sinks are responsible for consuming the messages from the configured stream(s) and topic(s) and sending them further to the specified destination. For example, the Quickwit sink connector is responsible for sending the messages to the Quickwit indexer.

Please refer to the **[Sink documentation](https://github.com/apache/iggy/tree/master/core/connectors/sinks)** for the details about the configuration and the sample implementation.

When implementing `Sink`, make sure to use the `sink_connector!` macro to expose the FFI interface and allow the connector runtime to register the sink with the runtime.
Each sink should have its own, custom configuration, which is passed along with the unique plugin ID via expected `new()` method.

## Source

Sources are responsible for producing the messages to the configured stream(s) and topic(s). For example, the Test source connector will generate the random messages that will be then sent to the configured stream and topic.

Please refer to the **[Source documentation](https://github.com/apache/iggy/tree/master/core/connectors/sources)** for the details about the configuration and the sample implementation.

## Building the connectors

New connector can be built simply by implementing either `Sink` or `Source` trait. Please check the **[sink](https://github.com/apache/iggy/tree/master/core/connectors/sinks)** or **[source](https://github.com/apache/iggy/tree/master/core/connectors/sources)** documentation, as well as the existing examples under `/sinks` and `/sources` directories.

## Transformations

Field transformations (depending on the supported payload formats) can be applied to the messages either before they are sent to the specified topic (e.g. when produced by the source connectors), or before consumed by the sink connectors. To add the new transformation, simply implement the `Transform` trait and extend the existing `load` function. Each transform may have its own, custom configuration.

To find out more about the transforms, stream decoders or encoders, please refer to the **[SDK documentation](https://github.com/apache/iggy/tree/master/core/connectors/sdk)**.
