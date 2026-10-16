# Apache Iggy Connectors

The highly performant and modular runtime for statically typed, yet dynamically loaded connectors. Ingest the data from the external sources and push it further to the Iggy streams, or fetch the data from the Iggy streams and push it further to the external sources. Create your own Rust plugins by simply implementing either the `Source` or `Sink` trait and build custom pipelines for the data processing.

The [docker image](https://hub.docker.com/r/apache/iggy-connect) is available via `docker pull apache/iggy-connect`. It ships in two flavors: a default image with every connector plugin bundled, and a `-slim` runtime-only image (see [Docker image](#docker-image)).

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

## Docker image

Published to Docker Hub as [`apache/iggy-connect`](https://hub.docker.com/r/apache/iggy-connect) in two flavors:

| Tag | Contents |
| --- | --- |
| `edge`, `x.y.z`, `latest` | Runtime plus every bundled connector plugin (default) |
| `edge-slim`, `x.y.z-slim`, `latest-slim` | Runtime binary only |

The default (fat) image bakes every connector plugin into `/usr/local/lib`, which is on the runtime's [plugin search path](#plugin-path-resolution). A connector config can therefore load a bundled plugin by library file name alone, with no directory and no extension:

```toml
# a connector definition in your config_dir
type = "sink"
key = "stdout"
name = "Stdout sink"
path = "libiggy_connector_stdout_sink"
```

Run the runtime with a config mounted in, pointing `IGGY_CONNECTORS_CONFIG_PATH` at it:

```bash
docker run --rm \
  -v "$PWD/my_config:/config" \
  -e IGGY_CONNECTORS_CONFIG_PATH=/config/config.toml \
  apache/iggy-connect
```

The mounted `config.toml` must set `[connectors].config_dir` to the connector definitions inside the container, and each definition's `path` must resolve there; bundled plugins resolve by file name as shown above.

Choose a `-slim` tag when you supply your own plugins and want a smaller image with a narrower dependency surface, and mount the plugin `.so` files into one of the search-path directories (for example `/usr/local/lib`).

Source connectors save their checkpoints under `[state].path`, which is `local_state` by default. In the image this resolves to `/app/local_state`. If you run sources, mount a volume there. Otherwise the checkpoints are lost when the container is replaced.

## Quick Start

This example runs the Iggy server, the connectors runtime with the Delta Lake sink and [floci](https://github.com/floci-io/floci), a local S3 emulator, then produces random user records and shows them in a Delta table. You need Docker with Compose, and a Rust toolchain for the producer.

1. Start the stack from the example directory:

    ```bash
    cd examples/rust/src/sink-data-producer
    docker compose up -d --build
    ```

    One-shot services create the `example_stream` stream with the `users` topic, and the `delta-warehouse` bucket with the Delta table `s3://delta-warehouse/users`. The runtime starts after they finish.

2. Check that everything is up:

    ```bash
    docker compose ps
    curl localhost:3001/health
    curl localhost:3001/sinks
    ```

    The `iggy-init` and `delta-tools` services exit after their work is done, the other services keep running. The `/sinks` response lists the `delta` sink, its status should be `running`.

3. From the repository root, run the producer. It sends 100 batches of random records to the `users` topic:

    ```bash
    cargo run --example sink-data-producer --release
    ```

4. See the results. The first command lists the Delta data and log files in the bucket, the second prints the table version, the row count and the newest rows:

    ```bash
    cd examples/rust/src/sink-data-producer
    docker compose run --rm tools s3 ls s3://delta-warehouse/users --recursive
    docker compose run --rm delta-tools python show.py
    ```

5. Stop the stack and remove all its data:

    ```bash
    docker compose down -v
    ```

The default `apache/iggy-connect:edge` image bundles the Delta sink plugin. To test an unreleased change to the image or the plugins, build the default image locally from the repository root and point the example at it:

```bash
docker buildx build -f core/connectors/runtime/Dockerfile --target runtime-fat \
  --platform linux/amd64 --build-arg LIBC=glibc --build-arg IGGY_CI_BUILD=true \
  -t iggy-connect:fat-local --load .
cd examples/rust/src/sink-data-producer
IGGY_CONNECT_IMAGE=iggy-connect:fat-local docker compose up -d --build
```

IMPORTANT: make sure you have pulled the code from the latest master to ensure connectors and server protocol compatibility. The incompatibility causes obscure errors coming from the connectors' client due to frequent changes done to the protocol at the moment. Run `docker compose pull` before `docker compose up` so that the `edge` images match your checkout.

The configuration used by the example lives in `examples/rust/src/sink-data-producer/config/`.

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

- **ClickHouse Sink** - inserts messages into ClickHouse tables as JSONEachRow, RowBinary or raw string data
- **Delta Sink** - appends messages to Delta Lake tables on local disk, S3, Azure Blob Storage or Google Cloud Storage
- **Doris Sink** - loads JSON messages into Apache Doris tables via the Stream Load HTTP API
- **Elasticsearch Sink** - sends messages to Elasticsearch indices
- **HTTP Sink** - delivers messages to any HTTP endpoint such as webhooks and REST APIs
- **Iceberg Sink** - writes data to Apache Iceberg tables via REST catalog
- **InfluxDB Sink** - writes messages to InfluxDB V2 and V3 as line-protocol points
- **Meilisearch Sink** - indexes messages in Meilisearch
- **MongoDB Sink** - stores messages as documents in a MongoDB collection
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
- **HTTP Source** - receives webhook POST requests over an embedded HTTP server and produces their bodies as messages
- **InfluxDB Source** - polls InfluxDB V2 (Flux) and V3 (SQL) with a persisted timestamp cursor
- **PostgreSQL Source** - reads rows from PostgreSQL tables with multiple consumption strategies (delete after read, mark as processed, timestamp tracking)
- **Random Source** - generates random test messages (useful for testing/development)

## Building the connectors

New connector can be built simply by implementing either `Sink` or `Source` trait. Please check the **[sink](https://github.com/apache/iggy/tree/master/core/connectors/sinks)** or **[source](https://github.com/apache/iggy/tree/master/core/connectors/sources)** documentation, as well as the existing examples under `core/connectors/sinks` and `core/connectors/sources`.

## Transformations

Field transformations (depending on the supported payload formats) can be applied to the messages either before they are sent to the specified topic (e.g. when produced by the source connectors), or before consumed by the sink connectors. To add a transformation, implement the `Transform` trait in the SDK, add its `TransformType` variant and extend `transforms::from_config`. Each transform may have its own, custom configuration.

To find out more about the transforms, stream decoders or encoders, please refer to the **[SDK documentation](https://github.com/apache/iggy/tree/master/core/connectors/sdk)**.
