# Apache Flink Connectors for Iggy

This repository contains Apache Flink connectors for Iggy message streaming platform, enabling seamless bidirectional data flow between Flink and Iggy.

## Overview

The Flink connectors provide:

- **Sink Connector**: Write data from Iggy to Flink jobs
- **Source Connector**: Read data from Flink sources into Iggy
- **Exactly-once semantics** support via Flink checkpointing
- **Schema flexibility**: JSON, Raw, Text, Protobuf, and FlatBuffer support
- **High performance**: Batching, compression, and async I/O
- **Production-ready**: Monitoring, error handling, and state management

## Architecture

```text
┌──────────────┐     ┌─────────────────┐     ┌──────────────┐
│     Iggy     │────▶│  Flink Sink     │────▶│    Flink     │
│   Streams    │     │   Connector     │     │    Cluster   │
└──────────────┘     └─────────────────┘     └──────────────┘
                                                     │
┌──────────────┐     ┌─────────────────┐           │
│     Iggy     │◀────│  Flink Source   │◀──────────┘
│   Streams    │     │   Connector     │
└──────────────┘     └─────────────────┘
```

## Quick Start

### Prerequisites

- Rust 1.75+
- Apache Iggy server running
- Apache Flink cluster (1.15+)
- Iggy Connector Runtime

### Installation

1. Build the connectors using cargo:

```bash
# Build both connectors
cd core/connectors/flink
./build.sh build

# Or build individually using cargo directly
cd core/connectors/sinks/flink_sink
cargo build --release

cd core/connectors/sources/flink_source
cargo build --release
```

1. Configure the runtime (`config.toml`):

```toml
# Flink Sink Configuration
[sinks.flink]
enabled = true
name = "Flink Sink"
path = "target/release/libiggy_connector_flink_sink"

[[sinks.flink.streams]]
stream = "events"
topics = ["transactions", "orders"]
schema = "json"
batch_length = 1000
poll_interval = "10ms"
consumer_group = "flink_sink_group"

[sinks.flink.config]
flink_cluster_url = "http://localhost:8081"
job_name = "iggy-processing-job"
parallelism = 4
batch_size = 1000
auto_flush_interval_ms = 5000
enable_checkpointing = true
checkpoint_interval_secs = 60
sink_type = "kafka"
target = "processed-events"
exactly_once = true

# Flink Source Configuration
[sources.flink]
enabled = true
name = "Flink Source"
path = "target/release/libiggy_connector_flink_source"

[[sources.flink.streams]]
stream = "flink_output"
topic = "results"
schema = "json"
batch_length = 500
linger_time = "10ms"

[sources.flink.config]
flink_cluster_url = "http://localhost:8081"
source_type = "kafka"
source_identifier = "input-events"
start_position = "latest"
batch_size = 100
poll_interval_ms = 1000
parallelism = 2
```

1. Start the connector runtime:

```bash
# Using cargo directly
cargo run --bin iggy-connectors --release

# Or with configuration
IGGY_CONNECTORS_CONFIG_PATH=config.toml cargo run --bin iggy-connectors --release
```

## Configuration Details

### Sink Connector Configuration

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `flink_cluster_url` | string | Flink JobManager REST API URL | required |
| `job_name` | string | Name of the Flink job | required |
| `job_id` | string? | Specific job ID to target | auto |
| `parallelism` | u32 | Parallelism for sink operator | 4 |
| `batch_size` | usize | Messages per batch | 1000 |
| `auto_flush_interval_ms` | u64 | Auto-flush interval (0 to disable) | 5000 |
| `enable_checkpointing` | bool | Enable Flink checkpointing | true |
| `checkpoint_interval_secs` | u64 | Checkpoint interval in seconds | 60 |
| `sink_type` | enum | Type of sink (kafka, jdbc, elasticsearch, etc.) | required |
| `target` | string | Target topic/table/index | required |
| `skip_errors` | bool | Continue on errors | false |
| `exactly_once` | bool | Enable exactly-once semantics | false |

#### Sink Types

- `kafka`: Apache Kafka sink
- `jdbc`: Database sink via JDBC
- `elasticsearch`: Elasticsearch sink
- `cassandra`: Apache Cassandra sink
- `redis`: Redis sink
- `hbase`: HBase sink
- `mongodb`: MongoDB sink
- `custom`: Custom sink implementation

### Source Connector Configuration

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `flink_cluster_url` | string | Flink JobManager REST API URL | required |
| `source_type` | enum | Type of source | required |
| `source_identifier` | string | Source topic/stream/queue | required |
| `source_pattern` | string? | Pattern for multiple sources | none |
| `start_position` | enum | Starting position | latest |
| `output_schema` | Schema | Output message format | json |
| `batch_size` | usize | Batch size for fetching | 100 |
| `poll_interval_ms` | u64 | Poll interval in ms | 1000 |
| `enable_background_fetch` | bool | Enable background fetching | false |
| `parallelism` | u32 | Source parallelism | 1 |

#### Source Types

- `kafka`: Apache Kafka source
- `kinesis`: AWS Kinesis source
- `rabbitmq`: RabbitMQ source
- `pulsar`: Apache Pulsar source
- `filesystem`: File system source
- `socket`: Socket source
- `custom`: Custom source implementation

#### Start Positions

- `earliest`: Start from beginning
- `latest`: Start from end
- `timestamp(u64)`: Start from specific timestamp
- `offset(u64)`: Start from specific offset
- `group_offsets`: Use consumer group offsets

## Advanced Features

### Authentication

```toml
[sinks.flink.config.auth]
auth_type = "basic"  # or "bearer", "certificate"
username = "admin"
password = "secret"
# token = "bearer-token"  # for bearer auth
# key_path = "/path/to/key"  # for certificate auth
```

### TLS Configuration

```toml
[sinks.flink.config.tls]
enabled = true
cert_path = "/path/to/cert.pem"
key_path = "/path/to/key.pem"
ca_path = "/path/to/ca.pem"
verify_hostname = true
```

### Watermark Strategy (Source)

```toml
[sources.flink.config.watermark_strategy]
strategy_type = "bounded_out_of_orderness"
max_out_of_orderness_ms = 5000
idle_timeout_ms = 30000
```

### Schema Registry

```toml
[sources.flink.config.schema_registry]
url = "http://localhost:8081"
cache_size = 1000

[sources.flink.config.schema_registry.auth]
auth_type = "basic"
username = "registry-user"
password = "registry-pass"
```

### Custom Properties

```toml
[sinks.flink.config.properties]
"header.X-Custom" = "value"
"bootstrap.servers" = "localhost:9092"
"security.protocol" = "SASL_SSL"
```

## Transformations

Both connectors support message transformations:

```toml
# Add fields
[sinks.flink.transforms.add_fields]
enabled = true

[[sinks.flink.transforms.add_fields.fields]]
key = "processed_by"
value.static = "flink-connector"

[[sinks.flink.transforms.add_fields.fields]]
key = "timestamp"
value.computed = "timestamp_millis"

# Delete fields
[sinks.flink.transforms.delete_fields]
enabled = true
fields = ["sensitive_data", "internal_id"]

# Filter fields
[sinks.flink.transforms.filter_fields]
enabled = true
keep = ["id", "name", "value"]
```

## Examples

### Example 1: Stream Processing Pipeline

```toml
# Read from Iggy, process in Flink, write back to Iggy
[sinks.flink_processor]
enabled = true
name = "Event Processor"
path = "target/release/libiggy_connector_flink_sink"

[[sinks.flink_processor.streams]]
stream = "raw_events"
topics = ["clicks", "views"]
schema = "json"
consumer_group = "flink_processor"

[sinks.flink_processor.config]
flink_cluster_url = "http://flink:8081"
job_name = "event-enrichment"
sink_type = "custom"
target = "enrichment-pipeline"

[sources.flink_results]
enabled = true
name = "Enriched Events"
path = "target/release/libiggy_connector_flink_source"

[[sources.flink_results.streams]]
stream = "enriched_events"
topic = "enriched"
schema = "json"

[sources.flink_results.config]
flink_cluster_url = "http://flink:8081"
source_type = "custom"
source_identifier = "enrichment-output"
```

### Example 2: Kafka to Iggy via Flink

```toml
[sources.kafka_via_flink]
enabled = true
name = "Kafka Import"
path = "target/release/libiggy_connector_flink_source"

[[sources.kafka_via_flink.streams]]
stream = "kafka_mirror"
topic = "events"
schema = "json"

[sources.kafka_via_flink.config]
flink_cluster_url = "http://flink:8081"
source_type = "kafka"
source_identifier = "production-events"
start_position = "group_offsets"

[sources.kafka_via_flink.config.properties]
"bootstrap.servers" = "kafka1:9092,kafka2:9092"
"group.id" = "iggy-importer"
"enable.auto.commit" = "false"
```

### Example 3: Real-time Analytics

```toml
[sinks.analytics]
enabled = true
name = "Analytics Pipeline"
path = "target/release/libiggy_connector_flink_sink"

[[sinks.analytics.streams]]
stream = "user_activity"
topics = ["events"]
schema = "json"
consumer_group = "analytics"

[sinks.analytics.config]
flink_cluster_url = "http://flink:8081"
job_name = "real-time-analytics"
sink_type = "elasticsearch"
target = "analytics-index"
exactly_once = true

[sinks.analytics.config.properties]
"elasticsearch.hosts" = "http://es1:9200,http://es2:9200"
"elasticsearch.bulk.flush.max.actions" = "1000"
```

## Monitoring

### Metrics

The connectors expose metrics via Prometheus:

- `iggy_flink_sink_messages_sent_total`
- `iggy_flink_sink_batches_sent_total`
- `iggy_flink_sink_errors_total`
- `iggy_flink_source_messages_received_total`
- `iggy_flink_source_poll_duration_seconds`

### Health Checks

HTTP endpoints for health monitoring:

```bash
# Check sink health
curl http://localhost:8081/connectors/sinks/flink/health

# Check source health
curl http://localhost:8081/connectors/sources/flink/health
```

### Logging

Configure logging via `RUST_LOG` environment variable:

```bash
RUST_LOG=iggy_connector_flink=debug cargo run --bin iggy-connectors
```

## Troubleshooting

### Common Issues

1. **Connection refused to Flink cluster**
   - Verify Flink JobManager is running
   - Check network connectivity
   - Ensure REST API is enabled

2. **Checkpointing failures**
   - Increase checkpoint timeout
   - Check state backend configuration
   - Verify sufficient resources

3. **Message loss**
   - Enable exactly-once semantics
   - Configure proper checkpointing
   - Use persistent state backend

4. **Performance issues**
   - Increase batch size
   - Adjust parallelism
   - Enable background fetching
   - Use compression

### Debug Mode

Enable detailed debugging:

```toml
[sinks.flink.config.properties]
"debug.enabled" = "true"
"debug.log_messages" = "true"
"debug.log_checkpoints" = "true"
```

## Development

### Building from Source

```bash
# Clone repository
git clone https://github.com/apache/iggy.git
cd iggy/core/connectors/flink

# Build using the build script
./build.sh build        # Build both connectors
./build.sh build-sink   # Build sink only
./build.sh build-source # Build source only

# Or use cargo directly
cd ../sinks/flink_sink
cargo build --release

cd ../sources/flink_source
cargo build --release
```

### Testing

```bash
# Run all tests
./build.sh test

# Run sink tests only
./build.sh test-sink

# Run source tests only
./build.sh test-source

# Run end-to-end tests
./build.sh e2e

# Or use cargo directly
cd sinks/flink_sink && cargo test
cd sources/flink_source && cargo test
```

### Code Quality

```bash
# Format code
./build.sh fmt

# Run clippy linter
./build.sh clippy

# Run all checks (fmt + clippy + check)
./build.sh check

# Clean build artifacts
./build.sh clean
```

### Docker Infrastructure

```bash
# Start test infrastructure (Iggy, Flink, Kafka)
./build.sh docker-up

# Stop test infrastructure
./build.sh docker-down
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

Apache License 2.0 - See LICENSE file for details.

## Support

- GitHub Issues: <https://github.com/apache/iggy/issues>
- Discord: <https://discord.gg/iggy>
- Documentation: <https://iggy.apache.org/docs/>
