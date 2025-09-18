# Flink Sink Connector

A sink connector that sends messages from Iggy to Apache Flink jobs for stream processing.

## Features

- Connect to existing Flink jobs or submit new ones
- Support for multiple sink types (Kafka, JDBC, Elasticsearch, etc.)
- Message transformation pipeline
- Schema conversion (JSON, Avro, Protobuf)
- Batch processing with configurable size
- Automatic checkpointing support
- Retry mechanism with exponential backoff
- Metrics collection and monitoring

## Configuration

### Basic Configuration

```toml
[sink]
type = "flink_sink"

[sink.config]
flink_cluster_url = "http://localhost:8081"
job_name = "iggy-flink-sink"
target = "output-topic"
sink_type = "kafka"
batch_size = 1000
auto_flush_interval_ms = 5000
```

### Advanced Configuration

```toml
[sink.config]
# Flink cluster connection
flink_cluster_url = "http://localhost:8081"
job_name = "data-processing-pipeline"

# Use existing job or submit new one
job_id = "abc123def456"  # Optional: connect to existing job
jar_path = "/path/to/job.jar"  # Optional: submit new job
entry_class = "com.example.FlinkJob"
program_args = "--input kafka --output elasticsearch"

# Sink configuration
sink_type = "kafka"  # kafka, jdbc, elasticsearch, cassandra, redis, hbase, mongodb
target = "processed-events"
parallelism = 4

# Batching and performance
batch_size = 1000
auto_flush_interval_ms = 5000
skip_errors = false
exactly_once = true

# Checkpointing
enable_checkpointing = true
checkpoint_interval_secs = 60

# Connection settings
connection_timeout_secs = 30
max_retries = 3
retry_delay_ms = 1000

# Authentication (optional)
[sink.config.auth]
auth_type = "basic"  # none, basic, bearer, certificate
username = "admin"
password = "secret"

# TLS configuration (optional)
[sink.config.tls]
enabled = true
cert_path = "/path/to/cert.pem"
key_path = "/path/to/key.pem"
ca_path = "/path/to/ca.pem"
verify_hostname = true

# Custom properties for specific sink types
[sink.config.properties]
"bootstrap.servers" = "kafka:9092"
"transaction.timeout.ms" = "60000"
```

## Schema Conversion

The connector supports schema conversion between different formats:

```toml
# Input schema (how messages are decoded from Iggy)
[sink.config.input_schema]
type = "json"

# Output schema (how messages are encoded for Flink)
[sink.config.output_schema]
type = "avro"
schema_registry_url = "http://localhost:8081"
```

Supported schema types:

- JSON
- Avro
- Protobuf
- MessagePack
- Text
- Binary

## Message Transformations

Apply transformations to messages before sending to Flink:

```toml
[[sink.config.transforms]]
type = "filter"
config = { field = "event_type", operator = "equals", value = "purchase" }

[[sink.config.transforms]]
type = "map"
config = { mappings = { "user_id" = "userId", "event_time" = "timestamp" } }

[[sink.config.transforms]]
type = "enrich"
config = { add_fields = { "processed_at" = "now", "version" = "1.0" } }
```

## Sink Types

### Kafka Sink

```toml
sink_type = "kafka"
target = "output-topic"

[sink.config.properties]
"bootstrap.servers" = "localhost:9092"
"compression.type" = "snappy"
"batch.size" = "16384"
```

### JDBC Sink

```toml
sink_type = "jdbc"
target = "events_table"

[sink.config.properties]
"jdbc.url" = "jdbc:postgresql://localhost:5432/mydb"
"jdbc.driver" = "org.postgresql.Driver"
"jdbc.user" = "postgres"
"jdbc.password" = "password"
"jdbc.batch.size" = "100"
```

### Elasticsearch Sink

```toml
sink_type = "elasticsearch"
target = "events-index"

[sink.config.properties]
"elasticsearch.hosts" = "http://localhost:9200"
"elasticsearch.index.prefix" = "iggy-"
"elasticsearch.bulk.flush.max.actions" = "1000"
```

## Running with Docker

Use the provided docker-compose file to run a complete test environment:

```bash
cd core/connectors/flink
docker-compose up -d

# Check Flink cluster status
curl http://localhost:8081/v1/overview

# View running jobs
curl http://localhost:8081/v1/jobs
```

## Building from Source

```bash
# Build the connector
cargo build --package iggy-connector-flink-sink --release

# Run tests
cargo test --package iggy-connector-flink-sink

# The built library will be at:
# target/release/libiggy_connector_flink_sink.so (Linux)
# target/release/libiggy_connector_flink_sink.dylib (macOS)
```

## Example Usage

See the `examples/flink-data-producer` directory for a complete example that:

1. Produces sensor data to Iggy
2. Processes it through the Flink sink connector
3. Outputs results to various targets

Run the example:

```bash
# Start the infrastructure
docker-compose up -d

# Run the producer
cargo run --example flink-data-producer -- \
  --server tcp://localhost:8090 \
  --stream sensor-data \
  --topic raw-events \
  --continuous
```

## Monitoring

The connector exposes metrics that can be monitored:

- `records_sent`: Total records sent to Flink
- `bytes_sent`: Total bytes sent
- `batch_count`: Number of batches processed
- `errors_count`: Number of errors encountered
- `retry_count`: Number of retries performed
- `checkpoint_count`: Number of checkpoints created

Access metrics via Flink's REST API:

```bash
curl http://localhost:8081/v1/jobs/{job_id}/metrics
```

## Troubleshooting

### Connection Issues

1. Verify Flink cluster is running:

   ```bash
   curl http://localhost:8081/v1/overview
   ```

2. Check network connectivity between Iggy and Flink

3. Verify authentication credentials if configured

### Performance Tuning

- Increase `batch_size` for higher throughput
- Adjust `parallelism` based on cluster resources
- Enable `exactly_once` only if needed (impacts performance)
- Configure appropriate checkpoint intervals

### Common Errors

- **Job not found**: Verify job_id exists or provide jar_path for new job
- **Serialization errors**: Check schema compatibility
- **Timeout errors**: Increase connection_timeout_secs
- **Memory issues**: Reduce batch_size or increase Flink TaskManager memory

## License

Apache License 2.0
