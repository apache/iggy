# Flink Source Connector

A source connector that reads data from Apache Flink jobs and publishes to Iggy streams.

## Features

- Connect to running Flink jobs with various source types
- Support for Kafka, Kinesis, FileSystem and custom sources
- Automatic job discovery and monitoring
- Schema conversion support
- Configurable polling and batching
- Checkpoint restoration for fault tolerance
- Metrics collection from Flink jobs

## Configuration

### Basic Configuration

```toml
[source]
type = "flink_source"

[source.config]
flink_cluster_url = "http://localhost:8081"
source_type = "kafka"
source_identifier = "input-events"
batch_size = 100
poll_interval_ms = 1000
```

### Advanced Configuration

```toml
[source.config]
# Flink cluster connection
flink_cluster_url = "http://localhost:8081"
connection_timeout_secs = 30

# Source identification
source_type = "kafka"  # kafka, kinesis, file_system, custom
source_identifier = "sensor-events"  # Topic/stream/path identifier
job_name_pattern = ".*sensor.*"  # Optional: regex to match job names

# Data fetching
batch_size = 500
poll_interval_ms = 1000
initial_offset = "latest"  # latest, earliest, or specific offset
skip_errors = false

# Checkpointing
enable_checkpointing = true
restore_from_checkpoint = "chk-123456"  # Optional: specific checkpoint ID

# Authentication (optional)
[source.config.auth]
auth_type = "bearer"
token = "your-auth-token"

# Schema configuration (optional)
[source.config.input_schema]
type = "avro"
schema_registry_url = "http://localhost:8081"

[source.config.output_schema]
type = "json"

# Custom properties
[source.config.properties]
"queryable.state.name" = "event-stream"
"state.backend" = "rocksdb"
```

## Source Types

### Kafka Source

Reads from Flink jobs that consume from Kafka:

```toml
source_type = "kafka"
source_identifier = "events-topic"  # Kafka topic name

[source.config.properties]
"kafka.group.id" = "flink-consumer-group"
"kafka.bootstrap.servers" = "localhost:9092"
```

### Kinesis Source

Reads from Flink jobs that consume from AWS Kinesis:

```toml
source_type = "kinesis"
source_identifier = "my-kinesis-stream"

[source.config.properties]
"aws.region" = "us-east-1"
"aws.credentials.provider" = "DEFAULT"
```

### FileSystem Source

Reads from Flink jobs that process files:

```toml
source_type = "file_system"
source_identifier = "/data/input/*.json"

[source.config.properties]
"file.format" = "json"
"file.monitor.interval" = "10s"
```

### Custom Source

For custom Flink sources:

```toml
source_type = { custom = "MyCustomSource" }
source_identifier = "custom-stream-id"

[source.config.properties]
"custom.property1" = "value1"
"custom.property2" = "value2"
```

## Message Format

Messages read from Flink are converted to Iggy's message format:

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": 1704067200000,
  "headers": {
    "source": "kafka",
    "partition": "0",
    "offset": "12345"
  },
  "payload": {
    "event_type": "sensor_reading",
    "sensor_id": "sensor-001",
    "temperature": 23.5,
    "humidity": 65.2
  }
}
```

## Schema Conversion

The connector supports automatic schema conversion:

```toml
# Read Avro from Flink, convert to JSON for Iggy
[source.config.input_schema]
type = "avro"
schema_registry_url = "http://localhost:8081"
schema_id = 123

[source.config.output_schema]
type = "json"
```

## Job Discovery

The connector can automatically discover Flink jobs:

1. **By Job ID**: Connect to a specific job

   ```toml
   job_id = "abc123def456"
   ```

2. **By Pattern**: Find jobs matching a pattern

   ```toml
   job_name_pattern = ".*sensor.*"
   ```

3. **By Source Type**: Find jobs with matching sources

   ```toml
   source_type = "kafka"
   source_identifier = "events"
   ```

## Checkpointing and Recovery

Enable checkpointing for fault tolerance:

```toml
enable_checkpointing = true
checkpoint_interval_secs = 60

# Restore from specific checkpoint on startup
restore_from_checkpoint = "chk-123456"
```

On failure, the connector can resume from the last checkpoint:

```bash
# List available checkpoints
curl http://localhost:8081/v1/jobs/{job_id}/checkpoints

# Restore from checkpoint
curl -X POST http://localhost:8081/v1/jobs/{job_id}/restore?checkpoint={checkpoint_id}
```

## Running with Docker

Use the provided docker-compose to run a test environment:

```bash
cd core/connectors/flink
docker-compose up -d

# Create a test Flink job that produces data
docker exec -it flink-jobmanager flink run \
  /opt/flink/examples/streaming/WordCount.jar
```

## Building from Source

```bash
# Build the connector
cargo build --package iggy-connector-flink-source --release

# Run tests
cargo test --package iggy-connector-flink-source

# The built library will be at:
# target/release/libiggy_connector_flink_source.so (Linux)
# target/release/libiggy_connector_flink_source.dylib (macOS)
```

## Monitoring

Monitor the connector and Flink job metrics:

```bash
# Get job metrics
curl http://localhost:8081/v1/jobs/{job_id}/metrics?get=numRecordsOut,numBytesOut

# Get vertex metrics (for specific source)
curl http://localhost:8081/v1/jobs/{job_id}/vertices/{vertex_id}/metrics

# Monitor connector state
curl http://localhost:8090/metrics/connectors/flink-source
```

Available metrics:

- `messages_read`: Total messages read from Flink
- `bytes_read`: Total bytes read
- `batches_processed`: Number of batches processed
- `errors_count`: Number of errors encountered
- `last_offset`: Last processed offset
- `lag`: Current lag behind source

## Performance Tuning

### Batch Size

- Larger batches = higher throughput, higher latency
- Smaller batches = lower throughput, lower latency

### Poll Interval

- Shorter interval = lower latency, higher CPU usage
- Longer interval = higher latency, lower CPU usage

### Parallelism

Match the parallelism of your Flink source operators:

```toml
parallelism = 4  # Should match Flink source parallelism
```

## Troubleshooting

### No Data Being Read

1. Verify Flink job is running:

   ```bash
   curl http://localhost:8081/v1/jobs
   ```

2. Check source vertex is producing data:

   ```bash
   curl http://localhost:8081/v1/jobs/{job_id}/vertices
   ```

3. Verify source identifier matches:
   - For Kafka: topic name must match
   - For Kinesis: stream name must match
   - For FileSystem: path pattern must match

### Connection Issues

1. Check Flink cluster is accessible
2. Verify authentication if configured
3. Check network connectivity

### Data Format Issues

1. Verify schema compatibility
2. Check for serialization errors in logs
3. Enable `skip_errors` to continue on format errors

### Performance Issues

1. Increase batch_size for better throughput
2. Adjust poll_interval_ms based on latency requirements
3. Check Flink job backpressure
4. Monitor TaskManager memory usage

## Example: Kafka to Iggy Pipeline

1. Start infrastructure:

   ```bash
   docker-compose up -d
   ```

2. Create Flink job that reads from Kafka:

   ```bash
   docker exec -it flink-jobmanager flink run \
     -c com.example.KafkaToFlinkJob \
     /path/to/job.jar \
     --source.topic input-events \
     --kafka.servers localhost:9092
   ```

3. Configure source connector:

   ```toml
   [source.config]
   flink_cluster_url = "http://localhost:8081"
   source_type = "kafka"
   source_identifier = "input-events"
   batch_size = 1000
   poll_interval_ms = 500
   ```

4. Messages flow: Kafka → Flink → Iggy

## License

Apache License 2.0
