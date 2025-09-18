# Testing Guide for Iggy Flink Connectors

This guide provides instructions for testing the Iggy Flink connectors end-to-end.

## Prerequisites

Before running the tests, ensure you have:

1. **Docker & Docker Compose** - For running test infrastructure
2. **Rust & Cargo** - For building from source
3. **Bash** - For running test scripts
4. **Make** (optional) - For using the Makefile commands

## Quick Start

### Using Make (Recommended)

```bash
# Start infrastructure and run all tests
make test

# Run specific test types
make unit-test          # Unit tests only
make integration-test   # Integration tests with Docker
make e2e-test          # Full end-to-end test

# Manage infrastructure
make docker-up         # Start test services
make docker-down       # Stop test services
```

### Manual Testing

#### 1. Start Test Infrastructure

```bash
cd /Users/chiradip/codes/iggy-my-work/core/connectors/flink
docker-compose up -d
```

This starts:

- Apache Iggy server (ports 8090, 8080)
- Apache Flink cluster (port 8081)
- Apache Kafka (port 9092)
- PostgreSQL (port 5432)
- Elasticsearch (port 9200)

#### 2. Build Connectors (if Rust is installed)

```bash
# Build sink connector
cd ../sinks/flink_sink
cargo build --release

# Build source connector
cd ../sources/flink_source
cargo build --release
```

#### 3. Run Bash Test Script

```bash
./test/e2e_test.sh
```

## Test Scenarios

### 1. Sink Connector Test (Iggy → Flink)

Tests data flow from Iggy to Flink/external systems:

1. **Basic Flow**: Send messages to Iggy, verify they reach Kafka via Flink
2. **Batch Processing**: Test batching configurations
3. **Error Handling**: Send invalid messages, verify error recovery
4. **Checkpointing**: Verify Flink checkpoint creation and recovery
5. **Exactly-Once**: Test duplicate prevention

### 2. Source Connector Test (Flink → Iggy)

Tests data flow from external systems to Iggy via Flink:

1. **Basic Flow**: Send messages to Kafka, verify they reach Iggy
2. **State Management**: Test offset tracking and recovery
3. **Multiple Sources**: Test pattern matching for multiple topics
4. **Watermarking**: Test event time processing

### 3. Bidirectional Test

Full round-trip test:

```text
Iggy → Flink Sink → Kafka → Flink Source → Iggy
```

### 4. Performance Test

Tests throughput and latency:

- Sends 1000+ messages
- Measures throughput (messages/second)
- Monitors resource usage

## Verification Steps

### Check Iggy Messages

```bash
# List streams
curl -u iggy:iggy http://localhost:8080/streams

# Poll messages
iggy --transport tcp --username iggy --password iggy \
  message poll --consumer 1 --offset 0 --message-count 10 \
  test_stream input_topic 1
```

### Check Flink Jobs

```bash
# View Flink dashboard
open http://localhost:8081

# Check jobs via API
curl http://localhost:8081/jobs
```

### Check Kafka Topics

```bash
# List topics
docker exec kafka kafka-topics --list \
  --bootstrap-server localhost:9092

# Consume messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic test-output \
  --from-beginning
```

## Test Configuration

### Example Test Config (`test/config.toml`)

```toml
[iggy]
address = "localhost:8090"
username = "iggy"
password = "iggy"

[sinks.flink_test]
enabled = true
path = "../sinks/flink_sink/target/release/libiggy_connector_flink_sink"

[[sinks.flink_test.streams]]
stream = "test_stream"
topics = ["input_topic"]
schema = "json"

[sinks.flink_test.config]
flink_cluster_url = "http://localhost:8081"
job_name = "test-job"
sink_type = "kafka"
target = "test-output"
```

## Troubleshooting

### Common Issues

1. **Docker services not starting**

   ```bash
   # Check logs
   docker-compose logs <service-name>

   # Restart services
   docker-compose restart
   ```

2. **Connection refused errors**

   ```bash
   # Verify services are running
   docker ps

   # Check network connectivity
   docker network ls
   ```

3. **Build failures**

   ```bash
   # Clean and rebuild
   cargo clean
   cargo build --release
   ```

4. **Test failures**

   ```bash
   # Run with debug logging
   RUST_LOG=debug ./test/e2e_test.sh
   ```

### Debug Commands

```bash
# View connector logs
docker logs iggy-server

# Check Flink job status
curl http://localhost:8081/jobs/<job-id>

# Monitor Kafka consumer groups
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list

# Check Elasticsearch indices
curl http://localhost:9200/_cat/indices
```

## Continuous Integration

### GitHub Actions Workflow

```yaml
name: Test Flink Connectors

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2

      - name: Setup Rust
        uses: actions-rs/toolchain@v1
        with:
          toolchain: stable

      - name: Start services
        run: docker-compose up -d

      - name: Build connectors
        run: make build

      - name: Run tests
        run: make test

      - name: Cleanup
        run: docker-compose down -v
```

## Performance Benchmarks

Expected performance metrics:

| Metric | Target | Actual |
|--------|--------|--------|
| Throughput (msg/s) | 10,000+ | TBD |
| Latency p50 (ms) | <10 | TBD |
| Latency p99 (ms) | <100 | TBD |
| Memory Usage | <500MB | TBD |
| CPU Usage | <50% | TBD |

## Test Reports

Test results are output in the following formats:

1. **Console Output**: Colored terminal output with pass/fail status
2. **JSON Report**: `test-results.json` with detailed metrics
3. **JUnit XML**: Compatible with CI systems (optional)

## Contributing

To add new tests:

1. Add test script to the `test/` directory
2. Update test scenarios in this README
3. Add corresponding integration test in Rust
4. Update CI workflow if needed

## Support

For issues or questions:

- Check logs in `docker-compose logs`
- Review troubleshooting section above
- Open an issue on GitHub
