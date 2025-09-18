# Flink Data Producer for Iggy

A data producer example that generates realistic sensor data and sends it to Iggy streams, designed for testing Flink connectors.

## Prerequisites

1. **Iggy Server** must be running on your machine
2. **Rust** and **Cargo** installed

## Quick Start

### Step 1: Ensure Iggy is Running

The easiest way is to use our setup script:

```bash
cd examples/flink-data-producer
./setup-iggy.sh
```

Or manually start Iggy:

```bash
# From the iggy root directory
cargo run --package server

# Or if already built
./target/release/iggy_server
```

### Step 2: Run the Producer

```bash
# Run with default settings (1000 messages to flink-demo stream)
cargo run --package flink-data-producer

# Run in continuous mode (keeps producing until stopped)
cargo run --package flink-data-producer -- --continuous

# Run with custom settings
cargo run --package flink-data-producer -- \
    --stream-id my-stream \
    --topic-id my-topic \
    --num-messages 5000 \
    --batch-size 100
```

## Command Line Options

```
Options:
  --stream-id <STRING>         Stream name [default: flink-demo]
  --topic-id <STRING>          Topic name [default: sensor-data]
  --interval-ms <MS>           Interval between batches in milliseconds [default: 1000]
  --batch-size <SIZE>          Number of messages per batch [default: 100]
  -c, --continuous             Run in continuous mode (never stops)
  -n, --num-messages <COUNT>   Total number of messages to send [default: 1000]
  --username <STRING>          Iggy username [default: iggy]
  --password <STRING>          Iggy password [default: iggy]
  --transport <TYPE>           Transport protocol (tcp/http) [default: tcp]
  --tcp-server-address <ADDR>  TCP server address [default: 127.0.0.1:8090]
  --http-api-url <URL>         HTTP API URL [default: http://localhost:3000]
  -h, --help                   Print help
  -V, --version                Print version
```

## Data Format

The producer generates sensor data with the following structure:

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": 1704067200000,
  "sensor_id": "sensor-0042",
  "temperature": 23.5,
  "humidity": 65.2,
  "pressure": 1013.25,
  "location": {
    "lat": 37.7749,
    "lon": -122.4194,
    "city": "San Francisco"
  }
}
```

## Common Issues and Solutions

### Error: PartitionNotFound

**Problem**: You see an error like:
```
ERROR iggy::tcp::tcp_client: Received an invalid response with status: 3007 (partition_not_found)
```

**Solution**: This error was fixed by using partition ID 1 (instead of 0). The producer now automatically creates the stream and topic with the correct partition settings.

### Error: Connection Refused

**Problem**: Cannot connect to Iggy server.

**Solution**:
1. Make sure Iggy server is running:
   ```bash
   ./setup-iggy.sh
   ```
2. Check if ports 8090 (TCP) or 8080 (HTTP) are available
3. Verify firewall settings

### Error: Authentication Failed

**Problem**: Invalid username or password.

**Solution**: Use the correct credentials (default: iggy/iggy) or specify custom ones:
```bash
cargo run --package flink-data-producer -- \
    --username myuser \
    --password mypass
```

## Testing with Flink Connectors

This producer is designed to work with the Flink sink and source connectors:

1. **Start Iggy Server**
   ```bash
   ./setup-iggy.sh
   ```

2. **Run the Producer** (generates data)
   ```bash
   cargo run --package flink-data-producer -- --continuous
   ```

3. **Configure Flink Sink** to consume from Iggy:
   - Stream: `flink-demo`
   - Topic: `sensor-data`
   - Partition: 1

4. **Process in Flink** and send results back to Iggy

5. **Configure Flink Source** to read processed data

## Development

### Building from Source

```bash
cd examples/flink-data-producer
cargo build --release
```

### Running Tests

```bash
cargo test --package flink-data-producer
```

### Modifying Data Schema

Edit the `SensorData` struct in `src/main.rs`:

```rust
#[derive(Debug, Serialize, Deserialize)]
struct SensorData {
    // Add your fields here
}
```

## Performance Tuning

- **Increase batch size** for higher throughput:
  ```bash
  cargo run --package flink-data-producer -- --batch-size 1000
  ```

- **Decrease interval** for faster data generation:
  ```bash
  cargo run --package flink-data-producer -- --interval-ms 100
  ```

- **Use multiple producers** in parallel for load testing

## License

Apache License 2.0