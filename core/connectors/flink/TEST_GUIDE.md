# Flink Connector Testing Guide

## Directory Structure

All tests should be run from the main Flink connector directory:
```bash
cd /Users/chiradip/codes/iggy-my-work/core/connectors/flink
```

## Available Test Scripts

### 1. Quick Test (Recommended First)
```bash
./test/quick_test.sh
```
- Checks if services are running
- Tests Flink REST APIs
- Verifies Iggy CLI
- No compilation needed

### 2. Full Integration Test
```bash
./test/full_integration_test.sh
```
- Complete end-to-end test
- Creates streams and topics
- Sends test messages
- Verifies all components

### 3. Iggy CLI Test
```bash
./test/iggy_cli_test.sh
```
- Tests iggy CLI functionality
- Creates test streams/topics
- Sends and polls messages

### 4. Flink Verification
```bash
./test/verify_flink.sh
```
- Detailed Flink cluster inspection
- Shows running jobs
- Checks uploaded JARs
- Monitors metrics

### 5. Connector API Test (Python)
```bash
python3 ./test/connector_flink_test.py
```
- Tests all Flink REST APIs
- Shows what connectors do
- Requires Python 3 with requests

## Starting Infrastructure

```bash
# Start all services (Flink, Iggy, Kafka)
docker-compose up -d

# Wait for services to be ready
sleep 10

# Check status
docker-compose ps
```

## Building Components

```bash
# Build connectors
./build.sh build

# Build iggy CLI (if needed)
cd /Users/chiradip/codes/iggy/core/cli && cargo build --release
```

## Common Issues & Solutions

### Issue: "cp: src/main.rs: No such file or directory"
**Solution**: Fixed in latest version - use `quick_test.sh` instead

### Issue: "iggy: command not found"
**Solution**: The iggy CLI is at `/Users/chiradip/codes/iggy/target/release/iggy`

### Issue: Services not running
**Solution**:
```bash
docker-compose down -v  # Clean stop
docker-compose up -d    # Start fresh
```

## Verifying Everything Works

Run this sequence:
```bash
# 1. Start services
docker-compose up -d

# 2. Quick check
./test/quick_test.sh

# 3. Full test (if quick test passes)
./test/full_integration_test.sh
```

## Monitoring

- **Flink UI**: http://localhost:8081
- **Flink logs**: `docker logs -f flink-jobmanager`
- **Iggy messages**: Use iggy CLI to poll messages
- **Connector logs**: `RUST_LOG=debug cargo run --bin iggy-connectors`

## What's Verified

✅ Flink cluster connectivity
✅ Job listing and monitoring
✅ Checkpoint/savepoint triggering
✅ Iggy server connectivity
✅ Stream/topic creation
✅ Message send/receive
✅ Connector REST API integration

## Next Steps

1. Create a connector configuration file
2. Run the connector runtime
3. Monitor data flow between Iggy and Flink
4. Check Flink UI for job activity