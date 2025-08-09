# Elasticsearch Source Connector Tests

This directory contains comprehensive tests for the Elasticsearch source connector with state management functionality.

## Test Structure

- `integration_test.rs` - Integration tests with real Elasticsearch instance
- `performance_test.rs` - Performance and load tests
- `docker-compose.yml` - Docker setup for Elasticsearch test environment
- `run_tests.sh` - Automated test runner script
- `test_config.toml` - Test configuration file

## Prerequisites

- Docker and Docker Compose
- Rust toolchain
- curl (for health checks)

## Running Tests

### Quick Start

Run all tests with the automated script:

```bash
chmod +x tests/run_tests.sh
./tests/run_tests.sh
```

This script will:

1. Start Elasticsearch using Docker Compose
2. Wait for Elasticsearch to be ready
3. Run all integration tests
4. Clean up the test environment

### Manual Setup

1. Start Elasticsearch:

```bash
cd tests
docker-compose up -d elasticsearch
```

2. Wait for Elasticsearch to be ready:

```bash
# Check health
curl http://localhost:9200/_cluster/health
```

3. Run tests:

```bash
# Run integration tests
cargo test --test integration_test -- --nocapture

# Run performance tests
cargo test --test performance_test -- --nocapture

# Run unit tests
cargo test
```

4. Clean up:

```bash
cd tests
docker-compose down
```

## Test Categories

### Integration Tests

Tests that require a real Elasticsearch instance:

- `test_basic_connector_functionality` - Basic connector operations
- `test_state_management_basic` - State management functionality
- `test_state_persistence_and_recovery` - State persistence and recovery
- `test_state_export_import` - State export/import functionality
- `test_state_reset` - State reset functionality
- `test_error_tracking` - Error tracking and handling
- `test_processing_statistics` - Processing statistics collection
- `test_state_manager_utilities` - State manager utility functions
- `test_incremental_processing` - Incremental data processing

### Performance Tests

Tests that measure performance characteristics:

- `test_performance_without_state_management` - Baseline performance
- `test_performance_with_state_management` - Performance with state management
- `test_state_save_performance` - State save operation performance
- `test_memory_usage_with_state_management` - Memory usage monitoring
- `test_concurrent_state_operations` - Concurrent state operations

### Unit Tests

Tests that don't require external dependencies:

- State manager creation and configuration
- File storage operations
- State serialization/deserialization
- Error handling

## Test Environment

### Elasticsearch Configuration

- Version: 8.11.0
- Single node setup
- Security disabled for testing
- Port: 9200 (HTTP), 9300 (Transport)
- Memory: 512MB heap

### Test Data

Tests create and use the following indices:

- `test_logs` - Basic test data
- `perf_test_logs` - Performance test data

### Test Data Structure

```json
{
  "@timestamp": "2024-01-15T10:30:00Z",
  "message": "Test log message",
  "level": "INFO",
  "service": "test-service",
  "user_id": "user-123"
}
```

## Test Results

### Expected Performance Metrics

- **Messages per second**: > 100 (varies by hardware)
- **State save time**: < 100ms average
- **Memory usage**: Stable over time
- **Error recovery**: Graceful handling of failures

### Test Coverage

- ✅ Basic connector functionality
- ✅ State management operations
- ✅ State persistence and recovery
- ✅ Error handling and tracking
- ✅ Performance characteristics
- ✅ Memory usage patterns
- ✅ Concurrent operations
- ✅ Configuration validation

## Troubleshooting

### Common Issues

1. **Elasticsearch not starting**:

   ```bash
   # Check Docker logs
   docker-compose logs elasticsearch
   
   # Check available memory
   docker system df
   ```

2. **Port conflicts**:

   ```bash
   # Check if port 9200 is in use
   lsof -i :9200
   
   # Modify docker-compose.yml to use different ports
   ```

3. **Test timeouts**:

   ```bash
   # Increase timeout in test files
   # Default is 30 seconds for ES startup
   ```

4. **Permission issues**:

   ```bash
   # Make script executable
   chmod +x tests/run_tests.sh
   ```

### Debug Mode

Run tests with verbose output:

```bash
RUST_LOG=debug cargo test --test integration_test -- --nocapture
```

### Individual Test Execution

Run specific tests:

```bash
# Run specific integration test
cargo test --test integration_test test_basic_connector_functionality -- --nocapture

# Run specific performance test
cargo test --test performance_test test_performance_with_state_management -- --nocapture
```

## Continuous Integration

These tests are designed to run in CI environments:

- Docker-based setup ensures consistent environment
- Health checks ensure services are ready
- Automated cleanup prevents resource leaks
- Comprehensive error reporting

### CI Configuration

Example GitHub Actions workflow:

```yaml
name: Elasticsearch Connector Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
      - name: Start Elasticsearch
        run: |
          cd core/connectors/sources/elasticsearch_source/tests
          docker-compose up -d elasticsearch
      - name: Wait for Elasticsearch
        run: |
          timeout 120 bash -c 'until curl -s http://localhost:9200/_cluster/health; do sleep 2; done'
      - name: Run tests
        run: |
          cd core/connectors/sources/elasticsearch_source
          cargo test --test integration_test --test performance_test
      - name: Cleanup
        run: |
          cd core/connectors/sources/elasticsearch_source/tests
          docker-compose down
```

## Contributing

When adding new tests:

1. Follow the existing test structure
2. Use descriptive test names
3. Include proper cleanup
4. Add appropriate assertions
5. Update this documentation

### Test Guidelines

- Use temporary directories for file-based tests
- Clean up resources after tests
- Use realistic test data
- Include performance benchmarks
- Test both success and failure scenarios
