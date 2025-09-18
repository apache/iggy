# Flink Connectors Build Guide

This guide provides instructions for building and testing the Iggy Flink connectors.

## Clean and Rebuild Commands

### 1. Clean Everything (Recommended)

```bash
cd /Users/chiradip/codes/chiradip-iggy-fork/iggy
cargo clean
```

### 2. Rebuild All Flink Components

```bash
# Build all Flink packages in release mode
cargo build --package iggy-connector-flink-sink \
            --package iggy-connector-flink-source \
            --package flink-data-producer \
            --release

# Or in debug mode (faster compile, slower runtime)
cargo build --package iggy-connector-flink-sink \
            --package iggy-connector-flink-source \
            --package flink-data-producer
```

### 3. Alternative: Clean and Build in One Command

```bash
cd /Users/chiradip/codes/chiradip-iggy-fork/iggy && \
cargo clean && \
cargo build --package iggy-connector-flink-sink \
            --package iggy-connector-flink-source \
            --package flink-data-producer \
            --release
```

### 4. Test After Building

```bash
# Run tests for the connectors
cargo test --package iggy-connector-flink-sink
cargo test --package iggy-connector-flink-source

# Run the example producer
cargo run --package flink-data-producer -- --help
```

### 5. Build Only Specific Component

```bash
# Clean and build only sink
cargo clean --package iggy-connector-flink-sink
cargo build --package iggy-connector-flink-sink --release

# Clean and build only source
cargo clean --package iggy-connector-flink-source
cargo build --package iggy-connector-flink-source --release

# Clean and build only example
cargo clean --package flink-data-producer
cargo build --package flink-data-producer --release
```

### 6. Location of Built Libraries

After building, the compiled libraries will be at:

```bash
# Release builds (optimized)
target/release/libiggy_connector_flink_sink.dylib  # macOS
target/release/libiggy_connector_flink_sink.so     # Linux

target/release/libiggy_connector_flink_source.dylib  # macOS
target/release/libiggy_connector_flink_source.so     # Linux

target/release/flink-data-producer  # Executable

# Debug builds (faster compile)
target/debug/libiggy_connector_flink_sink.dylib
target/debug/libiggy_connector_flink_source.dylib
target/debug/flink-data-producer
```

## Quick Reference

### Full Clean and Rebuild (Most Common)
```bash
cd /Users/chiradip/codes/chiradip-iggy-fork/iggy && \
cargo clean && \
cargo build --package iggy-connector-flink-sink \
            --package iggy-connector-flink-source \
            --package flink-data-producer \
            --release
```

### Run All Tests
```bash
cargo test --package iggy-connector-flink-sink --package iggy-connector-flink-source
```

### Quick Build (No Clean)
```bash
cargo build --package iggy-connector-flink-sink \
            --package iggy-connector-flink-source \
            --package flink-data-producer \
            --release
```

## Troubleshooting

### Build Warnings
If you encounter warnings during build:
1. Check that all dependencies are up to date
2. Run `cargo update` to update dependencies
3. Ensure you're using a compatible Rust version

### Test Failures
If tests fail:
1. Ensure Iggy server is not running (tests may conflict with a running instance)
2. Check that required ports are available
3. Review test output for specific error messages

### Performance Tips
- Use `--release` flag for production builds (optimized)
- Use debug builds (no `--release`) during development for faster compilation
- Use `cargo check` for quick syntax/type checking without full compilation

## Related Documentation

- [Flink Sink Connector README](../sinks/flink_sink/README.md)
- [Flink Source Connector README](../sources/flink_source/README.md)
- [Flink Data Producer README](../../../examples/flink-data-producer/README.md)
- [End-to-End Testing Guide](test-e2e.sh)