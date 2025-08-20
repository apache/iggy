# Iggy Examples

This directory contains comprehensive sample applications that showcase various usage patterns of the Iggy client SDK, from basic operations to advanced multi-tenant scenarios. To learn more about building applications with Iggy, please refer to the [getting started](https://iggy.apache.org/docs/introduction/getting-started) guide.

## Running Examples

To run any example, first start the server with `cargo run --bin iggy-server` and then run the desired example using `cargo run --example EXAMPLE_NAME`.

For server configuration options and help:

```bash
cargo run --bin iggy-server -- --help
```

You can also customize the server using environment variables:

```bash
## Example: Enable HTTP transport and set custom address
IGGY_HTTP_ENABLED=true IGGY_TCP_ADDRESS=0.0.0.0:8090 cargo run --bin iggy-server
```

You can run multiple producers and consumers simultaneously to observe how messages are distributed across clients. Most examples support configurable options via the [Args](https://github.com/apache/iggy/blob/master/examples/rust/src/shared/args.rs) struct, including transport protocol, stream/topic/partition settings, consumer ID, message size, and more.

![sample](../../assets/sample.png)

## Basic Examples

### Getting Started

Perfect introduction for newcomers to Iggy:

```bash
cargo run --example getting-started-producer
cargo run --example getting-started-consumer
```

These examples use IggyClientBuilder with TCP transport and demonstrate automatic stream/topic creation with basic message handling.

### Basic Usage

Core functionality with detailed configuration options:

```bash
cargo run --example basic-producer
cargo run --example basic-consumer
```

Demonstrates fundamental client connection, authentication, batch message sending, and polling with support for TCP/QUIC/HTTP protocols.

## Message Pattern Examples

### Message Headers

Shows metadata management using custom headers:

```bash
cargo run --example message-headers-producer
cargo run --example message-headers-consumer
```

Demonstrates using HeaderKey/HeaderValue for message metadata instead of payload-based typing, with header-based message routing.

### Message Envelopes

JSON envelope pattern for polymorphic message handling:

```bash
cargo run --example message-envelope-producer
cargo run --example message-envelope-consumer
```

Uses MessagesGenerator to create OrderCreated, OrderConfirmed, and OrderRejected messages wrapped in JSON envelopes for type identification.

## Advanced Examples

### Multi-Tenant Architecture

Complex example demonstrating enterprise-level isolation:

```bash
cargo run --example multi-tenant-producer
cargo run --example multi-tenant-consumer
```

Features multiple tenant setup, user creation with stream-specific permissions, concurrent producers/consumers across tenants, and security isolation. Configurable via environment variables (TENANTS_COUNT, PRODUCERS_COUNT, etc.).

### New SDK API

Modern, ergonomic SDK usage patterns:

```bash
cargo run --example new-sdk-producer
cargo run --example new-sdk-consumer
```

Showcases the newer SDK APIs with simplified setup, automatic topic creation, consumer groups, and AutoCommit configuration.

### High-Volume Data Generation

Testing and benchmarking support:

```bash
cargo run --example sink-data-producer
```

Produces high-throughput data (1000 messages per batch) with realistic user records, configurable via environment variables for connection and stream settings.

## Stream Builder Examples

### Basic Stream Management

IggyStream abstraction for producer/consumer pairs:

```bash
cargo run --example stream-basic
cargo run --example stream-producer
cargo run --example stream-consumer
```

### Advanced Configuration

Comprehensive configuration examples with detailed documentation:

```bash
cargo run --example stream-producer-config
cargo run --example stream-consumer-config
```

These examples document all available configuration options including partitioning strategies, retry policies, batching, AutoCommit strategies, polling strategies, and retry mechanisms.

## Example Structure

All examples can be executed directly from the repository. Follow these steps:

1. **Start the Iggy server**: `cargo run --bin iggy-server`
2. **Run desired example**: `cargo run --example EXAMPLE_NAME`
3. **Check source code**: Examples include detailed comments explaining concepts and usage patterns

Most examples use shared utilities from `examples/rust/src/shared/` including:

- Message type definitions (orders, events)
- Message generation utilities
- Common argument parsing
- Client setup helpers

The examples are automatically tested via `scripts/run-rust-examples-from-readme.sh` to ensure they remain functional and up-to-date with the latest API changes.
