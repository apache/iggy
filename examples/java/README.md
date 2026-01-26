# Iggy Java Examples

This directory contains comprehensive sample applications that showcase various usage patterns of the Iggy java client SDK, from basic operations to advanced multi-tenant scenarios.

Java 17 and Gradle 9.2.1 are recommended for running the examples.

## Running Examples

Iggy requires valid credentials to authenticate client requests. The examples assume that the server is using the default root credentials, which can be enabled in one of two ways:

1. Start the server with default credentials:

   ```bash
   cargo run --bin iggy-server -- --with-default-root-credentials
   ```

2. Set the appropriate environment variables before starting the server with `cargo run --bin iggy-server`:

   macOS/Linux:

   ```bash
   export IGGY_ROOT_USERNAME=iggy
   export IGGY_ROOT_PASSWORD=iggy
   ```

   Windows(Powershell):

   ```bash
   $env:IGGY_ROOT_USERNAME = "iggy"
   $env:IGGY_ROOT_PASSWORD = "iggy"
   ```

> **Note** <br>
> This setup is intended only for development and testing, not production use.

By default, all server data is stored in the `local_data` directory (this can be changed via `system.path` in `config.toml`).

Root credentials are applied **only on the very first startup**, when no data directory exists yet.
Once the server has created and populated the data directory, the existing stored credentials will always be used, and supplying the `--with-default-root-credentials` flag or setting the environment variables will no longer override them.

If the server has already been started once and your example returns `Error: InvalidCredentials`, then this means the stored credentials differ from the defaults.

You can reset the credentials in one of two ways:

1. Delete the existing data directory, then start the server again with the default-credential flag or environment variables.
2. Use the `--fresh` flag to force a reset:

   ```bash
   cargo run --bin iggy-server -- --with-default-root-credentials --fresh
   ```

   This will ignore any existing data directory and re-initialize it with the default credentials.

For server configuration options and help:

```bash
cargo run --bin iggy-server -- --help
```

You can also customize the server using environment variables:

```bash
## Example: Enable HTTP transport and set custom address
IGGY_HTTP_ENABLED=true IGGY_TCP_ADDRESS=0.0.0.0:8090 cargo run --bin iggy-server
```

## Basic Examples

### Getting Started

A good introduction for newcomers to Iggy:

```bash
gradle runGettingStartedProducer
gradle runGettingStartedConsumer
```

### Message Headers

Shows metadata management using custom headers:

```bash
gradle runMessageHeadersProducer
gradle runMessageHeadersConsumer
```

Demonstrates using header keys and values for message metadata instead of payload-based typing, with header-based message routing.

### Message Envelopes

JSON envelope pattern for polymorphic message handling:

```bash
gradle runMessageEnvelopeProducer
gradle runMessageEnvelopeConsumer
```

Uses MessagesGenerator to create OrderCreated, OrderConfirmed, and OrderRejected messages wrapped in JSON envelopes for type identification.

## Advanced Examples

### Multi-Tenant Architecture

Complex example demonstrating enterprise-level isolation:

```bash
gradle runMultiTenantProducer
gradle runMultiTenantConsumer
```

Features multiple tenant setup, user creation with stream-specific permissions, concurrent producers/consumers across tenants, and security isolation.

### High-Volume Data Generation

Testing and benchmarking support:

```bash
gradle runSinkDataProducer
```

Produces high-throughput data (1000+ messages per batch) with realistic user records.

## Stream Builder Examples

### Stream Builder

Building streams with advanced configuration:

```bash
gradle runStreamBasic
```

Shows how to use the stream builder API to create and configure streams with custom settings.

## Async Client

The following example demonstrates how to use the asynchronous client:

Async producer example:

```bash
gradle runAsyncProducer
```

Async consumer example:

```bash
gradle runAsyncConsumerExample
```
