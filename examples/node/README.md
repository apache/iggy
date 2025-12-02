# Iggy Examples

This directory contains comprehensive sample applications that showcase various usage patterns of the Iggy client SDK for Node.js, from basic operations to advanced scenarios. To learn more about building applications with Iggy, please refer to the [getting started](https://iggy.apache.org/docs/introduction/getting-started) guide.

## Running Examples

To run any example, first start the server with

```bash
# Using latest release
docker run --rm -p 8080:8080 -p 3000:3000 -p 8090:8090 apache/iggy:latest

# Or build from source (recommended for development)
cd ../../ && cargo run --bin iggy-server
```

For server configuration options and help:

```bash
cargo run --bin iggy-server -- --help
```

You can also customize the server using environment variables:

```bash
## Example: Enable HTTP transport and set custom address
IGGY_HTTP_ENABLED=true IGGY_TCP_ADDRESS=0.0.0.0:8090 cargo run --bin iggy-server
```

and then install Node.js dependencies:

```bash
npm ci
```

## Basic Examples

### Getting Started

Perfect introduction for newcomers to Iggy:

```bash
npm run test:getting-started:producer
npm run test:getting-started:consumer
```

### Basic Usage

Core functionality with detailed configuration options:

```bash
npm run test:basic:producer
npm run test:basic:consumer
```

Demonstrates fundamental client connection, authentication, batch message sending, and polling with support for TCP transport.

### Message Envelope

Working with message envelopes:

```bash
npm run test:message-envelope:producer
npm run test:message-envelope:consumer
```

Demonstrates how to create and handle message envelopes with custom metadata and headers.

### Message Headers

Using message headers:

```bash
npm run test:message-headers:producer
npm run test:message-headers:consumer
```

Shows how to attach and retrieve custom headers with messages for additional context and metadata.

### Multi-Tenant

Multi-tenant application patterns:

```bash
npm run test:multi-tenant:producer
npm run test:multi-tenant:consumer
```

Demonstrates how to implement multi-tenant patterns using separate streams and consumer groups.

### Stream Builder

Building streams with advanced configuration:

```bash
npm run test:stream-builder
```

Shows how to use the stream builder API to create and configure streams with custom settings.

### Sink Data Producer

Sending data to external sinks:

```bash
npm run test:sink-data-producer
```

Demonstrates how to produce data that can be consumed by external sinks for integration with other systems.
