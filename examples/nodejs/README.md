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
npm install
```

## Basic Examples

### Getting Started

Perfect introduction for newcomers to Iggy:

```bash
npm run test:getting-started:producer
npm run test:getting-started:consumer
```

JavaScript versions:

```bash
npm run test:js:getting-started:producer
npm run test:js:getting-started:consumer
```

### Basic Usage

Core functionality with detailed configuration options:

```bash
npm run test:basic:producer
npm run test:basic:consumer
```

JavaScript versions:

```bash
npm run test:js:basic:producer
npm run test:js:basic:consumer
```

Demonstrates fundamental client connection, authentication, batch message sending, and polling with support for TCP transport.