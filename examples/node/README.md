# Iggy Examples

This directory contains comprehensive sample applications that showcase various usage patterns of the Iggy client SDK for Node.js, from basic operations to advanced scenarios. To learn more about building applications with Iggy, please refer to the [getting started](https://iggy.apache.org/docs/introduction/getting-started) guide.

## Running Examples

These examples target server 0.9.0 and use the SDK from the same checkout. Start the server from the repository root in a separate terminal:

```bash
cargo run --bin iggy-server -- --fresh --with-default-root-credentials
```

`--fresh` deletes existing local server data. Explicit credential environment variables override the default-root flag, and bootstrap settings do not replace recovered credentials. See the repository [development prerequisites](../../README.md#development).

For server configuration options and help, from the repository root:

```bash
cargo run --bin iggy-server -- --help
```

You can also customize the server using environment variables:

```bash
# Enable HTTP transport and set the TCP address
IGGY_HTTP_ENABLED=true IGGY_TCP_ADDRESS=127.0.0.1:8090 \
  cargo run --bin iggy-server -- --fresh --with-default-root-credentials
```

From the repository root, build the local SDK before installing the examples:

```bash
npm --prefix foreign/node ci
npm --prefix foreign/node run build
npm --prefix examples/node ci
cd examples/node
```

Run the following scripts from `examples/node`. Set `DEBUG=iggy:examples*` to see progress. The separate producer and consumer scripts do not consistently share resources: envelope/header consumers create empty topics, and the tenant producer removes its streams before exiting. Getting-started/basic producers spread batches over five partitions while their consumers poll partition 0. The stream-builder example below is a self-contained send-and-consume walkthrough.

## Basic Examples

### Getting Started

Producer and consumer scripts:

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

Demonstrates a JSON envelope containing `message_type` and a serialized `payload`.

### Message Headers

Dispatching by a message type stored in the payload:

```bash
npm run test:message-headers:producer
npm run test:message-headers:consumer
```

This example stores `messageType` and `data` in a JSON payload wrapper.

### Multi-Tenant

Multi-tenant application patterns:

```bash
npm run test:multi-tenant:producer
npm run test:multi-tenant:consumer
```

Each script uses one client with the supplied credentials to access separate tenant streams. Consumers poll with `Consumer.Single`.

### Stream Builder

Creating a stream and topic, then sending and consuming messages:

```bash
npm run test:stream-builder
```

Uses the standard client API to send and consume three messages, then deletes the resources it created.

### Sink Data Producer

Generating messages for sink connectors:

```bash
npm run test:sink-data-producer
```

Produces 100 batches of 100-499 JSON records. On completion it deletes the selected topic and stream, including resources that already existed.

## TLS Examples

### TCP/TLS

Producer and consumer examples using TLS-encrypted TCP connections with custom CA certificates:

```bash
npm run test:tcp-tls:producer
npm run test:tcp-tls:consumer
```

These examples require a TLS-enabled Iggy server. From the repository root:

```bash
IGGY_TCP_TLS_ENABLED=true \
IGGY_TCP_TLS_CERT_FILE=core/certs/iggy_cert.pem \
IGGY_TCP_TLS_KEY_FILE=core/certs/iggy_key.pem \
cargo run --bin iggy-server -- --fresh --with-default-root-credentials
```

The clients use `transport: 'TLS'`, connect to `localhost`, and load `../../core/certs/iggy_ca_cert.pem` relative to `examples/node`. These are repository development certificates.

## Local checks

```bash
npm run test:unit
npm run lint
```
