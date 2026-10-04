# Iggy Examples

This directory contains comprehensive sample applications that showcase various usage patterns of the Iggy client SDK. To learn more about building applications with Iggy, please refer to the [getting started](https://iggy.apache.org/docs/introduction/getting-started) guide.

## Running Examples

Run from the Iggy repository root with .NET 10, using the server and SDK from the same checkout.
Initialize a new server data directory with the credentials used by the examples:

```bash
IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy cargo run --bin iggy-server
```

For an existing server, use its configured credentials. Run each producer before its consumer.

For server configuration options and help:

```bash
cargo run --bin iggy-server -- --help
```

You can also customize the server using environment variables:

```bash
# Enable HTTP transport and set its address
IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy IGGY_HTTP_ENABLED=true IGGY_HTTP_ADDRESS=127.0.0.1:3000 cargo run --bin iggy-server
```

## Basic Examples

### Getting Started

Perfect introduction for newcomers to Iggy:

```bash
dotnet run --project  examples/csharp/src/GettingStarted/Iggy_SDK.Examples.GettingStarted.Producer
dotnet run --project  examples/csharp/src/GettingStarted/Iggy_SDK.Examples.GettingStarted.Consumer
```

These examples use IIggyClient with TCP transport and demonstrate stream/topic creation with basic message handling.

### Basic Usage

Core functionality with detailed configuration options:

```bash
dotnet run --project  examples/csharp/src/Basic/Iggy_SDK.Examples.Basic.Producer
dotnet run --project  examples/csharp/src/Basic/Iggy_SDK.Examples.Basic.Consumer
```

Demonstrates fundamental client connection, authentication, batch message sending, and polling over TCP or HTTP.

## Message Pattern Examples

### Message Headers

Shows metadata management using custom headers:

```bash
dotnet run --project  examples/csharp/src/MessageHeaders/Iggy_SDK.Examples.MessageHeaders.Producer
dotnet run --project  examples/csharp/src/MessageHeaders/Iggy_SDK.Examples.MessageHeaders.Consumer
```

Demonstrates using HeaderKey/HeaderValue for message metadata instead of payload-based typing, with header-based message type dispatch in the consumer.

### Message Envelopes

JSON envelope pattern for polymorphic message handling:

```bash
dotnet run --project  examples/csharp/src/MessageEnvelope/Iggy_SDK.Examples.MessageEnvelope.Producer
dotnet run --project  examples/csharp/src/MessageEnvelope/Iggy_SDK.Examples.MessageEnvelope.Consumer
```

Uses MessagesGenerator to create OrderCreated, OrderConfirmed, and OrderRejected messages wrapped in JSON envelopes for type identification.

## Security Examples

### TCP/TLS

Demonstrates secure TLS-encrypted TCP connections:

```bash
dotnet run --project  examples/csharp/src/TcpTls/Iggy_SDK.Examples.TcpTls.Producer
dotnet run --project  examples/csharp/src/TcpTls/Iggy_SDK.Examples.TcpTls.Consumer
```

Uses `IggyClientConfigurator` with `TlsSettings` (Enabled, Hostname, CertificatePath) to establish TLS-encrypted TCP connections with CA certificate verification. Run from the repository root so the CA path resolves. Start the server with the example certificate:

```bash
IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy \
IGGY_TCP_TLS_ENABLED=true \
IGGY_TCP_TLS_CERT_FILE=core/certs/iggy_cert.pem \
IGGY_TCP_TLS_KEY_FILE=core/certs/iggy_key.pem \
cargo run --bin iggy-server
```

## Example Structure

All examples can be executed directly from the repository. Follow these steps:

1. **Start the matching server** with the credentials shown above
2. **Run the producer and consumer** using the concrete commands above
3. **Check source code**

These examples use IggyClient with TCP transport and demonstrate automatic stream/topic creation with basic message handling.

The examples are automatically tested via `scripts/run-examples-from-readme.sh --language csharp` to ensure they remain functional and up-to-date with the latest API changes.
