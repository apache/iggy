# Iggy Examples

This directory contains comprehensive sample applications that showcase various usage patterns of the Iggy client SDK. To learn more about building applications with Iggy, please refer to the [getting started](https://iggy.apache.org/docs/introduction/getting-started) guide.

## Running Examples

To run any example, first start the server with `cargo run --bin iggy-server` and then run the desired example.

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

Demonstrates fundamental client connection, authentication, batch message sending, and polling with support for TCP/QUIC/HTTP protocols.

## Message Pattern Examples

### Message Headers

Shows metadata management using custom headers:

```bash
dotnet run --project  examples/csharp/src/MessageHeaders/Iggy_SDK.Examples.MessageHeaders.Producer
dotnet run --project  examples/csharp/src/MessageHeaders/Iggy_SDK.Examples.MessageHeaders.Consumer
```

Demonstrates using HeaderKey/HeaderValue for message metadata instead of payload-based typing, with header-based message routing.

### Message Envelopes

JSON envelope pattern for polymorphic message handling:

```bash
dotnet run --project  examples/csharp/src/MessageEnvelope/Iggy_SDK.Examples.MessageEnvelope.Producer
dotnet run --project  examples/csharp/src/MessageEnvelope/Iggy_SDK.Examples.MessageEnvelope.Consumer
```

Uses MessagesGenerator to create OrderCreated, OrderConfirmed, and OrderRejected messages wrapped in JSON envelopes for type identification.

## Example Structure

All examples can be executed directly from the repository. Follow these steps:

1. **Start the Iggy server**: `cargo run --bin iggy-server`
2. **Run desired example**: `dotnet run --project examples/csharp/src/xxx`
3. **Check source code**

These examples use IggyClient with TCP transport and demonstrate automatic stream/topic creation with basic message handling.

The examples are automatically tested via `scripts/run-csharp-examples-from-readme.sh` to ensure they remain functional and up-to-date with the latest API changes.
