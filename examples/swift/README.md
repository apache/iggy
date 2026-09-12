# Iggy Swift Examples

Sample applications built on the Swift SDK in `foreign/swift`. To learn more about building applications
with Iggy, please refer to the [getting started](https://iggy.apache.org/docs/introduction/getting-started) guide.

## Running Examples

To run any example, first start a VSR server and then run the desired example.

For server configuration options and help:

```bash
cargo run --bin iggy-server -- --help
```

You can also customize the server using environment variables:

```bash
## Example: Enable HTTP transport and set custom address
IGGY_HTTP_ENABLED=true IGGY_TCP_ADDRESS=127.0.0.1:8090 cargo run --bin iggy-server
```

You can run multiple producers and consumers simultaneously to observe how messages are distributed across clients.

## Basic Examples

### Getting Started

Perfect introduction for newcomers to Iggy: the producer creates the sample stream and topic and sends a few
batches, the consumer reads them back through an `AsyncSequence`.

```bash
swift run --package-path getting-started producer
swift run --package-path getting-started consumer
```

## TLS Examples

To test with a TLS-enabled server, start the server with TLS configured (see main README), then run:

```bash
swift run --package-path getting-started producer --tcp-server-address localhost:8090 --tls --tls-ca-file ../../core/certs/iggy_ca_cert.pem
swift run --package-path getting-started consumer --tcp-server-address localhost:8090 --tls --tls-ca-file ../../core/certs/iggy_ca_cert.pem
```

## Example Structure

All examples can be executed directly from the repository. Follow these steps:

1. **Start the Iggy server**: the Swift SDK speaks the VSR wire protocol, so the
   examples need a VSR server.
   `cargo run --bin iggy-server`
2. **Run desired example**: `swift run --package-path <example-dir> <target>`, with the optional flags
   `--tcp-server-address host:port`, `--tls`, `--tls-ca-file path`, and `--tls-domain name`
3. **Check source code**: Examples include detailed comments explaining concepts and usage patterns

Each example directory is a Swift package that depends on the SDK by path, so it builds straight from the
checkout. The examples use `IggyClient` over TCP with automatic sign-in, an `IggyProducer` that creates the
stream and topic when they are missing, and an `IggyConsumer` iterated with `for try await`.

The examples are automatically tested via `scripts/run-examples-from-readme.sh --language swift` to ensure
they remain functional and up-to-date with the latest API changes.
