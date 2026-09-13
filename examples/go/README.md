# Iggy Examples

This directory contains a getting-started producer and consumer using the Go SDK over TCP, with optional TLS. To learn more about building applications with Iggy, please refer to the [getting started](https://iggy.apache.org/docs/introduction/getting-started) guide.

## Running Examples

Use Go 1.25 or newer. The local `go.mod` replaces the SDK dependency with `../../foreign/go`, so build the server from the same checkout. Start it from the repository root, then run the Go commands from `examples/go`.

For disposable development data and no root credential environment overrides:

```bash
cargo run --bin iggy-server -- --fresh --with-default-root-credentials
```

`--fresh` wipes this replica's local data directory. `IGGY_ROOT_USERNAME` and `IGGY_ROOT_PASSWORD` take precedence over the flag. Bootstrap settings do not replace recovered credentials, including credentials recovered from cluster peers. The examples use `iggy`/`iggy` and numeric stream/topic IDs `0`; run the producer first, before creating any other streams or topics.

For server configuration options and help:

```bash
cargo run --bin iggy-server -- --help
```

You can also customize the server using environment variables:

```bash
## Example: Enable HTTP transport and set custom address
IGGY_HTTP_ENABLED=true IGGY_HTTP_ADDRESS=127.0.0.1:3000 cargo run --bin iggy-server -- --with-default-root-credentials
```

Each producer sends 5 batches of 10 messages to partition `0`. Each consumer reads that partition independently and exits after 5 batches; multiple consumers read the same messages and do not join a consumer group.

![sample](../../assets/sample.png)

## Basic Examples

### Getting Started

Perfect introduction for newcomers to Iggy:

```bash
go run ./getting-started/producer/main.go
go run ./getting-started/consumer/main.go
```

## TLS Examples

From the repository root, start a disposable TLS server with the development certificate pair and the same credential prerequisites:

```bash
IGGY_TCP_TLS_ENABLED=true \
IGGY_TCP_TLS_CERT_FILE=core/certs/iggy_cert.pem \
IGGY_TCP_TLS_KEY_FILE=core/certs/iggy_key.pem \
cargo run --bin iggy-server -- --fresh --with-default-root-credentials
```

These certificates are for development only. From `examples/go`, run:

```bash
go run ./getting-started/producer/main.go --tcp-server-address localhost:8090 --tls --tls-ca-file ../../core/certs/iggy_ca_cert.pem
go run ./getting-started/consumer/main.go --tcp-server-address localhost:8090 --tls --tls-ca-file ../../core/certs/iggy_ca_cert.pem
```

## Example Structure

All examples can be executed directly from the repository. Follow these steps:

1. **Start the Iggy server**: the Go SDK speaks the VSR wire protocol, so the
   examples need a VSR server.
   Use the source-server command and prerequisites above.
2. **Run the producer, then the consumer**: use the commands above from `examples/go`.
3. **Check source code**: Examples include detailed comments explaining concepts and usage patterns

These examples use IggyClient with TCP transport and demonstrate automatic stream/topic creation with basic message handling.

The examples are automatically tested via `scripts/run-examples-from-readme.sh --language go` to ensure they remain functional and up-to-date with the latest API changes.
