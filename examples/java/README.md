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
./gradlew runGettingStartedProducer
./gradlew runGettingStartedConsumer
```

### Message Headers

Shows metadata management using custom headers:

```bash
./gradlew runMessageHeadersProducer
./gradlew runMessageHeadersConsumer
```

Demonstrates using header keys and values for message metadata instead of payload-based typing, with header-based message routing.

### Message Envelopes

JSON envelope pattern for polymorphic message handling:

```bash
./gradlew runMessageEnvelopeProducer
./gradlew runMessageEnvelopeConsumer
```

Uses MessagesGenerator to create OrderCreated, OrderConfirmed, and OrderRejected messages wrapped in JSON envelopes for type identification.

## Advanced Examples

### Multi-Tenant Architecture

Complex example demonstrating enterprise-level isolation:

```bash
./gradlew runMultiTenantProducer
./gradlew runMultiTenantConsumer
```

Features multiple tenant setup, user creation with stream-specific permissions, concurrent producers/consumers across tenants, and security isolation.

### High-Volume Data Generation

Testing and benchmarking support:

```bash
./gradlew runSinkDataProducer
```

Produces high-throughput data (1000+ messages per batch) with realistic user records.

## Stream Builder Examples

### Stream Builder

Building streams with advanced configuration:

```bash
./gradlew runStreamBasic
```

Shows how to use the stream builder API to create and configure streams with custom settings.

## Async Client Examples

### Async Producer

High-throughput async production with pipelining:

```bash
./gradlew runAsyncProducer
```

Shows:

- CompletableFuture chaining patterns
- Pipelining multiple sends without blocking
- Performance comparison with blocking client

### Async Consumer

Non-blocking async consumption with advanced patterns:

```bash
./gradlew runAsyncConsumer
```

Shows:

- Backpressure management (don't poll faster than you can process)
- Error recovery with exponential backoff
- Thread pool separation (Netty I/O threads vs. processing threads)
- Offset-based polling with CompletableFuture

**CRITICAL ASYNC PATTERN - Thread Pool Management:**

The async client uses Netty's event loop threads for I/O operations. **NEVER** block these threads with:

- `.join()` or `.get()` inside `thenApply/thenAccept`
- `Thread.sleep()`
- Blocking database calls
- Long-running computations

If your message processing involves blocking operations, offload to a separate thread pool using `thenApplyAsync(fn, executor)`.

## Blocking vs. Async - When to Use Each

The Iggy Java SDK provides two client types: **blocking (synchronous)** and **async (non-blocking)**. Choose based on your use case:

### Use Blocking Client When

- Writing scripts, CLI tools, or simple applications
- Sequential code is easier to reason about
- Integration tests

### Use Async Client When

- Need high throughput
- Application is already async/reactive (Spring WebFlux, Vert.x)
- Want to pipeline multiple requests over a single connection
- Building services that handle many concurrent streams

## Key Async Patterns

### CompletableFuture Chaining

```java
client.connect()
    .thenCompose(v -> client.login())
    .thenCompose(identity -> client.streams().createStream("my-stream"))
    .thenAccept(stream -> System.out.println("Created: " + stream.name()))
    .exceptionally(ex -> {
        System.err.println("Error: " + ex.getMessage());
        return null;
    });
```

### Pipelining for Throughput

```java
List<CompletableFuture<Void>> sends = new ArrayList<>();
for (int i = 0; i < 10; i++) {
    sends.add(client.messages().sendMessages(...));
}
CompletableFuture.allOf(sends.toArray(new CompletableFuture[0])).join();
```

### Thread Pool Offloading

```java
// WRONG - blocks Netty event loop
client.messages().pollMessages(...)
    .thenAccept(polled -> {
        saveToDatabase(polled);  // blocking I/O!
    });

// CORRECT - offloads to processing pool
var processingPool = Executors.newFixedThreadPool(8);
client.messages().pollMessages(...)
    .thenAcceptAsync(polled -> {
        saveToDatabase(polled);  // runs on processingPool
    }, processingPool);
```
