# Iggy Java Examples

This directory contains comprehensive sample applications that showcase various usage patterns of the Iggy java client SDK, from basic operations to advanced multi-tenant scenarios.

Java 17 or newer is required. The included Gradle wrapper downloads the pinned Gradle version. The project uses `includeBuild` to compile the SDK from `foreign/java` in the same checkout.

## Running Examples

These examples target server **0.9.0** and speak the VSR (Viewstamped Replication) wire protocol. Build the server and SDK from the same checkout for unreleased changes. Run the Gradle tasks below from `examples/java`, with each producer before its consumer.

Iggy requires valid credentials to authenticate client requests. The examples assume that the server is using the default root credentials, set through environment variables before starting the server:

Linux, from the repository root:

```bash
export IGGY_ROOT_USERNAME=iggy
export IGGY_ROOT_PASSWORD=iggy
cargo run --bin iggy-server -- --fresh --with-default-root-credentials
```

PowerShell environment variables:

```powershell
$env:IGGY_ROOT_USERNAME = "iggy"
$env:IGGY_ROOT_PASSWORD = "iggy"
```

> **Note** <br>
> This setup is intended only for development and testing, not production use.

By default, all server data is stored in the `local_data` directory (this can be changed via `IGGY_PATH`).

`--fresh` wipes this replica's local data directory. Environment credentials take precedence over `--with-default-root-credentials`. Bootstrap settings do not replace recovered credentials, and a fresh cluster replica can recover them from peers. On an existing server, use the credentials that were configured for it.

You can also customize the server using environment variables:

```bash
## Example: set a custom TCP address
IGGY_TCP_ADDRESS=127.0.0.1:8090 cargo run --bin iggy-server
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

Uses a `message_type` header to choose the application handler for each order event.

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

Produces 100 batches of 1000 to 1099 messages with generated user records.

## Stream Builder Examples

### Stream Builder

Producing and consuming messages in one class:

```bash
./gradlew runStreamBasic
```

Uses the blocking client to create a stream and topic, send three messages, and poll them. It deletes its `test_stream` stream after the run.

## Async Client Examples

### Async Producer

Non-blocking batch production with concurrent request submission:

```bash
./gradlew runAsyncProducer
```

Shows:

- CompletableFuture chaining patterns
- Submitting multiple sends without blocking

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

## Security Examples

### TCP/TLS

Demonstrates secure TLS-encrypted TCP connections:

```bash
./gradlew runTcpTlsProducer
./gradlew runTcpTlsConsumer
```

These examples require a TLS-enabled Iggy server. From the repository root, start a disposable server with the development certificates:

```bash
IGGY_TCP_TLS_ENABLED=true \
IGGY_TCP_TLS_CERT_FILE=core/certs/iggy_cert.pem \
IGGY_TCP_TLS_KEY_FILE=core/certs/iggy_key.pem \
cargo run --bin iggy-server -- --fresh --with-default-root-credentials
```

Uses `IggyTcpClientBuilder.enableTls()` and `tlsCertificate("../../core/certs/iggy_ca_cert.pem")` with CA verification. Run the clients from `examples/java` so that path resolves. The same data and credential prerequisites apply; these certificates are for development only.

## Blocking vs. Async - When to Use Each

The Iggy Java SDK provides two client types: **blocking (synchronous)** and **async (non-blocking)**. Choose based on your use case:

### Use Blocking Client When

- Writing scripts, CLI tools, or simple applications
- Sequential code is easier to reason about
- Integration tests

### Use Async Client When

- Need high throughput
- Application is already async/reactive (Spring WebFlux, Vert.x)
- Want to compose non-blocking requests with `CompletableFuture`
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

### Submitting Multiple Sends

```java
List<CompletableFuture<SendMessagesResponse>> sends = new ArrayList<>();
for (int i = 0; i < 10; i++) {
    sends.add(client.messages().sendMessages(...));
}
CompletableFuture.allOf(sends.toArray(new CompletableFuture[0])).join();
```

The client accepts these calls without blocking, but its single VSR-pinned TCP
connection processes them in order. Batch more messages into each send to improve
throughput.

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
