# Async Client Guide

Comprehensive documentation for the Apache Iggy Java SDK async client — a fully non-blocking
TCP client built on Netty for high-throughput message streaming.

## Table of Contents

- [Getting Started](#getting-started)
- [Connection Management](#connection-management)
- [Producing Messages](#producing-messages)
- [Consuming Messages](#consuming-messages)
- [Error Handling](#error-handling)
- [CompletableFuture Patterns](#completablefuture-patterns)
- [Example Applications](#example-applications)
- [Performance Characteristics](#performance-characteristics)
- [Migration Guide: Blocking to Async](#migration-guide-blocking-to-async)

---

## Getting Started

### Prerequisites

- Java 17+
- A running Apache Iggy server (default TCP port: `8090`)

### Installation

Add the dependency to your project:

**Gradle:**

```gradle
implementation 'org.apache.iggy:iggy:0.6.0'
```

**Maven:**

```xml
<dependency>
    <groupId>org.apache.iggy</groupId>
    <artifactId>iggy</artifactId>
    <version>0.6.0</version>
</dependency>
```

### Quickest Start — Build, Connect, Login

```java
import org.apache.iggy.Iggy;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;

// One-liner: build, connect, and authenticate
var client = Iggy.tcpClientBuilder()
    .async()
    .host("localhost")
    .port(8090)
    .credentials("iggy", "iggy")
    .buildAndLogin()
    .join();  // blocks until ready

// Use the client...

// Always close when done
client.close().join();
```

### Step-by-Step — Build, Connect, Login Separately

When you need more control over each phase:

```java
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;

// 1. Build the client
var client = AsyncIggyTcpClient.builder()
    .host("localhost")
    .port(8090)
    .build();

// 2. Connect (establishes the TCP connection)
client.connect().join();

// 3. Login explicitly
var identity = client.users().login("iggy", "iggy").join();
System.out.println("Logged in as user ID: " + identity.userId());

// 4. Now the client is ready for operations
var streams = client.streams().getStreams().join();
System.out.println("Found " + streams.size() + " streams");

// 5. Clean up
client.close().join();
```

### Your First Message

```java
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.*;

// Create infrastructure
var stream = client.streams().createStream("my-app").join();
client.topics().createTopic(
    StreamId.of(stream.id()), 3L,
    CompressionAlgorithm.None,
    java.math.BigInteger.ZERO,  // no expiry
    java.math.BigInteger.ZERO,  // unlimited size
    java.util.Optional.empty(), // default replication
    "events"
).join();

// Send a message
client.messages().sendMessages(
    StreamId.of(stream.id()),
    TopicId.of(1L),
    Partitioning.balanced(),
    java.util.List.of(Message.of("Hello, Iggy!"))
).join();

// Poll it back
var polled = client.messages().pollMessages(
    StreamId.of(stream.id()),
    TopicId.of(1L),
    java.util.Optional.empty(),         // any partition
    org.apache.iggy.consumergroup.Consumer.of(1L),
    PollingStrategy.first(),
    10L,
    true                                // auto-commit offset
).join();

for (var msg : polled.messages()) {
    System.out.println("Received: " + new String(msg.payload()));
}
```

---

## Connection Management

### Connection Lifecycle

The async client has three distinct phases:

| Phase       | Method                        | What Happens                            |
|-------------|-------------------------------|-----------------------------------------|
| **Build**   | `builder()...build()`         | Allocates configuration, no I/O         |
| **Connect** | `connect()`                   | Opens TCP socket, starts Netty pipeline |
| **Login**   | `login()` / `users().login()` | Authenticates the session               |

After `close()`, the client cannot be reused — create a new instance.

### TLS Configuration

```java
// Trust the system CA store (for production certificates)
var client = AsyncIggyTcpClient.builder()
    .host("iggy.example.com")
    .port(8090)
    .enableTls()
    .credentials("admin", "secret")
    .buildAndLogin()
    .join();

// Trust a specific certificate (self-signed or internal CA)
var client = AsyncIggyTcpClient.builder()
    .host("iggy.internal")
    .port(8090)
    .enableTls()
    .tlsCertificate("/path/to/ca-cert.pem")
    .credentials("admin", "secret")
    .buildAndLogin()
    .join();
```

### Connection Best Practices

1. **Create one client per application.** The async client multiplexes all requests
   over a single TCP connection using Netty's event loop — there is no need for
   multiple client instances.

2. **Always close the client.** Use try-with-resources patterns or shutdown hooks:

   ```java
   Runtime.getRuntime().addShutdownHook(new Thread(() -> {
       client.close().join();
   }));
   ```

3. **Handle connection failures.** The `connect()` future will fail if the server is
   unreachable. Implement reconnection logic for production systems:

   ```java
   CompletableFuture<AsyncIggyTcpClient> connectWithRetry(
           AsyncIggyTcpClientBuilder builder, int maxRetries) {
       var client = builder.build();
       return tryConnect(client, maxRetries, 0);
   }

   CompletableFuture<AsyncIggyTcpClient> tryConnect(
           AsyncIggyTcpClient client, int maxRetries, int attempt) {
       return client.connect()
           .thenCompose(v -> client.login())
           .thenApply(v -> client)
           .exceptionallyCompose(ex -> {
               if (attempt >= maxRetries) {
                   return CompletableFuture.failedFuture(ex);
               }
               long delay = Math.min(100L * (1L << attempt), 5000L);
               return CompletableFuture.supplyAsync(() -> null,
                       CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS))
                   .thenCompose(v -> tryConnect(client, maxRetries, attempt + 1));
           });
   }
   ```

4. **Consumer group membership is per-connection.** If you join a consumer group on
   one client instance, only that instance can poll using that group membership. A
   disconnect automatically removes the client from the group.

---

## Producing Messages

### Partitioning Strategies

Choose the right strategy based on your use case:

| Strategy                          | Behavior                      | Use When                                 |
|-----------------------------------|-------------------------------|------------------------------------------|
| `Partitioning.balanced()`         | Round-robin across partitions | Maximum throughput, order doesn't matter |
| `Partitioning.partitionId(id)`    | Fixed partition               | Explicit control                         |
| `Partitioning.messagesKey("key")` | Hash-based routing            | Need per-key ordering (e.g., per-user)   |

### Batch Sending

Iggy sends all messages in a single request. Accumulate messages and send in batches
for optimal throughput:

```java
var batch = new ArrayList<Message>();
for (int i = 0; i < 1000; i++) {
    batch.add(Message.of("event-" + i));
}

// One network round-trip for 1000 messages
client.messages().sendMessages(streamId, topicId, Partitioning.balanced(), batch).join();
```

---

## Consuming Messages

### Individual Consumer

```java
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.message.PollingStrategy;

// Poll from offset 0
var polled = client.messages().pollMessages(
    streamId, topicId,
    Optional.of(0L),            // partition 0
    Consumer.of(1L),            // consumer ID 1
    PollingStrategy.offset(BigInteger.ZERO),
    100L,                       // up to 100 messages
    true                        // auto-commit
).join();
```

### Consumer Group

Consumer groups distribute partitions across members for parallel consumption:

```java
import org.apache.iggy.identifier.ConsumerId;

// 1. Join the consumer group (must be done before polling)
client.consumerGroups()
    .joinConsumerGroup(streamId, topicId, ConsumerId.of(1L))
    .join();

// 2. Poll with group consumer (server assigns partitions)
var polled = client.messages().pollMessages(
    streamId, topicId,
    Optional.empty(),           // server picks the partition
    Consumer.group(1L),         // group ID 1
    PollingStrategy.next(),     // continue from last committed offset
    100L,
    true
).join();

// 3. Leave when done
client.consumerGroups()
    .leaveConsumerGroup(streamId, topicId, ConsumerId.of(1L))
    .join();
```

### Continuous Polling Loop

```java
void pollContinuously(AsyncIggyTcpClient client, StreamId streamId, TopicId topicId) {
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    scheduler.scheduleWithFixedDelay(() -> {
        try {
            var polled = client.messages().pollMessages(
                streamId, topicId, Optional.empty(),
                Consumer.of(1L), PollingStrategy.next(), 100L, true
            ).join();

            for (var msg : polled.messages()) {
                processMessage(msg);
            }
        } catch (Exception e) {
            System.err.println("Poll error: " + e.getMessage());
        }
    }, 0, 100, TimeUnit.MILLISECONDS);
}
```

---

## Error Handling

### Exception Hierarchy

All SDK exceptions extend `IggyException` (a `RuntimeException`):

| Exception                         | When It Occurs                           |
|-----------------------------------|------------------------------------------|
| `IggyServerException`             | Server returned an error status code     |
| `IggyNotConnectedException`       | Operation attempted before `connect()`   |
| `IggyMissingCredentialsException` | `login()` called without credentials     |
| `IggyAuthenticationException`     | Invalid username or password             |
| `IggyAuthorizationException`      | Insufficient permissions                 |
| `IggyResourceNotFoundException`   | Stream, topic, or group not found        |
| `IggyConflictException`           | Resource already exists (duplicate name) |
| `IggyConnectionException`         | TCP connection failure                   |
| `IggyTimeoutException`            | Operation timed out                      |
| `IggyTlsException`                | TLS/SSL configuration or handshake error |
| `IggyValidationException`         | Invalid request parameters               |

### Handling Errors with CompletableFuture

```java
// Handle specific error types
client.streams().createStream("orders")
    .thenAccept(stream -> System.out.println("Created: " + stream.name()))
    .exceptionally(ex -> {
        Throwable cause = ex.getCause();  // unwrap CompletionException
        if (cause instanceof IggyConflictException) {
            System.out.println("Stream already exists, continuing...");
        } else if (cause instanceof IggyAuthenticationException) {
            System.err.println("Not authenticated — re-login required");
        } else {
            System.err.println("Unexpected error: " + cause.getMessage());
        }
        return null;
    });
```

### Catch-All Pattern

```java
// Catch all Iggy errors in one place
client.messages().sendMessages(streamId, topicId, Partitioning.balanced(), messages)
    .whenComplete((result, ex) -> {
        if (ex != null) {
            Throwable cause = (ex instanceof CompletionException) ? ex.getCause() : ex;
            if (cause instanceof IggyException iggyEx) {
                log.error("Iggy error: {}", iggyEx.getMessage());
            } else {
                log.error("Unexpected error", cause);
            }
        } else {
            log.info("Messages sent successfully");
        }
    });
```

### Error Recovery with Retry

```java
<T> CompletableFuture<T> withRetry(Supplier<CompletableFuture<T>> operation, int maxRetries) {
    return operation.get().exceptionallyCompose(ex -> {
        if (maxRetries <= 0) {
            return CompletableFuture.failedFuture(ex);
        }
        Throwable cause = (ex instanceof CompletionException) ? ex.getCause() : ex;
        if (cause instanceof IggyServerException) {
            // Retry on server errors
            return CompletableFuture.supplyAsync(() -> null,
                    CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS))
                .thenCompose(v -> withRetry(operation, maxRetries - 1));
        }
        return CompletableFuture.failedFuture(ex);  // don't retry client errors
    });
}

// Usage
withRetry(() -> client.messages().sendMessages(
    streamId, topicId, Partitioning.balanced(), messages), 3);
```

---

## CompletableFuture Patterns

### Chaining Sequential Operations

```java
// Create stream → create topic → send message (sequential)
client.streams().createStream("pipeline")
    .thenCompose(stream -> client.topics().createTopic(
        StreamId.of(stream.id()), 1L,
        CompressionAlgorithm.None,
        BigInteger.ZERO, BigInteger.ZERO,
        Optional.empty(), "events"))
    .thenCompose(topic -> client.messages().sendMessages(
        StreamId.of(1L), TopicId.of(topic.id()),
        Partitioning.balanced(),
        List.of(Message.of("setup complete"))))
    .thenRun(() -> System.out.println("Pipeline ready"))
    .exceptionally(ex -> {
        System.err.println("Setup failed: " + ex.getMessage());
        return null;
    });
```

### Running Operations in Parallel

```java
// Send to multiple topics concurrently
var send1 = client.messages().sendMessages(
    streamId, topicId1, Partitioning.balanced(), batch1);
var send2 = client.messages().sendMessages(
    streamId, topicId2, Partitioning.balanced(), batch2);
var send3 = client.messages().sendMessages(
    streamId, topicId3, Partitioning.balanced(), batch3);

// Wait for all to complete
CompletableFuture.allOf(send1, send2, send3)
    .thenRun(() -> System.out.println("All batches sent"))
    .join();
```

### Timeout Handling

```java
import java.util.concurrent.TimeUnit;

// Apply a timeout to any async operation
client.messages().pollMessages(
        streamId, topicId, Optional.empty(),
        Consumer.of(1L), PollingStrategy.next(), 100L, true)
    .orTimeout(5, TimeUnit.SECONDS)
    .thenAccept(polled -> processMessages(polled))
    .exceptionally(ex -> {
        if (ex.getCause() instanceof TimeoutException) {
            System.err.println("Poll timed out after 5 seconds");
        }
        return null;
    });
```

### Transforming Results

```java
// Extract just the payloads as strings
CompletableFuture<List<String>> payloads = client.messages()
    .pollMessages(streamId, topicId, Optional.empty(),
        Consumer.of(1L), PollingStrategy.first(), 50L, true)
    .thenApply(polled -> polled.messages().stream()
        .map(msg -> new String(msg.payload()))
        .toList());
```

### Custom Executor for Callbacks

```java
// Run callbacks on a dedicated thread pool instead of the Netty event loop
var callbackPool = Executors.newFixedThreadPool(4);

client.messages().pollMessages(
        streamId, topicId, Optional.empty(),
        Consumer.of(1L), PollingStrategy.next(), 100L, true)
    .thenApplyAsync(polled -> {
        // Heavy processing here runs on callbackPool, not Netty threads
        return polled.messages().stream()
            .map(msg -> deserialize(msg.payload()))
            .toList();
    }, callbackPool);
```

---

## Example Applications

### High-Throughput Producer

A producer that batches messages for maximum throughput:

```java
import org.apache.iggy.Iggy;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class HighThroughputProducer {

    private static final int BATCH_SIZE = 1000;
    private static final int TOTAL_MESSAGES = 100_000;

    public static void main(String[] args) throws Exception {
        var client = Iggy.tcpClientBuilder()
            .async()
            .host("localhost")
            .port(8090)
            .credentials("iggy", "iggy")
            .buildAndLogin()
            .join();

        var streamId = StreamId.of(1L);
        var topicId = TopicId.of(1L);
        var sent = new AtomicLong(0);
        var start = System.nanoTime();

        // Send messages in batches, pipelining requests
        List<CompletableFuture<Void>> inflight = new ArrayList<>();

        for (int i = 0; i < TOTAL_MESSAGES; i += BATCH_SIZE) {
            var batch = new ArrayList<Message>();
            for (int j = 0; j < BATCH_SIZE && (i + j) < TOTAL_MESSAGES; j++) {
                batch.add(Message.of("event-" + (i + j)));
            }

            var future = client.messages()
                .sendMessages(streamId, topicId, Partitioning.balanced(), batch)
                .thenRun(() -> {
                    long count = sent.addAndGet(batch.size());
                    if (count % 10_000 == 0) {
                        System.out.printf("Sent %,d messages%n", count);
                    }
                });

            inflight.add(future);

            // Limit inflight to prevent unbounded memory usage
            if (inflight.size() >= 10) {
                CompletableFuture.allOf(inflight.toArray(new CompletableFuture[0])).join();
                inflight.clear();
            }
        }

        // Wait for remaining
        CompletableFuture.allOf(inflight.toArray(new CompletableFuture[0])).join();

        var elapsed = (System.nanoTime() - start) / 1_000_000_000.0;
        System.out.printf("Sent %,d messages in %.2fs (%.0f msg/s)%n",
            sent.get(), elapsed, sent.get() / elapsed);

        client.close().join();
    }
}
```

### Concurrent Consumer with Backpressure

A consumer that processes messages concurrently while limiting parallelism:

```java
import org.apache.iggy.Iggy;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.PollingStrategy;
import java.util.Optional;
import java.util.concurrent.*;

public class BackpressureConsumer {

    private static final int MAX_CONCURRENT = 8;

    public static void main(String[] args) throws Exception {
        var client = Iggy.tcpClientBuilder()
            .async()
            .host("localhost")
            .port(8090)
            .credentials("iggy", "iggy")
            .buildAndLogin()
            .join();

        var streamId = StreamId.of(1L);
        var topicId = TopicId.of(1L);

        // Semaphore limits concurrent processing
        var semaphore = new Semaphore(MAX_CONCURRENT);
        var executor = Executors.newFixedThreadPool(MAX_CONCURRENT);
        var running = new java.util.concurrent.atomic.AtomicBoolean(true);

        // Shutdown hook for graceful stop
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            executor.shutdown();
            client.close().join();
        }));

        System.out.println("Consumer started. Press Ctrl+C to stop.");

        while (running.get()) {
            try {
                var polled = client.messages().pollMessages(
                    streamId, topicId, Optional.empty(),
                    Consumer.of(1L), PollingStrategy.next(), 100L, true
                ).join();

                for (var msg : polled.messages()) {
                    // Backpressure: wait if we have too many inflight
                    semaphore.acquire();

                    CompletableFuture.runAsync(() -> {
                        try {
                            // Simulate processing work
                            processMessage(new String(msg.payload()));
                        } finally {
                            semaphore.release();
                        }
                    }, executor);
                }

                if (polled.messages().isEmpty()) {
                    Thread.sleep(100);  // avoid busy-waiting when no messages
                }
            } catch (Exception e) {
                System.err.println("Poll error: " + e.getMessage());
                Thread.sleep(1000);  // back off on error
            }
        }
    }

    static void processMessage(String payload) {
        // Your processing logic here
        System.out.println("Processing: " + payload);
    }
}
```

### Error Recovery Patterns

A resilient producer that handles transient failures:

```java
import org.apache.iggy.Iggy;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.exception.*;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class ResilientProducer {

    private final AsyncIggyTcpClient client;
    private final StreamId streamId;
    private final TopicId topicId;

    public ResilientProducer(AsyncIggyTcpClient client, StreamId streamId, TopicId topicId) {
        this.client = client;
        this.streamId = streamId;
        this.topicId = topicId;
    }

    /**
     * Sends messages with automatic retry on transient errors.
     */
    public CompletableFuture<Void> send(List<Message> messages) {
        return withRetry(
            () -> client.messages().sendMessages(
                streamId, topicId, Partitioning.balanced(), messages),
            3,      // max retries
            100L    // initial delay ms
        );
    }

    /**
     * Generic retry wrapper with exponential backoff.
     * Only retries on server/connection errors, not on validation errors.
     */
    static <T> CompletableFuture<T> withRetry(
            Supplier<CompletableFuture<T>> operation,
            int maxRetries,
            long delayMs) {

        return operation.get().exceptionallyCompose(ex -> {
            Throwable cause = unwrap(ex);

            // Don't retry client-side errors
            if (cause instanceof IggyValidationException
                    || cause instanceof IggyAuthenticationException
                    || cause instanceof IggyMissingCredentialsException) {
                return CompletableFuture.failedFuture(ex);
            }

            if (maxRetries <= 0) {
                return CompletableFuture.failedFuture(ex);
            }

            System.err.printf("Retrying after error (attempts left: %d): %s%n",
                maxRetries, cause.getMessage());

            return CompletableFuture.supplyAsync(() -> null,
                    CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS))
                .thenCompose(v -> withRetry(operation, maxRetries - 1,
                    Math.min(delayMs * 2, 5000L)));
        });
    }

    /** Unwraps CompletionException to get the actual cause. */
    static Throwable unwrap(Throwable ex) {
        return (ex instanceof CompletionException && ex.getCause() != null)
            ? ex.getCause() : ex;
    }

    public static void main(String[] args) throws Exception {
        var client = Iggy.tcpClientBuilder()
            .async()
            .host("localhost")
            .port(8090)
            .credentials("iggy", "iggy")
            .buildAndLogin()
            .join();

        var producer = new ResilientProducer(client, StreamId.of(1L), TopicId.of(1L));

        // Send with automatic retry
        producer.send(List.of(
            Message.of("order-1"),
            Message.of("order-2"),
            Message.of("order-3")
        )).join();

        System.out.println("Messages sent successfully (with retry if needed)");
        client.close().join();
    }
}
```

---

## Performance Characteristics

### When to Use Async vs Blocking

| Criteria                  | Async Client                            | Blocking Client                   |
|---------------------------|-----------------------------------------|-----------------------------------|
| **Throughput**            | Higher — pipelines requests             | Lower — one request at a time     |
| **Latency per request**   | Similar                                 | Similar                           |
| **Thread usage**          | Minimal (Netty event loop)              | One thread per operation          |
| **Code complexity**       | Higher (futures, callbacks)             | Lower (sequential)                |
| **Best for**              | High-throughput services, reactive apps | Scripts, simple services, testing |
| **Framework integration** | Spring WebFlux, Vert.x                  | Spring MVC, traditional servlets  |

**Use the async client when:**

- You need to send/receive thousands of messages per second
- Your application is already async/reactive (Spring WebFlux, Vert.x, etc.)
- You want to pipeline multiple requests over a single connection
- You're building a service that handles many concurrent streams

**Use the blocking client when:**

- You're writing scripts, CLI tools, or simple applications
- Your message rate is modest (< 1000 msg/s)
- Sequential code is easier to reason about for your use case
- You're writing integration tests

### Connection Pooling Recommendations

The async client uses a single TCP connection with Netty's event loop for multiplexing.
This is fundamentally different from blocking clients where one thread = one connection.

**For the async client:**

- A single client instance is sufficient for most applications. Netty's non-blocking
  I/O means one connection can saturate your network without thread contention.
- If you need isolation (e.g., separate consumer group sessions), create separate
  client instances — each gets its own TCP connection.
- Consumer group membership is per-connection: each client instance that joins a group
  counts as a separate member.

**For the blocking client:**

- Consider multiple client instances if you need concurrent operations from different
  threads, since each blocking call holds the connection.

### Thread Pool Configuration

The async client uses Netty's `NioEventLoopGroup`, which by default creates
`2 × availableProcessors` threads. These threads handle all I/O operations.

**Avoid blocking on Netty threads.** Never call `.join()` or `.get()` inside a
`thenApply`/`thenAccept` callback — this can deadlock the event loop. If your
callback performs heavy computation or blocking I/O, offload it:

```java
// WRONG — blocks the Netty event loop
client.messages().pollMessages(...)
    .thenAccept(polled -> {
        saveToDatabase(polled);  // blocking I/O in Netty thread!
    });

// CORRECT — offload to a dedicated pool
var processingPool = Executors.newFixedThreadPool(
    Runtime.getRuntime().availableProcessors());

client.messages().pollMessages(...)
    .thenAcceptAsync(polled -> {
        saveToDatabase(polled);  // runs on processingPool
    }, processingPool);
```

**Sizing your processing pool:**

- For CPU-bound work (parsing, transformations): `availableProcessors` threads
- For I/O-bound work (database, HTTP calls): `availableProcessors × 2` or more
- For mixed workloads: use a `ForkJoinPool` or virtual threads (Java 21+)

---

## Migration Guide: Blocking to Async

### API Comparison

The async client mirrors the blocking client's interface, with each method returning a
`CompletableFuture` instead of the raw value:

| Blocking                                | Async                                                                        |
|-----------------------------------------|------------------------------------------------------------------------------|
| `client.streams().getStreams()`         | `client.streams().getStreams()` → `CompletableFuture<List<StreamBase>>`      |
| `client.streams().createStream("name")` | `client.streams().createStream("name")` → `CompletableFuture<StreamDetails>` |
| `client.messages().sendMessages(...)`   | `client.messages().sendMessages(...)` → `CompletableFuture<Void>`            |
| `client.messages().pollMessages(...)`   | `client.messages().pollMessages(...)` → `CompletableFuture<PolledMessages>`  |
| `client.users().login("u", "p")`        | `client.users().login("u", "p")` → `CompletableFuture<IdentityInfo>`         |

### Step-by-Step Migration

#### 1. Change the Builder

```java
// Before (blocking)
var client = Iggy.tcpClientBuilder()
    .blocking()
    .host("localhost")
    .port(8090)
    .credentials("iggy", "iggy")
    .buildAndLogin();

// After (async)
var client = Iggy.tcpClientBuilder()
    .async()
    .host("localhost")
    .port(8090)
    .credentials("iggy", "iggy")
    .buildAndLogin()
    .join();  // or use .thenAccept() for fully non-blocking
```

#### 2. Convert Sequential Calls

```java
// Before (blocking)
var stream = client.streams().createStream("orders");
client.topics().createTopic(StreamId.of(stream.id()), 1L, ...);
client.messages().sendMessages(streamId, topicId, partitioning, messages);

// After (async) — chained
client.streams().createStream("orders")
    .thenCompose(stream -> client.topics().createTopic(
        StreamId.of(stream.id()), 1L, ...))
    .thenCompose(topic -> client.messages().sendMessages(
        streamId, topicId, partitioning, messages))
    .join();
```

#### 3. Convert Error Handling

```java
// Before (blocking)
try {
    client.streams().createStream("orders");
} catch (IggyConflictException e) {
    // stream already exists
}

// After (async)
client.streams().createStream("orders")
    .exceptionally(ex -> {
        if (ex.getCause() instanceof IggyConflictException) {
            // stream already exists
            return null;
        }
        throw new CompletionException(ex);
    });
```

#### 4. Convert Loops

```java
// Before (blocking)
while (running) {
    var polled = client.messages().pollMessages(...);
    for (var msg : polled.messages()) {
        process(msg);
    }
}

// After (async) — recursive chaining
void pollLoop(AsyncIggyTcpClient client) {
    client.messages().pollMessages(...)
        .thenAcceptAsync(polled -> {
            for (var msg : polled.messages()) {
                process(msg);
            }
            if (running) {
                pollLoop(client);  // schedule next poll
            }
        }, processingPool);
}
```

### Common Migration Pitfalls

1. **Forgetting `.join()` in tests.** Async operations won't execute unless the future
   is awaited. In tests, always `.join()` or use `assertTimeout()`.

2. **Blocking inside callbacks.** Never call `.join()`, `.get()`, or `Thread.sleep()`
   inside `thenApply`/`thenAccept` — use `thenApplyAsync` with a separate executor.

3. **Swallowed exceptions.** Exceptions in CompletableFuture chains are silent unless
   you add `.exceptionally()` or `.whenComplete()`. Always attach error handlers.

4. **Sequential where parallel is possible.** When operations are independent, use
   `CompletableFuture.allOf()` to run them concurrently instead of chaining with
   `thenCompose`.
