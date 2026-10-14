<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-darkbg.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg">
    <img alt="Apache Iggy" src="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg" width="320">
  </picture>
</div>

# Java SDK for Apache Iggy

Official Java client SDK for [Apache Iggy](https://iggy.apache.org) message streaming.

_This is part of the Apache Iggy monorepo. For the main project, see the [root repository](https://github.com/apache/iggy)._

## Installation

These examples target server **0.9.0**. SDK `0.9.0` is on Maven Central and works with server `0.9.0`. The older `0.8.0` artifact speaks the previous TCP protocol and does not work with server `0.9.0`. Java 17 or newer is required.

Add the dependency to your project:

**Gradle:**

```gradle
implementation 'org.apache.iggy:iggy:0.9.0'
```

**Maven:**

```xml
<dependency>
    <groupId>org.apache.iggy</groupId>
    <artifactId>iggy</artifactId>
    <version>0.9.0</version>
</dependency>
```

See [Maven Central](https://central.sonatype.com/artifact/org.apache.iggy/iggy) for all published versions.

### Snapshot Versions

Development builds of the next release carry the version `0.9.1-SNAPSHOT`. Get them from the ASF snapshot repository:

**Gradle:**

```gradle
repositories {
    mavenCentral()
    maven {
        url = uri("https://repository.apache.org/content/repositories/snapshots/")
    }
}

dependencies {
    implementation 'org.apache.iggy:iggy:0.9.1-SNAPSHOT'
}
```

**Maven:**

```xml
<repositories>
    <repository>
        <id>apache-snapshots</id>
        <url>https://repository.apache.org/content/repositories/snapshots/</url>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>org.apache.iggy</groupId>
        <artifactId>iggy</artifactId>
        <version>0.9.1-SNAPSHOT</version>
    </dependency>
</dependencies>
```

### Unreleased 0.11.1 protocol

The source checkout uses protocol 0.11.1. Build it with a matching server.
`CommandCode.System.ATTACH_CONSUMER_SESSION` (code 14) was removed. Use
`CommandCode.System.BIND_SESSION` (code 15), which requires the registered
session's private bind secret. Auxiliary poll connections reuse that session
instead of logging in independently.

Transport loss retains the logical identity and reconnect uses `BindSession`.
A definitive stale or unauthenticated bind response permits a fresh Register;
transient responses retain the identity. The bind proof is an independent
session credential: password/PAT changes or expiry do not end an established
session. Logout, lease expiry and user deactivation end session access.

## Quick Start

`Iggy.tcpClientBuilder()` handles the routing and the session for you. Over TCP and TCP/TLS, the client keeps the consumer group membership on the coordinator. It maintains one shared-session connection per advertised replica and multiplexes partition requests on those connections.

Start the server with the [example prerequisites](../../examples/java/#running-examples) and matching credentials. The following snippets show alternative clients. Close a blocking client with `close()` or an async client with `close().join()` when finished.

### TCP Client (Blocking)

```java
import org.apache.iggy.Iggy;

// Create and connect with auto-login
var client = Iggy.tcpClientBuilder()
    .blocking()
    .host("localhost")
    .port(8090)
    .credentials("iggy", "iggy")
    .buildAndLogin();

// Or build, connect, and login separately
var client = Iggy.tcpClientBuilder()
    .blocking()
    .host("localhost")
    .port(8090)
    .build();
client.connect();
client.users().login("iggy", "iggy");
```

### Connection String

`fromConnectionString` configures a TCP client from an `iggy://` connection string:

```java
// Blocking client
var client = Iggy.tcpClientBuilder()
    .fromConnectionString("iggy://iggy:iggy@localhost:8090")
    .buildAndLogin();

// Async client, with TLS and a custom CA certificate
var asyncClient = Iggy.tcpClientBuilder()
    .async()
    .fromConnectionString("iggy+tcp://admin:secret@iggy-server.example.com:8090?tls=true&tls_ca_file=/certs/ca.pem")
    .buildAndLogin()
    .join();
```

The connection string accepts `iggy://` and `iggy+tcp://` with `<username>:<password>` credentials and the
`tls` and `tls_ca_file` options, each at most once. `tls_ca_file` requires `tls=true`, and a string without `tls`
keeps the TLS setting already on the builder. Personal access tokens and other options are not supported yet
and are rejected with an `IggyInvalidArgumentException`.

### TCP Client (Async)

```java
import org.apache.iggy.Iggy;

// Create async client
var asyncClient = Iggy.tcpClientBuilder()
    .async()
    .host("localhost")
    .port(8090)
    .credentials("iggy", "iggy")
    .buildAndLogin()
    .join();

// Or with manual connect and login
var asyncClient = Iggy.tcpClientBuilder()
    .async()
    .host("localhost")
    .build();
asyncClient.connect().join();
asyncClient.users().login("iggy", "iggy").join();
```

### HTTP Client

```java
import org.apache.iggy.Iggy;

// Using URL
var httpClient = Iggy.httpClientBuilder()
    .blocking()
    .url("http://localhost:3000")
    .credentials("iggy", "iggy")
    .buildAndLogin();

// Using host/port
var httpClient = Iggy.httpClientBuilder()
    .blocking()
    .host("localhost")
    .port(3000)
    .credentials("iggy", "iggy")
    .buildAndLogin();
```

### TLS Support

Both TCP and HTTP clients support TLS:

```java
// TCP with TLS
var secureClient = Iggy.tcpClientBuilder()
    .blocking()
    .host("iggy-server.example.com")
    .port(8090)
    .enableTls()
    .tlsCertificate("/path/to/ca.pem")  // Optional custom CA
    .credentials("admin", "secret")
    .buildAndLogin();

// HTTPS
var secureHttpClient = Iggy.httpClientBuilder()
    .blocking()
    .host("iggy-server.example.com")
    .port(443)
    .enableTls()
    .credentials("admin", "secret")
    .buildAndLogin();
```

### Builder Options

The client builders support additional configuration:

```java
var client = Iggy.tcpClientBuilder()
    .blocking()
    .host("localhost")
    .port(8090)
    .connectionTimeout(Duration.ofSeconds(10))
    .requestTimeout(Duration.ofSeconds(30))
    .retryPolicy(RetryPolicy.exponentialBackoff())
    .credentials("iggy", "iggy")
    .buildAndLogin();
```

### Event Loop Threads

Each TCP client shares one event loop group across its replica connections. By default
the group has one thread. An application that opens many clients can instead register them all on
one caller-owned group. The clients never shut that group down. Close the clients first,
then shut the group down:

```java
var group = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());

var producer = Iggy.tcpClientBuilder()
    .blocking()
    .eventLoopGroup(group)
    .credentials("iggy", "iggy")
    .buildAndLogin();
var consumer = Iggy.tcpClientBuilder()
    .blocking()
    .eventLoopGroup(group)
    .credentials("iggy", "iggy")
    .buildAndLogin();

// ... later
producer.close();
consumer.close();
group.shutdownGracefully();
```

Do not block in a completion callback. Callbacks run on the group's loops, so a blocked
callback stalls every client that shares the group.

### Version Information

```java
// Get SDK version
String version = Iggy.version();  // e.g., "0.9.0"

// Get detailed version info
IggyVersion info = Iggy.versionInfo();
info.getVersion();     // Version string
info.getBuildTime();   // Build timestamp
info.getGitCommit();   // Git commit hash
info.getUserAgent();   // User-Agent string for HTTP
```

## Exception Handling

The SDK's custom exception types inherit from `IggyException`. When you join a failed future, the cause can come back inside a `CompletionException`. The HTTP client's `close()` method declares `IOException`. Handle these two boundaries as well as the specific SDK errors.

### Retained TCP operations

Replicated mutations serialize within their metadata or partition lane. If a sent mutation's
outcome cannot be established, `IggyOperationException` carries
`IggyErrorCode.OPERATION_UNCERTAIN` and its exact operation ID. A later mutation in the
same lane returns `LANE_BLOCKED` with that ID; unrelated partitions and pure reads continue.

Both blocking and async TCP clients expose `resolveOperation(operationId, timeout)` and
`abandonOperation(operationId)`. Resolve reuses the original request bytes and returns the
response body, or the proved business error. Abandon releases local retention; it does not
cancel or undo server work. `OPERATION_NOT_RETAINED` after abandonment or session reset
provides no evidence that the original mutation was uncommitted. Do not resend it automatically.
Java `long` preserves all 64 operation-ID bits, including negative values. Use
`Long.toUnsignedString(operationId)` when displaying an ID.

The TCP builders accept `.pendingOperations(count, bytes)`, defaulting to 256 mutations
and 64 MiB across active and retained frames. Admission waits within the call's existing
deadline; an oversized frame fails locally. Cancellation removes only the caller's waiter.
A mutation already sent may remain retained. The byte limit counts admitted frozen frames and
writer references; API input snapshots and queued serialization buffers are outside this limit.

Auto-commit and `Next` polls use a temporary partition ordering barrier. Their success
acknowledges local admission, not replicated offset durability. Code 57 or a lost reply ends
the call with unknown progress and releases the barrier; these polls are never retained or
replayed after uncertainty. Explicit non-admission (58 or 89) may retry within the same deadline.

## Examples

See the **[Java Examples](../../examples/java/)** directory for runnable applications demonstrating the SDK:

- **GettingStartedProducer**: synchronous message production with batch sending
- **GettingStartedConsumer**: synchronous consumption with polling loops
- **AsyncProducer**: non-blocking batch production with concurrent request submission
- **AsyncConsumer**: async consumption with backpressure and error recovery

The examples README describes blocking and async clients, CompletableFuture patterns, and thread pool management.

For Apache Flink integration, see the [Flink Connector Library](external-processors/iggy-connector-flink/iggy-connector-library/README.md).

## Building from Source

This project uses the Gradle Wrapper. Due to Apache Software Foundation policy, the `gradle-wrapper.jar` binary is not checked into the repository. Instead, the `gradlew` script automatically downloads it on first run.

```bash
# Build the project
./gradlew build

# Run tests
./gradlew test
```

The wrapper script will:

1. Download `gradle-wrapper.jar` from the official Gradle repository if missing
2. Verify the SHA256 checksum for security
3. Execute the requested Gradle command

No manual Gradle installation is required.

**Note:** Only the Unix shell wrapper (`gradlew`) is provided. On Windows, use WSL or Git Bash, or install Gradle manually.

## Contributing

Before opening a pull request:

1. **Format code:** `./gradlew spotlessApply`
2. **Validate build:** `./gradlew check`
3. **Use AssertJ for assertions:** Write test assertions with [AssertJ](https://assertj.github.io/doc/) (`assertThat(...)`) instead of JUnit assertions.

These steps keep the code style compliant and make sure that all tests and checkstyle validations pass.
