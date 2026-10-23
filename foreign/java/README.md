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

Development builds of the next release carry the version `0.9.2-SNAPSHOT`. Get them from the ASF snapshot repository:

**Gradle:**

```gradle
repositories {
    mavenCentral()
    maven {
        url = uri("https://repository.apache.org/content/repositories/snapshots/")
    }
}

dependencies {
    implementation 'org.apache.iggy:iggy:0.9.2-SNAPSHOT'
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
        <version>0.9.2-SNAPSHOT</version>
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
A definitive stale or unauthenticated bind reply permits a fresh Register, and
transient replies retain the identity. A stale-client eviction during the bind
is a lost connection, so the next connection binds the same session again. An
unauthenticated eviction during the bind ends the session. The bind proof is an
independent session credential: password/PAT changes or expiry do not end an
established session. Logout, lease expiry and user deactivation end session
access.

A reply with status 30 (`STALE_CLIENT`) means that the server no longer attaches
the connection to the session. The client closes that connection, and the next
one binds the session again. A live session keeps its retained operations and
its group membership. An ended session gets a refused bind, and the client signs
in again with the remembered sign-in or stays signed out.

`RetryPolicy` limits one burst of redials after a connection loss, not the life
of the client. A heartbeat that fails starts a new burst with a fresh budget. If
the client lost its session and every sign-in of a burst lost its link, the
burst ends with the client signed out. A heartbeat that the server answers then
signs in again with a fresh budget. A logout, a rejected sign-in and `close()`
end the automatic sign-in. With `RetryPolicy.noRetry()`, the client stays signed
out until the caller signs in.

An explicit sign-in ends the automatic sign-in as soon as it starts. It does not
wait for an automatic Register whose reply is late, and that reply cannot
replace the new session. If the explicit sign-in succeeds, it becomes the
automatic sign-in. If it fails, the client does not sign in again as the
previous user. While the explicit sign-in runs, a call that finds no signed-in
connection ends with 40 (`UNAUTHENTICATED`) at once.

After a logout, a refused automatic sign-in or a failed explicit sign-in, a call that needs a session fails with 40 (`UNAUTHENTICATED`) until the caller signs in again. Before the first sign-in, such a call fails with `IggyNotConnectedException`.

A committed password change or rename of the signed-in user updates the
credentials of the automatic sign-in. It also updates the builder credentials
that `login()` uses when they name that user. This holds for typed calls,
`sendBinaryRequest` and `resolveOperation`. A rejected change leaves the
credentials unchanged.

## Quick Start

`Iggy.tcpClientBuilder()` handles the routing and the session for you. Over TCP and TCP/TLS, the client keeps the consumer group membership on the coordinator. It maintains one shared-session connection per advertised replica and multiplexes partition requests on those connections. With a one-node roster, the client dials the configured endpoint instead of the advertised one. This keeps a single node behind a port map or a port-forward reachable.

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

### Resource Options

Streams, topics and users carry typed options. Create calls take an options value, updates send only the keys they change, and `reset` returns an updatable key to its server default.

```java
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.stream.StreamOptions;
import org.apache.iggy.system.OptionsScope;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.TopicOptionKey;
import org.apache.iggy.topic.TopicOptions;
import org.apache.iggy.topic.TopicUpdate;

import java.math.BigInteger;
import java.security.SecureRandom;

byte[] key = new byte[32];
new SecureRandom().nextBytes(key);
var stream = client.streams().createStream("payments", StreamOptions.builder().encryptionKey(key).build());
stream.encryptionKeyFingerprint();  // the key itself is never returned

var streamId = StreamId.of(stream.id());
var topic = client.topics().createTopic(streamId, "orders", 1L, TopicOptions.builder()
    .compressionAlgorithm(CompressionAlgorithm.Gzip)
    .messageExpiry(BigInteger.valueOf(3_600_000_000L))
    .build());

client.topics().updateTopic(streamId, TopicId.of(topic.id()), TopicUpdate.builder()
    .name("orders-v2")
    .reset(TopicOptionKey.MESSAGE_EXPIRY)
    .build());

client.system().describeOptions(OptionsScope.Topic);  // keys, kinds, defaults and flags
```

The stream key is 32 random bytes. It travels inside the create request over the client's transport, so protecting it on the network is the job of TLS. The server stores it wrapped under its own key and never returns it, reads report only its source and fingerprints. Without a key, a server with encryption enabled generates one for the stream.

The typed builders refuse an update the server would refuse for its shape before anything is sent, with the server's error code. `raw(key, value)` and `rawReset(key)` reach keys this build has no field for, and the server decides whether they are allowed.

## Exception Handling

The SDK's custom exception types inherit from `IggyException`. When you join a failed future, the cause can come back inside a `CompletionException`. The HTTP client's `close()` method declares `IOException`. Handle these two boundaries as well as the specific SDK errors.

The HTTP client treats 503 `relay_refused` through Apache HttpClient's ordinary
429/503 retry strategy. That behavior does not provide retained-operation
recovery over HTTP.

On a multi-node cluster, an HTTP call often reaches a node that is not the
partition primary. For message sends, polls and consumer offset calls, that
node relays the call to the primary. The relay needs identical `http.jwt`
secrets or `cluster.auth` on every node. Without that key material, these calls
answer 503 with code 58. See
[HTTP routing and backpressure](../../core/server/README.md#http-routing-and-backpressure).

### Retained TCP operations

Receipt access follows the server's [session and revocation policy](../../core/server/README.md#sessions-and-retry-receipts).
An unresolved operation cannot recover its receipt after its original session
ends, even after a new login or account reactivation. A fresh send can duplicate
that operation.

Each node counts the messages in its stream, topic and system stats. After a
successful send, a node that is not the partition primary shows the new count
only after the next prepare or commit broadcast. When no other write follows,
this delay is at most `cluster.commit_broadcast_interval` (500 ms by default).
See [metadata freshness and counters](../../core/server/README.md#metadata-freshness-and-counters).

Replicated mutations serialize within their metadata or partition lane. If a sent mutation's
outcome cannot be established, `IggyOperationException` carries
`IggyErrorCode.OPERATION_UNCERTAIN` and its exact operation ID. A later mutation in the same
lane returns `LANE_BLOCKED` with that ID. Unrelated partitions and pure reads continue.

Only a reply that refuses a sent mutation, or a committed result for it, settles it. A lost
connection, an eviction or a malformed reply leaves it uncertain. After a lost connection, a
stale-client eviction or a malformed reply, the call replays the same request on a new
connection within its deadline. If the server refuses to bind the session again, the client
registers a new session and drops the operation.

A call that loses its connection before it sends its request, or that cannot open one, waits for a connection within its deadline. If no connection carries it by then, the call ends with 58 (`TRANSIENT_NOT_ACCEPTED`).

An eviction and a reply can carry the same status, for example 30 (`STALE_CLIENT`) or 40
(`UNAUTHENTICATED`). Only the eviction is a lost connection, so the call does not replay a
request that a reply refused. A refused first attempt ends with that error. After an earlier
sent attempt without a proved reply, the call ends with `OPERATION_UNCERTAIN`.

Both blocking and async TCP clients expose `resolveOperation(operationId, timeout)` and
`abandonOperation(operationId)`. Resolve reuses the original request bytes and returns the
response body, or the proved business error. A resolve that finds a replay of the same operation
running waits for it within its own timeout. If that wait runs out, the resolve fails with
`OPERATION_UNCERTAIN`. Java `long` preserves all 64 operation-ID bits, including negative values.
When you display an ID, use `Long.toUnsignedString(operationId)`.

The client keeps the proved outcomes of its last replays, up to the operation count of
`.pendingOperations`. A later resolve returns the same outcome without a request, also after a
session reset. Abandon releases local retention and frees the pending operation slot at once. It
does not cancel or undo server work. A running replay keeps the lane until the replay ends, so a
later mutation in that lane waits for it. That replay still keeps the outcome that it proves.
`OPERATION_NOT_RETAINED` provides no evidence that the original mutation was uncommitted. It
means that a session reset or an abandon came before a proved outcome, or that the proved outcome
aged out. Do not resend it automatically.

The TCP builders accept `.pendingOperations(count, bytes)`, defaulting to 256 mutations
and 64 MiB across active and retained frames. Admission waits within the call's existing
deadline; an oversized frame fails locally. Cancellation removes only the caller's waiter.
A mutation already sent may remain retained. The byte limit counts admitted frozen frames and
writer references; API input snapshots and queued serialization buffers are outside this limit.

Auto-commit and `Next` polls use a temporary partition ordering barrier. Their success
acknowledges local admission, not replicated offset durability. Code 57, a lost or malformed
reply, or an eviction ends the call with code 57. Progress is unknown, and the call
releases the barrier. These polls are never retained or replayed after uncertainty. Explicit non-admission
(58 or 89) may retry within the same deadline.

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
