<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-darkbg.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg">
    <img alt="Apache Iggy" src="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg" width="320">
  </picture>
</div>

# C# SDK for [Iggy](https://github.com/apache/iggy) [![Nuget (with prereleases)](https://img.shields.io/nuget/v/Apache.Iggy)](https://www.nuget.org/packages/Apache.Iggy)

## Overview

The Apache Iggy C# SDK provides a comprehensive client library for interacting with Iggy message streaming servers. It
offers a modern, async-first API with support for multiple transport protocols and comprehensive message streaming
capabilities.

## Getting Started

### Installation

Install the NuGet package:

```bash
dotnet add package Apache.Iggy --version 0.9.0-edge.9
```

The examples target server 0.9.0. For source builds, use the server and SDK from the same checkout.
The SDK targets .NET 8 and .NET 10; repository examples require .NET 10. `0.9.0-edge.9`
includes the independent message and consumer-offset durability options.

### Supported Protocols

The SDK supports two transport protocols:

- **TCP** - Binary protocol for optimal performance and lower latency (recommended)
- **HTTP** - RESTful JSON API for stateless operations

Over TCP the SDK speaks the VSR consensus framing, which is the only wire protocol the server accepts.

See [Viewstamped Replication (VSR)](#viewstamped-replication-vsr) for what that means for the client API.

### Creating a Client

The SDK is built around the `IIggyClient` interface. The API fragments below reuse this client and
these imports. Create the named resources before read, update, delete, send, or poll operations:

```c#
using System.Buffers;
using System.Text;
using System.Text.Json;
using Apache.Iggy;
using Apache.Iggy.Configuration;
using Apache.Iggy.Consumers;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Enums;
using Apache.Iggy.Factory;
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Publishers;
using Microsoft.Extensions.Logging;
using Partitioning = Apache.Iggy.Kinds.Partitioning;


using var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
{
    BaseAddress = "127.0.0.1:8090",
    Protocol = Protocol.Tcp
});

await client.ConnectAsync();
await client.LoginUserAsync("iggy", "iggy");
```

Optionally, you can provide an `ILoggerFactory` for diagnostics and debugging (defaults to
`NullLoggerFactory.Instance`). Add `Microsoft.Extensions.Logging.Console` to use `AddConsole`:

```c#
using var loggerFactory = LoggerFactory.Create(builder =>
{
    builder
        .AddFilter("Apache.Iggy", LogLevel.Information)
        .AddConsole();
});

using var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
{
    BaseAddress = "127.0.0.1:8090",
    Protocol = Protocol.Tcp,
    LoggerFactory = loggerFactory
});

await client.ConnectAsync();
```

### Configuration

The `IggyClientConfigurator` provides comprehensive configuration options:

```c#
using var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
{
    BaseAddress = "127.0.0.1:8090",
    Protocol = Protocol.Tcp,

    // Socket buffer sizes in bytes (optional, null = OS default)
    ReceiveBufferSize = null,
    SendBufferSize = null,

    // Idle ping keeping the session alive under server-side heartbeat verification (TCP only).
    // Default 5 seconds, must be between 1 millisecond and about 49 days. Init-only.
    HeartbeatInterval = TimeSpan.FromSeconds(5),

    // Automatic reconnection with exponential backoff (enabled by default, infinite retries)
    ReconnectionSettings = new ReconnectionSettings
    {
        Enabled = true,
        MaxRetries = 0,              // 0 = infinite retries
        InitialDelay = TimeSpan.FromSeconds(5),
        MaxDelay = TimeSpan.FromSeconds(30),
        WaitAfterReconnect = TimeSpan.FromSeconds(1),
        UseExponentialBackoff = true,
        BackoffMultiplier = 2.0
    },

    // Auto-login after connection. Optional for reconnection: a client that signs in with
    // LoginUserAsync has that sign-in replayed on a reconnect too. Without either, a reconnect
    // cannot restore the session and a lost connection fails the request
    AutoLoginSettings = AutoLoginSettings.For("iggy", "iggy"),
    // or AutoLoginSettings.ForPersonalAccessToken("your_token")

    // Optional: logging
    LoggerFactory = loggerFactory
});

await client.ConnectAsync();
```

TCP applies the connection, heartbeat, TLS, and auto-login settings. HTTP `ConnectAsync` does no
network work and requires explicit login. For TLS configuration, see the [TcpTls example](../../examples/csharp/README.md#tcptls).

## Viewstamped Replication (VSR)

Over TCP every request is wrapped in a 256-byte consensus header, the client registers a consensus session at
login, and replicated writes complete after the required quorum acknowledges them.
`Durability.Persisted` additionally waits for persistence on that quorum; explicit offset stores have
an independent `ConsumerOffsetDurability` policy. The `IIggyClient` surface is unchanged, with the
few exceptions listed under [Limitations](#limitations).

```c#
using var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
{
    BaseAddress = "127.0.0.1:8090",
    Protocol = Protocol.Tcp,

    // Upper bound on a reply frame the server announces, 64 MiB by default.
    MaxResponseFrameSize = 64 * 1024 * 1024,

    AutoLoginSettings = AutoLoginSettings.For("iggy", "iggy")
});

await client.ConnectAsync();
```

### What changes under VSR

- **Login binds a session.** `LoginUserAsync` / `LoginWithPersonalAccessTokenAsync` run the register handshake
  at login, and the session lives for as long as the connection. Logging out, being evicted
  or losing the connection ends it, and the next login registers a fresh one.
- **Leader redirection is automatic.** The client reads the cluster roster, follows the current leader and
  re-checks it when a request is refused because the node stopped being primary.
- **The client picks partitions.** Balanced and message-key partitioning are resolved
  client-side (xxHash32 matches Rust for identical encoded key bytes and partition counts), and consumer-group polls round-robin
  over the partitions the coordinator assigned to this client.
- **Consumer groups are assignment-based.** `JoinConsumerGroupAsync` makes this client a member; the assignment
  is synced on demand and refreshed on every `PingAsync`. Partition counts are cached for 30 seconds, so a topic
  another client widens is picked up without waiting for a ping.
- **Credentials are bounds-checked locally.** A username outside 3-50 bytes, a password outside 3-100 bytes or a
  personal access token outside 1-255 bytes is rejected before the register body is framed.
- **`PingAsync` costs more than a ping.** Besides the ping it re-syncs the assignment of every consumer group
  this client has joined, so it makes one extra round trip per joined group. The TCP client pings on its own
  every `HeartbeatInterval` (5 seconds by default) while connected, so an idle session survives the server's
  heartbeat verification and assignments stay fresh; a lost session is repaired by the regular reconnect and
  auto login.

### Retries and failed requests

Within its request deadline, the SDK can replay a request the server reports as never admitted.
Replay-safe operations can also be retried after a lost connection. Two cases surface to the caller:

- `IggyInvalidStatusCodeException` carries the server status code, with `FromServer` telling apart a verdict the
  cluster reported from a failure the client raised itself.
- `VsrRequestOutcomeUnknownException` means no server verdict arrived after the request was written - the
  connection was lost, the call was cancelled, or the server evicted the session while the request was in
  flight - so the cluster may or may not have committed it. The SDK will not replay it on a new session,
  because that would bypass server-side deduplication - re-issuing it is the caller's decision.
  Background `IggyPublisher` sends report it through the message-batch-failed event without retrying;
  direct sends throw it to the caller, and
  `IggyConsumer` rethrows it rather than swallowing it, because an auto-committing poll may have advanced the
  offset already. Rethrowing ends the consumer's polling loop: catch it around the enumeration, decide whether
  the operation is safe to re-issue, and start consuming again.

### Limitations

- TCP uses VSR framing; HTTP uses the separate REST API. Join/leave, `GetMeAsync`, segment deletion,
  and raw binary requests are TCP-only and throw `FeatureUnavailableException` on HTTP.
- `StoreOffsetAsync` / `DeleteOffsetAsync` need an explicit partition id under VSR: the broker does not
  resolve a `null` partition for a consumer-offset request, so passing one throws client-side.
- No on-demand flush API is available; configure topic durability instead.
- Polling a missing topic throws `IggyInvalidStatusCodeException`. An existing topic with no available
  messages returns an empty poll.

### Behaviour changes for existing clients

- `MaxResponseFrameSize` bounds the reply frames the **VSR** reader accepts. A reply larger than the 64 MiB
  default is refused and the connection is dropped, so raise it if a single response legitimately exceeds that
  - a large `GetSnapshotAsync` is the usual case.
- Clients built with `IggyConsumerBuilder` / `IggyPublisherBuilder` now auto-login with the credentials passed
  to `WithConnection`. Before, a builder-created client came back from a
  reconnect unauthenticated; now the credentials are held for the lifetime of the connection and replayed.
- The TCP client pings every `HeartbeatInterval` (5 seconds) and reconnects by default with unlimited
  failed roster passes. Each pass tries the known addresses before consuming one retry. Connection
  and TLS handshake failures can be retried; an invalid local CA file stops after the pass, and
  rejected credentials are surfaced. Set `MaxRetries`, pass a cancellation token, or disable
  reconnection to bound attempts. Request replay has its own deadline and outcome-safety rules.
  Configured auto-login credentials take precedence over credentials remembered from successful
  username/password or PAT login. Without either, reconnecting cannot restore the authenticated session.
- `AutoLoginSettings` properties are now `init`-only, as is `IggyClientConfigurator.HeartbeatInterval`. Build
  them with an object initializer or the `AutoLoginSettings.For` / `AutoLoginSettings.ForPersonalAccessToken`
  factories instead of assigning after construction.
- `IggyConsumerBuilder` / `IggyPublisherBuilder` accept a personal access token through the
  `WithConnection(protocol, address, personalAccessToken, ...)` overload, as an alternative to a username and
  password.
- The SDK now ships a dependency on `System.IO.Hashing`, used for the client-side message-key partitioner.
- TCP sockets are opened with `NoDelay`. The protocol is request/reply, so a write is
  always the last one before the client waits for the answer and Nagle has nothing to coalesce it with - it
  only held back the trailing segment of a large request until the previous one was acked.

## Authentication

### User Login

Use the credentials configured for the server. The setup below initializes a new server with `iggy` / `iggy`:

```c#
var response = await client.LoginUserAsync("iggy", "iggy");
```

### Creating Users

Create new users with customizable permissions:

```c#
var permissions = new Permissions
{
    Global = new GlobalPermissions
    {
        ManageServers = true,
        ManageUsers = true,
        ManageStreams = true,
        ManageTopics = true,
        PollMessages = true,
        ReadServers = true,
        ReadStreams = true,
        ReadTopics = true,
        ReadUsers = true,
        SendMessages = true
    }
};

await client.CreateUserAsync("test_user", "secure_password", UserStatus.Active, permissions);

// Login with the new user
var loginResponse = await client.LoginUserAsync("test_user", "secure_password");
```

### Personal Access Tokens

Create and use Personal Access Tokens (PAT) for programmatic access:

```c#
// Create a PAT
var patResponse = await client.CreatePersonalAccessTokenAsync("api-token", TimeSpan.FromHours(1));

// Login with PAT
await client.LoginWithPersonalAccessTokenAsync(patResponse!.Token);
```

## Streams and Topics

### Creating Streams

```c#
await client.CreateStreamAsync("my-stream");
```

You can reference streams by either numeric ID or name:

```c#
var streamById = Identifier.Numeric(0);
var streamByName = Identifier.String("my-stream");
```

### Creating Topics

Every stream contains topics for organizing messages:

```c#
var streamId = Identifier.String("my-stream");

await client.CreateTopicAsync(
    streamId,
    name: "my-topic",
    partitionsCount: 3,
    compressionAlgorithm: CompressionAlgorithm.None,
    messageExpiry: TimeSpan.MaxValue,  // never expire
    maxTopicSize: 1024 * 1024 * 1024   // 1 GiB
);
```

Note: Stream and topic names use hyphens instead of spaces. Iggy automatically replaces spaces with hyphens.

## Publishing Messages

### Sending Messages

Send messages using the publisher interface:

```c#
var streamId = Identifier.String("my-stream");
var topicId = Identifier.String("my-topic");

var messages = new List<Message>
{
    new(Guid.NewGuid(), "Hello, Iggy!"u8.ToArray()),
    new(1, "Another message"u8.ToArray())
};

await client.SendMessagesAsync(
    streamId,
    topicId,
    Partitioning.None(),  // balanced partitioning
    messages
);
```

### Partitioning Strategies

Control which partition receives each message:

```c#
// Balanced partitioning (default)
Partitioning.None();

// Send to specific partition
Partitioning.PartitionId(1);

// Key-based partitioning (string)
Partitioning.EntityIdString("user-123");

// Key-based partitioning (integer)
Partitioning.EntityIdInt(12345);

// Key-based partitioning (GUID)
Partitioning.EntityIdGuid(Guid.NewGuid());
```

### User-Defined Headers

Add custom headers to messages with typed values:

```c#
var headers = new Dictionary<HeaderKey, HeaderValue>
{
    { HeaderKey.FromString("correlation_id"), HeaderValue.FromString("req-123") },
    { HeaderKey.FromString("priority"), HeaderValue.FromInt32(1) },
    { HeaderKey.FromString("timeout"), HeaderValue.FromInt64(5000) },
    { HeaderKey.FromString("confidence"), HeaderValue.FromFloat(0.95f) },
    { HeaderKey.FromString("is_urgent"), HeaderValue.FromBool(true) },
    { HeaderKey.FromString("request_id"), HeaderValue.FromGuid(Guid.NewGuid()) }
};

var messages = new List<Message>
{
    new(Guid.NewGuid(), "Message with headers"u8.ToArray(), headers)
};

await client.SendMessagesAsync(
    streamId,
    topicId,
    Partitioning.PartitionId(1),
    messages
);
```

### Message durability

There is no on-demand flush command. Set `TopicOptions.Durability = Durability.Persisted` when creating
a topic to wait for message persistence on the required quorum. `ConsumerOffsetDurability` controls
explicit offset-store completion independently. Flush thresholds alone do not provide that guarantee.

## Consumer Groups

### Creating Consumer Groups

Coordinate message consumption across multiple consumers:

```c#
var groupResponse = await client.CreateConsumerGroupAsync(
    Identifier.String("my-stream"),
    Identifier.String("my-topic"),
    "my-consumer-group"
);
```

### Joining and Leaving Groups

**Note:** Join/Leave operations are only supported on TCP protocol and will throw `FeatureUnavailableException` on HTTP.

```c#
// Join a consumer group
await client.JoinConsumerGroupAsync(
    Identifier.String("my-stream"),
    Identifier.String("my-topic"),
    Identifier.String("my-consumer-group")
);

// Leave a consumer group
await client.LeaveConsumerGroupAsync(
    Identifier.String("my-stream"),
    Identifier.String("my-topic"),
    Identifier.String("my-consumer-group")
);
```

## Consuming Messages

### Fetching Messages

Fetch a batch of messages:

```c#
var polledMessages = await client.PollMessagesAsync(new MessageFetchRequest
{
    StreamId = streamId,
    TopicId = topicId,
    Consumer = Consumer.New(1), // or Consumer.Group("my-consumer-group") for consumer group
    Count = 10,
    PartitionId = 0, // optional, null for consumer group
    PollingStrategy = PollingStrategy.Next(),
    AutoCommit = true
});

foreach (var message in polledMessages.Messages)
{
    Console.WriteLine($"Message: {Encoding.UTF8.GetString(message.Payload)}");
}
```

With `AutoCommit = true`, the server advances/submits the offset before application processing.
The poll reply does not wait for durable completion of that store. Use explicit offset stores after
processing when that distinction matters.

### Polling Strategies

Control where message consumption starts:

```c#
// Start from a specific offset
PollingStrategy.Offset(1000);

// Start from a specific timestamp (microseconds since epoch)
PollingStrategy.Timestamp(1699564800000000);

// Start from the first message
PollingStrategy.First();

// Start from the last message
PollingStrategy.Last();

// Start from the next unread message
PollingStrategy.Next();
```

## Offset Management

### Storing Offsets

Store the current consumer position:

```c#
await client.StoreOffsetAsync(
    Consumer.New(1),
    Identifier.String("my-stream"),
    Identifier.String("my-topic"),
    offset: 42,
    partitionId: 0
);
```

### Retrieving Offsets

Get the current stored offset:

```c#
var offsetInfo = await client.GetOffsetAsync(
    Consumer.New(1),
    Identifier.String("my-stream"),
    Identifier.String("my-topic"),
    partitionId: 0
);

Console.WriteLine($"Stored offset: {offsetInfo!.StoredOffset}");
```

### Deleting Offsets

Clear stored offsets:

```c#
await client.DeleteOffsetAsync(
    Consumer.New(1),
    Identifier.String("my-stream"),
    Identifier.String("my-topic"),
    partitionId: 0
);
```

## System Operations

### Cluster Information

Get cluster metadata and node information:

```c#
var metadata = await client.GetClusterMetadataAsync();
```

### Server Statistics

Retrieve server performance metrics:

```c#
var stats = await client.GetStatsAsync();
```

### Health Checks

Verify server connectivity:

```c#
await client.PingAsync();
```

### Client Information

Get information about connected clients:

```c#
var clients = await client.GetClientsAsync();
var currentClient = await client.GetMeAsync();
```

### Snapshots

Capture a system snapshot as a compressed ZIP archive:

```c#
var snapshotBytes = await client.GetSnapshotAsync(
    SnapshotCompression.Zstd,
    new List<SystemSnapshotType>
    {
        SystemSnapshotType.ServerLogs,
        SystemSnapshotType.ServerConfig,
        SystemSnapshotType.ResourceUsage
    }
);

// Or capture everything
var fullSnapshot = await client.GetSnapshotAsync(
    SnapshotCompression.Deflated,
    new List<SystemSnapshotType> { SystemSnapshotType.All }
);
```

Available compression methods: `Stored`, `Deflated`, `Bzip2`, `Zstd`, `Lzma`, `Xz`.

Available snapshot types: `FilesystemOverview`, `ProcessList`, `ResourceUsage`, `Test`, `ServerLogs`, `ServerConfig`,
`All`.

### Segment Management

Delete up to N of the oldest sealed segments from a partition; the active segment is retained:

```c#
await client.DeleteSegmentsAsync(
    Identifier.String("my-stream"),
    Identifier.String("my-topic"),
    partitionId: 1,
    segmentsCount: 2
);
```

## Event Subscription

Subscribe to connection events:

```c#
Func<ConnectionStateChangedEventArgs, Task> handler = async args =>
{
    Console.WriteLine($"Current connection state: {args.CurrentState}");
    await Task.CompletedTask;
};

client.SubscribeConnectionEvents(handler);

// Later
client.UnsubscribeConnectionEvents(handler);
```

## Advanced: IggyPublisher

High-level publisher with background sending and retries. Retry settings apply to background sends;
`maxAttempts` includes the first send. Await the queue drain before disposal:

```c#
using System.Buffers;
using System.Text;
using System.Text.Json;
using Apache.Iggy;
using Apache.Iggy.Consumers;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Publishers;

await using var publisher = IggyPublisherBuilder.Create(
    client,
    Identifier.String("my-stream"),
    Identifier.String("my-topic")
)
.WithPartitioning(Partitioning.None())
.WithBackgroundSending(enabled: true, batchSize: 100)
.WithRetry(maxAttempts: 3)
.Build();

await publisher.InitAsync();

var messages = new List<Message>
{
    new(Guid.NewGuid(), "Message 1"u8.ToArray()),
    new(0, "Message 2"u8.ToArray())
};

await publisher.SendMessagesAsync(messages);

// Drain the background queue, then dispose
await publisher.WaitUntilAllSendsAsync();
```

For automatic object serialization, use the typed variant:

```c#
await using var publisher = IggyPublisherBuilder<Order>.Create(
    client,
    Identifier.String("orders-stream"),
    Identifier.String("orders-topic"),
    new OrderSerializer()
).Build();

await publisher.InitAsync();
await publisher.SendAsync(new List<Order> { new(Guid.NewGuid(), 99.90m) });

record Order(Guid OrderId, decimal Amount);

class OrderSerializer : ISerializer<Order>
{
    public void Serialize(Order data, IBufferWriter<byte> writer) =>
        writer.Write(JsonSerializer.SerializeToUtf8Bytes(data));
}
```

## Advanced: IggyConsumer

High-level consumer with automatic offset management and consumer groups:

```c#
using System.Text;
using Apache.Iggy;
using Apache.Iggy.Consumers;
using Apache.Iggy.Kinds;

await using var consumer = IggyConsumerBuilder.Create(
    client,
    Identifier.String("my-stream"),
    Identifier.String("my-topic"),
    Consumer.New(1)
)
.WithPollingStrategy(PollingStrategy.Next())
.WithBatchSize(10)
.WithAutoCommitMode(AutoCommitMode.Auto)
.Build();

await consumer.InitAsync();

await foreach (var message in consumer.ReceiveAsync())
{
    var payload = Encoding.UTF8.GetString(message.Message.Payload);
    Console.WriteLine($"Offset {message.CurrentOffset}: {payload}");
}
```

For consumer groups (load-balanced across multiple consumers):

```c#
await using var consumer = IggyConsumerBuilder.Create(
    client,
    Identifier.String("my-stream"),
    Identifier.String("my-topic"),
    Consumer.Group("my-group")
)
.WithConsumerGroup("my-group", createIfNotExists: true, joinGroup: true)
.WithPollingStrategy(PollingStrategy.Next())
.WithAutoCommitMode(AutoCommitMode.AfterReceive)
.Build();

await consumer.InitAsync();

await foreach (var message in consumer.ReceiveAsync())
{
    var payload = Encoding.UTF8.GetString(message.Message.Payload);
    Console.WriteLine($"Partition {message.PartitionId}: {payload}");
}
```

For automatic deserialization:

```c#
var builder = IggyConsumerBuilder<OrderEvent>.Create(
    client,
    Identifier.String("orders-stream"),
    Identifier.String("orders-topic"),
    Consumer.Group("order-processors"),
    new OrderDeserializer()
);
builder.WithPollingStrategy(PollingStrategy.Next());
builder.WithAutoCommitMode(AutoCommitMode.AfterReceive);

await using var consumer = builder.Build();
await consumer.InitAsync();

await foreach (var message in consumer.ReceiveDeserializedAsync())
{
    if (message.Status == MessageStatus.Success)
    {
        Console.WriteLine($"Order: {message.Data?.OrderId}");
    }
}

record OrderEvent(Guid OrderId, decimal Amount);

class OrderDeserializer : IDeserializer<OrderEvent>
{
    public OrderEvent Deserialize(ReadOnlyMemory<byte> data) =>
        JsonSerializer.Deserialize<OrderEvent>(data.Span)!;
}
```

## API Reference

The SDK provides the following main interfaces:

- **IIggyClient** - Main client interface (aggregates all features)
- **IIggyPublisher** - Per-call message publishing interface
- **IIggyConsumer** - Per-call message consumption interface
- **IIggyStream** - Stream management
- **IIggyTopic** - Topic management
- **IIggyOffset** - Offset management
- **IIggyConsumerGroup** - Consumer group operations
- **IIggyPartition** - Partition operations
- **IIggySegment** - Segment management
- **IIggyUsers** - User and authentication management
- **IIggySystem** - System and cluster operations
- **IIggyPersonalAccessToken** - Personal access token management

Additionally, builder-based APIs are available:

- **IggyPublisherBuilder** / **IggyPublisherBuilder<T>** - Fluent publisher configuration
- **IggyConsumerBuilder** / **IggyConsumerBuilder<T>** - Fluent consumer configuration

## Running Examples

Examples are located in `examples/csharp/src/` in root iggy directory. Available examples:

- **GettingStarted** - Basic producer/consumer setup
- **Basic** - Simple message publishing and consuming
- **MessageHeaders** - Using custom message headers
- **MessageEnvelope** - Envelope pattern for message serialization
- **NewSdk** - High-level IggyPublisher/IggyConsumer API

Start the matching source server from the repository root (use configured credentials for an existing data directory):

```bash
IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy cargo run --bin iggy-server
```

Run an example from the `examples/csharp/` directory. Run TcpTls from the repository root so its
certificate paths resolve:

```bash
dotnet run -c Release --project src/GettingStarted/Iggy_SDK.Examples.GettingStarted.Producer
dotnet run -c Release --project src/GettingStarted/Iggy_SDK.Examples.GettingStarted.Consumer
```

## Integration Tests

Integration tests are located in `Iggy_SDK.Tests.Integration/`. Tests can run against:

- A dockerized Iggy server with TestContainers
- A local Iggy server (set `IGGY_SERVER_HOST` environment variable)

### Requirements

- .NET 10 SDK
- Docker (for TestContainers tests)

### Running Integration Tests Locally

#### 1. Dockerization

The suite runs against `iggy-server`. TCP only: the SDK frames TCP with the
VSR wire protocol, the cluster serves reads from the primary, and the HTTP surface has no equivalent path to
route them through.

```bash
cargo build --bin iggy-server --bin iggy

docker build --no-cache -f core/server/Dockerfile --platform linux/amd64 --target runtime-prebuilt --build-arg PREBUILT_IGGY_SERVER=target/debug/iggy-server --build-arg PREBUILT_IGGY_CLI=target/debug/iggy -t iggy-server:test .
```

#### 2. Build the Test Project

```bash
dotnet build foreign/csharp/Iggy_SDK.Tests.Integration
```

#### 3. Run test

```bash
cd foreign/csharp
export IGGY_SERVER_DOCKER_IMAGE=iggy-server:test
dotnet test -f net10.0 --project Iggy_SDK.Tests.Integration --no-build --verbosity diagnostic
```

`IGGY_SERVER_DOCKER_IMAGE` defaults to `iggy-server:test`, so the export above is only needed to point
at a different image. Rider and Visual Studio need nothing configured.

## Useful Resources

- [Iggy Documentation](https://iggy.apache.org/docs/)
- [NuGet Package](https://www.nuget.org/packages/Apache.Iggy)

## ROADMAP - TODO

- [ ] Error handling with status codes and descriptions
- [ ] Add support for `ASP.NET Core` Dependency Injection
