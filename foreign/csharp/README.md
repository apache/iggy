# C# SDK for [Iggy](https://github.com/apache/iggy) [![Nuget (with prereleases)](https://img.shields.io/nuget/v/Apache.Iggy)](https://www.nuget.org/packages/Apache.Iggy)

## Overview

The Apache Iggy C# SDK provides a comprehensive client library for interacting with Iggy message streaming servers. It
offers a modern, async-first API with support for multiple transport protocols and comprehensive message streaming
capabilities.

## Getting Started

### Installation

Install the NuGet package:

```bash
dotnet add package Apache.Iggy
```

### Supported Protocols

The SDK supports two transport protocols:

- **TCP** - Binary protocol for optimal performance and lower latency (recommended)
- **HTTP** - RESTful JSON API for stateless operations

### Creating a Client

The SDK is built around the `IIggyClient` interface. To create a client instance:

```c#
var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
{
    BaseAddress = "127.0.0.1:8090",
    Protocol = Protocol.Tcp
});

await client.ConnectAsync();
```

Optionally, you can provide an `ILoggerFactory` for diagnostics and debugging (defaults to `NullLoggerFactory.Instance`):

```c#
var loggerFactory = LoggerFactory.Create(builder =>
{
    builder
        .AddFilter("Apache.Iggy", LogLevel.Information)
        .AddConsole();
});

var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
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
var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
{
    BaseAddress = "127.0.0.1:8090",
    Protocol = Protocol.Tcp,

    // Buffer sizes (optional, default: 4096)
    ReceiveBufferSize = 4096,
    SendBufferSize = 4096,

    // TLS/SSL configuration
    TlsSettings = new TlsSettings
    {
        Enabled = true,
        Hostname = "iggy",
        CertificatePath = "/path/to/cert"
    },

    // Automatic reconnection with exponential backoff
    ReconnectionSettings = new ReconnectionSettings
    {
        Enabled = true,
        MaxRetries = 3,              // 0 = infinite retries
        InitialDelay = TimeSpan.FromSeconds(5),
        MaxDelay = TimeSpan.FromSeconds(30),
        WaitAfterReconnect = TimeSpan.FromSeconds(1),
        UseExponentialBackoff = true,
        BackoffMultiplier = 2.0
    },

    // Auto-login after connection
    AutoLoginSettings = new AutoLoginSettings
    {
        Enabled = true,
        Username = "your_username",
        Password = "your_password"
    },

    // Optional: logging
    LoggerFactory = loggerFactory
});

await client.ConnectAsync();
```

## Authentication

### User Login

Begin by using the root account (note: the root account cannot be removed or updated):

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
var patResponse = await client.CreatePersonalAccessTokenAsync("api-token", 3600);

// Login with PAT
await client.LoginWithPersonalAccessTokenAsync(patResponse.Token);
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
    replicationFactor: 1,
    messageExpiry: 0,  // 0 = never expire
    maxTopicSize: 0    // 0 = unlimited
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
Partitioning.None()

// Send to specific partition
Partitioning.PartitionId(1)

// Key-based partitioning (string)
Partitioning.EntityIdString("user-123")

// Key-based partitioning (integer)
Partitioning.EntityIdInt(12345)

// Key-based partitioning (GUID)
Partitioning.EntityIdGuid(Guid.NewGuid())
```

### User-Defined Headers

Add custom headers to messages with typed values:

```c#
var headers = new Dictionary<HeaderKey, HeaderValue>
{
    { new HeaderKey { Value = "correlation_id" }, HeaderValue.FromString("req-123") },
    { new HeaderKey { Value = "priority" }, HeaderValue.FromInt32(1) },
    { new HeaderKey { Value = "timeout" }, HeaderValue.FromInt64(5000) },
    { new HeaderKey { Value = "confidence" }, HeaderValue.FromFloat(0.95f) },
    { new HeaderKey { Value = "is_urgent" }, HeaderValue.FromBool(true) },
    { new HeaderKey { Value = "request_id" }, HeaderValue.FromGuid(Guid.NewGuid()) }
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

### Flushing Unsaved Buffer

Force a flush of the in-memory buffer to disk for a specific partition:

```c#
await client.FlushUnsavedBufferAsync(
    Identifier.String("my-stream"),
    Identifier.String("my-topic"),
    partitionId: 1,
    fsync: true
);
```

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
    Identifier.Numeric(1)  // consumer ID
);

// Leave a consumer group
await client.LeaveConsumerGroupAsync(
    Identifier.String("my-stream"),
    Identifier.String("my-topic"),
    Identifier.Numeric(1)  // consumer ID
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

### Polling Strategies

Control where message consumption starts:

```c#
// Start from a specific offset
PollingStrategy.Offset(1000)

// Start from a specific timestamp (microseconds since epoch)
PollingStrategy.Timestamp(1699564800000000)

// Start from the first message
PollingStrategy.First()

// Start from the last message
PollingStrategy.Last()

// Start from the next unread message
PollingStrategy.Next()
```

## Offset Management

### Storing Offsets

Store the current consumer position:

```c#
await client.StoreOffsetAsync(
    Identifier.String("my-stream"),
    Identifier.String("my-topic"),
    Identifier.Numeric(1),  // consumer ID
    0,                      // partition ID
    42                      // offset value
);
```

### Retrieving Offsets

Get the current stored offset:

```c#
var offsetInfo = await client.GetOffsetAsync(
    Identifier.String("my-stream"),
    Identifier.String("my-topic"),
    Identifier.Numeric(1),  // consumer ID
    0                       // partition ID
);

Console.WriteLine($"Current offset: {offsetInfo.Offset}");
```

### Deleting Offsets

Clear stored offsets:

```c#
await client.DeleteOffsetAsync(
    Identifier.String("my-stream"),
    Identifier.String("my-topic"),
    Identifier.Numeric(1),  // consumer ID
    0                       // partition ID
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

Available snapshot types: `FilesystemOverview`, `ProcessList`, `ResourceUsage`, `Test`, `ServerLogs`, `ServerConfig`, `All`.

### Segment Management

Delete the last N segments from a partition:

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
// Subscribe to connection events
client.SubscribeConnectionEvents(async connectionState =>
{
    Console.WriteLine($"Current connection state: {connectionState.CurrentState}");

    await SaveConnectionStateLog(connectionState.CurrentState);
});

// Unsubscribe
client.UnsubscribeConnectionEvents(handler);
```

## Advanced: IggyPublisher

High-level publisher with background sending, retries, and encryption:

```c#
var publisher = IggyPublisherBuilder.Create(
    client,
    Identifier.String("my-stream"),
    Identifier.String("my-topic")
)
.WithBackgroundSending(enabled: true, batchSize: 100)
.WithRetry(maxAttempts: 3)
.Build();

await publisher.InitAsync();

var messages = new List<Message>
{
    new(Guid.NewGuid(), "Message 1"u8.ToArray()),
    new(0, "Message 2"u8.ToArray())
};

await publisher.SendMessages(messages);

// Wait for all messages to be sent
await publisher.WaitUntilAllSends();
await publisher.DisposeAsync();
```

For automatic object serialization, use the typed variant:

```c#
class OrderSerializer : ISerializer<Order>
{
    public byte[] Serialize(Order data) =>
        Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data));
}

var publisher = IggyPublisherBuilder<Order>.Create(
    client,
    Identifier.String("orders-stream"),
    Identifier.String("orders-topic"),
    new OrderSerializer()
).Build();

await publisher.InitAsync();
await publisher.SendAsync(new List<Order> { /* ... */ });
```

## Advanced: IggyConsumer

High-level consumer with automatic offset management and consumer groups:

```c#
var consumer = IggyConsumerBuilder.Create(
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
var consumer = IggyConsumerBuilder.Create(
    client,
    Identifier.String("my-stream"),
    Identifier.String("my-topic"),
    Consumer.Group("my-group")
)
.WithConsumerGroup("my-group", createIfNotExists: true)
.WithPollingStrategy(PollingStrategy.Next())
.WithAutoCommitMode(AutoCommitMode.AfterReceive)
.Build();

await consumer.InitAsync();

await foreach (var message in consumer.ReceiveAsync())
{
    Console.WriteLine($"Partition {message.PartitionId}: {message.Message.Payload}");
}

await consumer.DisposeAsync();
```

For automatic deserialization:

```c#
class OrderDeserializer : IDeserializer<OrderEvent>
{
    public OrderEvent Deserialize(byte[] data) =>
        JsonSerializer.Deserialize<OrderEvent>(Encoding.UTF8.GetString(data))!;
}

var consumer = IggyConsumerBuilder<OrderEvent>.Create(
    client,
    Identifier.String("orders-stream"),
    Identifier.String("orders-topic"),
    Consumer.Group("order-processors"),
    new OrderDeserializer()
)
.WithAutoCommitMode(AutoCommitMode.Auto)
.Build();

await consumer.InitAsync();

await foreach (var message in consumer.ReceiveDeserializedAsync())
{
    if (message.Status == MessageStatus.Success)
    {
        Console.WriteLine($"Order: {message.Data?.OrderId}");
    }
}
```

## API Reference

The SDK provides the following main interfaces:

- **IIggyClient** - Main client interface (aggregates all features)
- **IIggyPublisher** - High-level message publishing interface
- **IIggyConsumer** - High-level message consumption interface
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

Start the Iggy server:

```bash
cargo run --bin iggy-server
```

Run an example (from the `examples/csharp/` directory):

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

```bash
cargo build

docker build --no-cache -f core/server/Dockerfile --platform linux/amd64 --target runtime-prebuilt --build-arg PREBUILT_IGGY_SERVER=target/debug/iggy-server --build-arg PREBUILT_IGGY_CLI=target/debug/iggy -t local-iggy-server .
```

#### 2. Build the Test Project

```bash
dotnet build foreign/csharp/Iggy_SDK.Tests.Integration
```

#### 3. Run test

```bash
cd foreign/csharp
export IGGY_SERVER_DOCKER_IMAGE=local-iggy-server
dotnet test -f net10.0 --project Iggy_SDK.Tests.Integration --no-build --verbosity diagnostic
```

## Useful Resources

- [Iggy Documentation](https://iggy.apache.org/docs/)
- [NuGet Package](https://www.nuget.org/packages/Apache.Iggy)

## ROADMAP - TODO

- [ ] Error handling with status codes and descriptions
- [ ] Add support for `ASP.NET Core` Dependency Injection
