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
dotnet add package Apache.Iggy --version 0.9.0
```

The package installation and API examples target server/SDK 0.9.0. For source builds, use the server and SDK
from the same checkout. The SDK targets .NET 8 and .NET 10; repository examples require .NET 10.
`0.9.0` includes the independent message and consumer-offset durability options.

This checkout uses binary protocol 0.12.0. Servers and applicable SDKs must deploy together. Mixed versions and
rolling upgrades are unsupported. This checkout requires fresh storage and refuses older data directories
without modification. `--fresh` deletes existing data in the configured directory.
See the [server recovery guide](../../core/server/README.md#upgrade-recovery).
Register creates a shared logical session; other TCP connections authenticate with BindSession (15), using the
parent's identity and proof. A disconnect removes its binding without ending the session. Logout or committed expiry
ends it; a new session must not replay an unresolved mutation from the old one.
GetPartitionRoute (105) discovers partition primaries. Commands 14, 103, 104 and 123 are retired.
Ordinary SDK sends and explicit offset mutations support the configured durability, including the Replicated default.
Crash-safe send retries require Persisted message durability; crash-safe explicit offset retries require Persisted
offset durability and Quorum acknowledgement. Weaker policies can lose data and receipts on a crash. HTTP NoAck's
202 confirms dispatch, not partition admission or commit, and supplies no recoverable caller receipt.

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
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Apache.Iggy;
using Apache.Iggy.Configuration;
using Apache.Iggy.Consumers;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Encryption;
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

    // Auto-login after connection. Reconnect first binds the retained session; configured or
    // remembered LoginUserAsync credentials allow a fresh login after that session ends
    AutoLoginSettings = AutoLoginSettings.For("iggy", "iggy"),
    // or AutoLoginSettings.ForPersonalAccessToken("your_token")

    // Optional: logging
    LoggerFactory = loggerFactory
});

await client.ConnectAsync();
```

TCP applies the connection, heartbeat, TLS, and auto-login settings. HTTP `ConnectAsync` does no
network work and requires explicit login. For TLS configuration, see the [TcpTls example](../../examples/csharp/README.md#tcptls).

A public TCP sign-in uses the supplied credentials and supersedes any automatic sign-in in progress.
If that call fails, including validation, cancellation, timeout, or discovery after registration,
the tentative session is discarded and automatic sign-in stays disabled until another explicit
sign-in succeeds. Logout also withdraws configured auto-login. Calls that had not been sent under
the previous caller session fail as unauthenticated; possibly applied writes retain their uncertainty.
An automatic sign-in rejected with an older password snapshot retries a newer password committed
by the same caller session.

### Connection String

A TCP client can also be created from the connection string format shared with the other SDKs. The
credentials become the auto-login settings, so the client signs in on `ConnectAsync`:

```c#
using var client = IggyClientFactory.CreateClient("iggy://iggy:iggy@127.0.0.1:8090");
await client.ConnectAsync();

// A personal access token instead of a username and password, plus options
using var tokenClient = IggyClientFactory.CreateClient(
    "iggy+tcp://iggypat-your-token@127.0.0.1:8090?heartbeat_interval=10s&reconnection_retries=5");

// Parse into a configurator to adjust the remaining settings
var config = IggyClientConfigurator.FromConnectionString("iggy://iggy:iggy@127.0.0.1:8090");
config.LoggerFactory = loggerFactory;
```

| Option                  | Default     | Description                                                                    |
|-------------------------|-------------|--------------------------------------------------------------------------------|
| `tls`                   | `false`     | `true` or `false`                                                              |
| `tls_domain`            | server host | Server name for the TLS handshake, used for every node the client dials        |
| `tls_ca_file`           | empty       | CA certificate path, read when connecting, required with `tls=true`            |
| `reconnection_retries`  | `unlimited` | Count or `unlimited`. `0` turns reconnection off, also after a lost connection |
| `reconnection_interval` | `1s`        | Fixed delay between attempts and before the first redial, at least `1ms`       |
| `heartbeat_interval`    | `5s`        | Between `1ms` and about 49 days                                                |
| `reestablish_after`     | -           | Validated, then ignored. `reconnection_interval` paces the first redial        |
| `nodelay`               | -           | Validated, then ignored. The client always disables Nagle                      |

`reestablish_after` and `nodelay` are accepted so that strings shared with other SDKs keep parsing, but
they change nothing. The client has no `reestablish_after` window after a lost connection, and it always opens
sockets with `NoDelay`, so `nodelay=false` does not turn Nagle back on even though the Rust and Node.js SDKs leave
Nagle on by default.

`reconnection_retries=0` differs from the Rust SDK. After a lost connection, Rust still makes one reconnect
attempt, but this client fails the request at once. It also differs from `ReconnectionSettings.MaxRetries`,
where `0` means unlimited, so the parser maps `reconnection_retries=0` to `Enabled = false` instead.

Reconnection uses no exponential backoff, and after each reconnect the client waits
`ReconnectionSettings.WaitAfterReconnect` (1 second by default). Credentials are taken literally: they are
not percent-decoded and must not contain `@` or `:`. Bracketed IPv6 hosts such as
`iggy://iggy:iggy@[::1]:8090` are accepted. Durations look like `500ms`, `5s` or `1m30s`. Only `iggy://` and
`iggy+tcp://` are supported. A malformed string throws `FormatException`, whose message never
contains the connection string.

## Viewstamped Replication (VSR)

Over TCP every request is wrapped in a 256-byte consensus header, the client registers a consensus session at
login, and replicated writes complete after the required quorum acknowledges them.
`Durability.Persisted` additionally waits for persistence on that quorum; explicit offset stores have
an independent `ConsumerOffsetDurability` policy. A NoAck success is a weak acknowledgement and does not prove commit.
Uncertain replicated operations can be resolved through the recovery methods below.

```c#
using var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
{
    BaseAddress = "127.0.0.1:8090",
    Protocol = Protocol.Tcp,

    // Upper bound on a reply frame the server announces, 64 MiB by default.
    MaxResponseFrameSize = 64 * 1024 * 1024,
    MaxPendingOperations = 256,
    MaxPendingOperationBytes = 64L * 1024 * 1024,

    AutoLoginSettings = AutoLoginSettings.For("iggy", "iggy")
});

await client.ConnectAsync();
```

### What changes under VSR

- **Login registers a shared session in this checkout.** `LoginUserAsync` / `LoginWithPersonalAccessTokenAsync`
  authenticate Register. Auxiliary and reconnected transports bind the retained identity and proof without another Register.
  Disconnecting removes only that binding; Logout, committed lease expiry or user deactivation ends the session.
  A `StaleClient` eviction also removes only the binding of that link. The reconnect binds the same session
  again, so its consumer-group memberships stay.
  A committed `StaleClient` result means that the server ended the session, for example because the user was
  deleted or deactivated. The client drops that session, and with reconnection enabled it signs in with configured
  or remembered credentials. A `StaleClient` refusal before commit means only that the server detached the link
  that carried it from the session. A live session also gets this refusal after a replica replaced its client
  table. The client closes that link and binds the session again on a new link. If the session ended, that bind
  fails with `Unauthenticated` and the client signs in again. Otherwise the session, its retained operations and
  its consumer-group memberships stay.
  Password changes and PAT changes or expiry do not end an established session.
  A terminal BindSession refusal (`Unauthenticated` or `StaleClient`) permits a fresh login. A transient refusal
  retains the identity while that replica's background reconnect retries the bind. An unresolved mutation
  cannot be replayed under a fresh session. The older 0.9.0 connection-lifetime rule does not apply to this checkout.
- **Replica links are independent.** The client proactively opens one shared session link per roster replica.
  Calls use the advertised metadata or partition primary. Replies are matched by request id, so concurrent
  reads and different partition groups can progress independently; cancelling one call preserves other calls.
  With a one-node roster, the client uses `BaseAddress` instead of the address that the node advertises, so a
  port mapping or a port-forward works.
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

HTTP retries 429 and 503 responses, including `relay_refused`, within its existing
30-second retry budget. It uses the delta value of `Retry-After`, or 50 ms when
that value is absent. Retained-operation recovery below applies to TCP.

On a multi-node cluster, an HTTP call often reaches a node that is not the
partition primary. For message sends, polls and consumer offset calls, that
node relays the call to the primary. The relay needs identical `http.jwt`
secrets or `cluster.auth` on every node. Without that key material, these calls
answer 503 with code 58. See
[HTTP routing and backpressure](../../core/server/README.md#http-routing-and-backpressure).

Receipt access follows the server's [session and revocation policy](../../core/server/README.md#sessions-and-retry-receipts).
An unresolved operation cannot recover its receipt after its original session
ends, even after a new login or account reactivation. A fresh send can duplicate
that operation.

Each node counts the messages in its stream, topic and system stats. After a
successful send, a node that is not the partition primary shows the new count
only after the next prepare or commit broadcast. When no other write follows,
this delay is at most `cluster.commit_broadcast_interval` (500 ms by default).
See [metadata freshness and counters](../../core/server/README.md#metadata-freshness-and-counters).

Within one call deadline, the SDK retries an unresolved replicated mutation using its original session,
request id, context, and frozen frame. Metadata mutations share one lane; partition mutations share a lane
by incarnation and partition id, including requests using different names for the same partition.

- `IggyInvalidStatusCodeException` carries the server status. `FromServer` distinguishes a server verdict
  from a local error, and `Committed` identifies a proved committed result.
- `VsrRequestOutcomeUnknownException.OperationId` identifies a retained replicated mutation after its call
  ends without proof. Its lane stays blocked. `VsrLaneBlockedException.OperationId` identifies the operation
  blocking a subsequent mutation in the same lane; independent groups and pure reads can continue.
- `ResolveOperationAsync(operationId, timeout, token)` replays that frozen operation. A proved success or
  business rejection releases the lane; a rejection throws its original status. Further uncertainty retains it.
  The client keeps the proved outcomes of its last `MaxPendingOperations` replays, also across a session
  reset. A later resolve of the same operation returns the same outcome without a request.
- `AbandonOperationAsync(operationId)` releases local ownership without determining the outcome. It does
  not undo a committed operation. The operation stops counting against `MaxPendingOperations` at once.
  A `ResolveOperationAsync` that is still running is not canceled and keeps the lane until it ends, so later
  mutations of the lane still wait behind its replay.
  `VsrOperationNotRetainedException` means that no replay proved the outcome
  before a session reset, such as logout or session replacement, or an abandon. A proved outcome that aged
  out also causes it. It never proves non-commit and never authorizes an automatic fresh resend.

Auto-commit and `Next` polls hold a transient partition lane but are not retained or replayed after uncertainty.
Their unknown-outcome error has no operation id. `IggyConsumer` reports it through its error event and polls again.
Under `AutoCommitMode.Auto`, the server can store the offset of a batch whose reply was lost. A `Next` poll then
starts after that batch, so the consumer skips it. Behind a retained mutation these polls throw
`VsrLaneBlockedException`. `IggyConsumer` propagates it without resolving the blocker and ends its polling loop.
The application decides how to resume. Background `IggyPublisher` sends report unknown outcomes through the
message-batch-failed event; direct sends throw them to the caller.

`IggyConsumer` offset stores and deletes resolve any blocking operation before their own request. A background
`IggyPublisher` send resolves only a blocking batch of its own. Both continue after a proved commit or committed
rejection. The client keeps that outcome for the caller that received the operation id, such as the receiver of
the message-batch-failed event. That caller reads it with `ResolveOperationAsync`.

`MaxPendingOperations` and `MaxPendingOperationBytes` bound active plus retained mutation frames across the
client, defaulting to 256 operations and 64 MiB. Admission waits within the call deadline before allocating an
id or frozen frame. A replay shares its reservation; outstanding writers keep bytes charged until released.
Both limits must be positive. The byte limit includes the 256-byte header and the mutation body.
Smaller positive limits are valid but reject mutations that cannot fit; reads and login remain available.
Reads and control requests have a separate per-link limit of 256 pending calls and a 32-frame write queue.
Their bodies, including unknown raw non-replicated commands, are outside the mutation byte budget.

Partition primaries are spread over the replicas, so many of them are metadata backups. A backup applies metadata
commits after the metadata primary, so it can miss a stream, topic or partition that the metadata primary already
created. A replica can answer a route lookup, a send, a poll or an offset request with "not found". The status is
1009, 1010, 2010, 2011 or 3007. If that replica is not the metadata primary, the call looks up the route again and
retries. If the 30-second call deadline ends first, the call fails with `IggyInvalidStatusCodeException` and
status 58. A mutation with an unknown earlier outcome ends with `VsrRequestOutcomeUnknownException` instead.

Only a "not found" reply from the metadata primary is final. A leader change can leave the client with an old view
of the metadata primary. When the client believes that the replica that replied is the metadata primary, it reads
the cluster roster from that replica. When it has no ready link to the metadata primary, it does the same. If that
roster names this replica as the metadata primary, the reply is final. Otherwise the call retries.

### Limitations

- TCP uses VSR framing; HTTP uses the separate REST API. Join/leave, `GetMeAsync`, segment deletion,
  and raw binary requests are TCP-only and throw `FeatureUnavailableException` on HTTP.
- `StoreOffsetAsync` / `DeleteOffsetAsync` need an explicit partition id under VSR: the broker does not
  resolve a `null` partition for a consumer-offset request, so passing one throws client-side.
- Polling a missing topic throws `IggyInvalidStatusCodeException`. An existing topic with no available
  messages returns an empty poll.

### Behaviour changes for existing clients

- `IIggyPublisher.FlushUnsavedBufferAsync` was removed. Set `TopicOptions.Durability` to
  `Durability.Persisted` when creating a topic to wait for message persistence.
- `MaxResponseFrameSize` bounds the reply frames the **VSR** reader accepts. A reply larger than the 64 MiB
  default is refused and the connection is dropped, so raise it if a single response legitimately exceeds that
  - a large `GetSnapshotAsync` is the usual case.
- Clients built with `IggyConsumerBuilder` / `IggyPublisherBuilder` now auto-login with the credentials passed
  to `WithConnection`. Before, a builder-created client came back from a
  reconnect unauthenticated; now the credentials are held for the lifetime of the connection and replayed.
- The TCP client pings each ready replica every `HeartbeatInterval` (5 seconds). Each replica reconnects
  independently, with at most four concurrent dials, a two-second dial and TLS deadline, and jittered
  backoff capped at ten seconds. `MaxRetries` bounds one burst of consecutive retries per replica, and zero
  allows unlimited retries. While the client is signed in, each heartbeat restarts a replica link that used
  up its burst, with a fresh budget, so the client recovers from a longer outage without an application call.
  Logout and `Dispose` stop the restarts. Invalid local CA configuration and rejected credentials surface as
  errors. A caller's cancellation stops that call while the link owner continues reconnecting. Request replay
  has its own deadline.
  Reconnecting signs in with the last successful username/password or PAT login, or with the configured
  auto-login credentials if the caller never signed in. Without either, reconnecting cannot restore the
  authenticated session.
  A committed password change or rename of the signed-in user updates these credentials. This includes a
  change that `ResolveOperationAsync` proves. A committed rejection leaves them unchanged.
- `AutoLoginSettings` properties are now `init`-only, as is `IggyClientConfigurator.HeartbeatInterval`. Build
  them with an object initializer or the `AutoLoginSettings.For` / `AutoLoginSettings.ForPersonalAccessToken`
  factories instead of assigning after construction.
- `IggyConsumerBuilder` / `IggyPublisherBuilder` accept a personal access token through the
  `WithConnection(protocol, address, personalAccessToken, ...)` overload, as an alternative to a username and
  password.
- The SDK now ships a dependency on `System.IO.Hashing`, used for the client-side message-key partitioner.
- TCP sockets use `NoDelay` to avoid delaying trailing request segments behind acknowledgements.

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

A stream can carry its own data encryption key, exactly 32 random bytes. The key travels inside the create request, so use TLS when the network is not trusted. The server stores it wrapped under its own key and never returns it: reads report only where the key came from and its fingerprints. When the server has encryption enabled and no key is given, it generates one for the stream. The key cannot change after creation.

```c#
var key = RandomNumberGenerator.GetBytes(EncryptionKeys.KEY_LENGTH);
var stream = await client.CreateStreamAsync("payments", new StreamOptions { EncryptionKey = key });

Console.WriteLine(stream?.EncryptionKeySource);      // Stream
Console.WriteLine(stream?.EncryptionKeyFingerprint); // equals EncryptionKeys.Fingerprint(key)
```

You can reference streams by either numeric ID or name:

```c#
var streamById = Identifier.Numeric(0);
var streamByName = Identifier.String("my-stream");
```

### Creating Topics

Every stream contains topics for organizing messages. Settings a topic takes at creation go in `TopicOptions`, and every one left null keeps the server default:

```c#
var streamId = Identifier.String("my-stream");

await client.CreateTopicAsync(
    streamId,
    name: "my-topic",
    partitionsCount: 3,
    options: new TopicOptions
    {
        CompressionAlgorithm = CompressionAlgorithm.None,
        MessageExpiry = TimeSpan.MaxValue,  // never expire
        MaxTopicSize = 1024 * 1024 * 1024   // 1 GiB
    }
);
```

### Updating Streams, Topics and Users

An update names only what changes. Every property left null keeps the stored value, and `Reset` returns a key to its default:

```c#
await client.UpdateStreamAsync(streamId, new StreamUpdate { Name = "my-stream-v2" });

await client.UpdateTopicAsync(streamId, Identifier.String("my-topic"), new TopicUpdate
{
    MessageExpiry = TimeSpan.FromDays(7),
    Reset = new HashSet<TopicOptionKey> { TopicOptionKey.MaxTopicSize }
});

await client.UpdateUserAsync(Identifier.String("alice"), new UserUpdate { Status = UserStatus.Inactive });
```

Settings fixed at creation, such as compression, cannot be updated. `DescribeOptionsAsync(OptionsScope.Topic)` lists every key the server knows with its kind and whether it can be updated or reset. For a key newer than this SDK, set it through `Options` and reset it through `RawReset`, and the server decides whether the change is allowed.

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

### Metadata Changes

`GetMetadataChangesAsync` reads the node's change log: one record per public result of a committed metadata operation, such as a stream created, a topic renamed, a user's permissions changed or a session closed. A mirror keeps a cursor, asks for the records after it, and stores the returned `Next` cursor. The call needs root or the global `ManageServers` permission and works over TCP and HTTP.

```c#
var query = new MetadataChangesQuery { Groups = [MetadataChangeGroup.Streams, MetadataChangeGroup.Topics] };
while (true)
{
    var page = await client.GetMetadataChangesAsync(query);
    switch (page.Status)
    {
        case MetadataChangeStatus.Ok:
            foreach (var record in page.Records)
            {
                Console.WriteLine($"op {record.Op} by {record.ActorUserId}: {record.Kind}");
            }

            if (page.Next is { } next)
            {
                query = query with { Start = MetadataChangeStart.After(next) };
            }

            if (!page.HasMore)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
            }

            break;
        case MetadataChangeStatus.Behind:
            await Task.Delay(TimeSpan.FromMilliseconds(100));
            break;
        default:
            query = query with { Start = await RebuildAsync(client, query.Groups) };
            break;
    }
}

// Pages the current state, then returns the position to follow from.
static async Task<MetadataChangeStart> RebuildAsync(IIggyClient client, IReadOnlyList<MetadataChangeGroup> groups)
{
    var query = new MetadataChangesQuery { Start = MetadataChangeStart.Current(), Groups = groups };
    while (true)
    {
        var page = await client.GetMetadataChangesAsync(query);
        if (page.Status == MetadataChangeStatus.Behind)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(100));
            continue;
        }

        if (page.Status != MetadataChangeStatus.Ok)
        {
            query = query with { Start = MetadataChangeStart.Current() };
            continue;
        }

        foreach (var item in page.Items)
        {
            Console.WriteLine($"{item.Group}: {item.Body.GetType().Name}");
        }

        if (page.CurrentPageToken is { } token)
        {
            query = query with { Start = MetadataChangeStart.Current(token) };
        }
        else if (page.Next is { } anchor)
        {
            return MetadataChangeStart.After(anchor);
        }
        else
        {
            query = query with { Start = MetadataChangeStart.Current() };
        }
    }
}
```

`Expired` means the log no longer holds the history after the cursor, and `Reset` means the cursor belongs to another history, for example after the data directory was rebuilt. Either way the mirror rebuilds from current state pages and then follows the changes after the anchor they return.

Current pages need the parent groups of every selected group, for example `Topics` with `Streams`. Keep a rebuilt state private until a change page answers `Ok` with `HasMore` false and a `HeadOp` at or above the last current page's.

A cursor's `ToString()` is its `mc1_` text, which `MetadataChangeCursor.Parse` reads back, so a mirror can store it. A query the rules refuse fails with code 3 before anything is sent, and a record of a kind this SDK does not know arrives as `MetadataUnknownBody` or `MetadataUnknownJsonBody`.

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

Disposal cancels and drains pending offset stores, then checkpoints the last message delivered
from each partition before leaving the group. Buffered messages are excluded, and each checkpoint
keeps the delivered message's partition context. `AutoCommitMode.Disabled` skips this final
checkpoint. The final stores share one 30-second budget; failures are logged and cleanup continues.
Disposing from inside a receive loop is supported, including rented and deserialized messages.

Under `AutoCommitMode.AfterReceive` a message counts as delivered once its loop body completes. A message whose body threw, broke or was still running when the consumer was disposed is not checkpointed and is delivered again.

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
