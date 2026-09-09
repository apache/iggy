<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-darkbg.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg">
    <img alt="Apache Iggy" src="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg" width="320">
  </picture>
</div>

# Swift SDK for Iggy

Official Swift client SDK for [Apache Iggy](https://iggy.apache.org) message streaming.

The client speaks the VSR wire protocol over TCP, with or without TLS, and is
written in Swift from the ground up: Swift Concurrency throughout, a
consumer that is an `AsyncSequence`, `Codable` models, structured teardown,
automatic reconnection with session restore, and a producer that batches in
the background with bounded memory. It runs on macOS, iOS, tvOS, watchOS,
visionOS, and Linux.

## Requirements

- Swift 6.0 or later (Swift 6 language mode, strict concurrency)
- macOS 13, iOS 16, tvOS 16, watchOS 9, visionOS 1, or Linux (Ubuntu 22.04
  and later, or any distribution with a Swift 6 toolchain)
- An Iggy server that speaks the VSR protocol (`iggy-server` from this
  repository)

The SDK depends on [SwiftNIO](https://github.com/apple/swift-nio),
[swift-nio-ssl](https://github.com/apple/swift-nio-ssl), and
[swift-log](https://github.com/apple/swift-log).

## Installation

Swift Package Manager fetches a package from the root of a git repository,
so the SDK cannot be depended on straight from this monorepo's subdirectory.
Add it through the package mirror instead:

```swift
// Package.swift
dependencies: [
    .package(url: "https://github.com/apache/iggy-swift.git", from: "0.1.0")
],
targets: [
    .target(name: "MyApp", dependencies: [.product(name: "Iggy", package: "iggy-swift")])
]
```

From a checkout of this repository, or a vendored copy, depend on it by path:

```swift
dependencies: [
    .package(name: "iggy", path: "../iggy/foreign/swift")
]
```

Releases are tagged `foreign/swift/v<version>` in this repository, and the
mirror carries the same versions as plain `v<version>` tags.

## Running a server

Build and start a VSR server from a checkout of this repository:

```bash
cargo build --bin iggy-server

IGGY_SYSTEM_PATH=/tmp/iggy-swift \
IGGY_TCP_ADDRESS=127.0.0.1:8090 \
IGGY_HTTP_ENABLED=false IGGY_QUIC_ENABLED=false IGGY_WEBSOCKET_ENABLED=false \
IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy \
target/debug/iggy-server
```

QUIC, WebSocket and HTTP are enabled by default on ports 8080, 8092 and 3000.
Disable the ones you do not need so they cannot race with another process.

## Quick start

```swift
import Iggy

let client = try IggyClient(connectionString: "iggy://iggy:iggy@127.0.0.1:8090")
try await client.connect()

let stream = try await client.createStream(name: "orders")
let topic = try await client.createTopic(streamID: "orders", name: "created", options: TopicCreateOptions(partitionsCount: 3))

try await client.sendMessages(
    streamID: "orders", topicID: "created", partitioning: .messagesKey("customer-42"),
    messages: [try IggyMessage("hello"), try IggyMessage(encoding: Order(id: 42, total: 9.99))])

let polled = try await client.pollMessages(
    streamID: "orders", topicID: "created", partitionID: 0, consumer: .default, strategy: .first, count: 10, autoCommit: false)
for message in polled.messages {
    print(message.offset, message.payloadString)
}

try await client.shutdown()
```

Every operation is `async` and throws ``IggyError``, a value with a typed
`code` (``IggyErrorCode``, one case per server error), the raw wire code, and
optional context.

## Client

`IggyClient` owns one connection and is safe to share between tasks: requests
are serialized on the connection, so one client feeds many producers and
consumers.

### Configuration

```swift
var configuration = ClientConfiguration(
    address: "127.0.0.1:8090",
    tls: TLSOptions(domain: "localhost", caFile: "core/certs/iggy_ca_cert.pem"),
    autoLogin: .usernamePassword(username: "iggy", password: "iggy"))
configuration.reconnection = ReconnectionOptions(enabled: true, maxRetries: nil, interval: .seconds(1), reestablishAfter: .seconds(5))
configuration.heartbeatInterval = .seconds(5)
configuration.requestTimeout = .seconds(30)
let client = IggyClient(configuration: configuration)
```

The same settings are accepted as a connection string, the format every Iggy
SDK shares:

```text
iggy://user:password@host:port?tls=true&tls_domain=localhost&tls_ca_file=/path/ca.pem
iggy+tcp://<personal_access_token>@host:port?reconnection_retries=unlimited&reconnection_interval=1s
```

Options: `tls`, `tls_domain`, `tls_ca_file`, `reconnection_retries`
(`unlimited` or a number), `reconnection_interval`, `reestablish_after`,
`heartbeat_interval` (durations such as `5s`, `500ms`, `1h 30m`), and
`nodelay`.

### Lifecycle

Construct, `connect()`, use, `shutdown()`. With `autoLogin` set, `connect()`
signs in and every reconnect restores the session. Without it, call
`login(username:password:)` or `login(personalAccessToken:)` after
connecting. `disconnect()` closes the connection but keeps the client usable.
A client that was shut down cannot reconnect.

The client emits lifecycle events on an `AsyncStream`:

```swift
for await event in await client.events {
    switch event {
    case .connected, .signedIn, .signedOut, .disconnected, .shutdown: print(event)
    }
}
```

### Reconnection and delivery semantics

When the connection drops, the client reconnects, signs in again with the
credentials it last succeeded with, and resumes. A request that was in
flight is replayed only when it provably never reached the log: reads,
pings, and requests the server refused before admission. A replicated
write whose outcome is unknown is not replayed, because a fresh connection
registers a new client identity and the server could not match the replay
against the original. The original error is returned instead, so a caller
that retries is choosing at-least-once delivery.

The client also follows the cluster: after signing in it reads the roster,
moves to the leader when it landed on a follower, and fails over to the
other nodes when the leader stops answering.

`sendMessages` returns the placements the server committed. A confirmation
reports an in-memory commit, not a flush to disk, and an empty confirmation
list is a valid success.

### TLS

```swift
let client = IggyClient(address: "localhost:8090", tls: TLSOptions(domain: "localhost", caFile: "core/certs/iggy_ca_cert.pem"))
```

`domain` is the name verified against the certificate and sent as SNI; it
defaults to the host of the address. `caFile` adds a PEM bundle to the trust
roots, for a private CA such as the test certificates in `core/certs`.
`validateCertificate: false` turns verification off, for development only.

### Operations

The client mirrors the Rust SDK's `Client` trait:

| Area | Methods |
| --- | --- |
| Streams | `getStream`, `getStreams`, `createStream`, `updateStream`, `deleteStream`, `purgeStream` |
| Topics | `getTopic`, `getTopics`, `createTopic`, `updateTopic`, `deleteTopic`, `purgeTopic`, `describeOptions` |
| Partitions | `createPartitions`, `deletePartitions`, `deleteSegments` |
| Messages | `sendMessages`, `pollMessages`, `flushUnsavedBuffer` |
| Consumer offsets | `storeConsumerOffset`, `getConsumerOffset`, `deleteConsumerOffset` |
| Consumer groups | `getConsumerGroup`, `getConsumerGroups`, `createConsumerGroup`, `deleteConsumerGroup`, `joinConsumerGroup`, `leaveConsumerGroup`, `syncConsumerGroup` |
| Users | `getUser`, `getUsers`, `createUser`, `deleteUser`, `updateUser`, `updatePermissions`, `changePassword` |
| Personal access tokens | `getPersonalAccessTokens`, `createPersonalAccessToken`, `deletePersonalAccessToken`, `login(personalAccessToken:)` |
| System | `ping`, `getStats`, `getMe`, `getClient`, `getClients`, `getClusterMetadata`, `snapshot` |
| Raw | `sendRawRequest(code:payload:)` for a command that has no typed method |

Partitioning is resolved on the client: `.balanced` round-robins the topic's
partitions, `.messagesKey` hashes the key with XXH32, and `.partition(n)`
targets one. A consumer-group poll with `partitionID: nil` picks one of the
member's assigned partitions, syncing the assignment from the coordinator
when it changes.

## Producer

`IggyProducer` appends to one topic of one stream and creates both when they
are missing:

```swift
let producer = client.producer(
    stream: "orders", topic: "created",
    configuration: ProducerConfiguration(partitioning: .balanced, topicPartitionsCount: 3, sendRetries: 3, sendRetryInterval: .seconds(1)))
try await producer.initialize()
let response = try await producer.send([try IggyMessage("hello"), try IggyMessage("world")])
for confirmation in response.confirmations {
    print("partition \(confirmation.partitionID) at offset \(confirmation.baseOffset)")
}
```

A `.direct` producer (the default) writes from the calling task, splits a
large batch into requests of `batchLength` messages, retries a failed write
or a disconnected client up to `sendRetries` times, and throws
`ProducerSendError` with the confirmations of the chunks that landed and the
unconfirmed tail.

A `.background` producer returns once the batch is queued and a worker
writes it later:

```swift
let options = BackgroundSendOptions(
    workerCount: 2, sharding: .ordered, batchSize: 1024 * 1024, batchLength: 1000, lingerTime: .milliseconds(1),
    maxBufferSize: 32 * 1024 * 1024, backpressure: .blockWithTimeout(.seconds(5)), maxInFlight: 1
) { failure in
    print("\(failure.messages.count) messages to \(failure.streamID)|\(failure.topicID) failed: \(failure.cause)")
}
let producer = client.producer(stream: "orders", topic: "created", configuration: ProducerConfiguration(mode: .background(options)))
try await producer.initialize()
try await producer.send(try IggyMessage("queued"))
await producer.shutdown()  // flushes what is still buffered
```

A worker flushes when it holds `batchSize` bytes or `batchLength` sends, or
once `lingerTime` passed since the first send entered its buffer.
`maxBufferSize` bounds the bytes queued or in flight across the producer and
`backpressure` says what a send does when that budget is full: wait, wait
with a timeout, or fail at once. Consecutive sends to one destination merge
into one request, and `.ordered` sharding keeps every stream and topic pair
on one worker so its order is preserved. Write failures reach the error
handler, since no caller waits for them.

## Consumer

`IggyConsumer` reads a topic and hands messages over one at a time as an
`AsyncSequence`. A standalone consumer reads one partition; a consumer-group
member reads the partitions the server assigns to it and shares the group's
offsets with the other members.

```swift
let consumer = try client.consumerGroup(
    name: "workers", stream: "orders", topic: "created",
    configuration: ConsumerConfiguration(
        pollingStrategy: .next, batchLength: 100, pollInterval: .milliseconds(100),
        autoCommit: .intervalOrWhen(.seconds(1), .consumingAllMessages)))
try await consumer.initialize()
for try await received in consumer {
    print(received.partitionID, received.message.offset, received.message.payloadString)
}
```

`pollingStrategy` says where reading a partition starts: `.first`, `.last`,
`.offset(n)`, `.timestamp(t)`, or `.next`, which continues after the offset
stored on the server. From then on every poll continues after the last
message handed over. Messages at or below an offset already handed over are
skipped unless `allowReplay` is set.

`autoCommit` decides when the reading position is stored on the server:

| Mode | Behaviour |
| --- | --- |
| `.disabled` | Nothing is stored unless `storeOffset(_:partitionID:)` is called. |
| `.interval(d)` | Every `d` the position of every partition read so far is stored. |
| `.when(.pollingMessages)` | The poll request itself commits the whole batch, before any message is handed over. |
| `.when(.consumingEachMessage)` | A commit is queued for every message handed over. |
| `.when(.consumingEveryNthMessage(n))` | A commit is queued for every message whose offset is a multiple of `n`. |
| `.when(.consumingAllMessages)` | A commit is queued once the last message of a batch is handed over. |
| `.intervalOrWhen(d, when)` | Both. This is the default, with one second and `.pollingMessages`. |
| `.after(...)`, `.intervalOrAfter(d, ...)` | Like `.when`, but the commit follows the handler passed to `consume(_:)`. |

`shutdown()` drains the commit tasks, stores the final reading positions,
leaves the consumer group, and ends the iteration with `nil`. A poll failure
is thrown from the iteration; the consumer stays usable and iterating again
continues where it was. When the connection drops, polling pauses until the
client signs in again, and a group member rejoins its group.

The consumer follows the Rust SDK closely: `initRetries` waits for a stream
or topic the producer creates dynamically, `pollingRetryInterval` paces the
retries while a poll is blocked, and `offsetDrainTimeout` bounds the wait for
in-flight commits at shutdown.

## Messages

```swift
let message = try IggyMessage("payload", id: MessageID(uuid: UUID()), userHeaders: ["trace-id": "abc", "attempt": .uint32(3), "urgent": true])
let encoded = try IggyMessage(encoding: order)       // JSON through Encodable
let order: Order = try received.message.decode()   // JSON through Decodable
received.message.payloadString
received.message[header: "trace-id"]?.stringValue
```

A message carries a 128-bit id (random when left zero), the offset and
timestamps the server assigned, the checksum, the payload, and typed user
headers.

## Logging

The SDK logs through [swift-log](https://github.com/apple/swift-log). Pass a
`Logger` to `IggyClient(configuration:logger:)` to route it; the default
label is `org.apache.iggy`.

## Testing

Unit tests need nothing running. They cover the wire protocol against golden
vectors generated from the Rust crates, the producer and consumer against an
in-memory backend, and the client's reconnection, replay, eviction, and
timeout paths against an in-process VSR server on loopback:

```bash
swift test
```

The end-to-end suite runs against a server at the address in
`IGGY_TCP_ADDRESS` and skips when that variable is unset:

```bash
IGGY_TCP_ADDRESS=127.0.0.1:8090 swift test --filter IggyE2ETests
```

Add `IGGY_TCP_TLS_ENABLED=true` to run it against a server started with
`IGGY_TCP_TLS_ENABLED=true` and the certificate pair in `core/certs`.

The golden vectors in `Tests/IggyTests/Fixtures/golden.json` come from the
Rust generator in `Tools/golden-vectors`; regenerate them after a protocol
change with `cargo run --manifest-path Tools/golden-vectors/Cargo.toml`.

## Examples and BDD

Runnable examples live in [`examples/swift`](../../examples/swift) and the
cross-SDK BDD scenarios in [`bdd/swift`](../../bdd/swift).

## Contributing

Format the sources with the toolchain's formatter before opening a pull
request; CI runs it in lint mode:

```bash
swift format format --in-place --recursive Sources Tests Package.swift
swift format lint --strict --recursive Sources Tests Package.swift
```

The SDK builds warning-free in Swift 6 language mode, which CI enforces with
`-warnings-as-errors`.
