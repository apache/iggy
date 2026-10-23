<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-darkbg.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg">
    <img alt="Apache Iggy" src="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg" width="320">
  </picture>
</div>

# Swift SDK for Iggy

Official Swift client SDK for [Apache Iggy](https://iggy.apache.org) message streaming.

The SDK is built up in stages. The client is a TCP connection with optional TLS that
speaks the consensus protocol, signs in, keeps the session alive with a heartbeat,
reconnects, and follows the cluster leader. It manages streams, topics, partitions,
segments, users, personal access tokens, and consumer groups, sends and polls messages,
and stores consumer offsets. The producer and consumer, examples, and BDD scenarios
follow in later changes.

Swift supports the consensus wire protocol and checks commitment proof during
automatic retries. It does not yet expose retained-operation resolve/abandon APIs,
mutation lanes, or pending-operation limits. Those recovery features in the Rust
and other native SDKs are outside the current Swift implementation.

## Requirements

- Swift 6.0 or later (Swift 6 language mode, strict concurrency)
- macOS 13, iOS 16, tvOS 16, watchOS 9, visionOS 1, or Linux with a Swift 6 toolchain
- An Iggy server that speaks the consensus protocol (`iggy-server` from this repository)

The SDK depends on [SwiftNIO](https://github.com/apple/swift-nio),
[swift-nio-ssl](https://github.com/apple/swift-nio-ssl), and
[swift-log](https://github.com/apple/swift-log).

## Installation

Swift Package Manager fetches a package from the root of a git repository, so the SDK
cannot be depended on straight from this monorepo's subdirectory. Add it through the
package mirror, which carries every release as a `v<version>` tag:

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
    .package(name: "apache-iggy", path: "../iggy/foreign/swift")
],
targets: [
    .target(name: "MyApp", dependencies: [.product(name: "Iggy", package: "apache-iggy")])
]
```

## Usage

Create a client, connect, and manage resources. With `autoLogin` set, `connect()` signs
in and every reconnect restores the session:

```swift
import Iggy

let client = IggyClient(address: "localhost:8090", autoLogin: .usernamePassword(username: "iggy", password: "iggy"))
try await client.connect()
let stream = try await client.createStream(name: "orders")
let topic = try await client.createTopic(streamID: "orders", name: "created", options: .init(partitionsCount: 3))
await client.shutdown()
```

One client owns one connection and is safe to share between tasks: requests are
serialized on the connection. `disconnect()` closes the connection until the next
`connect()`, `shutdown()` is final.

### Configuration

`ClientConfiguration` carries the address, TLS, the credentials to sign in with, the
reconnection policy, the heartbeat interval, and the connect and request timeouts:

```swift
var configuration = ClientConfiguration(address: "iggy.example:8090")
configuration.autoLogin = .personalAccessToken(token)
configuration.tls = TLSOptions(caFile: "/etc/iggy/ca.pem")
configuration.reconnection = ReconnectionOptions(maxRetries: 5, interval: .seconds(2))
let client = IggyClient(configuration: configuration)
```

The server certificate is verified against the system roots, or against `caFile` when
set, and against the host of the address unless `TLSOptions.domain` names another one.

### Reconnection

If a request hits a dropped connection, the client reconnects and signs in again within
the `requestTimeout` of the request. Only a request that provably never reached the log
goes again: a read, a ping, or a request that the server never admitted. If the server
refuses a request as a stale client, the request fails with `staleClient` (30). The
next call binds the same session again, and signs in only if the server no longer holds it.

If the connection of a sent replicated write drops, or an eviction ends its session, the
outcome of the write is unknown. Only a reconnect that binds the same session sends the
write again. Otherwise the write fails with `transientNotCommitted`, so that it is never
applied twice. If a request that was never sent still waits for the reconnect at its
deadline, it fails with `transientNotAccepted`.

If a timeout or an outage leaves the client disconnected, the next call or heartbeat
reconnects it. The reconnect needs credentials to sign in with. It signs in as the caller
last signed in. Before the caller signs in, it uses the configured `autoLogin`. After
`logout()`, the client does not sign in by itself. Only the next `login`, or the first
`connect()` after `disconnect()`, ends this. In a cluster, the client signs in, reads the
roster, and moves to the leader. If a node answers that it is not the primary, the client
moves to the next node.

A call made before a `login`, `logout`, or `disconnect` never runs under a session that
opened after it. This holds for every request of the call. A send with balanced or key
partitioning reads the topic first, and a consumer group poll syncs its assignment first.
If the caller signs in or out between two requests of a call, the next request is not
sent. If the outcome of the call is unknown, it fails with `transientNotCommitted`.
Otherwise it fails with `unauthenticated`.

If the server refuses the credentials of the automatic sign-in, a call that waits for it
fails with `unauthenticated`. The error has the refusal as its context. For example, the
server refuses a wrong password, or a personal access token that is invalid or expired.
Then the client stays signed out, and only a sign-in by the caller ends this.

An explicit `login` supersedes automatic sign-in and uses the credentials supplied by
the caller. Any failed explicit attempt, including invalid input or a timeout, disables
automatic sign-in until another explicit sign-in succeeds. A disconnect followed by a
connect does not lift that restriction. Older or queued sign-ins cannot restore credentials
after a newer attempt has failed. A public sign-in failure also discards any session
that Register already created and closes its connection, so reconnect cannot bind that
session. Best-effort roster discovery does not turn a successful sign-in into a failure.

### Messages

`sendMessages` writes a batch to one partition. The server routes explicit partitions
only, so `balanced` (the default) and `messagesKey` are resolved on the client:
balanced round-robins over the topic's partitions, and a key is hashed with XXH32 the
way every other SDK hashes it. A message without an id gets a random one, written back
into the batch, so sending the same batch again after `transientNotCommitted` carries
the same ids. `pollMessages` reads from a partition, starting where the strategy says:

```swift
var batch: [IggyMessage] = ["placed"]
try await client.sendMessages(streamID: "orders", topicID: "created", partitioning: .messagesKey("customer-42"), messages: &batch)
let polled = try await client.pollMessages(
    streamID: "orders", topicID: "created", partitionID: 0, consumer: .consumer(1), strategy: .next, count: 100, autoCommit: true)
for message in polled.messages {
    print(message.offset, message.payloadString)
}
```

Every partition request carries the partition context the server requires: the
partition's incarnation, which changes when a deleted partition id is reused. The
client reads it from the partition's route and caches it. A poll reply carries the
context it was served under; hand it back with `.offset(_:context:)` to continue the
same partition, or in a `ConsumerPosition` to store a checkpoint. The server refuses
either with `historyUnavailable` once the partition was recreated, and a consumer-group
checkpoint with `consumerGroupPartitionNotOwned` once the partition moved to another
member.

### Consumer groups

A member polls without a partition id and the client takes the next partition of its
assignment, round-robin, syncing the assignment from the server when it changes. The
heartbeat syncs every joined group, so a member picks up new partitions on its own:

```swift
try await client.joinConsumerGroup(streamID: "orders", topicID: "created", groupID: "billing")
let batch = try await client.pollMessages(
    streamID: "orders", topicID: "created", partitionID: nil, consumer: .group("billing"), strategy: .next, count: 100, autoCommit: true)
```

A member holding no partitions gets an empty poll whose partition id is
`PolledMessages.noAssignedPartition`, and a client that is not a member gets
`consumerGroupMemberNotFound`. Membership belongs to the session. A reconnect resumes
the session and keeps it, but when the server no longer holds the session the client
signs in under a new one and the caller must call `joinConsumerGroup` again: the next
group poll fails with `consumerGroupMemberNotFound` until it does. In a cluster, polls
and offset writes go to the partition's primary over a second connection bound to the
same session.

### Errors

Every operation throws `IggyError`, a value carrying the typed `code`
(`IggyErrorCode`, one case per server error code), the raw wire code, and optional
context. A cancelled task gets `CancellationError`:

```swift
func handle(_ error: IggyError) {
    switch error.code {
    case .streamNameNotFound:
        print("create the stream first")
    default:
        print("iggy failed: \(error)")
    }
}
```

Three codes tell what happened to a request that did not complete:

- `transientNotAccepted` (58): the server did not admit the request, so a retry is
  safe.
- `transientNotCommitted` (57): the outcome is unknown, and the server possibly applied
  the write. Read the state before you write it again.
- `unauthenticated` (40): the request needs a session. Sign in, then retry.

The Rust SDK and the other native SDKs keep a write with an unknown outcome in a
retained-operation registry and fail it with `operationUncertain` (14006). Swift has no
such registry, so it reports the same case as `transientNotCommitted` (57).

A lost reply followed by session end remains uncertain, including after account
reactivation or a new login. Swift has no resolver for that receipt; a fresh send
can duplicate the earlier write. See the server's
[session and revocation policy](../../core/server/README.md#sessions-and-retry-receipts).

Each node counts the messages in its stream, topic and system stats. After a
successful send, a node that is not the partition primary shows the new count
only after the next prepare or commit broadcast. When no other write follows,
this delay is at most `cluster.commit_broadcast_interval` (500 ms by default).
See [metadata freshness and counters](../../core/server/README.md#metadata-freshness-and-counters).

## Testing

```bash
cd foreign/swift
swift test
```

The client tests run against an in-process server that speaks the consensus framing
and is scripted per test, so they need no running `iggy-server`.

The end-to-end suite runs against a server at the address in `IGGY_TCP_ADDRESS` and
skips when that variable is unset. Start one from the repository root:

```bash
cargo build --bin iggy-server
IGGY_PATH=/tmp/iggy-swift IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy target/debug/iggy-server
```

Then, from `foreign/swift`:

```bash
IGGY_TCP_ADDRESS=127.0.0.1:8090 swift test --filter IggyE2ETests
```

Add `IGGY_TCP_TLS_ENABLED=true` to run it against a server started with
`IGGY_TCP_TLS_ENABLED=true` and the certificate pair in `core/certs`.

The hash, error-code, model, request, reply, and consensus header tests compare against
`Tests/IggyTests/Fixtures/golden.json`, a fixture dumped from the Rust protocol crates. Regenerate it after a protocol change, from
`foreign/swift`, and commit the result; CI fails when the checked-in copy is stale:

```bash
cargo run --manifest-path Tools/golden-vectors/Cargo.toml -- Tests/IggyTests/Fixtures/golden.json
```

## Contributing

Format the sources with the toolchain's formatter before opening a pull request; CI runs it
in lint mode and builds with warnings as errors:

```bash
swift format format --in-place --recursive Sources Tests Package.swift
swift format lint --strict --recursive Sources Tests Package.swift
swift build -Xswiftc -warnings-as-errors
```
