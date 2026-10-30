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
stores consumer offsets, produces with a batching producer, and consumes with a
consumer that tracks and commits its position. Examples and BDD scenarios follow in
later changes.

Swift supports the consensus wire protocol and checks commitment proof during
automatic retries. It does not yet expose retained-operation resolve/abandon APIs,
mutation lanes, or pending-operation limits. Those recovery features in the Rust
and other native SDKs are outside the current Swift implementation.

## Requirements

- Swift 6.0 or later (Swift 6 language mode, strict concurrency)
- macOS 13, iOS 16, tvOS 16, watchOS 9, visionOS 1, or Linux with a Swift 6 toolchain
- An Iggy server that speaks the consensus protocol (`iggy-server` from this repository)

The SDK depends on [SwiftNIO](https://github.com/apple/swift-nio),
[swift-nio-ssl](https://github.com/apple/swift-nio-ssl),
[swift-log](https://github.com/apple/swift-log), and
[swift-crypto](https://github.com/apple/swift-crypto) for the SHA-256 behind stream key fingerprints.

## Installation

Swift Package Manager fetches a package from the root of a git repository, so the SDK
cannot be depended on straight from this monorepo's subdirectory. Add it through the
package mirror, which carries every release as a `v<version>` tag:

```swift
// Package.swift
dependencies: [
    .package(url: "https://github.com/apache/iggy-swift.git", from: "0.1.1")
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
serialized on the connection. A deferred poll or fetch is the
exception. The server can hold it for its whole wait, so it goes over a connection of its
own, bound to the same session. `disconnect()` closes the connection until the next `connect()`,
`shutdown()` is final.

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

### Resource options

Streams, topics, and users carry options. A create call takes a typed options value, and an update names only the keys it changes, plus the keys to return to their defaults. Every other key keeps its stored value:

```swift
let key = try EncryptionKey.decode(base64: base64Key)
let stream = try await client.createStream(name: "payments", options: StreamCreateOptions(encryptionKey: key))
let topic = try await client.createTopic(
    streamID: "payments", name: "orders",
    options: TopicCreateOptions(partitionsCount: 3, messageExpiry: .after(.seconds(604_800)), partitionResizePolicy: .fixed))
try await client.updateStream("payments", options: StreamUpdateOptions(name: "payments-v2"))
try await client.updateTopic(
    streamID: "payments-v2", topicID: "orders", options: TopicUpdateOptions(messageExpiry: .after(.seconds(86_400)), reset: [.maxTopicSize]))
try await client.updateUser("alice", options: UserUpdateOptions(status: .inactive))
```

Only `message_expiry` and `max_topic_size` can be reset. Compression and the other topic settings are fixed at create, and no stream or user key is resettable yet. A typed update that breaks these rules fails before anything is sent, with the error the server would return.

Everything else, such as a taken name or a missing resource, is refused by the server. `describeOptions(scope:)` lists every key of a scope with flags saying what an update may do with it. Renaming the signed-in user, through `username` or a raw `username` entry, moves the credentials later reconnects use.

Keys this SDK has no typed member for go in `raw`, values as strings the server parses, and reset names in `rawReset`. The server alone decides on a patch that carries either, since it may know keys this SDK does not:

```swift
try await client.updateTopic(streamID: "payments-v2", topicID: "orders", options: TopicUpdateOptions(rawReset: ["future_retention"]))
```

`StreamCreateOptions.encryptionKey` gives the stream its own data key. It must be 32 random bytes, never derived from a password. It travels inside the create request over whatever transport the client uses, so protecting it in transit with TLS is the caller's choice.

The server stores it wrapped and never returns it: reads report `encryptionKeySource`, `encryptionKeyFingerprint`, and `encryptionKekFingerprints`, and `EncryptionKey.fingerprint(_:)` computes the fingerprint of a key to compare. Without a key, a server with encryption enabled generates one for the stream.

### Metadata changes

`getMetadataChanges(_:)` reads the node's change log: one record per public result of a committed metadata operation, such as a stream created, a topic renamed, a user's permissions changed, or a session closed. A mirror keeps a cursor, asks for the records after it, and stores the returned `next` cursor. The call needs root or the global `manageServers` permission:

```swift
func follow(_ client: IggyClient, groups: [MetadataChangeGroup]) async throws {
    var query = MetadataChangesQuery(start: .latest, groups: groups)
    while true {
        let page = try await client.getMetadataChanges(query)
        switch page.status {
        case .ok:
            for record in page.records {
                print("op \(record.op) by \(String(describing: record.actorUserID)): \(record.change)")
            }
            if let next = page.next {
                query.start = .after(next)
            }
            if !page.hasMore {
                try await Task.sleep(for: .seconds(1))
            }
        case .behind:
            try await Task.sleep(for: .milliseconds(100))
        case .expired, .reset:
            query.start = try await rebuild(client, groups: groups)
        }
    }
}

/// Pages the current state, then returns the position to follow from.
func rebuild(_ client: IggyClient, groups: [MetadataChangeGroup]) async throws -> MetadataChangeStart {
    var query = MetadataChangesQuery(start: .current(nil), groups: groups)
    while true {
        let page = try await client.getMetadataChanges(query)
        switch page.status {
        case .ok:
            break
        case .behind:
            try await Task.sleep(for: .milliseconds(100))
            continue
        case .expired, .reset:
            query.start = .current(nil)
            continue
        }
        for item in page.items {
            print(item)
        }
        if let token = page.currentPageToken {
            query.start = .current(token)
        } else if let anchor = page.next {
            return .after(anchor)
        } else {
            query.start = .current(nil)
        }
    }
}
```

`expired` means the log no longer holds the history after the cursor, and `reset` means the cursor belongs to another history, for example after the data directory was rebuilt. Either way the mirror rebuilds from current state pages and then follows the changes after the anchor they return. A cursor persists as its `description`, `mc1_` and 59 characters of unpadded Base64url, and `MetadataChangeCursor(text:)` reads it back.

Current pages need the parent groups of every selected group, for example `topics` with `streams`. Keep a rebuilt state private until a change page answers `ok` with `hasMore` false and a `headOp` at or above the last current page's. An invalid query fails with `invalidCommand` before anything is sent. A record of a kind this build does not know arrives as `.unknown(group:kind:body:)` with its codes and raw body, and records carry option values as the read routes return them, never key material.

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

`sendMessages` writes a batch to one partition. The server routes explicit partitions only, so `balanced` (the default) and `messagesKey` are resolved on the client: balanced round-robins over the topic's partitions, and a key is hashed with XXH32 the way every other SDK hashes it. A message without an id gets a random one, written back into the batch, so sending the same batch again after `transientNotCommitted` carries the same ids.

`pollMessages` reads one partition with one request, starting where the strategy says, within the limits of `PollOptions`:

```swift
var batch: [IggyMessage] = ["placed"]
try await client.sendMessages(streamID: "orders", topicID: "created", partitioning: .messagesKey("customer-42"), messages: &batch)
let polled = try await client.pollMessages(
    streamID: "orders", topicID: "created", partitionID: 0, consumer: .consumer(1), strategy: .next,
    options: PollOptions(count: 100, maxWait: .seconds(5), commit: .onRead))
for message in polled.messages {
    print(message.offset, message.payloadString)
}
let more = try await client.pollMessages(
    streamID: "orders", topicID: "created", partitionID: 0, consumer: .consumer(1), strategy: polled.continuation)
```

Every poll is deferred (binary command 106). The server answers once `minCount` messages are readable, the next message would pass `maxBytes`, or `maxWait` ran out, and then returns what the partition holds, which can be nothing. `PollOptions()` reads up to 1000 messages and 1 MiB with no wait and `minCount` 1.

The call takes at most its request timeout, `maxWait` plus ten seconds: the route lookup, a reconnect, the wait, the reply and every retry. A first message larger than `maxBytes` comes alone in its own reply. The client refuses options out of range before it sends anything: a `minCount` outside `1...count` with `invalidMessagesCount`, a wait above 600 seconds with `invalidCommand`, a `maxBytes` below 344 with `invalidSizeBytes`.

`commit` is a `CommitPolicy`. `.manual`, the default, stores nothing. `.onRead` makes the server store the offset of the last message of each read as part of the read, in one round trip. While the primary stays up a message is delivered at most once. A failover before the store replicates can deliver the last batch again.

`.auto(_)` stores counted positions on the client and needs a `PartitionReader` or an `IggyConsumer`: a single poll refuses it with `invalidConfiguration`, as it has nothing to count.

Positions are opaque. `polled.continuation` is where the next poll of the same partition starts, and a poll of another partition refuses it with `invalidCommand`. `polled.checkpoint` and `polled.checkpoint(at:)` are the positions after a message, which `storeConsumerPosition(consumer:streamID:topicID:checkpoint:)` stores.

Both carry the partition context the read was served under: the partition's incarnation, which changes when a deleted partition id is reused. The server refuses either with `historyUnavailable` once the partition was recreated, and a consumer-group checkpoint with `consumerGroupPartitionNotOwned` once the partition moved to another member.

To cancel a poll, cancel its task. The client then sends command 108 on the connection of the request. A poll that commits nothing gets no reply, and a reply that still arrives is dropped.

A poll with `.onRead` gets exactly one reply, which the cancelled caller waits for: `CancellationError` when the cancellation won and the server stored nothing, or the messages the server stored before the cancellation arrived. A poll with `.onRead` that lost its reply, to a broken connection or to `disconnect()`, or whose reply arrived after the caller signed in or out, may have stored its offset.

It fails with `fetchProgressUncertain` (14010), whose `operationID` names it, and is not sent again. Every later poll with `.onRead` or `.next` and every offset store or delete of that partition on the same client then fails with `partitionLaneBlocked` (14009) naming the same operation, also after a reconnect and a new session, until `abandonOperation(_:)` takes that operation.

Offset reads still work, so the caller can look at what the server stored before it abandons.

A client keeps at most 64 deferred requests in flight. A request waits for a free slot within its request timeout, and fails with `transientNotAccepted` when none frees up in time. If the server refuses to admit a request with `deferredPollCapacityExceeded`, the client pauses and sends it again within the same timeout.

### Partition reader

`PartitionReader` reads the partitions of one topic in batches. A standalone consumer reads the partitions it names, and a consumer group member reads the partitions the server assigns to it: the reader joins the group, creating it when it is missing and named by name, joins again after a lost membership, and leaves it in `close()`.

```swift
let reader = try await client.reader(
    streamID: "orders", topicID: "created", consumer: .group("billing"),
    configuration: ReaderConfiguration(options: PollOptions(count: 100, maxWait: .seconds(5), commit: .auto(.default))))
for try await batch in reader {
    print(batch.partitionID, batch.messages.count)
}
```

The reader groups its partitions by their primary and reads each group with one request, which the server holds until a message arrives or the wait runs out. A moved primary, a rebalance and a lost connection are repaired inside, so a caller never groups partitions or handles `notPrimary`.

The partitions that a read served go last in the next read of their group, so a backlog does not starve the other partitions. The partitions whose last read ended before their head are spread over the free requests of the reader, each share in a fetch of its own with the full `count` and `maxBytes`, while the quiet partitions of a group share one fetch that may wait.

`next()` returns a batch of one partition, never empty, in offset order within one generation of the partition: from its first read, from each `seek(partitionID:to:)`, and from each new assignment. The default wait of a reader is five seconds. With a zero `maxWait`, a partition that a read found empty waits `retryInterval` before it is read again.

- `.manual`: `commit(_:)` stores a checkpoint that the reader delivered.
- `.onRead`: the server stores each read, and the reader reads only when `next()` waits with nothing queued, so no batch is stored before it was asked for. It then fills its free requests: the quiet partitions of a group share one fetch that may wait, and the backlogged ones take a committing fetch each beside it, with the full `count` and `maxBytes`. A partition that leaves the assignment still hands over the reads that the server stored for it.

  `close()` moves the offset of each partition back to the last batch delivered. It cancels the committing reads in flight first, and each waits for its one reply, so a parked read ends the close in about a round trip.

  A read that lost its reply, or a section whose error leaves the outcome unknown, may have stored messages that never arrived. The reader abandons that read and reads its partitions again after `retryInterval`: the next reply then starts past those messages, and the reader reads that gap again without a commit and delivers it first.

  A forward jump without such a lost read is a store of the application or of another session, and the reader delivers from there. When the partition moved to another member before the gap was read, the reader reports `fetchProgressUncertain` with the lost range.

  A seek to an offset above zero stores the offset before it first, once the committing read of the partition in flight ended, so a restart resumes at the sought offset. A refused store fails the seek and moves nothing. A seek to offset zero or to another strategy stores nothing. A partition that a lost read of another caller on the same client blocks reports `partitionLaneBlocked` once and reads again after that operation was abandoned.

- `.auto(AutoCommit(interval:every:after:))`: the reader counts a batch when `next()` returns it (`.delivery`), when the next one is asked for or the reader closes (`.processing`), or when the application calls `mark(_:)` (`.marked`), and stores the latest counted position of each partition at the interval, on the event of `every` (`.message`, `.messages(n)` counted per partition, `.batch`), before a rebalance takes a partition away, and in `close()`.

  `AutoCommit.default` stores every 10,000 messages or 5 seconds after `.processing`. A store only moves forward, and a position of an older generation is dropped.

A failed read is thrown as a `ReadError` with the partition, the `IggyError` and, for a message that does not decode, its offset, its record and `skip`, the continuation after it. The reader reads its partitions again after `retryInterval`. A message that does not decode, and `historyUnavailable` for a position read in a history that is gone, stop the partition until a seek.

### Consumer groups

`joinConsumerGroup`, `leaveConsumerGroup`, `createConsumerGroup` and `getConsumerGroup` manage groups directly, but reading a group goes through a `PartitionReader` or an `IggyConsumer` that names `.group(id)`, which joins on its own. Membership belongs to the session. A reconnect resumes the session and keeps it.

When the server no longer holds the session, the client signs in under a new one and the reader joins again. In a cluster, reads and offset writes go to the partition's primary over a second connection bound to the same session, and the heartbeat syncs every joined group.

### Producer

`IggyProducer` appends to one topic. `initialize()` creates the stream and topic when
they are named and missing, then a send either writes from the calling task (`direct`,
the default) or queues the batch for a background worker (`background`):

```swift
let producer = try client.producer(streamID: "orders", topicID: "created")
try await producer.initialize()
let response = try await producer.send(["placed", "paid"])

let background = try client.producer(
    streamID: "orders", topicID: "created",
    configuration: ProducerConfiguration(mode: .background(BackgroundSendOptions(lingerTime: .milliseconds(5)))))
try await background.initialize()
try await background.send(["queued"])
await background.shutdown()
```

A direct send splits a batch into requests of at most `batchLength` messages, each
within the server's 64 MiB frame limit, and returns their confirmations; a failure,
cancellation included, throws `ProducerSendError` with the confirmations so far and the
unconfirmed tail. A background send returns once queued and reports write failures to
`errorHandler`; `shutdown()` writes what is still buffered, and the producer is closed
by then, so a resend from `errorHandler` during that final flush fails with
`producerClosed`.

The producer retries only a request the server refused to admit or that found no
connection, since a retry is a new request: anything that may have committed, such as
`transientNotCommitted` or `disconnected`, is left to the caller, and the server does
not deduplicate a resend. Resend a failed tail only when its `cause` proves the first
request never committed; `ProducerSendError` lists those causes. The Rust producer has
since handed these retries to its client's operation lanes, which the Swift client does
not have yet.

A send belongs to the session it started in. Once the caller signs in or out, what it
has not written yet, a background batch still queued, or a retry still waiting, fails
with `unauthenticated` instead of going under the new session, and a queued batch is
never merged with one from another session.

With the default ordered sharding, the sends to one stream and topic identifier are
written in order. A topic addressed by name and by numeric id counts as two, and a
failed request does not hold back the ones after it, so a resend from `errorHandler`
lands behind them.

### Consumer

`IggyConsumer` reads one topic through one `PartitionReader` and hands its messages over one at a time. A standalone consumer reads the partitions it names, and a member of a consumer group reads the partitions the server assigns, joining the group in `initialize()`:

```swift
let consumer = try client.consumerGroup(
    name: "billing", streamID: "orders", topicID: "created",
    configuration: ConsumerConfiguration(pollOptions: PollOptions(maxWait: .seconds(5), commit: .auto(.default))))
try await consumer.initialize()
for try await received in consumer {
    print(received.partitionID, received.message.offset, received.message.payloadString)
}
```

`pollOptions` sets the limits of each read and the `CommitPolicy`, `.manual` by default. Under `.manual`, `commit(received.checkpoint)` stores a message. Under `.auto`, a message counts when it is handed over (`.delivery`), once the next one is asked for or the consumer shuts down (`.processing`), or once it is marked with `mark(_:)` (`.marked`). `consumeMessages(_:)` counts a message under `.processing` after its handler returned.

A handler that throws leaves its message uncounted and ends `consumeMessages(_:)` with its error, so the message is read again by the next run. Under the other policies a handler that throws is logged and reading goes on.

`pollingStrategy` says where a partition without a position starts. A failed read is thrown as a `ReadError` and leaves the consumer usable: a caller of `next()` can call it again, and `consumeMessages(_:)` logs it and keeps reading until the client cannot come back on its own. Without `allowReplay` a message at or below the last one handed over from the same partition history is skipped, unless `seek(partitionID:to:)` moved the partition back.

`shutdown()` counts the last message under `.processing` and closes the reader within `offsetDrainTimeout`: it stores the counted positions under `.auto`, moves the offset of each partition back to the last message handed over under `.onRead`, and leaves the group.

Unlike the Rust consumer this one has no encryptor, and it does not resolve a group create or join whose reply was lost, since the Swift client has no operation lanes. For the same reason a store of a partition waits for a read in flight that reads or moves the stored offset of that partition, and no such read starts while the store waits.

### Raw requests

`sendRawRequest(code:payload:)` sends a command code with an encoded payload and returns the raw reply body. `sendRawRequest(code:payload:replica:)` sends it to one node of a cluster, named by the replica id that cluster metadata returns.

A raw deferred poll (106) takes the route and the limits of a typed poll. A raw fetch (109) goes to the primary of its first target and keeps the fences and the target limits of its payload. If its payload does not decode or its options are out of range, it fails with `invalidCommand`. A raw cancellation (108) fails with `invalidCommand`: cancel the task of the deferred request instead.

Each node answers non-replicated commands such as `GetStats` from its own state, so this reads the state of that node. Replicated commands, polls, deferred polls, fetches, cancellations, consumer-offset reads and session-control codes fail with `invalidCommand` before anything is sent. An id that cluster metadata does not name fails with `invalidIdentifier`. An unreachable node is retried until the request timeout, and `notPrimary` is returned rather than sent to another node.

### Errors

Every client operation throws `IggyError`, a value carrying the typed `code`
(`IggyErrorCode`, one case per server error code), the raw wire code, and optional
context. An error of a read carries the `partitionID` it names. A cancelled task gets `CancellationError`. A direct producer send is the
exception: a failure while sending, cancellation included, throws `ProducerSendError`,
carrying the `IggyError` as `cause`, the confirmations so far, and the unconfirmed
`failed` tail. A send refused before it starts, such as with `producerClosed` after
`shutdown()`, throws a plain `IggyError`:

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
- `fetchProgressUncertain` (14010): a poll or fetch with `.onRead` lost its reply, so the
  server may have stored its offset. Its partition is blocked with
  `partitionLaneBlocked` (14009) until `abandonOperation(_:)` takes the operation.
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
IGGY_PATH=/tmp/iggy-swift IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy \
IGGY_ENCRYPTION_ENABLED=true IGGY_ENCRYPTION_KEY=/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE= \
target/debug/iggy-server
```

The resource options scenarios need encryption enabled, so a stream created without a key gets one the server generates.

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

The resource options tests compare against `Tests/IggyTests/Fixtures/resource_options.json`, the fixture every SDK shares, generated by `core/common/examples/resource_options_fixtures.rs`. Never edit the copy. The metadata changes tests do the same with `Tests/IggyTests/Fixtures/metadata_changes.json`, generated by `core/common/examples/metadata_changes_fixtures.rs`.

The metadata changes scenario that churns the change log past its capacity runs only when `IGGY_METADATA_CHANGE_LOG_RESOURCE_CAPACITY` is set for both the server and the test run, for example to 64.

## Contributing

Format the sources with the toolchain's formatter before opening a pull request; CI runs it
in lint mode and builds with warnings as errors:

```bash
swift format format --in-place --recursive Sources Tests Package.swift
swift format lint --strict --recursive Sources Tests Package.swift
swift build -Xswiftc -warnings-as-errors
```
