<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-darkbg.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg">
    <img alt="Apache Iggy" src="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg" width="360">
  </picture>
</div>

# Apache Iggy Rust SDK

<div align="center">

[Website](https://iggy.apache.org) | [Getting started](https://iggy.apache.org/docs/introduction/getting-started/) | [Documentation](https://iggy.apache.org/docs/) | [Examples](https://github.com/apache/iggy/tree/master/examples/rust) | [Discord](https://discord.gg/apache-iggy)

</div>

<p align="center">
  <a href="https://crates.io/crates/iggy"><img alt="Crate" src="https://img.shields.io/crates/v/iggy?logo=rust&style=flat-square"></a>
  <a href="https://iggy.apache.org/docs/sdk/rust/intro/"><img alt="Docs" src="https://img.shields.io/badge/docs-iggy.apache.org-blue?style=flat-square"></a>
  <a href="https://crates.io/crates/iggy"><img alt="Downloads" src="https://img.shields.io/crates/d/iggy?style=flat-square"></a>
  <a href="https://github.com/apache/iggy/blob/master/LICENSE"><img alt="License: Apache 2.0" src="https://img.shields.io/badge/license-Apache%202.0-blue.svg?style=flat-square"></a>
</p>

Official Rust client SDK for [Apache Iggy](https://iggy.apache.org), the persistent message streaming platform written in Rust. The SDK ships a low-level transport client (QUIC, TCP, HTTP, WebSocket) for direct command access and a high-level producer/consumer API with batching, consumer groups, and commit policies.

## Features

- **Transports**: TCP (custom binary), QUIC, HTTP, WebSocket. One unified `IggyClient` API across all four.
- **TLS**: TCP and WebSocket expose TLS connection-string options; QUIC always uses TLS; HTTP uses an HTTPS URL configured through the builder.
- **Connection strings**: `iggy://` (TCP default), `iggy+tcp://`, `iggy+quic://`, `iggy+http://`, `iggy+ws://`. Binary transports apply credentials on `connect()`; HTTP requires an explicit login. Option keys and reconnection support differ by transport.
- **Authentication**: username/password and Personal Access Tokens (PAT).
- **Async, non-blocking** client built on Tokio with custom zero-copy (de)serialization.
- **High-level builders** on `IggyClient`: `producer(stream, topic)`, `consumer(name, stream, topic)`, `consumer_group(name, stream, topic)`, and `reader(stream, topic, consumer)`.
- **Producer modes**: `direct` (awaited send) and `background` (buffered with parallel shard workers using `OrderedSharding` or `BalancedSharding`). Configurable batch length / size and linger time.
- **Partitioning**: `balanced`, `messages_key`, or explicit `partition_id`. Custom `Partitioner` is pluggable.
- **Consumer**: standalone or consumer-group over binary transports; HTTP supports standalone consumers only. Consumed as an async `Stream`. Polling strategies: `next`, `offset`, `timestamp`, `first`, `last`.
- **Reader**: `PartitionReader` returns batches of many partitions, with routing, prefetch and the position of each partition inside.
- **Commits**: explicit commits of a `Checkpoint`, or a `ClientCommitPolicy` on an interval or a `CommitEvent`. With `ServerCommitPolicy::OnRead`, the server stores the offset of each read. See [reading messages](#reading-messages).
- **Stream builder** (`IggyStream`, `IggyStreamProducer`, `IggyStreamConsumer`) for declarative producer + consumer setup on shared or separate stream/topic.
- **Reliability**: managed replica connections, heartbeat, ordered mutation lanes, explicit recovery of uncertain mutations and lost committing reads.
- **Message features**: optional headers (`HeaderKey` / `HeaderValue`), client-side AES-256-GCM encryption (via `Aes256GcmEncryptor`), topic compression metadata (`None` and `Gzip`; no runtime compression yet), server-honored message expiry, and server-side deduplication.
- **Admin**: stream/topic/partition CRUD, consumer-group management, server-side consumer offsets, system stats.
- **Resource options**: typed create and update options for streams, topics and users (`StreamCreateOptions`, `TopicUpdateOptions`, `UserUpdateOptions` and friends). An update sends only the settings it names and resets others by key. A stream can carry its own at-rest key (`StreamCreateOptions::encryption_key`), reads report only its source and fingerprints.

## Installation

Run from your application crate. Use a release compatible with your server; for
unreleased changes, build the SDK and server from the same source checkout.

This checkout requires matching servers and SDKs with fresh storage. Older data
directories are refused without modification; `--fresh` deletes existing data
in the configured directory. See the [server recovery guide](../server/README.md#upgrade-recovery).
Mixed versions and rolling upgrades are unsupported. Binary clients share one
logical session across replica links using BindSession (15) and discover
partition primaries with GetPartitionRoute (105). The minimum binary protocol
version is 0.12.1. Commands 14, 103, 104, 107, 123 and 124 are retired. HTTP clients
use server-side forwarding.

```bash
cargo add iggy
```

All four transports are included; this crate declares no optional Cargo features.

The durable session API adds required `session_identity` and `session_bind_secret`
methods to `VsrSessionControl`. External transport implementations must provide
them, retain their logical identity and bind proof across transport reconnects,
and clear them on explicit logout. `VsrSessionSealed` is an implementation marker,
not a restriction on external implementations. The bind proof is a bearer
credential for internal session control and must never be logged or exposed.
`refresh_session_password` and `refresh_session_username` are removed. A
transport that signs in again applies a committed credential change where its
calls complete.

`IggyError::ProducerSendFailed` changes its public `stream_name` and `topic_name`
fields from `String` to `Box<str>`. Existing constructors must convert their names
with `.into()`; the callback's `ErrorCtx` fields remain `String`. It also adds a
public `uncertain: usize` field, so struct literals and exhaustive patterns must
name it. `ConsumerGroupClientState` no longer has its public `partition_count` and
`set_partition_context` methods.

An established session has its own lifetime. Password or PAT changes and PAT
expiry do not end it. Logout, session lease expiry, or user deactivation end the
session and prevent its bind proof from restoring it.

## Mutation recovery

TCP, QUIC and WebSocket serialize mutations within each partition history and
within the metadata lane. Different lanes can progress concurrently. An
`IggyError::OperationUncertain(operation_id)` retains the exact request for
recovery. A later mutation on the same lane returns
`IggyError::LaneBlocked(operation_id)` until that operation is resolved or abandoned.

Call `client.resolve_operation(operation_id, deadline).await`, where `deadline`
is a `std::time::Instant`. A proved success returns `Ok(())`; a proved committed
business rejection returns its original error. Both release the lane. Another
`OperationUncertain` keeps the request available for a later recovery attempt.
The client keeps the proved outcomes of its last `max_pending_operations`
replays, also across a session reset. So a later resolve returns the same
outcome without a request, also after a producer or a consumer of the client
resolved the operation first.
`client.abandon_operation(operation_id).await` releases the lane while leaving
the outcome unknown. A session reset drops retained requests. An
`OperationNotRetained` error does not establish the outcome. A reset or an
abandon came before a proved outcome, or the proved outcome aged out.
Recovery is unavailable over HTTP.

Receipt access follows the server's [session and revocation policy](../server/README.md#sessions-and-retry-receipts).
An unresolved operation cannot recover its receipt after its original session
ends, even after a new login or account reactivation. With a live session,
partition permission revocation prevents resolution with 41 until a re-grant;
metadata can replay the user's own committed cached result without a new
permission check. A fresh application send can duplicate an uncertain operation.

Each node counts the messages in its stream, topic and system stats. After a
successful send, a node that is not the partition primary shows the new count
only after the next prepare or commit broadcast. When no other write follows,
this delay is at most `cluster.commit_broadcast_interval` (500 ms by default).
See [metadata freshness and counters](../server/README.md#metadata-freshness-and-counters).

An explicit sign-in or logout ends earlier calls: unsent calls fail with 40,
and possibly applied mutations remain uncertain. SDK-managed recovery can keep
an unsent call waiting for a session or a primary within its original deadline.
Expiry returns 58. A refused automatic sign-in ends each unsent call that waits
for it with 40. Until the caller signs in, later calls also fail with 40.
`IggyError` carries no cause, so the client logs the refusal as a warning. If
the sign-in of a `connect()` call is refused, that call returns the refusal.
Recovery uses the caller's last explicit sign-in, or configured automatic
credentials only if the caller never signed in. Logout disables automatic sign-in.

HTTP 503 `relay_refused` uses the configured HTTP retry policy, regardless of
its body code. Rust's HTTP retry schedule does not use the server's `Retry-After`
header. This retry support does not provide retained-operation recovery.
If the last 503 body carries code 58, the call returns `TransientNotAccepted`.
A write that possibly committed, for example after a 504 or a lost reply, is
not sent again and returns `TransientNotCommitted`.

On a multi-node cluster, an HTTP call often reaches a node that is not the
partition primary. For message sends, polls and consumer offset calls, that
node relays the call to the primary. The relay needs identical `http.jwt`
secrets or `cluster.auth` on every node. Without that key material, these calls
answer 503 with code 58. See
[HTTP routing and backpressure](../server/README.md#http-routing-and-backpressure).

A committed password change or rename of the signed-in user updates the
credentials that the client signs in with again. This holds for a typed call, a
raw request and a change that `resolve_operation` proves. A committed rejection,
such as a wrong current password, leaves them unchanged.

The client defaults to 256 active or retained mutations and 64 MiB of frozen
request frames. Configure positive limits through `PendingOperationsConfig`
on the binary transport builder, or through connection-string options:

```text
iggy://iggy:iggy@localhost:8090?max_pending_operations=256&max_pending_operation_bytes=67108864
```

Admission waits within the call deadline before allocating a request ID or
sending. A frame larger than the byte limit fails locally. Completion,
abandonment and session reset release operation capacity; bytes remain charged
while a writer still holds the frame. Replaying a retained request reuses its
reservation.

Reads with `ServerCommitPolicy::OnRead` use a second pool with the same limits.
Each such read holds one slot for the whole call, also while the server waits
for messages. After an unknown outcome, its block keeps the slot until
`abandon_operation`. When the pool is full, the read returns
`TransientNotAccepted` at its deadline.

If a `next` poll without `OnRead` loses its reply after the send, or gets error
57, it ends at once with `TransientNotCommitted`. It is not sent again.
Errors 58 and 89 still retry a request that the server did not admit.
Polls are not retained for recovery. After an unknown outcome, a read with
`OnRead` keeps a local progress block instead, as described under
[reading messages](#reading-messages).

## Quick start

Start a source server from the repository root in a separate terminal:

```bash
cargo run --bin iggy-server -- --fresh --with-default-root-credentials
```

Use disposable replica data with `--fresh`. Environment credentials override the
flag, and recovered credentials are not replaced. The sample expects `iggy`/`iggy`
and requires `iggy`, Tokio and `futures-util` in the application.

```rust
use std::error::Error;
use std::str::FromStr;
use futures_util::StreamExt;
use iggy::prelude::*;

const STREAM: &str = "telemetry";
const TOPIC: &str = "device-events";
const CONSUMER_GROUP: &str = "telemetry-ingester";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = IggyClient::from_connection_string(
        "iggy://iggy:iggy@localhost:8090",
    )?;
    client.connect().await?;

    let producer = client
        .producer(STREAM, TOPIC)?
        .direct(
            DirectConfig::builder()
                .batch_length(1000)
                .linger_time(IggyDuration::from_str("1ms")?)
                .build(),
        )
        .partitioning(Partitioning::balanced())
        .build();
    producer.init().await?;
    producer
        .send(vec![IggyMessage::from_str("Hello Apache Iggy")?])
        .await?;

    let mut consumer = client
        .consumer_group(CONSUMER_GROUP, STREAM, TOPIC)?
        .create_consumer_group_if_not_exists()
        .polling_strategy(PollingStrategy::next())
        .build();
    consumer.init().await?;

    while let Some(message) = consumer.next().await {
        match message {
            Ok(message) => {
                let payload = std::str::from_utf8(&message.message.payload)
                    .unwrap_or("<non-utf8>");
                println!(
                    "offset={} partition={} current_offset={} payload={payload}",
                    message.message.header.offset,
                    message.partition_id,
                    message.current_offset,
                );
                if let Some(headers) = message.message.user_headers_map()? {
                    for (key, value) in headers {
                        println!("  header {key} = {value:?}");
                    }
                }
                consumer.commit(message.checkpoint()).await?;
            }
            Err(error) => eprintln!("read error: {error}"),
        }
    }
    Ok(())
}
```

For lower-level control over individual commands (login, stream/topic management, raw send, polling by offset or timestamp), use the transport-specific clients directly. See the [examples](https://github.com/apache/iggy/tree/master/examples/rust) and the [Rust SDK docs](https://iggy.apache.org/docs/sdk/rust/intro/).

`IggyClient::send_binary_request_to_replica(code, payload, replica_id)` sends a raw command to one node of a cluster, named by the replica id that cluster metadata returns.

Each node answers non-replicated commands such as `GetStats` from its own state, so this reads the state of that node. The request carries the session's metadata watermark. Replicated commands, polls, fetches, deferred-request cancels, consumer-offset reads and session-control codes fail with `InvalidCommand` before anything is sent.

An id that cluster metadata does not name fails with `InvalidIdentifier`. An unreachable node is retried until the request deadline, and a `NotPrimary` reply is returned rather than sent to another node.

HTTP clients return `FeatureUnavailable` for this call, as for `send_binary_request`.

For `IggyConsumerConfig`, `partitions_count` controls topic creation only. An ordinary consumer from the config reads partition `0` unless `with_partitions` selects others, and a consumer group ignores that setting. Code that previously used `partitions_count` to select an existing partition must call `with_partitions`. `IggyConsumerBuilder` has no default partition: a standalone consumer must set `partitions`, and `init()` refuses them for a group.

## Reading messages

The SDK reads in three ways. Each one takes `PollOptions` and starts from a `PollingStrategy`:
`next`, `offset`, `timestamp`, `first` or `last`.

- `poll_messages(stream, topic, partition, consumer, strategy, options)` reads one batch of one
  partition. The partition is required.
- `PartitionReader` reads many partitions and returns batches. Build one with
  `client.reader(stream, topic, consumer)`. A standalone `Consumer` reads the partitions of
  `.partitions(...)`. A consumer group member reads its assignment: the reader joins the group,
  joins again after a session loss, and leaves the group in `close()`.
- `IggyConsumer` is a `Stream` of messages over one `PartitionReader`, with commit policies, group
  creation, init retries and a replay filter. A standalone consumer needs `.partitions(...)`.

The reader and the consumer route each read, prefetch, and keep the position of each partition. The
[rustdoc](https://docs.rs/iggy) of `MessageClient::poll_messages`, `PollOptions`, `PartitionReader`
and `IggyConsumer` gives the full contracts.

```rust
use iggy::prelude::*;
use tokio::sync::watch;

// Reads partition 0 until it is empty, and commits after each batch.
async fn read_one(client: &IggyClient, consumer: &Consumer) -> Result<(), IggyError> {
    let (stream, topic) = (Identifier::named("orders")?, Identifier::named("events")?);
    let mut strategy = PollingStrategy::next();
    loop {
        let batch = client
            .poll_messages(&stream, &topic, 0, consumer, &strategy, PollOptions::default())
            .await?;
        let Some(checkpoint) = batch.checkpoint() else {
            return Ok(());
        };
        // Process batch.messages here.
        client
            .store_consumer_position(consumer, &stream, &topic, &checkpoint)
            .await?;
        strategy = batch.continuation();
    }
}

// Reads partitions 0 to 7 of a standalone consumer until `shutdown` changes, and commits after
// each batch.
async fn read_many(
    client: &IggyClient,
    consumer: &Consumer,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), IggyError> {
    let (stream, topic) = (Identifier::named("orders")?, Identifier::named("events")?);
    let mut reader = client
        .reader(&stream, &topic, consumer)
        .partitions(0..8)
        .build()
        .await?;
    loop {
        // next() is cancel safe, so select! can drop it.
        let result = tokio::select! {
            result = reader.next() => result,
            _ = shutdown.changed() => break,
        };
        let batch = match result {
            Ok(batch) => batch,
            // A recreated partition stops until a seek picks a start in its new history.
            Err(ReadError {
                partition_id: Some(partition),
                error: IggyError::HistoryUnavailable,
                ..
            }) => {
                reader.seek(partition, PollingStrategy::first()).await?;
                continue;
            }
            // The reader retries by itself after the other errors.
            Err(error) => {
                eprintln!("{error}");
                continue;
            }
        };
        // Process batch.messages here.
        if let Some(checkpoint) = batch.checkpoint() {
            reader.commit(checkpoint).await?;
        }
    }
    reader.close().await
}
```

### Poll options

`PollOptions` sets the limits of one request:

- `count`: the most messages in one batch. Default 1000.
- `max_wait`: how long the server waits for `min_count` messages, at most 600 s. Default zero,
  which answers at once.
- `min_count`: from 1 to `count`. Default 1.
- `max_bytes`: the most bytes in one reply, with the response metadata and the batch framing.
  Default 1 MiB.
- `server_commit`: `ServerCommitPolicy::Disabled` (default) or `OnRead`.

`PollOptions::wait(max_wait)` keeps the other defaults. `PartitionReader` and `IggyConsumer` use
`PollOptions::wait` of one second unless `.options(...)` on the reader builder or
`.poll_options(...)` on the consumer builder sets others. `IggyConsumerConfig` defaults to 100
messages and a one-second wait, and `with_poll_options` changes them.

The server answers at the first of three events: `min_count` messages are ready, the reply reaches
`max_bytes`, or the wait ends. After the wait it makes one last bounded read, so a reply can hold
fewer than `min_count` messages, or none. A fetch of a reader checks `min_count` against all its
partitions together, so one batch can hold fewer. HTTP applies the same byte limit before JSON
encoding. The server also stops a reply at `sharding.deferred_poll_max_read_bytes`, unless its first
message alone is larger. A message larger than `max_bytes` comes alone in its own read. A `last`
read of more than one message is the exception: it returns `InvalidSizeBytes` (80) and stores
nothing, because a second read of one message would start at the last message.

### Positions and commits

A batch (`PolledMessages`) gives two positions:

- `continuation()` is the position after the batch, for the next read of its partition. An empty
  batch keeps the start of its request, so `next` stays `next`. A continuation belongs to its
  partition, so a read of another partition refuses it with `InvalidCommand`. It also carries the
  partition history: after another client deletes and recreates the partition, a read from it
  returns `HistoryUnavailable` (87).
- `checkpoint()` is a `Checkpoint`, the position after the last message of the batch.
  `checkpoint_at(index)` covers a processed part of the batch, and `ReceivedMessage::checkpoint()`
  covers one message. A commit stores its offset under the partition history that delivered it, and
  it covers every earlier message of the partition.

Only a commit or `ServerCommitPolicy::OnRead` moves the stored offset. The reader and the consumer
keep their own read position. They resolve a `next` start once per partition: after the stored
offset, or at offset 0 without one. Both default to `next` and store nothing, so a restart before a
commit reads the same messages again. There are three ways to commit:

- Explicitly, after processing and in partition order: `store_consumer_position(...)` after
  `poll_messages`, `reader.commit(checkpoint)` or `consumer.commit(received.checkpoint())`. This
  gives at-least-once delivery. The reader skips a checkpoint at or below its last commit. It also
  skips a checkpoint from before a `seek` or a new assignment of its partition.
- With a `ClientCommitPolicy` on `IggyConsumer` (`.client_commit(...)`). It stores the position of
  the last message that the consumer yielded: on an `interval`, `on` a `CommitEvent` (`EachMessage`,
  `EveryNthMessage(n)` for offsets that are a multiple of `n`, or `EachBatch`), or both. The stream
  counts a message at its yield, and `consume_messages()` counts it after its handler returns, also
  after a failure. So a policy can commit a message that the application did not process.
- With `ServerCommitPolicy::OnRead`: the server stores the offset of the last message of each
  reply, as part of the reply. This gives at-most-once delivery. The reader does not prefetch. When
  the application asks for a batch and none is queued, the reader sends new reads. `commit`
  returns `InvalidCommand`, and `init()` refuses a client policy. An encryptor is refused too,
  because the server stores the offset before the client decrypts the batch. `close()` and
  `shutdown()` move the offset back to the last delivered message.

A consumer that delivered messages but stored no offset logs a warning at shutdown. With the default
policy and `next`, `consume_messages()` also warns at its start, because its handler cannot commit.

Earlier releases committed by default: the builder used
`AutoCommit::IntervalOrWhen(1s, PollingMessages)`, and `IggyConsumerConfig` used
`AutoCommit::When(PollingMessages)` with `Last`. Code that relied on this must now set `OnRead` or a
client policy, or each restart reads the same messages again. On `IggyConsumerConfig`,
`with_poll_options` and `with_client_commit` set them. The old types map like this: `Interval(d)`
sets `interval`, `When(e)` and `After(e)` set `on`, `IntervalOrWhen` and `IntervalOrAfter` set both,
and `When(PollingMessages)` becomes `OnRead`.

### Errors and lost replies

`PartitionReader::next()` and the consumer stream return a `ReadError` with `partition_id` (`None`
for an error of the whole reader), `error` and `skip`. The reader stays usable after it:

- It follows a moved primary without an error. Connection loss and transient refusals come out as
  errors, and the reader retries by itself. An error of one partition does not drop the batches of
  the other partitions.
- `HistoryUnavailable` (87) stops its partition until `seek(partition, start)`, because an old
  offset names no position in the new history. The other partitions go on.
- A message that does not decode or decrypt comes out as an error after the batch of the messages
  before it. Its `skip` is the continuation after the message. The reader reads the message again
  until `seek(partition, skip)` moves past it.

A read without `OnRead` stores nothing, so a retry after a lost reply is safe. The binary transport
sends such a poll again, with less wait and time left, unless it starts from `next`. A one-shot poll
from `next` returns `TransientNotCommitted` (57) instead.

With `OnRead`, the server can still store the offset after the client gives up. So a read with
`OnRead` whose reply is lost, or reports an unknown outcome, returns `FetchProgressUncertain(op)`
(14010). A dropped call that already sent its request leaves the same state. The SDK never sends an
`OnRead` read again. The partition stays blocked on this client, also after a reconnect or a new
sign-in: later progress calls on it return `PartitionLaneBlocked(partition_id, op)` (14009). Offset
reads still work during this block, but they do not prove the outcome.

`client.abandon_operation(op)` frees the partition and accepts the unknown outcome. It does not stop
the old server request or prove what it stored. `resolve_operation(op)` cannot prove the outcome of
a read, and it returns `FetchProgressUncertain` without I/O. `PartitionLaneBlocked` can also name an
offset store or delete with an unknown outcome, and `resolve_operation` can prove that one (see
[mutation recovery](#mutation-recovery)).

`PartitionReader` and `IggyConsumer` stop a blocked partition, and the other partitions go on. Call
`abandon_operation(op)`, then `seek`. `close()` and `shutdown()` wait for each `OnRead` read in
flight until its deadline, because a timeout around a committing read loses its outcome. They return
`PartitionLaneBlocked` for a partition that is still blocked. Abandon its operation before a new
reader reads that partition.

### Timeouts, admission and prefetch

The request timeout of a read is `max_wait` plus 10 s. One deadline covers admission, route
refreshes, retries and server forwarding. If the deadline passes before the request leaves the
client, the call returns `TransientNotAccepted` (58), and a retry is safe. A proxy between the
client and the server must allow more than `max_wait` plus 10 s for one request.

A client admits at most 64 waiting reads, and each reader at most 16 of them. A read that waits for
admission spends its own deadline. On TCP, QUIC and WebSocket, reads share the multiplexed
connections with other traffic. There, a read with `OnRead` also holds a slot of a pending pool (see
[mutation recovery](#mutation-recovery)). A dropped binary read sends command 108, which stops a
request that still waits on the server. The server turns a cancelled QUIC stream into the same
command.

The reader keeps reads in flight and batches queued within a budget of encoded bytes and messages:
`prefetch_bytes` (default 16 MiB) and `prefetch_messages` (default: the larger of 16,000 and
`count`). Each request reserves `count` messages and `max_bytes` bytes until `next()` hands its
batch over, and one request always fits. A full budget stops new requests. A batch that the
application holds is outside the budget, but `IggyConsumer` keeps the batch that it yields from
until its last message. The budget counts encoded bytes, not process memory: decoded metadata and
HTTP JSON overhead come on top. Routing, reconnection and cancellation go on while the application
stalls.

### Routing and lanes

On TCP, QUIC and WebSocket, the reader groups its partitions by primary and sends one fetch (command
109) for each group. After a rebalance, a partition that stays with the member continues after its
last delivered message. A newly assigned partition starts at the start of the builder: `.start(...)`
on the reader, `.polling_strategy(...)` on the consumer. In a new session, a group member starts
every partition there again. `last` and `timestamp` starts read with single polls until the first
message.

On these transports, progress calls of one partition share a progress lane: polls and fetches from
`next` or with `OnRead`, and offset reads, stores and deletes. Sends use a separate mutation lane,
so a waiting poll does not delay a producer of the same client. Offset stores and deletes hold both
lanes, so an unresolved store or delete blocks later progress until it is resolved or abandoned. All
consumers of one partition on one client share its progress lane.

A waiting progress poll holds the lane of its partition until it ends. Offset reads, stores and
deletes of the same client on that partition wait for it, each for at most the 30-second operation
timeout. Then the call fails with `TransientNotAccepted`. The client did not send it, so a retry is
safe.

### HTTP

HTTP has no fetch. A standalone reader on HTTP keeps one waiting poll for each partition, within its
budget. HTTP cannot join a consumer group, so a group reader or consumer, and a poll as a group,
return `FeatureUnavailable`. `resolve_operation` is unavailable too. HTTP keys the block of a lost
`OnRead` read by the partition id alone. So the block stops progress calls on that partition id in
every stream and topic of the client. HTTP does not order the progress calls of one partition, so a
call that started before a block can overlap the lost read.

HTTP polls use `GET /streams/{stream}/topics/{topic}/messages/deferred` with the poll fields plus
`wait_us`, `min_count`, `max_bytes` and `request_timeout_us`. A field that the query leaves out
takes the server default: a 1 s wait, a `min_count` of 1, 1 MiB and an 11 s timeout. Binary command
106 carries the same contract. The fetch (109) has no HTTP route, and it replaces the retired
`wait_for_messages` API and command 107. Upgrade servers before SDKs.

### Server limits

The server defaults to a 30-second maximum wait, and to 1024 pending polls per shard and 64 per
logical session (or HTTP user). A read reservation is at most 16 MiB, and the reservations in flight
of one shard at most 64 MiB. A longer requested wait is shortened to the server maximum. A disk read
stops after the messages that fill `max_bytes`, capped at the read reservation. If its buffers fill
their share of that reservation first, it stops there and returns what it read, even below
`min_count`. Configure `sharding.deferred_poll_*` accordingly. The reply limit applies to the
selected result, not to all storage work.

A full server refuses a poll with `DeferredPollCapacityExceeded` (90), and the client backs off on
the same route. Over HTTP, a partition primary without a free relay slot refuses a relayed poll with
the same code: 429 if the credential of the caller holds its whole share of slots, else 503. If a
read is still refused at its deadline, the reader returns the error. It reads that partition again
after its retry interval (`polling_retry_interval` on the consumer). A replica that cannot serve a
partition returns `TransientNotAccepted` (58) or `NotPrimary` (89), and the client refreshes the
route. Upgrade clients to support code 90 before you enable deferred polling against this server.
Metrics expose pending polls, read reservations and completion outcomes.

### Benchmarks

iggy-bench sets the options of both consumer APIs with `--max-wait` (default 1 s), `--min-count`
(default 1) and `--max-bytes`. The default of `--max-bytes` is the larger of 1 MiB and one full
batch. Report names record them with the derived request timeout. `--request-timeout` is gone,
because the timeout is always `max_wait` plus 10 s. Intentional batching waits count in the measured
latency.

## Metadata changes

`get_metadata_changes` reads the node's change log: one record per public result of a committed metadata operation, such as a stream created, a topic renamed, a user's permissions changed or a session closed. A mirror keeps a cursor, asks for the records after it, and stores the returned `next` cursor. The call needs root or the global `manage_servers` permission.

```rust
use iggy::prelude::*;
use std::time::Duration;

async fn follow(client: &IggyClient, groups: Vec<MetadataChangeGroup>) -> Result<(), IggyError> {
    let mut query = MetadataChangesQuery {
        groups,
        ..MetadataChangesQuery::default()
    };
    loop {
        let page = client.get_metadata_changes(&query).await?;
        match page.status {
            MetadataChangeStatus::Ok => {
                for record in &page.records {
                    println!("op {} by {:?}: {:?}", record.op, record.actor_user_id, record.change);
                }
                if let Some(next) = page.next {
                    query.start = MetadataChangeStart::After(next);
                }
                if !page.has_more {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
            MetadataChangeStatus::Behind => tokio::time::sleep(Duration::from_millis(100)).await,
            MetadataChangeStatus::Expired | MetadataChangeStatus::Reset => {
                query.start = rebuild(client, &query.groups).await?;
            }
        }
    }
}

/// Pages the current state, then returns the position to follow from.
async fn rebuild(
    client: &IggyClient,
    groups: &[MetadataChangeGroup],
) -> Result<MetadataChangeStart, IggyError> {
    let mut scan_groups = groups.to_vec();
    scan_groups.extend(groups.iter().flat_map(|group| group.parents()));
    let mut query = MetadataChangesQuery {
        start: MetadataChangeStart::Current(None),
        groups: scan_groups,
        ..MetadataChangesQuery::default()
    };
    loop {
        let page = client.get_metadata_changes(&query).await?;
        match page.status {
            MetadataChangeStatus::Ok => {}
            MetadataChangeStatus::Behind => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            MetadataChangeStatus::Expired | MetadataChangeStatus::Reset => {
                query.start = MetadataChangeStart::Current(None);
                continue;
            }
        }
        for item in &page.items {
            println!("{item:?}");
        }
        match (page.current_page_token, page.next) {
            (Some(token), _) => query.start = MetadataChangeStart::Current(Some(token)),
            (None, Some(anchor)) => return Ok(MetadataChangeStart::After(anchor)),
            (None, None) => query.start = MetadataChangeStart::Current(None),
        }
    }
}
```

`Expired` means the log no longer holds the history after the cursor, and `Reset` means the cursor belongs to another history, for example after the data directory was rebuilt. Either way the mirror rebuilds from current state pages and then follows the changes after the anchor they return.

Current pages need the parent groups of every selected group, for example `topics` with `streams`, so `rebuild` adds them through `MetadataChangeGroup::parents()`. Keep a rebuilt state private until a change page answers `Ok` with `has_more == false` and a `head_op` at or above the last current page's.

## Versioning

Stable releases follow semver (`x.y.z`). Edge releases (`x.y.z-edge.N`) are cut from `master` between stable versions and may include unreleased fixes; pin to a stable version for production.

## Resources

- [Rust SDK docs](https://iggy.apache.org/docs/sdk/rust/intro/)
- [High-level SDK guide](https://iggy.apache.org/docs/sdk/rust/high-level-sdk/)
- [Stream builder guide](https://iggy.apache.org/docs/sdk/rust/stream-builder/)
- [Project documentation](https://iggy.apache.org/docs/)
- [Runnable examples](https://github.com/apache/iggy/tree/master/examples/rust): getting-started, basic, new-sdk, stream-builder, multi-tenant, message-envelope, message-headers, tcp-tls, sink-data-producer.
- [Benchmarking platform](https://benchmarks.iggy.apache.org)
- [GitHub repository](https://github.com/apache/iggy)
- [Discord community](https://discord.gg/apache-iggy)

## Related crates

- [`iggy_common`](https://crates.io/crates/iggy_common): shared types and traits.
- [`iggy_binary_protocol`](https://crates.io/crates/iggy_binary_protocol): wire protocol codec.
- [`iggy-cli`](https://crates.io/crates/iggy-cli): command-line tool, `cargo install iggy-cli`.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](https://github.com/apache/iggy/blob/master/LICENSE).
