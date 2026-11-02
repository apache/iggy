<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-darkbg.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg">
    <img alt="Apache Iggy" src="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg" width="320">
  </picture>
</div>

# Go SDK for Iggy

Official Go client SDK for [Apache Iggy](https://iggy.apache.org) message streaming.

The client speaks the VSR wire protocol over TCP, with or without TLS.
Concurrent requests share one managed connection per replica and one logical
session. VSR is the only protocol it supports.

## Installation

The current source requires Go 1.25 or newer. From your application module,
install a release compatible with your server:

```bash
go get github.com/apache/iggy/foreign/go
```

Unversioned `go get` selects the latest stable release. VSR support starts
with `v0.9.0`. For unreleased changes,
build both SDK and server from the same checkout; `examples/go/go.mod`
replaces this module with the local SDK source.

## Running a server

From the repository root, build and start a VSR server using a new,
disposable data directory. The root environment variables bootstrap a new
instance; they do not replace credentials recovered from disk or peers:

```bash
cargo build --bin iggy-server

IGGY_PATH=/tmp/iggy-go \
IGGY_TCP_ADDRESS=127.0.0.1:8090 \
IGGY_HTTP_ENABLED=false IGGY_QUIC_ENABLED=false IGGY_WEBSOCKET_ENABLED=false \
IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy \
IGGY_ENCRYPTION_ENABLED=true IGGY_ENCRYPTION_KEY="$(head -c 32 /dev/urandom | base64)" \
target/debug/iggy-server
```

QUIC, WebSocket and HTTP are enabled by default on ports 8080, 8092 and 3000.
Disable the ones you do not need so they cannot race with another process.
The encryption variables enable at-rest encryption with a server key. Leave
them out for a plaintext server. The stream option end-to-end tests need them.

## Resource options

Streams, topics and users carry options from one server catalog. `CreateStream`, `CreateTopic` and `CreateUser` take typed option structs, and `UpdateStream`, `UpdateTopic` and `UpdateUser` take sparse patches: a nil field keeps its stored value, `Reset` returns a key to its default. `DescribeOptions` lists the catalog the server serves. `Raw`, `Options` and `RawReset` carry keys newer than this build, and the server decides whether such a patch is valid.

```go
key := make([]byte, iggcon.StreamEncryptionKeyLength)
if _, err := rand.Read(key); err != nil {
    log.Fatal(err)
}
stream, err := cli.CreateStream(ctx, "orders", &iggcon.StreamOptions{EncryptionKey: key})
if err != nil {
    log.Fatal(err)
}
source, _ := stream.EncryptionKeySource()
fingerprint, _ := stream.EncryptionKeyFingerprint()
log.Printf("key source %s, fingerprint %s", source, fingerprint)

streamId, _ := iggcon.NewIdentifier(stream.Id)
partitions := uint32(3)
expiry := iggcon.Duration(time.Hour.Microseconds())
durability := iggcon.DurabilityPersisted
topic, err := cli.CreateTopic(ctx, streamId, "events", &iggcon.TopicOptions{
    PartitionsCount: &partitions,
    MessageExpiry:   &expiry,
    Durability:      &durability,
})
if err != nil {
    log.Fatal(err)
}

topicId, _ := iggcon.NewIdentifier(topic.Id)
size := uint64(10 << 30)
if err := cli.UpdateTopic(ctx, streamId, topicId, iggcon.TopicUpdate{
    MaxTopicSize: &size,
    Reset:        []iggcon.TopicOptionKey{iggcon.TopicOptionMessageExpiry},
}); err != nil {
    log.Fatal(err)
}
```

The sample is `Example_resourceOptions` in `client/example_resource_options_test.go`.

A stream encryption key is exactly 32 random bytes. Never derive it from a password: the fingerprint reads would then be an offline guessing oracle. The key travels inside the create request over the client's transport, so use TLS when the network between client and server is not trusted.

The server stores the key wrapped under its own key and never returns it. Reads report only its source and fingerprints. Without a key, a server with encryption enabled generates one for the stream, and a server without it keeps the stream plaintext and refuses a client key.

## Metadata changes

`GetMetadataChanges` reads the node's change log: one record per public result of a committed metadata operation, such as a stream created, a topic renamed, a user's permissions changed or a session closed. A mirror keeps a cursor, asks for the records after it, and stores the returned `Next` cursor. The call needs root or the global `manage_servers` permission.

```go
func followMetadataChanges(ctx context.Context, cli iggcon.Client, groups ...iggcon.MetadataChangeGroup) error {
    query := iggcon.NewMetadataChangesQuery(iggcon.EarliestMetadataChangeStart(), groups...)
    for {
        page, err := cli.GetMetadataChanges(ctx, query)
        if err != nil {
            return err
        }
        switch page.Status {
        case iggcon.MetadataChangeStatusOk:
            for _, record := range page.Records {
                log.Printf("op %d %s: %+v", record.Op, record.Kind, record.Body)
            }
            if page.Next != nil {
                query.Start = iggcon.AfterMetadataChangeStart(*page.Next)
            }
            if !page.HasMore {
                time.Sleep(time.Second)
            }
        case iggcon.MetadataChangeStatusBehind:
            time.Sleep(100 * time.Millisecond)
        default:
            if query.Start, err = rebuildMetadataMirror(ctx, cli, groups); err != nil {
                return err
            }
        }
    }
}

// rebuildMetadataMirror pages the current state, then returns the position to
// follow from.
func rebuildMetadataMirror(
    ctx context.Context,
    cli iggcon.Client,
    groups []iggcon.MetadataChangeGroup,
) (iggcon.MetadataChangeStart, error) {
    query := iggcon.NewMetadataChangesQuery(iggcon.CurrentMetadataChangeStart(nil), groups...)
    for {
        page, err := cli.GetMetadataChanges(ctx, query)
        if err != nil {
            return iggcon.MetadataChangeStart{}, err
        }
        switch page.Status {
        case iggcon.MetadataChangeStatusBehind:
            time.Sleep(100 * time.Millisecond)
            continue
        case iggcon.MetadataChangeStatusExpired, iggcon.MetadataChangeStatusReset:
            query.Start = iggcon.CurrentMetadataChangeStart(nil)
            continue
        }
        for _, item := range page.Items {
            log.Printf("%s: %+v", item.Group, item.Body)
        }
        switch {
        case page.CurrentPageToken != nil:
            query.Start = iggcon.CurrentMetadataChangeStart(page.CurrentPageToken)
        case page.Next != nil:
            return iggcon.AfterMetadataChangeStart(*page.Next), nil
        default:
            query.Start = iggcon.CurrentMetadataChangeStart(nil)
        }
    }
}
```

The sample is in `client/example_metadata_changes_test.go`.

`MetadataChangeStatusExpired` means the log no longer holds the history after the cursor, and `MetadataChangeStatusReset` means the cursor belongs to another history, for example after the data directory was rebuilt. Either way the mirror rebuilds from current state pages and then follows the changes after the anchor they return.

Current pages need the parent groups of every selected group, for example `MetadataChangeGroupTopics` with `MetadataChangeGroupStreams`. Keep a rebuilt state private until a change page answers `MetadataChangeStatusOk` with `HasMore` false and a `HeadOp` at or above the last current page's.

Record and item bodies are typed by kind and group, for example `*iggcon.MetadataStreamChange` or `*iggcon.MetadataUserPermissionsUpdated`. A group or kind newer than this build arrives as `*iggcon.MetadataUnknownBody` with its raw codes on the record, so a mirror that cannot apply it can stop without advancing its cursor.

A query that breaks the request rules, such as a limit outside 1 to 1000 or a time bound with a latest or current start, is refused with `ierror.ErrInvalidCommand` before anything is sent. Cursors and page tokens print and parse as the `mc1_` and `cs1_` text forms the HTTP API and the CLI use. Page tokens also implement `encoding.TextMarshaler` and `encoding.TextUnmarshaler`, so `encoding/json` stores them as their `cs1_` text.

## Delivery semantics

Receipt access follows the server's [session and revocation policy](../../core/server/README.md#sessions-and-retry-receipts).
An unresolved operation cannot recover its receipt after its original session
ends, even after a new login or account reactivation. A fresh send can duplicate
that operation.

Each node counts the messages in its stream, topic and system stats. After a
successful send, a node that is not the partition primary shows the new count
only after the next prepare or commit broadcast. When no other write follows,
this delay is at most `cluster.commit_broadcast_interval` (500 ms by default).
See [metadata freshness and counters](../../core/server/README.md#metadata-freshness-and-counters).

`SendMessages` returns any placements the server reports. Replicated mutations
replay the same frozen request, client identity, and request ID after an uncertain
reply. Once uncertain, only a matching final committed-result proof can settle
the operation. A reconnect binds the existing session, also after a stale client
eviction; a terminal session refusal ends its retained operations and permits a
new registration. While the client registers that new session by itself, a call
that it did not send waits for the session. So does a call that starts while
`Connect` signs in. Then the call goes out under the new session. If no session
comes before the call deadline, the call fails with 58, and the caller can retry
it.

If the server refuses the current credentials of that registration, with code 4, 40, 42, 43, 44, 47, 53 or 54, every call that waits for it fails with 40 and the refusal as its cause. Such a refusal blocks automatic sign-in until another explicit sign-in. `Connect`, including after `Disconnect`, cannot unblock it. If a committed password change already updated the same owner's credentials, refusal of the older password retries the updated credentials.

Other refusals keep the last sign-in and leave the session owed. After a session loss the client keeps registering, and a call that waits for the session fails with 58 at its deadline. A call that waits for a `Connect` fails with 58 and the refusal as its cause, and a later `Connect` can retry the sign-in. A `Connect` whose own sign-in is refused returns the refusal itself. The client also logs every refusal.

A sign-in, a logout or a disconnect by the application ends every call that
started before it. A call that did not change server state fails with 40. A
mutation that possibly changed server state ends uncertain. A fetch with
auto-commit whose reply arrived returns its sections, because the server stored
their offsets. No call goes out under a session that the application opened
after the call started.

A reply with status 30 (`StaleClient`) can deny one attempt, because the server
no longer attaches the link to the session. Usually the session ended, for
example because an administrator deleted or deactivated the user. But after a
state transfer, which copies the state of another replica, a replica also denies
a live session with 30. So the client closes only that link, as after a stale
client eviction, and the next link binds the session again. If the bind
succeeds, the session keeps its retained operations and its consumer group
membership. If the server refuses the bind, the client registers a new session,
as after any terminal session refusal.

A committed 30 means that the replicated metadata ended the session, so the
client drops the session at once. A committed 30 still settles an uncertain
operation, like any other committed rejection.

The client registers a new session with the credentials of the last explicit
sign-in. After a successful session, `Disconnect` makes the client forget that
sign-in. If automatic sign-in is allowed and the client has no remembered
sign-in, it uses the `WithAutoLogin` credentials. After `LogoutUser`,
only a new sign-in by the application, or a `Connect` after `Disconnect`,
registers a session. When a password change or a rename of the user behind
those credentials commits, the client updates them. This holds for typed calls,
`SendBinaryRequest` and `ResolveOperation`. A rejected change leaves the
credentials unchanged.

`SendBinaryRequestToReplica(ctx, code, payload, replicaID)` sends a raw command to one node of a cluster, named by the replica id that cluster metadata returns.

Each node answers non-replicated commands such as `GetStats` from its own state, so this reads the state of that node. Replicated commands, polls, fetches, deferred-request cancellations, consumer-offset reads and session-control codes fail with `ErrInvalidCommand` before anything is sent. An id that cluster metadata does not name fails with `ErrInvalidIdentifier`. An unreachable node is retried until the request deadline, and `ErrNotPrimary` is returned rather than sent to another node.

If a mutation remains uncertain when its call ends, `errors.As` exposes
`errors.OperationUncertain` with an exact `uint64` `OperationID`. The SDK retains
that request and blocks later mutations on its lane with `errors.LaneBlocked`
carrying the same ID. Other partitions and pure reads can continue. Metadata
mutations share one lane; partition mutations use the partition incarnation
and partition ID.

Call `ResolveOperation(ctx, operationID)` to resume that exact request. A proven
business rejection returns its original error and releases the lane. The client
keeps the last `maxOperations` outcomes that its replays proved, also across a
session reset. So a later `ResolveOperation` of the same ID returns that outcome
without a request, also after another caller resolved the operation first.

Adding these two methods to `contracts.Client` requires external implementations,
wrappers, and mocks to implement them. The exported
`IggyTcpClient.HandleLeaderRedirection` method has been removed; replica routing
and reconnection are managed by the client.

If the application accepts the uncertainty, call
`AbandonOperation(ctx, operationID)` to release local recovery ownership.
Abandoning does not cancel server execution or make a new application send safe
from duplication. A `ResolveOperation` that is still running keeps the lane
until it ends, so later mutations still queue behind its replay. The client also
keeps the outcome that this replay proves. `errors.OperationNotRetained` does not
establish the outcome. Either the client never proved it, because a session
reset or an abandon came first, or the proven outcome aged out. An unknown ID
gets the same error.

`tcp.WithPendingOperations(maxOperations, maxBytes)` configures the shared bound
for active and retained mutation frames. Both values must be positive. Defaults
are 256 operations and 64 MiB, including each 256-byte header. Admission waits
within the call deadline before allocating a request ID; an oversized frame
fails locally. Writer references keep their bytes charged after abandonment or
session reset until the writer releases them. Calls without an earlier context
deadline have a 30-second operation budget.

Managed connections reconnect independently, with at most four simultaneous
dials and bounded exponential jitter. Each replica connection dials the address
that its node advertises in the cluster roster. With a one-node roster, it dials
the `tcp.WithServerAddress` endpoint instead, so a single node behind a port
mapping or a port-forward stays reachable. `tcp.WithHeartbeatInterval` controls
activity on every ready replica connection; the default is five seconds and a
nonpositive interval disables it. Logout, disconnect, and close stop the tasks.

After a session loss, the client registers a new session by itself. A
registration that loses its link or reaches no endpoint goes again after the
reconnect backoff. A terminal refusal, a disconnect or a close stops it.
A sign-in by the application also stops it and uses the supplied credentials.
Any failed explicit attempt, including invalid input, cancellation, timeout or
roster discovery failure after Register succeeds, discards its tentative session
and requires another explicit sign-in. The sign-in does not wait for a pending
automatic registration, and late completion cannot replace a newer session.

An established session has its own lifetime. Password or PAT changes and PAT
expiry do not end it. Logout, session lease expiry, or user deactivation end the
session and prevent its bind proof from restoring it.

Crash durability follows the topic's `durability` policy: `replicated`
confirms replication, while `persisted` also waits for the required replicas
to persist the message data. An empty confirmation list is a valid success
but does not by itself prove that new messages were appended.

In a cluster, sends, polls, and consumer offset operations use the authoritative
partition route (`GetPartitionRoute`, command 105). Metadata and membership
operations use the metadata primary. Every replica link binds the same session,
so partition routing preserves consumer-group membership. Route aliases share
view and refusal fences; route and topic caches have bounded capacity.

Use SDKs and servers from the same protocol generation and fresh storage for
this wire-contract cutover. The server refuses the previous storage format;
there is no implicit migration or fallback to retired routing commands.

`Next` polls and polls with `iggcon.OnReadCommit()` can advance progress before replying. A 57 reply, a lost reply or an eviction after sending ends a `Next` poll that stores nothing with 57, and a poll that stores its offset with `errors.FetchProgressUncertain` (14010) naming the operation, because the server can have stored it.

An error reply of a poll or fetch that stores its offset is uncertain too, unless its status proves that nothing was stored (the `commit_outcomes` table of the shared deferred polling vectors).

A `Next` poll that stores nothing releases its lane barrier and creates no retained operation. A poll or fetch that stores its offset and ends uncertain keeps the progress of its partitions blocked: the next poll or fetch of the partition that stores its offset and every offset store or delete of it fail with `errors.PartitionLaneBlocked` (14009) naming the operation, also after a reconnect and a new session of the same client, until `AbandonOperation(operation)`.

A later poll from `Next` starts after what the server stored. Pure reads retry within their deadline. Known non-admission replies 58
and 89 can retry without changing the frozen request. Manual offset writes are
replicated mutations and use the exact-ID recovery API above.

Each partition also has a progress lane, which admits one call at a time in
arrival order. Progress polls and consumer offset reads, stores and deletes
take it. Sends take only the mutation lane of the partition, so a parked
progress poll does not delay a send of the same client. An offset store or
delete takes both lanes. If it stays uncertain, it blocks the later progress
polls and offset calls of its partition with `errors.LaneBlocked` until you
resolve or abandon it. An uncertain send blocks only later mutations.

## Reading messages

The SDK reads messages at three levels: `PollMessages` reads one partition, a `reader.PartitionReader` reads many partitions in batches, and a `reader.IggyConsumer` hands over one message at a time. All three take `iggcon.PollOptions`, whose `CommitPolicy` says who stores the consumer offsets.

### Poll one partition

`PollMessages` lets the server hold the request until messages arrive. It waits up to `MaxWait` for `MinCount` messages and returns at most `Count` messages and `MaxBytes` of records. The call ends after `MaxWait` plus 10 s, or at the deadline of its context.

```go
options := iggcon.WaitPollOptions(5 * time.Second) // 1000 messages, 1 MiB, nothing stored
options.Commit = iggcon.OnReadCommit()
polled, err := client.PollMessages(ctx, stream, topic, 0, iggcon.DefaultConsumer(),
    iggcon.NextPollingStrategy(), options)
```

`iggcon.ManualCommit()` (the default) stores nothing, and the application stores a checkpoint with `StoreConsumerPosition`. `iggcon.OnReadCommit()` makes the server store the offset of the last message it returns as part of the read, in one round trip. While the primary stays up a message is delivered at most once. A failover before the store replicates can deliver the last batch again.

A poll of one partition refuses `iggcon.AutoCommitPolicy(...)` with `errors.ErrInvalidConfiguration`, because it has nothing to count. A reader and a consumer accept it.

A first message larger than `MaxBytes` comes alone in the reply, up to `iggcon.MaxRecordSize`. A `Last` poll of more than one message whose first message is larger than `MaxBytes` fails with `errors.ErrInvalidSizeBytes`. `PolledMessage.Checkpoint` and `CheckpointAt` name the position after a message with the history of its partition.

`PolledMessage.Continuation` is the strategy after the last message, bound to its partition, and a poll or a seek of another partition refuses it with `errors.ErrInvalidCommand`.

When the context of a poll with `OnReadCommit` is cancelled, the SDK cancels the poll on the server and waits for its one reply: refused with `errors.ErrTransientNotAccepted` (58) when the cancel won and nothing was stored, or with what the server read and stored, which the call returns. Until then the offset calls of the partition wait, so they never race a late store.

A reply that the connection loses ends the call with `errors.FetchProgressUncertain` (14010) and blocks the progress of the partition until `AbandonOperation`, as above. After the abandon the next `Next` poll starts after what the lost read stored. The client holds at most 64 deferred polls at once, a call waits for a free slot within its deadline, and `Close` waits for the reply of each poll that stores its offset.

### Partition reader

`reader.NewPartitionReader` reads the partitions of a topic in batches.

It groups the partitions by their primary and reads the partitions of a group together.

It puts the partitions that a read served last at the end of the next one so a busy partition does not starve the others, and spreads the partitions that hold more messages than their last read returned over the free prefetch slots, one read each with the whole `Count` and `MaxBytes`, while the quiet partitions of a group share one read that waits on the server.

Under `OnReadCommit` every read stores, so a group holds one read that waits at a time, and a waiting `Next` fills the free slots with the backlogged partitions, one committing read per partition.

It regroups after a primary moves, keeps a prefetch, and stays usable after an error.

A standalone consumer names its partitions with `reader.WithPartitions`. A consumer group reader joins its group, follows its assignment and leaves the group in `Close`. A later or concurrent `Close` waits for the first one and returns its result.

```go
partitionReader, err := reader.NewPartitionReader(ctx, client, stream, topic, iggcon.DefaultConsumer(),
    reader.WithPartitions(0, 1, 2), reader.WithStart(iggcon.FirstPollingStrategy()))
batch, err := partitionReader.Next(ctx)
checkpoint, _ := batch.Checkpoint()
err = partitionReader.Commit(ctx, checkpoint)
err = partitionReader.Close(ctx)
```

`Next` returns the next batch of one partition, never empty, in offset order within one generation of the partition: from its first read, from each `Seek`, and from each new assignment. A cancelled `Next` leaves its batch in the reader. The default wait of a reader is 5 s (`reader.DefaultMaxWait`).

- `ManualCommit`: `Commit` stores the offset of a delivered checkpoint, forward only within a generation.
- `OnReadCommit`: the server stores each read, and the reader reads only for a waiting `Next`. After a read whose reply was lost, the reader abandons it, waits its retry interval, and its next read starts after what the lost read stored. The reader reads those messages again without a store and delivers them first. A forward jump without such a lost read is a store of the application or of another session, and the reader goes on from there.

  A partition that leaves the assignment still hands over the reads that the server stored for it. A `Seek` to an offset above 0 stores the offset before it once the read in flight ends, also when `Close` starts first, and a seek to offset 0 or to another strategy stores nothing. A refused store fails the seek, or the close when it ran there. `Close` moves the offset of each partition with undelivered batches back to its last delivered message. `Commit` and `Mark` are refused.
- `AutoCommitPolicy(auto)`: the reader counts each batch as `auto.After` says (`CommitAfterProcessing` at the next `Next` or `Close`, `CommitAfterDelivery` when `Next` returns it, `CommitAfterMarked` on `Mark`), and stores the latest counted position at `auto.Interval`, on `auto.Every`, before a rebalance takes a partition away, and in `Close`. `iggcon.DefaultAutoCommit()` stores every 10,000 messages or 5 s after processing.

A `*reader.ReadError` names its partition. `errors.ErrHistoryUnavailable` stops a partition until `Seek` when its position came from an earlier read of a history that is gone. A section that does not decode is read again one message at a time: every good message is delivered, and a message that does not decode stops its partition with `Skip`, the continuation after it.

`errors.ErrConsumerGroupPartitionNotOwned` reads the partition again from its stored offset, and six in a row are reported. A refusal that a retry does not change is retried with a doubling wait and stops its partition after six in a row.

### Consumer

`reader.NewIggyConsumer` hands over one message at a time from a reader, as `Next`, as an iterator from `Messages`, or to a handler in `Consume`. Under `CommitAfterProcessing` a message counts when the next one is asked for, or after its handler succeeds in `Consume`. A handler that fails leaves its message uncounted and ends `Consume` with its error. `Consume` also ends when its context ends, between the messages of a batch too.

`Shutdown` stops `Consume`, counts the last message, stores, moves `OnRead` offsets back and leaves the group.

```go
options := iggcon.WaitPollOptions(5 * time.Second)
options.Commit = iggcon.AutoCommitPolicy(iggcon.DefaultAutoCommit())
consumer, err := reader.NewIggyConsumer(ctx, client, stream, topic, iggcon.NewGroupConsumer(group),
    reader.WithPollOptions(options))
err = consumer.Consume(ctx, func(ctx context.Context, message reader.ReceivedMessage) error {
    return process(message.Message)
})
err = consumer.Shutdown(ctx)
```

`SendBinaryRequest` sends codes 106 and 109 as they are and refuses code 108. External implementations, wrappers and mocks of `contracts.Client` implement `PollMessages` with its options.

## Testing

Unit tests need nothing running:

```bash
go test ./...
```

The end-to-end suite runs against a server at the address in
`IGGY_TCP_ADDRESS` and skips when that variable is unset. Start the server
with the encryption variables shown above, the stream option cases fail
without them:

```bash
IGGY_TCP_ADDRESS=127.0.0.1:8090 go test ./tests
```

Add `IGGY_TCP_TLS_ENABLED=true` to run the TLS cases against a server started
with `IGGY_TCP_TLS_ENABLED=true` and the certificate pair in `core/certs`.

From the repository root, the integration harness builds a three-node fixture,
seeds messages, moves metadata leadership independently of the partition
primary, and runs the Go regression. It requires Go on `PATH`:

```bash
cargo build --bin iggy-server --bin iggy
cargo test -p integration given_split_primaries_when_go_group_ -- --ignored --test-threads=1
```

To use an existing fixture, set its coordinator address and topic. The test
expects eight messages per partition by default; override that with the
positive integer `IGGY_POLL_ROUTING_MESSAGES_PER_PARTITION` when needed:

```bash
IGGY_TCP_ADDRESS=127.0.0.1:20016 \
IGGY_POLL_ROUTING_STREAM=sdk-primary-routing \
IGGY_POLL_ROUTING_TOPIC=go \
go test ./tests -run TestE2E_SplitPrimaryPollsPreserveCoordinatorMembership
```

## Contributing

Before creating a pull request, please run [golangci-lint](https://golangci-lint.run/welcome/quick-start/) and fix any reported lint issues:

```shell
golangci-lint run
```
