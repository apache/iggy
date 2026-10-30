<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-darkbg.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg">
    <img alt="Apache Iggy" src="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg" width="320">
  </picture>
</div>

# Iggy C++ Client

C++ client for [Apache Iggy](https://iggy.apache.org) message streaming.

Bazel uses the Cargo and Rust toolchain provisioned by `rules_rust`. The Rust bridge builds locally into a separate target directory in the Bazel output tree. Use the Bazel version pinned in `.bazelversion` with a compatible Java runtime (Java 21 or newer for Bazel 9.2.0).

Build commands

```bash
# Build library
bazel build //:iggy-cpp

# Unit tests
bazel test //:unit

# Low-level integration tests (require a running server)
bazel test //:e2e
```

The E2E suite expects a local server on the default ports with encryption enabled, because the resource options cases and the keyed metadata change cases create streams with their own keys and expect a server-generated key for keyless streams. Start it with `IGGY_ENCRYPTION_ENABLED=true` and `IGGY_ENCRYPTION_KEY` set to a base64 encoded 32-byte key, for example `IGGY_ENCRYPTION_ENABLED=true IGGY_ENCRYPTION_KEY=$(head -c 32 /dev/urandom | base64) cargo run --bin iggy-server -- --with-default-root-credentials`.

Receipt access follows the server's [session and revocation policy](../../core/server/README.md#sessions-and-retry-receipts).
An unresolved operation cannot recover its receipt after its original session
ends, even after a new login or account reactivation. A fresh send can duplicate
that operation.

Each node counts the messages in its stream, topic and system stats. After a
successful send, a node that is not the partition primary shows the new count
only after the next prepare or commit broadcast. When no other write follows,
this delay is at most `cluster.commit_broadcast_interval` (500 ms by default).
See [metadata freshness and counters](../../core/server/README.md#metadata-freshness-and-counters).

## Resource options

Streams, topics and users take typed create and update options. Unset settings use the server default on create and keep their stored value on update. `SetRawEntries()` and `RawReset()` name keys newer than this SDK build and leave the decision to the server. `DescribeOptions()` serves each scope's catalog.

```cpp
const std::string key_base64 = LoadStreamKey();
const auto stream = client.CreateStream("payments", iggy::StreamCreateOptions().SetEncryptionKeyBase64(key_base64));
const bool same_key = stream.EncryptionKeyFingerprint() == iggy::EncryptionKeyFingerprint(key_base64);

const auto stream_id = iggy::Identifier::Numeric(stream.Id());
const auto topic     = client.CreateTopic(stream_id, "orders",
                                          iggy::TopicCreateOptions()
                                              .SetPartitionsCount(3)
                                              .SetDurability(iggy::Durability::Persisted)
                                              .SetPartitionResizePolicy(iggy::PartitionResizePolicy::Fixed));
client.UpdateTopic(stream_id, iggy::Identifier::Numeric(topic.Id()),
                   iggy::TopicUpdateOptions()
                       .SetMessageExpiry(iggy::Expiry::Duration(3600000000))
                       .Reset(iggy::TopicOptionKey::MaxTopicSize));
```

A stream key must be 32 random bytes, never derived from a password, because reads expose its fingerprint. It travels inside the create request, so protecting it in transit with TLS is the caller's job. The server stores it wrapped and never returns it. Reads report only its source and fingerprints. Without a key, a server with encryption enabled generates one for the stream. An invalid key is refused before anything is sent.

Binary mutations retain an uncertain operation until it is resolved or abandoned.
`IggyException::OperationId()` carries an exact `std::uint64_t` for
`operation_uncertain`, `lane_blocked`, and `operation_not_retained`.
Keep using the same client to recover:

```cpp
try {
    client.CreateStream("events");
} catch (const iggy::IggyException &error) {
    if (error.Kind() == "operation_uncertain" && error.OperationId()) {
        client.ResolveOperation(*error.OperationId(), std::chrono::seconds(30));
    } else {
        throw;
    }
}
```

`ResolveOperation` retries the retained identity and proves a successful commit
without returning the original response payload. A proved committed business
rejection throws its original error. Both outcomes release the retained operation
and its lane. An `operation_uncertain` result remains retained.
`AbandonOperation` releases local state without undoing or determining the server
result. `operation_not_retained` does not prove noncommit, so submitting the old
payload again can duplicate work. A `lane_blocked` ID belongs to an earlier call;
resolve it before submitting the new request again. Polls are never replayed.
A poll with `CommitPolicy::OnRead` whose reply is lost fails with
`fetch_progress_uncertain` and blocks its partition on the client: later polls
of that partition fail with `partition_lane_blocked` until `AbandonOperation`
releases the operation that the error names.

`Disconnect()` and session reset discard retained operations without settling
their outcomes. Reconnecting cannot recover those discarded operations.
The client keeps the proved outcomes of its last `max_operations` replays, also
across a session reset. A later `ResolveOperation` of a proved operation returns
the same outcome without a request.
TCP, QUIC, and WebSocket support `ResolveOperation` and `AbandonOperation`.
HTTP reports `feature_unavailable` for `ResolveOperation`, and its
`AbandonOperation` releases only the poll operations of `fetch_progress_uncertain`.

The blocking wrapper populates `IggyException::Code()` and `Kind()` for Rust SDK
errors from these methods:

- `Login`, `Logout`, `ResolveOperation`, and `AbandonOperation`.
- `CreateUser`, `UpdateUser`, and `DeleteUser`.
- `CreateStream`, `UpdateStream`, and `DeleteStream`.
- `CreateTopic`, `UpdateTopic`, and `DeleteTopic`.
- `CreatePartitions`, `DeletePartitions`, and `DeleteSegments`.
- `CreateConsumerGroup`, `DeleteConsumerGroup`, `JoinConsumerGroup`, and `LeaveConsumerGroup`.
- `StoreConsumerOffset`, `StoreConsumerPosition`, `DeleteConsumerOffset`, and `DeleteConsumerPosition`.
- `PollMessages`, `Reader`, and the calls of `PartitionReader`.
- `GetMetadataChanges`.

Other methods, including `Connect`, `Disconnect`, and `Shutdown`, report errors through the exception message only. C++ and bridge-local argument errors also leave `Code()` and `OperationId()` empty and `Kind()` empty, including for the methods listed above. `GetMetadataChanges` is the exception: it reports every invalid query as `invalid_command`, including one the bridge refuses.

Direct generated-bridge callers pass a local `iggy::ffi::ErrorInfo` as the last argument to login/logout, mutations, polls, raw commands, recovery calls, and the metadata change functions. It is reset at entry and populated only when the Rust SDK returns an `IggyError`. Bridge-local argument errors leave it empty, except in the metadata change functions, which record invalid input as `invalid_command`.

`send_binary_request_to_replica(code, payload, replica_id, error_info)` sends a raw command to one node of a cluster, named by the replica id that cluster metadata returns.

Each node answers non-replicated commands such as `GetStats` from its own state, so this reads the state of that node. Replicated commands, polls, consumer-offset reads and session-control codes fail with `invalid_command` before anything is sent. An id that cluster metadata does not name fails with `invalid_identifier`. An unreachable node is retried until the request timeout, and `not_primary` is returned rather than sent to another node.

Inspect `kind`, `code`, and, when `has_operation_id` is true, `operation_id`. Use separate values for concurrent calls. Error state is never stored globally or on the client.

The `IggyException(const std::string &, const ffi::ErrorInfo &)` constructor is private. Only the SDK builds an exception with a code. Other code builds an `IggyException` from its message.

Concurrent calls on one client are safe. Moving, assigning, or destroying the
client while any call is in progress is undefined behavior.

`IggyBlockingClient::PollMessages` reads one batch of one partition for a `PollingStrategy` and `PollOptions`, and its overload that takes a `Continuation` reads on after a batch. A default `PollOptions` reads up to 1000 messages and 1 MiB and does not wait. A continuation belongs to the partition of its batch.

`PolledMessages::Checkpoint()` and `CheckpointAt(index)` name the position after a delivered message. Pass one to `StoreConsumerPosition` to store it, or to `DeleteConsumerPosition` to delete the stored offset of its partition. Only a polled batch or a read error makes a `Checkpoint` or a `Continuation`, so a caller cannot change the partition context that they keep.

`PollOptions::SetCommit` says when offsets are stored: `CommitPolicy::Manual` (the default) stores nothing, `CommitPolicy::OnRead` makes the server store the offset of each read, and `CommitPolicy::Auto` makes a reader store the latest counted position as its `AutoCommit` says. `SetAutoCommit` sets `Auto` with an interval, a `CommitEvery` event and a `CommitAfter` point. A default `AutoCommit` stores every 10,000 messages or every 5 s, after processing. A one-shot poll refuses `Auto`.

```cpp
auto reader = client.Reader(iggy::Consumer::Single(iggy::Identifier::Numeric(1)), stream, topic,
                            iggy::ReaderOptions().SetPartitions({0}).SetPollOptions(
                                iggy::PollOptions().SetAutoCommit(iggy::AutoCommit())));
while (auto batch = reader.Next(std::chrono::seconds(1))) {
    for (const auto &message : batch->Messages()) {
        process(message);
    }
}
reader.Close();
```

`IggyBlockingClient::Reader` builds a `PartitionReader` with routing and prefetch:

- `Next` waits up to its timeout and returns `std::nullopt` when the time runs out.
- `Commit` stores a checkpoint that the reader delivered, and under `CommitPolicy::Auto` `Mark` counts one for the policy to store.
- `Seek` moves a partition to a `PollingStrategy`, or to a `Continuation` of that partition.
- A failed read throws `ReadException`, which names its partition. After a message that does not decode, its `Skip()` holds the continuation past it.
- `Close` stops the reader, and a second `Close` does nothing. Destroying a reader without `Close` moves no offset back and does not leave the group.

A consumer group reader joins its group in `Reader` and leaves it in `Close`.

`IggyBlockingClient::StandaloneConsumer` and `GroupConsumer` build an `IggyConsumer`, which hands over one `ReceivedMessage` at a time through a reader, with the commit policy of the `PollOptions` of its `ConsumerOptions`. A standalone consumer reads the partitions of its options. A group consumer creates its group when it is missing, joins it, and leaves it in `Close`.

`IggyConsumer::Next` returns `std::nullopt` when its timeout runs out, and under `CommitAfter::Processing` it counts the message handed over before. `Close` stores as the commit policy says.

`IggyBlockingClient::StartPollMessages` runs the poll of `PollMessages` in the background and returns a `PendingPoll` at once. `Wait` returns its batch, or `std::nullopt` while the poll still runs, and `Cancel` drops the request, which cancels a parked deferred poll on the server. Destroying a `PendingPoll` cancels it. A parked poll holds its connection, so start it on a client that sends nothing else meanwhile.

The generated bridge offers the same reads as `Client::poll_messages` and `Client::reader`, with `default_poll_options()` and a `ReadErrorInfo` for the errors of `next`.

`StoreConsumerOffset` and `DeleteConsumerOffset` require an explicit partition on
TCP, QUIC, and WebSocket; an absent partition returns `invalid_identifier`.
An omitted partition, including the default on `DeleteConsumerOffset`, is for
HTTP only.

The binary session defaults to 256 active or retained mutations and 64 MiB of
frozen request bytes. Configure both limits with
`Builder().WithPendingOperations(max_operations, max_bytes)`. Both must be positive.
`FromConnectionString` accepts the same limits as `max_pending_operations` and
`max_pending_operation_bytes` query parameters for TCP, QUIC, and WebSocket.
HTTP continues to use its configured transport behavior and has no retained
operation recovery.

## Metadata changes

`GetMetadataChanges()` reads the node's change log: one record per public result of a committed metadata operation, such as a stream created, a topic renamed, a user's permissions changed or a session closed. A mirror keeps a cursor, asks for the records after it, and stores the returned `Next()` cursor. The call needs root or the global `manage_servers` permission and works on every transport.

```cpp
// Pages the current state into a private staged copy, then returns the query that follows the changes after it.
iggy::MetadataChangesQuery Rebuild(iggy::IggyBlockingClient &client, const std::vector<iggy::MetadataChangeGroup> &groups) {
    ClearStaged();
    auto query = iggy::MetadataChangesQuery::Current().SetGroups(groups);
    while (true) {
        const auto page = client.GetMetadataChanges(query);
        switch (page.Status()) {
            case iggy::MetadataChangeStatus::Ok:
                break;
            case iggy::MetadataChangeStatus::Behind:
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            case iggy::MetadataChangeStatus::Expired:
            case iggy::MetadataChangeStatus::Reset:
                ClearStaged();
                query = iggy::MetadataChangesQuery::Current().SetGroups(groups);
                continue;
        }
        for (const auto &item : page.Items()) {
            StageItem(item);
        }
        if (!page.CurrentPageToken()) {
            return iggy::MetadataChangesQuery::After(*page.Next()).SetGroups(groups);
        }
        query = iggy::MetadataChangesQuery::Current(*page.CurrentPageToken()).SetGroups(groups);
    }
}

void Follow(iggy::IggyBlockingClient &client, const std::vector<iggy::MetadataChangeGroup> &groups) {
    auto query = Rebuild(client, groups);
    while (true) {
        const auto page = client.GetMetadataChanges(query);
        switch (page.Status()) {
            case iggy::MetadataChangeStatus::Ok:
                for (const auto &record : page.Records()) {
                    if (const auto *stream = std::get_if<iggy::MetadataStreamChange>(&record.Change())) {
                        ApplyStream(*record.Kind(), *stream);
                    }
                }
                query = iggy::MetadataChangesQuery::After(*page.Next()).SetGroups(groups);
                if (!page.HasMore()) {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
                break;
            case iggy::MetadataChangeStatus::Behind:
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                break;
            case iggy::MetadataChangeStatus::Expired:
            case iggy::MetadataChangeStatus::Reset:
                query = Rebuild(client, groups);
                break;
        }
    }
}
```

`Expired` means the log no longer holds the history after the cursor, and `Reset` means the cursor belongs to another history, for example after the data directory was rebuilt. Either way the mirror rebuilds from current state pages, as it does on start, and then follows the changes after the anchor they return. Each scan begins from an empty staged state, so nothing an abandoned scan staged survives. Persist a cursor with `MetadataChangeCursor::ToString()` and restore it with `MetadataChangeCursor::Parse()`.

Current pages need the parent groups of every selected group, for example `topics` with `streams`. Keep a rebuilt state private until a change page answers `Ok` with `HasMore()` false and a `HeadOp()` at or above the last current page's. An invalid query is refused with `invalid_command` before anything is sent.

A record or item this build does not know arrives as `MetadataUnknownChange` or `MetadataUnknownItem` with its codes and raw body on TCP, QUIC and WebSocket, and as `MetadataUnknownJsonChange` or `MetadataUnknownJsonItem` with its names and whole JSON object on HTTP. Records carry option values as the read routes return them, never key material.
