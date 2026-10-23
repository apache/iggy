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

The E2E suite expects a local server on the default ports with encryption enabled, because the resource options cases create streams with their own keys and expect a server-generated key for keyless streams. Start it with `IGGY_ENCRYPTION_ENABLED=true` and `IGGY_ENCRYPTION_KEY` set to a base64 encoded 32-byte key, for example `IGGY_ENCRYPTION_ENABLED=true IGGY_ENCRYPTION_KEY=$(head -c 32 /dev/urandom | base64) cargo run --bin iggy-server -- --with-default-root-credentials`.

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
resolve it before submitting the new request again. Polls remain transient and
are never retained or replayed.

`Disconnect()` and session reset discard retained operations without settling
their outcomes. Reconnecting cannot recover those discarded operations.
The client keeps the proved outcomes of its last `max_operations` replays, also
across a session reset. A later `ResolveOperation` of a proved operation returns
the same outcome without a request.
TCP, QUIC, and WebSocket support `ResolveOperation` and `AbandonOperation`;
HTTP reports `feature_unavailable` for both.

The blocking wrapper populates `IggyException::Code()` and `Kind()` for Rust SDK
errors from these methods:

- `Login`, `Logout`, `ResolveOperation`, and `AbandonOperation`.
- `CreateUser`, `UpdateUser`, and `DeleteUser`.
- `CreateStream`, `UpdateStream`, and `DeleteStream`.
- `CreateTopic`, `UpdateTopic`, and `DeleteTopic`.
- `CreatePartitions`, `DeletePartitions`, and `DeleteSegments`.
- `CreateConsumerGroup`, `DeleteConsumerGroup`, `JoinConsumerGroup`, and `LeaveConsumerGroup`.
- `StoreConsumerOffset`, `StoreConsumerPosition`, and `DeleteConsumerOffset`.

Other methods, including reads and `Connect`, `Disconnect`, and `Shutdown`, report
errors through the exception message only. C++ and bridge-local argument errors
also leave `Code()` and `OperationId()` empty and `Kind()` empty, including for the
methods listed above.

Direct generated-bridge callers pass a local `iggy::ffi::ErrorInfo` as the last
argument to login/logout, mutations, polls, raw commands, and recovery calls.
It is reset at entry and populated only when the Rust SDK returns an `IggyError`.
Bridge-local argument errors leave it empty. Inspect `kind`, `code`, and, when
`has_operation_id` is true, `operation_id`. Use separate values for concurrent
calls; error state is never stored globally or on the client.

The `IggyException(const std::string &, const ffi::ErrorInfo &)` constructor is
private. Only `IggyBlockingClient` builds an exception with a code. Other code
builds an `IggyException` from its message.

Concurrent calls on one client are safe. Moving, assigning, or destroying the
client while any call is in progress is undefined behavior.

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
