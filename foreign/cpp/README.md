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
