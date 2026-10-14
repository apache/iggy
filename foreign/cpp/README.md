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
`IggyException::Code()` and `Kind()` identify the error; `OperationId()` carries an
exact `std::uint64_t` for `operation_uncertain`, `lane_blocked`, and
`operation_not_retained`. Keep using the same client to recover:

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

`ResolveOperation` retries the retained identity and confirms completion without
returning the original response payload. An unknown result remains retained.
`AbandonOperation` releases local state without undoing or determining the server
result. `operation_not_retained` does not prove noncommit, so submitting the old
payload again can duplicate work. A `lane_blocked` ID belongs to an earlier call;
resolve it before submitting the new request again. Polls remain transient and
are never retained or replayed.

Direct generated-bridge callers pass a local `iggy::ffi::ErrorInfo` as the last
argument to mutations, polls, raw commands, and recovery calls. It is reset at
entry and populated before the bridge throws a `std::exception`. Inspect `kind`, `code`, and,
when `has_operation_id` is true, `operation_id`. Use separate values for concurrent
calls; error state is never stored globally or on the client.

The binary session defaults to 256 active or retained mutations and 64 MiB of
frozen request bytes. Configure both limits with
`Builder().WithPendingOperations(max_operations, max_bytes)`. Both must be positive.
`FromConnectionString` accepts the same limits as `max_pending_operations` and
`max_pending_operation_bytes` query parameters for TCP, QUIC, and WebSocket.
HTTP continues to use its configured transport behavior and has no retained
operation recovery.
