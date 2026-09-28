<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-darkbg.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg">
    <img alt="Apache Iggy" src="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg" width="320">
  </picture>
</div>

# Go SDK for Iggy

Official Go client SDK for [Apache Iggy](https://iggy.apache.org) message streaming.

The client speaks the VSR wire protocol over TCP, with or without TLS, in a
blocking implementation. VSR is the only protocol it supports.

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
target/debug/iggy-server
```

QUIC, WebSocket and HTTP are enabled by default on ports 8080, 8092 and 3000.
Disable the ones you do not need so they cannot race with another process.

## Delivery semantics

`SendMessages` returns any placements the server reports. A send whose reply
is lost to a dropped connection returns `ErrDisconnected` without a replay.
A reconnect registers a new client identity, so a caller retry can append
the batch twice. Consumers must handle duplicates through idempotent
processing or application-level deduplication.

Crash durability follows the topic's `durability` policy: `replicated`
confirms replication, while `persisted` also waits for the required replicas
to persist the message data. An empty confirmation list is a valid success
but does not by itself prove that new messages were appended.

In a cluster, auto-commit polls, `StoreConsumerOffset` and `DeleteConsumerOffset`
go to the partition primary. The client asks the coordinator for the route and
sends the request on a data connection attached to the coordinator session. The
coordinator keeps the consumer's group membership. The client must reach the
advertised TCP address of each primary. With TLS certificate validation enabled,
each primary's certificate must match its advertised host or the configured TLS
domain. Servers must support primary routing and consumer-session attachment
(binary commands 14, 103, 104 and 123). Pause binary consumers that auto-commit,
store or delete offsets for the whole upgrade: upgrade every server first, then
the SDKs, and restart consumers so they rejoin their groups. If a backup refuses
an offset commit, older SDKs can lose the membership. The new SDK does not fall
back to legacy polling.

For clustered auto-commit polls, the SDK retries only those that the server
refused before admission.
`ErrTransientNotCommitted` or cancellation after sending a poll can mean its
offset advanced without a reply. The SDK does not replay that poll
automatically. If a clustered offset write loses its reply, the call returns
`ErrTransientNotCommitted`, not `ErrDisconnected`. The outcome of the write is
then unknown, and storing the same offset again is safe. Retrying a delete after
a lost reply can return nil or `ErrConsumerOffsetNotFound` (status 3021); both
mean that no offset is stored. `GetConsumerOffset`
reads the replica on the coordinator node. Right after a write on a primary on
another node, it can return the previous offset for a short time.

`Next` polls with auto-commit disabled also read the coordinator replica. After
a routed offset store or delete, they can temporarily use the previous offset,
repeating messages or skipping the intended restart after a delete.

Standalone auto-commit polls can be replayed after a lost reply while the
caller's context is active. With `Next`, the first poll may already have
advanced the offset, so the replay can skip messages from its unread batch.

When a caller cancels a request on a signed-in connection, or its deadline
passes, the call returns the context error at once. The client finishes that
exchange in the background, so the request can still reach the server and
commit. The client does not resend it after the caller gives up. The connection,
the session and the consumer group membership stay. Until that request gets its
reply or its 30 s budget ends, later requests on the same client wait for the
connection. Each one waits only as long as its own context allows.

If the server never answers, the client drops the connection when the 30 s
budget ends, and the next request reconnects with a new session. Consumers must
rejoin their groups after that reconnect. As in the Rust SDK, the heartbeat
does not drop it sooner. So failover from a hung node can take up to about 35 s
with the default 5 s heartbeat interval. `Close` does not wait for a request in
flight. That request fails at once.

For sign-in, logout and requests on a connection without a session, a cancel
in flight drops the connection. On a primary data connection, a cancel
drops only that data connection, and the next request opens a new one. The
coordinator session and the membership stay.

## Testing

Unit tests need nothing running:

```bash
go test ./...
```

The end-to-end suite runs against a server at the address in
`IGGY_TCP_ADDRESS` and skips when that variable is unset:

```bash
IGGY_TCP_ADDRESS=127.0.0.1:8090 go test ./tests
```

Add `IGGY_TCP_TLS_ENABLED=true` to run the TLS cases against a server started
with `IGGY_TCP_TLS_ENABLED=true` and the certificate pair in `core/certs`.

## Contributing

Before creating a pull request, please run [golangci-lint](https://golangci-lint.run/welcome/quick-start/) and fix any reported lint issues:

```shell
golangci-lint run
```
