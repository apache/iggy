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
target/debug/iggy-server
```

QUIC, WebSocket and HTTP are enabled by default on ports 8080, 8092 and 3000.
Disable the ones you do not need so they cannot race with another process.

## Delivery semantics

`SendMessages` returns any placements the server reports. Replicated mutations
replay the same frozen request, client identity, and request ID after an uncertain
reply. Once uncertain, only a matching final committed-result proof can settle
the operation. A reconnect binds the existing session; a terminal session refusal
ends its retained operations and permits a new registration.

If a mutation remains uncertain when its call ends, `errors.As` exposes
`errors.UncertainOutcome` with an exact `uint64` `OperationID`. The SDK retains
that request and blocks later mutations on its lane with `errors.LaneBlocked`
carrying the same ID. Other partitions and pure reads can continue. Metadata
mutations share one lane; partition mutations use the partition incarnation
and partition ID.

Call `ResolveOperation(ctx, operationID)` to resume that exact request. A proven
business rejection returns its original error and releases the lane. Call
`AbandonOperation(ctx, operationID)` to release local recovery ownership when the
application accepts the uncertainty. Abandoning does not cancel server execution
or make a new application send safe from duplication. Unknown or session-retired
IDs return `errors.OperationNotRetained`.

`tcp.WithPendingOperations(maxOperations, maxBytes)` configures the shared bound
for active and retained mutation frames. Both values must be positive. Defaults
are 256 operations and 64 MiB, including each 256-byte header. Admission waits
within the call deadline before allocating a request ID; an oversized frame
fails locally. Writer references keep their bytes charged after abandonment or
session reset until the writer releases them. Calls without an earlier context
deadline have a 30-second operation budget.

Managed connections reconnect independently, with at most four simultaneous
dials and bounded exponential jitter. `tcp.WithHeartbeatInterval` controls
activity on every ready replica connection; the default is five seconds and a
nonpositive interval disables it. Logout, disconnect, and close stop the tasks.

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

`Next` and auto-commit polls can advance progress before replying. A 57 reply or
a lost reply after sending ends that call immediately, releases its transient
lane barrier, and creates no retained operation. A later application poll is a
new operation and may skip messages whose progress already advanced. Pure reads
retry within their deadline. Known non-admission replies 58 and 89 can retry
without changing the frozen request. Manual offset writes are replicated
mutations and use the exact-ID recovery API above.

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
