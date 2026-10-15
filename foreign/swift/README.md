<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-darkbg.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg">
    <img alt="Apache Iggy" src="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg" width="320">
  </picture>
</div>

# Swift SDK for Iggy

Official Swift client SDK for [Apache Iggy](https://iggy.apache.org) message streaming.

The SDK is built up in stages. This stage adds the client: a TCP connection with
optional TLS that speaks the consensus protocol, signs in, keeps the session alive with
a heartbeat, reconnects, follows the cluster leader, and manages streams, topics,
partitions, segments, users, and personal access tokens. Sending and polling messages,
consumer groups and offsets, the producer and consumer, examples, and BDD scenarios
follow in later changes.

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

A request that hits a dropped connection reconnects, signs in again, and is replayed,
all within its `requestTimeout`, and only when it provably never reached the log:
reads, pings, and requests the server never admitted. A replicated write whose outcome
is unknown fails with `transientNotCommitted` instead of being applied twice. A client
left disconnected by a timeout or an outage reconnects on its next call or heartbeat
when it has credentials to sign in with. In a cluster the client reads the roster after
signing in, moves to the leader, and moves on when a node answers that it is not the
primary.

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

## Testing

```bash
cd foreign/swift
swift test
```

The client tests run against an in-process server that speaks the consensus framing
and is scripted per test, so they need no running `iggy-server`.

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
