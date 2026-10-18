<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-darkbg.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg">
    <img alt="Apache Iggy" src="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg" width="320">
  </picture>
</div>

# Swift SDK for Iggy

Official Swift client SDK for [Apache Iggy](https://iggy.apache.org) message streaming.

The SDK is built up in stages. This stage carries the package scaffold, the error table,
the byte codec the wire protocol is encoded with, native XXH3-64 and XXH32 hashes pinned
to the Rust crates by golden vectors, the domain models (streams, topics, messages,
user headers, options, permissions) with their wire encodings, and the protocol layer:
the consensus frame, the session and request id rules, the request encoders, the reply
decoders, and the message batch format. The TCP and TLS client, the producer and
consumer, examples, and BDD scenarios follow in later changes.

## Requirements

- Swift 6.0 or later (Swift 6 language mode, strict concurrency)
- macOS 13, iOS 16, tvOS 16, watchOS 9, visionOS 1, or Linux with a Swift 6 toolchain

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

Every operation of the SDK throws `IggyError`, a value carrying the typed `code`
(`IggyErrorCode`, one case per server error code), the raw wire code, and optional
context:

```swift
import Iggy

func handle(_ error: IggyError) {
    switch error.code {
    case .streamNameNotFound:
        print("create the stream first")
    default:
        print("iggy failed: \(error)")
    }
}
```

The client that raises these errors lands in the next changes of the series; see the
[Rust SDK](../../core/sdk) for the operations it mirrors.

## Testing

```bash
cd foreign/swift
swift test
```

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
