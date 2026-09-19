# Swift SDK for Iggy

Native Swift client for [Apache Iggy](https://iggy.apache.org), built up in stages. This
first stage carries the package scaffold, the error table, and the byte codec the wire
protocol is encoded with; the protocol layer, the TCP and TLS client, the producer and
consumer, examples, and BDD scenarios follow in later changes.

## Requirements

- Swift 6.0 or later
- macOS 13, iOS 16, tvOS 16, watchOS 9, visionOS 1, or Linux with a Swift 6 toolchain

## Testing

```bash
cd foreign/swift
swift test
```

## Contributing

Format the sources with the toolchain's formatter before opening a pull request; CI runs it
in lint mode:

```bash
swift format format --in-place --recursive Sources Tests Package.swift
swift format lint --strict --recursive Sources Tests Package.swift
```
