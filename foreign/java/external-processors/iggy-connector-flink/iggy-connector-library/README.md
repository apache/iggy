# Apache Iggy Connector Library

Core connector library for integrating Apache Iggy with stream processing engines.

## Overview

This library provides framework-agnostic abstractions and Flink-specific implementations for reading from and writing to Iggy streams.

### Package Structure

```text
org.apache.iggy.connector/
├── config/              - Configuration classes (connection, offset, retry)
├── serialization/       - Serialization/deserialization interfaces
├── offset/              - Offset tracking abstractions
├── partition/           - Partition discovery and assignment
├── error/               - Exception hierarchy
├── util/                - Utility classes (client pooling, metrics)
└── flink/               - Flink-specific implementations
    ├── source/          - Flink Source API integration
    ├── sink/            - Flink Sink API integration
    └── config/          - Flink-specific configuration
```

## Design Philosophy

### Framework-Agnostic Core

The `org.apache.iggy.connector.*` packages (excluding `flink`) are designed to be framework-agnostic and reusable across different stream processing engines (Spark, Beam, etc.).

**Design Principles:**

- No Flink imports in common packages
- Interface-based for extensibility
- Serializable for distributed execution
- Well-documented for external use

### Flink Integration

The `org.apache.iggy.connector.flink.*` packages provide Flink-specific implementations:

- Implement Flink's Source and Sink APIs
- Wrap common abstractions with Flink-specific adapters
- Handle Flink checkpointing and state management

## Usage

### As a Dependency

```kotlin
dependencies {
    implementation("org.apache.iggy:iggy-connector-library:0.6.0-SNAPSHOT")
    compileOnly("org.apache.flink:flink-streaming-java:1.18.0")
}
```

### Building from Source

```bash
cd iggy-connector-flink
./gradlew :iggy-connector-library:build
```

## Future Evolution

When Spark connector is added, common code may be extracted to a separate `iggy-connector-common` module. The current structure keeps Flink-first implementation simple while maintaining clear package boundaries for future refactoring.

See [connector_library_placement_deliberation](https://github.com/apache/iggy/discussions/2236#discussioncomment-14702253) for architectural decisions.

## License

Apache License 2.0 - See LICENSE file for details.
