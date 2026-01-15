# Apache Iggy Connector Library

Core connector library for integrating Apache Iggy with stream processing engines.

## Overview

This library provides framework-agnostic abstractions and Flink-specific implementations for reading from and writing to Iggy streams.

### Transport Protocols

The connector supports two transport protocols for communicating with the Iggy server:

| Transport          | Default Port | Use Case                                                     |
| ------------------ | ------------ | ------------------------------------------------------------ |
| **HTTP** (default) | 3000         | Development, debugging, environments with HTTP proxies       |
| **TCP**            | 8090         | Production workloads requiring high throughput and low latency |

#### HTTP Transport (Default)

HTTP transport uses the REST API and is the default for backward compatibility:

```java
IggyConnectionConfig config = IggyConnectionConfig.builder()
    .serverAddress("localhost")
    .transportType(TransportType.HTTP)  // Optional, HTTP is default
    .httpPort(3000)                      // Optional, 3000 is default
    .username("iggy")
    .password("iggy")
    .build();
```

#### TCP Transport

TCP transport provides persistent connections with lower latency, ideal for high-throughput streaming workloads:

```java
IggyConnectionConfig config = IggyConnectionConfig.builder()
    .serverAddress("localhost")
    .transportType(TransportType.TCP)
    .tcpPort(8090)                       // Optional, 8090 is default
    .username("iggy")
    .password("iggy")
    .build();
```

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
    implementation("org.apache.iggy:iggy-connector-library:0.6.0")
    compileOnly("org.apache.flink:flink-streaming-java:1.18.0")
}
```

### Building from Source

```bash
cd iggy-connector-flink
./gradlew :iggy-connector-library:build
```

## Configuration Reference

### IggyConnectionConfig Options

| Option              | Type          | Default      | Description                              |
| ------------------- | ------------- | ------------ | ---------------------------------------- |
| `serverAddress`     | String        | *required*   | Iggy server hostname or IP address       |
| `transportType`     | TransportType | `HTTP`       | Transport protocol: `HTTP` or `TCP`      |
| `httpPort`          | int           | `3000`       | HTTP API port                            |
| `tcpPort`           | int           | `8090`       | TCP API port                             |
| `username`          | String        | *required*   | Authentication username                  |
| `password`          | String        | *required*   | Authentication password                  |
| `connectionTimeout` | Duration      | `30s`        | Connection timeout                       |
| `requestTimeout`    | Duration      | `30s`        | Request timeout                          |
| `maxRetries`        | int           | `3`          | Maximum retry attempts                   |
| `retryBackoff`      | Duration      | `100ms`      | Backoff between retries                  |
| `enableTls`         | boolean       | `false`      | Enable TLS/SSL encryption                |

### Example: Complete Sink Configuration

```java
// TCP transport for high-throughput production workloads
IggyConnectionConfig config = IggyConnectionConfig.builder()
    .serverAddress("iggy-server.example.com")
    .transportType(TransportType.TCP)
    .tcpPort(8090)
    .username("iggy")
    .password("secret")
    .connectionTimeout(Duration.ofSeconds(10))
    .requestTimeout(Duration.ofSeconds(30))
    .maxRetries(5)
    .enableTls(true)
    .build();

IggySink<Event> sink = IggySink.<Event>builder()
    .setConnectionConfig(config)
    .setStreamId("events")
    .setTopicId("user-events")
    .setSerializer(new JsonSerializationSchema<>())
    .setBatchSize(1000)
    .setFlushInterval(Duration.ofSeconds(1))
    .withBalancedPartitioning()
    .build();

dataStream.sinkTo(sink);
```

## Future Evolution

When Spark connector is added, common code may be extracted to a separate `iggy-connector-common` module. The current structure keeps Flink-first implementation simple while maintaining clear package boundaries for future refactoring.

See [connector_library_placement_deliberation](https://github.com/apache/iggy/discussions/2236#discussioncomment-14702253) for architectural decisions.

## License

Apache License 2.0 - See LICENSE file for details.
