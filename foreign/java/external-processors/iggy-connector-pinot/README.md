# Apache Iggy Connector for Apache Pinot

This connector enables Apache Pinot to ingest real-time data from Apache Iggy streams using TCP-based communication.

## Overview

The Iggy Pinot connector implements Pinot's Stream Plugin API to provide:

- **TCP-based ingestion**: Uses Iggy's native TCP protocol via `AsyncIggyTcpClient` for efficient message consumption
- **Partition-aware consumption**: Supports parallel ingestion from multiple Iggy partitions
- **Consumer group support**: Leverages Iggy consumer groups for offset management and fault tolerance
- **Automatic offset management**: Consumer group state is maintained by Iggy server
- **JSON message decoding**: Built-in support for JSON-formatted messages

## Architecture

### Key Components

1. **IggyConsumerFactory**: Main entry point implementing Pinot's `StreamConsumerFactory`
2. **IggyPartitionGroupConsumer**: Partition-level consumer using TCP client
3. **IggyStreamMetadataProvider**: Provides partition discovery and offset information
4. **IggyJsonMessageDecoder**: Decodes JSON messages into Pinot records

### Differences from Flink Connector

The Pinot connector differs from the Iggy Flink connector in several ways:

- **TCP-based**: Uses `AsyncIggyTcpClient` directly (not HTTP-based)
- **Consumer group managed**: Relies on Iggy's consumer group offset management
- **Simpler offset handling**: No custom offset storage, leverages Iggy's built-in state
- **Pinot-specific APIs**: Implements `PartitionGroupConsumer` instead of Flink's `SourceReader`

## Configuration

### Required Properties

Add the following to your Pinot table configuration's `streamConfigs` section:

```json
{
  "streamConfigs": {
    "streamType": "iggy",
    "stream.iggy.consumer.factory.class.name": "org.apache.iggy.connector.pinot.consumer.IggyConsumerFactory",
    "stream.iggy.host": "localhost",
    "stream.iggy.port": "8090",
    "stream.iggy.stream.id": "my-stream",
    "stream.iggy.topic.id": "my-topic",
    "stream.iggy.consumer.group": "pinot-realtime-group",
    "stream.iggy.decoder.class.name": "org.apache.iggy.connector.pinot.decoder.IggyJsonMessageDecoder"
  }
}
```

### Configuration Properties

#### Connection Properties

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| `stream.iggy.host` | Yes | - | Iggy server hostname |
| `stream.iggy.port` | No | 8090 | Iggy server TCP port |
| `stream.iggy.username` | No | iggy | Authentication username |
| `stream.iggy.password` | No | iggy | Authentication password |
| `stream.iggy.enable.tls` | No | false | Enable TLS encryption |
| `stream.iggy.connection.pool.size` | No | 4 | TCP connection pool size |

#### Stream Properties

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| `stream.iggy.stream.id` | Yes | - | Iggy stream identifier (name or numeric ID) |
| `stream.iggy.topic.id` | Yes | - | Iggy topic identifier (name or numeric ID) |
| `stream.iggy.consumer.group` | Yes | - | Consumer group name for offset management |
| `stream.iggy.poll.batch.size` | No | 100 | Number of messages to fetch per poll |

## Complete Example

Here's a complete Pinot table configuration for real-time ingestion from Iggy:

```json
{
  "tableName": "events",
  "tableType": "REALTIME",
  "segmentsConfig": {
    "timeColumnName": "timestamp",
    "timeType": "MILLISECONDS",
    "replication": "1",
    "schemaName": "events"
  },
  "tenants": {},
  "tableIndexConfig": {
    "loadMode": "MMAP",
    "streamConfigs": {
      "streamType": "iggy",
      "stream.iggy.consumer.type": "lowlevel",
      "stream.iggy.consumer.factory.class.name": "org.apache.iggy.connector.pinot.consumer.IggyConsumerFactory",
      "stream.iggy.decoder.class.name": "org.apache.iggy.connector.pinot.decoder.IggyJsonMessageDecoder",

      "stream.iggy.host": "localhost",
      "stream.iggy.port": "8090",
      "stream.iggy.username": "iggy",
      "stream.iggy.password": "iggy",

      "stream.iggy.stream.id": "analytics",
      "stream.iggy.topic.id": "user-events",
      "stream.iggy.consumer.group": "pinot-events-consumer",

      "stream.iggy.poll.batch.size": "1000",
      "stream.iggy.connection.pool.size": "8",

      "realtime.segment.flush.threshold.rows": "50000",
      "realtime.segment.flush.threshold.time": "3600000"
    }
  },
  "metadata": {}
}
```

## Schema Definition

Example Pinot schema for the events table:

```json
{
  "schemaName": "events",
  "dimensionFieldSpecs": [
    {
      "name": "userId",
      "dataType": "STRING"
    },
    {
      "name": "eventType",
      "dataType": "STRING"
    },
    {
      "name": "deviceId",
      "dataType": "STRING"
    }
  ],
  "metricFieldSpecs": [
    {
      "name": "duration",
      "dataType": "LONG"
    },
    {
      "name": "count",
      "dataType": "INT"
    }
  ],
  "dateTimeFieldSpecs": [
    {
      "name": "timestamp",
      "dataType": "LONG",
      "format": "1:MILLISECONDS:EPOCH",
      "granularity": "1:MILLISECONDS"
    }
  ]
}
```

## Building the Connector

From the `foreign/java` directory:

```bash
./gradlew :iggy-connector-pinot:build
```

This produces a JAR file at:
```
foreign/java/external-processors/iggy-connector-pinot/build/libs/iggy-connector-pinot-<version>.jar
```

## Installation

1. Build the connector JAR
2. Copy the JAR to Pinot's plugins directory:
   ```bash
   cp build/libs/iggy-connector-pinot-*.jar /path/to/pinot/plugins/
   ```
3. Restart Pinot servers to load the plugin

## Message Format

The connector expects JSON-formatted messages in Iggy. Example message:

```json
{
  "userId": "user123",
  "eventType": "page_view",
  "deviceId": "device456",
  "duration": 1500,
  "count": 1,
  "timestamp": 1701234567890
}
```

## Consumer Group Behavior

The connector uses Iggy consumer groups for distributed consumption:

- Each Pinot server instance joins the same consumer group
- Iggy automatically assigns partitions to group members
- Offsets are stored and managed by Iggy server
- Automatic rebalancing when consumers join/leave

## Monitoring

Monitor your Iggy connector through:

1. **Pinot Metrics**: Check ingestion lag via Pinot's realtime metrics
2. **Iggy Stats**: Query Iggy for consumer group state and offset positions
3. **Logs**: Enable DEBUG logging for detailed consumption information:
   ```
   log4j.logger.org.apache.iggy.connector.pinot=DEBUG
   ```

## Troubleshooting

### Connection Issues

If Pinot cannot connect to Iggy:

1. Verify Iggy server is running and TCP port (default 8090) is accessible
2. Check firewall rules
3. Verify credentials in configuration
4. Check Pinot logs for connection errors

### No Messages Consumed

If messages aren't being ingested:

1. Verify stream and topic IDs are correct
2. Check that messages exist in the topic: `iggy topic get <stream> <topic>`
3. Verify consumer group has joined: `iggy consumer-group get <stream> <topic> <group>`
4. Check Pinot's offset criteria configuration

### Performance Tuning

For optimal performance:

1. Increase `poll.batch.size` for higher throughput (e.g., 1000-5000)
2. Adjust `connection.pool.size` based on partition count
3. Configure appropriate `realtime.segment.flush.threshold.rows`
4. Scale Pinot servers horizontally for more parallelism

## Comparison: Iggy vs Kafka for Pinot

| Feature | Iggy Connector | Kafka Connector |
|---------|----------------|-----------------|
| Protocol | TCP | Kafka Protocol |
| Consumer Groups | Native Iggy groups | Kafka consumer groups |
| Offset Management | Server-managed | Client or server-managed |
| Partition Discovery | Dynamic via API | Dynamic via metadata |
| Authentication | Username/password | SASL/SCRAM/etc. |
| TLS Support | Yes | Yes |

## Related Documentation

- [Apache Pinot Stream Ingestion](https://docs.pinot.apache.org/manage-data/data-import/pinot-stream-ingestion)
- [Writing Custom Stream Plugins](https://docs.pinot.apache.org/developers/plugin-architecture/write-custom-plugins/write-your-stream)
- [Iggy Documentation](https://iggy.rs)
- [Iggy Java SDK](../../java-sdk/README.md)

## License

Licensed under the Apache License, Version 2.0. See LICENSE file for details.
