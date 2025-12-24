<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Quick Start Guide: Apache Iggy Connector for Apache Pinot

This guide will help you get started with the Iggy Pinot connector in minutes.

## Prerequisites

- Java 17 or later
- Apache Iggy server running (default: localhost:8090)
- Apache Pinot cluster running
- Gradle (for building the connector)

## Step 1: Build the Connector

From the `foreign/java` directory:

```bash
gradle :iggy-connector-pinot:build
```

This produces the JAR at:

```text
external-processors/iggy-connector-pinot/build/libs/iggy-connector-pinot-<version>.jar
```

## Step 2: Deploy to Pinot

Copy the connector JAR and its dependencies to your Pinot installation:

```bash
# Copy to Pinot plugins directory
cp build/libs/iggy-connector-pinot-*.jar $PINOT_HOME/plugins/pinot-stream-ingestion/

# Also copy the Iggy Java SDK (from your local build or Maven)
cp ../java-sdk/build/libs/iggy-*.jar $PINOT_HOME/plugins/pinot-stream-ingestion/
```

## Step 3: Prepare Iggy

Create a stream and topic in Iggy:

```bash
# Using Iggy CLI
iggy stream create analytics
iggy topic create analytics user-events --partitions 4
```

## Step 4: Create Pinot Schema

Create `user_events_schema.json`:

```json
{
  "schemaName": "user_events",
  "dimensionFieldSpecs": [
    {"name": "userId", "dataType": "STRING"},
    {"name": "eventType", "dataType": "STRING"},
    {"name": "deviceType", "dataType": "STRING"}
  ],
  "metricFieldSpecs": [
    {"name": "duration", "dataType": "LONG"}
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

Upload to Pinot:

```bash
curl -X POST "http://localhost:9000/schemas" \
  -H "Content-Type: application/json" \
  -d @user_events_schema.json
```

## Step 5: Create Pinot Realtime Table

Create `user_events_table.json`:

```json
{
  "tableName": "user_events_realtime",
  "tableType": "REALTIME",
  "segmentsConfig": {
    "timeColumnName": "timestamp",
    "timeType": "MILLISECONDS",
    "replication": "1",
    "schemaName": "user_events"
  },
  "tenants": {
    "broker": "DefaultTenant",
    "server": "DefaultTenant"
  },
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
      "stream.iggy.consumer.group": "pinot-realtime-consumer",

      "stream.iggy.poll.batch.size": "100",

      "realtime.segment.flush.threshold.rows": "10000",
      "realtime.segment.flush.threshold.time": "3600000"
    }
  },
  "metadata": {}
}
```

Upload to Pinot:

```bash
curl -X POST "http://localhost:9000/tables" \
  -H "Content-Type: application/json" \
  -d @user_events_table.json
```

## Step 6: Send Test Data to Iggy

Create a test message file `test_message.json`:

```json
{
  "userId": "user123",
  "eventType": "page_view",
  "deviceType": "desktop",
  "duration": 1500,
  "timestamp": 1701234567890
}
```

Send to Iggy (using HTTP endpoint for simplicity):

```bash
curl -X POST "http://localhost:3000/streams/analytics/topics/user-events/messages" \
  -H "Content-Type: application/json" \
  -d @test_message.json
```

Or use the Iggy CLI:

```bash
echo '{"userId":"user123","eventType":"page_view","deviceType":"desktop","duration":1500,"timestamp":1701234567890}' | \
  iggy message send analytics user-events
```

## Step 7: Query Data in Pinot

After a few seconds, query your data:

```bash
curl -X POST "http://localhost:8099/query/sql" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM user_events_realtime LIMIT 10"
  }'
```

Or use the Pinot web UI at `http://localhost:9000` and run:

```sql
SELECT userId, eventType, deviceType, duration, timestamp
FROM user_events_realtime
ORDER BY timestamp DESC
LIMIT 100;
```

## Step 8: Monitor Ingestion

Check Pinot's ingestion status:

```bash
# Check table status
curl "http://localhost:9000/tables/user_events_realtime"

# Check segment status
curl "http://localhost:9000/segments/user_events_realtime"
```

Check Iggy consumer group status:

```bash
iggy consumer-group get analytics user-events pinot-realtime-consumer
```

## Troubleshooting

### No data appearing in Pinot

1. Check Pinot server logs:

```bash
tail -f $PINOT_HOME/logs/pinot-server.log
```

1. Verify Iggy connection:

```bash
# Test TCP connectivity
telnet localhost 8090
```

1. Check consumer group membership:

```bash
iggy consumer-group get analytics user-events pinot-realtime-consumer
```

### Connection errors

- Ensure Iggy server is running: `iggy ping`
- Check firewall rules allow TCP port 8090
- Verify credentials in table config match Iggy server

### Messages in Iggy but not in Pinot

- Check Pinot logs for errors
- Verify JSON format matches schema
- Check offset criteria in stream config
- Restart Pinot servers to reload config

## Next Steps

- [Full README](README.md) - Comprehensive documentation
- [Configuration Reference](README.md#configuration-properties) - All available settings
- [Example Configurations](examples/) - Sample configs for different use cases
- [Iggy Documentation](https://iggy.rs) - Learn more about Iggy

## Production Considerations

Before deploying to production:

1. **Scale appropriately**: Match Pinot servers to Iggy partitions
2. **Tune batch sizes**: Adjust `poll.batch.size` based on message size and throughput
3. **Monitor metrics**: Set up monitoring for both Pinot and Iggy
4. **Configure retention**: Set appropriate segment retention in Pinot
5. **Enable TLS**: Use `stream.iggy.enable.tls=true` for secure connections
6. **Resource limits**: Configure appropriate JVM heap for Pinot servers
7. **Replication**: Set `replication` > 1 for fault tolerance

## Support

For issues and questions:

- GitHub Issues: [iggy/issues](https://github.com/apache/iggy/issues)
- Pinot Slack: [Apache Pinot Community](https://pinot.apache.org/community)
- Iggy Discord: [Join Discord](https://iggy.rs/discord)
