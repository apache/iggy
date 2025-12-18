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

# Integration Testing Guide - Iggy Pinot Connector

## Prerequisites

- Docker and Docker Compose installed
- At least 4GB RAM available for Docker
- Ports available: 8090, 3000, 8080 (Iggy), 9000, 8099, 8098, 2181 (Pinot/Zookeeper)

## Quick Start

The connector includes an automated integration test script that:

1. Builds the connector JARs
2. Starts Iggy and Pinot in Docker (using official Apache images)
3. Creates Iggy stream and topic
4. Deploys the connector to Pinot
5. Creates Pinot schema and table
6. Sends test messages to Iggy
7. Verifies data is ingested into Pinot

### Run Automated Integration Test

```bash
cd external-processors/iggy-connector-pinot
./integration-test.sh
```

## Docker Images Used

All images are from official Apache repositories:

- **Iggy**: `apache/iggy:latest`
- **Pinot**: `apachepinot/pinot:latest`
- **Zookeeper**: `zookeeper:3.9`

## Manual Testing Steps

If you prefer to test manually:

### 1. Start Services

```bash
docker-compose up -d
```

### 2. Verify Services

```bash
# Check Iggy
curl http://localhost:3000/

# Check Pinot Controller
curl http://localhost:9000/health

# Check Pinot Broker
curl http://localhost:8099/health

# Check Pinot Server
curl http://localhost:8098/health
```

### 3. Create Iggy Stream and Topic

```bash
# Create stream
curl -X POST "http://localhost:3000/streams" \
  -H "Content-Type: application/json" \
  -d '{"stream_id": 1, "name": "test-stream"}'

# Create topic with 2 partitions
curl -X POST "http://localhost:3000/streams/test-stream/topics" \
  -H "Content-Type: application/json" \
  -d '{"topic_id": 1, "name": "test-events", "partitions_count": 2, "message_expiry": 0}'
```

### 4. Deploy Connector to Pinot

```bash
# Copy connector JAR
docker cp build/libs/iggy-connector-pinot-0.6.0.jar pinot-server:/opt/pinot/plugins/iggy-connector/

# Copy Iggy SDK JAR
docker cp ../../java-sdk/build/libs/iggy-0.6.0.jar pinot-server:/opt/pinot/plugins/iggy-connector/

# Restart Pinot server to load plugin
docker restart pinot-server

# Wait for server to be healthy
sleep 15
```

### 5. Create Pinot Schema

```bash
curl -X POST "http://localhost:9000/schemas" \
  -H "Content-Type: application/json" \
  -d @deployment/schema.json
```

### 6. Create Pinot Realtime Table

```bash
curl -X POST "http://localhost:9000/tables" \
  -H "Content-Type: application/json" \
  -d @deployment/table.json
```

### 7. Send Test Messages to Iggy

```bash
# Send a test message
TIMESTAMP=$(date +%s)000
MESSAGE='{"userId":"user1","eventType":"test_event","deviceType":"desktop","duration":100,"timestamp":'$TIMESTAMP'}'
ENCODED=$(echo -n "$MESSAGE" | base64)

curl -X POST "http://localhost:3000/streams/test-stream/topics/test-events/messages" \
  -H "Content-Type: application/json" \
  -d "{\"messages\": [{\"payload\": \"$ENCODED\"}]}"
```

### 8. Verify Data in Pinot

```bash
# Count messages
curl -X POST "http://localhost:8099/query/sql" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT COUNT(*) FROM test_events_realtime"}' | jq '.'

# Query sample data
curl -X POST "http://localhost:8099/query/sql" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM test_events_realtime LIMIT 10"}' | jq '.'
```

## Test Scenarios

### Scenario 1: Basic Ingestion (Included in automated test)

- **Messages**: 10 test events
- **Expected**: All 10 messages ingested into Pinot
- **Verification**: `SELECT COUNT(*) FROM test_events_realtime` returns 10

### Scenario 2: High Throughput

```bash
# Send 1000 messages rapidly
for i in {1..1000}; do
  TIMESTAMP=$(date +%s)000
  MESSAGE='{"userId":"user'$i'","eventType":"load_test","deviceType":"mobile","duration":'$i',"timestamp":'$TIMESTAMP'}'
  ENCODED=$(echo -n "$MESSAGE" | base64)

  curl -s -X POST "http://localhost:3000/streams/test-stream/topics/test-events/messages" \
    -H "Content-Type: application/json" \
    -d "{\"messages\": [{\"payload\": \"$ENCODED\"}]}" > /dev/null
done

# Wait for ingestion
sleep 30

# Verify count
curl -X POST "http://localhost:8099/query/sql" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT COUNT(*) FROM test_events_realtime"}' | jq '.resultTable.rows[0][0]'
```

### Scenario 3: Multiple Partitions

The connector automatically handles partition distribution:

- Topic created with 2 partitions
- Pinot creates one consumer per partition
- Messages are distributed across partitions by Iggy

Verify partition consumption:

```bash
# Check Pinot segment metadata
curl "http://localhost:9000/segments/test_events_realtime" | jq '.'
```

### Scenario 4: Consumer Group Offset Management

Test that offsets are properly managed:

```bash
# Send 10 messages
for i in {1..10}; do
  # Send message (as above)
done

# Wait for ingestion
sleep 10

# Restart Pinot server
docker restart pinot-server
sleep 20

# Send 10 more messages
for i in {11..20}; do
  # Send message (as above)
done

# Verify total is 20 (not 30, proving offset management works)
curl -X POST "http://localhost:8099/query/sql" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT COUNT(*) FROM test_events_realtime"}' | jq '.resultTable.rows[0][0]'
```

### Scenario 5: Large Messages

Test with large payloads (up to 10MB):

```bash
# Create a large payload
LARGE_DATA=$(python3 -c "print('x' * 1000000)")  # 1MB
MESSAGE='{"userId":"user_large","eventType":"large_event","deviceType":"server","duration":999,"timestamp":'$(date +%s)000',"data":"'$LARGE_DATA'"}'
ENCODED=$(echo -n "$MESSAGE" | base64)

curl -X POST "http://localhost:3000/streams/test-stream/topics/test-events/messages" \
  -H "Content-Type: application/json" \
  -d "{\"messages\": [{\"payload\": \"$ENCODED\"}]}"
```

## Monitoring and Debugging

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f pinot-server
docker-compose logs -f iggy

# Tail recent logs
docker logs pinot-server --tail 100
```

### Check Connector Status

```bash
# List loaded plugins
docker exec pinot-server ls -la /opt/pinot/plugins/iggy-connector/

# Check classpath
docker exec pinot-server bash -c "echo \$CLASSPATH" | grep iggy
```

### Verify Consumer Groups in Iggy

```bash
# Using Iggy HTTP API
curl "http://localhost:3000/streams/test-stream/topics/test-events/consumer-groups"

# Check specific consumer group
curl "http://localhost:3000/streams/test-stream/topics/test-events/consumer-groups/pinot-integration-test"
```

### Pinot Query Console

Access the Pinot web UI at: `http://localhost:9000`

Navigate to:

- **Query Console**: Run SQL queries
- **Cluster Manager**: View table status
- **Segment Status**: Check realtime segments

## Performance Testing

### Measure Ingestion Latency

```bash
# Send timestamped message
SEND_TIME=$(date +%s%3N)
MESSAGE='{"userId":"latency_test","eventType":"test","deviceType":"test","duration":100,"timestamp":'$SEND_TIME'}'
ENCODED=$(echo -n "$MESSAGE" | base64)

curl -X POST "http://localhost:3000/streams/test-stream/topics/test-events/messages" \
  -H "Content-Type: application/json" \
  -d "{\"messages\": [{\"payload\": \"$ENCODED\"}]}"

# Wait briefly
sleep 2

# Query and compare timestamps
QUERY_TIME=$(date +%s%3N)
curl -X POST "http://localhost:8099/query/sql" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT timestamp FROM test_events_realtime WHERE userId = '"'"'latency_test'"'"' LIMIT 1"}' | jq -r '.resultTable.rows[0][0]'

echo "End-to-end latency: $(($QUERY_TIME - $SEND_TIME)) ms"
```

### Measure Throughput

```bash
#!/bin/bash
START=$(date +%s)

for i in {1..10000}; do
  TIMESTAMP=$(date +%s)000
  MESSAGE='{"userId":"user'$i'","eventType":"throughput_test","deviceType":"test","duration":'$i',"timestamp":'$TIMESTAMP'}'
  ENCODED=$(echo -n "$MESSAGE" | base64)

  curl -s -X POST "http://localhost:3000/streams/test-stream/topics/test-events/messages" \
    -H "Content-Type: application/json" \
    -d "{\"messages\": [{\"payload\": \"$ENCODED\"}]}" > /dev/null
done

END=$(date +%s)
DURATION=$((END - START))

echo "Sent 10,000 messages in $DURATION seconds"
echo "Throughput: $((10000 / DURATION)) msg/sec"
```

## Troubleshooting

### No Messages in Pinot

1. **Check Pinot server logs**:

```bash
docker logs pinot-server --tail 200 | grep -i error
```

1. **Verify connector is loaded**:

```bash
docker exec pinot-server ls /opt/pinot/plugins/iggy-connector/
```

1. **Check Iggy has messages**:

```bash
curl "http://localhost:3000/streams/test-stream/topics/test-events/messages"
```

1. **Verify table configuration**:

```bash
curl "http://localhost:9000/tables/test_events_realtime" | jq '.REALTIME.tableIndexConfig.streamConfigs'
```

### Connection Errors

If Pinot cannot connect to Iggy:

1. **Verify network connectivity**:

```bash
docker exec pinot-server ping -c 3 iggy
docker exec pinot-server curl http://iggy:3000/
```

1. **Check Iggy is listening on TCP**:

```bash
docker exec iggy netstat -ln | grep 8090
```

1. **Verify credentials**:

Check `deployment/table.json` has correct username/password

### ClassNotFoundException

If you see `ClassNotFoundException` for Iggy connector classes:

1. Verify JARs are in the plugins directory
2. Restart Pinot server after deploying JARs
3. Check JAR permissions (should be readable)

## Cleanup

```bash
# Stop all services
docker-compose down

# Remove volumes (data)
docker-compose down -v

# Remove images
docker-compose down --rmi all
```

## Expected Results

### ✅ Successful Integration Test

```text
=====================================
✓ Integration Test PASSED!
Successfully ingested 10 messages
=====================================

Sample data:
{
  "resultTable": {
    "dataSchema": {...},
    "rows": [
      ["user1", "test_event", "desktop", 100, 1701234567890],
      ["user2", "test_event", "desktop", 200, 1701234567891],
      ...
    ]
  }
}
```

### Performance Expectations

Based on unit tests and design:

- **Throughput**: 1000+ msg/sec sustained
- **Latency**: < 2 seconds end-to-end (Iggy → Pinot → Query)
- **Memory**: ~2x message size overhead
- **Partition Handling**: Linear scaling with partition count

## CI/CD Integration

The integration test can be automated in CI/CD:

```yaml
# GitHub Actions example
- name: Run Integration Test
  run: |
    cd external-processors/iggy-connector-pinot
    ./integration-test.sh
  timeout-minutes: 10
```

## Next Steps

After successful integration testing:

1. **Performance Tuning**: Adjust batch sizes and connection pools
2. **Production Deployment**: Use external Zookeeper cluster
3. **Monitoring**: Set up Prometheus/Grafana for metrics
4. **High Availability**: Deploy multiple Pinot servers
5. **Security**: Enable TLS for Iggy connections

## Support

For issues during integration testing:

- Check [TEST_REPORT.md](TEST_REPORT.md) for performance benchmarks
- Review [README.md](README.md) for configuration details
- See [QUICKSTART.md](QUICKSTART.md) for setup guidance
- Check Docker logs for error messages
