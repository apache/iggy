# Here's a complete step-by-step guide to redeploy, test, and monitor the WordCountJob

1. Build the Project

   ```bash
   cd $IGGY_HOME/foreign/java
   ./gradlew :iggy-flink-examples:shadowJar --console=plain
   ```

1. Start Docker Compose (if not already running)

   ```bash
   cd $IGGY_HOME/foreign/java/external-processors/iggy-connector-flink
   docker-compose up -d
   ```

   Wait for services to be healthy (~30 seconds):

   ```bash
   sleep 30
   ```

1. Submit the Job to Flink

   ```bash
   docker exec flink-jobmanager flink run -d \
     -c org.apache.iggy.flink.example.WordCountJob \
     /opt/flink/usrlib/flink-iggy-examples.jar
   ```

   Note the JobID that's returned (e.g., Job has been submitted with JobID abc123...)

1. Verify Job is Running

   ```bash
   docker exec flink-jobmanager flink list
   ```

   You should see your job with status RUNNING.

1. Send Test Messages to Iggy

## Get authentication token

<!-- markdownlint-disable MD034 -->

```bash
TOKEN=$(curl -s -X POST http://localhost:3000/users/login \
  -H "Content-Type: application/json" \
  -d '{"username":"iggy","password":"iggy"}' | \
  grep -o '"token":"[^"]*"' | cut -d'"' -f4)
```

## Send test messages (base64 encoded payloads)

```bash
curl -s -X POST "http://localhost:3000/streams/3/topics/1/messages" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"partitioning":{"kind":"balanced","value":""},"messages":[{"payload":"aGVsbG8gd29ybGQgaGVsbG8gZmxpbms="}]}'
echo "✓ Sent: hello world hello flink"

curl -s -X POST "http://localhost:3000/streams/3/topics/1/messages" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"partitioning":{"kind":"balanced","value":""},"messages":[{"payload":"YXBhY2hlIGZsaW5rIGNvbm5lY3RvciBmb3IgaWdneQ=="}]}'
echo "✓ Sent: apache flink connector for iggy"

curl -s -X POST "http://localhost:3000/streams/3/topics/1/messages" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"partitioning":{"kind":"balanced","value":""},"messages":[{"payload":"c3RyZWFtaW5nIGRhdGEgcHJvY2Vzc2luZyB3aXRoIGZsaW5r"}]}'
echo "✓ Sent: streaming data processing with flink"
```

1. Monitor Job Processing

   Check TaskManager logs for message processing:

   ```bash
   docker logs flink-taskmanager --tail 50 | grep -E "Deserialized message"
   ```

   You should see messages being deserialized.

   Check offset advancement (ensure no infinite loop):

   ```bash
   docker logs flink-taskmanager --tail 30 | grep -E "currentOffset"
   ```

   The offset should advance and stay at the new position (not reset).

1. Check Output Topic for Word Counts

## Get fresh token

```bash
TOKEN=$(curl -s -X POST http://localhost:3000/users/login \
  -H "Content-Type: application/json" \
  -d '{"username":"iggy","password":"iggy"}' | \
  grep -o '"token":"[^"]*"' | cut -d'"' -f4)
```

## Check word-counts stream status

```bash
curl -s "http://localhost:3000/streams/word-counts" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

Look for messages_count - it should show the number of word count results.

1. Monitor via Web UIs

   - Flink Web UI: <http://localhost:8081>
     - View job details, metrics, task managers
     - Check "Records Sent" and "Records Received" counters
   - Iggy Web UI: <http://localhost:3000>
     - Monitor stream/topic stats
     - View consumer groups

1. Useful Monitoring Commands

   Check job is still running:

   ```bash
   docker exec flink-jobmanager flink list
   ```

   View JobManager logs:

   ```bash
   docker logs flink-jobmanager --tail 100
   ```

   View TaskManager logs:

   ```bash
   docker logs flink-taskmanager --tail 100
   ```

   Check Iggy server logs:

   ```bash
   docker logs iggy-server --tail 100
   ```

   Verify no infinite message loop:

## Wait 10 seconds and check record counts haven't exploded

```bash
sleep 10
docker logs flink-taskmanager --tail 20 | grep "messagesCount"
```

The messagesCount should be reasonable (0 when no new messages, or small numbers matching what you sent).

1. Stop/Cancel Job (if needed)

## List jobs to get JobID

```bash
docker exec flink-jobmanager flink list
```

## Cancel job

```bash
docker exec flink-jobmanager flink cancel <JOB_ID>
```

1. Clean Up (when done)

   ```bash
   cd $IGGY_HOME/foreign/java/external-processors/iggy-connector-flink
   docker-compose down -v  # -v removes volumes (full cleanup)
   ```

---

## Quick Test Script

Save this as test-word-count.sh:

```bash
#!/bin/bash
set -e

echo "=== WordCount Job Test ==="
echo ""
```

## Get token

```bash
echo "1. Authenticating..."
TOKEN=$(curl -s -X POST http://localhost:3000/users/login \
  -H "Content-Type: application/json" \
  -d '{"username":"iggy","password":"iggy"}' | \
  grep -o '"token":"[^"]*"' | cut -d'"' -f4)
echo "✓ Authenticated"
echo ""
```

## Send messages

```bash
echo "2. Sending test messages..."
curl -s -X POST "http://localhost:3000/streams/3/topics/1/messages" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"partitioning":{"kind":"balanced","value":""},"messages":[{"payload":"aGVsbG8gd29ybGQgaGVsbG8gZmxpbms="}]}' > /dev/null
echo "✓ Message 1 sent"

curl -s -X POST "http://localhost:3000/streams/3/topics/1/messages" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"partitioning":{"kind":"balanced","value":""},"messages":[{"payload":"YXBhY2hlIGZsaW5rIGNvbm5lY3RvciBmb3IgaWdneQ=="}]}' > /dev/null
echo "✓ Message 2 sent"

curl -s -X POST "http://localhost:3000/streams/3/topics/1/messages" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"partitioning":{"kind":"balanced","value":""},"messages":[{"payload":"c3RyZWFtaW5nIGRhdGEgcHJvY2Vzc2luZyB3aXRoIGZsaW5r"}]}' > /dev/null
echo "✓ Message 3 sent"
echo ""
```

## Wait and check

```bash
echo "3. Waiting for processing (10s)..."
sleep 10
echo ""

echo "4. Checking word counts output..."
WORD_COUNTS=$(curl -s "http://localhost:3000/streams/word-counts" \
  -H "Authorization: Bearer $TOKEN" | python3 -c "import json,sys; data=json.load(sys.stdin); print(f\"Messages: {data['messages_count']}, Size: {data['size']}\")")
echo "Output topic: $WORD_COUNTS"
echo ""

echo "5. Recent processing logs:"
docker logs flink-taskmanager --since 30s 2>&1 | grep "Deserialized message" | tail -5
echo ""

echo "✓ Test complete!"
```

Make it executable and run:

```bash
chmod +x test-word-count.sh
./test-word-count.sh
```

<!-- markdownlint-enable MD034 -->
