#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}=====================================${NC}"
echo -e "${GREEN}Iggy-Pinot Integration Test${NC}"
echo -e "${GREEN}=====================================${NC}"

# Navigate to connector directory
cd "$(dirname "$0")"

# Step 1: Build JARs
echo -e "\n${YELLOW}Step 1: Building JARs...${NC}"
cd ../../
gradle :iggy-connector-pinot:jar :iggy:jar
cd external-processors/iggy-connector-pinot
echo -e "${GREEN}✓ JARs built successfully${NC}"

# Step 2: Start Docker environment
echo -e "\n${YELLOW}Step 2: Starting Docker environment...${NC}"
docker-compose down -v
docker-compose up -d
echo -e "${GREEN}✓ Docker containers starting${NC}"

# Step 3: Wait for services to be healthy
echo -e "\n${YELLOW}Step 3: Waiting for services to be healthy...${NC}"

echo -n "Waiting for Iggy... "
for i in {1..30}; do
    if curl --connect-timeout 3 --max-time 5 -s http://localhost:3000/ > /dev/null 2>&1; then
        echo -e "${GREEN}✓${NC}"
        break
    fi
    sleep 2
    echo -n "."
done

echo -n "Waiting for Pinot Controller... "
for i in {1..60}; do
    if curl --connect-timeout 3 --max-time 5 -s http://localhost:9000/health > /dev/null 2>&1; then
        echo -e "${GREEN}✓${NC}"
        break
    fi
    sleep 2
    echo -n "."
done

echo -n "Waiting for Pinot Broker... "
for i in {1..60}; do
    if curl --connect-timeout 3 --max-time 5 -s http://localhost:8099/health > /dev/null 2>&1; then
        echo -e "${GREEN}✓${NC}"
        break
    fi
    sleep 2
    echo -n "."
done

echo -n "Waiting for Pinot Server... "
for i in {1..60}; do
    if curl --connect-timeout 3 --max-time 5 -s http://localhost:8097/health > /dev/null 2>&1; then
        echo -e "${GREEN}✓${NC}"
        break
    fi
    sleep 2
    echo -n "."
done

sleep 5 # Extra time for services to stabilize

# Step 4: Login to Iggy and create stream/topic
echo -e "\n${YELLOW}Step 4: Logging in to Iggy and creating stream/topic...${NC}"

# Login and get JWT token
TOKEN=$(curl -s -X POST "http://localhost:3000/users/login" \
  -H "Content-Type: application/json" \
  -d '{"username": "iggy", "password": "iggy"}' | jq -r '.access_token.token')

if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
  echo -e "${RED}✗ Failed to get authentication token${NC}"
  exit 1
fi

echo -e "${GREEN}✓ Authenticated${NC}"

# Create stream
curl -s -X POST "http://localhost:3000/streams" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"stream_id": 1, "name": "test-stream"}' \
  && echo -e "${GREEN}✓ Stream created${NC}" || echo -e "${RED}✗ Stream creation failed (may already exist)${NC}"

# Create topic
TOPIC_RESPONSE=$(curl -s -X POST "http://localhost:3000/streams/test-stream/topics" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"topic_id": 1, "name": "test-events", "partitions_count": 2, "compression_algorithm": "none", "message_expiry": 0, "max_topic_size": 0}')

if echo "$TOPIC_RESPONSE" | grep -q '"id"'; then
  echo -e "${GREEN}✓ Topic created${NC}"
else
  echo -e "${RED}✗ Topic creation failed: $TOPIC_RESPONSE${NC}"
  exit 1
fi

# Create consumer group (topic-scoped, not stream-scoped)
curl -s -X POST "http://localhost:3000/streams/test-stream/topics/test-events/consumer-groups" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "pinot-integration-test"}' \
  && echo -e "${GREEN}✓ Consumer group created${NC}" || echo -e "${YELLOW}Note: Consumer group may already exist${NC}"

# Step 5: Create Pinot schema
echo -e "\n${YELLOW}Step 5: Creating Pinot schema...${NC}"
curl -X POST "http://localhost:9000/schemas" \
  -H "Content-Type: application/json" \
  -d @deployment/schema.json \
  && echo -e "${GREEN}✓ Schema created${NC}" || echo -e "${RED}✗ Schema creation failed${NC}"

# Step 6: Create Pinot table
echo -e "\n${YELLOW}Step 6: Creating Pinot realtime table...${NC}"
TABLE_RESPONSE=$(curl -s -X POST "http://localhost:9000/tables" \
  -H "Content-Type: application/json" \
  -d @deployment/table.json)

if echo "$TABLE_RESPONSE" | grep -q '"status":"Table test_events_REALTIME successfully added"'; then
  echo -e "${GREEN}✓ Table created${NC}"
elif echo "$TABLE_RESPONSE" | grep -q '"code":500'; then
  echo -e "${RED}✗ Table creation failed${NC}"
  echo "$TABLE_RESPONSE" | jq '.'
  exit 1
else
  echo -e "${GREEN}✓ Table created${NC}"
fi

sleep 5 # Let table initialize

# Step 7: Send test messages to Iggy
echo -e "\n${YELLOW}Step 7: Sending test messages to Iggy...${NC}"

# Partition value for partition 0 (4-byte little-endian, base64 encoded)
PARTITION_VALUE=$(printf '\x00\x00\x00\x00' | base64)

for i in {1..10}; do
    TIMESTAMP=$(($(date +%s) * 1000))
    MESSAGE=$(cat <<EOF
{
  "userId": "user$i",
  "eventType": "test_event",
  "deviceType": "desktop",
  "duration": $((i * 100)),
  "timestamp": $TIMESTAMP
}
EOF
)

    curl -X POST "http://localhost:3000/streams/test-stream/topics/test-events/messages" \
      -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d "{\"partitioning\": {\"kind\": \"partition_id\", \"value\": \"$PARTITION_VALUE\"}, \"messages\": [{\"payload\": \"$(echo "$MESSAGE" | base64)\"}]}" \
      > /dev/null 2>&1
    echo -e "${GREEN}✓ Message $i sent${NC}"
    sleep 1
done

# Step 8: Wait for ingestion
echo -e "\n${YELLOW}Step 8: Waiting for Pinot to ingest messages...${NC}"
sleep 15

# Step 9: Query Pinot and verify data
echo -e "\n${YELLOW}Step 9: Querying Pinot for ingested data...${NC}"

QUERY_RESULT=$(curl -s -X POST "http://localhost:8099/query/sql" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT COUNT(*) FROM test_events_REALTIME"}')

echo "Query Result:"
echo "$QUERY_RESULT" | jq '.'

# Extract count from result
COUNT=$(echo "$QUERY_RESULT" | jq -r '.resultTable.rows[0][0]' 2>/dev/null || echo "0")

if [ "$COUNT" -gt "0" ]; then
    echo -e "\n${GREEN}=====================================${NC}"
    echo -e "${GREEN}✓ Integration Test PASSED!${NC}"
    echo -e "${GREEN}Successfully ingested $COUNT messages${NC}"
    echo -e "${GREEN}=====================================${NC}"

    # Show sample data
    echo -e "\n${YELLOW}Sample data:${NC}"
    curl -s -X POST "http://localhost:8099/query/sql" \
      -H "Content-Type: application/json" \
      -d '{"sql": "SELECT * FROM test_events_REALTIME LIMIT 5"}' | jq '.'

    EXIT_CODE=0
else
    echo -e "\n${RED}=====================================${NC}"
    echo -e "${RED}✗ Integration Test FAILED!${NC}"
    echo -e "${RED}No messages ingested${NC}"
    echo -e "${RED}=====================================${NC}"

    # Show logs for debugging
    echo -e "\n${YELLOW}Pinot Server logs:${NC}"
    docker logs pinot-server --tail 50

    EXIT_CODE=1
fi

# Cleanup option
echo -e "\n${YELLOW}To stop the environment: docker-compose down -v${NC}"
echo -e "${YELLOW}To view logs: docker-compose logs -f${NC}"

exit $EXIT_CODE
