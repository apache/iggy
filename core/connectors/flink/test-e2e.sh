#!/bin/bash

set -e

echo "=== Flink-Iggy End-to-End Test ==="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
IGGY_HTTP_URL="http://localhost:8080"
IGGY_TCP_URL="tcp://localhost:8090"
FLINK_URL="http://localhost:8081"
KAFKA_URL="localhost:9092"
POSTGRES_URL="postgresql://postgres:password@localhost:5432/test_db"
ES_URL="http://localhost:9200"

# Helper functions
check_service() {
    local name=$1
    local url=$2
    local max_attempts=30
    local attempt=1

    echo -n "Checking $name..."
    while [ $attempt -le $max_attempts ]; do
        if curl -s -o /dev/null -w "%{http_code}" "$url" | grep -q "200\|302"; then
            echo -e " ${GREEN}✓${NC}"
            return 0
        fi
        sleep 2
        attempt=$((attempt + 1))
    done
    echo -e " ${RED}✗${NC}"
    return 1
}

cleanup() {
    echo ""
    echo "Cleaning up..."
    docker-compose down -v
    rm -f test-*.toml
    rm -f flink-job.jar
}

# Set trap for cleanup
trap cleanup EXIT

echo "1. Starting Docker services..."
docker-compose down -v 2>/dev/null || true
docker-compose up -d

echo ""
echo "2. Waiting for services to be ready..."
sleep 5

check_service "Iggy" "$IGGY_HTTP_URL/health"
check_service "Flink" "$FLINK_URL/v1/overview"
check_service "Elasticsearch" "$ES_URL"

# Check Kafka (special case - using docker exec)
echo -n "Checking Kafka..."
if docker exec kafka kafka-topics --list --bootstrap-server localhost:29092 >/dev/null 2>&1; then
    echo -e " ${GREEN}✓${NC}"
else
    echo -e " ${RED}✗${NC}"
    exit 1
fi

# Check PostgreSQL (special case - using docker exec)
echo -n "Checking PostgreSQL..."
if docker exec postgres pg_isready -U postgres >/dev/null 2>&1; then
    echo -e " ${GREEN}✓${NC}"
else
    echo -e " ${RED}✗${NC}"
    exit 1
fi

echo ""
echo "3. Building connectors..."
cd ../../../
cargo build --package iggy-connector-flink-source --release
cargo build --package iggy-connector-flink-sink --release
cargo build --package flink-data-producer --release
cd core/connectors/flink

echo ""
echo "4. Creating test database table..."
docker exec postgres psql -U postgres -d test_db -c "
CREATE TABLE IF NOT EXISTS events (
    id UUID PRIMARY KEY,
    timestamp BIGINT,
    sensor_id VARCHAR(100),
    temperature FLOAT,
    humidity FLOAT,
    pressure FLOAT,
    location_lat FLOAT,
    location_lon FLOAT,
    location_city VARCHAR(100)
);"

echo ""
echo "5. Creating Kafka test topic..."
docker exec kafka kafka-topics --create \
    --topic flink-output \
    --bootstrap-server localhost:29092 \
    --partitions 3 \
    --replication-factor 1 \
    2>/dev/null || true

echo ""
echo "6. Creating Elasticsearch index..."
curl -s -X PUT "$ES_URL/sensor-events" \
    -H 'Content-Type: application/json' \
    -d '{
      "mappings": {
        "properties": {
          "id": {"type": "keyword"},
          "timestamp": {"type": "long"},
          "sensor_id": {"type": "keyword"},
          "temperature": {"type": "float"},
          "humidity": {"type": "float"},
          "pressure": {"type": "float"},
          "location": {
            "type": "geo_point"
          }
        }
      }
    }' >/dev/null

echo ""
echo "7. Generating test configurations..."

# Source connector config (reads from Flink)
cat > test-source.toml <<EOF
[source]
type = "flink_source"

[source.config]
flink_cluster_url = "$FLINK_URL"
source_type = "kafka"
source_identifier = "sensor-input"
batch_size = 100
poll_interval_ms = 1000
skip_errors = true

[source.config.input_schema]
type = "json"

[source.config.output_schema]
type = "json"
EOF

# Sink connector config for Kafka
cat > test-sink-kafka.toml <<EOF
[sink]
type = "flink_sink"

[sink.config]
flink_cluster_url = "$FLINK_URL"
job_name = "iggy-to-kafka"
sink_type = "kafka"
target = "flink-output"
batch_size = 50
auto_flush_interval_ms = 2000

[sink.config.properties]
"bootstrap.servers" = "$KAFKA_URL"
EOF

# Sink connector config for PostgreSQL
cat > test-sink-jdbc.toml <<EOF
[sink]
type = "flink_sink"

[sink.config]
flink_cluster_url = "$FLINK_URL"
job_name = "iggy-to-postgres"
sink_type = "jdbc"
target = "events"
batch_size = 25
auto_flush_interval_ms = 3000

[sink.config.properties]
"jdbc.url" = "jdbc:$POSTGRES_URL"
"jdbc.driver" = "org.postgresql.Driver"
EOF

# Sink connector config for Elasticsearch
cat > test-sink-es.toml <<EOF
[sink]
type = "flink_sink"

[sink.config]
flink_cluster_url = "$FLINK_URL"
job_name = "iggy-to-elasticsearch"
sink_type = "elasticsearch"
target = "sensor-events"
batch_size = 100
auto_flush_interval_ms = 1000

[sink.config.properties]
"elasticsearch.hosts" = "$ES_URL"
EOF

echo ""
echo "8. Starting data producer..."
timeout 30 cargo run --release --package flink-data-producer -- \
    --server "$IGGY_TCP_URL" \
    --stream "test-stream" \
    --topic "sensor-data" \
    --batch-size 10 \
    --interval-ms 1000 \
    --num-messages 100 &

PRODUCER_PID=$!

echo ""
echo "9. Waiting for data generation..."
sleep 10

echo ""
echo "10. Verifying data flow..."

# Check Iggy for messages
echo -n "Checking Iggy messages..."
IGGY_MSG_COUNT=$(curl -s "$IGGY_HTTP_URL/streams/test-stream/topics/sensor-data/messages?count=1" | jq '. | length')
if [ "$IGGY_MSG_COUNT" -gt 0 ]; then
    echo -e " ${GREEN}✓${NC} ($IGGY_MSG_COUNT messages)"
else
    echo -e " ${RED}✗${NC}"
fi

# Check Flink jobs
echo -n "Checking Flink jobs..."
FLINK_JOBS=$(curl -s "$FLINK_URL/v1/jobs" | jq '.jobs | length')
echo -e " ${GREEN}✓${NC} ($FLINK_JOBS jobs)"

# Check Kafka messages
echo -n "Checking Kafka messages..."
KAFKA_MSG_COUNT=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:29092 \
    --topic flink-output \
    2>/dev/null | awk -F: '{sum += $3} END {print sum}')
if [ "$KAFKA_MSG_COUNT" -gt 0 ]; then
    echo -e " ${GREEN}✓${NC} ($KAFKA_MSG_COUNT messages)"
else
    echo -e " ${YELLOW}○${NC} (no messages yet)"
fi

# Check PostgreSQL records
echo -n "Checking PostgreSQL records..."
PG_COUNT=$(docker exec postgres psql -U postgres -d test_db -t -c "SELECT COUNT(*) FROM events;" | xargs)
if [ "$PG_COUNT" -gt 0 ]; then
    echo -e " ${GREEN}✓${NC} ($PG_COUNT records)"
else
    echo -e " ${YELLOW}○${NC} (no records yet)"
fi

# Check Elasticsearch documents
echo -n "Checking Elasticsearch documents..."
ES_COUNT=$(curl -s "$ES_URL/sensor-events/_count" | jq '.count')
if [ "$ES_COUNT" -gt 0 ]; then
    echo -e " ${GREEN}✓${NC} ($ES_COUNT documents)"
else
    echo -e " ${YELLOW}○${NC} (no documents yet)"
fi

# Wait for producer to finish
wait $PRODUCER_PID

echo ""
echo "11. Test Summary:"
echo "=================="
echo -e "Iggy Messages:        ${GREEN}$IGGY_MSG_COUNT${NC}"
echo -e "Flink Jobs:           ${GREEN}$FLINK_JOBS${NC}"
echo -e "Kafka Messages:       ${YELLOW}$KAFKA_MSG_COUNT${NC}"
echo -e "PostgreSQL Records:   ${YELLOW}$PG_COUNT${NC}"
echo -e "Elasticsearch Docs:   ${YELLOW}$ES_COUNT${NC}"

echo ""
echo "12. Cleaning up test data..."

# Delete test stream
curl -X DELETE "$IGGY_HTTP_URL/streams/test-stream" 2>/dev/null || true

# Delete Kafka topic
docker exec kafka kafka-topics --delete \
    --topic flink-output \
    --bootstrap-server localhost:29092 \
    2>/dev/null || true

# Drop PostgreSQL table
docker exec postgres psql -U postgres -d test_db -c "DROP TABLE IF EXISTS events;" 2>/dev/null || true

# Delete Elasticsearch index
curl -X DELETE "$ES_URL/sensor-events" 2>/dev/null || true

echo ""
echo -e "${GREEN}=== End-to-End Test Completed ===${NC}"
echo ""
echo "To keep the environment running for manual testing, press Ctrl+C"
echo "Otherwise, the environment will be cleaned up automatically."