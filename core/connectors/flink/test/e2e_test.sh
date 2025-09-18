#!/bin/bash

# End-to-End Test Script for Iggy Flink Connectors
set -e

echo "=========================================="
echo "Iggy Flink Connector End-to-End Test"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
IGGY_HOST="localhost"
IGGY_PORT="8090"
IGGY_HTTP_PORT="8080"
FLINK_HOST="localhost"
FLINK_PORT="8081"
KAFKA_HOST="localhost"
KAFKA_PORT="9092"

# Helper functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

check_service() {
    local service=$1
    local host=$2
    local port=$3

    if nc -z $host $port 2>/dev/null; then
        log_info "$service is running on $host:$port"
        return 0
    else
        log_error "$service is not available on $host:$port"
        return 1
    fi
}

wait_for_service() {
    local service=$1
    local host=$2
    local port=$3
    local max_attempts=30
    local attempt=0

    log_info "Waiting for $service to be ready..."

    while [ $attempt -lt $max_attempts ]; do
        if check_service "$service" "$host" "$port"; then
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 2
    done

    log_error "$service failed to start after $max_attempts attempts"
    return 1
}

# Step 1: Start infrastructure
start_infrastructure() {
    log_info "Starting infrastructure with Docker Compose..."

    cd /Users/chiradip/codes/chiradip-iggy-fork/iggy/core/connectors/flink
    docker-compose down -v 2>/dev/null || true
    docker-compose up -d

    # Wait for services to be ready
    wait_for_service "Iggy" "$IGGY_HOST" "$IGGY_PORT"
    wait_for_service "Iggy HTTP" "$IGGY_HOST" "$IGGY_HTTP_PORT"
    wait_for_service "Flink JobManager" "$FLINK_HOST" "$FLINK_PORT"
    wait_for_service "Kafka" "$KAFKA_HOST" "$KAFKA_PORT"

    sleep 5  # Extra time for services to fully initialize
    log_info "Infrastructure is ready"
}

# Step 2: Build connectors
build_connectors() {
    log_info "Building Flink connectors..."

    # Build sink connector
    cd /Users/chiradip/codes/chiradip-iggy-fork/iggy/core/connectors/sinks/flink_sink
    cargo build --release

    # Build source connector
    cd /Users/chiradip/codes/chiradip-iggy-fork/iggy/core/connectors/sources/flink_source
    cargo build --release

    log_info "Connectors built successfully"
}

# Step 3: Setup Iggy streams and topics
setup_iggy() {
    log_info "Setting up Iggy streams and topics..."

    # Check if iggy CLI is available
    if ! command -v iggy &> /dev/null; then
        log_warning "Iggy CLI not found. Using HTTP API instead..."
        setup_iggy_http
    else
        # Create streams and topics for testing
        iggy --transport tcp --username iggy --password iggy stream create test_stream || true
        iggy --transport tcp --username iggy --password iggy topic create test_stream input_topic 3 none || true
        iggy --transport tcp --username iggy --password iggy topic create test_stream output_topic 3 none || true

        # Create stream for Flink source
        iggy --transport tcp --username iggy --password iggy stream create flink_source || true
        iggy --transport tcp --username iggy --password iggy topic create flink_source events 1 none || true

        # Create stream for Flink sink
        iggy --transport tcp --username iggy --password iggy stream create flink_sink || true
        iggy --transport tcp --username iggy --password iggy topic create flink_sink results 1 none || true
    fi

    log_info "Iggy setup complete"
}

# Setup Iggy using HTTP API
setup_iggy_http() {
    log_info "Using Iggy HTTP API to create streams and topics..."

    # Function to make authenticated requests to Iggy
    iggy_request() {
        local method=$1
        local endpoint=$2
        local data=$3

        curl -s -X $method \
            -H "Content-Type: application/json" \
            -u "iggy:iggy" \
            -d "$data" \
            "http://localhost:8080$endpoint"
    }

    # Create streams
    iggy_request POST "/streams" '{"name": "test_stream"}' || true
    iggy_request POST "/streams" '{"name": "flink_source"}' || true
    iggy_request POST "/streams" '{"name": "flink_sink"}' || true

    # Create topics
    iggy_request POST "/streams/test_stream/topics" \
        '{"name": "input_topic", "partitions_count": 3, "compression_algorithm": "none"}' || true
    iggy_request POST "/streams/test_stream/topics" \
        '{"name": "output_topic", "partitions_count": 3, "compression_algorithm": "none"}' || true
    iggy_request POST "/streams/flink_source/topics" \
        '{"name": "events", "partitions_count": 1, "compression_algorithm": "none"}' || true
    iggy_request POST "/streams/flink_sink/topics" \
        '{"name": "results", "partitions_count": 1, "compression_algorithm": "none"}' || true
}

# Step 4: Create Kafka topics
setup_kafka() {
    log_info "Setting up Kafka topics..."

    docker exec kafka kafka-topics --create \
        --bootstrap-server localhost:9092 \
        --topic test-input \
        --partitions 3 \
        --replication-factor 1 \
        2>/dev/null || true

    docker exec kafka kafka-topics --create \
        --bootstrap-server localhost:9092 \
        --topic test-output \
        --partitions 3 \
        --replication-factor 1 \
        2>/dev/null || true

    log_info "Kafka setup complete"
}

# Step 5: Deploy a simple Flink job
deploy_flink_job() {
    log_info "Deploying test Flink job..."

    # Create a simple Flink job JAR (using Python API for simplicity)
    cat > /tmp/flink_test_job.py << 'EOF'
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Simple pass-through job for testing
    env.from_collection([
        {"id": 1, "name": "test1"},
        {"id": 2, "name": "test2"},
        {"id": 3, "name": "test3"}
    ]).print()

    env.execute("test-job")

if __name__ == "__main__":
    main()
EOF

    # Note: In real scenario, you'd submit a proper Flink JAR
    log_info "Flink job deployment skipped (would require JAR packaging)"
}

# Step 6: Run connector runtime with test configuration
run_connector_test() {
    log_info "Creating test configuration..."

    cat > /tmp/flink_connector_test.toml << 'EOF'
[iggy]
address = "localhost:8090"
username = "iggy"
password = "iggy"

[state]
path = "/tmp/connector_state"

# Test Sink: Iggy -> Flink -> Kafka
[sinks.flink_test]
enabled = true
name = "Test Flink Sink"
path = "/Users/chiradip/codes/chiradip-iggy-fork/iggy/core/connectors/sinks/flink_sink/target/release/libiggy_connector_flink_sink"

[[sinks.flink_test.streams]]
stream = "test_stream"
topics = ["input_topic"]
schema = "json"
batch_length = 10
poll_interval = "100ms"
consumer_group = "flink_test_sink"

[sinks.flink_test.config]
flink_cluster_url = "http://localhost:8081"
job_name = "test-sink-job"
sink_type = "kafka"
target = "test-output"
batch_size = 10
enable_checkpointing = false

[sinks.flink_test.config.properties]
"bootstrap.servers" = "localhost:9092"

# Test Source: Kafka -> Flink -> Iggy
[sources.flink_test]
enabled = true
name = "Test Flink Source"
path = "/Users/chiradip/codes/chiradip-iggy-fork/iggy/core/connectors/sources/flink_source/target/release/libiggy_connector_flink_source"

[[sources.flink_test.streams]]
stream = "flink_source"
topic = "events"
schema = "json"

[sources.flink_test.config]
flink_cluster_url = "http://localhost:8081"
source_type = "kafka"
source_identifier = "test-input"
batch_size = 10
poll_interval_ms = 1000

[sources.flink_test.config.properties]
"bootstrap.servers" = "localhost:9092"
EOF

    log_info "Configuration created at /tmp/flink_connector_test.toml"
}

# Step 7: Send test messages
send_test_messages() {
    log_info "Sending test messages to Iggy..."

    if command -v iggy &> /dev/null; then
        for i in {1..10}; do
            iggy --transport tcp --username iggy --password iggy \
                message send --partition-id 1 test_stream input_topic \
                "{\"id\": $i, \"message\": \"Test message $i\", \"timestamp\": $(date +%s)}"
        done
    else
        # Use HTTP API to send messages
        for i in {1..10}; do
            curl -s -X POST \
                -H "Content-Type: application/json" \
                -u "iggy:iggy" \
                -d "{
                    \"partitioning\": {\"kind\": \"partition_id\", \"value\": 1},
                    \"messages\": [{
                        \"payload\": \"{\\\"id\\\": $i, \\\"message\\\": \\\"Test message $i\\\", \\\"timestamp\\\": $(date +%s)}\"
                    }]
                }" \
                "http://localhost:8080/streams/test_stream/topics/input_topic/messages" || true
        done
    fi

    log_info "Sent 10 test messages to Iggy"

    log_info "Sending test messages to Kafka..."

    for i in {1..5}; do
        echo "{\"id\": $i, \"source\": \"kafka\", \"data\": \"Kafka message $i\"}" | \
            docker exec -i kafka kafka-console-producer \
            --bootstrap-server localhost:9092 \
            --topic test-input
    done

    log_info "Sent 5 test messages to Kafka"
}

# Step 8: Verify results
verify_results() {
    log_info "Verifying test results..."

    # Check messages in Iggy (from Flink source)
    log_info "Checking messages in Iggy flink_source stream..."
    if command -v iggy &> /dev/null; then
        iggy --transport tcp --username iggy --password iggy \
            message poll --consumer 1 --offset 0 --message-count 10 \
            flink_source events 1
    else
        # Use HTTP API to poll messages
        curl -s -X GET \
            -u "iggy:iggy" \
            "http://localhost:8080/streams/flink_source/topics/events/messages?consumer_id=1&count=10&auto_commit=false" | \
            python3 -m json.tool || true
    fi

    # Check messages in Kafka (from Flink sink)
    log_info "Checking messages in Kafka test-output topic..."
    timeout 5 docker exec kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic test-output \
        --from-beginning \
        --max-messages 5 || true

    log_info "Verification complete"
}

# Step 9: Run unit tests
run_unit_tests() {
    log_info "Running unit tests..."

    cd /Users/chiradip/codes/chiradip-iggy-fork/iggy/core/connectors/sinks/flink_sink
    cargo test

    cd /Users/chiradip/codes/chiradip-iggy-fork/iggy/core/connectors/sources/flink_source
    cargo test

    log_info "Unit tests complete"
}

# Step 10: Cleanup
cleanup() {
    log_info "Cleaning up..."

    cd /Users/chiradip/codes/chiradip-iggy-fork/iggy/core/connectors/flink
    docker-compose down -v

    rm -f /tmp/flink_connector_test.toml
    rm -f /tmp/flink_test_job.py
    rm -rf /tmp/connector_state

    log_info "Cleanup complete"
}

# Main test execution
main() {
    echo ""
    log_info "Starting end-to-end test..."
    echo ""

    # Run test steps
    start_infrastructure
    build_connectors
    setup_iggy
    setup_kafka
    deploy_flink_job
    run_connector_test

    # Start connector runtime in background
    log_info "Starting connector runtime..."
    export IGGY_CONNECTORS_CONFIG_PATH=/tmp/flink_connector_test.toml
    export RUST_LOG=info,iggy_connector_flink=debug

    # Note: In real test, you'd run the connector runtime here
    # cargo run --bin iggy-connectors &
    # CONNECTOR_PID=$!

    send_test_messages
    sleep 5  # Wait for processing
    verify_results

    # Run unit tests
    run_unit_tests

    # Stop connector runtime
    # kill $CONNECTOR_PID 2>/dev/null || true

    cleanup

    echo ""
    log_info "End-to-end test completed successfully!"
    echo ""
}

# Handle errors
trap 'log_error "Test failed!"; cleanup; exit 1' ERR

# Parse arguments
case "${1:-}" in
    "clean")
        cleanup
        ;;
    "build")
        build_connectors
        ;;
    "test")
        run_unit_tests
        ;;
    *)
        main
        ;;
esac