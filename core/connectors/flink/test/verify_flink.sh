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


# Script to verify Flink connector activity

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

FLINK_HOST="${FLINK_HOST:-localhost}"
FLINK_PORT="${FLINK_PORT:-8081}"
FLINK_URL="http://$FLINK_HOST:$FLINK_PORT"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Flink Connector Verification Script${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Function to make Flink API calls
flink_api() {
    local endpoint=$1
    curl -s "${FLINK_URL}${endpoint}" 2>/dev/null || echo "{}"
}

# Function to pretty print JSON
pretty_json() {
    python3 -m json.tool 2>/dev/null || cat
}

# 1. Check Flink cluster status
check_cluster_status() {
    echo -e "${YELLOW}1. Checking Flink Cluster Status...${NC}"

    local overview=$(flink_api "/v1/overview")

    if [ -z "$overview" ] || [ "$overview" = "{}" ]; then
        echo -e "${RED}   ❌ Cannot connect to Flink at $FLINK_URL${NC}"
        echo -e "${RED}   Make sure Flink is running: docker-compose up -d${NC}"
        return 1
    fi

    local version=$(echo "$overview" | python3 -c "import sys, json; print(json.load(sys.stdin).get('flink-version', 'unknown'))" 2>/dev/null)
    local running=$(echo "$overview" | python3 -c "import sys, json; print(json.load(sys.stdin).get('jobs-running', 0))" 2>/dev/null)
    local taskmanagers=$(echo "$overview" | python3 -c "import sys, json; print(json.load(sys.stdin).get('taskmanagers', 0))" 2>/dev/null)
    local slots_total=$(echo "$overview" | python3 -c "import sys, json; print(json.load(sys.stdin).get('slots-total', 0))" 2>/dev/null)
    local slots_available=$(echo "$overview" | python3 -c "import sys, json; print(json.load(sys.stdin).get('slots-available', 0))" 2>/dev/null)

    echo -e "${GREEN}   ✓ Flink Version: $version${NC}"
    echo -e "${GREEN}   ✓ TaskManagers: $taskmanagers${NC}"
    echo -e "${GREEN}   ✓ Slots: $slots_available/$slots_total available${NC}"
    echo -e "${GREEN}   ✓ Running Jobs: $running${NC}"
    echo ""
}

# 2. List all jobs
list_jobs() {
    echo -e "${YELLOW}2. Listing All Flink Jobs...${NC}"

    local jobs=$(flink_api "/v1/jobs")
    local job_count=$(echo "$jobs" | python3 -c "import sys, json; print(len(json.load(sys.stdin).get('jobs', [])))" 2>/dev/null || echo "0")

    if [ "$job_count" = "0" ]; then
        echo -e "${YELLOW}   No jobs found in Flink${NC}"
        echo ""
        return
    fi

    echo -e "${GREEN}   Found $job_count jobs:${NC}"
    echo "$jobs" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for job in data.get('jobs', []):
    state_color = '\033[0;32m' if job['state'] == 'RUNNING' else '\033[1;33m' if job['state'] == 'FINISHED' else '\033[0;31m'
    print(f\"   - Job ID: {job['id'][:8]}... | State: {state_color}{job['state']}\033[0m\")
" 2>/dev/null || echo "$jobs" | pretty_json
    echo ""
}

# 3. Check for uploaded JARs
check_uploaded_jars() {
    echo -e "${YELLOW}3. Checking Uploaded JARs...${NC}"

    local jars=$(flink_api "/v1/jars")
    local jar_count=$(echo "$jars" | python3 -c "import sys, json; print(len(json.load(sys.stdin).get('files', [])))" 2>/dev/null || echo "0")

    if [ "$jar_count" = "0" ]; then
        echo -e "${YELLOW}   No JAR files uploaded to Flink${NC}"
        echo -e "${YELLOW}   (Connectors haven't uploaded any JARs yet)${NC}"
    else
        echo -e "${GREEN}   Found $jar_count uploaded JARs:${NC}"
        echo "$jars" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for jar in data.get('files', []):
    print(f\"   - {jar.get('name', 'unknown')} ({jar.get('uploaded', 'unknown')})\")
" 2>/dev/null || echo "$jars" | pretty_json
    fi
    echo ""
}

# 4. Get detailed job information for running jobs
check_running_jobs() {
    echo -e "${YELLOW}4. Checking Running Job Details...${NC}"

    local jobs=$(flink_api "/v1/jobs")
    local running_jobs=$(echo "$jobs" | python3 -c "
import sys, json
data = json.load(sys.stdin)
running = [j['id'] for j in data.get('jobs', []) if j['state'] == 'RUNNING']
print(' '.join(running))
" 2>/dev/null)

    if [ -z "$running_jobs" ]; then
        echo -e "${YELLOW}   No running jobs to inspect${NC}"
        echo ""
        return
    fi

    for job_id in $running_jobs; do
        echo -e "${BLUE}   Job $job_id:${NC}"

        # Get job details
        local job_detail=$(flink_api "/v1/jobs/$job_id")
        local job_name=$(echo "$job_detail" | python3 -c "import sys, json; print(json.load(sys.stdin).get('name', 'unknown'))" 2>/dev/null)

        echo -e "${GREEN}     Name: $job_name${NC}"

        # Get vertices (operators) in the job
        local vertices=$(echo "$job_detail" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for v in data.get('vertices', []):
    status = v.get('status', 'UNKNOWN')
    color = '\033[0;32m' if status == 'RUNNING' else '\033[1;33m'
    print(f\"     - {v.get('name', 'unknown')}: {color}{status}\033[0m\")
" 2>/dev/null)

        if [ -n "$vertices" ]; then
            echo -e "${GREEN}     Vertices:${NC}"
            echo "$vertices"
        fi

        # Check for checkpoints
        local checkpoints=$(flink_api "/v1/jobs/$job_id/checkpoints")
        local latest_checkpoint=$(echo "$checkpoints" | python3 -c "
import sys, json
data = json.load(sys.stdin)
latest = data.get('latest', {}).get('completed', {})
if latest:
    print(f\"     Latest Checkpoint: ID={latest.get('id', 'N/A')}, Size={latest.get('state_size', 0)} bytes\")
else:
    print('     No checkpoints found')
" 2>/dev/null)

        echo -e "${GREEN}$latest_checkpoint${NC}"
        echo ""
    done
}

# 5. Check metrics for data flow
check_metrics() {
    echo -e "${YELLOW}5. Checking Data Flow Metrics...${NC}"

    local jobs=$(flink_api "/v1/jobs")
    local running_jobs=$(echo "$jobs" | python3 -c "
import sys, json
data = json.load(sys.stdin)
running = [j['id'] for j in data.get('jobs', []) if j['state'] == 'RUNNING']
print(' '.join(running))
" 2>/dev/null)

    if [ -z "$running_jobs" ]; then
        echo -e "${YELLOW}   No running jobs to get metrics from${NC}"
        echo ""
        return
    fi

    for job_id in $running_jobs; do
        # Get job metrics
        local metrics=$(flink_api "/v1/jobs/$job_id/metrics?get=uptime,numRestarts,records-received,records-sent")

        if [ "$metrics" != "{}" ] && [ -n "$metrics" ]; then
            echo -e "${BLUE}   Job $job_id metrics:${NC}"
            echo "$metrics" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for metric in data:
    print(f\"     - {metric.get('id', 'unknown')}: {metric.get('value', 'N/A')}\")
" 2>/dev/null || echo "     Unable to parse metrics"
        fi
    done
    echo ""
}

# 6. Check for connector-specific activity
check_connector_activity() {
    echo -e "${YELLOW}6. Looking for Connector Activity...${NC}"

    # Check Flink logs for connector-related messages
    if command -v docker &> /dev/null; then
        echo -e "${GREEN}   Checking Flink logs for connector activity:${NC}"

        # Check JobManager logs
        local jm_logs=$(docker logs flink-jobmanager 2>&1 | tail -20 | grep -i "iggy\|connector\|sink\|source" || true)
        if [ -n "$jm_logs" ]; then
            echo -e "${GREEN}   JobManager logs:${NC}"
            echo "$jm_logs" | head -5
        else
            echo -e "${YELLOW}   No connector-related messages in JobManager logs${NC}"
        fi

        # Check TaskManager logs
        local tm_logs=$(docker logs flink-taskmanager 2>&1 | tail -20 | grep -i "iggy\|connector\|sink\|source" || true)
        if [ -n "$tm_logs" ]; then
            echo -e "${GREEN}   TaskManager logs:${NC}"
            echo "$tm_logs" | head -5
        else
            echo -e "${YELLOW}   No connector-related messages in TaskManager logs${NC}"
        fi
    else
        echo -e "${YELLOW}   Docker not available, skipping log check${NC}"
    fi
    echo ""
}

# 7. Test connector endpoints
test_connector_endpoints() {
    echo -e "${YELLOW}7. Testing Connector Endpoints...${NC}"

    # Test if connectors created any custom endpoints
    local endpoints=("/v1/sources/subscribe" "/v1/sink/status" "/v1/kafka/produce/test")

    for endpoint in "${endpoints[@]}"; do
        local response=$(curl -s -o /dev/null -w "%{http_code}" "${FLINK_URL}${endpoint}" 2>/dev/null || echo "000")
        if [ "$response" != "000" ] && [ "$response" != "404" ]; then
            echo -e "${GREEN}   ✓ Found endpoint: $endpoint (HTTP $response)${NC}"
        fi
    done

    echo -e "${YELLOW}   Note: Flink doesn't expose data endpoints by default${NC}"
    echo -e "${YELLOW}   Data transfer requires custom Flink jobs or queryable state${NC}"
    echo ""
}

# 8. Summary and recommendations
show_summary() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Summary & Recommendations${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""

    echo -e "${GREEN}To see connector activity in Flink:${NC}"
    echo ""
    echo "1. Submit a Flink job that reads/writes data:"
    echo "   ${BLUE}docker exec flink-jobmanager flink run /opt/flink/examples/streaming/WordCount.jar${NC}"
    echo ""
    echo "2. Monitor real-time logs:"
    echo "   ${BLUE}docker logs -f flink-jobmanager${NC}"
    echo "   ${BLUE}docker logs -f flink-taskmanager${NC}"
    echo ""
    echo "3. Access Flink Web UI:"
    echo "   ${BLUE}open http://localhost:8081${NC}"
    echo ""
    echo "4. Run connector with debug logging:"
    echo "   ${BLUE}RUST_LOG=debug,iggy_connector_flink=trace cargo run --bin iggy-connectors${NC}"
    echo ""
    echo -e "${YELLOW}Note: The connectors use Flink REST API to:${NC}"
    echo "  - Monitor cluster health"
    echo "  - List and track jobs"
    echo "  - Upload JARs and submit jobs"
    echo "  - Trigger checkpoints/savepoints"
    echo ""
    echo -e "${YELLOW}For actual data transfer, you need:${NC}"
    echo "  - A Flink job with queryable state"
    echo "  - Or a custom sink that exposes data"
    echo "  - Or use intermediate storage (Kafka/Redis)"
}

# Main execution
main() {
    check_cluster_status || exit 1
    list_jobs
    check_uploaded_jars
    check_running_jobs
    check_metrics
    check_connector_activity
    test_connector_endpoints
    show_summary
}

# Handle arguments
case "${1:-}" in
    cluster)
        check_cluster_status
        ;;
    jobs)
        list_jobs
        check_running_jobs
        ;;
    jars)
        check_uploaded_jars
        ;;
    metrics)
        check_metrics
        ;;
    logs)
        check_connector_activity
        ;;
    *)
        main
        ;;
esac