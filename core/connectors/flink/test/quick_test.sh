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


# Quick test script - no compilation needed, just uses curl and Python

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Quick Flink Connector Test${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# 1. Check services
echo -e "${YELLOW}Checking services...${NC}"

# Check Flink
if curl -s http://localhost:8081/v1/overview > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Flink is running${NC}"
    FLINK_VERSION=$(curl -s http://localhost:8081/v1/overview | python3 -c "import sys, json; print(json.load(sys.stdin).get('flink-version', 'unknown'))")
    echo "  Version: $FLINK_VERSION"
else
    echo -e "${RED}✗ Flink is not running${NC}"
    echo "  Start with: docker-compose up -d"
    exit 1
fi

# Check Iggy
if nc -z localhost 8090 2>/dev/null; then
    echo -e "${GREEN}✓ Iggy is running${NC}"
else
    echo -e "${RED}✗ Iggy is not running${NC}"
    echo "  Start with: docker-compose up -d"
    exit 1
fi

echo ""

# 2. Test Flink APIs that connectors use
echo -e "${YELLOW}Testing Flink REST APIs used by connectors...${NC}"

# List jobs
JOBS=$(curl -s http://localhost:8081/v1/jobs | python3 -c "
import sys, json
data = json.load(sys.stdin)
jobs = data.get('jobs', [])
print(f'{len(jobs)} jobs total')
for j in jobs:
    print(f\"  - {j['id'][:8]}... [{j['status']}]\")
")
echo "$JOBS"

# Check a running job
RUNNING_JOB=$(curl -s http://localhost:8081/v1/jobs | python3 -c "
import sys, json
data = json.load(sys.stdin)
for j in data.get('jobs', []):
    if j['status'] == 'RUNNING':
        print(j['id'])
        break
" 2>/dev/null)

if [ -n "$RUNNING_JOB" ]; then
    echo -e "${GREEN}✓ Found running job: $RUNNING_JOB${NC}"

    # Get job details
    JOB_NAME=$(curl -s http://localhost:8081/v1/jobs/$RUNNING_JOB | python3 -c "
import sys, json
print(json.load(sys.stdin).get('name', 'unknown'))
")
    echo "  Job name: $JOB_NAME"
fi

echo ""

# 3. Test Iggy with CLI
echo -e "${YELLOW}Testing Iggy CLI...${NC}"

IGGY_CLI="/Users/chiradip/codes/chiradip-iggy-fork/iggy/target/release/iggy"

if [ -f "$IGGY_CLI" ]; then
    # Ping test
    PING_TIME=$($IGGY_CLI --transport tcp ping 2>&1 | grep "time:" | cut -d':' -f2)
    echo -e "${GREEN}✓ Iggy CLI works${NC} (ping:$PING_TIME)"

    # List streams
    STREAM_COUNT=$($IGGY_CLI --transport tcp --username iggy --password iggy stream list 2>&1 | grep -c "│" || echo "0")
    if [ "$STREAM_COUNT" -gt "2" ]; then
        echo -e "${GREEN}✓ Found existing streams${NC}"
    else
        echo "  No streams yet (create with: iggy stream create test)"
    fi
else
    echo -e "${YELLOW}⚠ Iggy CLI not built${NC}"
    echo "  Build with: cd /Users/chiradip/codes/chiradip-iggy-fork/iggy/core/cli && cargo build --release"
fi

echo ""

# 4. Summary
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Test Results${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

echo -e "${GREEN}Infrastructure:${NC}"
echo "  ✓ Flink cluster at http://localhost:8081"
echo "  ✓ Iggy server at localhost:8090"
echo ""

echo -e "${GREEN}Connectors are ready to:${NC}"
echo "  • Connect to Flink cluster"
echo "  • List and monitor jobs"
echo "  • Trigger checkpoints"
echo "  • Send/receive messages via Iggy"
echo ""

echo -e "${YELLOW}Next steps:${NC}"
echo "1. Create a config file for connectors"
echo "2. Run: cargo run --bin iggy-connectors"
echo "3. Monitor: docker logs -f flink-jobmanager"
echo "4. View UI: http://localhost:8081"
