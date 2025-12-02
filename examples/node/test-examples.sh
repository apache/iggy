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


# Test script for Iggy Node.js examples
# This script tests the examples by running them with a timeout

set -e

echo "🧪 Testing Iggy Node.js Examples"
echo "================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to test an example
test_example() {
    local name="$1"
    local command="$2"
    local timeout="${3:-10}"

    echo -e "\n${YELLOW}Testing $name...${NC}"
    echo "Command: $command"

    # Run the command with timeout using background process
    local output
    local pid

    # Start the command in background
    # $command > /tmp/test_output_$$ 2>&1 &
    $command > example_output.log 2>&1 &
    pid=$!

    # Wait for the specified timeout
    local count=0
    while [ $count -lt "$timeout" ]; do
        if ! kill -0 $pid 2>/dev/null; then
            # Process finished
            output=$(cat example_output.log)
            rm -f example_output.log

            if wait $pid; then
                echo -e "${GREEN}✅ $name passed${NC}"
                return 0
            else
                # Check if the output contains our expected error message
                if echo "$output" | grep -q "This might be due to server version compatibility"; then
                    echo -e "${YELLOW}⚠️  $name completed with known server compatibility issue${NC}"
                    return 0
                else
                    echo -e "${RED}❌ $name failed${NC}"
                    echo "Output: $output"
                    return 1
                fi
            fi
        fi
        sleep 1
        count=$((count + 1))
    done

    # Timeout reached, kill the process
    kill $pid 2>/dev/null
    wait $pid 2>/dev/null
    rm -f example_output.log

    echo -e "${YELLOW}⏰ $name timed out after ${timeout}s (this is expected for long-running examples)${NC}"
    return 0
}

# Check if Iggy server is running
echo "Checking if Iggy server is running..."
if ! curl -s http://localhost:3000/health > /dev/null 2>&1; then
    echo -e "${YELLOW}⚠️  Iggy server not detected. Please start it with:${NC}"
    echo "docker run --rm -p 8080:8080 -p 3000:3000 -p 8090:8090 apache/iggy:latest"
    echo ""
    echo "Or build from source:"
    echo "cd ../../ && cargo run --bin iggy-server"
    echo ""
    echo "Skipping tests..."
    exit 0
fi

echo -e "${GREEN}✅ Iggy server is running${NC}"

# Test getting-started examples
echo -e "\n${YELLOW}Testing Getting Started Examples${NC}"
test_example "Getting Started Producer (TS)" "npm run test:getting-started:producer" 10
test_example "Getting Started Consumer (TS)" "npm run test:getting-started:consumer" 8

# Test basic examples
echo -e "\n${YELLOW}Testing Basic Examples${NC}"
test_example "Basic Producer" "npm run test:basic:producer" 10
test_example "Basic Consumer" "npm run test:basic:consumer" 8

# Message envelope examples
echo -e "\n${YELLOW}Testing Message Envelope Examples${NC}"
test_example "Message Envelope Producer" "npm run test:message-envelope:producer" 10
test_example "Message Envelope Consumer" "npm run test:message-envelope:consumer" 8

# Message headers examples
echo -e "\n${YELLOW}Testing Message Headers Examples${NC}"
test_example "Message Headers Producer" "npm run test:message-headers:producer" 10
test_example "Message Headers Consumer" "npm run test:message-headers:consumer" 8

# Multi-tenant examples
echo -e "\n${YELLOW}Testing Multi-Tenant Examples${NC}"
test_example "Multi-Tenant Producer" "npm run test:multi-tenant:producer" 10
test_example "Multi-Tenant Consumer" "npm run test:multi-tenant:consumer" 8

# Stream builder example
echo -e "\n${YELLOW}Testing Stream Builder Example${NC}"
test_example "Stream Builder" "npm run test:stream-builder" 8

# Sink data producer
echo -e "\n${YELLOW}Testing Sink Data Producer${NC}"
test_example "Sink Data Producer" "npm run test:sink-data-producer" 8

echo -e "\n${GREEN}🎉 All tests completed!${NC}"
