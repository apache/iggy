#!/bin/bash

# Test script to verify iggy CLI works with the connectors

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

# Iggy CLI path
IGGY_CLI="/Users/chiradip/codes/chiradip-iggy-fork/iggy/target/release/iggy"
IGGY_HOST="localhost"
IGGY_PORT="8090"

# Helper to run iggy commands
iggy_cmd() {
    ${IGGY_CLI} --transport tcp --tcp-server-address ${IGGY_HOST}:${IGGY_PORT} --username iggy --password iggy "$@"
}

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Iggy CLI Test for Flink Connectors${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# 1. Check if iggy CLI exists
if [ ! -f "${IGGY_CLI}" ]; then
    echo -e "${YELLOW}Building iggy CLI...${NC}"
    cd /Users/chiradip/codes/chiradip-iggy-fork/iggy/core/cli
    cargo build --release
    cd -
fi

# 2. Test connection
echo -e "${GREEN}1. Testing Iggy connection...${NC}"
${IGGY_CLI} --transport tcp --tcp-server-address ${IGGY_HOST}:${IGGY_PORT} ping

# 3. Create streams and topics
echo -e "${GREEN}2. Creating test streams and topics...${NC}"
iggy_cmd stream create connector_test 2>/dev/null || echo "Stream may already exist"
iggy_cmd topic create connector_test input 1 none 2>/dev/null || echo "Topic may already exist"
iggy_cmd topic create connector_test output 1 none 2>/dev/null || echo "Topic may already exist"

# 4. List streams
echo -e "${GREEN}3. Listing streams...${NC}"
iggy_cmd stream list

# 5. Send test messages
echo -e "${GREEN}4. Sending test messages...${NC}"
for i in {1..5}; do
    echo "{\"id\": $i, \"data\": \"Test message $i from CLI\"}" | \
        iggy_cmd message send --partition-id 1 connector_test input
done
echo "Sent 5 test messages"

# 6. Poll messages
echo -e "${GREEN}5. Polling messages...${NC}"
iggy_cmd message poll --consumer cli_test --offset 0 --message-count 10 connector_test input 1

# 7. Get stream stats
echo -e "${GREEN}6. Getting stream stats...${NC}"
iggy_cmd stream get connector_test

# 8. Summary
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Summary${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "${GREEN}✓ Iggy CLI is working${NC}"
echo -e "${GREEN}✓ Can create streams and topics${NC}"
echo -e "${GREEN}✓ Can send and receive messages${NC}"
echo ""
echo "To use with Flink connectors:"
echo "1. Update test scripts to use: ${IGGY_CLI}"
echo "2. Or add to PATH: export PATH=\$PATH:/Users/chiradip/codes/chiradip-iggy-fork/iggy/target/release"
echo "3. Or create alias: alias iggy='${IGGY_CLI}'
"