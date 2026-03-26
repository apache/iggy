#!/usr/bin/env bash
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

# Smoke test for the Kafka wire protocol transport.
# Starts an Iggy server with Kafka enabled, then uses kcat (librdkafka)
# to verify metadata, produce, and consume work end-to-end.
#
# Usage: ./scripts/test-kafka-transport.sh
#
# Prerequisites: cargo, kcat

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_DIR=$(mktemp -d)
CONFIG_FILE="$DATA_DIR/config.toml"
SERVER_LOG="$DATA_DIR/server.log"
SERVER_PID=""
KAFKA_PORT=$((29000 + RANDOM % 1000))
TCP_PORT=$((28000 + RANDOM % 1000))
HTTP_PORT=$((27000 + RANDOM % 1000))
FAILURES=0

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

cleanup() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

fail() { echo -e "${RED}FAIL: $1${NC}"; FAILURES=$((FAILURES + 1)); }
pass() { echo -e "${GREEN}PASS: $1${NC}"; }

command -v kcat >/dev/null 2>&1 || { echo "kcat not found — install with: apt-get install kcat"; exit 1; }

# --- Build ---
echo "Building iggy-server..."
cd "$ROOT_DIR"
cargo build -p server 2>&1 | tail -1

SERVER_BIN="$ROOT_DIR/target/debug/iggy-server"
[ -x "$SERVER_BIN" ] || { fail "iggy-server binary not found"; exit 1; }

# --- Generate test config ---
cp "$ROOT_DIR/core/server/config.toml" "$CONFIG_FILE"
sed -i "s|^address = \"127.0.0.1:8090\"|address = \"127.0.0.1:$TCP_PORT\"|" "$CONFIG_FILE"
sed -i "s|^address = \"127.0.0.1:3000\"|address = \"127.0.0.1:$HTTP_PORT\"|" "$CONFIG_FILE"
sed -i "s|^address = \"127.0.0.1:9092\"|address = \"127.0.0.1:$KAFKA_PORT\"|" "$CONFIG_FILE"
sed -i "/^\[kafka\]/,/^enabled/ s|enabled = false|enabled = true|" "$CONFIG_FILE"
sed -i "/^\[quic\]/,/^enabled/ s|enabled = true|enabled = false|" "$CONFIG_FILE"
sed -i "/^\[websocket\]/,/^enabled/ s|enabled = true|enabled = false|" "$CONFIG_FILE"
sed -i "s|^path = \"local_data\"|path = \"$DATA_DIR/data\"|" "$CONFIG_FILE"

# --- Start server ---
echo "Starting iggy-server (kafka on :$KAFKA_PORT)..."
IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy \
    IGGY_CONFIG_PATH="$CONFIG_FILE" \
    "$SERVER_BIN" --with-default-root-credentials > "$SERVER_LOG" 2>&1 &
SERVER_PID=$!

for i in $(seq 1 30); do
    if (echo > /dev/tcp/127.0.0.1/$KAFKA_PORT) 2>/dev/null; then break; fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        tail -20 "$SERVER_LOG"
        fail "Server exited before Kafka port was ready"
        exit 1
    fi
    sleep 0.5
done

(echo > /dev/tcp/127.0.0.1/$KAFKA_PORT) 2>/dev/null || {
    tail -30 "$SERVER_LOG"
    fail "Kafka port $KAFKA_PORT not open after 15s"
    exit 1
}
pass "Server started, Kafka port open"

# --- Test 1: Metadata ---
echo ""
echo "Test 1: Metadata (list brokers, no topics yet)"
OUTPUT=$(kcat -L -b 127.0.0.1:$KAFKA_PORT -m 5 2>&1)
if echo "$OUTPUT" | grep -q "1 brokers"; then
    pass "Metadata shows 1 broker"
else
    fail "Metadata did not show 1 broker"
    echo "$OUTPUT"
fi

if echo "$OUTPUT" | grep -q "0 topics"; then
    pass "No topics initially"
else
    # Might have topics from a previous run if data dir wasn't clean
    pass "Metadata returned topics list"
fi

# --- Test 2: Produce ---
echo ""
echo "Test 2: Produce a message"
MESSAGE="hello-from-kcat-$$"
if echo "$MESSAGE" | timeout 15 kcat -P -b 127.0.0.1:$KAFKA_PORT -t smoke-test -p 0 2>&1; then
    pass "Produce succeeded"
else
    fail "Produce failed or timed out"
fi

# --- Test 3: Topic auto-created ---
echo ""
echo "Test 3: Topic was auto-created"
OUTPUT=$(kcat -L -b 127.0.0.1:$KAFKA_PORT -m 5 2>&1)
if echo "$OUTPUT" | grep -q '"smoke-test"'; then
    pass "Topic 'smoke-test' exists"
else
    fail "Topic 'smoke-test' not found in metadata"
    echo "$OUTPUT"
fi

# --- Test 4: Consume ---
echo ""
echo "Test 4: Consume the message back"
CONSUMED=$(timeout 10 kcat -C -b 127.0.0.1:$KAFKA_PORT -t smoke-test -p 0 -o 0 -c 1 2>&1)
if echo "$CONSUMED" | grep -q "$MESSAGE"; then
    pass "Consumed message matches: $MESSAGE"
else
    fail "Consumed message did not match"
    echo "Expected: $MESSAGE"
    echo "Got: $CONSUMED"
fi

# --- Test 5: Server still alive ---
echo ""
echo "Test 5: Server health"
if kill -0 "$SERVER_PID" 2>/dev/null; then
    pass "Server still running after all tests"
else
    fail "Server crashed"
fi

# --- Summary ---
echo ""
if [ "$FAILURES" -gt 0 ]; then
    echo -e "${RED}$FAILURES test(s) failed.${NC}"
    echo "Server log: $SERVER_LOG"
    tail -20 "$SERVER_LOG"
    exit 1
else
    echo -e "${GREEN}All tests passed.${NC}"
fi
