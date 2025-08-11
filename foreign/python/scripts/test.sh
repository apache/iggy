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

echo "🐍 Python SDK Test Runner"
echo "========================="

# Wait for server to be ready
echo "⏳ Waiting for Iggy server to be ready..."
timeout 60 bash -c "
    until timeout 5 bash -c '</dev/tcp/\${IGGY_SERVER_HOST}/\${IGGY_SERVER_TCP_PORT}'; do
        echo '   Server not ready, waiting...'
        sleep 2
    done
"
echo "✅ Server is ready!"

# Test connection
echo "🔗 Testing basic connectivity..."

# Resolve hostname to IP address for Rust client compatibility
SERVER_IP=$(getent hosts "${IGGY_SERVER_HOST}" | awk '{ print $1 }' | head -n1)
if [ -z "$SERVER_IP" ]; then
    echo "❌ Could not resolve hostname: ${IGGY_SERVER_HOST}"
    exit 1
fi
echo "📍 Resolved ${IGGY_SERVER_HOST} to ${SERVER_IP}"

if ! python3 -c "
import asyncio
import sys
from apache_iggy import IggyClient

async def test_connection():
    try:
        client = IggyClient('${SERVER_IP}:${IGGY_SERVER_TCP_PORT}')
        await client.connect()
        await client.login_user('iggy', 'iggy')
        await client.ping()
        print('✅ Connection test passed')
        return True
    except Exception as e:
        print(f'❌ Connection test failed: {e}')
        return False

result = asyncio.run(test_connection())
sys.exit(0 if result else 1)
"; then
    echo "❌ Connection test failed, aborting tests"
    exit 1
fi

# Create test results directory
mkdir -p test-results

# Run tests with detailed output
echo "🧪 Running Python SDK tests..."
python3 -m pytest \
    "${PYTEST_ARGS:--v --tb=short}" \
    --junit-xml=test-results/pytest.xml \
    tests/

TEST_EXIT_CODE=$?

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo "✅ All tests passed!"
else
    echo "❌ Some tests failed (exit code: $TEST_EXIT_CODE)"
fi

# Run examples if tests pass
if [ $TEST_EXIT_CODE -eq 0 ] && [ "${RUN_EXAMPLES:-false}" = "true" ]; then
    echo "🚀 Running example scripts..."

    # Note: Examples might run indefinitely, so we'll just test they start correctly
    timeout 10 python3 examples/producer.py &
    PRODUCER_PID=$!
    sleep 5

    if kill -0 $PRODUCER_PID 2>/dev/null; then
        echo "✅ Producer example started successfully"
        kill $PRODUCER_PID
    else
        echo "❌ Producer example failed to start"
    fi
fi

echo "📊 Test results saved to test-results/"
exit $TEST_EXIT_CODE
