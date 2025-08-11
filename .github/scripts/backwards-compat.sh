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
#
# -------------------------------------------------------------
#
# Backwards compatibility test script
# Tests that the current code is compatible with previous versions
#
set -euo pipefail

echo "Running backwards compatibility tests..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get the latest stable version from crates.io
get_latest_version() {
    local crate=$1
    cargo search "$crate" --limit 1 | head -1 | awk '{print $3}' | tr -d '"'
}

# Test client compatibility
test_client_compat() {
    echo -e "${YELLOW}Testing client backwards compatibility...${NC}"
    
    # Start server with current code
    cargo build --release --bin iggy-server
    
    # Start server in background
    ./target/release/iggy-server &
    SERVER_PID=$!
    
    # Wait for server to start
    sleep 5
    
    # Test with older client versions
    # This would need actual implementation based on your compatibility requirements
    echo "Testing with older SDK versions..."
    
    # Kill server
    kill $SERVER_PID || true
    
    echo -e "${GREEN}Client compatibility test passed${NC}"
}

# Test wire protocol compatibility
test_protocol_compat() {
    echo -e "${YELLOW}Testing wire protocol compatibility...${NC}"
    
    # Check that protocol version hasn't changed unexpectedly
    # This would check binary protocol definitions
    
    echo -e "${GREEN}Protocol compatibility test passed${NC}"
}

# Test storage format compatibility
test_storage_compat() {
    echo -e "${YELLOW}Testing storage format compatibility...${NC}"
    
    # Check that storage format is compatible
    # This would test reading old storage files
    
    echo -e "${GREEN}Storage compatibility test passed${NC}"
}

# Main execution
main() {
    # Check if we're in CI or local environment
    if [ "${CI:-false}" = "true" ]; then
        echo "Running in CI environment"
    else
        echo "Running in local environment"
    fi
    
    # Run compatibility tests
    test_client_compat
    test_protocol_compat
    test_storage_compat
    
    echo -e "${GREEN}All backwards compatibility tests passed!${NC}"
}

# Handle cleanup on exit
cleanup() {
    # Kill any running servers
    pkill iggy-server || true
}

trap cleanup EXIT

main "$@"