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

# Build script for Iggy Flink Connectors using cargo

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Project directories
SINK_DIR="../sinks/flink_sink"
SOURCE_DIR="../sources/flink_source"

# Function to display help
show_help() {
    echo "Iggy Flink Connectors - Build Commands:"
    echo ""
    echo "Usage: ./build.sh [command]"
    echo ""
    echo "Commands:"
    echo "  build          - Build both connectors in release mode"
    echo "  build-sink     - Build sink connector only"
    echo "  build-source   - Build source connector only"
    echo "  test           - Run all tests"
    echo "  test-sink      - Test sink connector"
    echo "  test-source    - Test source connector"
    echo "  clean          - Clean all build artifacts"
    echo "  fmt            - Format all code"
    echo "  clippy         - Run clippy linter"
    echo "  check          - Run fmt, clippy, and check"
    echo "  docker-up      - Start test infrastructure"
    echo "  docker-down    - Stop test infrastructure"
    echo "  e2e            - Run end-to-end tests"
    echo "  help           - Show this help message"
    echo ""
}

# Build both connectors
build_all() {
    echo -e "${GREEN}Building both connectors...${NC}"
    cd "$SINK_DIR" && cargo build --release
    cd - > /dev/null
    cd "$SOURCE_DIR" && cargo build --release
    cd - > /dev/null
    echo -e "${GREEN}Build complete!${NC}"
}

# Build sink only
build_sink() {
    echo -e "${GREEN}Building sink connector...${NC}"
    cd "$SINK_DIR" && cargo build --release
    cd - > /dev/null
}

# Build source only
build_source() {
    echo -e "${GREEN}Building source connector...${NC}"
    cd "$SOURCE_DIR" && cargo build --release
    cd - > /dev/null
}

# Run all tests
test_all() {
    echo -e "${GREEN}Running all tests...${NC}"
    cd "$SINK_DIR" && cargo test
    cd - > /dev/null
    cd "$SOURCE_DIR" && cargo test
    cd - > /dev/null
}

# Test sink only
test_sink() {
    echo -e "${GREEN}Testing sink connector...${NC}"
    cd "$SINK_DIR" && cargo test
    cd - > /dev/null
}

# Test source only
test_source() {
    echo -e "${GREEN}Testing source connector...${NC}"
    cd "$SOURCE_DIR" && cargo test
    cd - > /dev/null
}

# Clean build artifacts
clean_all() {
    echo -e "${GREEN}Cleaning build artifacts...${NC}"
    cd "$SINK_DIR" && cargo clean
    cd - > /dev/null
    cd "$SOURCE_DIR" && cargo clean
    cd - > /dev/null
    rm -rf target/
    rm -f /tmp/flink_connector_test.toml
    rm -rf /tmp/connector_state
}

# Format code
format_code() {
    echo -e "${GREEN}Formatting code...${NC}"
    cd "$SINK_DIR" && cargo fmt
    cd - > /dev/null
    cd "$SOURCE_DIR" && cargo fmt
    cd - > /dev/null
}

# Run clippy
run_clippy() {
    echo -e "${GREEN}Running clippy...${NC}"
    cd "$SINK_DIR" && cargo clippy
    cd - > /dev/null
    cd "$SOURCE_DIR" && cargo clippy
    cd - > /dev/null
}

# Check code
check_code() {
    format_code
    run_clippy
    echo -e "${GREEN}Running cargo check...${NC}"
    cd "$SINK_DIR" && cargo check
    cd - > /dev/null
    cd "$SOURCE_DIR" && cargo check
    cd - > /dev/null
}

# Docker commands
docker_up() {
    echo -e "${GREEN}Starting Docker infrastructure...${NC}"
    docker-compose up -d
    echo -e "${YELLOW}Waiting for services to start...${NC}"
    sleep 10
    echo -e "${GREEN}Infrastructure ready!${NC}"
}

docker_down() {
    echo -e "${GREEN}Stopping Docker infrastructure...${NC}"
    docker-compose down -v
}

# E2E test
run_e2e() {
    echo -e "${GREEN}Running end-to-end tests...${NC}"
    ./test/e2e_test.sh
}

# Main command handler
case "${1:-help}" in
    build)
        build_all
        ;;
    build-sink)
        build_sink
        ;;
    build-source)
        build_source
        ;;
    test)
        test_all
        ;;
    test-sink)
        test_sink
        ;;
    test-source)
        test_source
        ;;
    clean)
        clean_all
        ;;
    fmt)
        format_code
        ;;
    clippy)
        run_clippy
        ;;
    check)
        check_code
        ;;
    docker-up)
        docker_up
        ;;
    docker-down)
        docker_down
        ;;
    e2e)
        run_e2e
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        echo "Unknown command: $1"
        show_help
        exit 1
        ;;
esac
