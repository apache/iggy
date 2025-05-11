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


# Exit on any error
set -euo pipefail

# Check if performance_results directory exists
if [ ! -d "${PWD}/performance_results" ]; then
    echo "Error: 'performance_results' directory does not exist."
    echo "Please run benchmarks with \`output\` subcommand first to generate performance results."
    exit 1
fi

# Check if trunk is installed
if ! command -v trunk &>/dev/null; then
    echo "Error: 'trunk' command not found."
    echo "Please install it via: cargo install trunk"
    exit 1
fi

# Function to cleanup background processes
cleanup() {
    echo "Shutting down services..."
    kill "$(jobs -p)" 2>/dev/null
    exit 0
}

# Set up trap for SIGINT (Ctrl+C)
trap cleanup SIGINT

# Parse arguments
TRUNK_ARGS=""
if [ $# -gt 0 ]; then
    if [ "$1" = "open" ]; then
        TRUNK_ARGS="--open"
    else
        echo "Error: Invalid argument. Only 'open' is supported."
        exit 1
    fi
fi

# Start server in background
echo "Starting server..."
cargo run --bin iggy-bench-dashboard-server &

# Wait a bit for server to start
sleep 1

# Start frontend in background
echo "Starting frontend..."
trunk serve --config core/bench/dashboard/frontend/Trunk.toml $TRUNK_ARGS &

# Wait for all background processes
wait
