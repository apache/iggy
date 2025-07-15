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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Starting Elasticsearch test environment..."
cd "$SCRIPT_DIR"

# Start Elasticsearch
docker-compose up -d elasticsearch

echo "Waiting for Elasticsearch to be ready..."
timeout=120
counter=0
while [ $counter -lt $timeout ]; do
    if curl -s http://localhost:9200/_cluster/health > /dev/null 2>&1; then
        echo "Elasticsearch is ready!"
        break
    fi
    echo "Waiting for Elasticsearch... ($counter/$timeout)"
    sleep 2
    counter=$((counter + 2))
done

if [ $counter -eq $timeout ]; then
    echo "Error: Elasticsearch failed to start within $timeout seconds"
    docker-compose logs elasticsearch
    exit 1
fi

# Wait a bit more for ES to be fully ready
sleep 5

echo "Running integration tests..."
cd "$PROJECT_ROOT"

# Run tests with verbose output
cargo test --test integration_test -- --nocapture

echo "Tests completed successfully!"

echo "Cleaning up test environment..."
cd "$SCRIPT_DIR"
docker-compose down

echo "Test environment cleaned up." 