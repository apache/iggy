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

# Generate or remove gitignored kafka-tool wire fixtures for CI.
# Run from the iggy workspace root.

set -euo pipefail

FIXTURES_DIR="gateways/kafka/tools/kafka-tool/kafka_messages"

# API keys exercised by decode_validation_tests and handler_regression_tests.
FIXTURE_API_KEYS=(0 1 2 19)

usage() {
  echo "Usage: $0 {generate|cleanup}" >&2
  exit 2
}

generate() {
  mkdir -p "$FIXTURES_DIR"
  for key in "${FIXTURE_API_KEYS[@]}"; do
    cargo run --locked -p kafka-message-gen -- generate \
      --output "$FIXTURES_DIR" \
      --api-key "$key"
  done
  echo "Generated wire fixtures under ${FIXTURES_DIR}/"
}

cleanup() {
  rm -rf "$FIXTURES_DIR"
  echo "Removed ${FIXTURES_DIR}/"
}

case "${1:-}" in
  generate) generate ;;
  cleanup) cleanup ;;
  *) usage ;;
esac
