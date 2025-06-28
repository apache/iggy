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

SDK=${1:-"all"}
FEATURE=${2:-"scenarios/basic_messaging.feature"}

echo "🧪 Running BDD tests for SDK: $SDK"
echo "📁 Feature file: $FEATURE"

# Change to BDD directory
cd "$(dirname "$0")/../bdd"

case $SDK in
"rust")
  echo "🦀 Running Rust BDD tests..."
  docker compose build --no-cache iggy-server rust-bdd
  docker compose up --abort-on-container-exit rust-bdd
  ;;
"python")
  echo "🐍 Running Python BDD tests..."
  docker compose build --no-cache iggy-server python-bdd
  docker compose up --abort-on-container-exit python-bdd
  ;;
"go")
  echo "🐹 Running Go BDD tests..."
  docker compose build --no-cache iggy-server go-bdd
  docker compose up --abort-on-container-exit go-bdd
  ;;
"node")
  echo "🐢🚀 Running node BDD tests..."
  docker compose build --no-cache iggy-server node-bdd
  docker compose up --abort-on-container-exit node-bdd
  ;;
"all")
  echo "🚀 Running all SDK BDD tests..."
  echo "🦀 Starting with Rust tests..."
  docker compose build --no-cache iggy-server rust-bdd python-bdd go-bdd
  docker compose up --abort-on-container-exit rust-bdd
  echo "🐍 Now running Python tests..."
  docker compose up --abort-on-container-exit python-bdd
  echo "🐹 Now running Go tests..."
  docker compose up --abort-on-container-exit go-bdd
  echo "🐢🚀 Now unning node BDD tests..."
  docker compose up --abort-on-container-exit node-bdd
  ;;
"clean")
  echo "🧹 Cleaning up Docker resources..."
  docker compose down -v
  docker compose rm -f
  ;;
*)
  echo "❌ Unknown SDK: $SDK"
  echo "📖 Usage: $0 [rust|python|go|all|clean] [feature_file]"
  echo "📖 Examples:"
  echo "   $0 rust                    # Run Rust tests only"
  echo "   $0 python                  # Run Python tests only"
  echo "   $0 go                      # Run Go tests only"
  echo "   $0 node                    # Run Node.js tests only"
  echo "   $0 all                     # Run all SDK tests"
  echo "   $0 clean                   # Clean up Docker resources"
  exit 1
  ;;
esac

echo "✅ BDD tests completed for: $SDK"
