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

set -Eeuo pipefail

SDK=${1:-"all"}
FEATURE=${2:-"scenarios/basic_messaging.feature"}

export DOCKER_BUILDKIT=1 FEATURE

cd "$(dirname "$0")/../bdd"

log(){ printf "%b\n" "$*"; }

cleanup(){
  log "🧹  cleaning up containers & volumes…"
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

# Check if Docker supports cap_add
check_docker_capabilities(){
  if ! docker info | grep -q "Security Options"; then
    log "⚠️  Warning: Docker may not support security options"
  fi

  # Verify cap_add is supported by checking docker compose config
  if ! docker compose config >/dev/null 2>&1; then
    log "❌ Error: docker-compose.yml validation failed"
    exit 1
  fi
}

log "🧪 Running BDD tests for SDK: ${SDK}"
log "📁 Feature file: ${FEATURE}"

check_docker_capabilities

run_suite(){
  local svc="$1" emoji="$2" label="$3"
  log "${emoji}  ${label}…"
  set +e
  docker compose up --build --abort-on-container-exit --exit-code-from "$svc" "$svc"
  local code=$?
  set -e
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  return "$code"
}

case "$SDK" in
  rust)   run_suite rust-bdd   "🦀"   "Running Rust BDD tests"   ;;
  python) run_suite python-bdd "🐍"   "Running Python BDD tests" ;;
  go)     run_suite go-bdd     "🐹"   "Running Go BDD tests"     ;;
  node)   run_suite node-bdd   "🐢🚀" "Running Node BDD tests"   ;;
  csharp) run_suite csharp-bdd "🔷"   "Running C# BDD tests"     ;;
  all)
    run_suite rust-bdd   "🦀"   "Running Rust BDD tests"   || exit $?
    run_suite python-bdd "🐍"   "Running Python BDD tests" || exit $?
    run_suite go-bdd     "🐹"   "Running Go BDD tests"     || exit $?
    run_suite node-bdd   "🐢🚀" "Running Node BDD tests"   || exit $?
    run_suite csharp-bdd "🔷"   "Running C# BDD tests"     || exit $?
    ;;
  clean)
    cleanup; exit 0 ;;
  *)
    log "❌ Unknown SDK: ${SDK}"
    log "📖 Usage: $0 [rust|python|go|node|csharp|all|clean] [feature_file]"
    exit 2 ;;
esac

log "✅ BDD tests completed for: ${SDK}"
