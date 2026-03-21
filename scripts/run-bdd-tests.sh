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

COVERAGE=0
ARGS=()
for arg in "$@"; do
  case "$arg" in
    --coverage) COVERAGE=1 ;;
    *) ARGS+=("$arg") ;;
  esac
done

SDK="${ARGS[0]:-all}"
FEATURE="${ARGS[1]:-scenarios/basic_messaging.feature}"

export DOCKER_BUILDKIT=1 FEATURE GO_TEST_EXTRA_FLAGS="${GO_TEST_EXTRA_FLAGS:-}"

cd "$(dirname "$0")/../bdd"

COMPOSE_CMD=(docker compose -f docker-compose.yml)
if [ "$COVERAGE" = "1" ]; then
  COMPOSE_CMD+=(-f docker-compose.coverage.yml)
  mkdir -p ../reports
fi

log(){ printf "%b\n" "$*"; }

cleanup(){
  log "🧹  cleaning up containers & volumes…"
  "${COMPOSE_CMD[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

log "🧪 Running BDD tests for SDK: ${SDK}"
log "📁 Feature file: ${FEATURE}"
if [ "$COVERAGE" = "1" ]; then
  log "📊 Coverage collection enabled → reports will be in ./reports/"
fi

run_suite(){
  local svc="$1" emoji="$2" label="$3"
  log "${emoji}  ${label}…"
  set +e
  "${COMPOSE_CMD[@]}" up --build --abort-on-container-exit --exit-code-from "$svc" "$svc"
  local code=$?
  set -e
  "${COMPOSE_CMD[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  return "$code"
}

case "$SDK" in
  rust)     run_suite rust-bdd   "🦀"   "Running Rust BDD tests"   ;;
  python)   run_suite python-bdd "🐍"   "Running Python BDD tests" ;;
  go)       run_suite go-bdd     "🐹"   "Running Go BDD tests"     ;;
  go-race)
    export GO_TEST_EXTRA_FLAGS="-race"
    run_suite go-bdd "🐹⚡" "Running Go BDD tests with data race detector"
    ;;
  node)     run_suite node-bdd   "🐢🚀" "Running Node BDD tests"   ;;
  csharp)   run_suite csharp-bdd "🔷"   "Running C# BDD tests"     ;;
  java)     run_suite java-bdd   "☕"   "Running Java BDD tests"   ;;
  all)
    run_suite rust-bdd   "🦀"   "Running Rust BDD tests"                       || exit $?
    run_suite python-bdd "🐍"   "Running Python BDD tests"                     || exit $?
    run_suite go-bdd     "🐹"   "Running Go BDD tests"                         || exit $?
    GO_TEST_EXTRA_FLAGS="-race" \
    run_suite go-bdd     "🐹⚡" "Running Go BDD tests with data race detector"  || exit $?
    run_suite node-bdd   "🐢🚀" "Running Node BDD tests"                       || exit $?
    run_suite csharp-bdd "🔷"   "Running C# BDD tests"                         || exit $?
    run_suite java-bdd   "☕"   "Running Java BDD tests"                       || exit $?
    ;;
  clean)
    cleanup; exit 0 ;;
  *)
    log "❌ Unknown SDK: ${SDK}"
    log "📖 Usage: $0 [--coverage] [rust|python|go|go-race|node|csharp|java|all|clean] [feature_file]"
    exit 2 ;;
esac

log "✅ BDD tests completed for: ${SDK}"
