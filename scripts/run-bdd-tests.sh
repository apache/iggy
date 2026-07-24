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
VSR=0
ARGS=()
for arg in "$@"; do
  case "$arg" in
    --coverage) COVERAGE=1 ;;
    --vsr) VSR=1 ;;
    *) ARGS+=("$arg") ;;
  esac
done

SDK="${ARGS[0]:-all}"
FEATURE="${ARGS[1]:-all}"

log(){ printf "%b\n" "$*"; }

usage(){
  log "Usage: $0 [--coverage] [--vsr] <sdk> [feature]"
  log ""
  log "  sdk:     rust | python | php | go | go-race | node | csharp | java | cpp | all | clean (default: all)"
  log "  feature: basic_messaging | leader_redirection | raw_command | all  (default: all)"
  log "  --vsr:   run against iggy-server-ng built with --features vsr (rust only);"
  log "           expects IGGY_SERVER_NG_PATH (default: target/debug/iggy-server-ng)"
  log "           and a vsr-built iggy CLI at IGGY_CLI_PATH"
  log ""
  log "Examples:"
  log "  $0 rust                         # run all features for Rust"
  log "  $0 rust basic_messaging         # run only basic_messaging for Rust"
  log "  $0 all leader_redirection       # run leader_redirection for all supporting SDKs"
  log "  $0 --vsr rust                   # run all features for Rust against server-ng"
  log "  $0 --coverage go basic_messaging"
}

if [ "$VSR" = "1" ]; then
  case "$SDK" in
    rust|clean) ;;
    *)
      log "❌ --vsr supports only the Rust SDK (server-ng speaks the VSR wire protocol)"
      usage
      exit 2 ;;
  esac
fi

case "$FEATURE" in
  basic_messaging|leader_redirection|raw_command|all) ;;
  *)
    log "Unknown feature: ${FEATURE}"
    usage
    exit 2 ;;
esac

export DOCKER_BUILDKIT=1 BDD_FEATURE="$FEATURE" GO_TEST_EXTRA_FLAGS="${GO_TEST_EXTRA_FLAGS:-}"

cd "$(dirname "$0")/../bdd"

ALL_COMPOSE_FILES=(
  -f docker-compose.yml
  -f docker-compose.server.yml
  -f docker-compose.cluster.yml
  -f docker-compose.coverage.yml
  -f docker-compose.vsr.yml
)

COMPOSE_FILES=(-f docker-compose.yml)
case "$FEATURE" in
  basic_messaging|leader_redirection|raw_command|all)
    COMPOSE_FILES+=(-f docker-compose.server.yml) ;;
esac
case "$FEATURE" in
  leader_redirection|all)
    COMPOSE_FILES+=(-f docker-compose.cluster.yml) ;;
esac
if [ "$COVERAGE" = "1" ]; then
  COMPOSE_FILES+=(-f docker-compose.coverage.yml)
  mkdir -p ../reports
fi
# vsr overrides must come last to win over the server/cluster/coverage files.
if [ "$VSR" = "1" ]; then
  COMPOSE_FILES+=(-f docker-compose.vsr.yml)
  export BDD_RUST_FEATURES="bdd,vsr"
fi

cleanup(){
  log "🧹  cleaning up containers & volumes…"
  docker compose "${ALL_COMPOSE_FILES[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

log "🧪 Running BDD tests for SDK: ${SDK}"
log "📁 Feature file: ${FEATURE}"
if [ "$VSR" = "1" ]; then
  log "🗳️ Server: iggy-server-ng (--features vsr)"
fi
if [ "$COVERAGE" = "1" ]; then
  log "📊 Coverage collection enabled → reports will be in ./reports/"
fi

run_suite(){
  local svc="$1" emoji="$2" label="$3"
  if [ "$FEATURE" = "leader_redirection" ]; then
    case "$svc" in
      rust-bdd|go-bdd|csharp-bdd|java-bdd) ;;
      *)
        if [ "$SDK" = "all" ]; then
          log "⚠️ skipping ${svc%-bdd} (does not support ${FEATURE})"
          return 0
        else
          log "❌ ${SDK} does not support feature '${FEATURE}'"
          return 1
        fi
        ;;
    esac
  fi

  log "${emoji} ${label}..."
  local code=0
  docker compose "${COMPOSE_FILES[@]}" \
    up --build --exit-code-from "$svc" "$svc" \
    || code=$?
  docker compose "${COMPOSE_FILES[@]}" \
    down -v --remove-orphans >/dev/null 2>&1 || true
  return "$code"
}

case "$SDK" in
  rust)     run_suite rust-bdd   "🦀"   "Running Rust BDD tests"   ;;
  python)   run_suite python-bdd "🐍"   "Running Python BDD tests" ;;
  php)      run_suite php-bdd    "🐘"   "Running PHP BDD tests"    ;;
  go)       run_suite go-bdd     "🐹"   "Running Go BDD tests"     ;;
  go-race)
    if [ "$COVERAGE" = "1" ]; then
      log "⚠️ coverage collection automatically drops -race flag as coverage does not need -race"
    fi
    export GO_TEST_EXTRA_FLAGS="-race"
    run_suite go-bdd "🐹⚡" "Running Go BDD tests with data race detector"
    ;;
  node)     run_suite node-bdd   "🐢🚀" "Running Node BDD tests"   ;;
  csharp)   run_suite csharp-bdd "🔷"   "Running C# BDD tests"     ;;
  java)     run_suite java-bdd   "☕"   "Running Java BDD tests"   ;;
  cpp)      run_suite cpp-bdd    "⚙️"   "Running C++ BDD tests"    ;;
  all)
    run_suite rust-bdd   "🦀"   "Running Rust BDD tests"                       || exit $?
    run_suite python-bdd "🐍"   "Running Python BDD tests"                     || exit $?
    run_suite php-bdd    "🐘"   "Running PHP BDD tests"                        || exit $?
    run_suite go-bdd     "🐹"   "Running Go BDD tests"                         || exit $?
    GO_TEST_EXTRA_FLAGS="-race" \
    run_suite go-bdd     "🐹⚡" "Running Go BDD tests with data race detector"  || exit $?
    run_suite node-bdd   "🐢🚀" "Running Node BDD tests"                       || exit $?
    run_suite csharp-bdd "🔷"   "Running C# BDD tests"                         || exit $?
    run_suite java-bdd   "☕"   "Running Java BDD tests"                       || exit $?
    run_suite cpp-bdd    "⚙️"   "Running C++ BDD tests"                        || exit $?
    ;;
  clean)
    cleanup
    exit 0 ;;
  *)
    log "❌ Unknown SDK: ${SDK}"
    usage
    exit 2 ;;
esac

log "✅ BDD tests completed for: sdk=${SDK}  feature=${FEATURE}"
