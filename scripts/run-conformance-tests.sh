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

# Runs the conformance corpus against the agent of one SDK. The script starts
# a single node and a node that cannot reach quorum on spare local ports, and
# stops both at its end.
#
# Usage: scripts/run-conformance-tests.sh <sdk> [extra arguments of iggy-conformance run]
#   sdk: rust | go
#
# Environment:
#   IGGY_SERVER_PATH        the iggy-server binary (default target/debug/iggy-server)
#   IGGY_CLI_PATH           the iggy binary, for the readiness ping (default target/debug/iggy)
#   IGGY_CONFORMANCE_PATH   the iggy-conformance binary (default target/debug/iggy-conformance)
#   REPORTS_DIR             where the JUnit and JSON reports go (default reports)
#   IGGY_SHARDING_CPU_ALLOCATION   passed to both servers when set

set -Eeuo pipefail

SDK="${1:-}"
if [[ $# -gt 0 ]]; then
  shift
fi

SERVER="${IGGY_SERVER_PATH:-target/debug/iggy-server}"
CLI="${IGGY_CLI_PATH:-target/debug/iggy}"
CONFORMANCE="${IGGY_CONFORMANCE_PATH:-target/debug/iggy-conformance}"
REPORTS_DIR="${REPORTS_DIR:-reports}"
ROOT_USERNAME="${IGGY_ROOT_USERNAME:-iggy}"
ROOT_PASSWORD="${IGGY_ROOT_PASSWORD:-iggy}"

SINGLE_ADDRESS="127.0.0.1:18090"
NO_QUORUM_ADDRESS="127.0.0.1:18093"

usage() {
  echo "Usage: $0 <rust|go> [extra arguments of iggy-conformance run]"
}

case "$SDK" in
  rust|go) ;;
  *)
    usage
    exit 2 ;;
esac

for binary in "$SERVER" "$CLI" "$CONFORMANCE"; do
  if [[ ! -x "$binary" ]]; then
    echo "Missing binary: $binary. Build it with: cargo build --bin iggy-server --bin iggy --bin iggy-conformance"
    exit 1
  fi
done
# The servers start in a work directory, so their binary needs an absolute path.
SERVER="$(cd "$(dirname "$SERVER")" && pwd)/$(basename "$SERVER")"

WORK_DIR=$(mktemp -d)
SERVER_PIDS=()

# shellcheck disable=SC2329 # The EXIT trap below calls it.
cleanup() {
  for pid in "${SERVER_PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${SERVER_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

start_single() {
  (
    cd "$WORK_DIR"
    # Encryption is on because the corpus checks server-generated stream keys,
    # and the small change log lets a test expire a metadata change cursor.
    exec env \
      IGGY_ROOT_USERNAME="$ROOT_USERNAME" \
      IGGY_ROOT_PASSWORD="$ROOT_PASSWORD" \
      IGGY_PATH="$WORK_DIR/single" \
      IGGY_TCP_ADDRESS="$SINGLE_ADDRESS" \
      IGGY_HTTP_ADDRESS=127.0.0.1:13000 \
      IGGY_QUIC_ADDRESS=127.0.0.1:18080 \
      IGGY_WEBSOCKET_ADDRESS=127.0.0.1:18070 \
      IGGY_ENCRYPTION_ENABLED=true \
      IGGY_ENCRYPTION_KEY=/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE= \
      IGGY_METADATA_CHANGE_LOG_RESOURCE_CAPACITY=64 \
      "$SERVER"
  ) > "$WORK_DIR/single.log" 2>&1 &
  SERVER_PIDS+=("$!")
}

# The node names a second replica that never starts, so it never reaches quorum.
start_no_quorum() {
  (
    cd "$WORK_DIR"
    exec env \
      IGGY_ROOT_USERNAME="$ROOT_USERNAME" \
      IGGY_ROOT_PASSWORD="$ROOT_PASSWORD" \
      IGGY_PATH="$WORK_DIR/no-quorum" \
      IGGY_CLUSTER_ENABLED=true \
      IGGY_CLUSTER_NAME=conformance-no-quorum \
      IGGY_CLUSTER_NODES_0_NAME=no-quorum-node \
      IGGY_CLUSTER_NODES_0_IP=127.0.0.1 \
      IGGY_CLUSTER_NODES_0_REPLICA_ID=0 \
      IGGY_CLUSTER_NODES_0_PORTS_TCP=18093 \
      IGGY_CLUSTER_NODES_0_PORTS_QUIC=18083 \
      IGGY_CLUSTER_NODES_0_PORTS_HTTP=13003 \
      IGGY_CLUSTER_NODES_0_PORTS_WEBSOCKET=18073 \
      IGGY_CLUSTER_NODES_0_PORTS_TCP_REPLICA=18193 \
      IGGY_CLUSTER_NODES_1_NAME=absent-node \
      IGGY_CLUSTER_NODES_1_IP=127.0.0.1 \
      IGGY_CLUSTER_NODES_1_REPLICA_ID=1 \
      IGGY_CLUSTER_NODES_1_PORTS_TCP=18094 \
      IGGY_CLUSTER_NODES_1_PORTS_QUIC=18084 \
      IGGY_CLUSTER_NODES_1_PORTS_HTTP=13004 \
      IGGY_CLUSTER_NODES_1_PORTS_WEBSOCKET=18074 \
      IGGY_CLUSTER_NODES_1_PORTS_TCP_REPLICA=18194 \
      IGGY_CLUSTER_AUTH_ENABLED=true \
      IGGY_CLUSTER_AUTH_SHARED_SECRET=conformance-no-quorum-shared-secret-0123456789 \
      IGGY_TCP_ADDRESS="$NO_QUORUM_ADDRESS" \
      IGGY_HTTP_ADDRESS=127.0.0.1:13003 \
      IGGY_QUIC_ADDRESS=127.0.0.1:18083 \
      IGGY_WEBSOCKET_ADDRESS=127.0.0.1:18073 \
      "$SERVER" --replica-id 0
  ) > "$WORK_DIR/no-quorum.log" 2>&1 &
  SERVER_PIDS+=("$!")
}

wait_for() {
  local address="$1" log="$2"
  for _ in $(seq 1 60); do
    if "$CLI" --tcp-server-address "$address" ping > /dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "The server at $address did not start. Its log:"
  tail -n 50 "$log"
  return 1
}

start_single
start_no_quorum
wait_for "$SINGLE_ADDRESS" "$WORK_DIR/single.log"
wait_for "$NO_QUORUM_ADDRESS" "$WORK_DIR/no-quorum.log"

case "$SDK" in
  rust)
    AGENT=rust ;;
  go)
    AGENT="$WORK_DIR/iggy-agent-go"
    (cd conformance/agents/go && go build -o "$AGENT" .) ;;
esac

mkdir -p "$REPORTS_DIR"
status=0
"$CONFORMANCE" run \
  --tests conformance/tests \
  --server "$SINGLE_ADDRESS" \
  --no-quorum "$NO_QUORUM_ADDRESS" \
  --username "$ROOT_USERNAME" \
  --password "$ROOT_PASSWORD" \
  --agent "$AGENT" \
  --skips conformance/skips.toml \
  --junit "$REPORTS_DIR/conformance-$SDK.xml" \
  --json "$REPORTS_DIR/conformance-$SDK.json" \
  "$@" || status=$?

if [[ $status -ne 0 ]]; then
  echo "The last lines of the single node's log:"
  tail -n 30 "$WORK_DIR/single.log"
fi
exit "$status"
