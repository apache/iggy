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
# a single node, a node that cannot reach quorum and a two-node cluster on
# spare local ports, and stops them at its end.
#
# Usage: scripts/run-conformance-tests.sh <sdk> [extra arguments of iggy-conformance run]
#   sdk: rust | go | python | csharp | java | node | cpp | php | swift, or any name with
#        CONFORMANCE_AGENT set. php, and swift without a local toolchain, build Docker images.
#
# Environment:
#   IGGY_SERVER_PATH        the iggy-server binary (default target/debug/iggy-server)
#   IGGY_CLI_PATH           the iggy binary, for the readiness ping (default target/debug/iggy)
#   IGGY_CONFORMANCE_PATH   the iggy-conformance binary (default target/debug/iggy-conformance)
#   REPORTS_DIR             where the JUnit and JSON reports go (default reports)
#   IGGY_SHARDING_CPU_ALLOCATION   passed to both servers when set
#   CONFORMANCE_HOST        the loopback address of both servers (default 127.0.0.1). Runs on
#                           different addresses, such as 127.0.0.2, can run side by side.
#   CONFORMANCE_AGENT       a command that starts an agent, instead of the built one of <sdk>
#   CONFORMANCE_CLUSTER     0 runs without the two-node cluster (default 1)
#   CONFORMANCE_CLUSTER_PROXY_HOST  the host that the cluster advertises, where the
#                           orchestrator proxies each node (default 127.0.1.<last octet of the host>)

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

HOST="${CONFORMANCE_HOST:-127.0.0.1}"
SINGLE_ADDRESS="$HOST:18090"
NO_QUORUM_ADDRESS="$HOST:18093"
LEADER_ADDRESS="$HOST:18091"
FOLLOWER_ADDRESS="$HOST:18092"
# The cluster advertises this host, where the orchestrator proxies each node.
# The default moves the last octet of the host to 127.0.1.0/24.
PROXY_HOST="${CONFORMANCE_CLUSTER_PROXY_HOST:-127.0.1.${HOST##*.}}"

usage() {
  echo "Usage: $0 <rust|go|python|csharp|java|node|cpp|php|swift> [extra arguments of iggy-conformance run]"
  echo "       CONFORMANCE_AGENT='<command>' $0 <sdk name> [extra arguments]"
}

SDKS=" rust go python csharp java node cpp php swift "
if [[ -z "$SDK" || ( -z "${CONFORMANCE_AGENT:-}" && "$SDKS" != *" $SDK "* ) ]]; then
  usage
  exit 2
fi

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
    # The server refuses IGGY_ variables that it does not know, so the paths
    # that this script reads stay out of its environment. Encryption is on
    # because the corpus checks server-generated stream keys,
    # and the small change log lets a test expire a metadata change cursor.
    exec env -u IGGY_SERVER_PATH -u IGGY_CLI_PATH -u IGGY_CONFORMANCE_PATH \
      IGGY_ROOT_USERNAME="$ROOT_USERNAME" \
      IGGY_ROOT_PASSWORD="$ROOT_PASSWORD" \
      IGGY_PATH="$WORK_DIR/single" \
      IGGY_TCP_ADDRESS="$SINGLE_ADDRESS" \
      IGGY_HTTP_ADDRESS="$HOST":13000 \
      IGGY_QUIC_ADDRESS="$HOST":18080 \
      IGGY_WEBSOCKET_ADDRESS="$HOST":18070 \
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
    exec env -u IGGY_SERVER_PATH -u IGGY_CLI_PATH -u IGGY_CONFORMANCE_PATH \
      IGGY_ROOT_USERNAME="$ROOT_USERNAME" \
      IGGY_ROOT_PASSWORD="$ROOT_PASSWORD" \
      IGGY_PATH="$WORK_DIR/no-quorum" \
      IGGY_CLUSTER_ENABLED=true \
      IGGY_CLUSTER_NAME=conformance-no-quorum \
      IGGY_CLUSTER_NODES_0_NAME=no-quorum-node \
      IGGY_CLUSTER_NODES_0_IP="$HOST" \
      IGGY_CLUSTER_NODES_0_REPLICA_ID=0 \
      IGGY_CLUSTER_NODES_0_PORTS_TCP=18093 \
      IGGY_CLUSTER_NODES_0_PORTS_QUIC=18083 \
      IGGY_CLUSTER_NODES_0_PORTS_HTTP=13003 \
      IGGY_CLUSTER_NODES_0_PORTS_WEBSOCKET=18073 \
      IGGY_CLUSTER_NODES_0_PORTS_TCP_REPLICA=18193 \
      IGGY_CLUSTER_NODES_1_NAME=absent-node \
      IGGY_CLUSTER_NODES_1_IP="$HOST" \
      IGGY_CLUSTER_NODES_1_REPLICA_ID=1 \
      IGGY_CLUSTER_NODES_1_PORTS_TCP=18094 \
      IGGY_CLUSTER_NODES_1_PORTS_QUIC=18084 \
      IGGY_CLUSTER_NODES_1_PORTS_HTTP=13004 \
      IGGY_CLUSTER_NODES_1_PORTS_WEBSOCKET=18074 \
      IGGY_CLUSTER_NODES_1_PORTS_TCP_REPLICA=18194 \
      IGGY_CLUSTER_AUTH_ENABLED=true \
      IGGY_CLUSTER_AUTH_SHARED_SECRET=conformance-no-quorum-shared-secret-0123456789 \
      IGGY_TCP_ADDRESS="$NO_QUORUM_ADDRESS" \
      IGGY_HTTP_ADDRESS="$HOST":13003 \
      IGGY_QUIC_ADDRESS="$HOST":18083 \
      IGGY_WEBSOCKET_ADDRESS="$HOST":18073 \
      "$SERVER" --replica-id 0
  ) > "$WORK_DIR/no-quorum.log" 2>&1 &
  SERVER_PIDS+=("$!")
}

# Replica 0 leads the cluster in its first view, so it is the leader.
start_cluster_node() {
  local replica="$1"
  (
    cd "$WORK_DIR"
    exec env -u IGGY_SERVER_PATH -u IGGY_CLI_PATH -u IGGY_CONFORMANCE_PATH \
      IGGY_ROOT_USERNAME="$ROOT_USERNAME" \
      IGGY_ROOT_PASSWORD="$ROOT_PASSWORD" \
      IGGY_PATH="$WORK_DIR/cluster-$replica" \
      IGGY_CLUSTER_ENABLED=true \
      IGGY_CLUSTER_NAME=conformance-cluster \
      IGGY_CLUSTER_HEARTBEAT_TIMEOUT=30s \
      IGGY_CLUSTER_NODES_0_NAME=leader-node \
      IGGY_CLUSTER_NODES_0_IP="$HOST" \
      IGGY_CLUSTER_NODES_0_REPLICA_ID=0 \
      IGGY_CLUSTER_NODES_0_PORTS_TCP=18091 \
      IGGY_CLUSTER_NODES_0_PORTS_QUIC=18081 \
      IGGY_CLUSTER_NODES_0_PORTS_HTTP=13001 \
      IGGY_CLUSTER_NODES_0_PORTS_WEBSOCKET=18071 \
      IGGY_CLUSTER_NODES_0_PORTS_TCP_REPLICA=18191 \
      IGGY_CLUSTER_NODES_0_ADVERTISED_ADDRESS="$PROXY_HOST" \
      IGGY_CLUSTER_NODES_1_NAME=follower-node \
      IGGY_CLUSTER_NODES_1_IP="$HOST" \
      IGGY_CLUSTER_NODES_1_REPLICA_ID=1 \
      IGGY_CLUSTER_NODES_1_PORTS_TCP=18092 \
      IGGY_CLUSTER_NODES_1_PORTS_QUIC=18082 \
      IGGY_CLUSTER_NODES_1_PORTS_HTTP=13002 \
      IGGY_CLUSTER_NODES_1_PORTS_WEBSOCKET=18072 \
      IGGY_CLUSTER_NODES_1_PORTS_TCP_REPLICA=18192 \
      IGGY_CLUSTER_NODES_1_ADVERTISED_ADDRESS="$PROXY_HOST" \
      IGGY_CLUSTER_AUTH_ENABLED=true \
      IGGY_CLUSTER_AUTH_SHARED_SECRET=conformance-cluster-shared-secret-0123456789 \
      IGGY_TCP_ADDRESS="$HOST":"$((18091 + replica))" \
      IGGY_HTTP_ADDRESS="$HOST":"$((13001 + replica))" \
      IGGY_QUIC_ADDRESS="$HOST":"$((18081 + replica))" \
      IGGY_WEBSOCKET_ADDRESS="$HOST":"$((18071 + replica))" \
      "$SERVER" --replica-id "$replica"
  ) > "$WORK_DIR/cluster-$replica.log" 2>&1 &
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

CLUSTER_ARGS=()
start_single
start_no_quorum
# An older iggy-conformance has no cluster options, so it runs without the cluster.
if [[ "${CONFORMANCE_CLUSTER:-1}" == 1 ]] && "$CONFORMANCE" run --help 2>/dev/null | grep -q -- '--leader'; then
  start_cluster_node 0
  start_cluster_node 1
  CLUSTER_ARGS=(--leader "$LEADER_ADDRESS" --follower "$FOLLOWER_ADDRESS" --cluster-proxy-host "$PROXY_HOST")
fi
wait_for "$SINGLE_ADDRESS" "$WORK_DIR/single.log"
wait_for "$NO_QUORUM_ADDRESS" "$WORK_DIR/no-quorum.log"
if [[ ${#CLUSTER_ARGS[@]} -gt 0 ]]; then
  wait_for "$LEADER_ADDRESS" "$WORK_DIR/cluster-0.log"
  wait_for "$FOLLOWER_ADDRESS" "$WORK_DIR/cluster-1.log"
fi

if [[ -n "${CONFORMANCE_AGENT:-}" ]]; then
  AGENT="$CONFORMANCE_AGENT"
else
  case "$SDK" in
    rust)
      AGENT=rust ;;
    go)
      AGENT="$WORK_DIR/iggy-agent-go"
      (cd conformance/agents/go && go build -o "$AGENT" .) ;;
    python)
      # The first sync builds the SDK from foreign/python, which takes minutes.
      uv sync --quiet --project conformance/agents/python
      AGENT="conformance/agents/python/.venv/bin/python conformance/agents/python/main.py" ;;
    csharp)
      # The artifacts path keeps the SDK's bin and obj out of foreign/csharp.
      dotnet build conformance/agents/csharp/IggyAgent.csproj -c Release --nologo -v quiet \
        --artifacts-path "$WORK_DIR/csharp-agent"
      AGENT="dotnet $WORK_DIR/csharp-agent/bin/IggyAgent/release/iggy-agent.dll" ;;
    java)
      (cd conformance/agents/java && ./gradlew --no-daemon -q installDist)
      AGENT="conformance/agents/java/build/install/iggy-conformance-agent/bin/iggy-conformance-agent" ;;
    node)
      # The agent imports the SDK from foreign/node/dist, so the SDK builds first.
      (cd foreign/node && npm ci --silent && npm run --silent build)
      (cd conformance/agents/node && npm ci --silent && npm run --silent build)
      AGENT="node conformance/agents/node/dist/main.js" ;;
    cpp)
      cmake -S conformance/agents/cpp -B conformance/agents/cpp/build -DCMAKE_BUILD_TYPE=Release > /dev/null
      cmake --build conformance/agents/cpp/build -j > /dev/null
      AGENT="conformance/agents/cpp/build/iggy-agent" ;;
    php)
      docker build --quiet -f conformance/agents/php/Dockerfile -t iggy-conformance-php . > /dev/null
      AGENT="docker run -i --rm --init --network host iggy-conformance-php" ;;
    swift)
      if command -v swift > /dev/null; then
        swift build --package-path conformance/agents/swift/agent -Xswiftc -warnings-as-errors
        AGENT="conformance/agents/swift/agent/.build/debug/iggy-agent"
      else
        docker build --quiet -f conformance/agents/swift/Dockerfile -t iggy-conformance-swift . > /dev/null
        AGENT="docker run -i --rm --network host iggy-conformance-swift"
      fi ;;
  esac
fi

mkdir -p "$REPORTS_DIR"
status=0
"$CONFORMANCE" run \
  --tests conformance/tests \
  --server "$SINGLE_ADDRESS" \
  --no-quorum "$NO_QUORUM_ADDRESS" \
  "${CLUSTER_ARGS[@]}" \
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
