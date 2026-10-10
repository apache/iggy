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

# A 3-node Iggy VSR cluster in Docker on one host. Each node is a container
# with a fixed IP on a dedicated network and its data in a named volume, so
# `kill` + `start` is a real process crash followed by recovery from disk.
#
# Usage: scripts/cluster-chaos/cluster.sh <command> [node]
#   up                  fresh cluster (wipes the volumes)
#   down                remove the containers, volumes and network
#   kill N              SIGKILL node N
#   stop N              graceful stop (SIGTERM, 30 s grace)
#   start N             start node N again from its volume
#   isolate N | heal N  cut every link of node N (peers and clients) / restore
#   pause N | unpause N freeze node N (cgroup freezer) / thaw
#   running N           succeed when node N's process is running
#   logs N              node N's log
#   addresses           client (TCP) addresses, comma-separated
#   checker NAME ARGS   run the checker image on the cluster network
#
# Environment:
#   IMAGE          server image (default apache/iggy:edge)
#   CHECKER_IMAGE  checker image (default iggy-cluster-chaos-checker)
#   CHAOS_PREFIX   container, network and volume prefix (default iggy-chaos)
#   CHAOS_SUBNET   first three octets of the network (default 172.31.250)
#   NODE_CPUS, NODE_MEMORY, NODE_SHARDS, NODE_MEMORY_POOL
#                  per-node limits (default 1, 2g, 1, 512MiB)
#   CHECKER_CPUS, CHECKER_MEMORY (default 2, 2g)
#   WORK           host directory mounted as /work in checker containers
#   RUST_LOG       server log filter (default info)

set -Eeuo pipefail

IMAGE=${IMAGE:-apache/iggy:edge}
CHECKER_IMAGE=${CHECKER_IMAGE:-iggy-cluster-chaos-checker}
PREFIX=${CHAOS_PREFIX:-iggy-chaos}
SUBNET=${CHAOS_SUBNET:-172.31.250}
NODES=(0 1 2)
TCP_PORT=8090
# Shared by every node and only valid for this throwaway cluster.
CLUSTER_SECRET=cluster-chaos-test-secret-0123456789abcdef

ip_of() { echo "$SUBNET.1$1"; }

run_node() {
  local node=$1
  local roster=()
  for peer in "${NODES[@]}"; do
    roster+=(
      -e "IGGY_CLUSTER_NODES_${peer}_NAME=$PREFIX-$peer"
      -e "IGGY_CLUSTER_NODES_${peer}_IP=$(ip_of "$peer")"
      -e "IGGY_CLUSTER_NODES_${peer}_REPLICA_ID=$peer"
      -e "IGGY_CLUSTER_NODES_${peer}_PORTS_TCP=$TCP_PORT"
      -e "IGGY_CLUSTER_NODES_${peer}_PORTS_QUIC=8080"
      -e "IGGY_CLUSTER_NODES_${peer}_PORTS_HTTP=3000"
      -e "IGGY_CLUSTER_NODES_${peer}_PORTS_WEBSOCKET=8092"
      -e "IGGY_CLUSTER_NODES_${peer}_PORTS_TCP_REPLICA=9090"
    )
  done
  # io_uring needs seccomp unconfined and an unlimited memlock, as in
  # bdd/docker-compose.cluster.yml.
  docker run -d --name "$PREFIX-$node" --hostname "$PREFIX-$node" \
    --network "$PREFIX" --ip "$(ip_of "$node")" \
    --cpus "${NODE_CPUS:-1}" --memory "${NODE_MEMORY:-2g}" \
    --cap-add SYS_NICE --security-opt seccomp=unconfined --ulimit memlock=-1:-1 \
    -v "$PREFIX-$node:/app/local_data" \
    -e RUST_LOG="${RUST_LOG:-info}" \
    -e IGGY_ROOT_USERNAME=iggy -e IGGY_ROOT_PASSWORD=iggy \
    -e IGGY_TCP_ADDRESS="0.0.0.0:$TCP_PORT" -e IGGY_HTTP_ADDRESS=0.0.0.0:3000 \
    -e IGGY_QUIC_ENABLED=false -e IGGY_WEBSOCKET_ENABLED=false \
    -e IGGY_SHARDING_CPU_ALLOCATION="${NODE_SHARDS:-1}" -e IGGY_SHARDING_PIN_CORES=false \
    -e IGGY_MEMORY_POOL_SIZE="${NODE_MEMORY_POOL:-512MiB}" \
    -e IGGY_CLUSTER_ENABLED=true -e IGGY_CLUSTER_NAME=cluster-chaos \
    -e IGGY_CLUSTER_AUTH_ENABLED=true -e IGGY_CLUSTER_AUTH_SHARED_SECRET="$CLUSTER_SECRET" \
    "${roster[@]}" "$IMAGE" --replica-id "$node" >/dev/null
}

up() {
  down >/dev/null
  docker network create --subnet "$SUBNET.0/24" "$PREFIX" >/dev/null
  for node in "${NODES[@]}"; do run_node "$node"; done
  echo "cluster up: $(addresses) ($IMAGE)"
}

down() {
  docker ps -aq --filter "name=^$PREFIX-checker-" | xargs -r docker rm -f >/dev/null 2>&1 || true
  for node in "${NODES[@]}"; do
    docker rm -fv "$PREFIX-$node" >/dev/null 2>&1 || true
    docker volume rm "$PREFIX-$node" >/dev/null 2>&1 || true
  done
  docker network rm "$PREFIX" >/dev/null 2>&1 || true
  echo "cluster down"
}

addresses() {
  local list=()
  for node in "${NODES[@]}"; do list+=("$(ip_of "$node"):$TCP_PORT"); done
  (IFS=,; echo "${list[*]}")
}

checker() {
  local name=$1
  shift
  docker run --rm --name "$PREFIX-checker-$name" --network "$PREFIX" \
    --cpus "${CHECKER_CPUS:-2}" --memory "${CHECKER_MEMORY:-2g}" \
    --user "$(id -u):$(id -g)" -v "${WORK:?set WORK}:/work" \
    "$CHECKER_IMAGE" "$@"
}

node=${2:-}
case "${1:-}" in
  up) up ;;
  down) down ;;
  addresses) addresses ;;
  kill) docker kill -s KILL "$PREFIX-$node" >/dev/null ;;
  stop) docker stop -t 30 "$PREFIX-$node" >/dev/null ;;
  start) docker start "$PREFIX-$node" >/dev/null ;;
  isolate) docker network disconnect "$PREFIX" "$PREFIX-$node" ;;
  heal) docker network connect --ip "$(ip_of "$node")" "$PREFIX" "$PREFIX-$node" ;;
  pause) docker pause "$PREFIX-$node" >/dev/null ;;
  unpause) docker unpause "$PREFIX-$node" >/dev/null ;;
  running) [ "$(docker inspect -f '{{.State.Running}}' "$PREFIX-$node" 2>/dev/null)" = true ] ;;
  logs) docker logs "$PREFIX-$node" 2>&1 ;;
  checker) shift; checker "$@" ;;
  *)
    sed -n '/^# Usage:/,/^# *RUST_LOG/s/^# \{0,1\}//p' "$0" >&2
    exit 1
    ;;
esac
