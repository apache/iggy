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

# The same 3-node cluster as ../cluster.sh, on a local kind cluster: one
# worker per Iggy node, each node installed with the repository's Helm chart
# in cluster mode (helm/charts/iggy/examples/cluster-3-node.yaml), with
# hostNetwork and node-local persistent volumes, as the chart requires.
# Implements the commands run-scenario.sh needs, so
#
#   CLUSTER=scripts/cluster-chaos/k8s/kind-cluster.sh scripts/cluster-chaos/run-scenario.sh crash-restart
#
# runs a scenario here. `kill` scales the Deployment to zero and force-deletes
# the pod (kubectl delete pod --force --grace-period=0), and `start` scales it
# back, so the replacement recovers from the same volume. The kubelet still
# sends SIGTERM first, so this is an abrupt stop rather than a SIGKILL crash.
# `pause` freezes the kind node container, kubelet included. `isolate` is not
# supported here. Only the produce and verify summaries (/work/*.json) are
# copied back; the produce logs stay in the checker pod.
#
# Usage: scripts/cluster-chaos/k8s/kind-cluster.sh <command> [node]
#   create | destroy    create or delete the kind cluster itself
#   up | down           (re)install the Iggy nodes and the checker pod / remove them
#   kill N | stop N | start N | pause N | unpause N | running N | logs N | addresses
#   (running fails once the server container restarted on its own)
#   checker NAME ARGS   run the checker in the checker pod, then copy /work to WORK
#
# Environment:
#   KIND_CLUSTER   kind cluster name (default iggy-chaos)
#   KIND_IMAGE     kind node image (default kindest/node:v1.35.0)
#   IMAGE          server image (default apache/iggy:edge), loaded into kind
#   CHECKER_IMAGE  checker image (default iggy-cluster-chaos-checker), loaded into kind
#   NODE_CPUS NODE_MEMORY NODE_SHARDS NODE_MEMORY_POOL (default 1, 2g, 1, 512MiB;
#                  NODE_MEMORY takes the Docker form, 2g or 512m)
#   WORK           host directory the checker's /work is copied to

set -Eeuo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
KIND_CLUSTER=${KIND_CLUSTER:-iggy-chaos}
KIND_IMAGE=${KIND_IMAGE:-kindest/node:v1.35.0}
IMAGE=${IMAGE:-apache/iggy:edge}
CHECKER_IMAGE=${CHECKER_IMAGE:-iggy-cluster-chaos-checker}
NAMESPACE=iggy-chaos
NODES=(0 1 2)
CHECKER_POD=chaos-checker
TCP_PORT=8090

kubectl() { command kubectl --context "kind-$KIND_CLUSTER" "$@"; }
helm() { command helm --kube-context "kind-$KIND_CLUSTER" "$@"; }
worker() { echo "$KIND_CLUSTER-worker$([ "$1" = 0 ] || echo $(($1 + 1)))"; }
worker_ip() { docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$(worker "$1")"; }
release() { echo "iggy-n$1"; }

create() {
  if kind get clusters | grep -Fxq "$KIND_CLUSTER"; then
    echo "kind cluster $KIND_CLUSTER exists"
    return
  fi
  kind create cluster --name "$KIND_CLUSTER" --image "$KIND_IMAGE" --wait 120s --config - <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
  - role: worker
EOF
}

destroy() { kind delete cluster --name "$KIND_CLUSTER"; }

image_repository() {
  case "${IMAGE##*/}" in
    *:*) echo "${IMAGE%:*}" ;;
    *) echo "$IMAGE" ;;
  esac
}
image_tag() {
  case "${IMAGE##*/}" in
    *:*) echo "${IMAGE##*:}" ;;
    *) echo latest ;;
  esac
}

# The chart's values for one release: the shared roster from the example,
# pointed at this cluster's worker IPs, with test-sized resources.
values() {
  local node=$1
  cat <<EOF
server:
  image:
    repository: $(image_repository)
    tag: "$(image_tag)"
    pullPolicy: IfNotPresent
  persistence:
    storageClass: standard
    size: 2Gi
  # Not under test here, and its config layout depends on the image version.
  encryption:
    enabled: false
  nodeSelector:
    kubernetes.io/hostname: $(worker "$node")
  cluster:
    selfReplicaId: $node
    nodes:
EOF
  for peer in "${NODES[@]}"; do
    cat <<EOF
      - name: iggy-node-$peer
        ip: $(worker_ip "$peer")
        replicaId: $peer
        ports:
          tcpReplica: 9090
EOF
  done
  cat <<EOF
  env:
    - name: RUST_LOG
      value: info
    - name: IGGY_HTTP_ADDRESS
      value: "0.0.0.0:3000"
    - name: IGGY_TCP_ADDRESS
      value: "0.0.0.0:$TCP_PORT"
    - name: IGGY_QUIC_ENABLED
      value: "false"
    - name: IGGY_WEBSOCKET_ENABLED
      value: "false"
    - name: IGGY_SHARDING_CPU_ALLOCATION
      value: "${NODE_SHARDS:-1}"
    - name: IGGY_SHARDING_PIN_CORES
      value: "false"
    - name: IGGY_MEMORY_POOL_SIZE
      value: "${NODE_MEMORY_POOL:-512MiB}"
resources:
  limits:
    cpu: "${NODE_CPUS:-1}"
    memory: $(memory_quantity "${NODE_MEMORY:-2g}")
EOF
}

up() {
  create
  down >/dev/null
  kind load docker-image --name "$KIND_CLUSTER" "$IMAGE" "$CHECKER_IMAGE"
  kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  # One Secret for every release: these values must match on all nodes.
  local jwt
  jwt=$(head -c 32 /dev/urandom | base64)
  kubectl -n "$NAMESPACE" create secret generic iggy-cluster-secrets \
    --from-literal=username=iggy --from-literal=password=iggy \
    --from-literal=clusterSharedSecret="$(head -c 32 /dev/urandom | base64)" \
    --from-literal=jwtEncodingSecret="$jwt" --from-literal=jwtDecodingSecret="$jwt" >/dev/null
  for node in "${NODES[@]}"; do
    values "$node" | helm upgrade --install "$(release "$node")" "$ROOT/helm/charts/iggy" \
      -n "$NAMESPACE" -f "$ROOT/helm/charts/iggy/examples/cluster-3-node.yaml" -f - >/dev/null
  done
  kubectl -n "$NAMESPACE" run "$CHECKER_POD" --image "$CHECKER_IMAGE" --image-pull-policy IfNotPresent \
    --restart Never --command --overrides '{"spec":{"nodeSelector":{"node-role.kubernetes.io/control-plane":""},"tolerations":[{"operator":"Exists"}]}}' \
    -- sh -c 'mkdir -p /work && exec sleep infinity' >/dev/null
  kubectl -n "$NAMESPACE" wait --for=condition=Ready "pod/$CHECKER_POD" --timeout 120s >/dev/null
  echo "cluster up: $(addresses) ($IMAGE)"
}

down() {
  for node in "${NODES[@]}"; do
    helm uninstall "$(release "$node")" -n "$NAMESPACE" --wait >/dev/null 2>&1 || true
  done
  kubectl delete namespace "$NAMESPACE" --wait >/dev/null 2>&1 || true
  echo "cluster down"
}

addresses() {
  local list=()
  for node in "${NODES[@]}"; do list+=("$(worker_ip "$node"):$TCP_PORT"); done
  (IFS=,; echo "${list[*]}")
}

# Kubernetes quantities spell gigabytes Gi, Docker spells them g.
memory_quantity() {
  case "$1" in
    *[gG]) echo "${1%?}Gi" ;;
    *[mM]) echo "${1%?}Mi" ;;
    *) echo "$1" ;;
  esac
}

pod_selector() { echo "app.kubernetes.io/instance=$(release "$1")"; }

# Scale to zero first so nothing replaces the pod, then force-delete it so it
# does not wait out its termination grace period.
kill_node() {
  scale "$1" 0
  kubectl -n "$NAMESPACE" delete pod -l "$(pod_selector "$1")" --force --grace-period 0 >/dev/null 2>&1
}

scale() {
  kubectl -n "$NAMESPACE" scale "$(kubectl -n "$NAMESPACE" get deploy -l "$(pod_selector "$1")" -o name)" \
    --replicas "$2" >/dev/null
}

checker() {
  local status=0
  shift
  kubectl -n "$NAMESPACE" exec "$CHECKER_POD" -- cluster-chaos-checker "$@" || status=$?
  kubectl -n "$NAMESPACE" exec "$CHECKER_POD" -- sh -c 'cd /work && tar -cf - -- *.json 2>/dev/null' \
    | tar -C "${WORK:?set WORK}" -xf - 2>/dev/null || true
  return "$status"
}

node=${2:-}
case "${1:-}" in
  create) create ;;
  destroy) destroy ;;
  up) up ;;
  down) down ;;
  addresses) addresses ;;
  kill) kill_node "$node" ;;
  stop)
    scale "$node" 0
    kubectl -n "$NAMESPACE" wait --for=delete pod -l "$(pod_selector "$node")" --timeout 60s >/dev/null
    ;;
  start) scale "$node" 1 ;;
  pause) docker pause "$(worker "$node")" >/dev/null ;;
  unpause) docker unpause "$(worker "$node")" >/dev/null ;;
  isolate | heal) echo "$1 is not supported on kind; use the Docker backend" >&2; exit 3 ;;
  running)
    [ "$(kubectl -n "$NAMESPACE" get pod -l "$(pod_selector "$node")" \
      -o jsonpath='{.items[*].status.containerStatuses[*].ready}/{.items[*].status.containerStatuses[*].restartCount}')" = true/0 ]
    ;;
  logs)
    kubectl -n "$NAMESPACE" logs -l "$(pod_selector "$node")" --tail -1 --previous 2>/dev/null || true
    kubectl -n "$NAMESPACE" logs -l "$(pod_selector "$node")" --tail -1
    ;;
  checker) shift; checker "$@" ;;
  *)
    sed -n '/^# Usage:/,/^# *WORK/s/^# \{0,1\}//p' "$0" >&2
    exit 1
    ;;
esac
