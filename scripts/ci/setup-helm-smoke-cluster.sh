#!/usr/bin/env bash
#
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
#

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/ci/setup-helm-smoke-cluster.sh

Create or reuse the kind cluster used by the Helm smoke test and ensure
ingress-nginx is installed and ready in that cluster.

Environment:
  HELM_SMOKE_KIND_NAME               kind cluster name (default: iggy-helm-smoke)
  HELM_SMOKE_KIND_IMAGE              kind node image (default: kindest/node:v1.35.0)
  HELM_SMOKE_KIND_PLATFORM           docker platform for the kind node image
  HELM_SMOKE_KIND_WAIT               kind create wait timeout (default: 120s)
  HELM_SMOKE_INGRESS_NGINX_VERSION   ingress-nginx controller tag (default: controller-v1.15.1)
  HELM_SMOKE_INGRESS_NGINX_TIMEOUT   rollout timeout (default: 5m)
EOF
}

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ] || [ "${1:-}" = "help" ]; then
  usage
  exit 0
fi

if [ "$#" -ne 0 ]; then
  usage
  exit 1
fi

HELM_SMOKE_KIND_NAME="${HELM_SMOKE_KIND_NAME:-iggy-helm-smoke}"
HELM_SMOKE_KIND_IMAGE="${HELM_SMOKE_KIND_IMAGE:-kindest/node:v1.35.0}"
HELM_SMOKE_KIND_PLATFORM="${HELM_SMOKE_KIND_PLATFORM:-}"
HELM_SMOKE_KIND_WAIT="${HELM_SMOKE_KIND_WAIT:-120s}"
HELM_SMOKE_INGRESS_NGINX_VERSION="${HELM_SMOKE_INGRESS_NGINX_VERSION:-controller-v1.15.1}"
HELM_SMOKE_INGRESS_NGINX_TIMEOUT="${HELM_SMOKE_INGRESS_NGINX_TIMEOUT:-5m}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: required command '$1' not found" >&2
    exit 1
  fi
}

duration_to_seconds() {
  local duration="$1"
  local total=0
  local value
  local unit
  local multiplier

  if [[ "$duration" =~ ^[0-9]+$ ]]; then
    echo "$duration"
    return 0
  fi

  while [ -n "$duration" ]; do
    if [[ "$duration" =~ ^([0-9]+)([smhd])(.*)$ ]]; then
      value="${BASH_REMATCH[1]}"
      unit="${BASH_REMATCH[2]}"
      duration="${BASH_REMATCH[3]}"
      case "$unit" in
        s) multiplier=1 ;;
        m) multiplier=60 ;;
        h) multiplier=3600 ;;
        d) multiplier=86400 ;;
      esac
      total=$((total + (value * multiplier)))
      continue
    fi

    echo "Error: unsupported timeout format '$1'" >&2
    echo "Use plain seconds or a combination of s, m, h, or d units such as 300, 5m, or 1h30m." >&2
    return 1
  done

  echo "$total"
}

wait_for_completed_job() {
  local job_name="$1"

  if ! kubectl -n ingress-nginx get "job/${job_name}" >/dev/null 2>&1; then
    return 0
  fi

  kubectl -n ingress-nginx wait \
    --for=condition=complete \
    "job/${job_name}" \
    --timeout="$HELM_SMOKE_INGRESS_NGINX_TIMEOUT"
}

wait_for_ingress_validation() {
  local sleep_seconds=4
  local validation_timeout_seconds
  local deadline
  local probe_file

  validation_timeout_seconds="$(duration_to_seconds "$HELM_SMOKE_INGRESS_NGINX_TIMEOUT")"
  deadline=$((SECONDS + validation_timeout_seconds))
  probe_file="$(mktemp "${TMPDIR:-/tmp}/ingress-nginx-probe.XXXXXX.yaml")"

  cat > "$probe_file" <<'EOF'
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: ingress-nginx-readiness-probe
  namespace: ingress-nginx
spec:
  ingressClassName: nginx
  rules:
    - host: readiness-probe.iggy.local
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: ingress-nginx-controller
                port:
                  number: 80
EOF

  while (( SECONDS < deadline )); do
    if kubectl apply --dry-run=server -f "$probe_file" >/dev/null 2>&1; then
      rm -f "$probe_file"
      return 0
    fi
    sleep "$sleep_seconds"
  done

  echo "Error: ingress-nginx admission webhook did not become ready within ${HELM_SMOKE_INGRESS_NGINX_TIMEOUT}" >&2
  if ! kubectl apply --dry-run=server -f "$probe_file"; then
    rm -f "$probe_file"
    return 1
  fi
  rm -f "$probe_file"
}

require_command kind
require_command kubectl

if [ -n "$HELM_SMOKE_KIND_PLATFORM" ]; then
  require_command docker
fi

kind_context="kind-${HELM_SMOKE_KIND_NAME}"
kind_config="$(mktemp "${TMPDIR:-/tmp}/iggy-kind.XXXXXX.yaml")"
trap 'rm -f "$kind_config"' EXIT

cat > "$kind_config" <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    kubeadmConfigPatches:
      - |
        kind: InitConfiguration
        nodeRegistration:
          kubeletExtraArgs:
            node-labels: "ingress-ready=true"
    extraPortMappings:
      - containerPort: 80
        hostPort: 80
        protocol: TCP
      - containerPort: 443
        hostPort: 443
        protocol: TCP
EOF

cluster_exists=false
if kind get clusters | grep -Fxq "$HELM_SMOKE_KIND_NAME"; then
  cluster_exists=true
fi

if [ "$cluster_exists" = true ] && [ -n "$HELM_SMOKE_KIND_PLATFORM" ]; then
  current_kind_image_id="$(
    docker inspect "${HELM_SMOKE_KIND_NAME}-control-plane" \
      --format '{{.Image}}' 2>/dev/null || true
  )"
  current_kind_platform="$(
    docker image inspect "$current_kind_image_id" \
      --format '{{.Os}}/{{.Architecture}}' 2>/dev/null || true
  )"
  if [ -n "$current_kind_platform" ] && [ "$current_kind_platform" != "$HELM_SMOKE_KIND_PLATFORM" ]; then
    echo "Recreating kind cluster '$HELM_SMOKE_KIND_NAME' to switch from ${current_kind_platform} to ${HELM_SMOKE_KIND_PLATFORM}"
    kind delete cluster --name "$HELM_SMOKE_KIND_NAME"
    cluster_exists=false
  fi
fi

if [ "$cluster_exists" = true ]; then
  echo "Reusing existing kind cluster '$HELM_SMOKE_KIND_NAME'"
else
  if [ -n "$HELM_SMOKE_KIND_PLATFORM" ]; then
    echo "Pulling ${HELM_SMOKE_KIND_IMAGE} for ${HELM_SMOKE_KIND_PLATFORM}"
    docker pull --platform "$HELM_SMOKE_KIND_PLATFORM" "$HELM_SMOKE_KIND_IMAGE" >/dev/null
  fi
  kind create cluster \
    --name "$HELM_SMOKE_KIND_NAME" \
    --wait "$HELM_SMOKE_KIND_WAIT" \
    --config "$kind_config" \
    --image "$HELM_SMOKE_KIND_IMAGE"
fi

kubectl config use-context "$kind_context" >/dev/null
kubectl apply -f "https://raw.githubusercontent.com/kubernetes/ingress-nginx/${HELM_SMOKE_INGRESS_NGINX_VERSION}/deploy/static/provider/kind/deploy.yaml"
kubectl -n ingress-nginx rollout status deployment/ingress-nginx-controller --timeout="$HELM_SMOKE_INGRESS_NGINX_TIMEOUT"
kubectl -n ingress-nginx wait \
  --for=condition=ready \
  pod \
  --selector=app.kubernetes.io/component=controller \
  --timeout="$HELM_SMOKE_INGRESS_NGINX_TIMEOUT"
wait_for_completed_job ingress-nginx-admission-create
wait_for_completed_job ingress-nginx-admission-patch
wait_for_ingress_validation
