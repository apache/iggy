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
the Gateway API CRDs and Envoy Gateway are installed and ready.

Environment:
  HELM_SMOKE_KIND_NAME              kind cluster name (default: iggy-helm-smoke)
  HELM_SMOKE_KIND_IMAGE             kind node image (default: kindest/node:v1.35.0)
  HELM_SMOKE_KIND_PLATFORM          docker platform for the kind node image
  HELM_SMOKE_KIND_WAIT              kind create wait timeout (default: 120s)
  HELM_SMOKE_GATEWAY_API_VERSION    Gateway API CRD release tag (default: v1.5.0)
  HELM_SMOKE_ENVOY_GATEWAY_VERSION  Envoy Gateway Helm chart version (default: v1.4.1)
  HELM_SMOKE_GATEWAY_TIMEOUT        gateway readiness timeout (default: 5m)
  HELM_SMOKE_GATEWAY_NAMESPACE      namespace for Envoy Gateway and the Gateway resource (default: envoy-gateway-system)
  HELM_SMOKE_GATEWAY_NAME           name of the Gateway resource (default: iggy-smoke-gateway)
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
HELM_SMOKE_GATEWAY_API_VERSION="${HELM_SMOKE_GATEWAY_API_VERSION:-v1.5.0}"
HELM_SMOKE_ENVOY_GATEWAY_VERSION="${HELM_SMOKE_ENVOY_GATEWAY_VERSION:-v1.4.1}"
HELM_SMOKE_GATEWAY_TIMEOUT="${HELM_SMOKE_GATEWAY_TIMEOUT:-5m}"
HELM_SMOKE_GATEWAY_NAMESPACE="${HELM_SMOKE_GATEWAY_NAMESPACE:-envoy-gateway-system}"
HELM_SMOKE_GATEWAY_NAME="${HELM_SMOKE_GATEWAY_NAME:-iggy-smoke-gateway}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: required command '$1' not found" >&2
    exit 1
  fi
}

require_command kind
require_command kubectl
require_command helm

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

# Install EG first so its bundled GW API CRDs land before the v1.5 safe-upgrade VAP.
# We then upgrade those CRDs to the pinned version via server-side apply.
echo "Installing Envoy Gateway ${HELM_SMOKE_ENVOY_GATEWAY_VERSION}..."
helm upgrade --install eg \
  oci://docker.io/envoyproxy/gateway-helm \
  --version "$HELM_SMOKE_ENVOY_GATEWAY_VERSION" \
  --namespace "$HELM_SMOKE_GATEWAY_NAMESPACE" \
  --create-namespace \
  --wait \
  --timeout "$HELM_SMOKE_GATEWAY_TIMEOUT"

echo "Upgrading Gateway API CRDs to ${HELM_SMOKE_GATEWAY_API_VERSION}..."
kubectl apply --server-side --force-conflicts \
  -f "https://github.com/kubernetes-sigs/gateway-api/releases/download/${HELM_SMOKE_GATEWAY_API_VERSION}/standard-install.yaml"

echo "Creating EnvoyProxy, GatewayClass, and Gateway..."
kubectl apply -f - <<EOF
apiVersion: gateway.envoyproxy.io/v1alpha1
kind: EnvoyProxy
metadata:
  name: iggy-smoke-proxy
  namespace: ${HELM_SMOKE_GATEWAY_NAMESPACE}
spec:
  provider:
    type: Kubernetes
    kubernetes:
      envoyService:
        type: NodePort
---
apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata:
  name: iggy-smoke
spec:
  controllerName: gateway.envoyproxy.io/gatewayclass-controller
  parametersRef:
    group: gateway.envoyproxy.io
    kind: EnvoyProxy
    name: iggy-smoke-proxy
    namespace: ${HELM_SMOKE_GATEWAY_NAMESPACE}
---
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: ${HELM_SMOKE_GATEWAY_NAME}
  namespace: ${HELM_SMOKE_GATEWAY_NAMESPACE}
spec:
  gatewayClassName: iggy-smoke
  listeners:
    - name: http
      protocol: HTTP
      port: 80
      allowedRoutes:
        namespaces:
          from: All
EOF

echo "Waiting for Gateway to be programmed..."
kubectl wait \
  --for=condition=Programmed \
  "gateway/${HELM_SMOKE_GATEWAY_NAME}" \
  --namespace "$HELM_SMOKE_GATEWAY_NAMESPACE" \
  --timeout="$HELM_SMOKE_GATEWAY_TIMEOUT"
