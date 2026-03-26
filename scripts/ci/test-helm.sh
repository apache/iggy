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
Usage: scripts/ci/test-helm.sh <validate|smoke|cleanup-smoke|collect-smoke-diagnostics> [--cleanup]

Commands:
  validate                  Run Helm lint and render validation scenarios.
  smoke                     Run the Helm runtime smoke scenario against the current Kubernetes context.
  cleanup-smoke             Remove the Helm smoke release namespace and any failed-install leftovers.
  collect-smoke-diagnostics Collect diagnostics for the Helm smoke namespace.

Notes:
  - validate requires helm.
  - smoke requires helm, kubectl, and curl, plus an existing cluster and ingress controller.
  - pass --cleanup with smoke to remove the Helm smoke namespace after a successful run.
  - cleanup-smoke requires kubectl and optionally helm.
  - collect-smoke-diagnostics is best-effort and does not fail on missing resources.
EOF
}

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

CHART_DIR="${CHART_DIR:-helm/charts/iggy}"
HELM_RENDER_DIR="${HELM_RENDER_DIR:-/tmp/helm-render}"
HELM_SMOKE_NAMESPACE="${HELM_SMOKE_NAMESPACE:-iggy-smoke}"
HELM_SMOKE_RELEASE="${HELM_SMOKE_RELEASE:-iggy-smoke}"
HELM_SMOKE_REPORT_DIR="${HELM_SMOKE_REPORT_DIR:-reports/helm-smoke}"
HELM_SMOKE_SERVER_HOST="${HELM_SMOKE_SERVER_HOST:-server.iggy.local}"
HELM_SMOKE_UI_HOST="${HELM_SMOKE_UI_HOST:-ui.iggy.local}"
HELM_SMOKE_TIMEOUT="${HELM_SMOKE_TIMEOUT:-5m}"
HELM_SMOKE_INGRESS_CLASS="${HELM_SMOKE_INGRESS_CLASS:-nginx}"
HELM_SMOKE_KIND_NAME="${HELM_SMOKE_KIND_NAME:-iggy-helm-smoke}"
HELM_SMOKE_SERVER_CPU_ALLOCATION="${HELM_SMOKE_SERVER_CPU_ALLOCATION:-1}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: required command '$1' not found" >&2
    exit 1
  fi
}

extract_chart_field() {
  local field="$1"
  local value

  value="$(
    awk -v field="$field" '
      $1 == field ":" {
        gsub(/"/, "", $2)
        print $2
        exit
      }
    ' "$CHART_DIR/Chart.yaml"
  )"

  if [ -z "$value" ]; then
    echo "Error: could not extract '$field' from $CHART_DIR/Chart.yaml" >&2
    exit 1
  fi

  printf '%s\n' "$value"
}

extract_values_tag() {
  local section="$1"
  local value

  value="$(
    awk -v section="$section" '
      $1 == section ":" {
        in_section = 1
        next
      }
      in_section && /^[^[:space:]]/ {
        in_section = 0
      }
      in_section && $1 == "tag:" {
        gsub(/"/, "", $2)
        print $2
        exit
      }
    ' "$CHART_DIR/values.yaml"
  )"

  if [ -z "$value" ]; then
    echo "Error: could not extract '$section.image.tag' from $CHART_DIR/values.yaml" >&2
    exit 1
  fi

  printf '%s\n' "$value"
}

prepare_render_dir() {
  if [ -z "$HELM_RENDER_DIR" ] || [ "$HELM_RENDER_DIR" = "/" ]; then
    echo "Error: HELM_RENDER_DIR must not be empty or /" >&2
    exit 1
  fi

  rm -rf "$HELM_RENDER_DIR"
  mkdir -p "$HELM_RENDER_DIR"
}

extract_kind_names() {
  local file="$1"
  local kind="$2"

  awk -v kind="$kind" '
    /^kind: / {
      current_kind = $2
      in_metadata = 0
      next
    }
    /^metadata:$/ {
      in_metadata = 1
      next
    }
    in_metadata && /^  name: / {
      if (current_kind == kind) {
        print $2
      }
      in_metadata = 0
    }
  ' "$file"
}

validate() {
  require_command helm

  local chart_version
  local chart_app_version
  local server_image_tag
  local ui_image_tag

  chart_version="$(extract_chart_field version)"
  chart_app_version="$(extract_chart_field appVersion)"
  server_image_tag="$(extract_values_tag server)"
  ui_image_tag="$(extract_values_tag ui)"

  prepare_render_dir

  helm lint --strict "$CHART_DIR"

  helm template iggy "$CHART_DIR" > "$HELM_RENDER_DIR/default.yaml"
  test "$(grep -c '^kind: Deployment$' "$HELM_RENDER_DIR/default.yaml")" -eq 2
  test "$(grep -c '^kind: Service$' "$HELM_RENDER_DIR/default.yaml")" -eq 2
  test "$(grep -c '^kind: ServiceAccount$' "$HELM_RENDER_DIR/default.yaml")" -eq 1
  test "$(grep -c '^kind: Secret$' "$HELM_RENDER_DIR/default.yaml")" -eq 1
  grep -q "helm.sh/chart: iggy-${chart_version}" "$HELM_RENDER_DIR/default.yaml"
  grep -q "helm.sh/chart: iggy-ui-${chart_version}" "$HELM_RENDER_DIR/default.yaml"
  grep -q "app.kubernetes.io/version: \"${chart_app_version}\"" "$HELM_RENDER_DIR/default.yaml"
  grep -q "image: \"apache/iggy:${server_image_tag}\"" "$HELM_RENDER_DIR/default.yaml"
  grep -q "image: \"apache/iggy-web-ui:${ui_image_tag}\"" "$HELM_RENDER_DIR/default.yaml"

  helm template iggy "$CHART_DIR" \
    --set server.persistence.enabled=true \
    --set autoscaling.enabled=true \
    --set autoscaling.targetCPUUtilizationPercentage=80 \
    --set server.ingress.enabled=true \
    --set ui.ingress.enabled=true \
    --set server.serviceMonitor.enabled=true \
    > "$HELM_RENDER_DIR/all-features.yaml"
  grep -q '^kind: PersistentVolumeClaim$' "$HELM_RENDER_DIR/all-features.yaml"
  grep -q '^kind: HorizontalPodAutoscaler$' "$HELM_RENDER_DIR/all-features.yaml"
  test "$(grep -c '^kind: Ingress$' "$HELM_RENDER_DIR/all-features.yaml")" -eq 2
  test "$(extract_kind_names "$HELM_RENDER_DIR/all-features.yaml" Ingress | sort -u | wc -l | tr -d ' ')" -eq 2
  extract_kind_names "$HELM_RENDER_DIR/all-features.yaml" Ingress | grep -qx 'iggy'
  extract_kind_names "$HELM_RENDER_DIR/all-features.yaml" Ingress | grep -qx 'iggy-ui'
  grep -q '^kind: ServiceMonitor$' "$HELM_RENDER_DIR/all-features.yaml"

  helm template iggy "$CHART_DIR" \
    --kube-version 1.18.0 \
    --api-versions networking.k8s.io/v1beta1 \
    --api-versions autoscaling/v2beta2 \
    --set autoscaling.enabled=true \
    --set autoscaling.targetCPUUtilizationPercentage=80 \
    --set server.ingress.enabled=true \
    --set ui.ingress.enabled=true \
    > "$HELM_RENDER_DIR/legacy-k8s-1.18.yaml"
  test "$(grep -c '^apiVersion: networking.k8s.io/v1beta1$' "$HELM_RENDER_DIR/legacy-k8s-1.18.yaml")" -eq 2
  grep -q '^apiVersion: autoscaling/v2beta2$' "$HELM_RENDER_DIR/legacy-k8s-1.18.yaml"

  helm template iggy "$CHART_DIR" --set ui.enabled=false > "$HELM_RENDER_DIR/server-only.yaml"
  test "$(grep -c '^kind: Deployment$' "$HELM_RENDER_DIR/server-only.yaml")" -eq 1
  test "$(grep -c '^kind: Service$' "$HELM_RENDER_DIR/server-only.yaml")" -eq 1

  helm template iggy "$CHART_DIR" --set server.enabled=false > "$HELM_RENDER_DIR/ui-only.yaml"
  test "$(grep -c '^kind: Deployment$' "$HELM_RENDER_DIR/ui-only.yaml")" -eq 1
  test "$(grep -c '^kind: Service$' "$HELM_RENDER_DIR/ui-only.yaml")" -eq 1

  helm template iggy "$CHART_DIR" \
    --set server.users.root.createSecret=false \
    --set server.users.root.existingSecret.name=supersecret \
    > "$HELM_RENDER_DIR/existing-secret.yaml"
  if grep -q 'root-credentials' "$HELM_RENDER_DIR/existing-secret.yaml"; then
    echo "Error: existing-secret render should not include generated root credentials" >&2
    exit 1
  fi
  grep -q 'name: supersecret' "$HELM_RENDER_DIR/existing-secret.yaml"
}

smoke() {
  local cleanup_after_success="$1"
  require_command helm
  require_command kubectl
  require_command curl

  local ui_image_tag
  local server_ping_status
  local ui_healthz_status
  local leftover_resources
  local helm_status
  local smoke_values_file

  mkdir -p "$HELM_SMOKE_REPORT_DIR"

  if ! helm status "$HELM_SMOKE_RELEASE" -n "$HELM_SMOKE_NAMESPACE" >/dev/null 2>&1; then
    leftover_resources="$(
      kubectl -n "$HELM_SMOKE_NAMESPACE" get deployment,service,ingress,secret,serviceaccount \
        -l "app.kubernetes.io/instance=${HELM_SMOKE_RELEASE}" \
        -o name 2>/dev/null || true
    )"
    if [ -n "$leftover_resources" ]; then
      cat >&2 <<EOF
Found leftover resources for a failed Helm smoke install in namespace '${HELM_SMOKE_NAMESPACE}'.
Run 'scripts/ci/test-helm.sh cleanup-smoke' once, then rerun the smoke test.
EOF
      printf '%s\n' "$leftover_resources" >&2
      exit 1
    fi
  fi

  ui_image_tag="$(extract_values_tag ui)"

  echo "$ui_image_tag" > "$HELM_SMOKE_REPORT_DIR/ui-image-tag.txt"

  smoke_values_file="$(mktemp "${TMPDIR:-/tmp}/iggy-helm-smoke-values.XXXXXX.yaml")"

  cat > "$smoke_values_file" <<EOF
server:
  ingress:
    enabled: true
    className: ${HELM_SMOKE_INGRESS_CLASS}
    hosts:
      - host: ${HELM_SMOKE_SERVER_HOST}
        paths:
          - path: /
            pathType: Prefix
  env:
    - name: RUST_LOG
      value: info
    - name: IGGY_HTTP_ADDRESS
      value: "0.0.0.0:3000"
    - name: IGGY_TCP_ADDRESS
      value: "0.0.0.0:8090"
    - name: IGGY_QUIC_ADDRESS
      value: "0.0.0.0:8080"
    - name: IGGY_WEBSOCKET_ADDRESS
      value: "0.0.0.0:8092"
    - name: IGGY_SYSTEM_SHARDING_CPU_ALLOCATION
      value: "${HELM_SMOKE_SERVER_CPU_ALLOCATION}"
ui:
  ingress:
    enabled: true
    className: ${HELM_SMOKE_INGRESS_CLASS}
    hosts:
      - host: ${HELM_SMOKE_UI_HOST}
        paths:
          - path: /
            pathType: Prefix
  image:
    tag: "${ui_image_tag}"
EOF

  helm_status=0
  if helm upgrade --install "$HELM_SMOKE_RELEASE" "$CHART_DIR" \
    --atomic \
    --namespace "$HELM_SMOKE_NAMESPACE" \
    --create-namespace \
    --wait \
    --timeout "$HELM_SMOKE_TIMEOUT" \
    -f "$smoke_values_file"; then
    helm_status=0
  else
    helm_status=$?
  fi

  rm -f "$smoke_values_file"

  if [ "$helm_status" -ne 0 ]; then
    return "$helm_status"
  fi

  kubectl version --client=true > "$HELM_SMOKE_REPORT_DIR/kubectl-version.txt"
  kubectl -n "$HELM_SMOKE_NAMESPACE" rollout status "deployment/$HELM_SMOKE_RELEASE" --timeout="$HELM_SMOKE_TIMEOUT"
  kubectl -n "$HELM_SMOKE_NAMESPACE" rollout status "deployment/${HELM_SMOKE_RELEASE}-ui" --timeout="$HELM_SMOKE_TIMEOUT"
  kubectl -n "$HELM_SMOKE_NAMESPACE" get pods,svc,ingress > "$HELM_SMOKE_REPORT_DIR/resources.txt"

  for _ in $(seq 1 30); do
    server_ping_status="$(
      curl -sS -o "$HELM_SMOKE_REPORT_DIR/ping.txt" -w '%{http_code}' \
        -H "Host: ${HELM_SMOKE_SERVER_HOST}" \
        http://127.0.0.1/ping || true
    )"
    if [ "$server_ping_status" = "200" ] && grep -qx 'pong' "$HELM_SMOKE_REPORT_DIR/ping.txt"; then
      break
    fi
    sleep 2
  done

  test "$server_ping_status" = "200"
  grep -qx 'pong' "$HELM_SMOKE_REPORT_DIR/ping.txt"

  for _ in $(seq 1 30); do
    ui_healthz_status="$(
      curl -sS -o "$HELM_SMOKE_REPORT_DIR/ui-healthz.txt" -w '%{http_code}' \
        -H "Host: ${HELM_SMOKE_UI_HOST}" \
        http://127.0.0.1/healthz || true
    )"
    if [ "$ui_healthz_status" = "200" ]; then
      break
    fi
    sleep 2
  done

  test "$ui_healthz_status" = "200"

  if [ "$cleanup_after_success" = true ]; then
    cleanup_smoke
  fi
}

cleanup_smoke() {
  require_command kubectl

  if command -v helm >/dev/null 2>&1; then
    helm uninstall "$HELM_SMOKE_RELEASE" -n "$HELM_SMOKE_NAMESPACE" >/dev/null 2>&1 || true
  fi

  kubectl delete namespace "$HELM_SMOKE_NAMESPACE" --ignore-not-found=true --wait=true >/dev/null 2>&1 || true
}

collect_smoke_diagnostics() {
  set +e

  mkdir -p "$HELM_SMOKE_REPORT_DIR"

  helm list -A > "$HELM_SMOKE_REPORT_DIR/helm-list.txt" 2>&1 || true
  kubectl get namespaces > "$HELM_SMOKE_REPORT_DIR/namespaces.txt" 2>&1 || true
  kubectl -n ingress-nginx get all > "$HELM_SMOKE_REPORT_DIR/ingress-nginx.txt" 2>&1 || true
  kubectl -n "$HELM_SMOKE_NAMESPACE" get all > "$HELM_SMOKE_REPORT_DIR/get-all.txt" 2>&1 || true
  kubectl -n "$HELM_SMOKE_NAMESPACE" get ingress > "$HELM_SMOKE_REPORT_DIR/ingresses.txt" 2>&1 || true
  kubectl -n "$HELM_SMOKE_NAMESPACE" describe "deployment/$HELM_SMOKE_RELEASE" > "$HELM_SMOKE_REPORT_DIR/describe-deployment.txt" 2>&1 || true
  kubectl -n "$HELM_SMOKE_NAMESPACE" describe "deployment/${HELM_SMOKE_RELEASE}-ui" > "$HELM_SMOKE_REPORT_DIR/describe-ui-deployment.txt" 2>&1 || true
  kubectl -n "$HELM_SMOKE_NAMESPACE" describe pods > "$HELM_SMOKE_REPORT_DIR/describe-pods.txt" 2>&1 || true
  kubectl -n "$HELM_SMOKE_NAMESPACE" logs "deployment/$HELM_SMOKE_RELEASE" --all-containers=true > "$HELM_SMOKE_REPORT_DIR/server.log" 2>&1 || true
  kubectl -n "$HELM_SMOKE_NAMESPACE" logs "deployment/${HELM_SMOKE_RELEASE}-ui" --all-containers=true > "$HELM_SMOKE_REPORT_DIR/ui.log" 2>&1 || true
  kubectl -n ingress-nginx logs deployment/ingress-nginx-controller --all-containers=true > "$HELM_SMOKE_REPORT_DIR/ingress-nginx-controller.log" 2>&1 || true

  if command -v kind >/dev/null 2>&1; then
    kind export logs "$HELM_SMOKE_REPORT_DIR/kind-logs" --name "$HELM_SMOKE_KIND_NAME" >/dev/null 2>&1 || true
  fi
}

main() {
  local command
  local smoke_cleanup=false

  if [ $# -lt 1 ]; then
    usage
    exit 1
  fi

  command="$1"
  shift

  while [ $# -gt 0 ]; do
    case "$1" in
      --cleanup)
        smoke_cleanup=true
        ;;
      --help|-h|help)
        usage
        exit 0
        ;;
      *)
        echo "Error: unknown option '$1'" >&2
        usage
        exit 1
        ;;
    esac
    shift
  done

  case "$command" in
    validate)
      if [ "$smoke_cleanup" = true ]; then
        echo "Error: --cleanup is only supported with the smoke command" >&2
        exit 1
      fi
      validate
      ;;
    smoke)
      smoke "$smoke_cleanup"
      ;;
    cleanup-smoke)
      if [ "$smoke_cleanup" = true ]; then
        echo "Error: --cleanup is only supported with the smoke command" >&2
        exit 1
      fi
      cleanup_smoke
      ;;
    collect-smoke-diagnostics)
      if [ "$smoke_cleanup" = true ]; then
        echo "Error: --cleanup is only supported with the smoke command" >&2
        exit 1
      fi
      collect_smoke_diagnostics
      ;;
    --help|-h|help)
      usage
      ;;
    *)
      echo "Error: unknown command '$command'" >&2
      usage
      exit 1
      ;;
  esac
}

main "$@"
