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
Usage: scripts/ci/setup-helm-tools.sh [--install-kind]

Install the pinned Helm toolchain used by Helm chart CI jobs. When requested,
also install the pinned kind binary used by the Helm smoke test.

Environment:
  HELM_VERSION         Helm release version (default: v4.1.3)
  HELM_CHECKSUM        SHA256 checksum for the Helm tarball
  KIND_VERSION         kind release version (default: v0.31.0)
  KIND_CHECKSUM        SHA256 checksum for the kind binary
  HELM_TOOLS_BIN_DIR   Target binary directory (default: /usr/local/bin)
EOF
}

INSTALL_KIND="false"

while [ $# -gt 0 ]; do
  case "$1" in
    --install-kind)
      INSTALL_KIND="true"
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

HELM_VERSION="${HELM_VERSION:-v4.1.3}"
HELM_CHECKSUM="${HELM_CHECKSUM:-02ce9722d541238f81459938b84cf47df2fdf1187493b4bfb2346754d82a4700}"
KIND_VERSION="${KIND_VERSION:-v0.31.0}"
KIND_CHECKSUM="${KIND_CHECKSUM:-eb244cbafcc157dff60cf68693c14c9a75c4e6e6fedaf9cd71c58117cb93e3fa}"
HELM_TOOLS_BIN_DIR="${HELM_TOOLS_BIN_DIR:-/usr/local/bin}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: required command '$1' not found" >&2
    exit 1
  fi
}

install_binary() {
  local source="$1"
  local destination="$2"

  mkdir -p "$HELM_TOOLS_BIN_DIR"
  if [ -w "$HELM_TOOLS_BIN_DIR" ]; then
    install -m 0755 "$source" "$destination"
  else
    require_command sudo
    sudo install -m 0755 "$source" "$destination"
  fi
}

if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "x86_64" ]]; then
  cat >&2 <<'EOF'
Error: scripts/ci/setup-helm-tools.sh currently supports Linux x86_64 only.
Install helm and kind manually on other platforms, then run the local Helm test scripts.
EOF
  exit 1
fi

require_command wget
require_command sha256sum
require_command tar
require_command install

helm_archive="/tmp/helm.tar.gz"
helm_dir="/tmp/linux-amd64"
wget -qO "$helm_archive" "https://get.helm.sh/helm-${HELM_VERSION}-linux-amd64.tar.gz"
echo "${HELM_CHECKSUM}  ${helm_archive}" | sha256sum -c -
rm -rf "$helm_dir"
tar -zxf "$helm_archive" -C /tmp linux-amd64/helm
install_binary "$helm_dir/helm" "${HELM_TOOLS_BIN_DIR}/helm"

if [ "$INSTALL_KIND" = "true" ]; then
  kind_binary="/tmp/kind"
  wget -qO "$kind_binary" "https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-linux-amd64"
  echo "${KIND_CHECKSUM}  ${kind_binary}" | sha256sum -c -
  install_binary "$kind_binary" "${HELM_TOOLS_BIN_DIR}/kind"
fi
