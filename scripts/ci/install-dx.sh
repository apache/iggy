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

# Installs the dx CLI that bundles the web UI. dx must match the dioxus crate,
# so the pinned version is checked against Cargo.lock before downloading.
# Dioxus publishes glibc Linux builds only, so this does not run on Alpine.
#
# Usage: scripts/ci/install-dx.sh [install-dir]
# The default install dir is $CARGO_HOME/bin, or ~/.cargo/bin.

DX_VERSION="0.7.10"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
INSTALL_DIR="${1:-${CARGO_HOME:-$HOME/.cargo}/bin}"

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)
    DX_TARGET="x86_64-unknown-linux-gnu"
    DX_SHA256="4363e4ed2a3f1eb7f4d38d2d59aed59ce43271c44c16b425e92c89a64761fbe7"
    ;;
  Linux-aarch64 | Linux-arm64)
    DX_TARGET="aarch64-unknown-linux-gnu"
    DX_SHA256="8f1a17d3218700ffbe15e6540d936a178b2556fc801121a31082e3ba4ab9ef55"
    ;;
  *)
    echo "No pinned dx build for $(uname -s) $(uname -m)." >&2
    echo "Install it with: cargo install dioxus-cli --version ${DX_VERSION} --locked" >&2
    exit 1
    ;;
esac

locked_dioxus="$(grep -A1 '^name = "dioxus"$' "${REPO_ROOT}/Cargo.lock" | sed -n 's/^version = "\(.*\)"$/\1/p')"
if [[ "${locked_dioxus}" != "${DX_VERSION}" ]]; then
  echo "dx ${DX_VERSION} does not match dioxus ${locked_dioxus} in Cargo.lock." >&2
  echo "Update DX_VERSION and both SHA-256 sums in ${BASH_SOURCE[0]}." >&2
  exit 1
fi

download_dir="$(mktemp -d)"
trap 'rm -rf "${download_dir}"' EXIT
archive="${download_dir}/dx.tar.gz"

curl -fsSL --retry 5 --retry-all-errors -o "${archive}" \
  "https://github.com/DioxusLabs/dioxus/releases/download/v${DX_VERSION}/dx-${DX_TARGET}.tar.gz"
echo "${DX_SHA256}  ${archive}" | sha256sum -c -

mkdir -p "${INSTALL_DIR}"
tar -xzf "${archive}" -C "${INSTALL_DIR}" dx
"${INSTALL_DIR}/dx" --version
