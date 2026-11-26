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
# The Linux dx builds link glibc, so this does not run on Alpine.
#
# On Linux it also installs wasm-bindgen, wasm-opt and esbuild. dx bundle
# downloads these itself without a checksum, so CI pins them here and sets
# NO_DOWNLOADS=1, which makes dx take them from PATH. Local builds may let dx
# download them, so macOS gets only dx.
#
# Usage: scripts/ci/install-dx.sh [install-dir]
# The default install dir is $CARGO_HOME/bin, or ~/.cargo/bin.

DX_VERSION="0.7.10"
# wasm-bindgen must match Cargo.lock. dx 0.7.10 hardcodes the binaryen and
# esbuild versions (packages/cli/src/wasm_opt.rs and esbuild.rs), so re-read
# them there whenever DX_VERSION changes.
WASM_BINDGEN_VERSION="0.2.130"
BINARYEN_VERSION="127"
ESBUILD_VERSION="0.27.3"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
INSTALL_DIR="${1:-${CARGO_HOME:-$HOME/.cargo}/bin}"

# wasm-bindgen publishes no x86_64 glibc build, so both arches use the static
# musl one. The binaryen and esbuild Linux builds are static too.
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)
    DX_TARGET="x86_64-unknown-linux-gnu"
    DX_SHA256="4363e4ed2a3f1eb7f4d38d2d59aed59ce43271c44c16b425e92c89a64761fbe7"
    WASM_BINDGEN_TARGET="x86_64-unknown-linux-musl"
    WASM_BINDGEN_SHA256="9687f3bb551b9f66f37ed32a40d7913a00f7962db653054e56857bc306c3227f"
    BINARYEN_TARGET="x86_64-linux"
    BINARYEN_SHA256="c8ebe5d00c978601086bcad33b2c80fcfe33d6a8b87b754ba4ea86a9a16cc316"
    ESBUILD_TARGET="linux-x64"
    ESBUILD_SHA256="066e20cdb882994160e18524a552b97e03648eb9aa0c7cdf5680a6493be65ab2"
    ;;
  Linux-aarch64)
    DX_TARGET="aarch64-unknown-linux-gnu"
    DX_SHA256="8f1a17d3218700ffbe15e6540d936a178b2556fc801121a31082e3ba4ab9ef55"
    WASM_BINDGEN_TARGET="aarch64-unknown-linux-musl"
    WASM_BINDGEN_SHA256="99ffc93ce80081b91632e2b539b782821a8f3439ab0fddc645287462b2f55d3d"
    BINARYEN_TARGET="aarch64-linux"
    BINARYEN_SHA256="1589778bcedde5ba5ed6b7107f902c4ec6bc4c94147daa79157d778ca08300a2"
    ESBUILD_TARGET="linux-arm64"
    ESBUILD_SHA256="04f0bfb132b8b0800c23b22caa9ad7a7adf41e2434c027fc8571318b9904712f"
    ;;
  Darwin-x86_64)
    DX_TARGET="x86_64-apple-darwin"
    DX_SHA256="e3edbba285fa23a834aa7ceb558c9d8088cb3fbdcb48fb8c619532ed4d91cab7"
    ;;
  Darwin-arm64)
    DX_TARGET="aarch64-apple-darwin"
    DX_SHA256="342c92bc33dca60dadd725ed5e249923f049ae2e1e50b7477f4968d56695b425"
    ;;
  *)
    echo "No pinned dx build for $(uname -s) $(uname -m)." >&2
    echo "Install it with: cargo install dioxus-cli --version ${DX_VERSION} --locked" >&2
    exit 1
    ;;
esac

locked_version() {
  grep -A1 "^name = \"$1\"$" "${REPO_ROOT}/Cargo.lock" | sed -n 's/^version = "\(.*\)"$/\1/p' || true
}

locked_dioxus="$(locked_version dioxus)"
if [[ "${locked_dioxus}" != "${DX_VERSION}" ]]; then
  echo "dx ${DX_VERSION} does not match dioxus ${locked_dioxus:-(none)} in Cargo.lock." >&2
  echo "Update DX_VERSION and every SHA-256 sum in ${BASH_SOURCE[0]}." >&2
  exit 1
fi

# dx refuses a wasm-bindgen that differs from Cargo.lock, so fail here first.
if [[ -n "${WASM_BINDGEN_TARGET:-}" ]]; then
  locked_wasm_bindgen="$(locked_version wasm-bindgen)"
  if [[ "${locked_wasm_bindgen}" != "${WASM_BINDGEN_VERSION}" ]]; then
    echo "wasm-bindgen ${WASM_BINDGEN_VERSION} does not match ${locked_wasm_bindgen:-(none)} in Cargo.lock." >&2
    echo "Update WASM_BINDGEN_VERSION and its SHA-256 sums in ${BASH_SOURCE[0]}." >&2
    exit 1
  fi
fi

download_dir="$(mktemp -d)"
trap 'rm -rf "${download_dir}"' EXIT

# Older macOS releases ship shasum but not sha256sum.
if command -v sha256sum >/dev/null 2>&1; then
  sha256_check=(sha256sum -c -)
else
  sha256_check=(shasum -a 256 -c -)
fi

# Downloads an archive, checks its SHA-256 and extracts one binary from it
# into download_dir.
fetch() {
  local url="$1" sha256="$2" member="$3" strip="$4"
  local archive="${download_dir}/${url##*/}"
  curl -fsSL --retry 5 --retry-all-errors -o "${archive}" "${url}"
  echo "${sha256}  ${archive}" | "${sha256_check[@]}"
  tar -xzf "${archive}" -C "${download_dir}" --strip-components="${strip}" "${member}"
  rm "${archive}"
}

fetch "https://github.com/DioxusLabs/dioxus/releases/download/v${DX_VERSION}/dx-${DX_TARGET}.tar.gz" \
  "${DX_SHA256}" dx 0
binaries=(dx)

if [[ -n "${WASM_BINDGEN_TARGET:-}" ]]; then
  wasm_bindgen_dir="wasm-bindgen-${WASM_BINDGEN_VERSION}-${WASM_BINDGEN_TARGET}"
  fetch "https://github.com/wasm-bindgen/wasm-bindgen/releases/download/${WASM_BINDGEN_VERSION}/${wasm_bindgen_dir}.tar.gz" \
    "${WASM_BINDGEN_SHA256}" "${wasm_bindgen_dir}/wasm-bindgen" 1
  fetch "https://github.com/WebAssembly/binaryen/releases/download/version_${BINARYEN_VERSION}/binaryen-version_${BINARYEN_VERSION}-${BINARYEN_TARGET}.tar.gz" \
    "${BINARYEN_SHA256}" "binaryen-version_${BINARYEN_VERSION}/bin/wasm-opt" 2
  fetch "https://registry.npmjs.org/@esbuild/${ESBUILD_TARGET}/-/${ESBUILD_TARGET}-${ESBUILD_VERSION}.tgz" \
    "${ESBUILD_SHA256}" package/bin/esbuild 2
  binaries+=(wasm-bindgen wasm-opt esbuild)
fi

# Run every new binary before any of them replaces a working one.
for binary in "${binaries[@]}"; do
  "${download_dir}/${binary}" --version
done
mkdir -p "${INSTALL_DIR}"
for binary in "${binaries[@]}"; do
  install -m 0755 "${download_dir}/${binary}" "${INSTALL_DIR}/${binary}"
done
