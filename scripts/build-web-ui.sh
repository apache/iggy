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

# Builds the Dioxus web UI into core/web_ui/dist. iggy-server does not embed
# this bundle yet, the switch-over from the Svelte app will make it do so.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CRATE_DIR="${REPO_ROOT}/core/web_ui"

# dx stages under the cargo target directory, which CARGO_TARGET_DIR or a
# cargo config can move, so ask cargo the same way dx does. Plain bash parses
# the JSON so the script needs no jq.
metadata="$(cargo metadata --format-version 1 --no-deps --manifest-path "${CRATE_DIR}/Cargo.toml")"
if [[ ! "${metadata}" =~ \"target_directory\":\"([^\"]+)\" ]]; then
    echo "cargo metadata did not report a target_directory" >&2
    exit 1
fi
TARGET_DIR="${BASH_REMATCH[1]}"

# dx keeps every previously hashed asset in its staging directory and copies
# all of them into the bundle, so stale builds would end up in the bundle.
rm -rf "${TARGET_DIR}/dx/iggy-web-ui" "${CRATE_DIR}/dist"

cd "${CRATE_DIR}"
dx bundle --platform web --release --out-dir dist
