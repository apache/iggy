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

# Builds the Dioxus web UI into core/web_ui/dist, where iggy-server embeds it.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CRATE_DIR="${REPO_ROOT}/core/web_ui"

# dx keeps every previously hashed asset in its staging directory and copies
# all of them into the bundle, so stale builds would end up in the server binary.
rm -rf "${REPO_ROOT}/target/dx/iggy-web-ui" "${CRATE_DIR}/dist"

cd "${CRATE_DIR}"
dx bundle --platform web --release --out-dir dist
