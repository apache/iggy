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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$REPO_ROOT"

if ! command -v cargo-deny >/dev/null 2>&1; then
  echo "error: cargo-deny is not installed" >&2
  echo "Install it with: cargo install cargo-deny --locked" >&2
  exit 1
fi

mode=${1:-policy}
if [[ $# -gt 0 ]]; then
  shift
fi

case "$mode" in
  policy)
    echo "Running blocking cargo-deny policy checks..."
    cargo deny --all-features --locked "$@" check licenses bans sources
    ;;
  advisories)
    echo "Running cargo-deny advisory checks..."
    cargo deny --all-features --locked "$@" check advisories
    ;;
  *)
    echo "Usage: $0 [policy|advisories] [cargo-deny options]" >&2
    exit 2
    ;;
esac
