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

# Single source of truth for the connector plugin set: every workspace crate
# under core/connectors/ that builds a cdylib (a .so the iggy-connectors
# runtime loads via dlopen). Derived from `cargo metadata` so a newly added
# connector is picked up automatically by every consumer:
#
#   - .github/workflows/_build_rust_artifacts.yml  (edge tarball plugin list)
#   - core/connectors/runtime/Dockerfile           (fat image build + bundle)
#   - .github/actions/utils/validate-third-party-licenses  (license gate)
#
# Output modes (one plugin per line unless noted):
#   --names           crate names           (iggy_connector_postgres_sink)
#   --comma-names     crate names, one CSV line
#   --package-flags   cargo -p flags, one line   (-p iggy_connector_... ...)
#   --manifests       repo-relative Cargo.toml paths
#   --manifest-flags  --manifest flags, one line (for third-party-licenses.sh)

MODE="--names"
if [[ $# -gt 0 ]]; then
    MODE="$1"
fi

METADATA="$(cargo metadata --format-version 1 --no-deps)"
WORKSPACE_ROOT="$(jq -r '.workspace_root' <<<"$METADATA")"

# cdylib packages whose manifest lives under core/connectors/. The path guard
# keeps a future non-connector cdylib elsewhere in the workspace out of the set.
NAMES="$(jq -r --arg root "$WORKSPACE_ROOT" '
    .packages[]
    | select(.manifest_path | startswith($root + "/core/connectors/"))
    | select(.targets[] | .kind[] == "cdylib")
    | .name
' <<<"$METADATA" | sort -u)"

if [[ -z "$NAMES" ]]; then
    echo "connector-plugins: no cdylib plugin crates found under core/connectors/" >&2
    exit 1
fi

# Map each crate name back to its repo-relative manifest path.
manifest_for() {
    jq -r --arg root "$WORKSPACE_ROOT" --arg name "$1" '
        .packages[]
        | select(.name == $name)
        | .manifest_path
        | ltrimstr($root + "/")
    ' <<<"$METADATA"
}

case "$MODE" in
    --names)
        echo "$NAMES"
        ;;
    --comma-names)
        paste -sd, - <<<"$NAMES"
        ;;
    --package-flags)
        mapfile -t names_arr <<<"$NAMES"
        printf '%s\n' "${names_arr[@]/#/-p }" | paste -sd' ' -
        ;;
    --manifests)
        while IFS= read -r name; do manifest_for "$name"; done <<<"$NAMES"
        ;;
    --manifest-flags)
        while IFS= read -r name; do echo "--manifest $(manifest_for "$name")"; done <<<"$NAMES" | paste -sd' ' -
        ;;
    *)
        echo "connector-plugins: unknown mode '$MODE'" >&2
        echo "usage: $0 [--names|--comma-names|--package-flags|--manifests|--manifest-flags]" >&2
        exit 2
        ;;
esac
