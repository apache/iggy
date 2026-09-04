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
# A plugin has two names and they are not interchangeable: cargo selects it by
# package name, and writes the artifact as lib<cdylib target name>.so. They
# match for every connector today only because every plugin package is named
# with underscores; a hyphenated package name, or an explicit [lib] name, makes
# them differ. Modes are split accordingly.
#
# Output modes (one plugin per line unless noted):
#   --names            cdylib target names, i.e. the <name> in lib<name>.so
#   --comma-names      cdylib target names, one CSV line
#   --packages         package names, i.e. what cargo -p takes
#   --comma-packages   package names, one CSV line
#   --package-flags    cargo -p flags, one line   (-p iggy_connector_... ...)
#   --manifests        repo-relative Cargo.toml paths
#   --manifest-flags   --manifest flags, one line (for third-party-licenses.sh)

MODE="--names"
if [[ $# -gt 0 ]]; then
    MODE="$1"
fi

METADATA="$(cargo metadata --format-version 1 --no-deps)"
WORKSPACE_ROOT="$(jq -r '.workspace_root' <<<"$METADATA")"

# Every cdylib target whose manifest lives under core/connectors/, as
# "<target name><TAB><package name><TAB><manifest path>". The path guard keeps a
# future non-connector cdylib elsewhere in the workspace out of the set.
PLUGINS="$(jq -r --arg root "$WORKSPACE_ROOT" '
    .packages[]
    | . as $pkg
    | select(.manifest_path | startswith($root + "/core/connectors/"))
    | .targets[]
    | select(.kind[] == "cdylib")
    | [.name, $pkg.name, ($pkg.manifest_path | ltrimstr($root + "/"))]
    | @tsv
' <<<"$METADATA" | sort -u)"

if [[ -z "$PLUGINS" ]]; then
    echo "connector-plugins: no cdylib plugin targets found under core/connectors/" >&2
    exit 1
fi

NAMES="$(cut -f1 <<<"$PLUGINS")"
PACKAGES="$(cut -f2 <<<"$PLUGINS")"
MANIFESTS="$(cut -f3 <<<"$PLUGINS")"

case "$MODE" in
    --names)
        echo "$NAMES"
        ;;
    --comma-names)
        paste -sd, - <<<"$NAMES"
        ;;
    --packages)
        echo "$PACKAGES"
        ;;
    --comma-packages)
        paste -sd, - <<<"$PACKAGES"
        ;;
    --package-flags)
        mapfile -t packages_arr <<<"$PACKAGES"
        printf '%s\n' "${packages_arr[@]/#/-p }" | paste -sd' ' -
        ;;
    --manifests)
        echo "$MANIFESTS"
        ;;
    --manifest-flags)
        mapfile -t manifests_arr <<<"$MANIFESTS"
        printf '%s\n' "${manifests_arr[@]/#/--manifest }" | paste -sd' ' -
        ;;
    *)
        echo "connector-plugins: unknown mode '$MODE'" >&2
        echo "usage: $0 [--names|--comma-names|--packages|--comma-packages|--package-flags|--manifests|--manifest-flags]" >&2
        exit 2
        ;;
esac
