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

# Decide which DockerHub :edge images a master push actually changed, so
# post-merge only refreshes the affected ones instead of all of them.
#
# Each Rust image builds via cargo-chef over the whole workspace (COPY . .),
# so the shipped binary changes only when its crate dependency closure does.
# That closure is exactly what `cargo rail plan` reports, the same DAG that
# scopes test runs in .github/actions/rust/pre-merge. web-ui is not a crate,
# and a Dockerfile-only edit touches no crate source, so each image also
# declares fallback `gate.paths`.
#
# Fail-open: on any uncertainty (force push, workspace-global change,
# cargo-rail failure, unconfigured image) emit the full image list so a needed
# edge refresh is never skipped.
#
# Usage:  edge-affected-images.sh <base-sha> <head-sha>
# Output: comma-separated publish.yml component keys (e.g. "rust-server,web-ui")

BASE="${1:-}"
HEAD="${2:-}"
CONFIG=".github/config/publish.yml"
ZERO="0000000000000000000000000000000000000000"

if [[ -z "$BASE" || -z "$HEAD" ]]; then
  echo "usage: $0 <base-sha> <head-sha>" >&2
  exit 2
fi

CFG_JSON="$(yq -o=json -I=0 '.components' "$CONFIG")"
mapfile -t ALL_IMAGES < <(jq -r 'to_entries[] | select(.value.registry == "dockerhub") | .key' <<<"$CFG_JSON")

emit() { (IFS=,; echo "$*"); }
emit_all() { emit "${ALL_IMAGES[@]}"; }

# Unusable base: initial push, force-push, or a commit not in history.
if [[ "$BASE" == "$ZERO" ]] || ! git cat-file -e "${BASE}^{commit}" 2>/dev/null; then
  echo "::notice::edge-gate: unusable base '$BASE', refreshing all images" >&2
  emit_all
  exit 0
fi

# Workspace-global edits rebuild every crate. cargo-rail does not flag
# toolchain/lockfile changes (they are not crate sources), so escalate here.
if ! git diff --quiet "$BASE" "$HEAD" -- Cargo.toml Cargo.lock rust-toolchain.toml .cargo; then
  echo "::notice::edge-gate: workspace-global change, refreshing all images" >&2
  emit_all
  exit 0
fi

RAIL_ERR="$(mktemp)"
trap 'rm -f "$RAIL_ERR"' EXIT
PLAN="$(cargo rail plan --since "$BASE" -f json 2>"$RAIL_ERR" || true)"
if [[ -z "$PLAN" ]]; then
  echo "::warning::edge-gate: cargo-rail failed, refreshing all images. $(cat "$RAIL_ERR" 2>/dev/null || true)" >&2
  emit_all
  exit 0
fi

MODE="$(jq -r '.scope.mode // "full"' <<<"$PLAN")"
if [[ "$MODE" != "crates" ]]; then
  echo "::notice::edge-gate: cargo-rail reports full workspace, refreshing all images" >&2
  emit_all
  exit 0
fi

mapfile -t AFFECTED < <(jq -r '.scope.crates // [] | .[]' <<<"$PLAN")

crate_affected() {
  local want="$1" crate
  for crate in ${AFFECTED[@]+"${AFFECTED[@]}"}; do
    [[ "$crate" == "$want" ]] && return 0
  done
  return 1
}

SELECTED=()
for img in "${ALL_IMAGES[@]}"; do
  # An image with no gate block fails open (always refreshed).
  if [[ "$(jq -r --arg k "$img" '.[$k] | has("gate")' <<<"$CFG_JSON")" != "true" ]]; then
    SELECTED+=("$img")
    continue
  fi

  keep=false

  while IFS= read -r crate; do
    [[ -z "$crate" ]] && continue
    if crate_affected "$crate"; then
      keep=true
      break
    fi
  done < <(jq -r --arg k "$img" '.[$k].gate.crates // [] | .[]' <<<"$CFG_JSON")

  if [[ "$keep" == false ]]; then
    mapfile -t gate_paths < <(jq -r --arg k "$img" '.[$k].gate.paths // [] | .[]' <<<"$CFG_JSON")
    if [[ ${#gate_paths[@]} -gt 0 ]] && ! git diff --quiet "$BASE" "$HEAD" -- "${gate_paths[@]}"; then
      keep=true
    fi
  fi

  [[ "$keep" == true ]] && SELECTED+=("$img")
done

emit ${SELECTED[@]+"${SELECTED[@]}"}
