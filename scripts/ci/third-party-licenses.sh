#!/bin/bash

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

# Validate or generate a LICENSE-binary manifest for a convenience
# binary artifact (Docker image, PyPI wheel, etc.). ASF release policy
# requires that each artifact bundling third-party code carries a
# license enumeration of the packages it actually bundles. This script
# drives cargo-about (Rust) and license-checker-rseidelsohn (Node) to
# produce that enumeration.
#
# Usage: third-party-licenses.sh <action> --manifest <path> [--manifest <path> ...] [--output <file>]
#
# Actions (exactly one required):
#   --validate   Verify deps satisfy the accept list. No file written.
#                Exits non-zero on disallowed license.
#   --generate   Render the manifest to --output.
#
# --manifest accepts:
#   * Path to a Cargo.toml                          -> Rust ecosystem
#   * Path to a directory containing package.json   -> Node ecosystem
#
# Multiple --manifest flags are supported. Multiple Rust manifests are
# rendered as a deduplicated union via a synthetic root crate. Multiple
# Node manifests are concatenated. Mixed ecosystems concatenate the
# Rust block first, Node block second.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

ACTION=""
MANIFESTS=()
OUTPUT=""

usage() {
  sed -n '/^# Usage:/,/^set -euo/{/^set -euo/d;p}' "$0" | sed 's/^# \{0,1\}//' >&2
}

die() {
  echo "error: $*" >&2
  exit 1
}

while (($# > 0)); do
  case "$1" in
    --validate)
      [[ -n "$ACTION" ]] && die "actions --validate and --generate are mutually exclusive"
      ACTION="validate"
      shift
      ;;
    --generate)
      [[ -n "$ACTION" ]] && die "actions --validate and --generate are mutually exclusive"
      ACTION="generate"
      shift
      ;;
    --manifest)
      [[ $# -lt 2 ]] && die "--manifest requires a path argument"
      MANIFESTS+=("$2")
      shift 2
      ;;
    --output)
      [[ $# -lt 2 ]] && die "--output requires a file argument"
      OUTPUT="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

[[ -z "$ACTION" ]] && { usage; die "missing action (--validate or --generate)"; }
[[ ${#MANIFESTS[@]} -eq 0 ]] && die "at least one --manifest is required"
[[ "$ACTION" == "generate" && -z "$OUTPUT" ]] && die "--generate requires --output"

# Classify each manifest by ecosystem.
RUST_MANIFESTS=()
NODE_DIRS=()
for m in "${MANIFESTS[@]}"; do
  if [[ -f "$m" && "$(basename "$m")" == "Cargo.toml" ]]; then
    RUST_MANIFESTS+=("$m")
  elif [[ -d "$m" && -f "$m/package.json" ]]; then
    NODE_DIRS+=("$m")
  else
    die "manifest does not look like a Cargo.toml or a directory containing package.json: $m"
  fi
done

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not installed: $1"
}

SYNTH_DIR=""
TMP_OUT=""
NODE_FMT_FILE=""
NODE_JSON_FILE=""

cleanup() {
  [[ -n "$SYNTH_DIR" && -d "$SYNTH_DIR" ]] && rm -rf "$SYNTH_DIR"
  [[ -n "$TMP_OUT" && -f "$TMP_OUT" ]] && rm -f "$TMP_OUT"
  [[ -n "$NODE_FMT_FILE" && -f "$NODE_FMT_FILE" ]] && rm -f "$NODE_FMT_FILE"
  [[ -n "$NODE_JSON_FILE" && -f "$NODE_JSON_FILE" ]] && rm -f "$NODE_JSON_FILE"
  return 0
}
trap cleanup EXIT

# Resolve the Cargo.toml that cargo-about should target. For a single
# Rust manifest, return it as-is. For multiple Rust manifests, build a
# synthetic root crate under /tmp that path-deps onto each requested
# member; cargo-about then renders the deduplicated union in a single
# pass. The empty [workspace] table prevents the synthetic root from
# attaching to any ancestor workspace.
build_rust_target_manifest() {
  local count=${#RUST_MANIFESTS[@]}
  if (( count == 1 )); then
    echo "${RUST_MANIFESTS[0]}"
    return
  fi

  SYNTH_DIR="$(mktemp -d -t iggy-license-root-XXXXXX)"

  {
    printf '[workspace]\n\n'
    printf '[package]\n'
    printf 'name = "iggy-license-union"\n'
    printf 'version = "0.0.0"\n'
    printf 'edition = "2021"\n'
    printf 'license = "Apache-2.0"\n'
    printf 'publish = false\n\n'
    printf '[dependencies]\n'
    for m in "${RUST_MANIFESTS[@]}"; do
      local crate_dir crate_name
      crate_dir="$(cd "$(dirname "$m")" && pwd)"
      crate_name="$(awk -F'"' '/^\[package\]/{p=1; next} p && /^name *= *"/{print $2; exit}' "$m")"
      [[ -z "$crate_name" ]] && die "could not extract package.name from $m"
      printf '%s = { path = "%s" }\n' "$crate_name" "$crate_dir"
    done
  } > "$SYNTH_DIR/Cargo.toml"

  mkdir -p "$SYNTH_DIR/src"
  : > "$SYNTH_DIR/src/lib.rs"

  echo "$SYNTH_DIR/Cargo.toml"
}

# Run cargo-about against the prepared manifest, writing the result to
# the supplied path. Always invoked from REPO_ROOT so that
# `--config about.toml` and the about.hbs template resolve correctly.
# `--fail` makes a disallowed license abort with a non-zero exit code.
run_cargo_about() {
  local target_manifest="$1"
  local out="$2"
  require_cmd cargo-about

  ( cd "$REPO_ROOT" && \
    cargo about generate \
      --config about.toml \
      --manifest-path "$target_manifest" \
      --fail \
      -o "$out" \
      about.hbs )
}

# Run license-checker-rseidelsohn against an npm package directory and
# pipe the JSON through render-node-licenses.mjs in either render or
# validate-only mode.
run_node_about() {
  local pkg_dir="$1"
  local mode="$2"   # render | validate
  local out="$3"
  require_cmd node
  require_cmd npx

  NODE_FMT_FILE="$(mktemp)"
  NODE_JSON_FILE="$(mktemp)"

  echo '{"licenseFile": ""}' > "$NODE_FMT_FILE"

  # Exclude the root package itself from the manifest: it is the
  # artifact we are licensing, not a third-party dependency. Without
  # this, an UNLICENSED root (private app workspace) would fail
  # validation against the third-party allow-list.
  local pkg_dir_abs root_pkg
  pkg_dir_abs="$(cd "$pkg_dir" && pwd)"
  root_pkg="$(node -e "const p=require('$pkg_dir_abs/package.json'); process.stdout.write(p.name+'@'+p.version)")"

  npx -y license-checker-rseidelsohn@latest \
    --production \
    --json \
    --start "$pkg_dir" \
    --excludePackages "$root_pkg" \
    --customPath "$NODE_FMT_FILE" > "$NODE_JSON_FILE"

  if [[ "$mode" == "validate" ]]; then
    node "$SCRIPT_DIR/render-node-licenses.mjs" --validate-only "$NODE_JSON_FILE"
  else
    node "$SCRIPT_DIR/render-node-licenses.mjs" "$NODE_JSON_FILE" >> "$out"
  fi
}

case "$ACTION" in
  validate)
    TMP_OUT="$(mktemp)"
    failures=()

    if (( ${#RUST_MANIFESTS[@]} > 0 )); then
      target="$(build_rust_target_manifest)"
      echo "Validating Rust manifests: ${RUST_MANIFESTS[*]}"
      if ! run_cargo_about "$target" "$TMP_OUT"; then
        failures+=("rust: ${RUST_MANIFESTS[*]}")
      fi
    fi

    for d in "${NODE_DIRS[@]}"; do
      echo "Validating Node manifest: $d"
      if ! run_node_about "$d" validate "$TMP_OUT"; then
        failures+=("node: $d")
      fi
    done

    if (( ${#failures[@]} > 0 )); then
      echo "" >&2
      echo "Validation failed for:" >&2
      for f in "${failures[@]}"; do
        echo "  - $f" >&2
      done
      exit 1
    fi
    echo "All manifests pass third-party license validation."
    ;;

  generate)
    : > "$OUTPUT"

    if (( ${#RUST_MANIFESTS[@]} > 0 )); then
      target="$(build_rust_target_manifest)"
      echo "Generating Rust manifest section: ${RUST_MANIFESTS[*]}"
      run_cargo_about "$target" "$OUTPUT"
    fi

    for d in "${NODE_DIRS[@]}"; do
      echo "Generating Node manifest section: $d"
      run_node_about "$d" render "$OUTPUT"
    done

    echo "Done. Wrote $OUTPUT"
    ;;
esac
