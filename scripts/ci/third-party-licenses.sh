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
# Multiple --manifest flags are supported. Each Rust manifest is scanned by
# its own cargo-about run against the real workspace, so the lockfile pins
# every version and bin-only crates are covered. --generate merges the runs
# into one deduplicated manifest. Multiple Node manifests are concatenated.
# Mixed ecosystems concatenate the Rust block first, Node block second.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# cargo-about shells out to `cargo metadata`, which refreshes the crates.io
# index and downloads dep manifests. Both can fail transiently on a network
# blip ("Connection reset by peer") even past cargo's own net retries. Retry
# the whole invocation a few times so a flake never reddens a release gate.
CARGO_ABOUT_ATTEMPTS=3
CARGO_ABOUT_RETRY_DELAY_SECONDS=5

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

JSON_DIR=""
TMP_OUT=""
NODE_FMT_FILE=""
NODE_JSON_FILE=""

cleanup() {
  [[ -n "$JSON_DIR" && -d "$JSON_DIR" ]] && rm -rf "$JSON_DIR"
  [[ -n "$TMP_OUT" && -f "$TMP_OUT" ]] && rm -f "$TMP_OUT"
  [[ -n "$NODE_FMT_FILE" && -f "$NODE_FMT_FILE" ]] && rm -f "$NODE_FMT_FILE"
  [[ -n "$NODE_JSON_FILE" && -f "$NODE_JSON_FILE" ]] && rm -f "$NODE_JSON_FILE"
  return 0
}
trap cleanup EXIT

# Run cargo-about against one manifest, writing the result to the supplied
# path. Always invoked from REPO_ROOT so that `--config about.toml` and the
# about.hbs template resolve correctly. `--fail` makes a disallowed license
# abort with a non-zero exit code. With format "json" there is no template:
# the scan data is written as-is so several runs can be merged.
run_cargo_about() {
  local target_manifest="$1"
  local out="$2"
  local format="${3:-handlebars}"
  local template=(about.hbs)
  [[ "$format" == "json" ]] && template=()
  require_cmd cargo-about

  local attempt
  for ((attempt = 1; attempt <= CARGO_ABOUT_ATTEMPTS; attempt++)); do
    if ( cd "$REPO_ROOT" && \
      cargo about generate \
        --config about.toml \
        --manifest-path "$target_manifest" \
        --fail \
        --format "$format" \
        -o "$out" \
        ${template[@]+"${template[@]}"} ); then
      return 0
    fi
    if ((attempt < CARGO_ABOUT_ATTEMPTS)); then
      echo "warning: cargo-about failed (attempt ${attempt}/${CARGO_ABOUT_ATTEMPTS}), retrying in ${CARGO_ABOUT_RETRY_DELAY_SECONDS}s" >&2
      sleep "$CARGO_ABOUT_RETRY_DELAY_SECONDS"
    fi
  done
  return 1
}

# Merge the per-section data of several cargo-about JSON scans into one
# manifest: one section per license id, the crates deduplicated by name and
# version, and the license text taken from the first scan. Section layout
# mirrors about.hbs, whose header is reused verbatim by render_rust_union.
UNION_JQ="$(cat <<'EOF'
def rule: "=" * 80;

def repository_suffix: if (.repository // "") != "" then " (\(.repository))" else "" end;

def section:
  "\n" + rule + "\nLICENSE: \(.name) (\(.id)) - used by \(.crates | length) crate(s)\nUSED BY:\n"
  + ([.crates[] | "  - \(.name) \(.version)\(repository_suffix)\n"] | join(""))
  + rule + "\n\n" + .text + "\n\n";

[ .[] as $scan
  | $scan.overview[]
  | { id, name, text, crates: [ .indices[] as $index | $scan.licenses[$index].used_by[].crate ] } ]
| group_by(.id)
| map({ id: .[0].id, name: .[0].name, text: .[0].text,
        crates: ([.[].crates[]] | unique_by([.name, .version])) })
| sort_by(-(.crates | length), .id)
| map(section)
| join("")
EOF
)"

# Scan every Rust manifest on its own, against the real workspace and its
# Cargo.lock, then append the merged manifest to the given file. A synthetic
# root crate cannot do this: cargo drops bin-only members such as the
# connectors runtime from a path-dependency union without failing.
render_rust_union() {
  local out="$1"
  require_cmd jq

  JSON_DIR="$(mktemp -d -t iggy-license-scans-XXXXXX)"

  local scans=() index=0 m
  for m in "${RUST_MANIFESTS[@]}"; do
    echo "Scanning Rust manifest: $m"
    run_cargo_about "$m" "$JSON_DIR/$index.json" json || return 1
    scans+=("$JSON_DIR/$index.json")
    index=$((index + 1))
  done

  sed '/^{{#each overview}}/,$d' "$REPO_ROOT/about.hbs" >> "$out"
  jq -rjs "$UNION_JQ" "${scans[@]}" >> "$out"
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

    for m in "${RUST_MANIFESTS[@]}"; do
      echo "Validating Rust manifest: $m"
      if ! run_cargo_about "$m" "$TMP_OUT"; then
        failures+=("rust: $m")
      fi
    done

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

    if (( ${#RUST_MANIFESTS[@]} == 1 )); then
      echo "Generating Rust manifest section: ${RUST_MANIFESTS[0]}"
      run_cargo_about "${RUST_MANIFESTS[0]}" "$OUTPUT"
    elif (( ${#RUST_MANIFESTS[@]} > 1 )); then
      echo "Generating Rust manifest section: ${RUST_MANIFESTS[*]}"
      render_rust_union "$OUTPUT"
    fi

    for d in "${NODE_DIRS[@]}"; do
      echo "Generating Node manifest section: $d"
      run_node_about "$d" render "$OUTPUT"
    done

    echo "Done. Wrote $OUTPUT"
    ;;
esac
