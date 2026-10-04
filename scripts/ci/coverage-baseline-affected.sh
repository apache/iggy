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

# Decide which coverage-baseline jobs a master push needs. Every Codecov flag
# has carryforward enabled, so a language whose sources did not change keeps
# serving its previous master report; only changed surfaces are re-measured.
#
# Fail-open: on any uncertainty (initial push, force push, unknown base) emit
# true for every gate so a needed refresh is never skipped. workflow_dispatch
# passes an empty base and lands on that path by design (manual full backfill).
#
# Usage:  coverage-baseline-affected.sh <base-sha> <head-sha>
# Output: one "<gate>=true|false" line per gate on stdout, shaped for
#         appending to $GITHUB_OUTPUT. Diagnostics go to stderr.

BASE="${1:-}"
HEAD="${2:-}"
ZERO="0000000000000000000000000000000000000000"
GATES=(rust java csharp python php node go)

emit_all() {
  local gate
  for gate in "${GATES[@]}"; do
    echo "${gate}=true"
  done
}

if [[ -z "$HEAD" ]]; then
  echo "usage: $0 <base-sha> <head-sha>" >&2
  exit 2
fi

# Unusable base: initial push, force push, workflow_dispatch (empty base),
# or a commit not in history.
if [[ -z "$BASE" || "$BASE" == "$ZERO" ]] || ! git cat-file -e "${BASE}^{commit}" 2>/dev/null; then
  echo "coverage-gate: unusable base '${BASE:-<empty>}', running all jobs" >&2
  emit_all
  exit 0
fi

# CI plumbing changes how every job builds and measures; re-baseline all.
if ! git diff --quiet "$BASE" "$HEAD" -- .github scripts codecov.yml; then
  echo "coverage-gate: CI/tooling change, running all jobs" >&2
  emit_all
  exit 0
fi

# A git failure here (exit > 1, e.g. corrupt object) reads as "changed",
# falling open for that gate.
changed() {
  ! git diff --quiet "$BASE" "$HEAD" -- "$@"
}

# GATES is the single source for both this loop and emit_all, so a gate can
# never be present on one path and missing on the other. A GATES entry
# without a pathspec arm fails loud instead of reusing the previous
# iteration's paths.
for gate in "${GATES[@]}"; do
  case "$gate" in
    rust) paths=(core gateways Cargo.toml Cargo.lock rust-toolchain.toml .cargo) ;;
    java) paths=(foreign/java) ;;
    csharp) paths=(foreign/csharp) ;;
    python) paths=(foreign/python) ;;
    php) paths=(foreign/php) ;;
    node) paths=(foreign/node) ;;
    # The go job also runs the bdd/go suite with foreign/go in -coverpkg.
    go) paths=(foreign/go bdd/go) ;;
    *)
      echo "coverage-gate: no pathspecs defined for gate '$gate'" >&2
      exit 1
      ;;
  esac
  if changed "${paths[@]}"; then
    echo "${gate}=true"
    echo "coverage-gate: ${gate} affected" >&2
  else
    echo "${gate}=false"
  fi
done
