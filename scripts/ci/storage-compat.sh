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

# shellcheck source-path=SCRIPTDIR
source "$(dirname "${BASH_SOURCE[0]}")/lib/init.sh"

# storage-compat.sh -- Prove HEAD still reads a data directory written by master.
#
# Builds two iggy-server binaries and hands both to one integration test:
#   baseline  built from --baseline-ref, copied aside. Default: on a
#             pull_request run, the master tip under the checked-out merge ref;
#             anywhere else, origin/master.
#   HEAD      built from the working tree, left at target/debug/iggy-server
# The test boots the baseline, seeds a data directory, swaps the binary to HEAD
# and restarts against that same directory.
#
# Both binaries MUST be built here. `core/integration` has no dependency on the
# `server` package; the harness only LOCATES a binary, through
# `assert_cmd::Command::cargo_bin`, which falls back to whatever file happens to
# sit at target/debug/iggy-server (assert_cmd 2.2.2 `legacy_cargo_bin`). So
# `cargo nextest run -p integration` compiles no server at all, and a lane that
# does not build both halves compares a stale binary against itself and reports
# green forever.
#
# Exit codes: 0 = compatible, non-zero = build failure or format regression.

# Empty selects the default described above once HEAD is known.
BASELINE_REF=""
REBUILD_BASELINE=0

# The single #[ignore]d test this script exists to drive. The integration crate
# is one binary built from tests/mod.rs, so tests are selected by module path.
TEST_FILTER="data_integrity::storage_compat"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --baseline-ref)
      BASELINE_REF="${2:-}"
      if [ -z "${BASELINE_REF}" ]; then
        echo "--baseline-ref requires a git ref"
        exit 1
      fi
      shift 2
      ;;
    --rebuild-baseline)
      REBUILD_BASELINE=1
      shift
      ;;
    --help|-h)
      echo "Usage: $0 [--baseline-ref <ref>] [--rebuild-baseline]"
      echo ""
      echo "Options:"
      echo "  --baseline-ref <ref>  Ref to build the baseline server from (default: the merge ref's"
      echo "                        first parent on a pull_request run, origin/master otherwise)"
      echo "  --rebuild-baseline    Rebuild the baseline even when its binary is already on disk"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Absolute before any cd: cargo resolves a relative CARGO_TARGET_DIR against
# its own CWD, and the baseline below builds from a worktree elsewhere on
# disk, so a relative value would scatter the two builds and the lookup of
# either binary across three directories. Exported so every cargo call here,
# nextest included, lands in the same place -- except the baseline build, which
# must have a target directory of its own (see the worktree build below).
TARGET_DIR="${CARGO_TARGET_DIR:-${REPO_ROOT}/target}"
mkdir -p "${TARGET_DIR}"
TARGET_DIR="$(cd "${TARGET_DIR}" && pwd)"
export CARGO_TARGET_DIR="${TARGET_DIR}"

cd "${REPO_ROOT}"

if ! command -v cargo-nextest &>/dev/null; then
  echo "cargo-nextest is not installed"
  echo ""
  echo "Install with:"
  echo "  cargo install cargo-nextest --locked"
  exit 1
fi

HEAD_SERVER="${TARGET_DIR}/debug/iggy-server"

WORKTREE_DIR=""

# Every command is guarded: under `set -e` a failure inside an EXIT trap would
# replace the script's real exit status with the trap's.
cleanup() {
  if [ -n "${WORKTREE_DIR}" ]; then
    git worktree remove --force "${WORKTREE_DIR}" 2>/dev/null || rm -rf "${WORKTREE_DIR}" || true
  fi
  git worktree prune 2>/dev/null || true
}
trap cleanup EXIT

HEAD_SHA="$(git rev-parse --verify HEAD)"

# On a pull_request run actions/checkout leaves HEAD at the synthetic
# refs/pull/N/merge commit, whose first parent is the exact master tip GitHub
# merged the PR onto: an ancestor of HEAD by construction. Live origin/master
# can already be newer by the time this step runs, and a baseline HEAD does
# not contain would blame the PR for master's own changes. Read from the raw
# object: in the depth-1 checkout the parent is a shallow boundary, so
# `HEAD^1` does not resolve. Two parents required, so a checkout pinned to the
# PR head instead of the merge ref falls through rather than picking the PR's
# own parent.
if [ -z "${BASELINE_REF}" ]; then
  if [[ "${GITHUB_REF:-}" =~ ^refs/pull/[0-9]+/merge$ ]] \
    && [ "$(git cat-file -p HEAD | grep -c '^parent ')" -eq 2 ]; then
    BASELINE_REF="$(git cat-file -p HEAD | awk '/^parent /{print $2; exit}')"
    echo "Pull request run: baseline is the master tip under the merge ref"
  else
    BASELINE_REF="origin/master"
  fi
fi

# The remote side of a fetch takes a branch or tag name (or a reachable SHA),
# never a remote-tracking name, so drop the remote prefix before asking origin.
#
# --depth=1 only where the clone is already shallow (the CI checkout). On a
# full clone that flag does not save anything, it WRITES a shallow boundary at
# the fetched tip and grafts every older commit off the developer's history,
# for every worktree sharing the repository.
FETCH_DEPTH=()
if [ "$(git rev-parse --is-shallow-repository)" = "true" ]; then
  FETCH_DEPTH=(--depth=1)
fi
FETCHED=0
if git fetch --no-tags "${FETCH_DEPTH[@]}" origin "${BASELINE_REF#origin/}" 2>/dev/null; then
  FETCHED=1
fi

BASELINE_SHA=""
if [ "${FETCHED}" -eq 1 ]; then
  # FETCH_HEAD in preference to the named ref: actions/checkout narrows
  # remote.origin.fetch on a shallow clone, so refs/remotes/origin/master can
  # stay absent or stale straight through a successful fetch.
  BASELINE_SHA="$(git rev-parse --verify --quiet "FETCH_HEAD^{commit}" || true)"
fi
if [ -z "${BASELINE_SHA}" ]; then
  BASELINE_SHA="$(git rev-parse --verify --quiet "${BASELINE_REF}^{commit}" || true)"
fi
if [ -z "${BASELINE_SHA}" ]; then
  echo "Could not resolve baseline ref '${BASELINE_REF}' locally or from origin"
  exit 1
fi

echo "Baseline: ${BASELINE_SHA} (${BASELINE_REF})"
echo "HEAD:     ${HEAD_SHA}"
if [ "${BASELINE_SHA}" = "${HEAD_SHA}" ]; then
  echo "WARNING: baseline and HEAD are the same commit, this run proves nothing"
fi
# Only decidable with history: a shallow clone answers "no" for every pair.
if [ "$(git rev-parse --is-shallow-repository)" = "false" ] \
  && ! git merge-base --is-ancestor "${BASELINE_SHA}" "${HEAD_SHA}"; then
  echo "WARNING: baseline is not an ancestor of HEAD; a failure below may be master's change, not HEAD's"
fi

# Keyed by baseline commit, which also pins that commit's Cargo.lock and
# rust-toolchain.toml. Only a developer machine ever hits this: hosted runners
# start empty and nothing publishes a baseline binary, so CI rebuilds it on
# every run.
BASELINE_SERVER="${TARGET_DIR}/storage-compat/${BASELINE_SHA}/iggy-server"

if [ "${REBUILD_BASELINE}" -eq 1 ]; then
  rm -f "${BASELINE_SERVER}"
fi

if [ -x "${BASELINE_SERVER}" ]; then
  echo "Reusing baseline server at ${BASELINE_SERVER}"
else
  # Outside the repo on purpose: an in-tree worktree gets swept up by the
  # repo-wide find(1) in the lint scripts and by cargo's workspace globs.
  # A fresh mktemp path per run cannot collide with an aborted run; the only
  # residue such a run leaves is an admin entry under .git/worktrees, which
  # prune clears.
  git worktree prune
  WORKTREE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/iggy-storage-compat.XXXXXX")"
  git worktree add --detach "${WORKTREE_DIR}" "${BASELINE_SHA}"

  echo "Building baseline iggy-server from ${BASELINE_SHA}..."
  # Built from the worktree as CWD so the baseline's own rust-toolchain.toml
  # applies, and into a target directory of the worktree's own.
  #
  # The two trees MUST NOT share one. Cargo keys a workspace member's unit hash
  # on its manifest path RELATIVE to the workspace root, and records that
  # member's sources in the dep-info relative too, so `core/configs` in the
  # worktree and `core/configs` here hash identically and both resolve against
  # whichever root cargo is invoked from. Sharing a target directory therefore
  # makes the second build read the first build's rlibs as fresh: HEAD's
  # `server` would compile against MASTER's `configs`, `consensus`,
  # `partitions` and `shard`. Any PR touching a crate below `server` fails to
  # build here with errors that do not reproduce anywhere else. The duplicated
  # dependency compile is the price of the two halves being what they claim.
  #
  # Inside the worktree so the cleanup trap reclaims it with the worktree; only
  # the copied binary below outlives the run.
  #
  # Debug profile on both sides, and no --all-features: release would compile
  # debug_assert! out of the baseline while HEAD still panics on it, and
  # --all-features turns on the server's `disable-mimalloc`, so the two halves
  # would differ in ways the storage format never changed.
  BASELINE_TARGET_DIR="${WORKTREE_DIR}/target"
  (
    cd "${WORKTREE_DIR}"
    CARGO_TARGET_DIR="${BASELINE_TARGET_DIR}" cargo build --locked -p server --bin iggy-server
  )

  BASELINE_BUILT="${BASELINE_TARGET_DIR}/debug/iggy-server"
  if [ ! -x "${BASELINE_BUILT}" ]; then
    echo "Baseline build did not produce ${BASELINE_BUILT}"
    exit 1
  fi

  mkdir -p "$(dirname "${BASELINE_SERVER}")"
  cp "${BASELINE_BUILT}" "${BASELINE_SERVER}"
  # Now, not at cleanup: a second full debug dependency graph is several GB, and
  # the HEAD build plus the integration test still have to fit on the runner.
  rm -rf "${BASELINE_TARGET_DIR}"
fi

# The baseline never writes here any more, but a binary left by an earlier run
# of this script (or by any other build in this tree) would satisfy the
# existence check below without cargo having produced it now. Delete it so that
# check means what it says.
rm -f "${HEAD_SERVER}"

echo "Building HEAD iggy-server from ${HEAD_SHA}..."
cargo build --locked -p server --bin iggy-server

if [ ! -x "${HEAD_SERVER}" ]; then
  echo "HEAD build did not produce ${HEAD_SERVER}"
  exit 1
fi

# Absolute path: ServerHandle only treats this value as a literal path when it
# has more than one component, otherwise it falls back to a cargo_bin lookup.
export COMPAT_BASELINE_SERVER="${BASELINE_SERVER}"

echo "Running storage compatibility test..."
# --run-ignored only: the test is #[ignore]d, so the normal lanes skip it.
# --no-tests=fail: a filter matching nothing has to be an error here, unlike the
# main lane's --no-tests=warn, which would report a typo'd filter as green.
# --retries 0: a read-back that only fails sometimes is exactly the signal this
# check exists for, so no retry may hide it. Explicit rather than "not
# --profile ci": a profile default or NEXTEST_RETRIES in the environment would
# otherwise apply.
# --ignore-default-filter: nextest intersects -E with the profile's
# default-filter, so one that excluded this module would turn the run into a
# --no-tests=fail abort instead of running the test.
# The version floor for these flags lives in .config/nextest.toml.
cargo nextest run --locked -p integration \
  --run-ignored only \
  --no-tests=fail \
  --retries 0 \
  --ignore-default-filter \
  -E "test(${TEST_FILTER})"

echo "Storage format compatible: ${BASELINE_SHA} -> ${HEAD_SHA}"
