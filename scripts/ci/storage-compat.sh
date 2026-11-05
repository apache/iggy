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
# Runs the two halves of core/integration/tests/data_integrity/storage_compat.rs,
# each from its own source tree, on one data directory:
#   seed    the baseline's own seed test, run from a worktree of --baseline-ref
#           against an iggy-server built from that worktree and copied aside.
#           It writes the data directory and a JSON record of what the
#           baseline served. Default baseline: on a pull_request run, the
#           master tip under the checked-out merge ref, anywhere else,
#           origin/master.
#   verify  HEAD's verify test, run from this tree against an iggy-server
#           restored from the CI runtime archive or built locally from HEAD.
#           It boots on a copy of that directory and checks the record.
# The seed talks to the baseline through the baseline's own SDK, so a client
# protocol change in HEAD cannot fail it. A baseline that predates this split
# has no seed test, and the run stops before any build with a pointer to the
# breaking:protocol label.
#
# The baseline's own core/server/config.toml is extracted next to its binary.
# The seed boots the baseline on it and the verify translates it into HEAD's
# schema, so the swap changes the binary and nothing else.
#
# The harness only locates binaries and never builds them. CI supplies HEAD
# through a revision-checked runtime archive. Local runs build HEAD here.
# The baseline is always resolved independently from its own source tree.
#
# Exit codes: 0 = compatible, non-zero = build failure, format regression or
# a baseline without the seed test.

# Empty selects the default described above once HEAD is known.
BASELINE_REF=""
REBUILD_BASELINE=0
RUNTIME_ARTIFACTS=""

# The two #[ignore]d halves this script exists to drive. The integration crate
# is one binary built from tests/mod.rs, so tests are selected by module path.
# Neither name contains the other, so each filter selects exactly one test.
SEED_TEST="given_baseline_server_when_seeding_data_directory_should_record_what_it_serves"
VERIFY_TEST="given_baseline_data_directory_when_booted_by_build_under_test_should_read_back_the_record"
TEST_MODULE="data_integrity::storage_compat"

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
    --runtime-artifacts)
      RUNTIME_ARTIFACTS="${2:?--runtime-artifacts requires an archive directory}"
      RUNTIME_ARTIFACTS="$(realpath "$RUNTIME_ARTIFACTS")"
      shift 2
      ;;
    --rebuild-baseline)
      REBUILD_BASELINE=1
      shift
      ;;
    --help|-h)
      echo "Usage: $0 [--baseline-ref <ref>] [--rebuild-baseline] [--runtime-artifacts <directory>]"
      echo ""
      echo "Options:"
      echo "  --baseline-ref <ref>              Ref to build the baseline server from (default: the merge ref's"
      echo "                                    first parent on a pull_request run, origin/master otherwise)"
      echo "  --runtime-artifacts <directory>   Use a revision-checked CI runtime archive for HEAD"
      echo "  --rebuild-baseline                Rebuild the baseline even when its binary is already on disk"
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
if [[ -n "$RUNTIME_ARTIFACTS" && "$TARGET_DIR" != "$REPO_ROOT/target" ]]; then
  echo "Runtime artifacts require CARGO_TARGET_DIR=$REPO_ROOT/target"
  exit 1
fi
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

# The baseline's own config file, handed to BOTH boots. Neither binary may fall
# back to the server's relative default path: figment resolves that by walking
# up from the test process's directory, which hands the BASELINE this branch's
# file, and a key added under a `deny_unknown_fields` table (every [cluster] and
# [node] one) then fails its extraction. Written on every run, cached binary or
# not, so the pair can never drift apart.
BASELINE_CONFIG="${TARGET_DIR}/storage-compat/${BASELINE_SHA}/config.toml"
mkdir -p "$(dirname "${BASELINE_CONFIG}")"
if ! git show "${BASELINE_SHA}:core/server/config.toml" >"${BASELINE_CONFIG}"; then
  echo "Baseline ${BASELINE_SHA} carries no core/server/config.toml"
  exit 1
fi

# The data directory the seed writes and the verify boots a copy of, with the
# seed's record beside it. Wiped before every seed and kept after the run, so a
# failed verify can be inspected and rerun by hand against the same pair.
SEED_DATA="${TARGET_DIR}/storage-compat/${BASELINE_SHA}/data"
SEED_RECORD="${SEED_DATA}.json"

# --run-ignored only: both halves are #[ignore]d, so the normal lanes skip them.
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
NEXTEST_ARGS=(
  --locked -p integration
  --run-ignored only
  --no-tests=fail
  --retries 0
  --ignore-default-filter
)

if [ "${REBUILD_BASELINE}" -eq 1 ]; then
  rm -f "${BASELINE_SERVER}"
fi

# Outside the repo on purpose: an in-tree worktree gets swept up by the
# repo-wide find(1) in the lint scripts and by cargo's workspace globs.
# A fresh mktemp path per run cannot collide with an aborted run. The only
# residue such a run leaves is an admin entry under .git/worktrees, which
# prune clears. Created on every run, cached binary or not, because the seed
# test runs from the baseline's own source.
git worktree prune
WORKTREE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/iggy-storage-compat.XXXXXX")"
git worktree add --detach "${WORKTREE_DIR}" "${BASELINE_SHA}"

# Checked before any build. A baseline from before the split carries a single
# test that seeds through HEAD's SDK, which a baseline server refuses at login
# once HEAD moves the client protocol, so it cannot stand in for the seed.
if ! grep -rqs "fn ${SEED_TEST}(" "${WORKTREE_DIR}/core/integration/tests"; then
  echo "Baseline ${BASELINE_SHA} has no ${SEED_TEST}."
  echo "It predates the seed and verify split of the storage compatibility test, so the data"
  echo "directory cannot be seeded from the baseline's own source. Its single test seeds"
  echo "through HEAD's SDK and fails whenever HEAD changes the client protocol."
  echo "Add the breaking:protocol label to the pull request to skip this check once."
  exit 1
fi

# Inside the worktree so the cleanup trap reclaims it with the worktree. The
# copied binary and the seeded directory are all that outlive the run.
BASELINE_TARGET_DIR="${WORKTREE_DIR}/target"

rm -rf "${SEED_DATA}" "${SEED_RECORD}"

# Every baseline cargo call runs from the worktree as CWD so the baseline's own
# rust-toolchain.toml applies, and into a target directory of the worktree's own.
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
# Debug profile on both sides, and no --all-features: release would compile
# debug_assert! out of the baseline while HEAD still panics on it, and
# --all-features turns on the server's `disable-mimalloc`, so the two halves
# would differ in ways the storage format never changed.
(
  cd "${WORKTREE_DIR}"
  # The reference revision must not contribute counters to HEAD's coverage report.
  if [[ "${__CARGO_LLVM_COV_RUSTC_WRAPPER:-}" == 1 ]]; then
    export RUSTC_WRAPPER="${__CARGO_LLVM_COV_RUSTC_WRAPPER_PRE_EXISTING:-}"
  fi
  export CARGO_TARGET_DIR="${BASELINE_TARGET_DIR}"

  if [ -x "${BASELINE_SERVER}" ]; then
    echo "Reusing baseline server at ${BASELINE_SERVER}"
  else
    echo "Building baseline iggy-server from ${BASELINE_SHA}..."
    cargo build --locked -p server --bin iggy-server

    BASELINE_BUILT="${BASELINE_TARGET_DIR}/debug/iggy-server"
    if [ ! -x "${BASELINE_BUILT}" ]; then
      echo "Baseline build did not produce ${BASELINE_BUILT}"
      exit 1
    fi

    mkdir -p "$(dirname "${BASELINE_SERVER}")"
    cp "${BASELINE_BUILT}" "${BASELINE_SERVER}"
  fi

  echo "Running the seed test of ${BASELINE_SHA}..."
  # Absolute path: ServerHandle only treats this value as a literal path when it
  # has more than one component, otherwise it falls back to a cargo_bin lookup.
  COMPAT_BASELINE_SERVER="${BASELINE_SERVER}" \
    COMPAT_BASELINE_CONFIG="${BASELINE_CONFIG}" \
    COMPAT_BASELINE_DATA="${SEED_DATA}" \
    cargo nextest run "${NEXTEST_ARGS[@]}" -E "test(${TEST_MODULE}::${SEED_TEST})"
)

# Now, not at cleanup: a second full debug dependency graph is several GB, and
# the HEAD build plus the verify test still have to fit on the runner.
rm -rf "${BASELINE_TARGET_DIR}"

if [[ -n "$RUNTIME_ARTIFACTS" ]]; then
  ./scripts/ci/restore-runtime.sh "$RUNTIME_ARTIFACTS"
else
  rm -f "${HEAD_SERVER}"
  echo "Building HEAD iggy-server from ${HEAD_SHA}..."
  cargo build --locked -p server --bin iggy-server
fi

if [ ! -x "${HEAD_SERVER}" ]; then
  echo "HEAD build did not produce ${HEAD_SERVER}"
  exit 1
fi

echo "Running the verify test of ${HEAD_SHA}..."
COMPAT_BASELINE_CONFIG="${BASELINE_CONFIG}" \
  COMPAT_BASELINE_DATA="${SEED_DATA}" \
  cargo nextest run "${NEXTEST_ARGS[@]}" -E "test(${TEST_MODULE}::${VERIFY_TEST})"

echo "Storage format compatible: ${BASELINE_SHA} -> ${HEAD_SHA}"
