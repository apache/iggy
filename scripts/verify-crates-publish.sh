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

# End-to-end verification that the four published Rust crates can actually
# be uploaded to a crates.io-like registry. Spins up cargo-http-registry as
# a local alt-registry, then publishes iggy_binary_protocol, iggy_common,
# iggy and iggy-cli in topological order. Catches missing `version = "..."`
# on path deps, wrong publish order, and any manifest-level regression that
# only surfaces at `cargo publish` time (the exact class of bug that broke
# Apache Iggy 0.8.0 rc1).
#
# This script is invoked both by developers (locally) and by CI. It must
# leave the working tree untouched on exit, even on failure.
#
# Usage:
#   scripts/verify-crates-publish.sh
#
# Dependencies (install manually, or re-run with AUTO_INSTALL=1):
#   cargo-http-registry  (cargo install cargo-http-registry --locked)
#
# Environment variables (all optional):
#   REGISTRY_PORT         Port for the local registry          (default: 35503)
#   CA_BUNDLE             Path to the system CA bundle used by libgit2.
#                         Auto-detected on common Linux distros if unset.
#   AUTO_INSTALL          If "1", install cargo-http-registry  (default: 0)
#
# Exit codes:
#   0  success, all four crates published to the local registry
#   1  tooling missing or preflight failure
#   2  publish chain failed on one of the four crates

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

REGISTRY_PORT="${REGISTRY_PORT:-35503}"
REGISTRY_DIR="$(mktemp -d -t iggy-local-reg.XXXXXX)"
REGISTRY_LOG="$(mktemp -t iggy-local-reg-log.XXXXXX)"
# Byte-exact snapshot of Cargo.toml. Restored by cleanup() instead of
# `git checkout`, so the revert is independent of git state and cannot
# eat an in-flight edit if the preflight dirty check is ever bypassed.
CARGO_TOML_BACKUP="$(mktemp -t iggy-cargo-toml.XXXXXX)"
cp Cargo.toml "$CARGO_TOML_BACKUP"
AUTO_INSTALL="${AUTO_INSTALL:-0}"

# Ordered topologically: each crate depends only on the ones before it.
CRATES=(
    iggy_binary_protocol
    iggy_common
    iggy
    iggy-cli
)

REGISTRY_PID=""
CARGO_CONFIG_CREATED=false
CARGO_TOML_PATCHED=false

log() { printf '>>> %s\n' "$*"; }
err() { printf '!!! %s\n' "$*" >&2; }

# Invoked via `trap ... EXIT INT TERM`; shellcheck can't see indirect calls.
# shellcheck disable=SC2329
cleanup() {
    local ec=$?
    set +e
    trap - EXIT INT TERM

    if [[ "$CARGO_TOML_PATCHED" == "true" ]]; then
        log "Reverting Cargo.toml patch"
        cp "$CARGO_TOML_BACKUP" Cargo.toml
    fi
    rm -f "$CARGO_TOML_BACKUP"

    if [[ "$CARGO_CONFIG_CREATED" == "true" ]]; then
        log "Removing scratch .cargo/config.toml"
        rm -f .cargo/config.toml
        rmdir .cargo 2>/dev/null || true
    fi

    if [[ -n "$REGISTRY_PID" ]] && kill -0 "$REGISTRY_PID" 2>/dev/null; then
        log "Stopping local registry (pid $REGISTRY_PID)"
        kill -TERM "$REGISTRY_PID" 2>/dev/null || true
        # Give it half a second then force
        for _ in 1 2 3 4 5; do
            kill -0 "$REGISTRY_PID" 2>/dev/null || break
            sleep 0.1
        done
        kill -KILL "$REGISTRY_PID" 2>/dev/null || true
        wait "$REGISTRY_PID" 2>/dev/null || true
    fi

    rm -rf "$REGISTRY_DIR" "$REGISTRY_LOG"
    exit "$ec"
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# 0. Preconditions
# ---------------------------------------------------------------------------
if ! command -v cargo >/dev/null 2>&1; then
    err "cargo is not on PATH"
    exit 1
fi

if ! command -v cargo-http-registry >/dev/null 2>&1; then
    if [[ "$AUTO_INSTALL" == "1" ]]; then
        log "Installing cargo-http-registry"
        cargo install cargo-http-registry --locked
    else
        err "cargo-http-registry is not installed"
        err "Install it with: cargo install cargo-http-registry --locked"
        err "Or re-run this script with AUTO_INSTALL=1"
        exit 1
    fi
fi

# libgit2 inside cargo needs an explicit CA bundle on many Linux distros.
# Auto-detect common locations when the caller hasn't overridden it.
if [[ -z "${CA_BUNDLE:-}" ]]; then
    for candidate in \
        /etc/ssl/certs/ca-certificates.crt \
        /etc/pki/tls/certs/ca-bundle.crt \
        /etc/ssl/cert.pem \
        /etc/ssl/ca-bundle.pem \
        /opt/homebrew/etc/ca-certificates/cert.pem \
        /usr/local/etc/ca-certificates/cert.pem
    do
        if [[ -r "$candidate" ]]; then
            CA_BUNDLE="$candidate"
            break
        fi
    done
fi

if [[ -z "${CA_BUNDLE:-}" || ! -r "$CA_BUNDLE" ]]; then
    err "Could not locate a system CA bundle. Set CA_BUNDLE=/path/to/ca.crt"
    exit 1
fi
log "Using CA bundle: $CA_BUNDLE"

if [[ -e .cargo/config.toml ]]; then
    err ".cargo/config.toml already exists; refusing to overwrite"
    err "Move it aside before running this script"
    exit 1
fi

# Refuse to run on top of in-flight Cargo.toml edits. The script patches
# this file in step 3, and we don't want to mix caller edits with the
# scratch registry patch: a successful run would happily roll both
# forward, and a failed run would force the caller to disentangle them.
if ! git diff --quiet HEAD -- Cargo.toml; then
    err "Cargo.toml has uncommitted changes; refusing to patch"
    err "Commit or stash your changes before running this script"
    exit 1
fi

# ---------------------------------------------------------------------------
# 1. Start the local registry
# ---------------------------------------------------------------------------
log "Starting cargo-http-registry at 127.0.0.1:${REGISTRY_PORT}"
cargo-http-registry --addr "127.0.0.1:${REGISTRY_PORT}" "$REGISTRY_DIR" \
    > "$REGISTRY_LOG" 2>&1 &
REGISTRY_PID=$!

# cargo-http-registry speaks git-http; `git ls-remote` is the closest
# thing to a health check. A GET on any URL path returns 405 because the
# git-upload-pack endpoint expects POST, so don't probe with plain curl.
for attempt in $(seq 1 20); do
    if git ls-remote "http://127.0.0.1:${REGISTRY_PORT}/git" HEAD >/dev/null 2>&1; then
        log "Registry is up (pid $REGISTRY_PID)"
        break
    fi
    if ! kill -0 "$REGISTRY_PID" 2>/dev/null; then
        err "cargo-http-registry died before becoming ready"
        cat "$REGISTRY_LOG" >&2 || true
        exit 1
    fi
    sleep 0.5
    if (( attempt == 20 )); then
        err "Registry failed to become ready within 10s"
        cat "$REGISTRY_LOG" >&2 || true
        exit 1
    fi
done

# ---------------------------------------------------------------------------
# 2. Write a local .cargo/config.toml pointing at the registry
# ---------------------------------------------------------------------------
log "Writing .cargo/config.toml"
mkdir -p .cargo
CARGO_CONFIG_CREATED=true
cat > .cargo/config.toml <<EOF
[registries.local-dev]
index = "http://127.0.0.1:${REGISTRY_PORT}/git"

[net]
git-fetch-with-cli = true

[http]
cainfo = "${CA_BUNDLE}"
EOF

# ---------------------------------------------------------------------------
# 3. Patch root Cargo.toml so path deps on the four iggy crates resolve
#    against local-dev during `cargo publish`. The edit is reverted by the
#    cleanup trap, whether the script succeeds or fails.
# ---------------------------------------------------------------------------
log "Patching workspace.dependencies to target local-dev"
CARGO_TOML_PATCHED=true
# Injects `, registry = "local-dev"` before the closing brace of the line.
# Match by crate name + path + version so we do not touch unrelated entries.
sed -i -E \
    -e 's|^(iggy_binary_protocol = \{ path = "[^"]+", version = "[^"]+")( \})$|\1, registry = "local-dev"\2|' \
    -e 's|^(iggy_common = \{ path = "[^"]+", version = "[^"]+")( \})$|\1, registry = "local-dev"\2|' \
    -e 's|^(iggy = \{ path = "[^"]+", version = "[^"]+")( \})$|\1, registry = "local-dev"\2|' \
    -e 's|^(iggy-cli = \{ path = "[^"]+", version = "[^"]+")( \})$|\1, registry = "local-dev"\2|' \
    Cargo.toml

# Sanity check: all four lines must now carry the registry marker. This
# catches formatting drift in Cargo.toml before it becomes a confusing
# publish-time error.
missing=0
for crate in "${CRATES[@]}"; do
    if ! grep -Eq "^${crate} = \{.*registry = \"local-dev\".*\}$" Cargo.toml; then
        err "Failed to patch workspace dep for ${crate}"
        missing=1
    fi
done
if (( missing )); then
    err "One or more workspace deps did not match the expected format"
    err "Expected: <crate> = { path = \"...\", version = \"...\" }"
    err "Current state:"
    grep -nE "^(iggy|iggy-cli|iggy_binary_protocol|iggy_common) = " Cargo.toml >&2 || true
    exit 1
fi

# ---------------------------------------------------------------------------
# 4. Publish the four crates to local-dev in topological order
# ---------------------------------------------------------------------------
# --no-verify: the PR build pipeline already compiles the workspace; this
#              job only checks the publish path. Skipping verify means we
#              don't need the transitive crates.io dep graph to be locally
#              resolvable.
# --allow-dirty: we just patched Cargo.toml; the working tree is dirty by
#                design. The cleanup trap reverts the patch on exit.
for crate in "${CRATES[@]}"; do
    log "Publishing ${crate} to local-dev"
    if ! cargo publish \
        -p "$crate" \
        --registry local-dev \
        --token ci \
        --allow-dirty \
        --no-verify
    then
        err "Publish failed for ${crate}"
        exit 2
    fi
done

# ---------------------------------------------------------------------------
# 5. Cross-check: every crate's tarball must actually exist on the registry
# ---------------------------------------------------------------------------
log "Verifying all four crates appear in the local registry"
# crates-index path layout for 4+ char names: "<first-2>/<next-2>/<name>".
# Every published iggy crate is >=4 chars, so the shorter-name buckets
# (1/, 2/, 3/<c>/) from the full layout are intentionally not handled.
for crate in "${CRATES[@]}"; do
    lc="${crate,,}"
    rel="${lc:0:2}/${lc:2:2}/$lc"
    if [[ ! -f "$REGISTRY_DIR/$rel" ]]; then
        err "Crate ${crate} missing from local registry (expected at $rel)"
        ls -la "$REGISTRY_DIR" >&2 || true
        exit 2
    fi
done

log "All four crates published successfully to local-dev"
log "Registry directory: $REGISTRY_DIR (will be cleaned up)"
exit 0
