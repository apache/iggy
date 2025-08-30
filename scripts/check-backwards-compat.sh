#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.

set -euo pipefail

# -----------------------------
# Config (overridable via args)
# -----------------------------
MASTER_REF="${MASTER_REF:-master}"           # branch or commit for "baseline"
PR_REF="${PR_REF:-HEAD}"                     # commit to test (assumes current checkout)
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8090}"
WAIT_SECS="${WAIT_SECS:-60}"
BATCHES="${BATCHES:-50}"
MSGS_PER_BATCH="${MSGS_PER_BATCH:-100}"
KEEP_TMP="${KEEP_TMP:-false}"

# -----------------------------
# Helpers
# -----------------------------
info(){ printf "\n\033[1;36m➤ %s\033[0m\n" "$*"; }
ok(){   printf "\033[0;32m✓ %s\033[0m\n" "$*"; }
err(){  printf "\033[0;31m✗ %s\033[0m\n" "$*" >&2; }
die(){  err "$*"; exit 1; }

need() {
  command -v "$1" >/dev/null 2>&1 || die "missing dependency: $1"
}

wait_for_port() {
  local host="$1" port="$2" deadline=$((SECONDS + WAIT_SECS))
  while (( SECONDS < deadline )); do
    if command -v nc >/dev/null 2>&1; then
      if nc -z "$host" "$port" 2>/dev/null; then return 0; fi
    else
      if (echo >"/dev/tcp/$host/$port") >/dev/null 2>&1; then return 0; fi
    fi
    sleep 1
  done
  return 1
}

stop_pid() {
  local pid="$1" name="${2:-process}"
  if kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid" || true
    for _ in $(seq 1 15); do
      kill -0 "$pid" 2>/dev/null || { ok "stopped $name (pid $pid)"; return 0; }
      sleep 1
    done
    err "$name (pid $pid) still running; sending SIGKILL"
    kill -KILL "$pid" || true
  fi
}

print_logs_if_any() {
  local dir="$1"
  if compgen -G "$dir/local_data/logs/iggy*" > /dev/null; then
    echo "---- $dir/local_data/logs ----"
    cat "$dir"/local_data/logs/iggy* || true
    echo "------------------------------"
  else
    echo "(no iggy logs found in $dir/local_data/logs)"
  fi
}

# -----------------------------
# Args
# -----------------------------
usage() {
  cat <<EOF
Usage: $0 [--master-ref REF] [--pr-ref REF] [--host 127.0.0.1] [--port 8090]
          [--wait-secs 60] [--batches 50] [--msgs-per-batch 100] [--keep-tmp]

Runs a backwards-compatibility check:
  1) Build and run master (or REF), write data via benches
  2) Stop server, capture local_data/
  3) Build PR (or REF), restore local_data/
  4) Run consumer bench; fail if anything breaks

Env overrides supported: MASTER_REF, PR_REF, HOST, PORT, WAIT_SECS, BATCHES, MSGS_PER_BATCH, KEEP_TMP
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --master-ref) MASTER_REF="$2"; shift 2;;
    --pr-ref) PR_REF="$2"; shift 2;;
    --host) HOST="$2"; shift 2;;
    --port) PORT="$2"; shift 2;;
    --wait-secs) WAIT_SECS="$2"; shift 2;;
    --batches) BATCHES="$2"; shift 2;;
    --msgs-per-batch) MSGS_PER_BATCH="$2"; shift 2;;
    --keep-tmp) KEEP_TMP=true; shift;;
    -h|--help) usage; exit 0;;
    *) die "unknown arg: $1";;
  esac
done

# -----------------------------
# Pre-flight
# -----------------------------
need git
need cargo
need pkill
need pgrep
need awk
need sed
command -v timeout >/dev/null 2>&1 || true  # optional, we'll use it if present

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$REPO_ROOT"

# Free the port proactively (best-effort)
pkill -f iggy-server >/dev/null 2>&1 || true

TMP_ROOT="$(mktemp -d -t iggy-backcompat-XXXXXX)"
MASTER_DIR="$TMP_ROOT/master"
PR_DIR="$REPO_ROOT"  # assume script is run from PR checkout
MASTER_LOG="$TMP_ROOT/server-master.stdout.log"
PR_LOG="$TMP_ROOT/server-pr.stdout.log"

cleanup() {
  # Stop any leftover iggy-server
  pkill -f iggy-server >/dev/null 2>&1 || true
  git worktree remove --force "$MASTER_DIR" >/dev/null 2>&1 || true
  if [[ "$KEEP_TMP" != "true" ]]; then
    rm -rf "$TMP_ROOT" || true
  else
    info "keeping temp dir: $TMP_ROOT"
  fi
}
trap cleanup EXIT

# -----------------------------
# 1) Prepare master worktree
# -----------------------------
info "Preparing baseline worktree at '$MASTER_REF'"
git fetch --all --tags --prune >/dev/null 2>&1 || true
git worktree add --force "$MASTER_DIR" "$MASTER_REF"
ok "worktree at $MASTER_DIR"

# -----------------------------
# 2) Build & run master server
# -----------------------------
pushd "$MASTER_DIR" >/dev/null

info "Building iggy-server & benches (baseline: $MASTER_REF)"
IGGY_CI_BUILD=true cargo build --bins
ok "built baseline"

info "Starting iggy-server (baseline)"
set +e
( nohup target/debug/iggy-server >"$MASTER_LOG" 2>&1 & echo $! > "$TMP_ROOT/master.pid" )
set -e
MASTER_PID="$(cat "$TMP_ROOT/master.pid")"
ok "iggy-server started (pid $MASTER_PID), logs: $MASTER_LOG"

info "Waiting for $HOST:$PORT to be ready (up to ${WAIT_SECS}s)"
if ! wait_for_port "$HOST" "$PORT"; then
  err "server did not become ready in ${WAIT_SECS}s"
  print_logs_if_any "$MASTER_DIR"
  [[ -f "$MASTER_LOG" ]] && tail -n 200 "$MASTER_LOG" || true
  exit 1
fi
ok "server is ready"

# Producer bench (baseline)
info "Running producer bench on baseline"
BENCH_CMD=( target/debug/iggy-bench --verbose --message-batches "$BATCHES" --messages-per-batch "$MSGS_PER_BATCH" pinned-producer tcp )
if command -v timeout >/dev/null 2>&1; then timeout 60s "${BENCH_CMD[@]}"; else "${BENCH_CMD[@]}"; fi
ok "producer bench done"

# Consumer bench (baseline)
info "Running consumer bench on baseline"
BENCH_CMD=( target/debug/iggy-bench --verbose --message-batches "$BATCHES" --messages-per-batch "$MSGS_PER_BATCH" pinned-consumer tcp )
if command -v timeout >/dev/null 2>&1; then timeout 60s "${BENCH_CMD[@]}"; else "${BENCH_CMD[@]}"; fi
ok "consumer bench done (baseline)"

# Stop baseline server
info "Stopping baseline server"
stop_pid "$MASTER_PID" "iggy-server(baseline)"
print_logs_if_any "$MASTER_DIR"

# Clean baseline logs (like CI step)
if compgen -G "local_data/logs/iggy*" > /dev/null; then
  rm -f local_data/logs/iggy* || true
fi

# Snapshot local_data/
info "Snapshotting baseline local_data/"
cp -a local_data "$TMP_ROOT/local_data"
ok "snapshot stored at $TMP_ROOT/local_data"

popd >/dev/null

# -----------------------------
# 3) Build PR & restore data
# -----------------------------
pushd "$PR_DIR" >/dev/null
info "Ensuring PR ref is present: $PR_REF"
git rev-parse --verify "$PR_REF^{commit}" >/dev/null 2>&1 || die "PR_REF '$PR_REF' not found"
git checkout -q "$PR_REF"

info "Building iggy-server & benches (PR: $PR_REF)"
IGGY_CI_BUILD=true cargo build --bins
ok "built PR"

info "Restoring baseline local_data/ into PR workspace"
rm -rf local_data
cp -a "$TMP_ROOT/local_data" ./local_data
ok "restored local_data/"

# -----------------------------
# 4) Run PR server & consumer bench
# -----------------------------
info "Starting iggy-server (PR)"
set +e
( nohup target/debug/iggy-server >"$PR_LOG" 2>&1 & echo $! > "$TMP_ROOT/pr.pid" )
set -e
PR_PID="$(cat "$TMP_ROOT/pr.pid")"
ok "iggy-server (PR) started (pid $PR_PID), logs: $PR_LOG"

info "Waiting for $HOST:$PORT to be ready (up to ${WAIT_SECS}s)"
if ! wait_for_port "$HOST" "$PORT"; then
  err "PR server did not become ready in ${WAIT_SECS}s"
  print_logs_if_any "$PR_DIR"
  [[ -f "$PR_LOG" ]] && tail -n 200 "$PR_LOG" || true
  exit 1
fi
ok "PR server is ready"

# Only consumer bench against PR
info "Running consumer bench on PR (compat check)"
BENCH_CMD=( target/debug/iggy-bench --verbose --message-batches "$BATCHES" --messages-per-batch "$MSGS_PER_BATCH" pinned-consumer tcp )
if command -v timeout >/dev/null 2>&1; then timeout 60s "${BENCH_CMD[@]}"; else "${BENCH_CMD[@]}"; fi
ok "consumer bench done (PR)"

# Stop PR server
info "Stopping PR server"
stop_pid "$PR_PID" "iggy-server(PR)"
print_logs_if_any "$PR_DIR"

ok "backwards-compatibility check PASSED"
popd >/dev/null
