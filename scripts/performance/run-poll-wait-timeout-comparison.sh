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

# shellcheck disable=SC1091

set -euo pipefail

IGGY_BENCH_CMD="target/release/iggy-bench"
IGGY_SERVER_CMD="target/release/iggy-server"
IDENTIFIER="$(hostname)"
OUTPUT_DIR="performance_results/poll_wait_timeout_comparison"
TRANSPORT="tcp"
SKIP_BUILD=false
DRY_RUN=false
QUICK=false

while [[ $# -gt 0 ]]; do
    case "$1" in
    --bench-cmd)
        IGGY_BENCH_CMD="$2"
        shift 2
        ;;
    --server-cmd)
        IGGY_SERVER_CMD="$2"
        shift 2
        ;;
    --identifier)
        IDENTIFIER="$2"
        shift 2
        ;;
    --output-dir)
        OUTPUT_DIR="$2"
        shift 2
        ;;
    --transport)
        TRANSPORT="$2"
        shift 2
        ;;
    --skip-build)
        SKIP_BUILD=true
        shift
        ;;
    --dry-run)
        DRY_RUN=true
        shift
        ;;
    --quick)
        QUICK=true
        shift
        ;;
    *)
        echo "Unknown argument: $1" >&2
        exit 1
        ;;
    esac
done

case "$TRANSPORT" in
tcp | websocket | quic)
    ;;
*)
    echo "Unsupported transport: ${TRANSPORT}. Use tcp, websocket, or quic." >&2
    exit 1
    ;;
esac

source "$(dirname "$0")/../utils.sh"
source "$(dirname "$0")/utils.sh"

function on_interrupt() {
    on_exit_bench
    exit 130
}

if [[ "$DRY_RUN" == false ]]; then
    trap on_interrupt SIGINT SIGTERM
    trap on_exit_bench EXIT
fi

if [[ "$SKIP_BUILD" == false && "$DRY_RUN" == false ]]; then
    echo "Building release binaries..."
    RUSTFLAGS="-C target-cpu=native" cargo build --release --bin iggy-server --bin iggy-bench
fi

if [[ "$DRY_RUN" == false ]]; then
    mkdir -p "$OUTPUT_DIR"
fi

commit_hash=$(get_git_iggy_server_tag_or_sha1 .)
commit_date=$(get_git_commit_date .)

if [[ "$QUICK" == true ]]; then
    SPARSE_BATCHES=30
    SATURATED_BATCHES=20
    GROUP_BATCHES=20
else
    SPARSE_BATCHES=1000
    SATURATED_BATCHES=250
    GROUP_BATCHES=1000
fi

wait_timeouts=("0s" "10ms" "100ms" "1s")

function bench_transport() {
    case "$TRANSPORT" in
    tcp)
        echo "tcp"
        ;;
    websocket)
        echo "websocket"
        ;;
    quic)
        echo "quic"
        ;;
    esac
}

function result_log_name() {
    local suite="$1"
    local wait_timeout="$2"
    echo "${OUTPUT_DIR}/${suite}_wait_${wait_timeout//[^a-zA-Z0-9]/_}.log"
}

function start_server() {
    echo "Cleaning old local_data..."
    rm -rf local_data

    echo "Starting iggy-server..."
    IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy "$IGGY_SERVER_CMD" &>"${OUTPUT_DIR}/iggy-server.log" &
    sleep 2

    local server_pid
    server_pid=$(pgrep -x "iggy-server")
    exit_if_process_is_not_running "$server_pid"
}

function stop_server() {
    echo "Stopping iggy-server..."
    send_signal "iggy-server" "TERM"
    wait_for_process "iggy-server" 10 || send_signal "iggy-server" "KILL"
}

function run_suite() {
    local suite="$1"
    local wait_timeout="$2"
    local command="$3"
    local log_file
    log_file=$(result_log_name "$suite" "$wait_timeout")

    echo
    echo "Suite: ${suite}, poll wait timeout: ${wait_timeout}"
    echo "Command: ${command}"

    if [[ "$DRY_RUN" == true ]]; then
        return 0
    fi

    start_server
    set +e
    eval "$command" 2>&1 | tee "$log_file"
    local status=${PIPESTATUS[0]}
    set -e
    stop_server

    echo
    echo "Summary from ${log_file}:"
    grep -E "Results:|Total throughput|latency:" "$log_file" || true

    if [[ $status -ne 0 ]]; then
        echo "Benchmark failed with status ${status}: ${suite}, wait=${wait_timeout}" >&2
        exit "$status"
    fi
}

function common_output_args() {
    local suite="$1"
    local wait_timeout="$2"
    echo "output --output-dir ${OUTPUT_DIR} --identifier ${IDENTIFIER} --remark ${suite}_wait_${wait_timeout} --extra-info poll_wait_timeout_comparison --gitref ${commit_hash} --gitref-date ${commit_date}"
}

function command_for_sparse_single() {
    local wait_timeout="$1"
    local output_args
    output_args=$(common_output_args "sparse_single" "$wait_timeout")
    echo "${IGGY_BENCH_CMD} --message-size 1000 --messages-per-batch 1 --message-batches ${SPARSE_BATCHES} --rate-limit 10KB --poll-wait-timeout ${wait_timeout} pinned-producer-and-consumer --streams 1 --producers 1 --consumers 1 $(bench_transport) ${output_args}"
}

function command_for_sparse_consumer_group() {
    local wait_timeout="$1"
    local output_args
    output_args=$(common_output_args "sparse_consumer_group" "$wait_timeout")
    echo "${IGGY_BENCH_CMD} --message-size 1000 --messages-per-batch 1 --message-batches ${GROUP_BATCHES} --rate-limit 10KB --poll-wait-timeout ${wait_timeout} balanced-producer-and-consumer-group --streams 1 --partitions 4 --producers 1 --consumers 4 $(bench_transport) ${output_args}"
}

function command_for_saturated_control() {
    local wait_timeout="$1"
    local output_args
    output_args=$(common_output_args "saturated_control" "$wait_timeout")
    echo "${IGGY_BENCH_CMD} --message-size 1000 --messages-per-batch 1000 --message-batches ${SATURATED_BATCHES} --poll-wait-timeout ${wait_timeout} pinned-producer-and-consumer --streams 1 --producers 1 --consumers 1 $(bench_transport) ${output_args}"
}

echo "Running PollMessages wait-timeout benchmark comparison"
echo "transport=${TRANSPORT}, identifier=${IDENTIFIER}, output_dir=${OUTPUT_DIR}, quick=${QUICK}, dry_run=${DRY_RUN}"

for wait_timeout in "${wait_timeouts[@]}"; do
    run_suite "sparse_single" "$wait_timeout" "$(command_for_sparse_single "$wait_timeout")"
done

for wait_timeout in "${wait_timeouts[@]}"; do
    run_suite "sparse_consumer_group" "$wait_timeout" "$(command_for_sparse_consumer_group "$wait_timeout")"
done

for wait_timeout in "0s" "100ms"; do
    run_suite "saturated_control" "$wait_timeout" "$(command_for_saturated_control "$wait_timeout")"
done

echo
echo "Comparison complete. Full logs and benchmark artifacts are in ${OUTPUT_DIR}."
