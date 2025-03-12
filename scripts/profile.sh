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

# Load utility functions
source "$(dirname "$0")/utils.sh"

# Trap SIGINT (Ctrl+C) and execute the on_exit function
trap on_exit_profile SIGINT
trap on_exit_profile EXIT

PROFILED_APP_NAME=$1
PROFILING_MODE=$2

# Check if the script is being run with the correct arguments
if [[ -z "${PROFILED_APP_NAME}" || -z "${PROFILING_MODE}" ]]; then
    echo "Usage: $0 <app_name> <profiling_mode>"
    exit 1
fi

# Check if the app name and profiling mode are valid
if [[ "${PROFILED_APP_NAME}" != "iggy-bench" ]] && [[ "${PROFILED_APP_NAME}" != "iggy-server" ]]; then
    echo "Invalid app name. Please use 'iggy-bench' or 'iggy-server'."
    exit 1
fi

# Check if the profiling mode is valid
if [[ "${PROFILING_MODE}" != "cpu" ]] && [[ "${PROFILING_MODE}" != "io" ]]; then
    echo "Invalid profiling mode. Please use 'cpu' or 'io'."
    exit 1
fi

# Clone FlameGraph repository to /tmp if it doesn't exist
if [[ "$PROFILING_MODE" == "io" ]]; then
    if [[ -d /tmp/FlameGraph ]]; then
        echo "FlameGraph repository exists in /tmp/FlameGraph"
    else
        echo "Cloning FlameGraph repository to /tmp/FlameGraph..."
        git clone https://github.com/brendangregg/FlameGraph.git /tmp/FlameGraph
    fi
fi

# Detect OS
OS="$(uname -s)"
if [ "$OS" = "Darwin" ]; then
    echo "Error: Profiling with perf and flamegraph is currently only supported on Linux systems."
    echo "For macOS profiling, consider using Instruments.app or DTrace."
    exit 1
fi

# Linux-specific profiling setup
paranoid=$(cat /proc/sys/kernel/perf_event_paranoid)
restrict=$(cat /proc/sys/kernel/kptr_restrict)

# Check if the system settings for perf are configured correctly
if [[ "${paranoid}" -ne -1 ]] || [[ "${restrict}" -ne 0 ]]; then
    echo "System settings for perf are not configured correctly."
    echo "Please run the following commands"
    echo "echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid"
    echo "echo 0 | sudo tee /proc/sys/kernel/kptr_restrict"
    exit 1
fi

# Check for required tools
for tool in addr2line flamegraph perf; do
    if ! command -v "${tool}" &> /dev/null; then
        echo "Required tool ${tool} is not installed."
        exit 1
    fi
done

# Validate addr2line version
addr2line_version=$(addr2line --version)
if [[ ! "${addr2line_version}" == *"0.2"* ]]; then
    echo "Incompatible addr2line version. Please install the correct version from https://github.com/gimli-rs/addr2line"
    exit 1
fi

# Export environment variable for Cargo
export CARGO_PROFILE_RELEASE_DEBUG=true

# Build the project
echo "Building project..."
cargo build --release > /dev/null

# Remove old data
echo "Cleaning old local_data..."
rm -rf local_data perf*

# Variables for log and output files
GIT_INFO=$(get_git_info)
SERVER_LOG_FILE="profiling_server_${GIT_INFO}.log"
BENCH_SEND_LOG_FILE="profiling_bench_send_${GIT_INFO}.log"
BENCH_POLL_LOG_FILE="profiling_bench_poll_${GIT_INFO}.log"
FLAMEGRAPH_SEND_SVG="flamegraph_send_${PROFILING_MODE}_${PROFILED_APP_NAME}_${GIT_INFO}.svg"
FLAMEGRAPH_POLL_SVG="flamegraph_poll_${PROFILING_MODE}_${PROFILED_APP_NAME}_${GIT_INFO}.svg"

# Start iggy-server and capture its PID
echo "Running iggy-server, log will be in ${SERVER_LOG_FILE}..."
target/release/iggy-server > "${SERVER_LOG_FILE}" 2>&1 &
sleep 1

# Run iggy-bench pinned-producer tcp
echo "Running iggy-bench pinned-producer tcp..."
if [[ "${PROFILED_APP_NAME}" == "iggy-server" ]]; then
    # Start flamegraph for iggy-server (send)
    echo "Starting flamegraph (send) on iggy-server..."

    if [[ "${PROFILING_MODE}" == "cpu" ]]; then
        flamegraph -o "${FLAMEGRAPH_SEND_SVG}" --pid "$(pgrep iggy-server)" 2>&1 &
    else
        perf record -a -g -p "$(pgrep iggy-server)" -o perf_server_send.data -- sleep 30 2>&1 &
    fi
    sleep 1

    target/release/iggy-bench pinned-producer tcp > "${BENCH_SEND_LOG_FILE}"

    # Trigger flamegraph (send) completion
    send_signal "perf" "TERM"

    # Wait for perf to finish
    wait_for_process "perf" 10

    # Process perf data
    if [[ "${PROFILING_MODE}" == "io" ]]; then
        perf script --header -i perf_server_send.data > perf_server_send.stacks
        rm perf_server_send.data
        /tmp/FlameGraph/stackcollapse-perf.pl < perf_server_send.stacks | /tmp/FlameGraph/flamegraph.pl --color=io \
            --title="iggy-server send I/O Flame Graph" --countname="I/O" > "${FLAMEGRAPH_SEND_SVG}"
    fi

    # Start flamegraph for iggy-server (poll)
    echo "Starting flamegraph (poll)..."
    if [[ "${PROFILING_MODE}" == "cpu" ]]; then
        flamegraph -o "${FLAMEGRAPH_POLL_SVG}" --pid "$(pgrep iggy-server)" 2>&1 &
    else
        perf record -a -g -p "$(pgrep iggy-server)" -o perf_server_poll.data -- sleep 30 2>&1 &
    fi
    sleep 1

    # Run iggy-bench pinned-consumer tcp
    echo "Running iggy-bench pinned-consumer tcp..."
    target/release/iggy-bench pinned-consumer tcp > "${BENCH_POLL_LOG_FILE}"

    # Trigger flamegraph (poll) completion
    send_signal "perf" "TERM"

    # Wait for perf to finish
    wait_for_process "perf" 10

    # Process perf data
    if [[ "${PROFILING_MODE}" == "io" ]]; then
        perf script --header -i perf_server_poll.data > perf_server_poll.stacks
        rm perf_server_poll.data
        /tmp/FlameGraph/stackcollapse-perf.pl < perf_server_poll.stacks | /tmp/FlameGraph/flamegraph.pl --color=io \
            --title="iggy-server poll I/O Flame Graph" --countname="I/O" > "${FLAMEGRAPH_POLL_SVG}"
    fi
else
    echo "Starting flamegraph (send) on iggy-bench..."
    if [[ "${PROFILING_MODE}" == "cpu" ]]; then
        cargo flamegraph --bin iggy-bench -o "${FLAMEGRAPH_SEND_SVG}" -- pinned-producer tcp > "${BENCH_SEND_LOG_FILE}"
    else
        perf record -a -g -o perf_bench_send.data -- target/release/iggy-bench pinned-producer tcp > "${BENCH_SEND_LOG_FILE}"
        perf script --header -i perf_bench_send.data > perf_bench_send.stacks
        /tmp/FlameGraph/stackcollapse-perf.pl < perf_bench_send.stacks | /tmp/FlameGraph/flamegraph.pl --color=io \
            --title="iggy client send I/O Flame Graph" --countname="I/O" > "${FLAMEGRAPH_SEND_SVG}"
    fi

    sleep 1

    echo "Starting flamegraph (poll) on iggy-bench..."
    if [[ "${PROFILING_MODE}" == "cpu" ]]; then
        cargo flamegraph --bin iggy-bench -o "${FLAMEGRAPH_POLL_SVG}" -- pinned-consumer tcp > "${BENCH_POLL_LOG_FILE}"
    else
        perf record -a -g -o perf_bench_poll.data -- target/release/iggy-bench pinned-consumer tcp > "${BENCH_POLL_LOG_FILE}"
        perf script --header -i perf_bench_poll.data > perf_bench_poll.stacks
        /tmp/FlameGraph/stackcollapse-perf.pl < perf_bench_poll.stacks | /tmp/FlameGraph/flamegraph.pl --color=io \
            --title="iggy client poll I/O Flame Graph" --countname="I/O" > "${FLAMEGRAPH_POLL_SVG}"
    fi
fi

# Gracefully stop the server
send_signal "iggy-server" "TERM"
wait_for_process "iggy-server" 10

# Display results
echo
echo "Send results:"
grep -e "Results: " "${BENCH_SEND_LOG_FILE}"

echo
echo "Poll results:"
grep -e "Results: " "${BENCH_POLL_LOG_FILE}"

echo
echo "Flamegraph svg (send) saved to ${FLAMEGRAPH_SEND_SVG}"
echo "Flamegraph svg (poll) saved to ${FLAMEGRAPH_POLL_SVG}"

exit 0
