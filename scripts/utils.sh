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

# Function to get the git branch and commit hash
function get_git_info() {
    local git_info
    git_info=$(git log -1 --pretty=format:"%h")
    local git_branch
    git_branch=$(git rev-parse --abbrev-ref HEAD)
    echo "${git_branch}_${git_info}"
}

# OS detection
OS=$(uname -s)

# Function to check if process exists - OS specific implementations
function process_exists() {
    local check_pid=$1
    if [ "$OS" = "Darwin" ]; then
        ps -p "${check_pid}" >/dev/null
    else
        [[ -e /proc/${check_pid} ]]
    fi
}

# Function to send signal to process - OS specific implementations
function send_signal_to_pid() {
    local target_pid=$1
    local signal=$2
    if [ "$OS" = "Darwin" ]; then
        kill "-${signal}" "${target_pid}"
    else
        kill -s "${signal}" "${target_pid}"
    fi
}

# Function to wait for a process with specific name to exit
function wait_for_process() {
    local process_name=$1
    local timeout=$2
    local start_time
    start_time=$(date +%s)
    local end_time=$((start_time + timeout))
    local continue_outer_loop=false

    while [[ $(date +%s) -lt ${end_time} ]]; do
        continue_outer_loop=false
        local proc_pid
        for proc_pid in $(pgrep -x "${process_name}"); do
            if process_exists "${proc_pid}"; then
                sleep 0.1
                continue_outer_loop=true
                break
            fi
        done
        [[ $continue_outer_loop == true ]] && continue
        return 0
    done

    echo "Timeout waiting for process ${process_name} to exit."
    return 1
}

# Function to wait for a process with specific PID to exit
function wait_for_process_pid() {
    local wait_pid=$1
    local timeout=$2
    local start_time
    start_time=$(date +%s)
    local end_time=$((start_time + timeout))

    while [[ $(date +%s) -lt ${end_time} ]]; do
        if ! process_exists "${wait_pid}"; then
            return 0
        fi
        sleep 0.1
    done

    echo "Timeout waiting for process with PID ${wait_pid} to exit."
    return 1
}

# Function to send a signal to a process
function send_signal() {
    local process_name=$1
    local pids
    pids=$(pgrep -x "${process_name}") || true
    local signal=$2

    if [[ -n "${pids}" ]]; then
        local proc_pid
        for proc_pid in ${pids}; do
            if process_exists "${proc_pid}"; then
                send_signal_to_pid "${proc_pid}" "${signal}"
            fi
        done
    fi
}

# Function to exit with error if a process with the given PID is running
exit_if_process_is_not_running() {
    local check_pid="$1"

    if kill -0 "$check_pid" 2>/dev/null; then
        echo "Process with PID $check_pid is running."
        return 0
    else
        echo "Error: Process with PID $check_pid is not running."
        exit 1
    fi
}

# Exit hook for profile.sh
function on_exit_profile() {
    # Gracefully stop the server
    send_signal "iggy-server" "KILL"
    send_signal "iggy-bench" "KILL"
    send_signal "flamegraph" "KILL"
    send_signal "perf" "KILL"
}

# Exit hook for run-benches.sh
function on_exit_bench() {
    send_signal "iggy-server" "KILL"
    # Use exact match for iggy-bench to avoid killing iggy-bench-dashboard
    pids=$(pgrep -x "iggy-bench") || true
    if [[ -n "${pids}" ]]; then
        local bench_pid
        for bench_pid in ${pids}; do
            if process_exists "${bench_pid}"; then
                send_signal_to_pid "${bench_pid}" "KILL"
            fi
        done
    fi
}

# ---------------------------------------------------------------------------
# Server lifecycle helpers for example-runner scripts
# ---------------------------------------------------------------------------

readonly EXAMPLES_LOG_FILE="iggy-server.log"
readonly EXAMPLES_PID_FILE="iggy-server.pid"
readonly EXAMPLES_SERVER_TIMEOUT=300

# Resolve and validate the server binary path.
# Usage: resolve_server_binary [target]
# Sets global SERVER_BIN.
function resolve_server_binary() {
    local target="${1:-}"
    if [ -n "${target}" ]; then
        SERVER_BIN="target/${target}/debug/iggy-server"
    else
        SERVER_BIN="target/debug/iggy-server"
    fi

    if [ ! -f "${SERVER_BIN}" ]; then
        echo "Error: Server binary not found at ${SERVER_BIN}"
        echo "Please build the server binary before running this script:"
        if [ -n "${target}" ]; then
            echo "  cargo build --target ${target} --bin iggy-server"
        else
            echo "  cargo build --bin iggy-server"
        fi
        exit 1
    fi
    echo "Using server binary at ${SERVER_BIN}"
}

# Resolve and validate the CLI binary path.
# Usage: resolve_cli_binary [target]
# Sets global CLI_BIN.
function resolve_cli_binary() {
    local target="${1:-}"
    if [ -n "${target}" ]; then
        CLI_BIN="target/${target}/debug/iggy"
    else
        CLI_BIN="target/debug/iggy"
    fi

    if [ ! -f "${CLI_BIN}" ]; then
        echo "Error: CLI binary not found at ${CLI_BIN}"
        echo "Please build the CLI and examples before running this script:"
        if [ -n "${target}" ]; then
            echo "  cargo build --target ${target} --bin iggy --examples"
        else
            echo "  cargo build --bin iggy --examples"
        fi
        exit 1
    fi
    echo "Using CLI binary at ${CLI_BIN}"
}

# Remove old server data, log, and PID files.
function cleanup_server_state() {
    rm -fr local_data
    rm -f "${EXAMPLES_LOG_FILE}" "${EXAMPLES_PID_FILE}"
}

# Start the plain (non-TLS) iggy server in background.
# Usage: start_plain_server [extra_args...]
function start_plain_server() {
    echo "Starting server from ${SERVER_BIN}..."
    IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy \
        ${SERVER_BIN} "$@" &>"${EXAMPLES_LOG_FILE}" &
    echo $! >"${EXAMPLES_PID_FILE}"
}

# Start the TLS-enabled iggy server in background.
# Usage: start_tls_server [extra_args...]
function start_tls_server() {
    echo "Starting TLS server from ${SERVER_BIN}..."
    IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy \
        IGGY_TCP_TLS_ENABLED=true \
        IGGY_TCP_TLS_CERT_FILE=core/certs/iggy_cert.pem \
        IGGY_TCP_TLS_KEY_FILE=core/certs/iggy_key.pem \
        ${SERVER_BIN} "$@" &>"${EXAMPLES_LOG_FILE}" &
    echo $! >"${EXAMPLES_PID_FILE}"
}

# Block until "has started" appears in the server log or timeout.
# Usage: wait_for_server_ready [label]
function wait_for_server_ready() {
    local label="${1:-Iggy}"
    local elapsed=0
    while ! grep -q "has started" "${EXAMPLES_LOG_FILE}"; do
        if [ ${elapsed} -gt ${EXAMPLES_SERVER_TIMEOUT} ]; then
            echo "${label} server did not start within ${EXAMPLES_SERVER_TIMEOUT} seconds."
            ps fx 2>/dev/null || ps aux
            cat "${EXAMPLES_LOG_FILE}"
            exit 1
        fi
        echo "Waiting for ${label} server to start... ${elapsed}"
        sleep 1
        ((elapsed += 1))
    done
}

# Gracefully stop the server using the PID file.
function stop_server() {
    if [ -e "${EXAMPLES_PID_FILE}" ]; then
        kill -TERM "$(cat "${EXAMPLES_PID_FILE}")" 2>/dev/null || true
        sleep 2
        rm -f "${EXAMPLES_PID_FILE}"
    fi
}

# Print final result and dump the log on failure.
function report_result() {
    local exit_code=$1
    if [ "${exit_code}" -eq 0 ]; then
        echo "Test passed"
    else
        echo "Test failed, see log file:"
        test -e "${EXAMPLES_LOG_FILE}" && cat "${EXAMPLES_LOG_FILE}"
    fi
    rm -f "${EXAMPLES_LOG_FILE}" "${EXAMPLES_PID_FILE}"
}

# Portable timeout wrapper (macOS has gtimeout via coreutils, Linux has timeout).
function portable_timeout() {
    local secs="$1"
    shift
    if command -v timeout >/dev/null 2>&1; then
        timeout "${secs}" "$@"
    elif command -v gtimeout >/dev/null 2>&1; then
        gtimeout "${secs}" "$@"
    else
        eval "$@"
    fi
}

# Run commands extracted from a README file.
# Usage: run_readme_commands readme_file grep_pattern [cmd_timeout]
# Reads matching lines, strips backticks/comments, executes each.
# Calls TRANSFORM_COMMAND function on each command if defined.
# Returns: sets global EXAMPLES_EXIT_CODE.
function run_readme_commands() {
    local readme_file="$1"
    local grep_pattern="$2"
    local cmd_timeout="${3:-0}"

    if [ ! -f "${readme_file}" ]; then
        return
    fi

    local commands
    commands=$(grep -E "${grep_pattern}" "${readme_file}" || true)
    if [ -z "${commands}" ]; then
        return
    fi

    while IFS= read -r command; do
        command=$(echo "${command}" | tr -d '`' | sed 's/^#.*//')
        if [ -z "${command}" ]; then
            continue
        fi

        if declare -f TRANSFORM_COMMAND >/dev/null 2>&1; then
            command=$(TRANSFORM_COMMAND "${command}")
        fi

        echo -e "\e[33mChecking command from ${readme_file}:\e[0m ${command}"
        echo ""

        set +e
        if [ "${cmd_timeout}" -gt 0 ] 2>/dev/null; then
            eval "portable_timeout ${cmd_timeout} ${command}"
            local test_exit_code=$?
            # timeout exit 124 is tolerated (command ran but exceeded timeout)
            if [[ ${test_exit_code} -ne 0 && ${test_exit_code} -ne 124 ]]; then
                EXAMPLES_EXIT_CODE=${test_exit_code}
            fi
        else
            eval "${command}"
            EXAMPLES_EXIT_CODE=$?
        fi
        set -e

        if [ "${EXAMPLES_EXIT_CODE}" -ne 0 ]; then
            echo ""
            echo -e "\e[31mCommand failed:\e[0m ${command}"
            echo ""
            return
        fi

        sleep 2
    done <<< "${commands}"
}
