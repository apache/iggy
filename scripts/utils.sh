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
