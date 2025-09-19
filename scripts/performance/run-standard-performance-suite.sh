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

IGGY_BENCH_CMD=""
IDENTIFIER=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
    --identifier)
        IDENTIFIER="$2"
        shift 2
        ;;
    *)
        if [ -z "$IGGY_BENCH_CMD" ]; then
            IGGY_BENCH_CMD="$1"
        fi
        shift
        ;;
    esac
done

# Set default binary path if not provided
if [ -z "$IGGY_BENCH_CMD" ]; then
    IGGY_BENCH_CMD="target/release/iggy-bench"
fi

echo "Using iggy-bench binary: ${IGGY_BENCH_CMD}"
if [ -n "$IDENTIFIER" ]; then
    echo "Using identifier: ${IDENTIFIER}"
else
    echo "Using hostname: $(hostname) as identifier"
fi

set -euo pipefail

# Load utility functions
source "$(dirname "$0")/../utils.sh"
source "$(dirname "$0")/utils.sh"

# Trap SIGINT (Ctrl+C) and execute the on_exit function, do the same on script exit
trap on_exit_bench SIGINT
trap on_exit_bench EXIT

# Function to get environment variables based on benchmark type
get_env_vars() {
    local bench_type="$1"
    local env_vars=()

    env_vars+=("IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy")

    # Specific env vars based on bench type
    case "$bench_type" in
    *"no_cache_fsync"*)
        env_vars+=("IGGY_SYSTEM_CACHE_ENABLED=false IGGY_SYSTEM_PARTITION_ENFORCE_FSYNC=true")
        ;;
    *"only_cache"*)
        env_vars+=("IGGY_SYSTEM_CACHE_SIZE=9GB")
        ;;
    *"no_cache"*)
        env_vars+=("IGGY_SYSTEM_CACHE_ENABLED=false")
        ;;
    *"no_wait"*)
        env_vars+=("IGGY_SYSTEM_SEGMENT_SERVER_CONFIRMATION=no_wait")
        ;;
    esac

    # Convert array to env var string
    local env_string=""
    for var in "${env_vars[@]}"; do
        env_string+="$var "
    done
    echo "$env_string"
}

# Build the project
echo "Building project..."
RUSTFLAGS="-C target-cpu=native" cargo build --release

# Create a directory for the performance results
(mkdir -p performance_results || true) &>/dev/null

##############################
#      Double benchmarks     #
##############################

# Large batch tests with cache enabled
NORMAL_BATCH_ONLY_CACHE_PINNED_PRODUCER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-producer" 8 8 1000 1000 1000 tcp "send_only_cache" "$IDENTIFIER") # 8GB data, 1KB messages, 1000 msgs/batch with forced cache
NORMAL_BATCH_ONLY_CACHE_PINNED_CONSUMER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-consumer" 8 8 1000 1000 1000 tcp "poll_only_cache" "$IDENTIFIER") # 8GB data, 1KB messages, 1000 msgs/batch with forced cache

# Large batch tests with cache disabled
NORMAL_BATCH_NO_CACHE_PINNED_PRODUCER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-producer" 8 8 1000 1000 1000 tcp "send_no_cache" "$IDENTIFIER") # 8GB data, 1KB messages, 1000 msgs/batch with disabled cache
NORMAL_BATCH_NO_CACHE_PINNED_CONSUMER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-consumer" 8 8 1000 1000 1000 tcp "send_no_cache" "$IDENTIFIER") # 8GB data, 1KB messages, 1000 msgs/batch with disabled cache

# Large batch tests with no wait and with cache configuration
NORMAL_BATCH_NO_WAIT_ONLY_CACHE_PINNED_PRODUCER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-producer" 8 8 1000 1000 1000 tcp "send_no_wait_only_cache" "$IDENTIFIER") # 8GB data, 1KB messages, 1000 msgs/batch with no_wait config
NORMAL_BATCH_NO_WAIT_ONLY_CACHE_PINNED_CONSUMER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-consumer" 8 8 1000 1000 1000 tcp "send_no_wait_only_cache" "$IDENTIFIER") # 8GB data, 1KB messages, 1000 msgs/batch with no_wait config

# Single actor tests with cache disabled
# shellcheck disable=SC2034
NO_CACHE_SINGLE_PINNED_PRODUCER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-producer" 1 1 1000 1000 5000 tcp "1_producer_no_cache" "$IDENTIFIER") # 5GB data, 1KB messages, 100 msgs/batch with forced cache
# shellcheck disable=SC2034
NO_CACHE_SINGLE_PINNED_CONSUMER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-consumer" 1 1 1000 1000 5000 tcp "1_consumer_no_cache" "$IDENTIFIER") # 5GB data, 1KB messages, 100 msgs/batch with forced cache

# Consumer group tests with cache enabled
BALANCED_ONLY_CACHE_PRODUCER=$(construct_bench_command "$IGGY_BENCH_CMD" "balanced-producer" 1 8 1000 1000 1000 tcp "only_cache" "$IDENTIFIER")             # Balanced producer benchmark
BALANCED_ONLY_CACHE_CONSUMER_GROUP=$(construct_bench_command "$IGGY_BENCH_CMD" "balanced-consumer-group" 1 8 1000 1000 1000 tcp "only_cache" "$IDENTIFIER") # Consumer group benchmark

# Single actor tests with cache disabled and rate limit 100 MB/s
NO_CACHE_RL_SINGLE_PINNED_PRODUCER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-producer" 1 1 1000 1000 2000 tcp "1_producer_no_cache_rl_100MB" "$IDENTIFIER" "100MB") # 2GB data, 1KB messages, 100 msgs/batch with forced cache
NO_CACHE_RL_SINGLE_PINNED_CONSUMER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-consumer" 1 1 1000 1000 2000 tcp "1_consumer_no_cache_rl_100MB" "$IDENTIFIER" "100MB") # 2GB data, 1KB messages, 100 msgs/batch with forced cache

# Single actor tests with cache disabled, fsync enabled and rate limit 100 MB/s
NO_CACHE_FSYNC_RL_SINGLE_PINNED_PRODUCER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-producer" 1 1 1000 1000 2000 tcp "1_producer_no_cache_fsync_rl_100MB" "$IDENTIFIER" "100MB") # 2GB data, 1KB messages, 100 msgs/batch with forced cache
NO_CACHE_FSYNC_RL_SINGLE_PINNED_CONSUMER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-consumer" 1 1 1000 1000 2000 tcp "1_consumer_no_cache_fsync_rl_100MB" "$IDENTIFIER" "100MB") # 2GB data, 1KB messages, 100 msgs/batch with forced cache

###############################
#      Single benchmarks      #
###############################

# Parallel producer and consumer
NORMAL_BATCH_NO_CACHE_PINNED_PRODUCER_AND_CONSUMER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-producer-and-consumer" 8 8 1000 1000 1000 tcp "no_cache" "$IDENTIFIER")     # 8GB data, 1KB messages, 100 msgs/batch, no cache
NORMAL_BATCH_ONLY_CACHE_PINNED_PRODUCER_AND_CONSUMER=$(construct_bench_command "$IGGY_BENCH_CMD" "pinned-producer-and-consumer" 8 8 1000 1000 1000 tcp "only_cache" "$IDENTIFIER") # 8GB data, 1KB messages, 100 msgs/batch, no cache

# Parallel producer and consumer group test
BALANCED_NO_CACHE_CONSUMER_GROUP_PRODUCER_AND_CONSUMER=$(construct_bench_command "$IGGY_BENCH_CMD" "balanced-producer-and-consumer-group" 1 8 1000 1000 1000 tcp "cg_no_cache" "$IDENTIFIER")
BALANCED_ONLY_CACHE_CONSUMER_GROUP_PRODUCER_AND_CONSUMER=$(construct_bench_command "$IGGY_BENCH_CMD" "balanced-producer-and-consumer-group" 1 8 1000 1000 1000 tcp "cg_only_cache" "$IDENTIFIER")

# End-to-end tests
END_TO_END_NO_CACHE_PRODUCING_CONSUMER=$(construct_bench_command "$IGGY_BENCH_CMD" "end-to-end-producing-consumer" 8 8 1000 1000 1000 tcp "e2e_no_cache" "$IDENTIFIER")                   # Combined producer and consumer benchmark
END_TO_END_ONLY_CACHE_PRODUCING_CONSUMER=$(construct_bench_command "$IGGY_BENCH_CMD" "end-to-end-producing-consumer" 8 8 1000 1000 1000 tcp "e2e_only_cache" "$IDENTIFIER")               # Combined producer and consumer benchmark
END_TO_END_NO_CACHE_PRODUCING_CONSUMER_GROUP=$(construct_bench_command "$IGGY_BENCH_CMD" "end-to-end-producing-consumer-group" 1 8 1000 1000 1000 tcp "e2ecg_no_cache" "$IDENTIFIER")     # Combined producer and consumer benchmark
END_TO_END_ONLY_CACHE_PRODUCING_CONSUMER_GROUP=$(construct_bench_command "$IGGY_BENCH_CMD" "end-to-end-producing-consumer-group" 1 8 1000 1000 1000 tcp "e2ecg_only_cache" "$IDENTIFIER") # Combined producer and consumer benchmark

# Make an array of the suites
DOUBLE_SUITES=(
    "$NORMAL_BATCH_ONLY_CACHE_PINNED_PRODUCER"
    "$NORMAL_BATCH_ONLY_CACHE_PINNED_CONSUMER"
    "$NORMAL_BATCH_NO_CACHE_PINNED_PRODUCER"
    "$NORMAL_BATCH_NO_CACHE_PINNED_CONSUMER"
    "$NORMAL_BATCH_NO_WAIT_ONLY_CACHE_PINNED_PRODUCER"
    "$NORMAL_BATCH_NO_WAIT_ONLY_CACHE_PINNED_CONSUMER"
    "$BALANCED_ONLY_CACHE_PRODUCER"
    "$BALANCED_ONLY_CACHE_CONSUMER_GROUP"
    "$NO_CACHE_RL_SINGLE_PINNED_PRODUCER"
    "$NO_CACHE_RL_SINGLE_PINNED_CONSUMER"
    "$NO_CACHE_FSYNC_RL_SINGLE_PINNED_PRODUCER"
    "$NO_CACHE_FSYNC_RL_SINGLE_PINNED_CONSUMER"
)

SINGLE_SUITES=(
    "$NORMAL_BATCH_NO_CACHE_PINNED_PRODUCER_AND_CONSUMER"
    "$NORMAL_BATCH_ONLY_CACHE_PINNED_PRODUCER_AND_CONSUMER"
    "$END_TO_END_ONLY_CACHE_PRODUCING_CONSUMER"
    "$END_TO_END_NO_CACHE_PRODUCING_CONSUMER"
    "$BALANCED_ONLY_CACHE_CONSUMER_GROUP_PRODUCER_AND_CONSUMER"
    "$BALANCED_NO_CACHE_CONSUMER_GROUP_PRODUCER_AND_CONSUMER"
    "$END_TO_END_ONLY_CACHE_PRODUCING_CONSUMER_GROUP"
    "$END_TO_END_NO_CACHE_PRODUCING_CONSUMER_GROUP"
)

echo
echo "Running single benchmark tests..."
echo

# Run each single benchmark suites
for suite in "${SINGLE_SUITES[@]}"; do
    # Remove old local_data
    echo "Cleaning old local_data..."
    rm -rf local_data || true

    # Get environment variables based on benchmark type
    ENV_VARS=$(get_env_vars "$suite")

    echo "Starting iggy-server with command:"

    # Start iggy-server with appropriate configuration
    if [[ -n "$ENV_VARS" ]]; then
        echo "$ENV_VARS target/release/iggy-server"
        eval "$ENV_VARS target/release/iggy-server" &>/dev/null &
    else
        echo "target/release/iggy-server"
        IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy target/release/iggy-server &>/dev/null &
    fi
    echo
    sleep 1
    IGGY_SERVER_PID=$(pgrep -x "iggy-server")
    sleep 1

    # Check if the server is running
    exit_if_process_is_not_running "$IGGY_SERVER_PID"

    # Run the benchmark
    echo "Running benchmark:"
    echo "$ENV_VARS ${suite}"
    results=$(eval "$ENV_VARS ${suite}" | grep -e "Results:")
    echo
    echo "Results:"
    echo "${results}"
    echo
    sleep 1

    # Stop the server gracefully
    echo "Stopping iggy-server with pid ${IGGY_SERVER_PID}..."
    send_signal_to_pid "$IGGY_SERVER_PID" "TERM"
    wait_for_process_pid "$IGGY_SERVER_PID" 10

    # Wait for disk DRAM to empty
    echo "Waiting for disk to clean cache..."
    sleep 5
done

echo
echo "Running double benchmark tests..."
echo

# Run the double benchmark suites, iterate over two elements at a time
for ((i = 0; i < ${#DOUBLE_SUITES[@]}; i += 2)); do
    PRODUCER_BENCH="${DOUBLE_SUITES[i]}"
    CONSUMER_BENCH="${DOUBLE_SUITES[i + 1]}"

    # Remove old local_data
    echo "Cleaning old local_data..."
    rm -rf local_data || true

    # Get environment variables based on benchmark type
    ENV_VARS=$(get_env_vars "$PRODUCER_BENCH")

    echo "Starting iggy-server with command:"

    # Start iggy-server with appropriate configuration
    if [[ -n "$ENV_VARS" ]]; then
        echo "$ENV_VARS target/release/iggy-server"
        eval "$ENV_VARS target/release/iggy-server" &>/dev/null &
    else
        echo "target/release/iggy-server"
        target/release/iggy-server &>/dev/null &
    fi
    echo
    sleep 1
    IGGY_SERVER_PID=$(pgrep -x "iggy-server")
    sleep 1

    # Check if the server is running
    exit_if_process_is_not_running "$IGGY_SERVER_PID"

    # Start producer bench
    echo "Running producer bench:"
    echo "$ENV_VARS ${PRODUCER_BENCH}"
    producer_results=$(eval "$ENV_VARS ${PRODUCER_BENCH}" | grep -e "Results:")
    echo
    echo "Producer results:"
    echo "${producer_results}"
    echo
    sleep 1

    # Start consumer bench
    echo "Running consumer bench:"
    echo "$ENV_VARS ${CONSUMER_BENCH}"
    consumer_results=$(eval "$ENV_VARS ${CONSUMER_BENCH}" | grep -e "Results:")
    echo
    echo "Consumer results:"
    echo "${consumer_results}"
    echo
    sleep 1

    # Stop the server gracefully
    echo "Stopping iggy-server with pid ${IGGY_SERVER_PID}..."
    send_signal_to_pid "$IGGY_SERVER_PID" "TERM"
    wait_for_process_pid "$IGGY_SERVER_PID" 10

    # Wait for disk DRAM to empty
    echo "Waiting for disk to clean cache..."
    sleep 5
done

exit 0
