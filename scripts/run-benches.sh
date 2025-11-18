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

# Trap SIGINT (Ctrl+C) and execute the on_exit function, do the same on script exit
trap on_exit_bench SIGINT
trap on_exit_bench EXIT

# Remove old local_data
echo "Cleaning old local_data..."
rm -rf local_data

# Build the project
echo "Building project..."
cargo build --release

# Start iggy-server
echo "Running iggy-server..."
IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy target/release/iggy-server &> /dev/null &
sleep 1

# Start tcp send bench
echo "Running iggy-bench pinned-producer tcp..."
send_results=$(target/release/iggy-bench pinned-producer tcp | grep -e "Results:")
sleep 1

# Display results
echo
echo "Send results:"
echo "${send_results}"
echo

# Start tcp poll bench
echo "Running iggy-bench pinned-consumer tcp..."
poll_results=$(target/release/iggy-bench pinned-consumer tcp | grep -e "Results: total throughput")

echo "Poll results:"
echo "${poll_results}"
echo

# Gracefully stop the server
send_signal "iggy-server" "TERM"
wait_for_process "iggy-server" 5

exit 0
