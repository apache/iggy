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

set -euo pipefail

# Script to run Go examples from examples/go/README.md files
# Usage: ./scripts/run-go-examples-from-readme.sh [OPTIONS]
#
# --goos   - Optional target OS (e.g., linux, darwin)
# --goarch - Optional target architecture (e.g., amd64, arm64)
# --target - Optional target architecture for rust (e.g., x86_64-unknown-linux-musl)
# If not provided, uses the default target
#
# This script will run all the commands from examples/go/README.md files
# and check if they pass or fail.
# If any command fails, it will print the command and exit with non-zero status.
# If all commands pass, it will remove the log file and exit with zero status.
#
# Note: This script assumes that the iggy-server is not running and will start it in the background.
#       It will wait until the server is started before running the commands.
#       It will also terminate the server after running all the commands.
#       Script executes every command in examples/go/README.md files which is enclosed in backticks (`) and starts
#       with `go run`. Other commands are ignored.
#       Order of commands in README files is important as script will execute them from top to bottom.
#

readonly LOG_FILE="iggy-server.log"
readonly PID_FILE="iggy-server.pid"
readonly TIMEOUT=300

GOOS="" # Go target OS
GOARCH="" # Go target architecture
TARGET="" # Iggy server target architecture

# Get GOOS, GOARCH, and Cargo --target values from arguments or use defaults
while [[ $# -gt 0 ]]; do
    case "$1" in
        --goos)
            GOOS="$2"
            shift 2
            ;;
        --goarch)
            GOARCH="$2"
            shift 2
            ;;
        --target)
            TARGET="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--goos GOOS] [--goarch GOARCH] [--target TARGET]"
            exit 1
            ;;
    esac
done

if [ -n "${GOOS}" ]; then
    echo "Using GOOS=${GOOS}"
fi
if [ -n "${GOARCH}" ]; then
    echo "Using GOARCH=${GOARCH}"
fi
if [ -n "${TARGET}" ]; then
    echo "Using cargo --target ${TARGET}"
fi

# Remove old server data if present
test -d local_data && rm -fr local_data
test -e ${LOG_FILE} && rm ${LOG_FILE}
test -e ${PID_FILE} && rm ${PID_FILE}

# Check if server binary exists
SERVER_BIN=""
if [ -n "${TARGET}" ]; then
    SERVER_BIN="target/${TARGET}/debug/iggy-server"
else
    SERVER_BIN="target/debug/iggy-server"
fi

if [ ! -f "${SERVER_BIN}" ]; then
    echo "Error: Server binary not found at ${SERVER_BIN}"
    echo "Please build the server binary before running this script:"
    if [ -n "${TARGET}" ]; then
        echo "  cargo build --target ${TARGET} --bin iggy-server"
    else
        echo "  cargo build --bin iggy-server"
    fi
    exit 1
fi

echo "Using server binary at ${SERVER_BIN}"

# Run iggy server using the prebuilt binary
echo "Starting server from ${SERVER_BIN}..."
IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy ${SERVER_BIN} &>${LOG_FILE} &
echo $! >${PID_FILE}

# Wait until "Iggy server has started" string is present inside iggy-server.log
SERVER_START_TIME=0
while ! grep -q "has started" ${LOG_FILE}; do
    if [ ${SERVER_START_TIME} -gt ${TIMEOUT} ]; then
        echo "Server did not start within ${TIMEOUT} seconds."
        ps fx
        cat ${LOG_FILE}
        exit 1
    fi
    echo "Waiting for Iggy server to start... ${SERVER_START_TIME}"
    sleep 1
    ((SERVER_START_TIME += 1))
done

cd examples/go

# Execute all example commands from examples/go/README.md and check if they pass or fail
exit_code=0
if [ -f "README.md" ]; then
    while IFS= read -r command; do
        # Remove backticks and comments from command
        command=$(echo "${command}" | tr -d '`' | sed 's/^#.*//')
        # Skip empty lines
        if [ -z "${command}" ]; then
            continue
        fi
        # Add GOOS/GOARCH env if specified
        [ -n "${GOOS}" ] && command="GOOS=${GOOS} ${command}"
        [ -n "${GOARCH}" ] && command="GOARCH=${GOARCH} ${command}"

        echo -e "\e[33mChecking example command from examples/go/README.md:\e[0m ${command}"
        echo ""

        set +e
        eval "${command}"
        exit_code=$?
        set -e

        # Stop at first failure
        if [ ${exit_code} -ne 0 ]; then
            echo ""
            echo -e "\e[31mExample command failed:\e[0m ${command}"
            echo ""
            break
        fi
        # Add a small delay between examples to avoid potential race conditions
        sleep 2

    done < <(grep -E "^go run" "README.md")
fi

cd ../..

# Terminate server
kill -TERM "$(cat ${PID_FILE})"
test -e ${PID_FILE} && rm ${PID_FILE}

# If everything is ok remove log and pid files otherwise cat server log
if [ "${exit_code}" -eq 0 ]; then
    echo "Test passed"
else
    echo "Test failed, see log file:"
    test -e ${LOG_FILE} && cat ${LOG_FILE}
fi

test -e ${LOG_FILE} && rm ${LOG_FILE}
test -e ${PID_FILE} && rm ${PID_FILE}

exit "${exit_code}"
