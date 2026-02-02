#!/bin/bash

#
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
#

set -euo pipefail

# Script to run Java examples from examples/java/README.md
# Usage: ./scripts/run-java-examples-from-readme.sh [TARGET]
#
# TARGET - Optional target architecture (e.g., x86_64-unknown-linux-musl)
#          If not provided, uses the default target
#
# This script scans examples/java/README.md for commands starting with
# `./gradlew` and executes them in order. If any command fails, the script
# stops immediately and prints the relevant iggy-server logs.

readonly LOG_FILE="iggy-server.log"
readonly PID_FILE="iggy-server.pid"
readonly TIMEOUT=300

ROOT_WORKDIR="$(pwd)"
TARGET="${1:-}"

if [ -n "${TARGET}" ]; then
    echo "Using target architecture: ${TARGET}"
else
    echo "Using default target architecture"
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

cd examples/java

exit_code=0
README_FILE="README.md"

if [ -f "${README_FILE}" ]; then
    while IFS= read -r command; do
        # Remove backticks and comments from command
        command=$(echo "${command}" | tr -d '`' | sed 's/^#.*//')
        # Skip empty lines
        if [ -z "${command}" ]; then
            continue
        fi

        echo -e "\e[33mChecking example command from ${README_FILE}:\e[0m ${command}"
        echo ""

        set +e
        eval "${command}"
        exit_code=$?
        set -e

        if [ ${exit_code} -ne 0 ]; then
            echo ""
            echo -e "\e[31mExample command failed:\e[0m ${command}"
            echo ""
            break
        fi

        # Small delay between runs to avoid thrashing the server
        sleep 2
    done < <(grep -E '^\./gradlew' "${README_FILE}")
else
    echo "README file ${README_FILE} not found in examples/java."
fi

cd "${ROOT_WORKDIR}"

# Terminate server
kill -TERM "$(cat ${PID_FILE})"
test -e ${PID_FILE} && rm ${PID_FILE}

if [ "${exit_code}" -eq 0 ]; then
    echo "Test passed"
else
    echo "Test failed, see log file:"
    test -e ${LOG_FILE} && cat ${LOG_FILE}
fi

test -e ${LOG_FILE} && rm ${LOG_FILE}
test -e ${PID_FILE} && rm ${PID_FILE}

exit "${exit_code}"
