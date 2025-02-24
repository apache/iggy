#!/bin/bash

set -euo pipefail

# Script to run examples from README.md file
# Usage: ./scripts/run-examples-from-readme.sh
#
# This script will run all the commands from README.md file and check if they pass or fail.
# If any command fails, it will print the command and exit with non-zero status.
# If all commands pass, it will remove the log file and exit with zero status.
#
# Note: This script assumes that the iggy-server is not running and will start it in the background.
#       It will wait until the server is started before running the commands.
#       It will also terminate the server after running all the commands.
#       Script executes every command in README.md which is enclosed in backticks (`) and and start
#       with `cargo r --bin iggy -- `. Other commands are ignored. Order of commands in README.md
#       is important as script will execute them from top to bottom.
#

readonly LOG_FILE="iggy-server.log"
readonly PID_FILE="iggy-server.pid"
readonly TIMEOUT=300

# Remove old server data if present
test -d local_data && rm -fr local_data
test -e ${LOG_FILE} && rm ${LOG_FILE}
test -e ${PID_FILE} && rm ${PID_FILE}

# Run iggy server and let it run in the background
cargo run --bin iggy-server &>${LOG_FILE} &
echo $! >${PID_FILE}

# Wait until "Iggy server has started" string is present inside iggy-server.log
SERVER_START_TIME=0
while ! grep -q "Iggy server has started" ${LOG_FILE}; do
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

# Execute all matching CLI commands from README.md and check if they pass or fail
while IFS= read -r command; do
    # Remove backticks from command
    command=$(echo "${command}" | tr -d '`')
    echo -e "\e[33mChecking CLI command:\e[0m ${command}"
    echo ""

    set +e
    eval "${command}"
    exit_code=$?
    set -e

    # Stop at first failure
    if [ ${exit_code} -ne 0 ]; then
        echo ""
        echo -e "\e[31mCLI command failed:\e[0m ${command}"
        echo ""
        break
    fi

done < <(grep -E "^\`cargo r --bin iggy -- " README.md)

# Execute all example commands from README.md and check if they pass or fail
while IFS= read -r command; do
    # Remove backticks and comments from command
    command=$(echo "${command}" | tr -d '`' | sed 's/^#.*//')
    # Skip empty lines
    if [ -z "${command}" ]; then
        continue
    fi
    echo -e "\e[33mChecking example command:\e[0m ${command}"
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

done < <(grep -E "^cargo run --example" README.md)

# Terminate server
kill -TERM "$(cat ${PID_FILE})"
test -e ${PID_FILE} && rm ${PID_FILE}

# If everything is ok remove log and pid files otherwise cat server log
if [ "${exit_code}" -eq 0 ]; then
    test -e ${LOG_FILE} && rm ${LOG_FILE}
else
    cat ${LOG_FILE}
fi

exit "${exit_code}"
