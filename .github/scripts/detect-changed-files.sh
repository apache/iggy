#!/usr/bin/env bash
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
# -------------------------------------------------------------
#
# This script is used to detect the changed files in a pull request
# and analyze them based on the configuration provided in the
# .github/changed-files-config.json file.
#
set -euo pipefail

# Check if the debug flag is set in GitHub Actions
if [[ "${RUNNER_DEBUG:-0}" = "1" ]]; then
    # If the debug flag is set, enable verbose mode in current bash session
    # to print the executed commands and their output to the console
    # for debugging purposes and set verbose flag for Python script.
    set -x
    VERBOSE_FLAG="--verbose"
else
    VERBOSE_FLAG=""
fi

# Check if yq is installed
if ! command -v python3 &> /dev/null; then
    echo "python3 is required but not installed, please install it"
    exit 1
fi

if [ -z "${GITHUB_EVENT_NAME:-}" ]; then
    # If the script is not running in a GitHub Actions environment (e.g., running locally),
    # get the changed files based on the last commit
    echo "The script is not running in a GitHub Actions environment"
    CHANGED_FILES=$(git diff --name-only HEAD^)
else
    # If the script is running in a GitHub Actions environment, check the event type
    # Get the changed files based on the event type
    if [[ "${GITHUB_EVENT_NAME}" == "push" ]]; then
        # If the event type is push, get the changed files based on the last commit
        echo "The script is running in a GitHub Actions environment for a push event"
        CHANGED_FILES=$(git diff --name-only HEAD^)
    else
        # If the event type is not push (assuming pull request), get the changed files based
        # on the base and head refs of the pull request. If the GITHUB_BASE_REF and GITHUB_HEAD_REF
        # environment variables are not set, exit the script with an error message.
        if [[ -z "${GITHUB_BASE_REF:-}" || -z "${GITHUB_HEAD_REF:-}" ]]; then
            echo "The GITHUB_BASE_REF or GITHUB_HEAD_REF environment variable is not set"
            exit 1
        fi
        # Get the changed files based on the base and head refs of the pull request
        echo "The script is running in a GitHub Actions environment for a pull request event"
        CHANGED_FILES=$(git diff --name-only "origin/${GITHUB_BASE_REF}" "origin/${GITHUB_HEAD_REF}")
    fi
fi

# Analyze the changed files
# shellcheck disable=SC2086
python3 .github/scripts/analyze_changed_files.py ${VERBOSE_FLAG} "${CHANGED_FILES}" .github/changed-files-config.json
