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

# This script is used to generate Cross.toml file for user which executes
# this script. This is needed since Cross.toml build.dockerfile.build-args
# section requires statically defined Docker build arguments and parameters
# like current UID or GID must be entered (cannot be generated or fetched
# during cross execution time).

set -euo pipefail

# Default mode
MODE="help"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
    --update)
        MODE="update"
        shift
        ;;
    --check)
        MODE="check"
        shift
        ;;
    *)
        echo "Unknown option: $1"
        MODE="help"
        shift
        ;;
    esac
done

# Display usage if no valid arguments provided
if [ "$MODE" = "help" ]; then
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  --check    Check if DEPENDENCIES.md is up to date"
    echo "  --update   Update DEPENDENCIES.md with current dependencies"
    exit 0
fi

# Check if cargo-license is installed
if ! command -v cargo-license &>/dev/null; then
    echo "Installing cargo-license..."
    cargo install cargo-license
fi

# Check if DEPENDENCIES.md exists
if [ ! -f "DEPENDENCIES.md" ] && [ "$MODE" = "check" ]; then
    echo "Error: DEPENDENCIES.md does not exist."
    exit 1
fi

# Generate current dependencies
TEMP_FILE=$(mktemp)
trap 'rm -f "$TEMP_FILE"' EXIT

echo "Generating current dependencies list..."
cargo license --color never --do-not-bundle --all-features >"$TEMP_FILE"

# Update mode
if [ "$MODE" = "update" ]; then
    echo "Updating DEPENDENCIES.md..."
    cp "$TEMP_FILE" DEPENDENCIES.md
    echo "DEPENDENCIES.md has been updated."
    exit 0
fi

# Check mode
if [ "$MODE" = "check" ]; then
    echo "Checking if DEPENDENCIES.md is up to date..."
    if ! diff -q "$TEMP_FILE" DEPENDENCIES.md >/dev/null; then
        echo "Error: DEPENDENCIES.md is out of date. Please run '$0 --update' to update it."
        echo "Diff:"
        diff -u DEPENDENCIES.md "$TEMP_FILE"
        exit 1
    else
        echo "DEPENDENCIES.md is up to date."
    fi
fi
