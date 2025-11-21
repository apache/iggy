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

set -euo pipefail

# Default mode
MODE="check"

# Parse arguments
if [ $# -gt 0 ]; then
  case "$1" in
    --check)
      MODE="check"
      ;;
    --update|--fix)
      MODE="update"
      ;;
    *)
      echo "Usage: $0 [--check|--update|--fix]"
      echo "  --check   Check if DEPENDENCIES.md is up to date (default)"
      echo "  --update  Update DEPENDENCIES.md with current dependencies"
      echo "  --fix     Alias for --update"
      exit 1
      ;;
  esac
fi

# Check if cargo-license is installed
if ! command -v cargo-license &>/dev/null; then
  echo "❌ cargo-license command not found"
  echo "💡 Install it using: cargo install cargo-license"
  exit 1
fi

# Check if DEPENDENCIES.md exists
if [ ! -f "DEPENDENCIES.md" ] && [ "$MODE" = "check" ]; then
  echo "❌ DEPENDENCIES.md does not exist"
  echo "💡 Run '$0 --fix' to create it"
  exit 1
fi

# Generate current dependencies
TEMP_FILE=$(mktemp)
trap 'rm -f "$TEMP_FILE"' EXIT

echo "Generating current dependencies list..."
cargo license --color never --do-not-bundle --all-features >"$TEMP_FILE"

# Update mode
if [ "$MODE" = "update" ]; then
  echo "🔧 Updating DEPENDENCIES.md..."
  {
    echo "# Dependencies"
    echo ""
    cat "$TEMP_FILE"
  } >DEPENDENCIES.md
  echo "✅ DEPENDENCIES.md has been updated"
  exit 0
fi

# Check mode
if [ "$MODE" = "check" ]; then
  echo "🔍 Checking if DEPENDENCIES.md is up to date..."
  # Create expected format for comparison
  EXPECTED_FILE=$(mktemp)
  trap 'rm -f "$TEMP_FILE" "$EXPECTED_FILE"' EXIT
  {
    echo "# Dependencies"
    echo ""
    cat "$TEMP_FILE"
  } >"$EXPECTED_FILE"

  if ! diff -q "$EXPECTED_FILE" DEPENDENCIES.md >/dev/null; then
    echo "❌ DEPENDENCIES.md is out of date"
    echo ""
    echo "Diff:"
    diff -u DEPENDENCIES.md "$EXPECTED_FILE" || true
    echo ""
    echo "💡 Run '$0 --fix' to update it"
    exit 1
  else
    echo "✅ DEPENDENCIES.md is up to date"
  fi
fi
