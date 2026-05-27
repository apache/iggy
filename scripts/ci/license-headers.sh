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

# Parse arguments
MODE="check"
if [ $# -gt 0 ]; then
  case "$1" in
    --check)
      MODE="check"
      ;;
    --fix)
      MODE="fix"
      ;;
    *)
      echo "Usage: $0 [--check|--fix]"
      echo "  --check  Check files for Apache license headers (default)"
      echo "  --fix    Add Apache license headers to files missing them"
      exit 1
      ;;
  esac
fi

# Get the repository root
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# Check if ASF_LICENSE.txt exists
if [ ! -f "ASF_LICENSE.txt" ]; then
  echo "❌ ASF_LICENSE.txt not found in repository root"
  exit 1
fi

if [ ! -f "licenserc.toml" ]; then
  echo "❌ licenserc.toml not found in repository root"
  exit 1
fi

# Determine how to run HawkEye: prefer local binary, fall back to Docker
USE_DOCKER=false
HAWKEYE_CMD=""

if command -v hawkeye &> /dev/null; then
  HAWKEYE_CMD="hawkeye"
  echo "Using local HawkEye binary"
elif command -v docker &> /dev/null; then
  USE_DOCKER=true
  echo "Local HawkEye not found, using Docker fallback"
  echo "Pulling HawkEye Docker image..."
  docker pull ghcr.io/korandoru/hawkeye:latest >/dev/null 2>&1
else
  echo "❌ Neither hawkeye nor docker command found"
  echo "💡 Install HawkEye: cargo install hawkeye"
  echo "   Or install Docker to use the containerized version"
  exit 1
fi

run_hawkeye() {
  local extra_args=("$@")

  if [ "$USE_DOCKER" = true ]; then
    docker run --rm -v "$REPO_ROOT:/src" -w /src \
      ghcr.io/korandoru/hawkeye:latest \
      "${extra_args[@]}"
  else
    "$HAWKEYE_CMD" "${extra_args[@]}"
  fi
}

extract_hawkeye_paths() {
  local key="$1"
  local file="$2"

  sed -n "s/.*\"$key\":\\[//; s/\\],\".*//; s/\\]}.*//; p" "$file" \
    | tr ',' '\n' \
    | sed 's/^"//; s/"$//' \
    | awk -v root="$REPO_ROOT" '
        NF {
          if (index($0, root "/") == 1) {
            print "./" substr($0, length(root) + 2)
          } else {
            print $0
          }
        }
      ' \
    | sort
}

append_hawkeye_section() {
  local title="$1"
  local paths_file="$2"

  if [ ! -s "$paths_file" ]; then
    return
  fi

  local count
  count=$(wc -l < "$paths_file" | tr -d ' ')

  echo "$title ($count):"
  sed 's/^/  - /' "$paths_file"
  echo ""
}

write_hawkeye_report() {
  local json_file="$1"
  local report_file="$2"
  local missing_file
  local unknown_file

  missing_file=$(mktemp)
  unknown_file=$(mktemp)

  extract_hawkeye_paths missing "$json_file" > "$missing_file"
  extract_hawkeye_paths unknown "$json_file" > "$unknown_file"

  {
    append_hawkeye_section "Missing license headers" "$missing_file"
    append_hawkeye_section "Unknown file types" "$unknown_file"
  } > "$report_file"

  rm -f "$missing_file" "$unknown_file"
}

if [ "$MODE" = "fix" ]; then
  echo "🔧 Adding license headers to files..."
  run_hawkeye format --config licenserc.toml --fail-if-updated false --fail-if-unknown
  echo "✅ License headers have been added to files"
else
  echo "🔍 Checking license headers..."

  TEMP_FILE=$(mktemp)
  REPORT_FILE=$(mktemp)
  LOG_FILE=$(mktemp)
  trap 'rm -f "$TEMP_FILE" "$REPORT_FILE" "$LOG_FILE"' EXIT

  if run_hawkeye check --config licenserc.toml --fail-if-unknown -o "$TEMP_FILE" > "$LOG_FILE" 2>&1; then
    echo "✅ All files have proper license headers"
  else
    write_hawkeye_report "$TEMP_FILE" "$REPORT_FILE"

    echo "❌ Found files with missing or inconsistent license headers:"
    echo ""
    if [ -s "$REPORT_FILE" ]; then
      sed 's/^/  /' < "$REPORT_FILE"
    else
      sed 's/^/  /' < "$LOG_FILE"
    fi
    echo ""
    echo "💡 Run '$0 --fix' to add license headers automatically"

    if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
      {
        echo "## ❌ License Headers Missing"
        echo ""
        echo "HawkEye reported the following license header issues:"
        echo '```'
        if [ -s "$REPORT_FILE" ]; then
          cat "$REPORT_FILE"
        else
          cat "$LOG_FILE"
        fi
        echo '```'
        echo "Please run \`./scripts/ci/license-headers.sh --fix\` to fix automatically."
      } >> "$GITHUB_STEP_SUMMARY"
    fi

    exit 1
  fi
fi
