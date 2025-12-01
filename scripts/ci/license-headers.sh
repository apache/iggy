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

# Common patterns to ignore (build artifacts, dependencies, IDE files)
IGNORE_PATTERNS=(
  "**/target/**"
  "target/**"
  "**/node_modules/**"
  "node_modules/**"
  "**/.venv/**"
  ".venv/**"
  "**/venv/**"
  "venv/**"
  "**/dist/**"
  "dist/**"
  "**/build/**"
  "build/**"
  "**/.idea/**"
  ".idea/**"
  "**/.vscode/**"
  ".vscode/**"
  "**/.gradle/**"
  ".gradle/**"
  "**/.svelte-kit/**"
  ".svelte-kit/**"
  "**/bin/**"
  "**/obj/**"
  "**/local_data*/**"
  "**/performance_results*/**"
)

# Determine how to run addlicense: prefer local binary, fall back to Docker
USE_DOCKER=false
ADDLICENSE_CMD=""

if command -v addlicense &> /dev/null; then
  ADDLICENSE_CMD="addlicense"
  echo "Using local addlicense binary"
elif command -v docker &> /dev/null; then
  USE_DOCKER=true
  echo "Local addlicense not found, using Docker fallback"
  echo "Pulling addlicense Docker image..."
  docker pull ghcr.io/google/addlicense:latest >/dev/null 2>&1
else
  echo "❌ Neither addlicense nor docker command found"
  echo "💡 Install addlicense: go install github.com/google/addlicense@latest"
  echo "   Or install Docker to use the containerized version"
  exit 1
fi

# Build ignore flags for addlicense
IGNORE_FLAGS=()
for pattern in "${IGNORE_PATTERNS[@]}"; do
  IGNORE_FLAGS+=("-ignore" "$pattern")
done

run_addlicense() {
  local extra_args=("$@")

  if [ "$USE_DOCKER" = true ]; then
    docker run --rm -v "$REPO_ROOT:/src" -w /src \
      ghcr.io/google/addlicense:latest \
      -f ASF_LICENSE.txt "${IGNORE_FLAGS[@]}" "${extra_args[@]}"
  else
    "$ADDLICENSE_CMD" -f ASF_LICENSE.txt "${IGNORE_FLAGS[@]}" "${extra_args[@]}"
  fi
}

if [ "$MODE" = "fix" ]; then
  echo "🔧 Adding license headers to files..."
  run_addlicense .
  echo "✅ License headers have been added to files"
else
  echo "🔍 Checking license headers..."

  TEMP_FILE=$(mktemp)
  trap 'rm -f "$TEMP_FILE"' EXIT

  if run_addlicense -check . > "$TEMP_FILE" 2>&1; then
    echo "✅ All files have proper license headers"
  else
    file_count=$(wc -l < "$TEMP_FILE")
    echo "❌ Found $file_count files missing license headers:"
    echo ""
    cat "$TEMP_FILE" | sed 's/^/  • /'
    echo ""
    echo "💡 Run '$0 --fix' to add license headers automatically"

    if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
      {
        echo "## ❌ License Headers Missing"
        echo ""
        echo "The following files are missing Apache license headers:"
        echo '```'
        cat "$TEMP_FILE"
        echo '```'
        echo "Please run \`./scripts/ci/license-headers.sh --fix\` to fix automatically."
      } >> "$GITHUB_STEP_SUMMARY"
    fi

    exit 1
  fi
fi
