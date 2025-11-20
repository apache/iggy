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
      echo "  --check  Check shell scripts for issues (default)"
      echo "  --fix    Show detailed suggestions for fixes"
      exit 1
      ;;
  esac
fi

# Check if shellcheck is installed
if ! command -v shellcheck &> /dev/null; then
  echo "❌ shellcheck command not found"
  echo "💡 Install it using:"
  echo "   • Ubuntu/Debian: sudo apt-get install shellcheck"
  echo "   • macOS: brew install shellcheck"
  echo "   • Or visit: https://www.shellcheck.net/"
  exit 1
fi

echo "shellcheck version: $(shellcheck --version)"

# Get repository root
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# Directories to exclude
EXCLUDE_PATHS=(
  "./target/*"
  "./node_modules/*"
  "./.git/*"
  "./foreign/node/node_modules/*"
  "./foreign/python/.venv/*"
  "./.venv/*"
  "./venv/*"
  "./build/*"
  "./dist/*"
)

# Build find exclusion arguments
FIND_EXCLUDE_ARGS=()
for path in "${EXCLUDE_PATHS[@]}"; do
  FIND_EXCLUDE_ARGS+=("-not" "-path" "$path")
done

if [ "$MODE" = "fix" ]; then
  echo "🔧 Running shellcheck with detailed suggestions..."
  echo ""
  echo "Note: shellcheck does not support automatic fixing."
  echo "Please review the suggestions below and fix issues manually."
  echo ""

  # Run with detailed format
  FAILED=0
  while IFS= read -r -d '' script; do
    echo "Checking: $script"
    if ! shellcheck -f gcc "$script"; then
      FAILED=1
    fi
    echo ""
  done < <(find . -type f -name "*.sh" "${FIND_EXCLUDE_ARGS[@]}" -print0)

  if [ $FAILED -eq 1 ]; then
    echo "❌ Found issues in shell scripts"
    echo "💡 Fix the issues reported above manually"
    exit 1
  else
    echo "✅ All shell scripts passed shellcheck"
  fi
else
  echo "🔍 Checking shell scripts..."

  # Run shellcheck on all shell scripts (checks all severities: error, warning, info, style)
  if find . -type f -name "*.sh" "${FIND_EXCLUDE_ARGS[@]}" -exec shellcheck {} +; then
    echo "✅ All shell scripts passed shellcheck"
  else
    echo ""
    echo "❌ Shellcheck found issues in shell scripts"
    echo "💡 Run '$0 --fix' to see detailed suggestions"
    exit 1
  fi
fi
