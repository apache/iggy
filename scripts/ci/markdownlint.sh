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
      echo "  --check  Check markdown files for issues (default)"
      echo "  --fix    Automatically fix markdown issues"
      exit 1
      ;;
  esac
fi

# Check if markdownlint is installed
if ! command -v markdownlint &> /dev/null; then
  echo "❌ markdownlint command not found"
  echo "💡 Install it using: npm install -g markdownlint-cli"
  exit 1
fi

# Files to ignore (in addition to .gitignore)
IGNORE_FILES="CLAUDE.md"

if [ "$MODE" = "fix" ]; then
  echo "🔧 Fixing markdown files..."
  markdownlint '**/*.md' --ignore-path .gitignore --ignore "$IGNORE_FILES" --fix
  echo "✅ Markdown files have been fixed"
else
  echo "🔍 Checking markdown files..."
  if markdownlint '**/*.md' --ignore-path .gitignore --ignore "$IGNORE_FILES"; then
    echo "✅ All markdown files are properly formatted"
  else
    echo "❌ Markdown linting failed"
    echo "💡 Run '$0 --fix' to auto-fix issues"
    exit 1
  fi
fi
