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

# Default values
MODE="check"
FILE_MODE="staged"
FILES=()

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --check)
      MODE="check"
      shift
      ;;
    --fix)
      MODE="fix"
      shift
      ;;
    --staged)
      FILE_MODE="staged"
      shift
      ;;
    --ci)
      FILE_MODE="ci"
      shift
      ;;
    --all)
      FILE_MODE="all"
      shift
      ;;
    --help|-h)
      echo "Usage: $0 [--check|--fix] [--staged|--ci|--all] [files...]"
      echo ""
      echo "Modes:"
      echo "  --check   Check TOML formatting (default)"
      echo "  --fix     Format TOML files"
      echo ""
      echo "File selection:"
      echo "  --staged  Check staged files (default, for git hooks)"
      echo "  --ci      Check files changed in PR (for CI)"
      echo "  --all     Check all TOML files"
      echo "  [files]   Check specific files"
      exit 0
      ;;
    -*)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
    *)
      # Treat as file argument
      FILES+=("$1")
      shift
      ;;
  esac
done

# Check if taplo is installed
if ! command -v taplo &> /dev/null; then
  echo "❌ taplo is not installed"
  echo ""
  echo "Install with one of:"
  echo "  cargo install taplo-cli --locked"
  echo "  brew install taplo (macOS)"
  echo "  Download from: https://github.com/tamasfe/taplo/releases"
  exit 1
fi

# Get files to check based on mode
get_files() {
  case "$FILE_MODE" in
    staged)
      # Get staged TOML files for git hooks
      git diff --cached --name-only --diff-filter=ACM -- '*.toml'
      ;;
    ci)
      # Get TOML files changed in PR for CI
      if [ -n "${GITHUB_BASE_REF:-}" ]; then
        # GitHub Actions PR context
        git fetch --no-tags --depth=1 origin "${GITHUB_BASE_REF}:${GITHUB_BASE_REF}" 2>/dev/null || true
        git diff --name-only --diff-filter=ACM "${GITHUB_BASE_REF}...HEAD" -- '*.toml'
      elif [ -n "${CI:-}" ]; then
        # Generic CI - compare with HEAD~1
        git diff --name-only --diff-filter=ACM HEAD~1 -- '*.toml'
      else
        # Fallback to staged files
        git diff --cached --name-only --diff-filter=ACM -- '*.toml'
      fi
      ;;
    all)
      # Get all TOML files (excluding common build directories)
      find . -name '*.toml' \
        -not -path './target/*' \
        -not -path './node_modules/*' \
        -not -path './.git/*' \
        -not -path './venv/*' \
        -type f
      ;;
  esac
}

# If files were provided as arguments, use them
if [ ${#FILES[@]} -gt 0 ]; then
  TOML_FILES=("${FILES[@]}")
else
  # Get files based on mode
  TOML_FILES=()
  while IFS= read -r file; do
    [ -n "$file" ] && TOML_FILES+=("$file")
  done < <(get_files)
fi

# Exit early if no files to check
if [ ${#TOML_FILES[@]} -eq 0 ]; then
  echo "✅ No TOML files to check"
  exit 0
fi

echo "Checking ${#TOML_FILES[@]} TOML file(s)..."

# Fix mode
if [ "$MODE" = "fix" ]; then
  echo "🔧 Formatting TOML files..."
  FAILED=0
  for file in "${TOML_FILES[@]}"; do
    if [ -f "$file" ]; then
      if taplo fmt "$file" 2>/dev/null; then
        echo "  Formatted: $file"
      else
        echo "  ❌ Failed: $file"
        FAILED=1
      fi
    fi
  done
  if [ $FAILED -eq 0 ]; then
    echo "✅ All TOML files formatted"
  else
    echo "❌ Some files failed to format"
    exit 1
  fi
  exit 0
fi

# Check mode
FILES_WITH_ISSUES=()
for file in "${TOML_FILES[@]}"; do
  if [ -f "$file" ]; then
    if ! taplo fmt --check "$file" 2>/dev/null; then
      FILES_WITH_ISSUES+=("$file")
    fi
  fi
done

if [ ${#FILES_WITH_ISSUES[@]} -eq 0 ]; then
  echo "✅ All TOML files properly formatted"
  exit 0
fi

echo "❌ Found formatting issues in ${#FILES_WITH_ISSUES[@]} file(s):"
echo ""
for file in "${FILES_WITH_ISSUES[@]}"; do
  echo "  • $file"
  # Show diff preview (limit output)
  taplo fmt --check --diff "$file" 2>/dev/null | head -20 | sed 's/^/    /'
  echo ""
done

echo "💡 Run '$0 --fix' to format automatically"
exit 1
