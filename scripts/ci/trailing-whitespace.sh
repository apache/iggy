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
      echo "  --check   Check for trailing whitespace (default)"
      echo "  --fix     Remove trailing whitespace"
      echo ""
      echo "File selection:"
      echo "  --staged  Check staged files (default, for git hooks)"
      echo "  --ci      Check files changed in PR (for CI)"
      echo "  --all     Check all tracked files"
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

# Get files to check based on mode
get_files() {
  case "$FILE_MODE" in
    staged)
      # Get staged files for git hooks
      git diff --cached --name-only --diff-filter=ACM
      ;;
    ci)
      # Get files changed in PR for CI
      if [ -n "${GITHUB_BASE_REF:-}" ]; then
        # GitHub Actions PR context
        git fetch --no-tags --depth=1 origin "${GITHUB_BASE_REF}:${GITHUB_BASE_REF}" 2>/dev/null || true
        git diff --name-only --diff-filter=ACM "${GITHUB_BASE_REF}...HEAD"
      elif [ -n "${CI:-}" ]; then
        # Generic CI - compare with HEAD~1
        git diff --name-only --diff-filter=ACM HEAD~1
      else
        # Fallback to staged files
        git diff --cached --name-only --diff-filter=ACM
      fi
      ;;
    all)
      # Get all tracked files
      git ls-files
      ;;
  esac
}

# If files were provided as arguments, use them
if [ ${#FILES[@]} -gt 0 ]; then
  CHANGED_FILES=("${FILES[@]}")
else
  # Get files based on mode
  CHANGED_FILES=()
  while IFS= read -r file; do
    CHANGED_FILES+=("$file")
  done < <(get_files)
fi

# Exit early if no files to check
if [ ${#CHANGED_FILES[@]} -eq 0 ]; then
  echo "No files to check"
  exit 0
fi

echo "Checking ${#CHANGED_FILES[@]} file(s) for trailing whitespace..."

# Track files with issues
FILES_WITH_TRAILING=()

# Check each file
for file in "${CHANGED_FILES[@]}"; do
  # Skip if file doesn't exist (might be deleted)
  if [ ! -f "$file" ]; then
    continue
  fi

  # Skip binary files
  if file "$file" | grep -qE "binary|data|executable|compressed"; then
    continue
  fi

  # Check for trailing whitespace
  if grep -q '[[:space:]]$' "$file" 2>/dev/null; then
    FILES_WITH_TRAILING+=("$file")
  fi
done

# Fix mode
if [ "$MODE" = "fix" ]; then
  if [ ${#FILES_WITH_TRAILING[@]} -eq 0 ]; then
    echo "✅ No trailing whitespace found"
    exit 0
  fi

  echo "🔧 Removing trailing whitespace from ${#FILES_WITH_TRAILING[@]} file(s)..."
  for file in "${FILES_WITH_TRAILING[@]}"; do
    # Remove trailing whitespace (in-place)
    sed -i 's/[[:space:]]*$//' "$file"
    echo "  Fixed: $file"
  done
  echo "✅ Trailing whitespace removed from ${#FILES_WITH_TRAILING[@]} file(s)"
  exit 0
fi

# Check mode
if [ ${#FILES_WITH_TRAILING[@]} -eq 0 ]; then
  echo "✅ No trailing whitespace found"
  exit 0
fi

echo "❌ Found trailing whitespace in ${#FILES_WITH_TRAILING[@]} file(s):"
echo ""

for file in "${FILES_WITH_TRAILING[@]}"; do
  echo "  • $file"
  # Show lines with trailing whitespace (limit to first 3 occurrences per file)
  grep -n '[[:space:]]$' "$file" | head -3 | while IFS=: read -r line_num content; do
    # Show the line with visible whitespace markers
    visible_content=$(echo "$content" | sed 's/ /·/g; s/\t/→/g')
    echo "    Line $line_num: '${visible_content}'"
  done

  TOTAL_LINES=$(grep -c '[[:space:]]$' "$file")
  if [ "$TOTAL_LINES" -gt 3 ]; then
    echo "    ... and $((TOTAL_LINES - 3)) more lines"
  fi
  echo ""
done

echo "💡 Run '$0 --fix' to remove trailing whitespace automatically"
exit 1
