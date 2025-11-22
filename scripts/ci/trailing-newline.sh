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
      echo "  --check   Check for missing trailing newlines (default)"
      echo "  --fix     Add trailing newlines to files"
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

echo "Checking ${#CHANGED_FILES[@]} file(s) for trailing newlines..."

# Track files with issues
FILES_WITHOUT_NEWLINE=()

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

  # Skip empty files
  if [ ! -s "$file" ]; then
    continue
  fi

  # Check if file ends with a newline
  # Use tail to get last byte and od to check if it's a newline (0x0a)
  if ! tail -c 1 "$file" | od -An -tx1 | grep -q '0a'; then
    FILES_WITHOUT_NEWLINE+=("$file")
  fi
done

# Fix mode
if [ "$MODE" = "fix" ]; then
  if [ ${#FILES_WITHOUT_NEWLINE[@]} -eq 0 ]; then
    echo "✅ All files have trailing newlines"
    exit 0
  fi

  echo "🔧 Adding trailing newlines to ${#FILES_WITHOUT_NEWLINE[@]} file(s)..."
  for file in "${FILES_WITHOUT_NEWLINE[@]}"; do
    # Add newline if file doesn't end with one
    if [ -n "$(tail -c 1 "$file")" ]; then
      echo >> "$file"
      echo "  Fixed: $file"
    fi
  done
  echo "✅ Trailing newlines added to ${#FILES_WITHOUT_NEWLINE[@]} file(s)"
  exit 0
fi

# Check mode
if [ ${#FILES_WITHOUT_NEWLINE[@]} -eq 0 ]; then
  echo "✅ All text files have trailing newlines"
  exit 0
fi

echo "❌ Found ${#FILES_WITHOUT_NEWLINE[@]} file(s) without trailing newline:"
echo ""

for file in "${FILES_WITHOUT_NEWLINE[@]}"; do
  echo "  • $file"
  # Show last few characters of the file for context
  echo -n "    Last characters: '"
  tail -c 20 "$file" | tr '\n' '↵' | sed 's/\t/→/g'
  echo "'"
  echo ""
done

echo "💡 Run '$0 --fix' to add trailing newlines automatically"
exit 1
