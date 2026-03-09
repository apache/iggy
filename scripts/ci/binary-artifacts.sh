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

# binary-artifacts.sh -- Prevent compiled binaries from entering the repo.
#
# .gitignore catches common extensions (*.o, *.so, *.exe, *.out, etc.) but
# extensionless binaries slip through (e.g. `rust_out` from `rustc --test`).
#
# This script uses file(1) to inspect actual file content and reject:
#   - ELF executables, shared objects, and relocatables (Linux)
#   - Mach-O executables and universal binaries (macOS)
#   - PE32/PE32+ executables (Windows)
#   - WebAssembly modules, compiled Java classes, .NET assemblies
#
# Runs in two contexts:
#   pre-commit hook  -- checks staged files   (--check --staged)
#   CI workflow      -- checks PR diff files  (--check --ci)
#
# Exit codes: 0 = clean, 1 = binary artifacts found or error.

FILE_MODE="staged"
FILES=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check)
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
      echo "Usage: $0 [--check] [--staged|--ci|--all] [files...]"
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
      FILES+=("$1")
      shift
      ;;
  esac
done

get_files() {
  case "$FILE_MODE" in
    staged)
      git diff --cached --name-only --diff-filter=ACM
      ;;
    ci)
      if [ -n "${GITHUB_BASE_REF:-}" ]; then
        git fetch --no-tags --depth=1 origin "${GITHUB_BASE_REF}:${GITHUB_BASE_REF}" 2>/dev/null || true
        git diff --name-only --diff-filter=ACM "${GITHUB_BASE_REF}...HEAD"
      elif [ -n "${CI:-}" ]; then
        git diff --name-only --diff-filter=ACM HEAD~1
      else
        git diff --cached --name-only --diff-filter=ACM
      fi
      ;;
    all)
      git ls-files
      ;;
  esac
}

if [ ${#FILES[@]} -gt 0 ]; then
  CHANGED_FILES=("${FILES[@]}")
else
  CHANGED_FILES=()
  while IFS= read -r file; do
    CHANGED_FILES+=("$file")
  done < <(get_files)
fi

if [ ${#CHANGED_FILES[@]} -eq 0 ]; then
  echo "No files to check"
  exit 0
fi

echo "Checking ${#CHANGED_FILES[@]} file(s) for binary artifacts..."

BINARY_PATTERN="ELF .* executable|ELF .* shared object|ELF .* relocatable|Mach-O .* executable|Mach-O universal binary|PE32\+ executable|PE32 executable|WebAssembly .* module|compiled Java class|\.NET assembly"

BINARY_FILES=()

for file in "${CHANGED_FILES[@]}"; do
  if [ ! -f "$file" ]; then
    continue
  fi

  file_type=$(file -b "$file" 2>/dev/null) || continue

  if echo "$file_type" | grep -qE "$BINARY_PATTERN"; then
    BINARY_FILES+=("$file")
  fi
done

if [ ${#BINARY_FILES[@]} -eq 0 ]; then
  echo "No binary artifacts found"
  exit 0
fi

echo "Found ${#BINARY_FILES[@]} binary artifact(s) that must not be committed:"
echo ""

for file in "${BINARY_FILES[@]}"; do
  file_type=$(file -b "$file" 2>/dev/null)
  echo "  $file"
  echo "    Type: $file_type"
  echo ""
done

echo "Binary artifacts (compiled executables, object files, shared libraries)"
echo "must not be checked into the repository. Remove them and add appropriate"
echo "patterns to .gitignore."
exit 1
