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

# shellcheck source-path=SCRIPTDIR
source "$(dirname "${BASH_SOURCE[0]}")/lib/init.sh"

# file-length.sh -- Keep source files at or under MAX_LINES lines of code.
#
# The counting rule follows rustc tidy (src/tools/tidy/src/style.rs): a line
# counts unless it is empty or its trimmed text starts with `//`. The limit is
# stricter than the 3000 lines of tidy.
#
# Paths in ALLOWLIST are exempt. Every run also reports an entry whose file is
# gone or back within the limit, so dead entries do not pile up.
#
# Checks all tracked files, or only the files given.
#
# Exit codes: 0 = clean, 1 = violations found, other non-zero = error.

MAX_LINES=2500
CHECKED_FILES_REGEX='\.rs$'
# Fits `//` comment syntax only. A language with other comment syntax needs
# its own rule before it joins CHECKED_FILES_REGEX.
NON_CODE_LINE_REGEX='^$|^[[:space:]]*//'
ALLOWLIST=".github/config/file-length-allowlist.txt"

FILES=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check)
      shift
      ;;
    --help|-h)
      echo "Usage: $0 [--check] [files...]"
      echo ""
      echo "File selection:"
      echo "  [none]    Check all tracked files (default)"
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

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# Prints "<path>:<lines of code>" for each file, with one grep for all files.
count_code_lines() {
  local status=0
  grep -H -c -v -E "$NON_CODE_LINE_REGEX" -- "$@" || status=$?
  # Status 1 only means that no file has a line of code.
  if [ "$status" -gt 1 ]; then
    return "$status"
  fi
}

ISSUES=0

# Arguments: annotated file, line in that file (may be empty), message.
report() {
  local location="file=$1"
  if [ -n "$2" ]; then
    location+=",line=$2"
  fi
  if [ -n "${GITHUB_ACTIONS:-}" ]; then
    echo "::error $location,title=File length::$3"
  else
    echo "  • $3"
  fi
  ISSUES=$((ISSUES + 1))
}

declare -A ALLOWLIST_LINE=()
ALLOWLIST_PATHS=()
line_number=0
while IFS= read -r line; do
  line_number=$((line_number + 1))
  # An entry is the first word of a line, and `#` starts a comment.
  if [[ ! "$line" =~ ^[[:space:]]*([^#[:space:]]+) ]]; then
    continue
  fi
  entry="${BASH_REMATCH[1]}"
  ALLOWLIST_LINE["$entry"]=$line_number
  ALLOWLIST_PATHS+=("$entry")
done < "$ALLOWLIST"

if [ ${#FILES[@]} -eq 0 ]; then
  mapfile -t FILES < <(git ls-files)
fi

CANDIDATES=()
if [ ${#FILES[@]} -gt 0 ]; then
  for file in "${FILES[@]}"; do
    if [[ "$file" =~ $CHECKED_FILES_REGEX ]] && [ -f "$file" ] && [ -z "${ALLOWLIST_LINE[$file]+listed}" ]; then
      CANDIDATES+=("$file")
    fi
  done
fi

echo "Checking ${#CANDIDATES[@]} file(s) and ${#ALLOWLIST_PATHS[@]} allowlist entries against the limit of $MAX_LINES lines of code..."

TO_COUNT=()
if [ ${#CANDIDATES[@]} -gt 0 ]; then
  TO_COUNT+=("${CANDIDATES[@]}")
fi
if [ ${#ALLOWLIST_PATHS[@]} -gt 0 ]; then
  for entry in "${ALLOWLIST_PATHS[@]}"; do
    if [ -f "$entry" ]; then
      TO_COUNT+=("$entry")
    else
      report "$ALLOWLIST" "${ALLOWLIST_LINE[$entry]}" "$ALLOWLIST lists $entry, which does not exist. Remove the entry."
    fi
  done
fi

if [ ${#TO_COUNT[@]} -gt 0 ]; then
  COUNTS="$(count_code_lines "${TO_COUNT[@]}")"
  while IFS= read -r result; do
    path="${result%:*}"
    lines="${result##*:}"
    if [ -n "${ALLOWLIST_LINE[$path]+listed}" ]; then
      if [ "$lines" -le "$MAX_LINES" ]; then
        report "$ALLOWLIST" "${ALLOWLIST_LINE[$path]}" "$path has $lines lines of code, within the limit of $MAX_LINES. Remove it from $ALLOWLIST."
      fi
    elif [ "$lines" -gt "$MAX_LINES" ]; then
      report "$path" "" "$path has $lines lines of code, over the limit of $MAX_LINES. Split it, or add it to $ALLOWLIST if the split must wait."
    fi
  done <<< "$COUNTS"
fi

if [ "$ISSUES" -eq 0 ]; then
  echo "✅ All checked files are within $MAX_LINES lines of code and the allowlist is up to date"
  exit 0
fi

echo "❌ Found $ISSUES file length issue(s)"
exit 1
