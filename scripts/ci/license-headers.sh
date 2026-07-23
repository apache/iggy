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
      shift
      ;;
    --fix)
      MODE="fix"
      shift
      ;;
    *)
      echo "Usage: $0 [--check|--fix] [file...]"
      echo "  --check  Check files for Apache license headers (default)"
      echo "  --fix    Add Apache license headers to files missing them"
      echo "  file...  Scope the duplicate-header scan to these paths"
      exit 1
      ;;
  esac
fi

# Remaining args are explicit paths that scope the (slow) duplicate-header
# scan. pre-commit passes the staged files here; with none (CI) the scan
# covers the whole tree. HawkEye itself always scans the whole repo: it is
# fast (~tens of ms with [git] ignore = "auto") and the authoritative check.
FILES=("$@")

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

if ! command -v jq &> /dev/null; then
  echo "❌ jq command not found"
  echo "💡 Install jq: https://jqlang.github.io/jq/download/"
  exit 1
fi

HAWKEYE_VERSION="$(cat .github/config/hawkeye.version)"

# Check if HawkEye is available
if ! command -v hawkeye &> /dev/null; then
  echo "❌ hawkeye command not found"
  echo "💡 Install HawkEye: cargo install hawkeye --version $HAWKEYE_VERSION --locked"
  exit 1
fi

INSTALLED_HAWKEYE_VERSION="$(hawkeye -V | awk '{print $2}')"
if [ "$INSTALLED_HAWKEYE_VERSION" != "$HAWKEYE_VERSION" ]; then
  echo "❌ hawkeye $HAWKEYE_VERSION is required, found ${INSTALLED_HAWKEYE_VERSION:-unknown}"
  echo "💡 Install HawkEye: cargo install hawkeye --version $HAWKEYE_VERSION --locked --force"
  exit 1
fi

run_hawkeye() {
  hawkeye "$@"
}

load_license_excludes() {
  awk '
    /^[[:space:]]*excludes[[:space:]]*=/ {
      in_excludes = 1
      next
    }
    in_excludes && /^[[:space:]]*\]/ {
      exit
    }
    in_excludes {
      sub(/[[:space:]]*#.*/, "")
      gsub(/^[[:space:]]*"/, "")
      gsub(/",[[:space:]]*$/, "")
      if (length($0) > 0) {
        print
      }
    }
  ' licenserc.toml
}

is_license_excluded() {
  local path="$1"
  local pattern
  local root_pattern

  for pattern in "${LICENSE_EXCLUDES[@]}"; do
    # Match the gitignore-style glob patterns from licenserc.toml intentionally.
    # shellcheck disable=SC2254
    case "$path" in
      $pattern)
        return 0
        ;;
    esac

    if [[ "${pattern:0:3}" == "**/" ]]; then
      root_pattern="${pattern#**/}"
      # Match the root-level form of **/ globs intentionally.
      # shellcheck disable=SC2254
      case "$path" in
        $root_pattern)
          return 0
          ;;
      esac
    fi
  done

  return 1
}

extract_hawkeye_paths() {
  local key="$1"
  local file="$2"

  jq -r --arg key "$key" '.[$key] // [] | .[]' "$file" \
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

find_duplicate_license_headers() {
  local output_file="$1"
  local path

  : > "$output_file"
  LICENSE_EXCLUDES=()
  while IFS= read -r _hdr_tmp; do LICENSE_EXCLUDES+=("$_hdr_tmp"); done < <(load_license_excludes)

  while IFS= read -r -d '' path; do
    if is_license_excluded "$path"; then
      continue
    fi

    # short-circuit the symlink that may point to dir.
    if [ ! -f "$path" ] || ! LC_ALL=C grep -Iq . "$path"; then
      continue
    fi

    if awk '
      function count_keyword() {
        if (index($0, "Licensed to the Apache Software Foundation")) {
          count++
        }
      }

      NR > 120 { exit }
      /^[[:space:]]*$/ { next }
      NR == 1 && /^#!/ { next }
      NR == 1 && /^<\?(php|xml)/ { next }

      in_xml_comment {
        count_keyword()
        if ($0 ~ /-->/) {
          in_xml_comment = 0
        }
        next
      }

      in_slashstar_comment {
        count_keyword()
        if ($0 ~ /\*\//) {
          in_slashstar_comment = 0
        }
        next
      }

      /^[[:space:]]*<!--/ {
        in_xml_comment = ($0 !~ /-->/)
        count_keyword()
        next
      }

      /^[[:space:]]*\/\*/ {
        in_slashstar_comment = ($0 !~ /\*\//)
        count_keyword()
        next
      }

      /^[[:space:]]*(#|\/\/|\*|-->)/ {
        count_keyword()
        next
      }

      { exit }

      END { exit count > 1 ? 0 : 1 }
    ' "$path"; then
      printf './%s\n' "$path" >> "$output_file"
    fi
  done < <(
    if [ "${#FILES[@]}" -gt 0 ]; then
      printf '%s\0' "${FILES[@]}"
    else
      git ls-files -z
    fi
  )

  sort -o "$output_file" "$output_file"
}

if [ "$MODE" = "fix" ]; then
  echo "🔧 Adding license headers to files..."
  TEMP_FILE=$(mktemp)
  MISSING_FILE=$(mktemp)
  UNKNOWN_FILE=$(mktemp)
  DUPLICATE_FILE=$(mktemp)
  LOG_FILE=$(mktemp)
  trap 'rm -f "$TEMP_FILE" "$MISSING_FILE" "$UNKNOWN_FILE" "$DUPLICATE_FILE" "$LOG_FILE"' EXIT

  run_hawkeye check --config licenserc.toml --fail-if-unknown -o "$TEMP_FILE" > "$LOG_FILE" 2>&1 || true
  extract_hawkeye_paths missing "$TEMP_FILE" > "$MISSING_FILE"
  extract_hawkeye_paths unknown "$TEMP_FILE" > "$UNKNOWN_FILE"

  if [ -s "$UNKNOWN_FILE" ]; then
    echo "❌ Cannot add license headers to unknown file types:"
    echo ""
    append_hawkeye_section "Unknown file types" "$UNKNOWN_FILE"
    exit 1
  fi

  run_hawkeye format --config licenserc.toml --fail-if-updated false --fail-if-unknown
  find_duplicate_license_headers "$DUPLICATE_FILE"

  if [ -s "$MISSING_FILE" ]; then
    append_hawkeye_section "Updated license headers" "$MISSING_FILE"
  fi

  if [ -s "$DUPLICATE_FILE" ]; then
    echo "❌ Found duplicate license headers after fixing:"
    echo ""
    append_hawkeye_section "Duplicate license headers" "$DUPLICATE_FILE"
    echo "💡 Remove duplicate license headers and keep a single header using the comment style configured for the file in licenserc.toml"
    exit 1
  fi

  if [ ! -s "$MISSING_FILE" ]; then
    echo "✅ No license header changes needed"
  else
    echo "✅ License header fix completed"
  fi
else
  echo "🔍 Checking license headers..."

  TEMP_FILE=$(mktemp)
  REPORT_FILE=$(mktemp)
  DUPLICATE_FILE=$(mktemp)
  LOG_FILE=$(mktemp)
  trap 'rm -f "$TEMP_FILE" "$REPORT_FILE" "$DUPLICATE_FILE" "$LOG_FILE"' EXIT

  HAWKEYE_STATUS=0
  run_hawkeye check --config licenserc.toml --fail-if-unknown -o "$TEMP_FILE" > "$LOG_FILE" 2>&1 || HAWKEYE_STATUS=$?
  find_duplicate_license_headers "$DUPLICATE_FILE"

  if [ "$HAWKEYE_STATUS" -eq 0 ] && [ ! -s "$DUPLICATE_FILE" ]; then
    echo "✅ All files have proper license headers"
  else
    write_hawkeye_report "$TEMP_FILE" "$REPORT_FILE"

    echo "❌ Found files with missing or inconsistent license headers:"
    echo ""
    if [ -s "$REPORT_FILE" ]; then
      sed 's/^/  /' < "$REPORT_FILE"
    fi
    if [ -s "$DUPLICATE_FILE" ]; then
      append_hawkeye_section "Duplicate license headers" "$DUPLICATE_FILE" | sed 's/^/  /'
      echo "  💡 Remove duplicate license headers and keep a single header using the comment style configured for the file in licenserc.toml"
    fi
    if [ ! -s "$REPORT_FILE" ] && [ ! -s "$DUPLICATE_FILE" ]; then
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
        fi
        if [ -s "$DUPLICATE_FILE" ]; then
          append_hawkeye_section "Duplicate license headers" "$DUPLICATE_FILE"
          echo "Remove duplicate license headers and keep a single header using the comment style configured for the file in licenserc.toml"
        fi
        if [ ! -s "$REPORT_FILE" ] && [ ! -s "$DUPLICATE_FILE" ]; then
          cat "$LOG_FILE"
        fi
        echo '```'
        echo "Please run \`./scripts/ci/license-headers.sh --fix\` to fix automatically."
      } >> "$GITHUB_STEP_SUMMARY"
    fi

    exit 1
  fi
fi
