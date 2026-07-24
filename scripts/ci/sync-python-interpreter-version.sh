#!/bin/bash
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

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Default mode
MODE=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --check)
            MODE="check"
            shift
            ;;
        --fix)
            MODE="fix"
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [--check|--fix]"
            echo ""
            echo "Sync the Python interpreter version across SDK files, CI, and Dockerfiles"
            echo ""
            echo "Options:"
            echo "  --check    Check if Python interpreter versions are synchronized"
            echo "  --fix      Update files to use the Python interpreter version from foreign/python/pyproject.toml"
            echo "  --help     Show this help message"
            exit 0
            ;;
        *)
            echo -e "${RED}Error: Unknown option $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Require mode to be specified
if [ -z "$MODE" ]; then
    echo -e "${RED}Error: Please specify either --check or --fix${NC}"
    echo "Use --help for usage information"
    exit 1
fi

# Get the repository root (two levels up from scripts/ci/)
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

SOURCE_FILE="foreign/python/pyproject.toml"
PYTHON_VERSION=$(sed -nE 's/^requires-python = ">=([0-9]+\.[0-9]+)".*/\1/p' "$SOURCE_FILE" | head -1)

if [ -z "$PYTHON_VERSION" ]; then
    echo -e "${RED}Error: Could not extract Python interpreter version from $SOURCE_FILE${NC}"
    exit 1
fi

PYTHON_VERSION_REGEX=${PYTHON_VERSION//./\\.}
PYTHON_VERSION_MAJOR=${PYTHON_VERSION%%.*}
PYTHON_VERSION_MINOR=${PYTHON_VERSION#*.}
PYTHON_VERSION_NUMBER=$((PYTHON_VERSION_MAJOR * 100 + PYTHON_VERSION_MINOR))
FAILED=0
TOTAL_CHECKS=0
FIXED_CHECKS=0

echo -e "Python interpreter version from $SOURCE_FILE: ${GREEN}$PYTHON_VERSION${NC}"
echo ""

ensure_line() {
    local file="$1"
    local current_pattern="$2"
    local expected_pattern="$3"
    local replacement="$4"
    local description="$5"
    local mismatched=0
    local total=0

    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))

    if [ ! -f "$file" ]; then
        echo -e "${RED}✗${NC} $file: file does not exist"
        FAILED=1
        return
    fi

    while IFS= read -r line; do
        total=$((total + 1))
        if [[ ! "$line" =~ $expected_pattern ]]; then
            mismatched=1
        fi
    done < <(grep -E "$current_pattern" "$file" || true)

    if [ "$total" -eq 0 ]; then
        echo -e "${RED}✗${NC} $file: could not find $description"
        FAILED=1
        return
    fi

    if [ "$mismatched" -eq 0 ]; then
        echo -e "${GREEN}✓${NC} $file: $description"
        return
    fi

    if [ "$MODE" = "fix" ]; then
        sed -i.bak -E "s|$current_pattern|$replacement|" "$file"
        rm -f "$file.bak"
        FIXED_CHECKS=$((FIXED_CHECKS + 1))
        echo -e "${GREEN}Fixed${NC} $file: $description"
    else
        echo -e "${RED}✗${NC} $file: $description is not $PYTHON_VERSION"
        FAILED=1
    fi
}

ensure_classifiers() {
    local file="$1"
    local versions=()
    local version
    local version_major
    local version_minor
    local version_number
    local has_minimum=0
    local mismatched=0

    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))

    if [ ! -f "$file" ]; then
        echo -e "${RED}✗${NC} $file: file does not exist"
        FAILED=1
        return
    fi

    versions=()
    while IFS= read -r _py_ver_tmp; do versions+=("$_py_ver_tmp"); done < <(sed -nE 's/^    "Programming Language :: Python :: ([0-9]+\.[0-9]+)",$/\1/p' "$file")

    if [ "${#versions[@]}" -eq 0 ]; then
        echo -e "${RED}✗${NC} $file: could not find Python version classifiers"
        FAILED=1
        return
    fi

    for version in "${versions[@]}"; do
        if [ "$version" = "$PYTHON_VERSION" ]; then
            has_minimum=1
        fi

        version_major=${version%%.*}
        version_minor=${version#*.}
        version_number=$((version_major * 100 + version_minor))

        if [ "$version_number" -lt "$PYTHON_VERSION_NUMBER" ]; then
            mismatched=1
        fi
    done

    if [ "$has_minimum" -eq 0 ]; then
        mismatched=1
    fi

    if [ "$mismatched" -eq 0 ]; then
        echo -e "${GREEN}✓${NC} $file: Python version classifiers"
        return
    fi

    if [ "$MODE" = "fix" ]; then
        for version in "${versions[@]}"; do
            version_major=${version%%.*}
            version_minor=${version#*.}
            version_number=$((version_major * 100 + version_minor))

            if [ "$version_number" -lt "$PYTHON_VERSION_NUMBER" ]; then
                sed -i.bak -E "/^    \"Programming Language :: Python :: ${version//./\\.}\",$/d" "$file"
                rm -f "$file.bak"
            fi
        done

        if [ "$has_minimum" -eq 0 ]; then
            awk -v major="$PYTHON_VERSION_MAJOR" -v version="$PYTHON_VERSION" '
              { print }
              $0 == "    \"Programming Language :: Python :: " major "\"," {
                printf "    \"Programming Language :: Python :: %s\",\n", version
              }
            ' "$file" > "$file.new" && mv "$file.new" "$file"
        fi

        FIXED_CHECKS=$((FIXED_CHECKS + 1))
        echo -e "${GREEN}Fixed${NC} $file: Python version classifiers"
    else
        echo -e "${RED}✗${NC} $file: Python version classifiers are not aligned with $PYTHON_VERSION"
        FAILED=1
    fi
}

ensure_lock_python_requirement() {
    local file="$1"
    local current_pattern="^(requires-python = \">=)[0-9]+\\.[0-9]+(\".*)$"
    local expected_pattern="^requires-python = \">=${PYTHON_VERSION_REGEX}\".*$"
    local replacement="\\1${PYTHON_VERSION}\\2"
    local first_table_line
    local top_level_end

    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))

    if [ ! -f "$file" ]; then
        echo -e "${RED}✗${NC} $file: file does not exist"
        FAILED=1
        return
    fi

    first_table_line=$(grep -n -m1 '^\[' "$file" | cut -d: -f1 || true)
    if [ -n "$first_table_line" ]; then
        top_level_end=$((first_table_line - 1))
    else
        top_level_end=$(wc -l < "$file")
    fi

    if [ "$top_level_end" -lt 1 ]; then
        echo -e "${RED}✗${NC} $file: could not find lock file Python requirement"
        FAILED=1
        return
    fi

    if sed -n "1,${top_level_end}p" "$file" | grep -Eq "$expected_pattern"; then
        echo -e "${GREEN}✓${NC} $file: lock file Python requirement"
        return
    fi

    if ! sed -n "1,${top_level_end}p" "$file" | grep -Eq "$current_pattern"; then
        echo -e "${RED}✗${NC} $file: could not find lock file Python requirement"
        FAILED=1
        return
    fi

    if [ "$MODE" = "fix" ]; then
        sed -i.bak -E "1,${top_level_end}s|$current_pattern|$replacement|" "$file"
        rm -f "$file.bak"
        FIXED_CHECKS=$((FIXED_CHECKS + 1))
        echo -e "${GREEN}Fixed${NC} $file: lock file Python requirement"
    else
        echo -e "${RED}✗${NC} $file: lock file Python requirement is not $PYTHON_VERSION"
        FAILED=1
    fi
}

ensure_wheel_interpreters() {
    local file="$1"
    local classifier_versions=()
    local expected_interpreters=""
    local current_interpreters
    local version
    local mismatched=0
    local total=0

    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))

    if [ ! -f "$file" ]; then
        echo -e "${RED}✗${NC} $file: file does not exist"
        FAILED=1
        return
    fi

    classifier_versions=()
    while IFS= read -r _py_cls_tmp; do classifier_versions+=("$_py_cls_tmp"); done < <(sed -nE 's/^    "Programming Language :: Python :: ([0-9]+\.[0-9]+)",$/\1/p' "$SOURCE_FILE")

    if [ "${#classifier_versions[@]}" -eq 0 ]; then
        echo -e "${RED}✗${NC} $SOURCE_FILE: could not find Python version classifiers"
        FAILED=1
        return
    fi

    for version in "${classifier_versions[@]}"; do
        expected_interpreters+=" python${version}"
    done
    expected_interpreters=${expected_interpreters# }

    while IFS= read -r line; do
        total=$((total + 1))
        current_interpreters=$(echo "$line" | sed -E 's/^.*--interpreter[[:space:]]+//')
        if [ "$current_interpreters" != "$expected_interpreters" ]; then
            mismatched=1
        fi
    done < <(grep -- "--interpreter" "$file" || true)

    if [ "$total" -eq 0 ]; then
        echo -e "${RED}✗${NC} $file: could not find wheel interpreter list"
        FAILED=1
        return
    fi

    if [ "$mismatched" -eq 0 ]; then
        echo -e "${GREEN}✓${NC} $file: wheel interpreter versions"
        return
    fi

    if [ "$MODE" = "fix" ]; then
        sed -i.bak -E "s|(--interpreter ).*$|\\1${expected_interpreters}|" "$file"
        rm -f "$file.bak"
        FIXED_CHECKS=$((FIXED_CHECKS + 1))
        echo -e "${GREEN}Fixed${NC} $file: wheel interpreter versions"
    else
        echo -e "${RED}✗${NC} $file: wheel interpreter versions are not $expected_interpreters"
        FAILED=1
    fi
}

ensure_classifiers "$SOURCE_FILE"

PYTHON_VERSION_FILES=(
    "foreign/python/.python-version"
    "bdd/python/.python-version"
    "examples/python/.python-version"
)

for python_version_file in "${PYTHON_VERSION_FILES[@]}"; do
    ensure_line \
        "$python_version_file" \
        "^[0-9]+\\.[0-9]+(\\.[0-9]+)?$" \
        "^${PYTHON_VERSION_REGEX}$" \
        "$PYTHON_VERSION" \
        "pyenv Python version"
done

ensure_line \
    "foreign/python/Dockerfile.test" \
    "^(FROM python:)[0-9]+\\.[0-9]+(-slim-trixie AS base)$" \
    "^FROM python:${PYTHON_VERSION_REGEX}-slim-trixie AS base$" \
    "\\1${PYTHON_VERSION}\\2" \
    "Docker image Python version"

ensure_line \
    "foreign/python/README.md" \
    "^(- Python )[0-9]+\\.[0-9]+(\\+.*)$" \
    "^- Python ${PYTHON_VERSION_REGEX}\\+.*$" \
    "\\1${PYTHON_VERSION}\\2" \
    "README Python requirement"

ensure_line \
    "bdd/python/pyproject.toml" \
    "^(requires-python = \">=)[0-9]+\\.[0-9]+(\".*)$" \
    "^requires-python = \">=${PYTHON_VERSION_REGEX}\".*$" \
    "\\1${PYTHON_VERSION}\\2" \
    "BDD Python requirement"

ensure_line \
    "examples/python/pyproject.toml" \
    "^(requires-python = \">=)[0-9]+\\.[0-9]+(\".*)$" \
    "^requires-python = \">=${PYTHON_VERSION_REGEX}\".*$" \
    "\\1${PYTHON_VERSION}\\2" \
    "examples Python requirement"

ensure_line \
    ".github/actions/python-maturin/pre-merge/action.yml" \
    "^([[:space:]]*python-version: \")[0-9]+\\.[0-9]+(\".*)$" \
    "^[[:space:]]*python-version: \"${PYTHON_VERSION_REGEX}\".*$" \
    "\\1${PYTHON_VERSION}\\2" \
    "maturin action Python version"

ensure_line \
    ".github/workflows/_test_examples.yml" \
    "^([[:space:]]*python-version: \")[0-9]+\\.[0-9]+(\".*)$" \
    "^[[:space:]]*python-version: \"${PYTHON_VERSION_REGEX}\".*$" \
    "\\1${PYTHON_VERSION}\\2" \
    "examples workflow Python version"

ensure_line \
    ".github/workflows/coverage-baseline.yml" \
    "^([[:space:]]*python-version: \")[0-9]+\\.[0-9]+(\".*)$" \
    "^[[:space:]]*python-version: \"${PYTHON_VERSION_REGEX}\".*$" \
    "\\1${PYTHON_VERSION}\\2" \
    "coverage workflow Python version"

ensure_line \
    ".github/workflows/_build_python_wheels.yml" \
    "^([[:space:]]*python-version: \")[0-9]+\\.[0-9]+(\".*)$" \
    "^[[:space:]]*python-version: \"${PYTHON_VERSION_REGEX}\".*$" \
    "\\1${PYTHON_VERSION}\\2" \
    "wheel workflow setup-python versions"

ensure_wheel_interpreters ".github/workflows/_build_python_wheels.yml"

PYLOCK_FILES=(
    "foreign/python/pylock.toml"
    "bdd/python/pylock.toml"
    "examples/python/pylock.toml"
)

for pylock_file in "${PYLOCK_FILES[@]}"; do
    if [ -f "$pylock_file" ]; then
        ensure_lock_python_requirement "$pylock_file"
    fi
done

echo ""
echo "────────────────────────────────────────────────"

if [ "$MODE" = "check" ]; then
    if [ "$FAILED" -eq 0 ]; then
        echo -e "${GREEN}✓ All $TOTAL_CHECKS Python interpreter version checks are synchronized with $PYTHON_VERSION${NC}"
        exit 0
    fi

    echo -e "${RED}✗ Python interpreter version checks failed${NC}"
    echo -e "${YELLOW}Run '$0 --fix' to fix supported drift points automatically${NC}"
    exit 1
fi

if [ "$FIXED_CHECKS" -eq 0 ]; then
    echo -e "${GREEN}✓ All $TOTAL_CHECKS Python interpreter version checks were already synchronized with $PYTHON_VERSION${NC}"
else
    echo -e "${GREEN}✓ Fixed $FIXED_CHECKS Python interpreter version check(s) to use $PYTHON_VERSION${NC}"
fi
