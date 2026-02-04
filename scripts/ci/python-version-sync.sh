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
YELLOW='\033[1;33m'
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
            echo "Sync Python SDK version between Cargo.toml and pyproject.toml"
            echo ""
            echo "Options:"
            echo "  --check    Check if versions are synchronized"
            echo "  --fix      Update the older version to match the newer one"
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

CARGO_TOML="foreign/python/Cargo.toml"
PYPROJECT_TOML="foreign/python/pyproject.toml"

# Extract version from Cargo.toml
CARGO_VERSION=$(grep '^version = ' "$CARGO_TOML" | head -1 | sed 's/version = "\(.*\)"/\1/')

# Extract version from pyproject.toml
PYPROJECT_VERSION=$(grep '^version = ' "$PYPROJECT_TOML" | head -1 | sed 's/version = "\(.*\)"/\1/')

if [ -z "$CARGO_VERSION" ]; then
    echo -e "${RED}Error: Could not extract version from $CARGO_TOML${NC}"
    exit 1
fi

if [ -z "$PYPROJECT_VERSION" ]; then
    echo -e "${RED}Error: Could not extract version from $PYPROJECT_TOML${NC}"
    exit 1
fi

# Normalize versions for comparison (both to PEP 440 format with .dev)
normalize_version() {
    local v="$1"
    echo "${v//-dev/.dev}"
}

# Convert PEP 440 format to Cargo format
to_cargo_format() {
    local v="$1"
    echo "${v//.dev/-dev}"
}

# Compare two versions, returns:
#   0 if equal
#   1 if first is greater
#   2 if second is greater
compare_versions() {
    local v1="$1"
    local v2="$2"

    # Normalize both versions
    v1=$(normalize_version "$v1")
    v2=$(normalize_version "$v2")

    if [ "$v1" = "$v2" ]; then
        echo 0
        return
    fi

    # Extract base version and dev number
    local base1 dev1 base2 dev2
    if [[ "$v1" =~ ^([0-9]+\.[0-9]+\.[0-9]+)(\.dev([0-9]+))?$ ]]; then
        base1="${BASH_REMATCH[1]}"
        dev1="${BASH_REMATCH[3]:-0}"
    else
        base1="$v1"
        dev1="0"
    fi

    if [[ "$v2" =~ ^([0-9]+\.[0-9]+\.[0-9]+)(\.dev([0-9]+))?$ ]]; then
        base2="${BASH_REMATCH[1]}"
        dev2="${BASH_REMATCH[3]:-0}"
    else
        base2="$v2"
        dev2="0"
    fi

    # Compare base versions using sort -V
    local sorted
    sorted=$(printf '%s\n%s' "$base1" "$base2" | sort -V | head -1)

    if [ "$base1" != "$base2" ]; then
        if [ "$sorted" = "$base1" ]; then
            echo 2  # v2 is greater
        else
            echo 1  # v1 is greater
        fi
        return
    fi

    # Base versions are equal, compare dev numbers
    if [ "$dev1" -gt "$dev2" ]; then
        echo 1
    elif [ "$dev1" -lt "$dev2" ]; then
        echo 2
    else
        echo 0
    fi
}

CARGO_NORMALIZED=$(normalize_version "$CARGO_VERSION")
PYPROJECT_NORMALIZED=$(normalize_version "$PYPROJECT_VERSION")

echo "Python SDK version check:"
echo "  Cargo.toml:     $CARGO_VERSION (normalized: $CARGO_NORMALIZED)"
echo "  pyproject.toml: $PYPROJECT_VERSION"
echo ""

if [ "$MODE" = "check" ]; then
    if [ "$CARGO_NORMALIZED" = "$PYPROJECT_NORMALIZED" ]; then
        echo -e "${GREEN}✓ Python SDK versions are synchronized${NC}"
        exit 0
    else
        echo -e "${RED}✗ Python SDK versions are NOT synchronized${NC}"
        echo ""
        echo "Please ensure both files have the same version:"
        echo "  - $CARGO_TOML: use format like '0.6.4-dev1'"
        echo "  - $PYPROJECT_TOML: use format like '0.6.4.dev1'"
        echo ""
        echo -e "${YELLOW}Run '$0 --fix' to fix this automatically${NC}"
        exit 1
    fi
elif [ "$MODE" = "fix" ]; then
    if [ "$CARGO_NORMALIZED" = "$PYPROJECT_NORMALIZED" ]; then
        echo -e "${GREEN}✓ Python SDK versions are already synchronized${NC}"
        exit 0
    fi

    COMPARISON=$(compare_versions "$CARGO_VERSION" "$PYPROJECT_VERSION")

    if [ "$COMPARISON" = "1" ]; then
        # Cargo version is newer, update pyproject.toml
        NEW_PYPROJECT_VERSION="$CARGO_NORMALIZED"
        echo -e "${YELLOW}Cargo.toml has newer version, updating pyproject.toml...${NC}"
        sed -i "s/^version = \"$PYPROJECT_VERSION\"/version = \"$NEW_PYPROJECT_VERSION\"/" "$PYPROJECT_TOML"
        echo -e "${GREEN}✓ Updated $PYPROJECT_TOML: $PYPROJECT_VERSION -> $NEW_PYPROJECT_VERSION${NC}"
    elif [ "$COMPARISON" = "2" ]; then
        # pyproject version is newer, update Cargo.toml
        NEW_CARGO_VERSION=$(to_cargo_format "$PYPROJECT_NORMALIZED")
        echo -e "${YELLOW}pyproject.toml has newer version, updating Cargo.toml...${NC}"
        sed -i "s/^version = \"$CARGO_VERSION\"/version = \"$NEW_CARGO_VERSION\"/" "$CARGO_TOML"
        echo -e "${GREEN}✓ Updated $CARGO_TOML: $CARGO_VERSION -> $NEW_CARGO_VERSION${NC}"
    fi

    exit 0
fi
