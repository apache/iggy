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
            echo "Sync Rust version from rust-toolchain.toml to all Dockerfiles"
            echo ""
            echo "Options:"
            echo "  --check    Check if all Dockerfiles have the correct Rust version"
            echo "  --fix      Update all Dockerfiles to use the correct Rust version"
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

# Extract Rust version from rust-toolchain.toml
RUST_VERSION=$(grep 'channel' rust-toolchain.toml | sed 's/.*"\(.*\)".*/\1/')

if [ -z "$RUST_VERSION" ]; then
    echo -e "${RED}Error: Could not extract Rust version from rust-toolchain.toml${NC}"
    exit 1
fi

# Strip trailing ".0" -> e.g., 1.89.0 -> 1.89 (no change if it doesn't end in .0)
RUST_VERSION_SHORT=$(echo "$RUST_VERSION" | sed -E 's/^([0-9]+)\.([0-9]+)\.0$/\1.\2/')
RUST_IMAGE_VARIANT="slim-trixie"
RUST_IMAGE_PATTERN="FROM[[:space:]].*\\brust:"
RUST_IMAGE_TAG_PATTERN="(rust:[^-[:space:]]+)"

echo "Rust version from rust-toolchain.toml: ${GREEN}$RUST_VERSION${NC} (using ${GREEN}$RUST_VERSION_SHORT${NC} for Dockerfiles)"
echo ""

# Find all Dockerfiles
DOCKERFILES=$(find . -name "Dockerfile*" -type f | grep -v node_modules | grep -v target | sort)

# Track misaligned files
MISALIGNED_FILES=()
TOTAL_FILES=0
FIXED_FILES=0

for dockerfile in $DOCKERFILES; do
    SOURCE=""
    CURRENT_VERSION=""
    EXPECTED_VERSION=""
    HAS_RUST_IMAGE="false"
    RUST_IMAGE_MISMATCH="false"

    # Two ways a Dockerfile pins the toolchain:
    #   1. `ARG RUST_VERSION=1.96`  (+ `FROM rust:${RUST_VERSION}...`)
    #   2. hardcoded `FROM rust:1.96-slim` / `FROM rust:1.96.0-alpine`
    # Both must stay in sync; a Dockerfile that pins neither is skipped.
    if grep -q "^ARG RUST_VERSION=" "$dockerfile" 2>/dev/null; then
        SOURCE="arg"
        CURRENT_VERSION=$(grep "^ARG RUST_VERSION=" "$dockerfile" | head -1 | sed 's/^ARG RUST_VERSION=//')
        EXPECTED_VERSION="$RUST_VERSION_SHORT"
    elif grep -qE "FROM[[:space:]].*\brust:[0-9]" "$dockerfile" 2>/dev/null; then
        SOURCE="from"
        CURRENT_VERSION=$(grep -E "FROM[[:space:]].*\brust:[0-9]" "$dockerfile" | head -1 | sed -nE 's/.*\brust:([0-9]+\.[0-9]+(\.[0-9]+)?).*/\1/p')
        # Preserve the file's precision: full patch (1.96.0) or short (1.96).
        if [[ "$CURRENT_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
            EXPECTED_VERSION="$RUST_VERSION"
        else
            EXPECTED_VERSION="$RUST_VERSION_SHORT"
        fi
    fi

    if grep -qE "$RUST_IMAGE_PATTERN" "$dockerfile" 2>/dev/null; then
        HAS_RUST_IMAGE="true"
        if grep -E "$RUST_IMAGE_PATTERN" "$dockerfile" | grep -qvE -- "-$RUST_IMAGE_VARIANT([[:space:]]|$)"; then
            RUST_IMAGE_MISMATCH="true"
        fi
    fi

    if [ -z "$SOURCE" ] && [ "$HAS_RUST_IMAGE" = "false" ]; then
        continue
    fi

    TOTAL_FILES=$((TOTAL_FILES + 1))
    STATUS_DETAILS="$CURRENT_VERSION"
    if [ "$HAS_RUST_IMAGE" = "true" ]; then
        STATUS_DETAILS="${STATUS_DETAILS:+$STATUS_DETAILS, }$RUST_IMAGE_VARIANT"
    fi

    if [ "$MODE" = "check" ]; then
        if { [ -n "$SOURCE" ] && [ "$CURRENT_VERSION" != "$EXPECTED_VERSION" ]; } || [ "$RUST_IMAGE_MISMATCH" = "true" ]; then
            MISALIGNED_FILES+=("$dockerfile")
            MESSAGE=""
            if [ -n "$SOURCE" ] && [ "$CURRENT_VERSION" != "$EXPECTED_VERSION" ]; then
                MESSAGE="Rust version ${RED}$CURRENT_VERSION${NC} (expected: ${GREEN}$EXPECTED_VERSION${NC})"
            fi
            if [ "$RUST_IMAGE_MISMATCH" = "true" ]; then
                if [ -n "$MESSAGE" ]; then
                    MESSAGE="$MESSAGE; "
                fi
                MESSAGE="${MESSAGE}Rust base image must use ${GREEN}$RUST_IMAGE_VARIANT${NC}"
            fi
            echo -e "${RED}✗${NC} $dockerfile: $MESSAGE"
        else
            echo -e "${GREEN}✓${NC} $dockerfile${STATUS_DETAILS:+: $STATUS_DETAILS}"
        fi
    elif [ "$MODE" = "fix" ]; then
        if { [ -n "$SOURCE" ] && [ "$CURRENT_VERSION" != "$EXPECTED_VERSION" ]; } || [ "$RUST_IMAGE_MISMATCH" = "true" ]; then
            if [ -n "$SOURCE" ] && [ "$CURRENT_VERSION" != "$EXPECTED_VERSION" ] && [ "$SOURCE" = "arg" ]; then
                sed -i "s/^ARG RUST_VERSION=.*/ARG RUST_VERSION=$EXPECTED_VERSION/" "$dockerfile"
            elif [ -n "$SOURCE" ] && [ "$CURRENT_VERSION" != "$EXPECTED_VERSION" ]; then
                sed -i -E "/FROM[[:space:]].*\\brust:[0-9]/ s#(\\brust:)[0-9]+\\.[0-9]+(\\.[0-9]+)?#\\1$EXPECTED_VERSION#g" "$dockerfile"
            fi
            if [ "$RUST_IMAGE_MISMATCH" = "true" ]; then
                sed -i -E "/$RUST_IMAGE_PATTERN/ s#$RUST_IMAGE_TAG_PATTERN-[^[:space:]]+#\\1-$RUST_IMAGE_VARIANT#g" "$dockerfile"
                sed -i -E "/$RUST_IMAGE_PATTERN/ { /-$RUST_IMAGE_VARIANT([[:space:]]|$)/! s#$RUST_IMAGE_TAG_PATTERN([[:space:]]|$)#\\1-$RUST_IMAGE_VARIANT\\2#g; }" "$dockerfile"
            fi
            FIXED_FILES=$((FIXED_FILES + 1))
            MESSAGE=""
            if [ -n "$SOURCE" ] && [ "$CURRENT_VERSION" != "$EXPECTED_VERSION" ]; then
                MESSAGE="Rust version ${RED}$CURRENT_VERSION${NC} -> ${GREEN}$EXPECTED_VERSION${NC}"
            fi
            if [ "$RUST_IMAGE_MISMATCH" = "true" ]; then
                if [ -n "$MESSAGE" ]; then
                    MESSAGE="$MESSAGE; "
                fi
                MESSAGE="${MESSAGE}Rust base image -> ${GREEN}$RUST_IMAGE_VARIANT${NC}"
            fi
            echo -e "${GREEN}Fixed${NC} $dockerfile: $MESSAGE"
        else
            echo -e "${GREEN}✓${NC} $dockerfile${STATUS_DETAILS:+: already correct ($STATUS_DETAILS)}"
        fi
    fi
done

echo ""
echo "────────────────────────────────────────────────"

if [ "$MODE" = "check" ]; then
    if [ ${#MISALIGNED_FILES[@]} -eq 0 ]; then
        echo -e "${GREEN}✓ All $TOTAL_FILES Dockerfiles are aligned with Rust version $RUST_VERSION_SHORT${NC}"
        exit 0
    else
        echo -e "${RED}✗ Found ${#MISALIGNED_FILES[@]} misaligned Dockerfile(s) out of $TOTAL_FILES:${NC}"
        for file in "${MISALIGNED_FILES[@]}"; do
            echo -e "  ${RED}• $file${NC}"
        done
        echo ""
        echo -e "${YELLOW}Run '$0 --fix' to fix these files${NC}"
        exit 1
    fi
elif [ "$MODE" = "fix" ]; then
    if [ $FIXED_FILES -eq 0 ]; then
        echo -e "${GREEN}✓ All $TOTAL_FILES Dockerfiles were already aligned with Rust version $RUST_VERSION_SHORT${NC}"
    else
        echo -e "${GREEN}✓ Fixed $FIXED_FILES out of $TOTAL_FILES Dockerfiles to use Rust version $RUST_VERSION_SHORT${NC}"
    fi
    exit 0
fi
