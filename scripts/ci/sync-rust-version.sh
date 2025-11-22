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

echo "Rust version from rust-toolchain.toml: ${GREEN}$RUST_VERSION${NC} (using ${GREEN}$RUST_VERSION_SHORT${NC} for Dockerfiles)"
echo ""

# Find all Dockerfiles
DOCKERFILES=$(find . -name "Dockerfile*" -type f | grep -v node_modules | grep -v target | sort)

# Track misaligned files
MISALIGNED_FILES=()
TOTAL_FILES=0
FIXED_FILES=0

for dockerfile in $DOCKERFILES; do
    # Skip files without ARG RUST_VERSION
    if ! grep -q "^ARG RUST_VERSION=" "$dockerfile" 2>/dev/null; then
        continue
    fi

    TOTAL_FILES=$((TOTAL_FILES + 1))

    # Get current version in the Dockerfile
    CURRENT_VERSION=$(grep "^ARG RUST_VERSION=" "$dockerfile" | head -1 | sed 's/^ARG RUST_VERSION=//')

    if [ "$MODE" = "check" ]; then
        if [ "$CURRENT_VERSION" != "$RUST_VERSION_SHORT" ]; then
            MISALIGNED_FILES+=("$dockerfile")
            echo -e "${RED}✗${NC} $dockerfile: ${RED}$CURRENT_VERSION${NC} (expected: ${GREEN}$RUST_VERSION_SHORT${NC})"
        else
            echo -e "${GREEN}✓${NC} $dockerfile: $CURRENT_VERSION"
        fi
    elif [ "$MODE" = "fix" ]; then
        if [ "$CURRENT_VERSION" != "$RUST_VERSION_SHORT" ]; then
            # Update the ARG RUST_VERSION line
            sed -i "s/^ARG RUST_VERSION=.*/ARG RUST_VERSION=$RUST_VERSION_SHORT/" "$dockerfile"
            FIXED_FILES=$((FIXED_FILES + 1))
            echo -e "${GREEN}Fixed${NC} $dockerfile: ${RED}$CURRENT_VERSION${NC} -> ${GREEN}$RUST_VERSION_SHORT${NC}"
        else
            echo -e "${GREEN}✓${NC} $dockerfile: already correct ($RUST_VERSION_SHORT)"
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