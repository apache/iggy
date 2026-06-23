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

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

MODE=""

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
            echo "Validate PHP SDK Cargo version and Composer metadata"
            echo ""
            echo "Options:"
            echo "  --check    Check Cargo version and Composer version metadata"
            echo "  --fix      Remove a manual Composer version field"
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

if [ -z "$MODE" ]; then
    echo -e "${RED}Error: Please specify either --check or --fix${NC}"
    echo "Use --help for usage information"
    exit 1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

CARGO_TOML="foreign/php/Cargo.toml"
COMPOSER_JSON="foreign/php/composer.json"

CARGO_VERSION=$(grep '^version = ' "$CARGO_TOML" | head -1 | sed 's/version = "\(.*\)"/\1/')
COMPOSER_VERSION_LINE=$(grep '"version"' "$COMPOSER_JSON" | head -1 || true)
COMPOSER_VERSION=""
if [ -n "$COMPOSER_VERSION_LINE" ]; then
    COMPOSER_VERSION=$(printf '%s\n' "$COMPOSER_VERSION_LINE" | sed 's/.*"version": *"\([^"]*\)".*/\1/')
fi

if [ -z "$CARGO_VERSION" ]; then
    echo -e "${RED}Error: Could not extract version from $CARGO_TOML${NC}"
    exit 1
fi

echo "PHP SDK version check:"
echo "  Cargo.toml:    $CARGO_VERSION"
if [ -n "$COMPOSER_VERSION" ]; then
    echo "  composer.json: $COMPOSER_VERSION (must be removed)"
else
    echo "  composer.json: no manual version"
fi
echo ""

if [ "$MODE" = "check" ]; then
    if [ -z "$COMPOSER_VERSION" ]; then
        echo -e "${GREEN}✓ PHP SDK Composer metadata lets tags drive package versions${NC}"
        exit 0
    fi

    echo -e "${RED}✗ composer.json must not declare a manual version${NC}"
    echo ""
    echo "Packagist/Composer derive VCS package versions from tags."
    echo "Remove the version field from $COMPOSER_JSON."
    echo ""
    echo -e "${YELLOW}Run '$0 --fix' to remove the Composer version field${NC}"
    exit 1
fi

if [ -z "$COMPOSER_VERSION" ]; then
    echo -e "${GREEN}✓ PHP SDK Composer metadata already omits a manual version${NC}"
    exit 0
fi

if sed --version 2>/dev/null | grep -q 'GNU'; then
    sed -i '/^[[:space:]]*"version"[[:space:]]*:/d' "$COMPOSER_JSON"
else
    sed -i '' '/^[[:space:]]*"version"[[:space:]]*:/d' "$COMPOSER_JSON"
fi

if grep -q '"version"' "$COMPOSER_JSON"; then
    echo -e "${RED}Error: Failed to remove version field from $COMPOSER_JSON${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Removed manual version field from $COMPOSER_JSON${NC}"
