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
            echo "Sync PHP SDK version between Cargo.toml and composer.json"
            echo ""
            echo "Options:"
            echo "  --check    Check if versions are synchronized"
            echo "  --fix      Update composer.json to match Cargo.toml"
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
COMPOSER_VERSION=$(perl -0777 -ne 'if (/"version"\s*:\s*"([^"]+)"/) { print $1; exit; }' "$COMPOSER_JSON")

if [ -z "$CARGO_VERSION" ]; then
    echo -e "${RED}Error: Could not extract version from $CARGO_TOML${NC}"
    exit 1
fi

if [ -z "$COMPOSER_VERSION" ]; then
    echo -e "${RED}Error: Could not extract version from $COMPOSER_JSON${NC}"
    exit 1
fi

echo "PHP SDK version check:"
echo "  Cargo.toml:    $CARGO_VERSION"
echo "  composer.json: $COMPOSER_VERSION"
echo ""

if [ "$MODE" = "check" ]; then
    if [ "$CARGO_VERSION" = "$COMPOSER_VERSION" ]; then
        echo -e "${GREEN}✓ PHP SDK versions are synchronized${NC}"
        exit 0
    fi

    echo -e "${RED}✗ PHP SDK versions are NOT synchronized${NC}"
    echo ""
    echo "Please ensure both files have the same version:"
    echo "  - $CARGO_TOML"
    echo "  - $COMPOSER_JSON"
    echo ""
    echo -e "${YELLOW}Run '$0 --fix' to update composer.json from Cargo.toml${NC}"
    exit 1
fi

if [ "$CARGO_VERSION" = "$COMPOSER_VERSION" ]; then
    echo -e "${GREEN}✓ PHP SDK versions are already synchronized${NC}"
    exit 0
fi

perl -0pi -e "s/\"version\"\\s*:\\s*\"\\Q$COMPOSER_VERSION\\E\"/\"version\": \"$CARGO_VERSION\"/" "$COMPOSER_JSON"
echo -e "${GREEN}✓ Updated $COMPOSER_JSON: $COMPOSER_VERSION -> $CARGO_VERSION${NC}"
