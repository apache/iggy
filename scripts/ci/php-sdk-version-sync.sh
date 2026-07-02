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
            echo "Validate PHP SDK Cargo, Rust SDK dependency, and Composer metadata"
            echo ""
            echo "Options:"
            echo "  --check    Check Cargo dependency and Composer version metadata"
            echo "  --fix      Sync the Rust SDK dependency and remove a manual Composer version field"
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

RUST_SDK_CARGO_TOML="core/sdk/Cargo.toml"
PHP_CARGO_TOML="foreign/php/Cargo.toml"
COMPOSER_JSON="foreign/php/composer.json"

PHP_CARGO_VERSION=$(grep '^version = ' "$PHP_CARGO_TOML" | head -1 | sed 's/version = "\(.*\)"/\1/')
RUST_SDK_VERSION=$(grep '^version = ' "$RUST_SDK_CARGO_TOML" | head -1 | sed 's/version = "\(.*\)"/\1/')
PHP_IGGY_DEP_LINE=$(grep '^iggy = ' "$PHP_CARGO_TOML" | head -1 || true)
PHP_IGGY_DEP_VERSION=""
if [[ "$PHP_IGGY_DEP_LINE" == *'version = "'* ]]; then
    PHP_IGGY_DEP_VERSION=$(printf '%s\n' "$PHP_IGGY_DEP_LINE" | sed 's/.*version = "\([^"]*\)".*/\1/')
fi
COMPOSER_VERSION_LINE=$(grep '"version"' "$COMPOSER_JSON" | head -1 || true)
COMPOSER_VERSION=""
if [ -n "$COMPOSER_VERSION_LINE" ]; then
    COMPOSER_VERSION=$(printf '%s\n' "$COMPOSER_VERSION_LINE" | sed 's/.*"version": *"\([^"]*\)".*/\1/')
fi

validate_composer_json() {
    if ! python3 -c 'import json, sys; json.load(open(sys.argv[1], encoding="utf-8"))' "$COMPOSER_JSON"; then
        echo -e "${RED}Error: Invalid JSON in $COMPOSER_JSON${NC}"
        exit 1
    fi
}

if [ -z "$PHP_CARGO_VERSION" ]; then
    echo -e "${RED}Error: Could not extract version from $PHP_CARGO_TOML${NC}"
    exit 1
fi

if [ -z "$RUST_SDK_VERSION" ]; then
    echo -e "${RED}Error: Could not extract version from $RUST_SDK_CARGO_TOML${NC}"
    exit 1
fi
validate_composer_json

echo "PHP SDK version check:"
echo "  PHP Cargo.toml:       $PHP_CARGO_VERSION"
echo "  core/sdk Cargo.toml:  $RUST_SDK_VERSION"
if [ -n "$PHP_IGGY_DEP_VERSION" ]; then
    echo "  PHP iggy dependency:  $PHP_IGGY_DEP_VERSION"
else
    echo "  PHP iggy dependency:  missing version"
fi
if [ -n "$COMPOSER_VERSION" ]; then
    echo "  composer.json:        $COMPOSER_VERSION (must be removed)"
else
    echo "  composer.json:        no manual version"
fi
echo ""

if [ "$MODE" = "check" ]; then
    if [ "$PHP_IGGY_DEP_VERSION" = "$RUST_SDK_VERSION" ] && [ -z "$COMPOSER_VERSION" ]; then
        echo -e "${GREEN}✓ PHP SDK metadata is synchronized${NC}"
        exit 0
    fi

    echo -e "${RED}✗ PHP SDK metadata is NOT synchronized${NC}"
    echo ""
    if [ "$PHP_IGGY_DEP_VERSION" != "$RUST_SDK_VERSION" ]; then
        echo "The PHP iggy dependency must match $RUST_SDK_CARGO_TOML."
    fi
    if [ -n "$COMPOSER_VERSION" ]; then
        echo "Packagist/Composer derive VCS package versions from tags."
        echo "Remove the version field from $COMPOSER_JSON."
    fi
    echo ""
    echo -e "${YELLOW}Run '$0 --fix' to sync PHP SDK metadata${NC}"
    exit 1
fi

changed=0

sedi() {
    if sed --version 2>/dev/null | grep -q 'GNU'; then
        sed -i "$@"
    else
        sed -i '' "$@"
    fi
}

if [ "$PHP_IGGY_DEP_VERSION" != "$RUST_SDK_VERSION" ]; then
    if grep -q '^iggy = .*version = ' "$PHP_CARGO_TOML"; then
        sedi "s/^\(iggy = .*version = \"\)[^\"]*/\1${RUST_SDK_VERSION}/" "$PHP_CARGO_TOML"
    else
        sedi "s/^\(iggy = {[^}]*\) }/\1, version = \"${RUST_SDK_VERSION}\" }/" "$PHP_CARGO_TOML"
    fi
    changed=1
fi

if [ -n "$COMPOSER_VERSION" ]; then
    sedi '/^[[:space:]]*"version"[[:space:]]*:/d' "$COMPOSER_JSON"
    changed=1
fi

if [ "$changed" -eq 0 ]; then
    echo -e "${GREEN}✓ PHP SDK metadata already synchronized${NC}"
    exit 0
fi

if grep -q '"version"' "$COMPOSER_JSON"; then
    echo -e "${RED}Error: Failed to remove version field from $COMPOSER_JSON${NC}"
    exit 1
fi
validate_composer_json

PHP_IGGY_DEP_LINE=$(grep '^iggy = ' "$PHP_CARGO_TOML" | head -1 || true)
PHP_IGGY_DEP_VERSION=""
if [[ "$PHP_IGGY_DEP_LINE" == *'version = "'* ]]; then
    PHP_IGGY_DEP_VERSION=$(printf '%s\n' "$PHP_IGGY_DEP_LINE" | sed 's/.*version = "\([^"]*\)".*/\1/')
fi
if [ "$PHP_IGGY_DEP_VERSION" != "$RUST_SDK_VERSION" ]; then
    echo -e "${RED}Error: Failed to sync PHP iggy dependency in $PHP_CARGO_TOML${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Synchronized PHP SDK metadata${NC}"
