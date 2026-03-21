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
            echo "Verify uv.lock files are up to date with their pyproject.toml"
            echo ""
            echo "Options:"
            echo "  --check    Check if lock files are up to date"
            echo "  --fix      Regenerate stale lock files"
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

PYTHON_DIRS=(
    "foreign/python"
    "bdd/python"
    "examples/python"
)

FAILED=0

for dir in "${PYTHON_DIRS[@]}"; do
    if [ ! -f "$dir/uv.lock" ]; then
        continue
    fi

    if (cd "$dir" && uv lock --check 2>/dev/null); then
        echo -e "${GREEN}$dir/uv.lock is up to date${NC}"
    else
        if [ "$MODE" = "check" ]; then
            echo -e "${RED}$dir/uv.lock is out of date${NC}"
            FAILED=1
        else
            echo -e "${YELLOW}$dir/uv.lock is out of date, regenerating...${NC}"
            (cd "$dir" && uv lock 2>/dev/null)
            echo -e "${GREEN}$dir/uv.lock updated${NC}"
        fi
    fi
done

if [ "$FAILED" -ne 0 ]; then
    echo ""
    echo -e "${RED}Some uv.lock files are out of date${NC}"
    echo -e "${YELLOW}Run '$0 --fix' to fix this automatically${NC}"
    exit 1
fi
