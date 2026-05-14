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

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

if ! command -v uv >/dev/null 2>&1; then
    echo -e "${RED}Error: uv is required but not installed${NC}"
    echo -e "${YELLOW}Install with: curl -LsSf https://astral.sh/uv/install.sh | sh${NC}"
    echo -e "${YELLOW}Or use: brew install uv${NC}"
    exit 127
fi

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

    echo -e "${GREEN}uv audit: $dir${NC}"
    if ! (cd "$dir" && uv audit --frozen --preview-features audit); then
        FAILED=1
    fi
done

if [ "$FAILED" -ne 0 ]; then
    echo ""
    echo -e "${RED}uv audit reported vulnerabilities (see output above)${NC}"
    exit 1
fi
