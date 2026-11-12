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

# Wheel jobs also run under the macOS system Bash 3.2.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source_file="$repo_root/foreign/python/pyproject.toml"

requires_python=$(sed -nE 's/^requires-python = "([^"]+)"$/\1/p' "$source_file" | head -1)
if [ -z "$requires_python" ]; then
    echo "Could not find requires-python in $source_file" >&2
    exit 1
fi

interpreter_versions=""
python_versions=""
minimum_version=""
minimum_number=-1
count=0

while IFS= read -r version; do
    interpreter_versions+=" python${version}"
    python_versions+="${version}"$'\n'
    count=$((count + 1))

    major=${version%%.*}
    minor=${version#*.}
    number=$((major * 100 + minor))
    if [ "$minimum_number" -lt 0 ] || [ "$number" -lt "$minimum_number" ]; then
        minimum_number=$number
        minimum_version=$version
    fi
done < <(sed -nE 's/^    "Programming Language :: Python :: ([0-9]+\.[0-9]+)",$/\1/p' "$source_file")

if [ "$count" -eq 0 ]; then
    echo "No Python version classifiers found in $source_file" >&2
    exit 1
fi

printf 'versions=%s\n' "${interpreter_versions# }"
printf 'python_versions<<PYTHON_VERSIONS\n%sPYTHON_VERSIONS\n' "$python_versions"
printf 'count=%s\n' "$count"
printf 'minimum_version=%s\n' "$minimum_version"
printf 'requires_python=%s\n' "$requires_python"
