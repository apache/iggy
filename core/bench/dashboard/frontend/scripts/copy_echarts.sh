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

# Exit on any error
set -e

# Get the directory where the script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Go up one level to frontend directory
FRONTEND_DIR="$(dirname "$SCRIPT_DIR")"

# The index page and the server embed page load ECharts from /vendor instead of a CDN.
# The file comes from the charming crate, so Cargo.lock pins its content.
ECHARTS_FILE="echarts-5.5.1.min.js"

if [ -z "$TRUNK_STAGING_DIR" ]; then
    echo "Error: TRUNK_STAGING_DIR environment variable must be set, run this script through trunk"
    exit 1
fi

# cargo metadata also downloads the charming sources when they're missing.
METADATA="$(cargo metadata --format-version 1 --manifest-path "$FRONTEND_DIR/Cargo.toml")"
CHARMING_MANIFEST="$(grep -o '"manifest_path":"[^"]*/charming-[0-9][^"/]*/Cargo.toml"' <<<"$METADATA" || true)"
case "$CHARMING_MANIFEST" in
"" | *$'\n'*)
    echo "Error: expected exactly one charming package in cargo metadata, found: '$CHARMING_MANIFEST'"
    exit 1
    ;;
esac
CHARMING_MANIFEST="${CHARMING_MANIFEST#\"manifest_path\":\"}"
CHARMING_DIR="$(dirname "${CHARMING_MANIFEST%\"}")"

ECHARTS_SOURCE="$CHARMING_DIR/src/asset/$ECHARTS_FILE"
if [ ! -f "$ECHARTS_SOURCE" ]; then
    echo "Error: $ECHARTS_SOURCE not found. If charming ships another ECharts version, update" \
        "ECHARTS_FILE here and the /vendor paths in index.html and server/src/handlers.rs"
    exit 1
fi

mkdir -p "$TRUNK_STAGING_DIR/vendor"
cp "$ECHARTS_SOURCE" "$TRUNK_STAGING_DIR/vendor/$ECHARTS_FILE"
echo "Copied $ECHARTS_SOURCE to $TRUNK_STAGING_DIR/vendor"
