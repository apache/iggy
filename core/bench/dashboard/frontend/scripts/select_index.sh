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

# Exit on any error
set -e

# Get the directory where the script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Go up one level to frontend directory
FRONTEND_DIR="$(dirname "$SCRIPT_DIR")"

if [ "$TRUNK_PROFILE" = "debug" ]; then
    echo "Debug build detected, using index.dev.html"
    cp "$FRONTEND_DIR/index.dev.html" "$FRONTEND_DIR/index.html"
elif [ "$TRUNK_PROFILE" = "release" ]; then
    echo "Release build detected, using index.prod.html"
    cp "$FRONTEND_DIR/index.prod.html" "$FRONTEND_DIR/index.html"
else
    echo "Error: TRUNK_PROFILE environment variable must be set to 'debug' or 'release'"
    exit 1
fi
