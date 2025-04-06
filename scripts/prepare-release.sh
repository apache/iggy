#!/bin/bash

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

# This script is used to generate Cross.toml file for user which executes
# this script. This is needed since Cross.toml build.dockerfile.build-args
# section requires statically defined Docker build arguments and parameters
# like current UID or GID must be entered (cannot be generated or fetched
# during cross execution time).

set -euo pipefail

# Determine tar command based on OS
TAR_CMD="tar"
OS_NAME=$(uname)

if [[ "$OS_NAME" == "Darwin" ]]; then
  if command -v gtar >/dev/null 2>&1; then
    TAR_CMD="gtar"
  else
    echo "❌ GNU tar (gtar) is required on macOS. Install it with: brew install gnu-tar"
    exit 1
  fi
fi

if [ "$(basename "$PWD")" == "scripts" ]; then
  cd ..
fi

SRC_DIR="./"
RELEASE_DIR="iggy_release"
TEMP_DIR="iggy_release_tmp"

rm -rf "$TEMP_DIR"
rm -rf "$RELEASE_DIR"
mkdir -p "$TEMP_DIR"
mkdir -p "$RELEASE_DIR"

FILES=(
  "Cargo.lock"
  "Cargo.toml"
  "DEPENDENCIES.md"
  "DISCLAIMER"
  "Dockerfile"
  "LICENSE"
  "NOTICE"
  "docker-compose.yml"
  "justfile"
)

DIRS=(
  "bench"
  "certs"
  "cli"
  "configs"
  "examples"
  "integration"
  "licenses"
  "scripts"
  "sdk"
  "server"
  "tools"
)

for file in "${FILES[@]}"; do
  cp "$SRC_DIR/$file" "$TEMP_DIR/"
done

for dir in "${DIRS[@]}"; do
  cp -r "$SRC_DIR/$dir" "$TEMP_DIR/"
done

VERSION=$(grep '^version' "$TEMP_DIR/server/Cargo.toml" | head -n 1 | cut -d '"' -f2)

echo "Preparing release for version: $VERSION"

ARCHIVE_NAME="iggy-${VERSION}-incubating-src.tar.gz"

GZIP=-n "$TAR_CMD" --sort=name \
             --mtime='UTC 2020-01-01' \
             --owner=0 --group=0 --numeric-owner \
             -czf "$ARCHIVE_NAME" -C "$TEMP_DIR" .

CHECKSUM_FILE="${ARCHIVE_NAME}.sha512"
sha512sum "$ARCHIVE_NAME" > "$CHECKSUM_FILE"

rm -rf "$TEMP_DIR"

echo "Release directory: $RELEASE_DIR"
echo "SHA-512 checksum:"
cat "$CHECKSUM_FILE"
echo "✔ Archive created: $ARCHIVE_NAME"
echo "✔ Checksum saved to: $CHECKSUM_FILE"

mv "$ARCHIVE_NAME" "$RELEASE_DIR/"
mv "$CHECKSUM_FILE" "$RELEASE_DIR/"


