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

# This script creates source release archives for Apache Iggy.

set -euo pipefail

# Force consistent locale for reproducible file ordering
export LC_ALL=C

if [ "$(basename "$PWD")" == "scripts" ]; then
  cd ..
fi

# Ensure we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
  echo "Error: Must be run from within the git repository"
  exit 1
fi

RELEASE_DIR="iggy_release"
rm -rf "$RELEASE_DIR"
mkdir -p "$RELEASE_DIR"

# Get version from Cargo.toml
VERSION=$(grep '^version' "core/server/Cargo.toml" | head -n 1 | cut -d '"' -f2)

echo "Preparing release for version: $VERSION"

TAR_NAME="iggy-${VERSION}-incubating-src.tar"
ARCHIVE_NAME="iggy-${VERSION}-incubating-src.tar.gz"

# Files/directories to include in the release
RELEASE_PATHS=(
  "Cargo.lock"
  "Cargo.toml"
  "DEPENDENCIES.md"
  "DISCLAIMER"
  "Dockerfile"
  "LICENSE"
  "NOTICE"
  "docker-compose.yml"
  "justfile"
  "bdd"
  "core"
  "examples"
  "foreign"
  "helm"
  "scripts"
  "web"
)

# Create tar using git archive for consistent output
git archive \
  --format=tar \
  --prefix="iggy-${VERSION}-incubating-src/" \
  -o "$RELEASE_DIR/$TAR_NAME" \
  HEAD \
  "${RELEASE_PATHS[@]}"

# Compress - pipe through stdin to avoid embedding filename in gzip header
gzip -n -9 < "$RELEASE_DIR/$TAR_NAME" > "$RELEASE_DIR/$ARCHIVE_NAME"
rm -f "$RELEASE_DIR/$TAR_NAME"

# Generate checksum
cd "$RELEASE_DIR"
CHECKSUM_FILE="${ARCHIVE_NAME}.sha512"
sha512sum "$ARCHIVE_NAME" > "$CHECKSUM_FILE"

echo ""
echo "Release artifacts created in: $RELEASE_DIR/"
echo "  - $ARCHIVE_NAME"
echo "  - $CHECKSUM_FILE"
echo ""
echo "SHA-512 checksum:"
cat "$CHECKSUM_FILE"
