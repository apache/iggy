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

# This script is used to prepare the release artifacts for the Apache Iggy project.

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

RELEASE_FILES=(
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

RELEASE_DIRS=(
  "bdd"
  "core"
  "examples"
  "foreign"
  "licenses"
  "scripts"
  "web"
)

IGNORED_FILES=(
  ".DS_Store"
  ".gitignore"
)

IGNORED_DIRS=(
  "target"
  "node_modules"
  "pkg"
  "build"
  "out"
  "dist"
  "bin"
  "obj"
  "__pycache__"
  ".elixir_ls"
  ".tox"
  ".eggs"
  ".venv"
  ".svelte-kit"
)

for file in "${RELEASE_FILES[@]}"; do
  cp "$SRC_DIR/$file" "$TEMP_DIR/"
done

shopt -s dotglob nullglob

for dir in "${RELEASE_DIRS[@]}"; do
  src="$SRC_DIR/$dir"
  dest="$TEMP_DIR/$dir"

  [ -d "$src" ] || continue
  mkdir -p "$dest"

  for item in "$src"/*; do
    name=$(basename "$item")

    if [[ "$dir" == "web" ]]; then
      if [ -d "$item" ]; then
        for ignored in "${IGNORED_DIRS[@]}"; do
          if [[ "$name" == "$ignored" ]]; then
            continue 2
          fi
        done
      fi

      if [ -f "$item" ]; then
        for ignored_file in "${IGNORED_FILES[@]}"; do
          if [[ "$name" == "$ignored_file" ]]; then
            continue 2
          fi
        done
      fi
    fi

    if [[ "$dir" == "foreign" && -d "$item" ]]; then
      sdk=$(basename "$item")
      mkdir -p "$dest/$sdk"

      for sdk_item in "$item"/*; do
        sdk_name=$(basename "$sdk_item")

        if [ -d "$sdk_item" ]; then
          for ignored in "${IGNORED_DIRS[@]}"; do
            if [[ "$sdk_name" == "$ignored" ]]; then
              continue 2
            fi
          done
        fi

        if [ -f "$sdk_item" ]; then
          for ignored_file in "${IGNORED_FILES[@]}"; do
            if [[ "$sdk_name" == "$ignored_file" ]]; then
              continue 2
            fi
          done
        fi

        cp -r "$sdk_item" "$dest/$sdk/"
      done

      continue
    fi

    cp -r "$item" "$dest/"
  done
done

shopt -u dotglob nullglob


VERSION=$(grep '^version' "$TEMP_DIR/core/server/Cargo.toml" | head -n 1 | cut -d '"' -f2)

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
