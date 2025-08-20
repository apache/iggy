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


# Extract Rust version from rust-toolchain.toml
RUST_VERSION=$(grep 'channel' rust-toolchain.toml | sed 's/.*"\(.*\)".*/\1/')

if [ -z "$RUST_VERSION" ]; then
    echo "Error: Could not extract Rust version from rust-toolchain.toml"
    exit 1
fi

# Strip trailing ".0" -> e.g., 1.89.0 -> 1.89 (no change if it doesn't end in .0)
RUST_TAG=$(echo "$RUST_VERSION" | sed -E 's/^([0-9]+)\.([0-9]+)\.0$/\1.\2/')

echo "Syncing Rust version $RUST_VERSION (using tag: $RUST_TAG) to Dockerfiles..."

# Update regular rust image (no suffix)
# Matches things like: FROM rust:1.88, FROM rust:1.88.1, etc.
sed -Ei "s|(FROM[[:space:]]+rust:)[0-9]+(\.[0-9]+){1,2}|\1$RUST_TAG|g" bdd/rust/Dockerfile

# Update slim-bookworm image
sed -Ei "s|(FROM[[:space:]]+rust:)[0-9]+(\.[0-9]+){1,2}-slim-bookworm|\1$RUST_TAG-slim-bookworm|g" core/bench/dashboard/server/Dockerfile

echo "Updated Dockerfiles to use:"
echo " - Regular image: rust:$RUST_TAG"
echo " - Slim bookworm: rust:$RUST_TAG-slim-bookworm"
