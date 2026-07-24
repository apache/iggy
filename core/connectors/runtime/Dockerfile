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

ARG RUST_VERSION=1.97.1
ARG ALPINE_VERSION=3.23

FROM --platform=$BUILDPLATFORM lukemathwalker/cargo-chef:latest-rust-${RUST_VERSION}-alpine${ALPINE_VERSION} AS chef
WORKDIR /app
# bash + jq back scripts/ci/connector-plugins.sh, which the planner runs to
# derive the connector plugin set (see below).
RUN apk add --no-cache musl-dev pkgconfig bash jq

FROM --platform=$BUILDPLATFORM chef AS planner
COPY . .
# Prepare the chef recipe and, from the same source snapshot, derive the
# connector plugin set (crate names + manifest paths) so the cook, build and
# license steps below stay in sync with the workspace without a hardcoded list.
RUN cargo chef prepare --recipe-path recipe.json && \
    scripts/ci/connector-plugins.sh --names > connector-plugins.txt && \
    scripts/ci/connector-plugins.sh --manifests > connector-manifests.txt

FROM --platform=$BUILDPLATFORM chef AS builder
ARG PROFILE=release
ARG TARGETPLATFORM
ARG LIBC=glibc
ARG IGGY_CI_BUILD
ENV IGGY_CI_BUILD=${IGGY_CI_BUILD}

RUN apk add --no-cache bash curl tar zig make autoconf automake libtool pkgconfig hwloc-dev xz-dev xz-static && \
    cargo install cargo-zigbuild --version '=0.22.1' --locked && \
    rustup target add \
    x86_64-unknown-linux-musl \
    aarch64-unknown-linux-musl \
    x86_64-unknown-linux-gnu \
    aarch64-unknown-linux-gnu

# Allow pkg-config to work in cross-compilation mode (needed for hwlocality-sys)
ENV PKG_CONFIG_ALLOW_CROSS=1

COPY --from=planner /app/recipe.json recipe.json
COPY --from=planner /app/connector-plugins.txt connector-plugins.txt
COPY --from=planner /app/connector-manifests.txt connector-manifests.txt

#
# Cook dependencies
#
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry-${TARGETPLATFORM}-${LIBC} \
    --mount=type=cache,target=/usr/local/cargo/git,id=cargo-git-${TARGETPLATFORM}-${LIBC} \
    --mount=type=cache,target=/app/target,id=cargo-target-${TARGETPLATFORM}-${LIBC} \
    case "$TARGETPLATFORM:$LIBC" in \
    "linux/amd64:musl") RUST_TARGET="x86_64-unknown-linux-musl" ;; \
    "linux/arm64:musl") RUST_TARGET="aarch64-unknown-linux-musl" ;; \
    "linux/amd64:glibc") RUST_TARGET="x86_64-unknown-linux-gnu" ;; \
    "linux/arm64:glibc") RUST_TARGET="aarch64-unknown-linux-gnu" ;; \
    *) echo "Unsupported platform/libc combination: $TARGETPLATFORM/$LIBC" && exit 1 ;; \
    esac && \
    PLUGIN_FLAGS="$(sed 's/^/-p /' connector-plugins.txt | tr '\n' ' ')" && \
    if [ "$PROFILE" = "debug" ]; then \
    cargo chef cook --recipe-path recipe.json --target ${RUST_TARGET} --zigbuild \
      -p iggy-connectors $PLUGIN_FLAGS; \
    else \
    cargo chef cook --recipe-path recipe.json --target ${RUST_TARGET} --zigbuild --release \
      -p iggy-connectors $PLUGIN_FLAGS; \
    fi

COPY . .

#
# Build
#
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry-${TARGETPLATFORM}-${LIBC} \
    --mount=type=cache,target=/usr/local/cargo/git,id=cargo-git-${TARGETPLATFORM}-${LIBC} \
    --mount=type=cache,target=/app/target,id=cargo-target-${TARGETPLATFORM}-${LIBC} \
    case "$TARGETPLATFORM:$LIBC" in \
    "linux/amd64:musl") RUST_TARGET="x86_64-unknown-linux-musl" ;; \
    "linux/arm64:musl") RUST_TARGET="aarch64-unknown-linux-musl" ;; \
    "linux/amd64:glibc") RUST_TARGET="x86_64-unknown-linux-gnu" ;; \
    "linux/arm64:glibc") RUST_TARGET="aarch64-unknown-linux-gnu" ;; \
    *) echo "Unsupported platform/libc combination: $TARGETPLATFORM/$LIBC" && exit 1 ;; \
    esac && \
    PLUGIN_FLAGS="$(sed 's/^/-p /' connector-plugins.txt | tr '\n' ' ')" && \
    if [ "$PROFILE" = "debug" ]; then PROFILE_DIR=debug; PROFILE_FLAG=""; else PROFILE_DIR=release; PROFILE_FLAG="--release"; fi && \
    cargo zigbuild --locked --target ${RUST_TARGET} -p iggy-connectors $PLUGIN_FLAGS $PROFILE_FLAG && \
    cp /app/target/${RUST_TARGET}/${PROFILE_DIR}/iggy-connectors /app/iggy-connectors && \
    mkdir -p /app/plugins && \
    while IFS= read -r name; do \
      [ -z "$name" ] && continue; \
      so_file="/app/target/${RUST_TARGET}/${PROFILE_DIR}/lib${name}.so"; \
      if [ ! -f "$so_file" ]; then echo "expected plugin artifact missing: $so_file" >&2 && exit 1; fi; \
      cp "$so_file" /app/plugins/; \
    done < connector-plugins.txt

#
# Generate third-party license manifests. Required by ASF release policy:
# convenience binaries that statically link third-party code MUST include
# the full license text of every bundled crate inside the artifact.
#
# Two manifests: LICENSE-binary covers the runtime only (slim image);
# LICENSE-binary-fat additionally covers every bundled plugin closure (fat
# image). Each image ships the manifest that matches what it actually bundles.
#
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry-${TARGETPLATFORM}-${LIBC} \
    --mount=type=cache,target=/usr/local/cargo/git,id=cargo-git-${TARGETPLATFORM}-${LIBC} \
    TARGET="$(uname -m)-unknown-linux-musl" && \
    curl -sSfL "https://github.com/EmbarkStudios/cargo-about/releases/download/0.9.0/cargo-about-0.9.0-${TARGET}.tar.gz" \
      | tar -xz -C /usr/local/bin --strip-components=1 "cargo-about-0.9.0-${TARGET}/cargo-about" && \
    ./scripts/ci/third-party-licenses.sh --generate --manifest core/connectors/runtime/Cargo.toml --output /app/LICENSE-binary && \
    MANIFEST_FLAGS="$(sed 's/^/--manifest /' connector-manifests.txt | tr '\n' ' ')" && \
    ./scripts/ci/third-party-licenses.sh --generate --manifest core/connectors/runtime/Cargo.toml $MANIFEST_FLAGS --output /app/LICENSE-binary-fat

#
# Slim runtime - Debian trixie Slim. Runtime binary only, no plugins.
# Published as the `-slim` image flavor. Built explicitly with
# `--target runtime`.
#
FROM debian:trixie-slim AS runtime
WORKDIR /app

COPY --from=builder /app/iggy-connectors /usr/local/bin/iggy-connectors
COPY --from=builder /app/LICENSE-binary /usr/share/doc/iggy-connect/LICENSE-binary
COPY LICENSE NOTICE /usr/share/doc/iggy-connect/

ENTRYPOINT ["iggy-connectors"]

#
# Fat runtime - slim plus every connector plugin baked into /usr/local/lib,
# which is already on the runtime plugin search path, so a config can load a
# plugin by name with no path. This is the default build target (last stage)
# and the default published image flavor.
#
FROM runtime AS runtime-fat
COPY --from=builder /app/plugins/ /usr/local/lib/
COPY --from=builder /app/LICENSE-binary-fat /usr/share/doc/iggy-connect/LICENSE-binary
