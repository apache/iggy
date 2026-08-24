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
    scripts/ci/connector-plugins.sh --package-flags > connector-packages.txt && \
    scripts/ci/connector-plugins.sh --manifests > connector-manifests.txt

#
# Shared build toolchain. Everything below it that both image flavors need:
# the cross toolchain, cargo-zigbuild, cargo-about, and the chef recipe.
#
FROM --platform=$BUILDPLATFORM chef AS toolchain
ARG IGGY_CI_BUILD
ENV IGGY_CI_BUILD=${IGGY_CI_BUILD}

# The base image tag and rust-toolchain.toml agree only while the pinned
# channel is an x.y.z with z > 0: scripts/ci/sync-rustc-version.sh rewrites an
# x.y.0 channel to the floating x.y tag, which then resolves to whatever patch
# the base image publishes. Bring the pin in before rustup runs so the cross
# targets land on the toolchain cargo selects once the sources arrive, whichever
# of the two it turns out to be.
COPY rust-toolchain.toml rust-toolchain.toml

RUN apk add --no-cache bash curl tar zig make autoconf automake libtool pkgconfig hwloc-dev xz-dev xz-static && \
    cargo install cargo-zigbuild --version '=0.22.1' --locked && \
    rustup target add \
    x86_64-unknown-linux-musl \
    aarch64-unknown-linux-musl \
    x86_64-unknown-linux-gnu \
    aarch64-unknown-linux-gnu

# cargo-about backs scripts/ci/third-party-licenses.sh, which both flavors run
# to generate the license manifest they ship.
RUN TARGET="$(uname -m)-unknown-linux-musl" && \
    curl -sSfL "https://github.com/EmbarkStudios/cargo-about/releases/download/0.9.0/cargo-about-0.9.0-${TARGET}.tar.gz" \
      | tar -xz -C /usr/local/bin --strip-components=1 "cargo-about-0.9.0-${TARGET}/cargo-about"

# Allow pkg-config to work in cross-compilation mode (needed for hwlocality-sys)
ENV PKG_CONFIG_ALLOW_CROSS=1

COPY --from=planner /app/recipe.json recipe.json

#
# Slim builder - runtime binary and its license manifest, nothing else.
#
# This is a sibling of fat-builder, not its ancestor: a stage resolves to its
# final filesystem, so anything the fat flavor needs that lived here would be
# built by a `--target runtime` build too, and then discarded.
#
FROM --platform=$BUILDPLATFORM toolchain AS builder
ARG PROFILE=release
ARG TARGETPLATFORM
ARG LIBC=glibc

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
    if [ "$PROFILE" = "debug" ]; then \
    cargo chef cook --recipe-path recipe.json --target ${RUST_TARGET} --zigbuild -p iggy-connectors; \
    else \
    cargo chef cook --recipe-path recipe.json --target ${RUST_TARGET} --zigbuild --release -p iggy-connectors; \
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
    if [ "$PROFILE" = "debug" ]; then PROFILE_DIR=debug; PROFILE_FLAG=""; else PROFILE_DIR=release; PROFILE_FLAG="--release"; fi && \
    cargo zigbuild --locked --target ${RUST_TARGET} -p iggy-connectors $PROFILE_FLAG && \
    cp /app/target/${RUST_TARGET}/${PROFILE_DIR}/iggy-connectors /app/iggy-connectors

#
# Generate the third-party license manifest. Required by ASF release policy:
# convenience binaries that statically link third-party code MUST include the
# full license text of every bundled crate inside the artifact. This one covers
# the runtime only, which is all the slim image ships.
#
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry-${TARGETPLATFORM}-${LIBC} \
    --mount=type=cache,target=/usr/local/cargo/git,id=cargo-git-${TARGETPLATFORM}-${LIBC} \
    ./scripts/ci/third-party-licenses.sh --generate --manifest core/connectors/runtime/Cargo.toml --output /app/LICENSE-binary

#
# Fat builder - runtime binary plus every connector plugin cdylib, and a
# license manifest covering all of them.
#
FROM --platform=$BUILDPLATFORM toolchain AS fat-builder
ARG PROFILE=release
ARG TARGETPLATFORM
ARG LIBC=glibc

# Only this branch reads the derived plugin set, so adding a connector leaves
# the slim branch's cooked dependency layer untouched. Two lists, because cargo
# selects a plugin by package name while the artifact is lib<target name>.so.
COPY --from=planner /app/connector-plugins.txt connector-plugins.txt
COPY --from=planner /app/connector-packages.txt connector-packages.txt
COPY --from=planner /app/connector-manifests.txt connector-manifests.txt

#
# Cook dependencies
#
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry-${TARGETPLATFORM}-${LIBC} \
    --mount=type=cache,target=/usr/local/cargo/git,id=cargo-git-${TARGETPLATFORM}-${LIBC} \
    --mount=type=cache,target=/app/target,id=cargo-target-fat-${TARGETPLATFORM}-${LIBC} \
    case "$TARGETPLATFORM:$LIBC" in \
    "linux/amd64:musl") RUST_TARGET="x86_64-unknown-linux-musl" ;; \
    "linux/arm64:musl") RUST_TARGET="aarch64-unknown-linux-musl" ;; \
    "linux/amd64:glibc") RUST_TARGET="x86_64-unknown-linux-gnu" ;; \
    "linux/arm64:glibc") RUST_TARGET="aarch64-unknown-linux-gnu" ;; \
    *) echo "Unsupported platform/libc combination: $TARGETPLATFORM/$LIBC" && exit 1 ;; \
    esac && \
    PLUGIN_FLAGS="$(cat connector-packages.txt)" && \
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
    --mount=type=cache,target=/app/target,id=cargo-target-fat-${TARGETPLATFORM}-${LIBC} \
    case "$TARGETPLATFORM:$LIBC" in \
    "linux/amd64:musl") RUST_TARGET="x86_64-unknown-linux-musl" ;; \
    "linux/arm64:musl") RUST_TARGET="aarch64-unknown-linux-musl" ;; \
    "linux/amd64:glibc") RUST_TARGET="x86_64-unknown-linux-gnu" ;; \
    "linux/arm64:glibc") RUST_TARGET="aarch64-unknown-linux-gnu" ;; \
    *) echo "Unsupported platform/libc combination: $TARGETPLATFORM/$LIBC" && exit 1 ;; \
    esac && \
    PLUGIN_FLAGS="$(cat connector-packages.txt)" && \
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
# Generate the third-party license manifest covering the runtime plus every
# bundled plugin closure. Same ASF policy as the slim manifest above; each
# image ships the manifest that matches what it actually bundles, so the slim
# image never over-declares plugin dependencies it does not carry.
#
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry-${TARGETPLATFORM}-${LIBC} \
    --mount=type=cache,target=/usr/local/cargo/git,id=cargo-git-${TARGETPLATFORM}-${LIBC} \
    MANIFEST_FLAGS="$(sed 's/^/--manifest /' connector-manifests.txt | tr '\n' ' ')" && \
    ./scripts/ci/third-party-licenses.sh --generate --manifest core/connectors/runtime/Cargo.toml $MANIFEST_FLAGS --output /app/LICENSE-binary-fat

#
# Runtime base - Debian trixie Slim, shared by both published flavors.
#
FROM debian:trixie-slim AS runtime-base
WORKDIR /app

COPY LICENSE NOTICE /usr/share/doc/iggy-connect/

ENTRYPOINT ["iggy-connectors"]

#
# Slim runtime - runtime binary only, no plugins. Published as the `-slim`
# image flavor. Built explicitly with `--target runtime`.
#
FROM runtime-base AS runtime

COPY --from=builder /app/iggy-connectors /usr/local/bin/iggy-connectors
COPY --from=builder /app/LICENSE-binary /usr/share/doc/iggy-connect/LICENSE-binary

#
# Fat runtime - runtime plus every connector plugin baked into /usr/local/lib,
# which is already on the runtime plugin search path, so a config can load a
# plugin by name with no path. This is the default build target (last stage)
# and the default published image flavor.
#
FROM runtime-base AS runtime-fat

COPY --from=fat-builder /app/iggy-connectors /usr/local/bin/iggy-connectors
COPY --from=fat-builder /app/plugins/ /usr/local/lib/
COPY --from=fat-builder /app/LICENSE-binary-fat /usr/share/doc/iggy-connect/LICENSE-binary
