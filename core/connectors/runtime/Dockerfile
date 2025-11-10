# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

ARG RUST_VERSION=1.91
ARG ALPINE_VERSION=3.22

FROM --platform=$BUILDPLATFORM lukemathwalker/cargo-chef:latest-rust-${RUST_VERSION}-alpine AS chef
WORKDIR /app
RUN apk add --no-cache musl-dev pkgconfig

FROM --platform=$BUILDPLATFORM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM --platform=$BUILDPLATFORM chef AS builder
ARG PROFILE=release
ARG TARGETPLATFORM
ARG LIBC=musl
ARG IGGY_CI_BUILD
ENV IGGY_CI_BUILD=${IGGY_CI_BUILD}

RUN apk add --no-cache zig && \
    cargo install cargo-zigbuild --locked && \
    rustup target add \
    x86_64-unknown-linux-musl \
    aarch64-unknown-linux-musl \
    x86_64-unknown-linux-gnu \
    aarch64-unknown-linux-gnu

COPY --from=planner /app/recipe.json recipe.json

#
# Cook dependencies
#
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry-${TARGETPLATFORM}-${LIBC} \
    --mount=type=cache,target=/usr/local/cargo/git,id=cargo-git-${TARGETPLATFORM}-${LIBC} \
    case "$TARGETPLATFORM:$LIBC" in \
    "linux/amd64:musl") RUST_TARGET="x86_64-unknown-linux-musl" ;; \
    "linux/arm64:musl") RUST_TARGET="aarch64-unknown-linux-musl" ;; \
    "linux/amd64:glibc") RUST_TARGET="x86_64-unknown-linux-gnu" ;; \
    "linux/arm64:glibc") RUST_TARGET="aarch64-unknown-linux-gnu" ;; \
    *) echo "Unsupported platform/libc combination: $TARGETPLATFORM/$LIBC" && exit 1 ;; \
    esac && \
    if [ "$PROFILE" = "debug" ]; then \
    cargo chef cook --recipe-path recipe.json --target ${RUST_TARGET} --zigbuild; \
    else \
    cargo chef cook --recipe-path recipe.json --target ${RUST_TARGET} --zigbuild --release; \
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
    if [ "$PROFILE" = "debug" ]; then \
    cargo zigbuild --target ${RUST_TARGET} --bin iggy-connectors && \
    cp /app/target/${RUST_TARGET}/debug/iggy-connectors /app/iggy-connectors; \
    else \
    cargo zigbuild --target ${RUST_TARGET} --bin iggy-connectors --release && \
    cp /app/target/${RUST_TARGET}/release/iggy-connectors /app/iggy-connectors; \
    fi

#
# Final runtime - Debian trixie Slim
#
FROM debian:trixie-slim AS runtime
WORKDIR /app

COPY --from=builder /app/iggy-connectors /usr/local/bin/iggy-connectors

ENTRYPOINT ["iggy-connectors"]
