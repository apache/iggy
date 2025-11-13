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

ARG RUST_VERSION=1.91
ARG ALPINE_VERSION=3.22

# ── from-source path (unchanged) ──────────────────────────────────────────────
FROM --platform=$BUILDPLATFORM lukemathwalker/cargo-chef:latest-rust-${RUST_VERSION}-alpine AS chef
WORKDIR /app
#RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static
RUN apk add musl-dev

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

RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry-${TARGETPLATFORM}-${LIBC} \
    --mount=type=cache,target=/usr/local/cargo/git,id=cargo-git-${TARGETPLATFORM}-${LIBC} \
    case "$TARGETPLATFORM:$LIBC" in \
    "linux/amd64:musl")  RUST_TARGET="x86_64-unknown-linux-musl" ;; \
    "linux/arm64:musl")  RUST_TARGET="aarch64-unknown-linux-musl" ;; \
    "linux/amd64:glibc") RUST_TARGET="x86_64-unknown-linux-gnu"  ;; \
    "linux/arm64:glibc") RUST_TARGET="aarch64-unknown-linux-gnu" ;; \
    *) echo "Unsupported $TARGETPLATFORM/$LIBC" && exit 1 ;; \
    esac && \
    if [ "$PROFILE" = "debug" ]; then \
    cargo chef cook --recipe-path recipe.json --target ${RUST_TARGET} --zigbuild; \
    else \
    cargo chef cook --recipe-path recipe.json --target ${RUST_TARGET} --zigbuild --release; \
    fi

COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry-${TARGETPLATFORM}-${LIBC} \
    --mount=type=cache,target=/usr/local/cargo/git,id=cargo-git-${TARGETPLATFORM}-${LIBC} \
    --mount=type=cache,target=/app/target,id=cargo-target-${TARGETPLATFORM}-${LIBC} \
    case "$TARGETPLATFORM:$LIBC" in \
    "linux/amd64:musl")  RUST_TARGET="x86_64-unknown-linux-musl" ;; \
    "linux/arm64:musl")  RUST_TARGET="aarch64-unknown-linux-musl" ;; \
    "linux/amd64:glibc") RUST_TARGET="x86_64-unknown-linux-gnu"  ;; \
    "linux/arm64:glibc") RUST_TARGET="aarch64-unknown-linux-gnu" ;; \
    *) echo "Unsupported $TARGETPLATFORM/$LIBC" && exit 1 ;; \
    esac && \
    if [ "$PROFILE" = "debug" ]; then \
    cargo zigbuild --target ${RUST_TARGET} --bin iggy-server --bin iggy && \
    cp /app/target/${RUST_TARGET}/debug/iggy-server /app/iggy-server && \
    cp /app/target/${RUST_TARGET}/debug/iggy /app/iggy; \
    else \
    cargo zigbuild --target ${RUST_TARGET} --bin iggy-server --bin iggy --release && \
    cp /app/target/${RUST_TARGET}/release/iggy-server /app/iggy-server && \
    cp /app/target/${RUST_TARGET}/release/iggy /app/iggy; \
    fi

# ── prebuilt path (FAST) ──────────────────────────────────────────────────────
FROM debian:trixie-slim AS prebuilt
WORKDIR /out
ARG PREBUILT_IGGY_SERVER
ARG PREBUILT_IGGY_CLI
COPY ${PREBUILT_IGGY_SERVER} /out/iggy-server
COPY ${PREBUILT_IGGY_CLI}    /out/iggy
RUN chmod +x /out/iggy-server /out/iggy

# ── final images ──────────────────────────────────────────────────────────────
FROM debian:trixie-slim AS runtime-prebuilt
ARG TARGETPLATFORM
ARG PREBUILT_IGGY_SERVER
ARG PREBUILT_IGGY_CLI
WORKDIR /app
COPY --from=prebuilt /out/iggy-server /usr/local/bin/iggy-server
COPY --from=prebuilt /out/iggy        /usr/local/bin/iggy
RUN echo "═══════════════════════════════════════════════════════════════" && \
    echo "                    IGGY SERVER BUILD SUMMARY                  " && \
    echo "═══════════════════════════════════════════════════════════════" && \
    echo "Build Type:     PREBUILT BINARIES" && \
    echo "Platform:       ${TARGETPLATFORM:-linux/amd64}" && \
    echo "Source Path:    ${PREBUILT_IGGY_SERVER:-not specified}" && \
    echo "Binary Info:" && \
    (command -v file >/dev/null 2>&1 && file /usr/local/bin/iggy-server | sed 's/^/  /' || \
    echo "  $(ldd /usr/local/bin/iggy-server 2>&1 | head -1)") && \
    echo "Binary Size:" && \
    ls -lh /usr/local/bin/iggy-server /usr/local/bin/iggy | awk '{print "  " $9 ": " $5}' && \
    echo "Build Date:     $(date -u '+%Y-%m-%d %H:%M:%S UTC')" && \
    echo "Container Base: debian:trixie-slim" && \
    echo "═══════════════════════════════════════════════════════════════"
ENTRYPOINT ["iggy-server"]

FROM debian:trixie-slim AS runtime
ARG TARGETPLATFORM
ARG PROFILE=release
ARG LIBC=musl
WORKDIR /app
COPY --from=builder /app/iggy-server /usr/local/bin/iggy-server
COPY --from=builder /app/iggy        /usr/local/bin/iggy
RUN echo "═══════════════════════════════════════════════════════════════" && \
    echo "                    IGGY SERVER BUILD SUMMARY                  " && \
    echo "═══════════════════════════════════════════════════════════════" && \
    echo "Build Type:     FROM SOURCE" && \
    echo "Platform:       ${TARGETPLATFORM:-linux/amd64}" && \
    echo "Profile:        ${PROFILE}" && \
    echo "Libc:           ${LIBC}" && \
    case "${TARGETPLATFORM:-linux/amd64}:${LIBC}" in \
    "linux/amd64:musl")  echo "Target:         x86_64-unknown-linux-musl" ;; \
    "linux/arm64:musl")  echo "Target:         aarch64-unknown-linux-musl" ;; \
    "linux/amd64:glibc") echo "Target:         x86_64-unknown-linux-gnu" ;; \
    "linux/arm64:glibc") echo "Target:         aarch64-unknown-linux-gnu" ;; \
    *) echo "Target:         unknown" ;; \
    esac && \
    echo "Binary Info:" && \
    (command -v file >/dev/null 2>&1 && file /usr/local/bin/iggy-server | sed 's/^/  /' || \
    echo "  $(ldd /usr/local/bin/iggy-server 2>&1 | head -1)") && \
    echo "Binary Size:" && \
    ls -lh /usr/local/bin/iggy-server /usr/local/bin/iggy | awk '{print "  " $9 ": " $5}' && \
    echo "Build Date:     $(date -u '+%Y-%m-%d %H:%M:%S UTC')" && \
    echo "═══════════════════════════════════════════════════════════════"
ENTRYPOINT ["iggy-server"]
