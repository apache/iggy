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

# Build context is the repository root (a Cargo workspace member cannot build standalone) -
# `docker-compose.yml` in this directory sets `context: ../..` accordingly.

ARG RUST_VERSION=1.98
FROM rust:${RUST_VERSION}-slim-trixie AS builder

WORKDIR /build
# build-essential: the zstd/lz4/snap codecs kafka-protocol pulls in compile a small amount of
# vendored C. No libssl-dev - core/sdk's TLS is rustls, not openssl.
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY rust-toolchain.toml rust-toolchain.toml
# Resolves/installs the toolchain now, in a layer keyed only on rust-toolchain.toml - matching
# RUST_VERSION above means this is a no-op resolution, not a download, but isolates the cost
# from the full COPY . . below, which invalidates on any source change in the workspace.
# Same ordering core/server/Dockerfile uses for the same reason.
RUN cargo --version

COPY . .
# Cache mounts persist across builds but not into the image layer, so the binary is copied out
# to a plain path before the mount unmounts. --locked matches CI (scripts never resolve a
# dependency version CI didn't already lock).
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/build/target,sharing=locked \
    cargo build --release --locked --package iggy-gateway-kafka --bin iggy-gateway-kafka && \
    cp target/release/iggy-gateway-kafka /iggy-gateway-kafka

FROM debian:trixie-slim
# netcat-openbsd: nothing in the gateway's own dependency graph needs it - it's here only so
# docker-compose.yml's healthcheck has a TCP-probe primitive to call.
RUN apt-get update && apt-get install -y \
    ca-certificates \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /iggy-gateway-kafka .

ENV IGGY_KAFKA_BIND_ADDR=0.0.0.0:9093
# No baked IGGY_KAFKA_ADVERTISED_HOST default: wildcard bind means the gateway cannot derive
# its own advertised host (server.rs's BrokerAdvertise::from_server_config refuses to start
# without one), and that refusal is deliberate - it forces whoever runs this image to say what
# host a client can actually reach it on, rather than silently advertising "localhost" to a
# client on a different machine. docker-compose.yml sets it explicitly for this quick start.

CMD ["/iggy-gateway-kafka"]
