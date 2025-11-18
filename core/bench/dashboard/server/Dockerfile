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

# Build stage
FROM rust:${RUST_VERSION}-slim-trixie AS builder
ARG IGGY_CI_BUILD
ENV IGGY_CI_BUILD=${IGGY_CI_BUILD}

WORKDIR /usr/src/iggy-bench-dashboard

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    openssl \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install cargo-binstall
RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash

# Install trunk via cargo-binstall
RUN cargo binstall -y trunk

# Add wasm target
RUN rustup target add wasm32-unknown-unknown

RUN echo "Using Rust toolchain from rust-toolchain.toml: $(rustup show)"

# Copy the entire workspace
COPY . .

# Build frontend
RUN cd core/bench/dashboard/frontend && trunk build --release

# Build the server with release profile
RUN cargo build --release --bin iggy-bench-dashboard-server

# Runtime stage
FROM debian:trixie-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    openssl \
    curl \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Copy the built binary and frontend files
COPY --from=builder /usr/src/iggy-bench-dashboard/target/release/iggy-bench-dashboard-server /app/
COPY --from=builder /usr/src/iggy-bench-dashboard/core/bench/dashboard/frontend/dist /app/frontend/dist

# Create data directory and non-root user
RUN groupadd -r iggy && \
    useradd -r -g iggy -s /bin/false iggy && \
    mkdir -p /data/performance_results && \
    chown -R iggy:iggy /app /data && \
    chmod -R 755 /data/performance_results

# Copy the entrypoint script
COPY core/bench/dashboard/server/docker-entrypoint.sh /app/
RUN chmod +x /app/docker-entrypoint.sh && \
    chown iggy:iggy /app/docker-entrypoint.sh

# Set default environment variables for configuration
ENV HOST=0.0.0.0 \
    PORT=80 \
    RESULTS_DIR=/data/performance_results

# Set volume for results with proper permissions
VOLUME ["/data/performance_results"]

# Switch to non-root user
USER iggy

# Set the entrypoint script
ENTRYPOINT ["/app/docker-entrypoint.sh"]
