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

ARG RUST_VERSION=1.99
FROM rust:${RUST_VERSION}-slim-trixie

ARG COVERAGE=false
ARG BDD_RUST_FEATURES=bdd
ARG LLVM_COV_VERSION=0.9.1

ENV RUSTFLAGS="-C linker=gcc" \
    CARGO_INCREMENTAL=0 \
    CARGO_PROFILE_DEV_DEBUG=0 \
    CARGO_PROFILE_TEST_DEBUG=0

SHELL ["/bin/bash", "-euo", "pipefail", "-c"]
WORKDIR /app

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    libhwloc-dev \
    libudev-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN if [[ "$COVERAGE" == true ]]; then \
        rustup component add llvm-tools-preview; \
        host=$(rustc -vV | sed -n 's/^host: //p'); \
        curl -fsSL --retry 5 --retry-all-errors \
            "https://github.com/taiki-e/cargo-llvm-cov/releases/download/v${LLVM_COV_VERSION}/cargo-llvm-cov-${host}.tar.gz" \
            -o /tmp/llvm-cov.tar.gz; \
        tar -xzf /tmp/llvm-cov.tar.gz -C /usr/local/cargo/bin; \
        rm /tmp/llvm-cov.tar.gz; \
    fi

COPY . .
# The shared Docker context includes host server binaries for other images.
# Their coverage mappings belong in the host report, not the BDD report.
RUN rm -rf target && mkdir -p /app/features
RUN if [[ "$COVERAGE" == true ]]; then \
        coverage_env=$(cargo llvm-cov show-env --sh --remap-path-prefix); \
        eval "$coverage_env"; \
    fi; \
    cargo test --locked --no-run -p bdd --features "$BDD_RUST_FEATURES"

CMD ["cargo", "test", "--locked", "-p", "bdd", "--features", "bdd"]
