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

# The conformance agent of the PHP SDK. The extension builds as in bdd/php/Dockerfile, with the
# same Ubuntu, PHP and Rust, and the final image keeps only PHP, the extension and the agent.
# Build it from the repository root:
#
#   docker build -f conformance/agents/php/Dockerfile -t iggy-conformance-php .
#
# The orchestrator starts the agent with:
#
#   docker run -i --rm --init --network host iggy-conformance-php

ARG RUST_VERSION=1.99.1
FROM rust:${RUST_VERSION}-slim-trixie AS rust-toolchain

FROM ubuntu:24.04 AS extension

COPY --from=rust-toolchain /usr/local/cargo /usr/local/cargo
COPY --from=rust-toolchain /usr/local/rustup /usr/local/rustup

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    clang \
    libclang-dev \
    libssl-dev \
    php8.3-cli \
    php8.3-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

ENV CARGO_HOME=/usr/local/cargo
ENV RUSTUP_HOME=/usr/local/rustup
ENV PATH=/usr/local/cargo/bin:$PATH
ENV PHP=/usr/bin/php8.3
ENV PHP_CONFIG=/usr/bin/php-config8.3

WORKDIR /workspace

COPY Cargo.toml Cargo.lock ./
COPY core/binary_protocol/ ./core/binary_protocol/
COPY core/common/ ./core/common/
COPY core/sdk/ ./core/sdk/
COPY foreign/php/Cargo.toml ./foreign/php/
COPY foreign/php/.cargo/ ./foreign/php/.cargo/
COPY foreign/php/src/ ./foreign/php/src/

WORKDIR /workspace/foreign/php

RUN cargo build --release

FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    php8.3-cli \
    php8.3-gmp \
    php8.3-mbstring \
    && rm -rf /var/lib/apt/lists/*

COPY --from=extension /workspace/foreign/php/target/release/libiggy_php.so /usr/local/lib/iggy_php.so
COPY conformance/agents/php/agent.php /agent/
COPY conformance/agents/php/src/ /agent/src/

# PHP prints its errors to stdout by default, where they would break the protocol.
ENTRYPOINT ["php", "-d", "extension=/usr/local/lib/iggy_php.so", "-d", "display_errors=stderr", "/agent/agent.php"]
