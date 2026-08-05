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
FROM rust:${RUST_VERSION}-slim-trixie AS rust-toolchain

FROM ubuntu:24.04

COPY --from=rust-toolchain /usr/local/cargo /usr/local/cargo
COPY --from=rust-toolchain /usr/local/rustup /usr/local/rustup

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    clang \
    composer \
    curl \
    git \
    libclang-dev \
    libssl-dev \
    php8.3-cli \
    php8.3-dev \
    php8.3-mbstring \
    php8.3-xml \
    pkg-config \
    unzip \
    && rm -rf /var/lib/apt/lists/*

ENV CARGO_HOME=/usr/local/cargo
ENV RUSTUP_HOME=/usr/local/rustup
ENV PATH=/usr/local/cargo/bin:$PATH
ENV PHP=/usr/bin/php8.3
ENV PHP_CONFIG=/usr/bin/php-config8.3
ENV IGGY_HOST=iggy-server
ENV IGGY_PORT=8090
ENV BDD_FEATURE_FILE=/workspace/bdd/scenarios/basic_messaging.feature

RUN rustup component add llvm-tools-preview \
    && cargo install cargo-php --locked --version 0.1.21 \
    && cargo install cargo-llvm-cov --locked

WORKDIR /workspace

COPY Cargo.toml Cargo.lock ./
COPY core/binary_protocol/ ./core/binary_protocol/
COPY core/common/ ./core/common/
COPY core/sdk/ ./core/sdk/

COPY foreign/php/Cargo.toml ./foreign/php/
COPY foreign/php/composer.json ./foreign/php/
COPY foreign/php/phpunit.xml.dist ./foreign/php/
COPY foreign/php/README.md foreign/php/LICENSE foreign/php/NOTICE ./foreign/php/
COPY foreign/php/.cargo/ ./foreign/php/.cargo/
COPY foreign/php/src/ ./foreign/php/src/
COPY foreign/php/tests/ ./foreign/php/tests/
COPY foreign/php/scripts/ ./foreign/php/scripts/

COPY bdd/php/ ./bdd/php/
COPY bdd/scenarios/ ./bdd/scenarios/

WORKDIR /workspace/foreign/php

RUN composer install --no-interaction --prefer-dist
RUN cargo php install --yes
RUN chmod +x ./scripts/test.sh

CMD ["./scripts/test.sh", "--configuration", "/workspace/bdd/php/phpunit.xml.dist"]
