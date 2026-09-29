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
COPY . .
RUN cargo build --release --package iggy-gateway-kafka --bin iggy-gateway-kafka

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/iggy-gateway-kafka .

ENV IGGY_KAFKA_BIND_ADDR=0.0.0.0:9093

CMD ["/iggy-gateway-kafka"]
