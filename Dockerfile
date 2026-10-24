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

ARG RUST_VERSION=1.99.1
FROM rust:${RUST_VERSION}-slim-trixie AS builder

WORKDIR /build
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    libhwloc-dev \
    libudev-dev \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN npm --prefix web ci && npm --prefix web run build:static
RUN cargo build --bin iggy --release
RUN cargo build --bin iggy-server -p server --release

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y \
    ca-certificates \
    liblzma5 \
    libhwloc15 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/iggy .
COPY --from=builder /build/target/release/iggy-server .

# Listeners default to the loopback addresses from the shipped config.toml, so a
# bare `docker run` boots and listens on 127.0.0.1 inside the container. Binding
# a wildcard here would make the image unbootable with cluster mode disabled:
# an unspecified bind names no address a client can dial, so the server refuses
# to start unless node.advertised_address is also set. To make the server
# reachable from the host, override the listener(s) together with the
# client-facing address, for example:
#   -e IGGY_TCP_ADDRESS=0.0.0.0:8090 -e IGGY_NODE_ADVERTISED_ADDRESS=localhost

CMD ["/iggy-server"]
