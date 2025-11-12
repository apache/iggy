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

FROM rust:1.89.0-alpine3.22 AS builder
RUN apk add musl-dev

WORKDIR /app
COPY . .

# Build server in debug mode for faster compilation
RUN cargo build --bin iggy-server

FROM debian:bookworm-slim

# Install curl for healthcheck
RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

# Copy server binary
COPY --from=builder /app/target/debug/iggy-server /usr/local/bin/

# Copy configs directory
COPY core/configs ./configs

# Create directory for data
RUN mkdir -p /app/local_data

WORKDIR /app

ENV IGGY_HTTP_ADDRESS=0.0.0.0:3000
ENV IGGY_QUIC_ADDRESS=0.0.0.0:8080
ENV IGGY_TCP_ADDRESS=0.0.0.0:8090
ENV IGGY_WEBSOCKET_ADDRESS=0.0.0.0:8092
ENV IGGY_ROOT_USERNAME=iggy
ENV IGGY_ROOT_PASSWORD=iggy

# Expose ports
EXPOSE 8090 3000 8080

# Start server with --fresh flag to clean local data on startup
CMD ["iggy-server", "--fresh"]
