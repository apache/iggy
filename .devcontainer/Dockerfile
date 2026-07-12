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

# Toolchain is pinned by rust-toolchain.toml (channel 1.96.0). The base
# image ships rustup, so the pin auto-resolves on first cargo invocation.
ARG RUST_VERSION=1.96
FROM rust:${RUST_VERSION}-slim-trixie

# Build + runtime dependencies mirror the production Dockerfile builder
# stage (native TLS, hwloc topology, udev) plus the Node.js toolchain the
# web UI build requires. git/curl/sudo/pkg-config round out the dev shell.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    git \
    pkg-config \
    libssl-dev \
    libhwloc-dev \
    libudev-dev \
    liblzma5 \
    nodejs \
    npm \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Pre-fetch the pinned toolchain components (rustfmt, clippy) so the first
# in-container build does not stall resolving them. rust-src is added for
# rust-analyzer sysroot / standard-library resolution.
COPY rust-toolchain.toml /tmp/rust-toolchain.toml
RUN cd /tmp && rustup show && rustup component add rust-src && rm rust-toolchain.toml

# Create a non-root user matching the default VS Code devcontainer UID/GID
# so bind-mounted workspace files keep sane ownership.
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=${USER_UID}
RUN groupadd --gid ${USER_GID} ${USERNAME} \
    && useradd --uid ${USER_UID} --gid ${USER_GID} -m ${USERNAME} \
    && echo "${USERNAME} ALL=(root) NOPASSWD:ALL" > /etc/sudoers.d/${USERNAME} \
    && chmod 0440 /etc/sudoers.d/${USERNAME}

USER ${USERNAME}
