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

# syntax=docker/dockerfile:1
ARG RUST_VERSION=1.93
FROM rust:${RUST_VERSION}-slim-trixie

RUN apt-get update && \
    apt-get install -y \
    python3 \
    python3-dev \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

# Copy the entire workspace for dependencies
WORKDIR /workspace
COPY . .

# Install Python dependencies for BDD (includes local apache-iggy source)
WORKDIR /workspace/bdd/python
RUN uv sync --frozen

# Set up BDD test directory
WORKDIR /app
RUN mkdir -p /app/features
RUN mkdir -p /app/tests

# Copy BDD test files
RUN cp -r /workspace/bdd/python/tests/* /app/tests/

# Default command
CMD ["uv", "run", "--project", "/workspace/bdd/python", "pytest", "tests/", "-v"]
