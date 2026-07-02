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

# C++ BDD harness, built in three stages so the runtime image carries only what the test run
# needs (~330 MB instead of ~10 GB for a single toolchain image):
#   1. build   - Bazel builds the cucumber-cpp wire server. bdd/cpp is its own Bazel module and
#                links the Iggy C++ SDK from foreign/cpp via local_path_override; rules_rust
#                fetches its own Rust toolchain, so no system Rust is needed.
#   2. deps    - Bundler installs the Ruby Cucumber runner (compiles the ffi native extension).
#   3. runtime - copies the wire server binary and the vendored gems, then runs cucumber.
# cucumber-cpp v0.8.0 is wire-only: the Ruby runner reads the shared feature files and drives
# the C++ wire server over the wire protocol.

# Stage 1: build the wire server with Bazel.
FROM gcr.io/bazel-public/bazel:9.1.1 AS build

USER root
WORKDIR /workspace
COPY . .

RUN cd bdd/cpp \
    && bazel build //:bdd_wire_server \
    && cp bazel-bin/bdd_wire_server /tmp/bdd_wire_server

# Stage 2: install the Ruby Cucumber runner. build-essential is needed to compile the ffi
# native extension and never reaches the runtime image.
FROM ruby:3.3-slim-trixie AS deps

RUN apt-get update && apt-get install --yes --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace/bdd/cpp
COPY bdd/cpp/Gemfile bdd/cpp/Gemfile.lock ./
RUN gem install bundler -v 2.6.9 --no-document \
    && bundle install

# Stage 3: runtime image.
FROM ruby:3.3-slim-trixie

# The wire server is a C++ binary and needs the C++ runtime at run time.
RUN apt-get update && apt-get install --yes --no-install-recommends \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=deps /usr/local/bundle /usr/local/bundle
COPY --from=build /tmp/bdd_wire_server /usr/local/bin/bdd_wire_server

WORKDIR /workspace/bdd/cpp
COPY bdd/cpp/Gemfile bdd/cpp/Gemfile.lock ./
COPY bdd/cpp/features ./features

# Entrypoint: copy the mounted shared feature files, start the wire server, then run cucumber.
COPY bdd/cpp/scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
