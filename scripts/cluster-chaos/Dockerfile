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

# The cluster chaos checker, built from this checkout so its SDK speaks the
# same wire protocol as a server built from the same commit. Build from the
# repository root:
#
#   docker build -f scripts/cluster-chaos/Dockerfile -t iggy-cluster-chaos-checker .

ARG RUST_VERSION=1.99
FROM rust:${RUST_VERSION}-slim-trixie AS builder
WORKDIR /build
COPY . .
# Caps parallel rustc jobs on a shared machine; empty means one per CPU.
ARG CARGO_JOBS=
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked -p tools --bin cluster-chaos-checker ${CARGO_JOBS:+--jobs "$CARGO_JOBS"} \
    && cp target/release/cluster-chaos-checker /usr/local/bin/

FROM debian:trixie-slim
COPY --from=builder /usr/local/bin/cluster-chaos-checker /usr/local/bin/
ENTRYPOINT ["cluster-chaos-checker"]
