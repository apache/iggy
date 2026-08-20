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

ARG RUNNER_PLATFORM=linux/amd64

FROM --platform=${RUNNER_PLATFORM} docker.io/library/rust@sha256:39f68a3e8e3ff425f8945ffa91128e60ff930d53e17fbb5214e95824bdd46f1b AS rust-toolchain

RUN rustup component add clippy rustfmt --toolchain 1.97.1

FROM --platform=${RUNNER_PLATFORM} ghcr.io/actions/actions-runner@sha256:a1919047b038c38871d667c58cfdc7a878452711ab1212fb6036188f27a7ab16

USER root

ENV CARGO_HOME=/home/runner/.cargo
ENV RUSTUP_HOME=/usr/local/rustup
ENV RUSTUP_TOOLCHAIN=1.97.1-x86_64-unknown-linux-gnu
ENV PATH=/usr/local/cargo/bin:${PATH}

COPY --from=rust-toolchain /usr/local/cargo /usr/local/cargo
COPY --from=rust-toolchain /usr/local/rustup /usr/local/rustup
COPY start.sh /usr/local/bin/iggy-cloud-runner-start

RUN chmod 0555 /usr/local/bin/iggy-cloud-runner-start \
    && mkdir -p /home/runner/.cargo \
    && chown runner:runner /home/runner/.cargo \
    && gpasswd --delete runner sudo \
    && gpasswd --delete runner docker \
    && rm -f /usr/bin/docker \
       /usr/bin/sudo \
       /usr/local/lib/docker/cli-plugins/docker-buildx \
    && command -v curl \
    && command -v jq \
    && command -v tail \
    && rustc --version \
    && cargo --version \
    && rustfmt --version

USER runner

ENTRYPOINT ["/usr/local/bin/iggy-cloud-runner-start"]
