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

# Apache Iggy (Incubating) is an effort undergoing incubation at the Apache
# Software Foundation (ASF), sponsored by the Apache Incubator PMC.
#
# Incubation is required of all newly accepted projects until a further review
# indicates that the infrastructure, communications, and decision making
# process have stabilized in a manner consistent with other successful ASF
# projects.
#
# While incubation status is not necessarily a reflection of the completeness
# or stability of the code, it does indicate that the project has yet to be
# fully endorsed by the ASF.

# Build context for this Dockerfile is the repository root (NOT web/), so
# that the consolidated scripts/ci/third-party-licenses.sh and its renderer
# are reachable. See .github/actions/utils/docker-buildx/action.yml.

FROM node:lts-alpine AS deps
WORKDIR /app
COPY web/package*.json ./
RUN npm ci --prefer-offline --no-audit

FROM node:lts-alpine AS builder
WORKDIR /app

# bash required by scripts/ci/third-party-licenses.sh; node:lts-alpine
# ships only ash by default. curl + tar used to fetch cargo-about
# (third-party-licenses.sh autodetects rust manifests vs node manifest;
# the web Dockerfile only feeds it a node manifest, so cargo-about is
# not actually invoked here, but bash is.)
RUN apk add --no-cache bash

COPY --from=deps /app/node_modules ./node_modules
COPY web/ ./

RUN npm run build

# Generate LICENSE-binary for the production npm dependencies bundled
# into this image. Required by ASF release policy: convenience binaries
# that bundle third-party code MUST include the full license text of
# every bundled package inside the artifact. Uses the same consolidated
# script and SPDX allow-list that the pre-merge `--validate` gate uses,
# so the published image cannot diverge from policy.
COPY scripts/ci/third-party-licenses.sh /opt/iggy/scripts/ci/third-party-licenses.sh
COPY scripts/ci/render-node-licenses.mjs /opt/iggy/scripts/ci/render-node-licenses.mjs
RUN /opt/iggy/scripts/ci/third-party-licenses.sh --generate \
      --manifest /app \
      --output /app/LICENSE-binary

FROM node:lts-alpine AS production

WORKDIR /app

COPY web/package*.json ./
RUN npm ci --production --prefer-offline --no-audit

COPY --from=builder /app/build ./build
COPY --from=builder /app/LICENSE-binary /usr/share/doc/iggy-web-ui/LICENSE-binary
COPY LICENSE NOTICE /usr/share/doc/iggy-web-ui/
USER node

EXPOSE 3050

ENV PORT=3050
ENV NODE_ENV=production

CMD ["node", "build"]
