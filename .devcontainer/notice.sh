#!/usr/bin/env bash
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

# Licenced under the Apache License, Version 2.0.

set -euo pipefail

RED=$'\033[0;31m'
BOLD=$'\033[1m'
NC=$'\033[0m'

cat <<EOF

${RED}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}
${BOLD}KNOWN LIMITATION: Docker-based bind-mount tests will fail in this devcontainer${NC}
${RED}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}

This devcontainer uses docker-outside-of-docker: Docker commands inside the container
talk to the host's Docker daemon via the Docker socket.

The workspace is mounted at ${BOLD}/workspaces/iggy${NC} inside the container, but the host
Docker daemon only knows the real host path (${BOLD}\${localWorkspaceFolder}${NC}).

Bind mounts from compose files or testcontainers resolve to /workspaces/iggy/...,
which the host daemon cannot reach.

${BOLD}Affected tests:${NC}
  - BDD tests:          ./scripts/run-bdd-tests.sh
  - HTTP sink:          cargo test -p integration -- connectors::http
  - HTTP config provider: cargo test -p integration -- connectors::http_config_provider

${BOLD}Workarounds:${NC}
  - Run affected tests on the host directly, outside the devcontainer
  - Use native CI (GitHub Actions) where path translation is handled

EOF
