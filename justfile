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

# Convenience commands for iggy
# See https://github.com/casey/just
#
# Usage: just <command>
#
# Commands:

alias b  := build
alias t  := test
alias c  := tests
alias n  := nextest
alias s  := nextests
alias rb := run-benches
alias pcs := profile-cpu-server
alias pcc := profile-cpu-client
alias pis := profile-io-server
alias pic := profile-io-client
alias ctc := clean-test-containers

# Every test container is named `iggy-test-<service>[-<uuid>]` (see
# fixtures::unique_container_name). Reuse fixtures (elasticsearch, doris) keep
# their container running, and a crashed run can orphan any fixture's container,
# so the recipes below reap everything matching the prefix on exit.
reap_test_containers := "docker ps -aqf 'name=^iggy-test-' | xargs -r docker rm -f >/dev/null 2>&1 || true"

build:
  cargo build

build-vsr:
  cargo build --features vsr

test: build
  #!/usr/bin/env bash
  set -euo pipefail
  trap "{{reap_test_containers}}" EXIT
  cargo test

tests TEST: build
  #!/usr/bin/env bash
  set -euo pipefail
  trap "{{reap_test_containers}}" EXIT
  cargo test {{TEST}}

nextest: build
  #!/usr/bin/env bash
  set -euo pipefail
  trap "{{reap_test_containers}}" EXIT
  cargo nextest run

# Like `nextest` but with the `vsr` feature; builds vsr first so the
# harness-spawned iggy-server-ng carries the vsr wire format. `--no-fail-fast`
# because nextest otherwise cancels the run on the first red, which on a loaded
# box means a stray flake hides most of the suite and the reported counts
# understate what actually ran.
nextest-vsr: build-vsr
  #!/usr/bin/env bash
  set -euo pipefail
  trap "{{reap_test_containers}}" EXIT
  cargo nextest run --features vsr --no-fail-fast

nextests TEST: build
  #!/usr/bin/env bash
  set -euo pipefail
  trap "{{reap_test_containers}}" EXIT
  cargo nextest run --nocapture -- {{TEST}}

# Force-remove all `iggy-test-*` containers on demand. Useful after running
# `cargo nextest run` directly (outside the recipes above).
clean-test-containers:
  {{reap_test_containers}}

# Run Miri (UB detector) on the unsafe-heavy crates that don't pull
# tokio/compio. Mirrors the `miri` task in CI. Pinned to the same nightly
# as `.github/actions/rust/pre-merge/action.yml` so local runs don't drift
# from CI on the next nightly bump — keep these two dates in sync.
# Requires:
#
#   rustup toolchain install nightly-2026-04-21 --component miri
#
miri:
  MIRIFLAGS="-Zmiri-tree-borrows -Zmiri-strict-provenance" \
    cargo +nightly-2026-04-21 miri test -p iggy_binary_protocol -p consensus

server *ARGS:
  cargo run --bin iggy-server {{ARGS}}

server-ng *ARGS:
  cargo run --bin iggy-server-ng {{ARGS}}

run-benches:
  ./scripts/run-benches.sh

profile-cpu-server:
  ./scripts/profile.sh iggy-server cpu

profile-cpu-client:
  ./scripts/profile.sh iggy-bench cpu

profile-io-server:
  ./scripts/profile.sh iggy-server io

profile-io-client:
  ./scripts/profile.sh iggy-bench io

licenses-fix:
  ./scripts/ci/license-headers.sh --fix

licenses-check:
  ./scripts/ci/license-headers.sh --check

markdownlint:
  markdownlint '**/*.md' --ignore-path .gitignore
