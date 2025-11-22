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

build:
  cargo build

test: build
  cargo test

tests TEST: build
  cargo test {{TEST}}

nextest: build
  cargo nextest run

nextests TEST: build
  cargo nextest run --nocapture -- {{TEST}}

server:
  cargo run --bin iggy-server

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

licenses-list-check:
  ./scripts/ci/licenses-list.sh --check

licenses-list-fix:
  ./scripts/ci/licenses-list.sh --fix

markdownlint:
  markdownlint '**/*.md' --ignore-path .gitignore
