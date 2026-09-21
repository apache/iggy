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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CPP_ROOT="$REPO_ROOT/foreign/cpp"

cd "$CPP_ROOT"

OUTPUT="$REPO_ROOT/reports/cpp-coverage.lcov"
COVERAGE_DIR="$CPP_ROOT/target/cpp-coverage"
BUILD_PROFRAW_DIR="$COVERAGE_DIR/build"
PROFDATA="$COVERAGE_DIR/shim.profdata"
CPP_RAW="$CPP_ROOT/bazel-out/_coverage/_coverage_report.dat"

mkdir -p "$COVERAGE_DIR" "$BUILD_PROFRAW_DIR" "$(dirname "$OUTPUT")"
rm -f "$COVERAGE_DIR"/*.profraw "$BUILD_PROFRAW_DIR"/*.profraw "$PROFDATA" "$OUTPUT"

if ! LLVM_COV_ENV="$(cargo llvm-cov show-env --no-rustc-wrapper --sh)"; then
  echo "cpp-coverage: 'cargo llvm-cov show-env' failed" >&2
  exit 1
fi
eval "$LLVM_COV_ENV"
export LLVM_PROFILE_FILE="$COVERAGE_DIR/%p-%m.profraw"

LLVM_BIN="$(rustc --print target-libdir)/../bin"
LLVM_COV="$LLVM_BIN/llvm-cov"
LLVM_PROFDATA="$LLVM_BIN/llvm-profdata"

bazel coverage \
  --config=debug \
  --lockfile_mode=error \
  --combined_report=lcov \
  '--instrumentation_filter=//:iggy-cpp' \
  --strategy=TestRunner=standalone \
  --define=iggy_cpp_coverage=1 \
  --action_env=RUSTFLAGS \
  "--action_env=LLVM_PROFILE_FILE=$BUILD_PROFRAW_DIR/%p-%m.profraw" \
  "--run_under=/usr/bin/env LLVM_PROFILE_FILE=$LLVM_PROFILE_FILE" \
  //:unit //:e2e

"$LLVM_PROFDATA" merge -sparse \
  "$COVERAGE_DIR"/*.profraw \
  -o "$PROFDATA"

sed 's|^SF:|SF:foreign/cpp/|' "$CPP_RAW" >"$OUTPUT"

"$LLVM_COV" export \
  --object bazel-bin/unit \
  --object bazel-bin/e2e \
  -instr-profile="$PROFDATA" \
  -format=lcov \
  -ignore-filename-regex='(\.cargo/|/rustc/|/core/|cxxbridge|/registry/)' |
  awk '
    /^SF:/ {
      include = /foreign\/cpp\/src\/[^/]+\.rs$/
      if (include) {
        sub(/^SF:.*foreign\/cpp\//, "SF:foreign/cpp/")
      }
    }
    include
  ' >>"$OUTPUT"

"$SCRIPT_DIR/validate-lcov.sh" "$OUTPUT"
