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

if [[ ! -f MODULE.bazel || ! -f BUILD.bazel ]]; then
  echo "cpp-coverage: must run with CWD set to foreign/cpp" >&2
  exit 2
fi

for tool in bazel cargo; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "cpp-coverage: required tool '$tool' not on PATH" >&2
    exit 2
  fi
done

OUTPUT="${1:-../../reports/cpp-coverage.lcov}"
OUTPUT_TMP="${OUTPUT}.tmp"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_DIR="$(pwd)/target/cpp-coverage-tmp"
PROFRAW_DIR="$TMP_DIR/profraw"
BUILD_PROFRAW_DIR="$TMP_DIR/build-profraw"
PROFDATA="$TMP_DIR/cpp-shim.profdata"
CPP_RAW="$(pwd)/bazel-out/_coverage/_coverage_report.dat"
SHIM_RAW="$TMP_DIR/rust-shim-raw.lcov"
CPP_FILTERED="$TMP_DIR/cpp-coverage.lcov"
SHIM_FILTERED="$TMP_DIR/rust-shim.lcov"

mkdir -p "$PROFRAW_DIR" "$BUILD_PROFRAW_DIR" "$(dirname "$OUTPUT")"
rm -f "$PROFRAW_DIR"/*.profraw "$BUILD_PROFRAW_DIR"/*.profraw \
  "$PROFDATA" "$CPP_FILTERED" "$SHIM_RAW" "$SHIM_FILTERED" "$OUTPUT_TMP"

show_env_file="$TMP_DIR/show-env.sh"
if ! cargo llvm-cov show-env --no-rustc-wrapper --sh >"$show_env_file"; then
  echo "cpp-coverage: 'cargo llvm-cov show-env' failed (see output above)" >&2
  exit 1
fi
# shellcheck disable=SC1090
source "$show_env_file"
if [[ -z "${RUSTFLAGS:-}" ]]; then
  echo "cpp-coverage: cargo llvm-cov show-env did not export RUSTFLAGS" >&2
  exit 1
fi
export LLVM_PROFILE_FILE="$PROFRAW_DIR/cpp-%p-%m.profraw"

llvm_bin_dir="$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | sed -n 's/^host: //p')/bin"
LLVM_COV="$llvm_bin_dir/llvm-cov"
LLVM_PROFDATA="$llvm_bin_dir/llvm-profdata"
if [[ ! -x "$LLVM_COV" || ! -x "$LLVM_PROFDATA" ]]; then
  echo "cpp-coverage: $LLVM_COV or $LLVM_PROFDATA missing;" >&2
  echo "cpp-coverage: install with 'rustup component add llvm-tools'" >&2
  exit 1
fi

BAZEL_FLAGS=(--config=cpp-coverage)
COVERAGE_DEFINE="--define=iggy_cpp_coverage=1"
TEST_QUERY='kind("cc_test rule", //...)'
if ! TEST_TARGET_ROWS="$(
  bazel cquery "$TEST_QUERY" \
    "${BAZEL_FLAGS[@]}" \
    "$COVERAGE_DEFINE" \
    --output=starlark \
    '--starlark:expr="//" + target.label.package + ":" + target.label.name + "\t" + providers(target)["DefaultInfo"].files_to_run.executable.path'
)"; then
  echo "cpp-coverage: failed to discover C++ test targets" >&2
  exit 1
fi
if [[ -z "$TEST_TARGET_ROWS" ]]; then
  echo "cpp-coverage: no C++ test targets found" >&2
  exit 1
fi

TEST_TARGETS=()
TEST_OBJECTS=()
while IFS=$'\t' read -r test_target test_object; do
  if [[ -z "$test_target" || -z "$test_object" ]]; then
    echo "cpp-coverage: invalid C++ test target entry: $test_target $test_object" >&2
    exit 1
  fi
  TEST_TARGETS+=("$test_target")
  TEST_OBJECTS+=("$(pwd)/$test_object")
done <<<"$TEST_TARGET_ROWS"

bazel coverage "${BAZEL_FLAGS[@]}" \
  --combined_report=lcov \
  '--instrumentation_filter=//:iggy-cpp' \
  --nocache_test_results \
  --strategy=CoverageReport=local \
  --strategy=TestRunner=standalone \
  "$COVERAGE_DEFINE" \
  "--action_env=RUSTFLAGS=$RUSTFLAGS" \
  "--action_env=LLVM_PROFILE_FILE=$BUILD_PROFRAW_DIR/build-%p-%m.profraw" \
  "--run_under=/usr/bin/env LLVM_PROFILE_FILE=$LLVM_PROFILE_FILE" \
  "${TEST_TARGETS[@]}"

if [[ ! -s "$CPP_RAW" ]]; then
  echo "cpp-coverage: bazel coverage produced no combined report" >&2
  exit 1
fi

shopt -s nullglob
profraws=("$PROFRAW_DIR"/*.profraw)
shopt -u nullglob
if [[ ${#profraws[@]} -eq 0 ]]; then
  echo "cpp-coverage: no profraw files in $PROFRAW_DIR; RUSTFLAGS or LLVM_PROFILE_FILE did not reach the test binaries" >&2
  exit 1
fi

"$LLVM_PROFDATA" merge -sparse "${profraws[@]}" -o "$PROFDATA"

LLVM_COV_OBJECT_ARGS=()
for test_object in "${TEST_OBJECTS[@]}"; do
  if [[ ! -f "$test_object" ]]; then
    echo "cpp-coverage: test executable not found: $test_object" >&2
    exit 1
  fi
  LLVM_COV_OBJECT_ARGS+=(--object "$test_object")
done

"$LLVM_COV" export "${LLVM_COV_OBJECT_ARGS[@]}" \
  -instr-profile="$PROFDATA" \
  -format=lcov \
  -ignore-filename-regex='(\.cargo/|/rustc/|/core/|cxxbridge|/registry/)' >"$SHIM_RAW"

filter_lcov() {
  local input="$1" output="$2" mode="$3"
  awk -v mode="$mode" '
    function map_sf(raw,   mapped) {
      if (mode == "rs") {
        if (match(raw, /foreign\/cpp\/src\/[^\/]+\.rs/)) {
          return substr(raw, RSTART, RLENGTH)
        }
        return ""
      }
      if (match(raw, /foreign\/cpp\/(src\/[^\/]+\.cpp|include\/[^\/]+\.hpp?)/)) {
        return substr(raw, RSTART, RLENGTH)
      }
      if (match(raw, /\/proc\/self\/cwd\/(src\/[^\/]+\.cpp|include\/[^\/]+\.hpp?)/)) {
        return "foreign/cpp/" substr(raw, RSTART + 15, RLENGTH - 15)
      }
      if (raw ~ /^(src\/[^\/]+\.cpp|include\/[^\/]+\.hpp?)$/) {
        return "foreign/cpp/" raw
      }
      return ""
    }
    /^SF:/ {
      mapped = map_sf(substr($0, 4))
      if (mapped != "") {
        print "SF:" mapped
        in_block = 1
      } else {
        in_block = 0
      }
      next
    }
    /^end_of_record/ {
      if (in_block) {
        print
      }
      in_block = 0
      next
    }
    {
      if (in_block) {
        print
      }
    }
  ' "$input" >"$output"
}

filter_lcov "$CPP_RAW" "$CPP_FILTERED" "cpp"
filter_lcov "$SHIM_RAW" "$SHIM_FILTERED" "rs"

cat "$CPP_FILTERED" "$SHIM_FILTERED" >"$OUTPUT_TMP"

"$REPO_ROOT/scripts/ci/validate-lcov.sh" "$OUTPUT_TMP"
mv "$OUTPUT_TMP" "$OUTPUT"
