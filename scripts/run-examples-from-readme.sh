#!/bin/bash

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

# Unified script to run SDK examples from README.md files.
# Usage: ./scripts/run-examples-from-readme.sh [OPTIONS]
#
#   --language LANG   Language to test: rust|go|node|python|java|csharp (default: all)
#   --target TARGET   Cargo target architecture for the server binary
#   --skip-tls        Skip TLS example tests
#
# The script sources shared utilities from scripts/utils.sh, starts the iggy
# server (built from source), parses example commands from each language's
# README.md, and executes them. Non-TLS examples run first; then the server
# is restarted with TLS for TLS-specific examples.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/utils.sh
source "${SCRIPT_DIR}/utils.sh"

ROOT_WORKDIR="$(pwd)"

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
LANGUAGE="all"
TARGET=""
SKIP_TLS=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --language)   LANGUAGE="$2";  shift 2 ;;
        --target)     TARGET="$2";    shift 2 ;;
        --skip-tls)   SKIP_TLS=true;  shift   ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--language LANG] [--target TARGET] [--skip-tls]"
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Run a full non-TLS + TLS cycle for one language.
# Arguments:
#   $1 - language label (for logging)
#   $2 - working directory (relative to repo root, or "." for root)
#   $3 - space-separated list of README files (relative to workdir)
#   $4 - grep pattern for non-TLS commands
#   $5 - grep exclude pattern for non-TLS commands (empty = no exclude)
#   $6 - grep pattern for TLS commands (empty = no TLS examples)
#   $7 - per-command timeout in seconds (0 = no timeout)
#   $8 - extra server args (e.g. "--fresh")
#   $9 - optional pre-flight callback function name (run before non-TLS examples, with server already started)
# shellcheck disable=SC2329
run_language_examples() {
    local lang="$1"
    local workdir="$2"
    local readme_files="$3"
    local grep_pattern="$4"
    local grep_exclude="$5"
    local tls_grep_pattern="$6"
    local cmd_timeout="$7"
    local server_extra_args="$8"
    local preflight_fn="${9:-}"

    echo ""
    echo "============================================================"
    echo "  Running ${lang} examples"
    echo "============================================================"
    echo ""

    EXAMPLES_EXIT_CODE=0

    # --- Non-TLS pass ---
    cleanup_server_state
    # shellcheck disable=SC2086
    start_plain_server ${server_extra_args}
    wait_for_server_ready "${lang}"

    # Run optional pre-flight callback (e.g. CLI commands from root README)
    if [ -n "${preflight_fn}" ] && declare -f "${preflight_fn}" >/dev/null 2>&1; then
        ${preflight_fn}
        if [ "${EXAMPLES_EXIT_CODE}" -ne 0 ]; then
            cd "${ROOT_WORKDIR}"
            stop_server
            report_result "${EXAMPLES_EXIT_CODE}"
            return "${EXAMPLES_EXIT_CODE}"
        fi
    fi

    if [ "${workdir}" != "." ]; then
        cd "${workdir}"
    fi

    for readme in ${readme_files}; do
        if [ "${EXAMPLES_EXIT_CODE}" -ne 0 ]; then
            break
        fi
        run_readme_commands "${readme}" "${grep_pattern}" "${cmd_timeout}" "${grep_exclude}"
    done

    cd "${ROOT_WORKDIR}"
    stop_server

    # --- TLS pass ---
    if [ "${EXAMPLES_EXIT_CODE}" -eq 0 ] && [ "${SKIP_TLS}" = false ] && [ -n "${tls_grep_pattern}" ]; then
        local has_tls=false
        for readme in ${readme_files}; do
            local readme_path="${readme}"
            if [ "${workdir}" != "." ]; then
                readme_path="${workdir}/${readme}"
            fi
            if [ -f "${readme_path}" ] && grep -qE "${tls_grep_pattern}" "${readme_path}" 2>/dev/null; then
                has_tls=true
                break
            fi
        done

        if [ "${has_tls}" = true ]; then
            echo ""
            echo "=== Running ${lang} TLS examples ==="
            echo ""

            cleanup_server_state
            # shellcheck disable=SC2086
            start_tls_server ${server_extra_args}
            wait_for_server_ready "${lang} TLS"

            if [ "${workdir}" != "." ]; then
                cd "${workdir}"
            fi

            for readme in ${readme_files}; do
                if [ "${EXAMPLES_EXIT_CODE}" -ne 0 ]; then
                    break
                fi
                run_readme_commands "${readme}" "${tls_grep_pattern}" "${cmd_timeout}"
            done

            cd "${ROOT_WORKDIR}"
            stop_server
        fi
    fi

    report_result "${EXAMPLES_EXIT_CODE}"
    return "${EXAMPLES_EXIT_CODE}"
}

# ---------------------------------------------------------------------------
# Per-language runners
# ---------------------------------------------------------------------------

# shellcheck disable=SC2329
run_rust_examples() {
    resolve_server_binary "${TARGET}"
    resolve_cli_binary "${TARGET}"

    if [ -n "${TARGET}" ]; then
        TRANSFORM_COMMAND() {
            echo "$1" | sed "s|cargo r |cargo r --target ${TARGET} |g" | sed "s|cargo run |cargo run --target ${TARGET} |g"
        }
    else
        unset -f TRANSFORM_COMMAND 2>/dev/null || true
    fi

    # Pre-flight: run CLI commands from root README
    _rust_preflight() {
        run_readme_commands "README.md" '^\`cargo r --bin iggy -- '
    }

    run_language_examples \
        "Rust" \
        "." \
        "README.md examples/rust/README.md" \
        "^cargo run --example" \
        "tcp-tls" \
        "^cargo run --example.*tcp-tls" \
        0 \
        "" \
        "_rust_preflight"
}

# shellcheck disable=SC2329
run_node_examples() {
    resolve_server_binary "${TARGET}"

    export DEBUG=iggy:examples
    unset -f TRANSFORM_COMMAND 2>/dev/null || true

    run_language_examples \
        "Node.js" \
        "examples/node" \
        "README.md" \
        "^(npm run|tsx)" \
        "tcp-tls" \
        "^(npm run|tsx).*tcp-tls" \
        0 \
        ""
}

# shellcheck disable=SC2329
run_go_examples() {
    resolve_server_binary "${TARGET}"
    unset -f TRANSFORM_COMMAND 2>/dev/null || true

    run_language_examples \
        "Go" \
        "examples/go" \
        "README.md" \
        "^go run" \
        "" \
        "" \
        0 \
        ""
}

# shellcheck disable=SC2329
run_python_examples() {
    resolve_server_binary "${TARGET}"
    unset -f TRANSFORM_COMMAND 2>/dev/null || true

    echo ""
    echo "============================================================"
    echo "  Running Python examples"
    echo "============================================================"
    echo ""

    EXAMPLES_EXIT_CODE=0

    # --- Non-TLS pass ---
    cleanup_server_state
    start_plain_server --fresh
    wait_for_server_ready "Python"

    cd examples/python || exit 1
    echo "Syncing Python dependencies with uv..."
    uv sync --frozen

    run_readme_commands "README.md" "^uv run " 10

    cd "${ROOT_WORKDIR}"
    stop_server

    # --- TLS pass ---
    if [ "${EXAMPLES_EXIT_CODE}" -eq 0 ] && [ "${SKIP_TLS}" = false ]; then
        local tls_readme="examples/python/README.md"
        if [ -f "${tls_readme}" ] && grep -qE "^uv run.*tls" "${tls_readme}" 2>/dev/null; then
            echo ""
            echo "=== Running Python TLS examples ==="
            echo ""
            cleanup_server_state
            start_tls_server --fresh
            wait_for_server_ready "Python TLS"

            cd examples/python || exit 1
            run_readme_commands "README.md" "^uv run.*tls" 10
            cd "${ROOT_WORKDIR}"
            stop_server
        fi
    fi

    report_result "${EXAMPLES_EXIT_CODE}"
    return "${EXAMPLES_EXIT_CODE}"
}

# shellcheck disable=SC2329
run_java_examples() {
    resolve_server_binary "${TARGET}"
    unset -f TRANSFORM_COMMAND 2>/dev/null || true

    run_language_examples \
        "Java" \
        "examples/java" \
        "README.md" \
        '^\./gradlew' \
        "TcpTls" \
        '^\./gradlew.*TcpTls' \
        0 \
        ""
}

# shellcheck disable=SC2329
run_csharp_examples() {
    resolve_server_binary "${TARGET}"
    unset -f TRANSFORM_COMMAND 2>/dev/null || true

    # Pre-flight: run CLI commands from root README
    _csharp_preflight() {
        run_readme_commands "README.md" '^\`cargo r --bin iggy -- '
    }

    run_language_examples \
        "C#" \
        "." \
        "README.md examples/csharp/README.md" \
        "^dotnet run --project" \
        "TcpTls" \
        "^dotnet run --project.*TcpTls" \
        0 \
        "" \
        "_csharp_preflight"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

overall_exit_code=0

run_one() {
    local lang_fn="$1"
    local lang_name="$2"

    EXAMPLES_EXIT_CODE=0
    unset -f TRANSFORM_COMMAND 2>/dev/null || true

    set +e
    ${lang_fn}
    local rc=$?
    set -e

    unset -f TRANSFORM_COMMAND 2>/dev/null || true

    if [ ${rc} -ne 0 ]; then
        echo ""
        echo -e "\e[31m${lang_name} examples FAILED (exit code ${rc})\e[0m"
        overall_exit_code=1
    fi
    cd "${ROOT_WORKDIR}"
}

case "${LANGUAGE}" in
    rust)   run_one run_rust_examples   "Rust"   ;;
    node)   run_one run_node_examples   "Node"   ;;
    go)     run_one run_go_examples     "Go"     ;;
    python) run_one run_python_examples "Python" ;;
    java)   run_one run_java_examples   "Java"   ;;
    csharp) run_one run_csharp_examples "C#"     ;;
    all)
        for lang in rust node go python java csharp; do
            run_one "run_${lang}_examples" "${lang}"
        done
        ;;
    *)
        echo "Unknown language: ${LANGUAGE}"
        echo "Supported: rust, node, go, python, java, csharp, all"
        exit 1
        ;;
esac

exit "${overall_exit_code}"
