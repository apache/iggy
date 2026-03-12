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
#   --goos GOOS       Go OS target
#   --goarch GOARCH   Go architecture target
#   --csharpos OS     C# --os flag
#   --csharparch ARCH C# --arch flag
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
GOOS=""
GOARCH=""
CSOS=""
CSARCH=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --language)   LANGUAGE="$2";  shift 2 ;;
        --target)     TARGET="$2";    shift 2 ;;
        --skip-tls)   SKIP_TLS=true;  shift   ;;
        --goos)       GOOS="$2";      shift 2 ;;
        --goarch)     GOARCH="$2";    shift 2 ;;
        --csharpos)   CSOS="$2";      shift 2 ;;
        --csharparch) CSARCH="$2";    shift 2 ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--language LANG] [--target TARGET] [--skip-tls] [--goos GOOS] [--goarch GOARCH] [--csharpos OS] [--csharparch ARCH]"
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
run_language_examples() {
    local lang="$1"
    local workdir="$2"
    local readme_files="$3"
    local grep_pattern="$4"
    local grep_exclude="$5"
    local tls_grep_pattern="$6"
    local cmd_timeout="$7"
    local server_extra_args="$8"

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

    if [ "${workdir}" != "." ]; then
        cd "${workdir}"
    fi

    for readme in ${readme_files}; do
        if [ "${EXAMPLES_EXIT_CODE}" -ne 0 ]; then
            break
        fi
        if [ -n "${grep_exclude}" ]; then
            # Build a combined pattern that filters out the exclude
            local filtered
            filtered=$(grep -E "${grep_pattern}" "${readme}" 2>/dev/null | grep -v "${grep_exclude}" || true)
            if [ -n "${filtered}" ]; then
                while IFS= read -r command; do
                    command=$(echo "${command}" | tr -d '`' | sed 's/^#.*//')
                    if [ -z "${command}" ]; then
                        continue
                    fi
                    if declare -f TRANSFORM_COMMAND >/dev/null 2>&1; then
                        command=$(TRANSFORM_COMMAND "${command}")
                    fi
                    echo -e "\e[33mChecking command from ${readme}:\e[0m ${command}"
                    echo ""
                    set +e
                    if [ "${cmd_timeout}" -gt 0 ] 2>/dev/null; then
                        eval "portable_timeout ${cmd_timeout} ${command}"
                        local test_exit_code=$?
                        if [[ ${test_exit_code} -ne 0 && ${test_exit_code} -ne 124 ]]; then
                            EXAMPLES_EXIT_CODE=${test_exit_code}
                        fi
                    else
                        eval "${command}"
                        EXAMPLES_EXIT_CODE=$?
                    fi
                    set -e
                    if [ "${EXAMPLES_EXIT_CODE}" -ne 0 ]; then
                        echo ""
                        echo -e "\e[31mCommand failed:\e[0m ${command}"
                        echo ""
                        break
                    fi
                    sleep 2
                done <<< "${filtered}"
            fi
        else
            run_readme_commands "${readme}" "${grep_pattern}" "${cmd_timeout}"
        fi
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

run_rust_examples() {
    resolve_server_binary "${TARGET}"
    resolve_cli_binary "${TARGET}"

    # Transform: inject --target flag when cross-compiling
    if [ -n "${TARGET}" ]; then
        TRANSFORM_COMMAND() {
            echo "$1" | sed "s|cargo r |cargo r --target ${TARGET} |g" | sed "s|cargo run |cargo run --target ${TARGET} |g"
        }
    else
        unset -f TRANSFORM_COMMAND 2>/dev/null || true
    fi

    # First run CLI commands from root README.md (these match `cargo r --bin iggy -- `)
    echo ""
    echo "============================================================"
    echo "  Running Rust CLI + example tests"
    echo "============================================================"
    echo ""

    EXAMPLES_EXIT_CODE=0
    cleanup_server_state
    start_plain_server
    wait_for_server_ready "Rust"

    # CLI commands from root README (abbreviated `cargo r`, not `cargo run`)
    run_readme_commands "README.md" '^\`cargo r --bin iggy -- '
    if [ "${EXAMPLES_EXIT_CODE}" -ne 0 ]; then
        cd "${ROOT_WORKDIR}"
        stop_server
        report_result "${EXAMPLES_EXIT_CODE}"
        return "${EXAMPLES_EXIT_CODE}"
    fi

    # Non-TLS example commands
    for readme in README.md examples/rust/README.md; do
        if [ "${EXAMPLES_EXIT_CODE}" -ne 0 ]; then
            break
        fi
        local filtered
        filtered=$(grep -E "^cargo run --example" "${readme}" 2>/dev/null | grep -v "tcp-tls" || true)
        if [ -n "${filtered}" ]; then
            while IFS= read -r command; do
                command=$(echo "${command}" | tr -d '`' | sed 's/^#.*//')
                if [ -z "${command}" ]; then continue; fi
                if declare -f TRANSFORM_COMMAND >/dev/null 2>&1; then
                    command=$(TRANSFORM_COMMAND "${command}")
                fi
                echo -e "\e[33mChecking example command from ${readme}:\e[0m ${command}"
                echo ""
                set +e
                eval "${command}"
                EXAMPLES_EXIT_CODE=$?
                set -e
                if [ "${EXAMPLES_EXIT_CODE}" -ne 0 ]; then
                    echo ""
                    echo -e "\e[31mExample command failed:\e[0m ${command}"
                    echo ""
                    break 2
                fi
                sleep 2
            done <<< "${filtered}"
        fi
    done

    stop_server

    # TLS pass
    if [ "${EXAMPLES_EXIT_CODE}" -eq 0 ] && [ "${SKIP_TLS}" = false ]; then
        local tls_readme="examples/rust/README.md"
        if [ -f "${tls_readme}" ] && grep -qE "^cargo run --example.*tcp-tls" "${tls_readme}"; then
            echo ""
            echo "=== Running Rust TLS examples ==="
            echo ""

            cleanup_server_state
            start_tls_server
            wait_for_server_ready "Rust TLS"

            run_readme_commands "${tls_readme}" "^cargo run --example.*tcp-tls"

            stop_server
        fi
    fi

    unset -f TRANSFORM_COMMAND 2>/dev/null || true
    report_result "${EXAMPLES_EXIT_CODE}"
    return "${EXAMPLES_EXIT_CODE}"
}

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

run_go_examples() {
    resolve_server_binary "${TARGET}"

    if [ -n "${GOOS}" ] || [ -n "${GOARCH}" ]; then
        TRANSFORM_COMMAND() {
            local cmd="$1"
            [ -n "${GOOS}" ] && cmd="GOOS=${GOOS} ${cmd}"
            [ -n "${GOARCH}" ] && cmd="GOARCH=${GOARCH} ${cmd}"
            echo "${cmd}"
        }
    else
        unset -f TRANSFORM_COMMAND 2>/dev/null || true
    fi

    run_language_examples \
        "Go" \
        "examples/go" \
        "README.md" \
        "^go run" \
        "" \
        "" \
        0 \
        ""

    unset -f TRANSFORM_COMMAND 2>/dev/null || true
}

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

run_csharp_examples() {
    resolve_server_binary "${TARGET}"

    if [ -n "${CSOS}" ] || [ -n "${CSARCH}" ]; then
        TRANSFORM_COMMAND() {
            local cmd="$1"
            [ -n "${CSOS}" ] && cmd="${cmd//dotnet run /dotnet run --os ${CSOS} }"
            [ -n "${CSARCH}" ] && cmd="${cmd//dotnet run /dotnet run --arch ${CSARCH} }"
            echo "${cmd}"
        }
    else
        unset -f TRANSFORM_COMMAND 2>/dev/null || true
    fi

    # C# also runs CLI commands from root README
    echo ""
    echo "============================================================"
    echo "  Running C# examples"
    echo "============================================================"
    echo ""

    EXAMPLES_EXIT_CODE=0
    cleanup_server_state
    start_plain_server
    wait_for_server_ready "C#"

    # CLI commands from root README (abbreviated `cargo r`, not `cargo run`)
    run_readme_commands "README.md" '^\`cargo r --bin iggy -- '
    if [ "${EXAMPLES_EXIT_CODE}" -ne 0 ]; then
        cd "${ROOT_WORKDIR}"
        stop_server
        report_result "${EXAMPLES_EXIT_CODE}"
        return "${EXAMPLES_EXIT_CODE}"
    fi

    # Non-TLS dotnet examples (run from repo root since paths include examples/csharp/...)
    for readme in README.md examples/csharp/README.md; do
        if [ "${EXAMPLES_EXIT_CODE}" -ne 0 ]; then
            break
        fi
        local filtered
        filtered=$(grep -E "^dotnet run --project" "${readme}" 2>/dev/null | grep -v "TcpTls" || true)
        if [ -n "${filtered}" ]; then
            while IFS= read -r command; do
                command=$(echo "${command}" | tr -d '`' | sed 's/^#.*//')
                if [ -z "${command}" ]; then continue; fi
                if declare -f TRANSFORM_COMMAND >/dev/null 2>&1; then
                    command=$(TRANSFORM_COMMAND "${command}")
                fi
                echo -e "\e[33mChecking example command from ${readme}:\e[0m ${command}"
                echo ""
                set +e
                eval "${command}"
                EXAMPLES_EXIT_CODE=$?
                set -e
                if [ "${EXAMPLES_EXIT_CODE}" -ne 0 ]; then
                    echo ""
                    echo -e "\e[31mExample command failed:\e[0m ${command}"
                    echo ""
                    break 2
                fi
                sleep 2
            done <<< "${filtered}"
        fi
    done

    stop_server

    # TLS pass
    if [ "${EXAMPLES_EXIT_CODE}" -eq 0 ] && [ "${SKIP_TLS}" = false ]; then
        local tls_readme="examples/csharp/README.md"
        if [ -f "${tls_readme}" ] && grep -qE "^dotnet run --project.*TcpTls" "${tls_readme}"; then
            echo ""
            echo "=== Running C# TLS examples ==="
            echo ""

            cleanup_server_state
            start_tls_server
            wait_for_server_ready "C# TLS"

            run_readme_commands "${tls_readme}" "^dotnet run --project.*TcpTls"

            stop_server
        fi
    fi

    unset -f TRANSFORM_COMMAND 2>/dev/null || true
    report_result "${EXAMPLES_EXIT_CODE}"
    return "${EXAMPLES_EXIT_CODE}"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

overall_exit_code=0

run_one() {
    local lang_fn="$1"
    local lang_name="$2"
    set +e
    ${lang_fn}
    local rc=$?
    set -e
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
