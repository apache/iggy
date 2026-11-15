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

# Builds a PGO or BOLT+PGO iggy-server with cargo-pgo. usage() explains what the
# script does, how it works, and what it needs.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OS="$(uname -s)"
readonly OS

# The nodes read core/server/config.toml, and the training directory is
# relative to the working directory.
cd "${REPO_ROOT}"

readonly SCRIPT_NAME="scripts/pgo.sh"
readonly USAGE_LINE="Usage: ${SCRIPT_NAME} [--bolt] [--target-cpu <cpu>]"
readonly DEFAULT_TARGET_CPU="native"
# Every run sends 1000-byte messages in batches of 250, so one batch holds
# 250 kB of messages.
readonly BENCH_MESSAGE_ARGS="--message-size 1000 --messages-per-batch 250"
# A persisted run also sets the journal flush threshold of its topics to one
# message, so the nodes flush after every batch.
readonly PERSISTED_ARGS="--durability persisted --messages-required-to-save 1"
# The bench clients turn off Nagle's algorithm, as clients that care about
# latency do.
readonly BENCH_TRANSPORT_ARGS="tcp --nodelay"
# All runs go to one cluster start, in this order. An entry holds the
# iggy-bench arguments of one run that go between BENCH_MESSAGE_ARGS and
# BENCH_TRANSPORT_ARGS: the options before the bench, the bench and its
# options.
#
# The runs cover 1, 5, 20 and 200 actors, one stream per actor and one stream
# with 20 partitions, a consumer group, actors that produce and consume, and
# both durabilities. The first two runs have no rate limit, so the nodes run
# at their limit. The other runs are paced. --rate-limit is the sum over all
# actors of a run, and it is low, so that a build host with few CPUs can hold
# it. A run that cannot hold its limit only takes longer.
# end-to-end-producing-consumer counts the sent and the received bytes, so its
# limit is twice as high.
#
# --message-batches counts per actor. A consumer run reads the streams of the
# producer run before it, and it never ends if it asks for more batches than
# that run wrote. The consumers of balanced-consumer-group share their
# 20 x 300 batches, which are the 5 x 1200 batches of balanced-producer.
#
# Durability is a topic option. A run that produces creates its streams with
# it, and the default is replicated. Such a run first deletes each of its
# streams that an earlier run left, and logs a warning about it.
readonly -a TRAINING_RUNS=(
    "--message-batches 1000 pinned-producer --producers 20 --streams 20"
    "--message-batches 1000 pinned-consumer --consumers 20 --streams 20"
    "--message-batches 300 --rate-limit 80MB pinned-producer --producers 20 --streams 20"
    "--message-batches 300 --rate-limit 80MB pinned-consumer --consumers 20 --streams 20"
    "--message-batches 300 --rate-limit 80MB ${PERSISTED_ARGS} pinned-producer --producers 20 --streams 20"
    "--message-batches 1200 --rate-limit 80MB balanced-producer --producers 5 --streams 1 --partitions 20"
    "--message-batches 300 --rate-limit 80MB balanced-consumer-group --consumers 20 --streams 1 --consumer-groups 1"
    "--message-batches 300 --rate-limit 160MB end-to-end-producing-consumer --producing-consumers 20 --streams 20"
    "--message-batches 2000 --rate-limit 50MB pinned-producer --producers 1 --streams 1"
    "--message-batches 2000 --rate-limit 50MB pinned-consumer --consumers 1 --streams 1"
    "--message-batches 2000 --rate-limit 50MB ${PERSISTED_ARGS} pinned-producer --producers 1 --streams 1"
    "--message-batches 30 --rate-limit 80MB pinned-producer --producers 200 --streams 200"
    "--message-batches 30 --rate-limit 80MB ${PERSISTED_ARGS} pinned-producer --producers 200 --streams 200"
)
readonly CLUSTER_NODES=3
# Shards per node, whatever the CPU count of the host. With more than one shard
# a node forwards requests between its shards, so that path gets a profile.
readonly SHARDS_PER_NODE=4
# The nodes keep their data in node-<n>/ and their output in node-<n>.log here.
# The script deletes only this directory, so the local_data/ of a developer
# server stays.
readonly TRAINING_DATA_DIR="local_data_pgo"
# Peak disk use of one node, rounded up: the first run writes 5 GB of
# messages, which take about 5.3 GB on disk. No later run holds more than
# 3 GB, because a run that produces deletes its streams before it writes.
readonly NODE_DISK_GB=6
# Node <n> listens on LISTENER_HOST at <base port> + n. The ranges must not
# overlap, so the WebSocket ports start at 8100 and not at the default 8092.
readonly LISTENER_HOST="127.0.0.1"
readonly TCP_BASE_PORT=8090
readonly HTTP_BASE_PORT=3000
readonly WEBSOCKET_BASE_PORT=8100
readonly REPLICA_BASE_PORT=9090
readonly QUIC_BASE_PORT=8080
# Every TCP port of the cluster. QUIC uses UDP, which a connect probe cannot see.
LISTENER_TCP_PORTS=()
for base_port in "${TCP_BASE_PORT}" "${HTTP_BASE_PORT}" "${WEBSOCKET_BASE_PORT}" "${REPLICA_BASE_PORT}"; do
    for ((node = 0; node < CLUSTER_NODES; node++)); do
        LISTENER_TCP_PORTS+=("$((base_port + node))")
    done
done
readonly -a LISTENER_TCP_PORTS
# The node output lines that wait_for_cluster_ready in
# core/integration/src/harness/orchestrator/harness.rs waits for. Its comments
# explain why the mesh must be complete on every node before the first client
# operation.
readonly MESH_MARKER="replica mesh complete"
readonly ADMISSION_MARKER="partition admission opened"
# Time limits of the node waits. SECONDS counts whole seconds, and >= can end a
# wait up to 1 s early, so the waits compare with >.
# Each marker must appear on every node within this time.
readonly NODE_READY_TIMEOUT_SECONDS=60
# When the script stops early, the nodes get this long after SIGTERM before
# SIGKILL, as stop_server in scripts/utils.sh gives one server.
readonly NODE_STOP_TIMEOUT_SECONDS=5
# preflight refuses these, also when empty. cargo takes rustflags from the first
# set source (CARGO_ENCODED_RUSTFLAGS, RUSTFLAGS, then the target.* config
# entries), and cargo-pgo appends its flags to a set RUSTFLAGS, so both drop
# the --config target CPU entry. cargo-pgo never reads CARGO_ENCODED_RUSTFLAGS,
# so that one also drops the profile flags. LLVM_PROFILE_FILE moves the PGO
# profile out of the cargo-pgo directory.
readonly -a REFUSED_ENV_VARS=(RUSTFLAGS CARGO_ENCODED_RUSTFLAGS LLVM_PROFILE_FILE)

# Links and versions surfaced by preflight when a check fails.
readonly CARGO_PGO_URL="https://github.com/kobzol/cargo-pgo"
readonly CARGO_PGO_VERSION="0.3.0"  # script is coupled to the 0.3.x CLI surface
readonly CARGO_PGO_INSTALL="cargo install --locked cargo-pgo@${CARGO_PGO_VERSION}"
readonly MIN_RUSTC_VERSION="1.100"
# rust-lang/rust#163616 was fixed on master on 2026-10-15 and on beta on
# 2026-10-16, so a beta or nightly toolchain needs a later commit date.
readonly MIN_PRERELEASE_COMMIT_DATE="2026-10-17"
readonly RUSTC_PGO_BUG_URL="https://github.com/rust-lang/rust/issues/163616"
readonly RUSTUP_INSTALL_URL="https://rustup.rs"
readonly JQ_URL="https://github.com/jqlang/jq"
readonly LLVM_TOOLS_DOC_URL="https://doc.rust-lang.org/rustc/profile-guided-optimization.html"
readonly BOLT_REPO_URL="https://github.com/llvm/llvm-project/tree/main/bolt"

readonly OK_TAG="[ok]"
readonly FAIL_TAG="[FAIL]"

DO_BOLT=0
TARGET_CPU="${DEFAULT_TARGET_CPU}"
# PIDs of the training nodes, indexed by node. The EXIT trap of train() stops
# the ones that still run.
NODE_PIDS=()

# iggy-bench exits 0 on Ctrl-C, so without these traps the script would go on
# and optimize with a truncated profile. bash runs a trap after the foreground
# command ends, and the exit codes follow the 128 + signal number convention.
# train() arms the EXIT trap that stops the nodes.
trap 'exit 130' INT
trap 'exit 143' TERM

usage() {
    local run training_runs=""
    for run in "${TRAINING_RUNS[@]}"; do
        training_runs+=$'\n'"       ${run}"
    done
    cat <<EOF
${USAGE_LINE}

Builds an optimized iggy-server with profile-guided optimization (PGO).
PGO builds the server twice. The first build is instrumented: it counts which
code runs and writes the counts to a profile when it exits. A training
workload runs against it. The second build uses the profile to optimize the
code that runs most. With --bolt, LLVM BOLT then reorders the machine code of
the PGO binary with a second profile from a second training run.

cargo-pgo runs the build steps. This script adds the prerequisite checks and
the training workload. Every build passes --locked, so it stops when Cargo.lock
does not match Cargo.toml.

The training runs a ${CLUSTER_NODES}-node cluster on this host with plain TCP clients,
an empty data directory, and replicated and persisted durability. The
optimizer treats code that the training does not run as cold, for example the
QUIC, WebSocket, HTTP and TLS paths, a server without cluster mode, failover
(view changes, repair, state transfer) and the recovery of existing data at
startup. That code can run slower than in a normal release build. PGO and
--target-cpu do not reach C code that cc-rs builds, for example the mimalloc
allocator.

Options:
  --bolt              Also apply LLVM BOLT to the PGO binary. Linux only.
  --target-cpu <cpu>  rustc target CPU for every iggy-server build
                      (default: ${DEFAULT_TARGET_CPU}). The binaries need a CPU with the
                      same instruction set extensions. List the names:
                      rustc --print target-cpus
  -h, --help          Show this help.

How it works:
  1. Checks the prerequisites in the first list below and reports every
     failed check together.
  2. Builds iggy-bench, the load generator, without instrumentation.
  3. Builds the instrumented iggy-server (cargo pgo build).
  4. Runs the training. It checks the TCP ports again, deletes ${TRAINING_DATA_DIR}/
     in the repository root and starts ${CLUSTER_NODES} nodes of the instrumented server
     on ${LISTENER_HOST}, as root user iggy/iggy. Node <n> keeps its data in
     ${TRAINING_DATA_DIR}/node-<n>/ and its output in ${TRAINING_DATA_DIR}/node-<n>.log.
     It listens on TCP ${TCP_BASE_PORT}+<n>, QUIC ${QUIC_BASE_PORT}+<n>, HTTP ${HTTP_BASE_PORT}+<n>,
     WebSocket ${WEBSOCKET_BASE_PORT}+<n> and replica TCP ${REPLICA_BASE_PORT}+<n>. Each node runs
     ${SHARDS_PER_NODE} unpinned shards. When every node logged that its replica mesh
     is complete and then that partition admission is open, the script runs
     iggy-bench ${BENCH_MESSAGE_ARGS} <run> ${BENCH_TRANSPORT_ARGS}
     against node 0, once for each <run>, in order:${training_runs}
     --message-batches counts per actor, and --rate-limit is the sum over all
     actors of a run. The first two runs have no rate limit, so the nodes run
     at their limit. A run without --durability uses replicated topics. A
     consumer run reads the streams of the producer run before it. The client
     sends each partition request to the primary node of that partition.
     Last, it stops every node with SIGTERM and waits for the exits, because
     a node writes its profile only on a clean exit. The server limits its
     shutdown time with the shutdown_* settings in [sharding]. If the exit
     status of a node is not 0, the script stops and keeps ${TRAINING_DATA_DIR}/
     for debugging. Otherwise it prints the LLVM profile runtime messages from
     the node output and deletes ${TRAINING_DATA_DIR}/.
  5. Without --bolt, builds the PGO binary (cargo pgo optimize).
     With --bolt, builds the PGO binary and a BOLT-instrumented copy of it
     (cargo pgo bolt build --with-pgo). Then it trains the copy as in step 4
     and builds the BOLT+PGO binary (cargo pgo bolt optimize --with-pgo),
     which builds the PGO binary again before BOLT runs.
  6. Prints the binary paths.

You can run the script from any directory, because it changes to the
repository root first. The nodes read core/server/config.toml, or the file
that IGGY_CONFIG_PATH names, and the IGGY_* variables. They also read
variables from the file that IGGY_ENV_PATH names, or else from the first .env
file in the repository root or a parent directory. The script sets the cluster
roster, IGGY_PATH, the listener addresses, the shard settings and the info log
level of every node, and it unsets RUST_LOG for them, so none of these
sources can change those settings. The configuration must list at most
${CLUSTER_NODES} cluster.nodes entries, because the script starts only ${CLUSTER_NODES} nodes.

Output, where <target-dir> is the cargo target directory (target/ by default):
  PGO:       <target-dir>/<host-triple>/release/iggy-server
  BOLT+PGO:  <target-dir>/<host-triple>/release/iggy-server-bolt-optimized
  Profiles:  <target-dir>/pgo-profiles/ and <target-dir>/bolt-profiles/
Step 3 deletes the old PGO profiles, and step 5 with --bolt deletes the old
BOLT profiles. Until step 5 ends, the PGO path holds the instrumented server,
so use a binary only after the script printed its path. The binaries have the
default cargo features only, so they lack, for example, the systemd feature
that a Type=notify unit needs (see core/server/README.md).

Prerequisites that step 1 checks:
  - rustup and rustc ${MIN_RUSTC_VERSION} or later. rustc 1.99 miscompiles the PGO counters:
    ${RUSTC_PGO_BUG_URL}
    A beta or nightly toolchain also needs a commit date of ${MIN_PRERELEASE_COMMIT_DATE} or
    later.
  - llvm-profdata of that toolchain. Step 1 prints the install command:
    rustup component add llvm-tools-preview --toolchain <toolchain>
  - cargo-pgo 0.3.x (${CARGO_PGO_URL}):
    ${CARGO_PGO_INSTALL}
  - jq, to read the target directory from cargo metadata.
  - For --bolt: Linux, because BOLT needs ELF binaries, and llvm-bolt and
    merge-fdata on PATH.
  - These variables unset, also as empty values:
    ${REFUSED_ENV_VARS[*]}
    Each one changes what the build or the training writes, or where.
  - No listener on these ${LISTENER_HOST} TCP ports, because the nodes listen
    on them and the benches must reach them:
    ${LISTENER_TCP_PORTS[*]}

Prerequisites that no step checks:
  - Free UDP ports ${QUIC_BASE_PORT} to $((QUIC_BASE_PORT + CLUSTER_NODES - 1)) for QUIC.
  - A host CPU that runs --target-cpu code, because training runs here.
  - At least ${SHARDS_PER_NODE} CPUs, because each node runs ${SHARDS_PER_NODE} shards and the server
    refuses more shards than CPUs.
  - About $((NODE_DISK_GB * CLUSTER_NODES)) GB of free disk for ${TRAINING_DATA_DIR}/, ${NODE_DISK_GB} GB per node, because
    every node stores every message.
  - One run of this script per checkout at a time. A second run replaces the
    binaries and profiles of the first one.
  - Linux. Without --bolt, macOS can work, but that path is not tested.

Exit status: 0 on success, 130 after SIGINT (Ctrl-C), 143 after SIGTERM, and
another non-zero value on any other failure. After a signal, the script stops
when the current command ends. In a terminal, Ctrl-C also stops that command.

Examples:
  ${SCRIPT_NAME}
  ${SCRIPT_NAME} --bolt
  ${SCRIPT_NAME} --target-cpu icelake-server
EOF
}

usage_error() {
    echo "ERROR: $1" >&2
    echo "${USAGE_LINE}" >&2
    echo "Run ${SCRIPT_NAME} --help for the details." >&2
    exit 1
}

parse_args() {
    while (($#)); do
        case "$1" in
            --bolt) DO_BOLT=1 ;;
            --target-cpu)
                (($# >= 2)) || usage_error "--target-cpu needs a value"
                TARGET_CPU="$2"
                shift
                ;;
            -h | --help)
                usage
                exit 0
                ;;
            *) usage_error "unknown argument: $1" ;;
        esac
        shift
    done
}

# Report a PATH tool as present (with its location) or missing (with a hint).
# Returns non-zero when missing so preflight can collect every gap in one pass.
check_required() {
    local tool="$1" install_hint="$2" resolved
    if resolved="$(command -v "${tool}")"; then
        echo "  ${OK_TAG}   ${tool} -> ${resolved}"
        return 0
    fi
    echo "  ${FAIL_TAG} ${tool} - install: ${install_hint}" >&2
    return 1
}

# rustc 1.99 miscompiles the PGO counters. A beta or nightly of 1.100 or later
# passes the version test before the fix reached it, so a pre-release also
# needs a commit date from after the fix.
check_rustc_version() {
    local verbose release commit_date
    verbose="$(rustc -vV)"
    release="$(sed -n 's/^release: //p' <<<"${verbose}")"
    commit_date="$(sed -n 's/^commit-date: //p' <<<"${verbose}")"
    # sort -C succeeds only when the minimum sorts first, so version >= minimum.
    if printf '%s\n' "${MIN_RUSTC_VERSION}" "${release%%-*}" | sort -C -V &&
        [[ "${release}" != *-* || ! "${commit_date}" < "${MIN_PRERELEASE_COMMIT_DATE}" ]]; then
        echo "  ${OK_TAG}   rustc ${release} (${commit_date})"
        return 0
    fi
    echo "  ${FAIL_TAG} rustc ${release} (${commit_date}) - PGO needs ${MIN_RUSTC_VERSION} or later, and a beta or nightly from ${MIN_PRERELEASE_COMMIT_DATE} or later (${RUSTC_PGO_BUG_URL})" >&2
    echo "         use the pinned toolchain, or update the toolchain that overrides it" >&2
    return 1
}

# rustc only warns about an unknown target CPU and then builds for a generic one.
check_target_cpu() {
    if rustc --print target-cpus | awk -v cpu="${TARGET_CPU}" 'NR > 1 && $1 == cpu { found = 1 } END { exit !found }'; then
        echo "  ${OK_TAG}   target CPU ${TARGET_CPU}"
        return 0
    fi
    echo "  ${FAIL_TAG} target CPU ${TARGET_CPU} - unknown to rustc; list: rustc --print target-cpus" >&2
    return 1
}

# llvm-profdata (used by cargo-pgo) MUST match the rustc-bundled LLVM: a
# different major version can misread .profraw. It ships in llvm-tools-preview,
# in the toolchain's lib-sibling bin dir, not on PATH.
check_llvm_profdata() {
    local libdir toolchain
    if ! libdir="$(rustc --print target-libdir 2>/dev/null)"; then
        echo "  ${FAIL_TAG} llvm-profdata - cannot resolve toolchain libdir (install rustc first)" >&2
        return 1
    fi
    if [[ -x "${libdir%/lib}/bin/llvm-profdata" ]]; then
        echo "  ${OK_TAG}   llvm-profdata -> ${libdir%/lib}/bin/llvm-profdata"
        return 0
    fi
    # rustup names the sysroot directory after the toolchain. Without
    # --toolchain, rustup installs into the toolchain of the current directory,
    # which is the pinned one in this repository.
    toolchain="$(basename "$(rustc --print sysroot)")"
    echo "  ${FAIL_TAG} llvm-profdata (toolchain-matched, version-locked) - run:" >&2
    echo "         rustup component add llvm-tools-preview --toolchain ${toolchain}" >&2
    echo "         docs: ${LLVM_TOOLS_DOC_URL}" >&2
    return 1
}

# The script is coupled to the cargo-pgo 0.3.x CLI (subcommands, --with-pgo,
# output suffixes). cargo-pgo is pre-1.0, so another series may rename these.
check_cargo_pgo_series() {
    local ver
    ver="$(cargo-pgo --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)"
    if [[ "${ver}" == 0.3.* ]]; then
        echo "  ${OK_TAG}   cargo-pgo version ${ver}"
        return 0
    fi
    echo "  ${FAIL_TAG} cargo-pgo ${ver:-unknown} - this script targets 0.3.x; install: ${CARGO_PGO_INSTALL}" >&2
    return 1
}

# The training nodes listen on these ports. Another listener makes a node fail
# after the build, or takes the bench connections in its place.
check_ports_free() {
    local port busy=0
    for port in "${LISTENER_TCP_PORTS[@]}"; do
        if (exec 3<>"/dev/tcp/${LISTENER_HOST}/${port}") 2>/dev/null; then
            echo "  ${FAIL_TAG} ${LISTENER_HOST}:${port} - another process listens on it; stop it first" >&2
            busy=1
        fi
    done
    if ((busy == 0)); then
        echo "  ${OK_TAG}   TCP ports ${LISTENER_TCP_PORTS[*]} are free"
    fi
    return "${busy}"
}

# Check every prerequisite that a script can check up front. Gaps are collected
# and reported together (one fix-and-rerun cycle, not one per tool).
preflight() {
    echo "=== Preflight (bolt=${DO_BOLT} target-cpu=${TARGET_CPU}) ==="
    local failed=0 var bolt_hint="package llvm-bolt (Debian, Ubuntu, Arch), or ${BOLT_REPO_URL}"
    for var in "${REFUSED_ENV_VARS[@]}"; do
        if [[ -n "${!var+set}" ]]; then
            echo "  ${FAIL_TAG} ${var} is set - unset it, also when it is empty (see --help)" >&2
            failed=1
        fi
    done
    check_required cargo "${RUSTUP_INSTALL_URL}" || failed=1
    if check_required rustc "${RUSTUP_INSTALL_URL}"; then
        check_rustc_version || failed=1
        check_target_cpu || failed=1
    else
        failed=1
    fi
    check_required rustup "${RUSTUP_INSTALL_URL}" || failed=1
    check_required jq "${JQ_URL}" || failed=1
    if check_required cargo-pgo "${CARGO_PGO_INSTALL}  (${CARGO_PGO_URL})"; then
        check_cargo_pgo_series || failed=1
    else
        failed=1
    fi
    check_llvm_profdata || failed=1
    if ((DO_BOLT)); then
        if [[ "${OS}" == "Linux" ]]; then
            check_required llvm-bolt "${bolt_hint}" || failed=1
            check_required merge-fdata "${bolt_hint}" || failed=1
        else
            echo "  ${FAIL_TAG} --bolt - BOLT needs Linux and ELF binaries, this host runs ${OS}" >&2
            failed=1
        fi
    fi
    check_ports_free || failed=1
    if ((failed)); then
        echo "Preflight failed: resolve the items marked ${FAIL_TAG} above, then re-run." >&2
        exit 1
    fi
    echo "=== Preflight OK ==="
}

# cargo-pgo passes --target <host>, so its builds land in <target-dir>/<host>/release,
# while the plain iggy-bench build lands in <target-dir>/release.
resolve_paths_and_flags() {
    local target_dir host_triple
    target_dir="$(cargo metadata --format-version 1 --no-deps | jq -r .target_directory)"
    host_triple="$(rustc -vV | sed -n 's/^host: //p')"
    readonly BENCH_BIN="${target_dir}/release/iggy-bench"
    readonly PGO_BIN="${target_dir}/${host_triple}/release/iggy-server" # instrumented, then optimized in place
    readonly BOLT_INSTR_BIN="${PGO_BIN}-bolt-instrumented"
    readonly BOLT_OPT_BIN="${PGO_BIN}-bolt-optimized"
    readonly BOLT_PROFILE_DIR="${target_dir}/bolt-profiles/iggy-server"
    # -Ctarget-cpu goes through `cargo --config`, not RUSTFLAGS: cargo joins every
    # matching target.*.rustflags entry, so this one merges with cargo-pgo's own
    # entry and with any from a .cargo/config.toml. Every server build takes
    # these arguments, so the function hashes in the profile match.
    readonly -a SERVER_BUILD_ARGS=(--locked --bin iggy-server
        --config "target.${host_triple}.rustflags=['-C', 'target-cpu=${TARGET_CPU}']")
}

# The roster and the settings that every node gets, in the
# IGGY_CLUSTER_NODES_<n>_* form that the integration harness uses. Unpinned
# shards let the nodes and the benches share the cores, instead of every node
# pinning its shards to the same ones.
resolve_cluster_env() {
    local node
    CLUSTER_ENV=(IGGY_CLUSTER_ENABLED=true IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy
        IGGY_LOGGING_LEVEL=info "IGGY_SHARDING_CPU_ALLOCATION=${SHARDS_PER_NODE}" IGGY_SHARDING_PIN_CORES=false)
    for ((node = 0; node < CLUSTER_NODES; node++)); do
        CLUSTER_ENV+=(
            "IGGY_CLUSTER_NODES_${node}_NAME=node-${node}"
            "IGGY_CLUSTER_NODES_${node}_IP=${LISTENER_HOST}"
            "IGGY_CLUSTER_NODES_${node}_REPLICA_ID=${node}"
            "IGGY_CLUSTER_NODES_${node}_PORTS_TCP=$((TCP_BASE_PORT + node))"
            "IGGY_CLUSTER_NODES_${node}_PORTS_QUIC=$((QUIC_BASE_PORT + node))"
            "IGGY_CLUSTER_NODES_${node}_PORTS_HTTP=$((HTTP_BASE_PORT + node))"
            "IGGY_CLUSTER_NODES_${node}_PORTS_WEBSOCKET=$((WEBSOCKET_BASE_PORT + node))"
            "IGGY_CLUSTER_NODES_${node}_PORTS_TCP_REPLICA=$((REPLICA_BASE_PORT + node))"
        )
    done
    readonly -a CLUSTER_ENV
}

# Usage: start_node <server binary> <node>
start_node() {
    local server_bin="$1" node="$2"
    # The readiness markers are info lines, and the server lets RUST_LOG win
    # over IGGY_LOGGING_LEVEL.
    env -u RUST_LOG "${CLUSTER_ENV[@]}" \
        "IGGY_PATH=${TRAINING_DATA_DIR}/node-${node}" \
        "IGGY_TCP_ADDRESS=${LISTENER_HOST}:$((TCP_BASE_PORT + node))" \
        "IGGY_QUIC_ADDRESS=${LISTENER_HOST}:$((QUIC_BASE_PORT + node))" \
        "IGGY_HTTP_ADDRESS=${LISTENER_HOST}:$((HTTP_BASE_PORT + node))" \
        "IGGY_WEBSOCKET_ADDRESS=${LISTENER_HOST}:$((WEBSOCKET_BASE_PORT + node))" \
        "${server_bin}" --replica-id "${node}" &>"${TRAINING_DATA_DIR}/node-${node}.log" &
    NODE_PIDS+=("$!")
}

# Usage: wait_for_nodes <label> <marker>
# Waits until the output of every node has the marker. A node exit or the
# timeout stops the script.
wait_for_nodes() {
    local label="$1" marker="$2" node log deadline=$((SECONDS + NODE_READY_TIMEOUT_SECONDS))
    echo ">>> [${label}] waiting for \"${marker}\" on every node"
    for node in "${!NODE_PIDS[@]}"; do
        log="${TRAINING_DATA_DIR}/node-${node}.log"
        until grep -qsF "${marker}" "${log}"; do
            if ! kill -0 "${NODE_PIDS[node]}" 2>/dev/null; then
                echo "ERROR: [${label}] node ${node} exited before it logged \"${marker}\". See ${REPO_ROOT}/${log}." >&2
                exit 1
            fi
            if ((SECONDS > deadline)); then
                echo "ERROR: [${label}] node ${node} did not log \"${marker}\" within ${NODE_READY_TIMEOUT_SECONDS} s. See ${REPO_ROOT}/${log}." >&2
                exit 1
            fi
            sleep 1
        done
    done
}

# EXIT trap of train(): stops every node that still runs. All nodes share one
# deadline, so the wait does not grow with the node count.
stop_nodes() {
    local pid deadline=$((SECONDS + NODE_STOP_TIMEOUT_SECONDS))
    kill -TERM "${NODE_PIDS[@]}" 2>/dev/null || true
    for pid in "${NODE_PIDS[@]}"; do
        while kill -0 "${pid}" 2>/dev/null; do
            if ((SECONDS > deadline)); then
                echo "Node PID ${pid} did not exit ${NODE_STOP_TIMEOUT_SECONDS} s after SIGTERM, sending SIGKILL" >&2
                kill -KILL "${pid}" 2>/dev/null || true
                break
            fi
            sleep 0.1
        done
    done
}

# Run every training bench against one cluster start, then stop every node with
# SIGTERM: an instrumented node writes its profile only on a clean exit.
train() {
    local server_bin="$1" label="$2" node run status failed=0
    local -a bench_args
    check_ports_free || exit 1
    rm -rf "${TRAINING_DATA_DIR}"
    mkdir -p "${TRAINING_DATA_DIR}"
    NODE_PIDS=()
    trap 'stop_nodes' EXIT
    for ((node = 0; node < CLUSTER_NODES; node++)); do
        start_node "${server_bin}" "${node}"
    done
    wait_for_nodes "${label}" "${MESH_MARKER}"
    wait_for_nodes "${label}" "${ADMISSION_MARKER}"
    for run in "${TRAINING_RUNS[@]}"; do
        read -ra bench_args <<<"${BENCH_MESSAGE_ARGS} ${run} ${BENCH_TRANSPORT_ARGS}"
        bench_args+=(--server-address "${LISTENER_HOST}:${TCP_BASE_PORT}")
        echo ">>> [${label}] iggy-bench ${bench_args[*]}"
        "${BENCH_BIN}" "${bench_args[@]}"
    done
    echo ">>> [${label}] SIGTERM every node for a clean exit that writes its profile"
    # No deadline: the server bounds its own shutdown with its sharding
    # timeouts and exits 1 when a shard misses them. A SIGKILL leaves no profile
    # or a cut one, and cargo pgo bolt optimize accepts a cut one.
    kill -TERM "${NODE_PIDS[@]}" 2>/dev/null || true
    for node in "${!NODE_PIDS[@]}"; do
        status=0
        wait "${NODE_PIDS[node]}" || status=$?
        if ((status != 0)); then
            echo "ERROR: [${label}] node ${node} exited with status ${status}, not 0. See ${REPO_ROOT}/${TRAINING_DATA_DIR}/node-${node}.log." >&2
            failed=1
        fi
    done
    trap - EXIT
    ((failed == 0)) || exit 1
    # The profile runtime reports write errors and dropped counters only in the
    # node output.
    grep -H 'LLVM Profile' "${TRAINING_DATA_DIR}"/node-*.log >&2 || true
    rm -rf "${TRAINING_DATA_DIR}"
}

# Each output path comes from the target directory and the cargo-pgo layout, so
# a wrong path stops the script at the step that built the binary.
require_executable() {
    local path="$1" label="$2"
    if [[ ! -x "${path}" ]]; then
        echo "ERROR: ${label} missing at ${path}" >&2
        exit 1
    fi
}

# Uninstrumented load driver. cargo-pgo only builds the server; the bench must
# stay free of profile instrumentation so it does not pollute the profile dir.
build_bench() {
    echo "=== Building uninstrumented iggy-bench (load driver) ==="
    cargo build --locked --release --bin iggy-bench
    require_executable "${BENCH_BIN}" "bench binary"
}

pgo_instrument() {
    echo "=== PGO: instrument build (cargo pgo build) ==="
    cargo pgo build -- "${SERVER_BUILD_ARGS[@]}"
    require_executable "${PGO_BIN}" "instrumented server"
}

pgo_optimize() {
    echo "=== PGO: optimize build (cargo pgo optimize) ==="
    cargo pgo optimize build -- "${SERVER_BUILD_ARGS[@]}"
}

# --with-pgo merges the PGO profile and builds the PGO-optimized binary as the
# BOLT input (the cargo-pgo README flow), so `cargo pgo optimize` does not run
# first. `bolt build` clears only <target-dir>/bolt-profiles, so the PGO
# profile survives the whole BOLT phase.
bolt_instrument() {
    echo "=== BOLT: instrument build (cargo pgo bolt build --with-pgo) ==="
    cargo pgo bolt build --with-pgo -- "${SERVER_BUILD_ARGS[@]}"
    require_executable "${BOLT_INSTR_BIN}" "BOLT-instrumented server"
}

# `bolt optimize` only warns when the BOLT profile is missing and still writes
# the binary. The PGO steps fail on their own without a PGO profile.
bolt_optimize() {
    local profiles=("${BOLT_PROFILE_DIR}"/*.fdata)
    if [[ ! -e "${profiles[0]}" ]]; then
        echo "ERROR: BOLT training wrote no profile to ${BOLT_PROFILE_DIR}" >&2
        exit 1
    fi
    echo "=== BOLT: optimize (cargo pgo bolt optimize --with-pgo) ==="
    cargo pgo bolt optimize --with-pgo -- "${SERVER_BUILD_ARGS[@]}"
    require_executable "${BOLT_OPT_BIN}" "BOLT-optimized server"
}

report() {
    echo
    echo "=== Done ==="
    echo "PGO-optimized binary: ${PGO_BIN} ($(du -h "${PGO_BIN}" | cut -f1))"
    if ((DO_BOLT)); then
        echo "BOLT+PGO binary (final deliverable): ${BOLT_OPT_BIN} ($(du -h "${BOLT_OPT_BIN}" | cut -f1))"
    fi
    echo "NOTE: built with -Ctarget-cpu=${TARGET_CPU}; CPUs without its instruction set extensions cannot run it."
}

main() {
    parse_args "$@"
    preflight
    resolve_paths_and_flags
    resolve_cluster_env
    build_bench

    pgo_instrument
    train "${PGO_BIN}" "PGO-instrumented"
    if ((DO_BOLT)); then
        bolt_instrument
        train "${BOLT_INSTR_BIN}" "BOLT-instrumented"
        bolt_optimize
    else
        pgo_optimize
    fi

    report
}

main "$@"
