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

# Guided live demo of the Kafka gateway. Stock Kafka clients create a topic and write and read
# records through the gateway, and native Iggy clients read and write the same messages.
# README.md next to this file has the setup, the talk track and the recovery steps.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
BIN_DIR="${DEMO_BIN_DIR:-$REPO_ROOT/target/release}"
STATE_DIR="${DEMO_STATE_DIR:-${TMPDIR:-/tmp}/iggy-kafka-demo}"
IGGY_ADDR="127.0.0.1:${DEMO_IGGY_PORT:-8090}"
KAFKA_ADDR="127.0.0.1:${DEMO_KAFKA_PORT:-9093}"
TOPIC="${DEMO_TOPIC:-orders}"
STREAM="kafka"
ROOT_USER="iggy"
ROOT_PASSWORD="iggy"
KAFKA_IMAGE="apache/kafka:3.9.0"
KCAT_IMAGE="edenhill/kcat:1.7.1"
# Every tail container carries this label, so `down` finds them whichever terminal started them.
TAIL_LABEL="org.apache.iggy.kafka-demo=tail"
# `down` deletes the state directory only when this file is in it, so a mistyped
# DEMO_STATE_DIR can never point it at someone's real data.
STATE_MARKER=".iggy-kafka-demo"
RECORD_FORMAT='partition %p  offset %o  key %k  headers %h  value %s\n'
CLIENT_TIMEOUT="120s"
STEPS=7

# live waits for Enter before each command. check runs everything unattended and asserts on the
# output, as a rehearsal before the meeting.
MODE="live"
CURRENT_STEP=0
LAST_OUTPUT=""
CHECK_TAIL_PID=""
CHECK_TAIL_OUTPUT=""
CHECK_PASSED=false

if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
    BOLD=$'\033[1m' DIM=$'\033[2m' CYAN=$'\033[36m' GREEN=$'\033[32m'
    YELLOW=$'\033[33m' RED=$'\033[31m' RESET=$'\033[0m'
else
    BOLD="" DIM="" CYAN="" GREEN="" YELLOW="" RED="" RESET=""
fi

usage() {
    cat <<EOF
Usage: $0 <command>

  run [STEP]   Walk through the demo, one command per Enter key. Step 1 starts a fresh
               stack. A later STEP resumes on the stack that is already running.
  tail         Follow the topic with a Kafka consumer. Run it in a second terminal.
  check        Rehearse the whole demo unattended and check every output, then stop.
  up           Start a fresh iggy-server and Kafka gateway, without the walkthrough.
  status       Show what is running.
  down         Stop everything and delete the demo data.

Environment: DEMO_IGGY_PORT (8090), DEMO_KAFKA_PORT (9093), DEMO_TOPIC (orders),
DEMO_BIN_DIR (target/release), DEMO_STATE_DIR (\${TMPDIR:-/tmp}/iggy-kafka-demo).
EOF
}

main() {
    local command="${1:-help}"
    case "$command" in
        run) cmd_run "${2:-1}" ;;
        tail) cmd_tail ;;
        check) cmd_check ;;
        up) cmd_up ;;
        status) cmd_status ;;
        down) cmd_down ;;
        help | -h | --help) usage ;;
        *)
            usage >&2
            exit 1
            ;;
    esac
}

cmd_run() {
    local from="$1"
    if ! [[ $from =~ ^[1-9]$ ]] || ((from > STEPS)); then
        die "STEP must be a number from 1 to $STEPS"
    fi
    preflight
    if ((from > 1)) && ! stack_running; then
        die "the stack is not running, so step $from has nothing to resume. Start over: $0 run"
    fi
    [[ -t 0 && -t 1 ]] || die "run needs a terminal. For an unattended rehearsal, use: $0 check"
    # With echo off, an Enter pressed while a command runs does not add a blank line to its output.
    TERMINAL_STATE="$(stty -g </dev/tty)"
    stty -echo </dev/tty
    trap 'stty "$TERMINAL_STATE" </dev/tty' EXIT
    trap 'on_interrupt' INT
    if ((from == 1)); then
        title_card
    fi
    local step
    for ((step = from; step <= STEPS; step++)); do
        "step_$step"
    done
    pause
    closing_card
}

cmd_check() {
    MODE="check"
    preflight
    CHECK_TAIL_OUTPUT="$(mktemp)"
    trap 'finish_check' EXIT
    step_1
    # Follows the topic the way the second terminal does, so the check covers `tail` too.
    cmd_tail >"$CHECK_TAIL_OUTPUT" 2>&1 &
    CHECK_TAIL_PID=$!
    local step
    for ((step = 2; step <= STEPS; step++)); do
        "step_$step"
    done
    CURRENT_STEP="tail"
    local seen=0 attempt
    for ((attempt = 0; attempt < 40; attempt++)); do
        seen="$(grep -c '^partition ' "$CHECK_TAIL_OUTPUT" || true)"
        ((seen >= 4)) && break
        sleep 0.5
    done
    stop_check_tail
    LAST_OUTPUT="$(cat "$CHECK_TAIL_OUTPUT")"
    expect_output 'key order-1005  headers source=java' "the tail saw the Java producer's records"
    expect_output 'key order-1006  headers source=kcat' "the tail saw the kcat producer's record"
    expect_output 'headers source=iggy-cli' "the tail saw the Iggy CLI's message"
    stop_stack
    CHECK_PASSED=true
    printf '\n%sRehearsal passed: every step produced the expected output.%s\n' "$GREEN$BOLD" "$RESET"
}

# Runs on every exit from check, so a failed rehearsal leaves no consumer behind. The stack
# stays up after a failure, for its logs.
finish_check() {
    stop_check_tail
    rm -f "$CHECK_TAIL_OUTPUT"
    if [[ $CHECK_PASSED != true ]] && stack_running; then
        info "The stack is still running, for its logs in $STATE_DIR. Stop it with: $0 down" >&2
    fi
}

stop_check_tail() {
    if [[ -n $CHECK_TAIL_PID ]]; then
        kill "$CHECK_TAIL_PID" 2>/dev/null || true
        wait "$CHECK_TAIL_PID" 2>/dev/null || true
        CHECK_TAIL_PID=""
    fi
}

cmd_up() {
    preflight
    start_stack
    ok "iggy-server on $IGGY_ADDR and the Kafka gateway on $KAFKA_ADDR are running."
    info "Logs: $STATE_DIR/iggy-server.log and $STATE_DIR/gateway.log. Stop with: $0 down"
}

cmd_status() {
    local name
    for name in iggy-server gateway; do
        if process_running "$name"; then
            ok "$name is running, pid $(cat "$STATE_DIR/$name.pid"), log $STATE_DIR/$name.log"
        else
            say "$name is not running"
        fi
    done
    if port_open "$KAFKA_ADDR" && topic_exists; then
        ok "topic '$TOPIC' exists"
    fi
}

cmd_down() {
    stop_stack
    ok "Stopped. Demo data deleted."
}

# Follows every partition from the beginning. Starts over when the gateway restarts, because a
# fresh stack starts its offsets at 0 again.
cmd_tail() {
    require_docker
    # One name per tail process, so two tails, or a tail and check, never share a container.
    local container="iggy-kafka-demo-tail-$$"
    # shellcheck disable=SC2064 # The name is fixed for this process, so expand it now.
    trap "docker rm -f $container >/dev/null 2>&1 || true" EXIT
    say "${BOLD}Kafka consumer (kcat) following topic '$TOPIC' on every partition${RESET}"
    info "Leave this running. Records from Kafka clients and from Iggy clients show up here."
    local gateway_pid kcat_pid
    while true; do
        wait_for_topic
        gateway_pid="$(current_gateway_pid)"
        say ""
        info "\$ kcat -b $KAFKA_ADDR -C -t $TOPIC -o beginning -u -q -f '$RECORD_FORMAT'"
        docker run --rm --name "$container" --label "$TAIL_LABEL" --pull never --network host \
            "$KCAT_IMAGE" -b "$KAFKA_ADDR" -C -t "$TOPIC" -o beginning -u -q -f "$RECORD_FORMAT" &
        kcat_pid=$!
        while kill -0 "$kcat_pid" 2>/dev/null && [[ "$(current_gateway_pid)" == "$gateway_pid" ]]; do
            sleep 1
        done
        docker rm -f "$container" >/dev/null 2>&1 || true
        wait "$kcat_pid" 2>/dev/null || true
        if [[ "$(current_gateway_pid)" == "$gateway_pid" ]]; then
            info "(the consumer stopped, so it starts again)"
            sleep 2
        else
            info "(the gateway stopped or restarted, so the consumer starts over)"
        fi
    done
}

current_gateway_pid() {
    cat "$STATE_DIR/gateway.pid" 2>/dev/null || true
}

step_1() {
    step_header 1 "Start Iggy and the Kafka gateway" \
        "Iggy runs as usual. The gateway is one more binary. It speaks the Kafka protocol" \
        "on $KAFKA_ADDR and keeps every record in Iggy."
    present "IGGY_ROOT_USERNAME=$ROOT_USER IGGY_ROOT_PASSWORD=$ROOT_PASSWORD iggy-server --fresh" \
        start_server_and_report
    present "IGGY_KAFKA_BRIDGE_ENABLED=true IGGY_KAFKA_IGGY_ADDR=$IGGY_ADDR \\
IGGY_KAFKA_IGGY_USERNAME=$ROOT_USER IGGY_KAFKA_IGGY_PASSWORD=$ROOT_PASSWORD iggy-gateway-kafka" \
        start_gateway_and_report
    expect_output 'Iggy bridge connected' "the gateway connected to Iggy"
    expect_output "kafka listener bound on $KAFKA_ADDR" "the gateway listens for Kafka clients"
}

step_2() {
    step_header 2 "Create a topic with kafka-topics.sh" \
        "The stock Kafka admin tool creates a topic. The gateway creates an Iggy topic" \
        "with the same name and the same three partitions."
    present "kafka-topics.sh --bootstrap-server $KAFKA_ADDR --create --topic $TOPIC --partitions 3" \
        kafka_tool kafka-topics.sh --bootstrap-server "$KAFKA_ADDR" --create --topic "$TOPIC" \
        --partitions 3
    expect_output "Created topic $TOPIC" "kafka-topics.sh created the topic"
    present "kafka-topics.sh --bootstrap-server $KAFKA_ADDR --list" \
        kafka_tool kafka-topics.sh --bootstrap-server "$KAFKA_ADDR" --list
    expect_output "^$TOPIC\$" "kafka-topics.sh lists the topic"
    narrate "Iggy has it too, as topic '$TOPIC' in stream '$STREAM':"
    present "iggy topic get $STREAM $TOPIC" iggy_cli topic get "$STREAM" "$TOPIC"
    expect_output 'Partitions count +\| 3 ' "Iggy reports three partitions"
}

step_3() {
    step_header 3 "Produce with two different Kafka clients" \
        "The official Java client, and librdkafka: the C library under Confluent's Python," \
        "Go, .NET and JavaScript clients. Each record has a key and a header."
    present "kafka-console-producer.sh --bootstrap-server $KAFKA_ADDR --topic $TOPIC \\
--property parse.key=true --property key.separator=: \\
--property parse.headers=true --property headers.delimiter='|' --property headers.key.separator='='
> source=java|order-1004:{\"item\":\"book\",\"qty\":1}
> source=java|order-1005:{\"item\":\"lamp\",\"qty\":2}" \
        produce_with_java
    present "kcat -b $KAFKA_ADDR -P -t $TOPIC -K: -H source=kcat
> order-1006:{\"item\":\"desk\",\"qty\":1}" \
        produce_with_kcat
}

step_4() {
    step_header 4 "Consume with kcat" \
        "kcat reads every partition from the beginning and stops at the end. Each record" \
        "has its key and header, in the partition that its producer picked from the key."
    present "kcat -b $KAFKA_ADDR -C -t $TOPIC -o beginning -e -q \\
-f '$RECORD_FORMAT'" \
        kcat -C -t "$TOPIC" -o beginning -e -q -f "$RECORD_FORMAT"
    expect_output 'partition 0  offset 0  key order-1004  headers source=java' "order-1004 at 0/0"
    expect_output 'partition 0  offset 1  key order-1005  headers source=java' "order-1005 at 0/1"
    expect_output 'partition 2  offset 0  key order-1006  headers source=kcat' "order-1006 at 2/0"
}

step_5() {
    step_header 5 "Read the same records with the Iggy CLI" \
        "Each Kafka record is stored as a native Iggy message. The value is the payload," \
        "and the key and headers are Iggy headers. The offsets are the same numbers."
    present "iggy message poll --offset 0 --message-count 10 --show-headers $STREAM $TOPIC 0" \
        iggy_cli message poll --offset 0 --message-count 10 --show-headers "$STREAM" "$TOPIC" 0
    expect_output '\| 0 +\|.*order-1004' "Iggy offset 0 is order-1004"
    expect_output '\| 1 +\|.*order-1005' "Iggy offset 1 is order-1005"
    narrate "kafka.key and kafka.h.source carry the Kafka key and header. kafka.v marks a" \
        "message that the gateway wrote, so a plain Iggy message is never misread."
}

step_6() {
    step_header 6 "Write with the Iggy CLI" \
        "A native Iggy client writes to partition 0 of the same topic. The Kafka consumer" \
        "in the second terminal prints it at once."
    present "iggy message send --partition-id 0 --headers source:string:iggy-cli \\
$STREAM $TOPIC '{\"item\":\"pen\",\"qty\":5}'" \
        iggy_cli message send --partition-id 0 --headers source:string:iggy-cli "$STREAM" \
        "$TOPIC" '{"item":"pen","qty":5}'
}

step_7() {
    step_header 7 "Read it with the Java consumer, from a chosen offset" \
        "The consumer assigns itself partition 0 and starts at offset 1. It reads a record" \
        "that a Kafka client wrote and then the message that the Iggy CLI wrote."
    present "kafka-console-consumer.sh --bootstrap-server $KAFKA_ADDR --topic $TOPIC \\
--partition 0 --offset 1 --max-messages 2 --timeout-ms 20000 \\
--property print.timestamp=true --property print.offset=true \\
--property print.headers=true --property print.key=true" \
        kafka_tool kafka-console-consumer.sh --bootstrap-server "$KAFKA_ADDR" --topic "$TOPIC" \
        --partition 0 --offset 1 --max-messages 2 --timeout-ms 20000 \
        --property print.timestamp=true --property print.offset=true \
        --property print.headers=true --property print.key=true
    expect_output 'Offset:1[[:space:]]+source:java[[:space:]]+order-1005' "the Kafka record at offset 1"
    expect_output 'Offset:2[[:space:]]+source:iggy-cli[[:space:]]+null' "the Iggy message at offset 2, with no key"
    expect_output 'Processed a total of 2 messages' "the consumer stopped after two"
    narrate "The Iggy message has no Kafka key, so the consumer prints null. Its Iggy header" \
        "arrives as an ordinary Kafka header."
}

title_card() {
    clear_screen
    printf '\n%s  Apache Iggy: Kafka gateway demo%s\n\n' "$BOLD" "$RESET"
    cat <<EOF
     Kafka clients               Kafka gateway               Iggy server
  +-------------------+       +-----------------+        +-----------------+
  | Java client       |       |                 |        |                 |
  | kcat / librdkafka | ----> | $(printf '%-15s' "$KAFKA_ADDR") | -----> | $(printf '%-15s' "$IGGY_ADDR") |
  +-------------------+ Kafka +-----------------+  Iggy  +-----------------+
                       protocol                 protocol         ^
                                                                 |
                                                     Iggy CLI and SDKs
EOF
    say ""
    narrate "The Kafka tools are the unmodified upstream builds: kafka-*.sh from the" \
        "$KAFKA_IMAGE image and kcat from the $KCAT_IMAGE image."
    say ""
    info "Optional: run '$0 tail' in a second terminal now, to watch a Kafka consumer live."
    warn_if_narrow
}

closing_card() {
    clear_screen
    printf '\n%s== Where this stands ==%s\n' "$BOLD" "$RESET"
    cat <<EOF

  Works now
    - Stock Kafka clients create topics, produce and consume through the gateway.
    - Keys, headers and timestamps are kept, and a Kafka offset is the Iggy offset.
    - Kafka clients and native Iggy clients read each other's messages.
    - Idempotent producers start, and SASL/PLAIN sign-in can be turned on.

  Next
    - Consumer group offsets (OffsetCommit, OffsetFetch, #3542), for '--group' consumers.
    - Docker Compose, a quick start and a CI end-to-end job (#3539), to close Phase 1.
    - Throughput: one Iggy client carries every Produce today.
    - TLS on the Kafka listener.

EOF
    info "The stack is still running for questions. Stop it with: $0 down"
}

produce_with_java() {
    printf '%s\n' \
        'source=java|order-1004:{"item":"book","qty":1}' \
        'source=java|order-1005:{"item":"lamp","qty":2}' |
        kafka_tool_with_input kafka-console-producer.sh --bootstrap-server "$KAFKA_ADDR" \
            --topic "$TOPIC" --property parse.key=true --property key.separator=: \
            --property parse.headers=true --property 'headers.delimiter=|' \
            --property 'headers.key.separator=='
}

produce_with_kcat() {
    printf '%s\n' 'order-1006:{"item":"desk","qty":1}' |
        kcat_with_input -P -t "$TOPIC" -K: -H source=kcat
}

start_server_and_report() {
    start_stack_dir
    start_server
    printf 'iggy-server is listening on %s\n' "$IGGY_ADDR"
}

start_gateway_and_report() {
    start_gateway
    log_messages "$STATE_DIR/gateway.log" 'Iggy bridge connected to|kafka listener bound'
}

# Runs one of the Kafka distribution's own command line tools from the official image.
kafka_tool() {
    local tool="$1"
    shift
    timeout --foreground "$CLIENT_TIMEOUT" docker run --rm --pull never --network host \
        "$KAFKA_IMAGE" "/opt/kafka/bin/$tool" "$@" </dev/null
}

kafka_tool_with_input() {
    local tool="$1"
    shift
    timeout --foreground "$CLIENT_TIMEOUT" docker run --rm -i --pull never --network host \
        "$KAFKA_IMAGE" "/opt/kafka/bin/$tool" "$@"
}

kcat() {
    timeout --foreground "$CLIENT_TIMEOUT" docker run --rm --pull never --network host \
        "$KCAT_IMAGE" -b "$KAFKA_ADDR" "$@" </dev/null
}

kcat_with_input() {
    timeout --foreground "$CLIENT_TIMEOUT" docker run --rm -i --pull never --network host \
        "$KCAT_IMAGE" -b "$KAFKA_ADDR" "$@"
}

iggy_cli() {
    "$BIN_DIR/iggy" --transport tcp --tcp-server-address "$IGGY_ADDR" --username "$ROOT_USER" \
        --password "$ROOT_PASSWORD" "$@" </dev/null
}

preflight() {
    [[ "$(uname -s)" == Linux ]] ||
        die "this demo needs Linux, because the Kafka clients run in containers on the host network"
    local binary
    for binary in iggy-server iggy-gateway-kafka iggy; do
        [[ -x "$BIN_DIR/$binary" ]] || die "$BIN_DIR/$binary is missing. Build it from the repo root:
  cargo build --release --bin iggy-server --bin iggy --bin iggy-gateway-kafka"
    done
    warn_if_binaries_stale
    require_docker
    local image
    for image in "$KAFKA_IMAGE" "$KCAT_IMAGE"; do
        if ! docker image inspect "$image" >/dev/null 2>&1; then
            info "pulling $image"
            docker pull --quiet "$image" >/dev/null
        fi
    done
}

warn_if_binaries_stale() {
    local head_time binary
    head_time="$(git -C "$REPO_ROOT" log -1 --format=%ct 2>/dev/null || echo 0)"
    for binary in iggy-server iggy-gateway-kafka iggy; do
        if (($(stat -c %Y "$BIN_DIR/$binary") < head_time)); then
            warn "$BIN_DIR/$binary is older than the last commit. Rebuild it before the demo."
        fi
    done
}

require_docker() {
    command -v docker >/dev/null || die "docker is not installed"
    command -v setsid >/dev/null || die "setsid is not installed (it comes with util-linux)"
    docker info >/dev/null 2>&1 || die "docker is not running, or this user cannot use it"
}

start_stack() {
    start_stack_dir
    start_server
    start_gateway
}

# Every start is fresh, so offsets start at 0 and the output matches the talk track.
start_stack_dir() {
    stop_stack
    mkdir -p "$STATE_DIR"
    touch "$STATE_DIR/$STATE_MARKER"
    local address
    for address in "$IGGY_ADDR" "$KAFKA_ADDR"; do
        if port_open "$address"; then
            die "$address is already in use. Stop what uses it, or set DEMO_IGGY_PORT and DEMO_KAFKA_PORT."
        fi
    done
}

# env -i keeps IGGY_* variables from the presenter's shell out of both processes. The gateway
# refuses an IGGY_KAFKA_* name it does not know, and the server would read any other one.
# setsid detaches both from the terminal, so a Ctrl-C in the walkthrough leaves them running and
# `run STEP` can resume.
start_server() {
    (
        cd "$STATE_DIR"
        exec setsid env -i PATH="$PATH" HOME="$HOME" \
            IGGY_CONFIG_PATH="$REPO_ROOT/core/server/config.toml" \
            IGGY_PATH="$STATE_DIR/iggy-data" \
            IGGY_TCP_ADDRESS="$IGGY_ADDR" \
            IGGY_HTTP_ENABLED=false IGGY_QUIC_ENABLED=false IGGY_WEBSOCKET_ENABLED=false \
            IGGY_SHARDING_PIN_CORES=false IGGY_SHARDING_CPU_ALLOCATION=0..4 \
            IGGY_ROOT_USERNAME="$ROOT_USER" IGGY_ROOT_PASSWORD="$ROOT_PASSWORD" \
            "$BIN_DIR/iggy-server" --fresh
    ) >"$STATE_DIR/iggy-server.log" 2>&1 &
    echo $! >"$STATE_DIR/iggy-server.pid"
    wait_until_listening iggy-server "$IGGY_ADDR"
}

start_gateway() {
    (
        cd "$STATE_DIR"
        exec setsid env -i PATH="$PATH" HOME="$HOME" \
            IGGY_KAFKA_BIND_ADDR="$KAFKA_ADDR" \
            IGGY_KAFKA_INSTANCE_ID=0 \
            IGGY_KAFKA_SHUTDOWN_DRAIN_TIMEOUT_SECS=2 \
            IGGY_KAFKA_BRIDGE_ENABLED=true \
            IGGY_KAFKA_IGGY_ADDR="$IGGY_ADDR" \
            IGGY_KAFKA_IGGY_USERNAME="$ROOT_USER" \
            IGGY_KAFKA_IGGY_PASSWORD="$ROOT_PASSWORD" \
            "$BIN_DIR/iggy-gateway-kafka"
    ) >"$STATE_DIR/gateway.log" 2>&1 &
    echo $! >"$STATE_DIR/gateway.pid"
    wait_until_listening gateway "$KAFKA_ADDR"
}

wait_until_listening() {
    local name="$1" address="$2" pid attempt
    pid="$(cat "$STATE_DIR/$name.pid")"
    for ((attempt = 0; attempt < 120; attempt++)); do
        if port_open "$address"; then
            return 0
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            tail -n 20 "$STATE_DIR/$name.log" >&2 || true
            die "$name stopped during startup. Its log is above and in $STATE_DIR/$name.log"
        fi
        sleep 0.25
    done
    die "$name did not listen on $address within 30 s. Log: $STATE_DIR/$name.log"
}

# Stops the gateway before the tail containers, so an open tail reports the stop as a stop.
stop_stack() {
    stop_process gateway iggy-gateway-ka
    stop_process iggy-server iggy-server
    docker ps -aq --filter "label=$TAIL_LABEL" 2>/dev/null |
        xargs -r docker rm -f >/dev/null 2>&1 || true
    if [[ -f "$STATE_DIR/$STATE_MARKER" ]]; then
        rm -rf "$STATE_DIR"
    fi
}

# Checks the command name before it signals, so a stale pid file never stops an unrelated
# process that reused the pid. The kernel cuts the name to 15 characters.
stop_process() {
    local name="$1" command_name="$2" pid attempt
    [[ -f "$STATE_DIR/$name.pid" ]] || return 0
    pid="$(cat "$STATE_DIR/$name.pid")"
    if [[ "$(ps -p "$pid" -o comm= 2>/dev/null || true)" == "$command_name" ]]; then
        kill "$pid" 2>/dev/null || true
        for ((attempt = 0; attempt < 40; attempt++)); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.25
        done
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" 2>/dev/null || true
        fi
    fi
    rm -f "$STATE_DIR/$name.pid"
}

stack_running() {
    process_running iggy-server && process_running gateway
}

process_running() {
    [[ -f "$STATE_DIR/$1.pid" ]] && kill -0 "$(cat "$STATE_DIR/$1.pid")" 2>/dev/null
}

port_open() {
    (exec 3<>"/dev/tcp/${1%:*}/${1##*:}") 2>/dev/null
}

wait_for_topic() {
    local announced=false
    until port_open "$KAFKA_ADDR" && topic_exists; do
        if ! $announced; then
            info "waiting for the gateway and topic '$TOPIC'"
            announced=true
        fi
        sleep 2
    done
}

topic_exists() {
    kcat -L -t "$TOPIC" 2>/dev/null | grep -Eq "topic \"$TOPIC\" with [1-9][0-9]* partitions"
}

# Prints the message part of matching log lines, without the colors, timestamp and target.
log_messages() {
    sed -E 's/\x1b\[[0-9;]*m//g' "$1" | grep -E "$2" | sed -E 's/^[^ ]+ +[A-Z]+ +[^ ]+: //'
}

# Waits first, so the last output stays on screen while the presenter talks about it.
step_header() {
    CURRENT_STEP="$1"
    pause
    clear_screen
    printf '\n%s== Step %s of %s: %s ==%s\n' "$BOLD" "$1" "$STEPS" "$2" "$RESET"
    shift 2
    narrate "$@"
}

narrate() {
    local line
    for line in "$@"; do
        printf '   %s\n' "$line"
    done
}

# Shows a command the way the audience reads it, waits for Enter, then runs the real one.
# The display can span lines. Lines that start with '>' are what the producer reads.
present() {
    local display="$1"
    shift
    printf '\n'
    local line first=true
    while IFS= read -r line; do
        if [[ $line == '>'* ]]; then
            printf '  %s%s%s\n' "$YELLOW" "$line" "$RESET"
        elif $first; then
            printf '%s$ %s%s\n' "$CYAN$BOLD" "$line" "$RESET"
        else
            printf '%s      %s%s\n' "$CYAN$BOLD" "$line" "$RESET"
        fi
        first=false
    done <<<"$display"
    pause
    local output_file status=0
    output_file="$(mktemp)"
    "$@" 2>&1 | tee "$output_file" || status=$?
    LAST_OUTPUT="$(cat "$output_file")"
    rm -f "$output_file"
    if ((status != 0)); then
        step_failed "$status"
    fi
}

pause() {
    [[ $MODE == live ]] || return 0
    # Drops keys pressed while the last command ran, so a double press never skips a pause.
    read -r -s -N 10000 -t 0.05 _ </dev/tty || true
    printf '%s[Enter]%s' "$DIM" "$RESET"
    read -r -s _ </dev/tty
    printf '\r\033[K'
}

clear_screen() {
    if [[ $MODE == live && ${DEMO_CLEAR:-1} == 1 ]]; then
        printf '\033[H\033[2J'
    fi
}

# The Iggy CLI prints step 5 as one table, about 180 columns wide.
warn_if_narrow() {
    local columns
    columns="$(tput cols 2>/dev/null || echo 0)"
    if ((columns > 0 && columns < 180)); then
        warn "this terminal is $columns columns wide. Step 5 prints a table 180 columns wide," \
            "so widen the window or make the font smaller."
    fi
}

expect_output() {
    [[ $MODE == check ]] || return 0
    if grep -Eq -- "$1" <<<"$LAST_OUTPUT"; then
        ok "   check: $2"
    else
        die "step $CURRENT_STEP: expected $2, but the output did not match /$1/"
    fi
}

step_failed() {
    printf '\n%sStep %s failed (exit code %s).%s\n' "$RED$BOLD" "$CURRENT_STEP" "$1" "$RESET" >&2
    info "Gateway log: $STATE_DIR/gateway.log. Iggy log: $STATE_DIR/iggy-server.log" >&2
    info "To try this step again: $0 run $CURRENT_STEP. To start over: $0 run" >&2
    exit 1
}

on_interrupt() {
    printf '\n' >&2
    info "Stopped at step $CURRENT_STEP. Resume with: $0 run $CURRENT_STEP" >&2
    exit 130
}

say() { printf '%s\n' "$*"; }
info() { printf '%s%s%s\n' "$DIM" "$*" "$RESET"; }
ok() { printf '%s%s%s\n' "$GREEN" "$*" "$RESET"; }
warn() { printf '%swarning: %s%s\n' "$YELLOW" "$*" "$RESET" >&2; }
die() {
    printf '%serror: %s%s\n' "$RED" "$*" "$RESET" >&2
    exit 1
}

main "$@"
