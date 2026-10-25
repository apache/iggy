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

# Stops Iggy nodes under Kafka load through one gateway, then reports send latency, the error
# codes that clients got and the time to recover.
#
# Runs 3 Iggy nodes and one gateway on 127.0.0.1, with kafka-producer-perf-test.sh and a
# kafka-verifiable-consumer.sh group in Docker. Under load it stops, then restarts:
#
#   1. a node that holds partition primaries,
#   2. the metadata primary.
#
# The gateway dials GATEWAY_NODE. With the default, no stop hits it. At the end
# tests/java/ReadYourCommitsProbe.java commits offsets and reads each back at once.
#
# Needs Linux for Docker host networking, the KAFKA_IMAGE image pulled, jq, perl and a JDK 17+.
# Build first, from the workspace root:
#
#   cargo build --release --bin iggy-server
#   cargo build --release -p iggy-gateway-kafka --bin iggy-gateway-kafka --example partition_routes
#
# Usage: gateways/kafka/scripts/cluster-failover.sh [work dir]
#        gateways/kafka/scripts/cluster-failover.sh --report <work dir>
#
# The work dir keeps every log, each line stamped in epoch ms. --report prints a run's report
# again. Both exit 1 if the consumer missed a record or the probe read back a wrong offset.

set -euo pipefail

TARGET_DIR="${TARGET_DIR:-target/release}"
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:4.4.0}"
BASE_PORT="${BASE_PORT:-28000}"
GATEWAY_NODE="${GATEWAY_NODE:-2}"
PARTITIONS="${PARTITIONS:-6}"
# Records per second.
RATE="${RATE:-2000}"
WARMUP_SECS="${WARMUP_SECS:-30}"
DOWN_SECS="${DOWN_SECS:-30}"
SETTLE_SECS="${SETTLE_SECS:-60}"
# TERM stops a node. KILL crashes it.
STOP_SIGNAL="${STOP_SIGNAL:-TERM}"
SHARDS="${SHARDS:-2}"
PROBE_ROUNDS="${PROBE_ROUNDS:-300}"

NODES=3
PROBE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../tests/java" && pwd)"
TOPIC="failover"
GROUP="failover-group"
STREAM="kafka"
GATEWAY_PORT=$((BASE_PORT + 100))
GATEWAY_ADDR="127.0.0.1:${GATEWAY_PORT}"
PRODUCER="iggy-failover-producer-$$"
CONSUMER="iggy-failover-consumer-$$"
RECORDS=$((RATE * (WARMUP_SECS + 2 * (DOWN_SECS + SETTLE_SECS) + 10)))

if [[ "${1:-}" == "--report" ]]; then
  WORK_DIR="${2:?usage: $0 --report <work dir>}"
  # shellcheck source=/dev/null
  source "$WORK_DIR/settings"
else
  WORK_DIR="${1:-$(mktemp -d "${TMPDIR:-/tmp}/iggy-cluster-failover.XXXXXX")}"
fi
mkdir -p "$WORK_DIR"
EVENTS="$WORK_DIR/events.log"

NODE_PIDS=()
GATEWAY_PID=""
ROUTES_PID=""
PRODUCER_PID=""
CONSUMER_PID=""

log() {
  echo "[$(date +%T)] $*" >&2
}

# Appends "<epoch ms> <kind> [<node> <note>]" to events.log. Every kind but "stopped" opens a
# phase of the report.
event() {
  echo "$(date +%s%3N) $*" >>"$EVENTS"
  log "$*"
}

# Prefixes each line with its time in epoch milliseconds and drops ANSI colors.
stamp() {
  perl -MTime::HiRes=time -ne 'BEGIN { $| = 1 } s/\e\[[0-9;]*m//g; printf "%d %s", time * 1000, $_'
}

tcp_port() {
  echo $((BASE_PORT + 10 * $1))
}

replica_port() {
  echo $((BASE_PORT + 10 * $1 + 1))
}

cleanup() {
  docker rm -f "$PRODUCER" "$CONSUMER" >/dev/null 2>&1 || true
  for pid in "$ROUTES_PID" "$GATEWAY_PID" "${NODE_PIDS[@]}"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  wait 2>/dev/null || true
}

preflight() {
  local binary
  for binary in iggy-server iggy-gateway-kafka examples/partition_routes; do
    if [[ ! -x "$TARGET_DIR/$binary" ]]; then
      echo "missing $TARGET_DIR/$binary, see the build commands at the top of $0" >&2
      exit 1
    fi
  done
  for binary in docker jq perl javac; do
    command -v "$binary" >/dev/null || {
      echo "$binary is required" >&2
      exit 1
    }
  done
  docker image inspect "$KAFKA_IMAGE" >/dev/null 2>&1 || {
    echo "pull $KAFKA_IMAGE first" >&2
    exit 1
  }
}

# Waits until something accepts on `port`. Fails early if process `pid` exits first.
wait_for_port() {
  local port=$1 pid=$2 deadline=$((SECONDS + 60))
  until (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; do
    if ! kill -0 "$pid" 2>/dev/null || ((SECONDS >= deadline)); then
      return 1
    fi
    sleep 0.2
  done
}

start_node() {
  local node=$1 peer
  local env=(
    "PATH=$PATH"
    "IGGY_PATH=$WORK_DIR/node-$node"
    "IGGY_TCP_ADDRESS=127.0.0.1:$(tcp_port "$node")"
    "IGGY_HTTP_ENABLED=false"
    "IGGY_QUIC_ENABLED=false"
    "IGGY_WEBSOCKET_ENABLED=false"
    "IGGY_SHARDING_CPU_ALLOCATION=$SHARDS"
    "IGGY_SHARDING_PIN_CORES=false"
    "IGGY_ROOT_USERNAME=iggy"
    "IGGY_ROOT_PASSWORD=iggy"
    "IGGY_CLUSTER_ENABLED=true"
    "IGGY_CLUSTER_NAME=kafka-failover"
  )
  for ((peer = 0; peer < NODES; peer++)); do
    env+=(
      "IGGY_CLUSTER_NODES_${peer}_NAME=node-$peer"
      "IGGY_CLUSTER_NODES_${peer}_IP=127.0.0.1"
      "IGGY_CLUSTER_NODES_${peer}_REPLICA_ID=$peer"
      "IGGY_CLUSTER_NODES_${peer}_PORTS_TCP=$(tcp_port "$peer")"
      "IGGY_CLUSTER_NODES_${peer}_PORTS_TCP_REPLICA=$(replica_port "$peer")"
    )
  done
  env -i "${env[@]}" "$TARGET_DIR/iggy-server" --replica-id "$node" \
    > >(stamp >>"$WORK_DIR/node-$node.log") 2>&1 &
  NODE_PIDS[node]=$!
  wait_for_port "$(tcp_port "$node")" "${NODE_PIDS[node]}" || {
    echo "node $node did not start, see $WORK_DIR/node-$node.log" >&2
    exit 1
  }
}

stop_node() {
  local node=$1
  kill -s "$STOP_SIGNAL" "${NODE_PIDS[node]}"
  wait "${NODE_PIDS[node]}" 2>/dev/null || true
}

# The bridge signs in before the listener binds, and fails while the cluster has no metadata
# primary yet. So a gateway that exits early gets another try.
start_gateway() {
  local attempt
  for attempt in 1 2 3 4 5; do
    env -i "PATH=$PATH" \
      "RUST_LOG=info,iggy=info,iggy_gateway_kafka::protocol::handlers=debug,iggy_gateway_kafka::bridge=debug" \
      "IGGY_KAFKA_BIND_ADDR=$GATEWAY_ADDR" \
      "IGGY_KAFKA_BRIDGE_ENABLED=true" \
      "IGGY_KAFKA_IGGY_ADDR=127.0.0.1:$(tcp_port "$GATEWAY_NODE")" \
      "IGGY_KAFKA_IGGY_PASSWORD=iggy" \
      "$TARGET_DIR/iggy-gateway-kafka" > >(stamp >>"$WORK_DIR/gateway.log") 2>&1 &
    GATEWAY_PID=$!
    if wait_for_port "$GATEWAY_PORT" "$GATEWAY_PID"; then
      return 0
    fi
    log "gateway attempt $attempt failed"
    sleep 2
  done
  echo "the gateway did not start, see $WORK_DIR/gateway.log" >&2
  exit 1
}

kafka_tool() {
  docker run --rm --pull never --network host "$KAFKA_IMAGE" "/opt/kafka/bin/$1" "${@:2}"
}

latest_routes() {
  grep '"primaries"' "$WORK_DIR/routes.jsonl" | tail -n 1 || true
}

# Every node holds primaries, and no node holds two more than another.
wait_for_spread_primaries() {
  local deadline=$((SECONDS + 120))
  until latest_routes | jq -e --argjson nodes "$NODES" \
    '.primaries | group_by(.) | map(length) | length == $nodes and max - min <= 1' >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      echo "partition primaries did not spread: $(latest_routes)" >&2
      exit 1
    fi
    sleep 1
  done
}

metadata_primary() {
  latest_routes | jq '.metadata_primary'
}

# A node that holds partition primaries, and is neither the metadata primary nor the gateway's.
primary_holder() {
  latest_routes | jq --argjson gateway "$GATEWAY_NODE" \
    '.metadata_primary as $metadata | .primaries | unique
     | map(select(. != $metadata and . != $gateway)) | first // empty'
}

partitions_of() {
  latest_routes | jq -c --argjson node "$1" '[.primaries | to_entries[] | select(.value == $node) | .key]'
}

start_load() {
  docker run --rm --pull never --network host --name "$CONSUMER" "$KAFKA_IMAGE" \
    /opt/kafka/bin/kafka-verifiable-consumer.sh --bootstrap-server "$GATEWAY_ADDR" \
    --topic "$TOPIC" --group-id "$GROUP" --reset-policy earliest --verbose \
    > >(stamp >"$WORK_DIR/consumer.out") 2> >(stamp >"$WORK_DIR/consumer.err") &
  CONSUMER_PID=$!
  # Keys spread the records over every partition. The monotonic payload is the record number,
  # so the consumer can tell a lost record from a duplicate.
  docker run --rm --pull never --network host --name "$PRODUCER" "$KAFKA_IMAGE" \
    /opt/kafka/bin/kafka-producer-perf-test.sh --bootstrap-server "$GATEWAY_ADDR" \
    --topic "$TOPIC" --num-records "$RECORDS" --throughput "$RATE" --payload-monotonic \
    --record-key-range $((PARTITIONS * 1000)) --key-distribution range \
    --reporting-interval 1000 --command-property acks=all --print-metrics \
    > >(stamp >"$WORK_DIR/producer.out") 2> >(stamp >"$WORK_DIR/producer.err") &
  PRODUCER_PID=$!
}

# Stops `node`, keeps it down for DOWN_SECS, restarts it, then waits SETTLE_SECS.
outage() {
  local node=$1 role=$2
  event "stop $node $role, primary of partitions $(partitions_of "$node")"
  stop_node "$node"
  event "stopped $node"
  sleep "$DOWN_SECS"
  event "restart $node"
  start_node "$node"
  sleep "$SETTLE_SECS"
}

# Waits for the producer to finish, then for the consumer to read as many records as the
# producer had acknowledged, or to read nothing more for 30 s.
finish_load() {
  local acked read last_read=-1 idle=0 deadline=$((SECONDS + 300))
  while kill -0 "$PRODUCER_PID" 2>/dev/null && ((SECONDS < deadline)); do
    sleep 1
  done
  docker stop -t 20 "$PRODUCER" >/dev/null 2>&1 || true
  wait "$PRODUCER_PID" || true
  acked=$(grep -o '^[0-9]* [0-9]* records sent.*50th' "$WORK_DIR/producer.out" | awk '{ print $2 }')
  while ((idle < 30)); do
    read=$(grep -c '"name":"record_data"' "$WORK_DIR/consumer.out" || true)
    if [[ -n "$acked" ]] && ((read >= acked)); then
      break
    fi
    if ((read == last_read)); then
      idle=$((idle + 1))
    else
      idle=0
      last_read=$read
    fi
    sleep 1
  done
  docker stop -t 20 "$CONSUMER" >/dev/null 2>&1 || true
  wait "$CONSUMER_PID" || true
  event "end"
  kafka_tool kafka-consumer-groups.sh --bootstrap-server "$GATEWAY_ADDR" --describe \
    --group "$GROUP" >"$WORK_DIR/group.txt" 2>&1 || true
}

# Commits an offset and reads it back at once through the gateway, in every partition. The
# gateway's node is a backup of the partitions whose primary is elsewhere. The image has no
# compiler, so the probe compiles here against the image's kafka-clients jar.
read_your_commits() {
  local classes="$WORK_DIR/probe"
  mkdir -p "$classes"
  docker run --rm --pull never --entrypoint cat "$KAFKA_IMAGE" \
    "/opt/kafka/libs/kafka-clients-${KAFKA_IMAGE##*:}.jar" >"$classes/kafka-clients.jar"
  javac --release 17 -cp "$classes/kafka-clients.jar" -d "$classes" \
    "$PROBE_DIR/ReadYourCommitsProbe.java"
  log "read your commits, routes $(latest_routes)"
  docker run --rm --pull never --network host -v "$classes:/probe:ro" "$KAFKA_IMAGE" \
    java -cp '/opt/kafka/libs/*:/probe' ReadYourCommitsProbe "$GATEWAY_ADDR" "$TOPIC" \
    "$PARTITIONS" "$PROBE_ROUNDS" read-your-commits >"$WORK_DIR/probe.out" 2>&1 || true
}

report() {
  local status=0
  awk -v rate="$RATE" -v records="$RECORDS" -v gateway_node="127.0.0.1:$(tcp_port "$GATEWAY_NODE")" '
    function phase_of(at,   phase) {
      for (phase = phases; phase > 0; phase--) if (at >= start[phase]) return phase
      return 0
    }
    function since(at, phase) { return sprintf("%.1f", (at - start[phase]) / 1000) }
    function count(phase, what) { if (phase > 0) seen_in[phase, what]++ }
    # The message of a Kafka or a tracing log line, with numbers as N.
    function message(text) {
      sub(/^[0-9]+ /, "", text)
      if (!sub(/^\[[^]]*\] [A-Z]+ (\[[^]]*\] )?/, "", text)) {
        sub(/^[^ ]+ +[A-Z]+ +[A-Za-z0-9_:]+: /, "", text)
      }
      sub(/ \(org[.]apache[.]kafka[^)]*\)$/, "", text)
      gsub(/[0-9]+/, "N", text)
      return text
    }
    FILENAME ~ /events.log$/ {
      if ($2 == "stopped") { stopped[phases] = $1; next }
      phases++
      start[phases] = $1
      label[phases] = $2 (NF > 2 ? " node " $3 : "")
      note[phases] = $0
      if (!sub(/^[^ ]+ [^ ]+ [^ ]+ /, "", note[phases])) note[phases] = ""
      next
    }
    FILENAME ~ /producer.out$/ && / records sent, / && !/50th/ {
      p = phase_of($1)
      split($0, part, ", ")
      split(part[2], sent, " ")
      split(part[4], slowest, " ")
      if (slowest[1] + 0 > max_latency[p]) max_latency[p] = slowest[1] + 0
      if (sent[1] + 0 < rate / 2 || slowest[1] + 0 > 1000) {
        impaired[p]++
        recovered[p] = $1
      }
      next
    }
    FILENAME ~ /producer.out$/ && /50th/ { summary = substr($0, index($0, " ") + 1); next }
    FILENAME ~ /producer.(out|err)$/ && /WARN/ {
      p = phase_of($1)
      retries[p]++
      count(p, "producer: " message($0))
      next
    }
    FILENAME ~ /producer.(out|err)$/ && $2 ~ /^org[.]apache[.]kafka[.]common[.]errors[.]/ {
      p = phase_of($1)
      failed[p]++
      count(p, "producer record failed: " message($0))
      next
    }
    FILENAME ~ /consumer.out$/ && /"name":"records_consumed"/ {
      line = $0
      while (match(line, /"partition":[0-9]+/)) {
        partition = substr(line, RSTART + 12, RLENGTH - 12)
        if (partition in last_read) {
          p = phase_of($1)
          if ($1 - last_read[partition] > read_gap[p]) read_gap[p] = $1 - last_read[partition]
        }
        last_read[partition] = $1
        line = substr(line, RSTART + RLENGTH)
      }
      next
    }
    FILENAME ~ /consumer.out$/ && /"name":"record_data"/ && match($0, /"value":"[0-9]+"/) {
      value = substr($0, RSTART + 9, RLENGTH - 10)
      if (++read_count[value] == 1) distinct++
      else if (read_count[value] == 2) repeated++
      next
    }
    FILENAME ~ /consumer.out$/ && /"name":"offsets_committed"/ && /"success":false/ {
      p = phase_of($1)
      retries[p]++
      count(p, "consumer commit failed")
      next
    }
    FILENAME ~ /consumer.out$/ && /"name":"partitions_assigned"/ {
      count(phase_of($1), "consumer partitions assigned")
      next
    }
    FILENAME ~ /consumer.err$/ && /WARN/ {
      p = phase_of($1)
      retries[p]++
      count(p, "consumer: " message($0))
      next
    }
    FILENAME ~ /gateway.log$/ && /Connecting to / {
      if (index($0, "Connecting to " gateway_node) == 0) moves[phase_of($1)]++
      next
    }
    FILENAME ~ /gateway.log$/ && (/ (WARN|ERROR) / || / DEBUG .*(refused|failed)/) {
      count(phase_of($1), "gateway: " message($0))
      next
    }
    FILENAME ~ /node-[0-9].log$/ && /sim_event="PrimaryElected"/ {
      p = phase_of($1)
      if (p == 0) next
      match($0, /replica_id=[0-9]+/)
      node = substr($0, RSTART + 11, RLENGTH - 11)
      if (/plane="metadata"/) {
        count(p, "metadata primary elected on node " node " at +" since($1, p) " s")
      } else {
        elections[p]++
        last_election[p] = $1
      }
      next
    }
    END {
      printf "%-16s %7s %7s %8s %9s %8s %7s %5s\n", "phase", "start s", "max ms", "impaired", \
        "recovered", "read gap", "retries", "moves"
      for (p = 1; p < phases; p++) {
        printf "%-16s %7s %7.0f %8d %9s %8.1f %7d %5d\n", label[p], since(start[p], 1), \
          max_latency[p], impaired[p], p in recovered ? since(recovered[p], p) : "-", \
          read_gap[p] / 1000, retries[p], moves[p]
      }
      print ""
      print "max ms: slowest send. impaired: 1 s windows under half the rate or with a send over"
      print "1 s, and recovered: seconds into the phase when the last of them ended. read gap: the"
      print "longest time a partition went unread, in seconds. retries: client warnings and failed"
      print "commits. moves: SDK sessions in the gateway that dialed another Iggy node."
      for (p = 1; p < phases; p++) {
        print ""
        print label[p] (note[p] != "" ? ", " note[p] : "")
        if (p in stopped) print "  the node exited after " since(stopped[p], p) " s"
        if (elections[p]) {
          print "  " elections[p] " partition primaries elected, the last at +" since(last_election[p], p) " s"
        }
        for (key in seen_in) {
          split(key, at, SUBSEP)
          if (at[1] == p) print "  " seen_in[key] " x " at[2]
        }
      }
      print ""
      print "producer: " summary
      missing = 0
      for (value = 0; value < records; value++) if (!(value in read_count)) missing++
      printf "consumer: read %d of %d records, %d of them more than once, %d never\n", \
        distinct, records, repeated, missing
      exit missing > 0 ? 1 : 0
    }
  ' "$EVENTS" "$WORK_DIR"/producer.* "$WORK_DIR"/consumer.* "$WORK_DIR/gateway.log" \
    "$WORK_DIR"/node-[0-9].log || status=1
  echo
  cat "$WORK_DIR/group.txt"
  echo
  grep -E '^(read_your_commits|partition=)' "$WORK_DIR/probe.out" || cat "$WORK_DIR/probe.out"
  grep -q '^read_your_commits .* wrong=0 ' "$WORK_DIR/probe.out" || status=1
  echo
  if ((status == 0)); then
    echo "pass: the consumer read every record, and every offset read back as committed"
  else
    echo "fail: the consumer missed a record, or an offset read back wrong"
  fi
  return "$status"
}

main() {
  preflight
  trap cleanup EXIT
  log "work dir $WORK_DIR"
  printf 'RATE=%s\nRECORDS=%s\nGATEWAY_NODE=%s\nBASE_PORT=%s\n' \
    "$RATE" "$RECORDS" "$GATEWAY_NODE" "$BASE_PORT" >"$WORK_DIR/settings"
  local node
  for ((node = 0; node < NODES; node++)); do
    start_node "$node"
  done
  start_gateway
  kafka_tool kafka-topics.sh --bootstrap-server "$GATEWAY_ADDR" --create --topic "$TOPIC" \
    --partitions "$PARTITIONS"
  env -i "IGGY_USERNAME=iggy" "IGGY_PASSWORD=iggy" "$TARGET_DIR/examples/partition_routes" \
    "127.0.0.1:$(tcp_port "$GATEWAY_NODE")" "$STREAM" "$TOPIC" "$PARTITIONS" \
    >"$WORK_DIR/routes.jsonl" 2>&1 &
  ROUTES_PID=$!
  wait_for_spread_primaries
  log "routes $(latest_routes)"

  event "load"
  start_load
  sleep "$WARMUP_SECS"
  local holder
  holder=$(primary_holder)
  if [[ -z "$holder" ]]; then
    echo "no node outside the gateway's and the metadata primary holds primaries: $(latest_routes)" >&2
    exit 1
  fi
  outage "$holder" "a node"
  local metadata
  metadata=$(metadata_primary)
  if ((metadata == GATEWAY_NODE)); then
    log "the metadata primary is the gateway's node $metadata, so new Iggy clients fail while it is down"
  fi
  outage "$metadata" "the metadata primary"
  finish_load
  read_your_commits
  report | tee "$WORK_DIR/report.txt"
}

if [[ "${1:-}" == "--report" ]]; then
  report
else
  main
fi
