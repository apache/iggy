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

# Runs one fault scenario against a fresh 3-node cluster: creates a topic,
# produces while injecting the scenario's faults, heals every node, reads the
# whole topic back and checks it. See README.md for what each scenario
# reproduces.
#
# Usage: scripts/cluster-chaos/run-scenario.sh <scenario> [results-dir]
# Scenarios: baseline control crash-restart rolling-restart isolate pause
#   full-outage full-outage-replicated kill-storm cascading-crash
#   rolling-crash split-primaries
#
# Environment (scenario defaults in README.md):
#   CLUSTER       backend script (default cluster.sh next to this one;
#                 k8s/kind-cluster.sh for Kubernetes)
#   PARTITIONS RATE PRODUCERS BATCH SECONDS_RUN RESEND_RATE DURABILITY
#                 topic and load shape
#   KILL_GAP DOWN_SECONDS SECOND_KILL_DELAY
#                 seconds before each kill, seconds a killed node stays down,
#                 cascading-crash: seconds between the two kills
#   MEASURE_SECONDS P99_LIMIT_MS
#                 split-primaries: clean produce phase after the restarts,
#                 and the p99 send latency it must stay under
#   KEEP=1        leave the cluster up afterwards
#   KEEP_WORK=1   keep the work directory (produce logs, node logs) on success
# plus everything cluster.sh reads (IMAGE, CHECKER_IMAGE, NODE_CPUS, ...).

set -Eeuo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCENARIO=${1:-}
RESULTS=${2:-${TMPDIR:-/tmp}/cluster-chaos-results}
CLUSTER=${CLUSTER:-$HERE/cluster.sh}

case "$SCENARIO" in
  baseline | crash-restart | rolling-restart | isolate | pause | kill-storm) ;;
  control) : "${RESEND_RATE:=0.2}" ;;
  full-outage) : "${DURABILITY:=persisted}" ;;
  full-outage-replicated) : "${DURABILITY:=replicated}" ;;
  rolling-crash)
    : "${PARTITIONS:=1}" "${RATE:=30000}" "${PRODUCERS:=12}" "${BATCH:=100}"
    : "${SECONDS_RUN:=360}" "${KILL_GAP:=80}" "${DOWN_SECONDS:=40}" "${DURABILITY:=persisted}"
    ;;
  split-primaries) : "${SECONDS_RUN:=110}" ;;
  cascading-crash) : "${DURABILITY:=persisted}" "${DOWN_SECONDS:=5}" ;;
  *)
    sed -n '/^# Usage:/,/^# plus/s/^# \{0,1\}//p' "$0" >&2
    exit 2
    ;;
esac
: "${PARTITIONS:=6}" "${RATE:=2000}" "${PRODUCERS:=6}" "${BATCH:=20}" "${SECONDS_RUN:=120}"
: "${RESEND_RATE:=0}" "${DURABILITY:=replicated}" "${KILL_GAP:=15}" "${DOWN_SECONDS:=15}"
: "${MEASURE_SECONDS:=60}" "${P99_LIMIT_MS:=1000}" "${SECOND_KILL_DELAY:=6}"
for setting in PARTITIONS RATE PRODUCERS BATCH SECONDS_RUN RESEND_RATE KILL_GAP DOWN_SECONDS \
  MEASURE_SECONDS P99_LIMIT_MS SECOND_KILL_DELAY; do
  [[ ${!setting} =~ ^[0-9]+(\.[0-9]+)?$ ]] || { echo "$setting=${!setting} is not a number" >&2; exit 2; }
done

WORK=$(mktemp -d "${TMPDIR:-/tmp}/cluster-chaos-$SCENARIO.XXXXXX")
export WORK
STAMP=$(date -u +%Y%m%dT%H%M%SZ)

log() { echo "[$(date -u +%H:%M:%S)] $SCENARIO: $*"; }
# A fault that cannot be applied (for example killing a node that already
# died on its own) is logged, and the run goes on to collect the evidence.
step() { log "$*"; "$CLUSTER" "$@" || log "$* failed"; }
checker() { "$CLUSTER" checker "$@"; }
save_logs() {
  for node in 0 1 2; do "$CLUSTER" logs "$node" >"$WORK/node-$node.log" 2>&1 || true; done
}

# On an early exit, stop the producers and save the node logs before `down`
# removes the containers.
finished=
trap '[ -n "${producer:-}" ] && kill "$producer" 2>/dev/null
  [ -n "$finished" ] || { save_logs; log "aborted, work directory kept: $WORK"; }
  [ -n "${KEEP:-}" ] || "$CLUSTER" down >/dev/null 2>&1 || true' EXIT

crash_each_node() {
  for node in 0 1 2; do
    sleep "$KILL_GAP"; step kill "$node"
    sleep "$DOWN_SECONDS"; step start "$node"
  done
}

inject_faults() {
  case "$SCENARIO" in
    baseline | control) ;;
    crash-restart | rolling-crash | split-primaries) crash_each_node ;;
    cascading-crash)
      # A second node dies while the view changes the first kill started are
      # still running, and both come back together.
      for pair in "0 1" "1 2" "2 0"; do
        read -r first second <<<"$pair"
        sleep "$KILL_GAP"; step kill "$first"
        sleep "$SECOND_KILL_DELAY"; step kill "$second"
        sleep "$DOWN_SECONDS"; step start "$first"; step start "$second"
      done ;;
    rolling-restart)
      for node in 0 1 2; do sleep 15; step stop "$node"; sleep 5; step start "$node"; done ;;
    isolate)
      for node in 0 1; do sleep 15; step isolate "$node"; sleep 20; step heal "$node"; done ;;
    pause)
      for node in 1 2; do sleep 15; step pause "$node"; sleep 20; step unpause "$node"; done ;;
    full-outage | full-outage-replicated)
      sleep 20
      for node in 0 1 2; do step kill "$node"; done
      sleep "$DOWN_SECONDS"
      for node in 0 1 2; do step start "$node"; done ;;
    kill-storm)
      for node in 2 0 1 2; do sleep 12; step kill "$node"; sleep 8; step start "$node"; done ;;
  esac
}

mkdir -p "$RESULTS"
"$CLUSTER" up
ADDRESSES=$("$CLUSTER" addresses)
target=(--addresses "$ADDRESSES")
checker setup setup "${target[@]}" --partitions "$PARTITIONS" --durability "$DURABILITY"
log "topic: $PARTITIONS partitions, durability $DURABILITY; load: $RATE events/s from $PRODUCERS producers for ${SECONDS_RUN}s, batch $BATCH, resend rate $RESEND_RATE"

checker produce-a produce "${target[@]}" --out /work --run a --seconds "$SECONDS_RUN" \
  --rate "$RATE" --producers "$PRODUCERS" --batch "$BATCH" --resend-rate "$RESEND_RATE" \
  >"$WORK/produce-a.log" 2>&1 &
producer=$!
inject_faults
produce_status=0
wait "$producer" || produce_status=$?
producer=
log "producers exited $produce_status"

# Undo whatever the scenario left behind, then wait until the cluster serves
# the topic again (setup is idempotent).
for node in 0 1 2; do
  "$CLUSTER" unpause "$node" >/dev/null 2>&1 || true
  "$CLUSTER" heal "$node" >/dev/null 2>&1 || true
  "$CLUSTER" start "$node" >/dev/null 2>&1 || true
done
checker ready setup "${target[@]}" --partitions "$PARTITIONS" --durability "$DURABILITY" >/dev/null \
  || log "the cluster did not serve the topic again"

if [ "$SCENARIO" = split-primaries ]; then
  log "measuring sends for ${MEASURE_SECONDS}s after the restarts"
  checker produce-b produce "${target[@]}" --out /work --run b --seconds "$MEASURE_SECONDS" \
    --rate "$RATE" --producers "$PRODUCERS" --batch "$BATCH" >"$WORK/produce-b.log" 2>&1 || true
fi

log "verifying"
verify_status=0
checker verify verify "${target[@]}" --out /work --summary /work/verify.json \
  >"$WORK/verify.log" 2>&1 || verify_status=$?
down_nodes=0
for node in 0 1 2; do
  "$CLUSTER" running "$node" || { down_nodes=$((down_nodes + 1)); log "node $node is not running"; }
done
save_logs
finished=1
panics=$(cat "$WORK"/node-*.log | grep -c -E 'panicked at|shard thread returned error' || true)
state_transfer_refusals=$(cat "$WORK"/node-*.log | grep -c 'needs state transfer' || true)

if [ ! -s "$WORK/verify.json" ]; then
  log "verifier produced no summary (exit $verify_status), see $WORK/verify.log"
  tail -20 "$WORK/verify.log" >&2
  exit 1
fi
[ -s "$WORK/produce-b.json" ] || echo null >"$WORK/produce-b.json"
[ -s "$WORK/produce-a.json" ] || echo '{"producer_failures":["no summary"]}' >"$WORK/produce-a.json"

result="$RESULTS/$SCENARIO-$STAMP.json"
jq -n \
  --arg scenario "$SCENARIO" --arg image "${IMAGE:-apache/iggy:edge}" \
  --argjson partitions "$PARTITIONS" --argjson rate "$RATE" --argjson producers "$PRODUCERS" \
  --argjson batch "$BATCH" --argjson seconds "$SECONDS_RUN" --argjson resend_rate "$RESEND_RATE" \
  --arg durability "$DURABILITY" --argjson p99_limit_ms "$P99_LIMIT_MS" \
  --argjson panics "$panics" --argjson down_nodes "$down_nodes" --argjson state_transfer_refusals "$state_transfer_refusals" \
  --slurpfile produce "$WORK/produce-a.json" --slurpfile measure "$WORK/produce-b.json" \
  --slurpfile verify "$WORK/verify.json" '
  ($verify[0]) as $v | ($produce[0]) as $p | ($measure[0]) as $m |
  ($scenario == "full-outage-replicated") as $loss_allowed |
  [
    (if ($p.producer_failures | length) > 0 then "producers gave up: \($p.producer_failures[0])" else empty end),
    (if $v.lost_acked > 0 and ($loss_allowed | not) then "\($v.lost_acked) acknowledged events lost" else empty end),
    (if $v.read_incomplete then "reads did not settle (\($v.poll_errors) failed polls)" else empty end),
    (if $v.unreadable_tail > 0 then "\($v.unreadable_tail) listed offsets unreadable" else empty end),
    (if $v.offset_gaps > 0 then "\($v.offset_gaps) offset gaps" else empty end),
    (if $v.reordered > 0 then "\($v.reordered) events out of order" else empty end),
    (if $v.misplaced > 0 then "\($v.misplaced) events in the wrong partition" else empty end),
    (if $v.phantoms > 0 then "\($v.phantoms) events nobody sent" else empty end),
    (if $panics > 0 then "\($panics) panics or fatal shard errors in node logs" else empty end),
    (if $down_nodes > 0 then "\($down_nodes) nodes not running after the heal" else empty end),
    (if $scenario == "control" and $v.duplicates < $p.resent_events
      then "control: \($p.resent_events) events were resent but the verifier saw only \($v.duplicates) duplicates" else empty end),
    (if $scenario == "split-primaries" and ($m == null or ($m.producer_failures | length) > 0 or $m.sends == 0)
      then "the measure phase after the restarts did not complete" else empty end),
    (if $m != null and $m.send_latency_ms.p99 > $p99_limit_ms
      then "send p99 \($m.send_latency_ms.p99) ms after the restarts (limit \($p99_limit_ms) ms), \($m.events_per_second // 0 | floor) events/s"
      else empty end)
  ] as $failures |
  {
    scenario: $scenario, image: $image, passed: ($failures | length == 0), failures: $failures,
    settings: {partitions: $partitions, rate: $rate, producers: $producers, batch: $batch,
      seconds: $seconds, resend_rate: $resend_rate, durability: $durability},
    info: {
      duplicates: $v.duplicates, unacked_but_present: $v.unacked_but_present,
      lost_acked: $v.lost_acked, empty_polls_mid_log: $v.empty_polls_mid_log,
      topic_stats_mismatches: $v.topic_stats_mismatches,
      state_transfer_refusals: $state_transfer_refusals
    },
    produce: $p, measure: $m, verify: ($v | del(.violations))
  }' >"$result"

jq -r '"result: \(if .passed then "PASS" else "FAIL" end)",
  (.failures[] | "  failure: \(.)"),
  "  produced \(.produce.new_events // 0) new + \(.produce.resent_events // 0) resent events, \(.produce.events_per_second // 0 | floor)/s, send p99 \(.produce.send_latency_ms.p99 // 0) ms",
  (if .measure != null then "  after restarts: \(.measure.events_per_second // 0 | floor)/s, send p99 \(.measure.send_latency_ms.p99 // 0) ms" else empty end),
  "  info: \(.info | to_entries | map("\(.key)=\(.value)") | join(" "))"' "$result"
log "summary in $result"
if jq -e .passed "$result" >/dev/null; then
  if [ -n "${KEEP_WORK:-}" ]; then log "work directory $WORK"; else rm -rf "$WORK"; fi
  exit 0
fi
jq -r '.violations[]? | "  " + .' "$WORK/verify.json" | head -20
log "work directory kept: $WORK"
exit 1
