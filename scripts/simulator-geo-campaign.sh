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

# Run `workload-fuzz --geo` for every seed from FIRST to LAST and sort the
# runs into passed, vacuous and failed.
#
# Usage: scripts/simulator-geo-campaign.sh FIRST LAST OUT_DIR [JOBS]
#
# The log of a failed run stays in OUT_DIR as SEED.log. The logs of the other
# runs are removed. OUT_DIR/results.txt has one line per seed, and the script
# prints a summary with the first panic of each failed run. It exits with 1
# when a run failed.
#
# Environment:
#   BIN         the workload-fuzz binary (target/fuzz/workload-fuzz)
#   FAULTS      the --faults profile (swarm)
#   TICKS       steps of each run before the faults heal (8000)
#   CLIENTS     clients of each cluster (2)
#   REPLICAS    replicas of each cluster (3)
#   PARTITIONS  partitions of each geo topic (2)
#   REPLAY      when 1, run each seed twice and fail a seed whose two runs
#               report different results, as a run that does not replay its
#               seed (0)

set -euo pipefail

if [[ $# -lt 3 || $# -gt 4 ]]; then
    echo "usage: $0 FIRST LAST OUT_DIR [JOBS]" >&2
    exit 2
fi

FIRST=$1
LAST=$2
OUT_DIR=$3
JOBS=${4:-$(nproc)}

export BIN=${BIN:-target/fuzz/workload-fuzz}
export FAULTS=${FAULTS:-swarm}
export TICKS=${TICKS:-8000}
export CLIENTS=${CLIENTS:-2}
export REPLICAS=${REPLICAS:-3}
export PARTITIONS=${PARTITIONS:-2}
export REPLAY=${REPLAY:-0}
export OUT_DIR

if [[ ! -x $BIN ]]; then
    echo "no workload-fuzz binary at $BIN; build it with" >&2
    echo "  cargo build --profile fuzz -p simulator --bin workload-fuzz" >&2
    exit 2
fi
mkdir -p "$OUT_DIR"

run_once() {
    "$BIN" --geo --seed "$1" --faults "$FAULTS" --ticks "$TICKS" \
        --clients "$CLIENTS" --replicas "$REPLICAS" \
        --geo-partitions "$PARTITIONS"
}
export -f run_once

run_seed() {
    local seed=$1
    local log="$OUT_DIR/$seed.log"
    local code=0
    run_once "$seed" >"$log" 2>&1 || code=$?
    if [[ $code -eq 0 && $REPLAY -eq 1 ]]; then
        local replay="$OUT_DIR/$seed.replay.log"
        run_once "$seed" >"$replay" 2>&1 || true
        if ! cmp -s <(grep '^geo report' "$log") <(grep '^geo report' "$replay"); then
            echo "$seed FAIL replay"
            return
        fi
        rm -f "$replay"
    fi
    case $code in
        0)
            echo "$seed PASS"
            rm -f "$log"
            ;;
        3)
            echo "$seed VACUOUS"
            rm -f "$log"
            ;;
        *)
            echo "$seed FAIL $code"
            ;;
    esac
}
export -f run_seed

# The child shell expands "$1", which is the seed that xargs hands it.
# shellcheck disable=SC2016
seq "$FIRST" "$LAST" |
    xargs -P "$JOBS" -I{} bash -c 'run_seed "$1"' _ {} |
    sort -n >"$OUT_DIR/results.txt"

passed=$(grep -c ' PASS$' "$OUT_DIR/results.txt" || true)
vacuous=$(grep -c ' VACUOUS$' "$OUT_DIR/results.txt" || true)
failed=$(grep -c ' FAIL ' "$OUT_DIR/results.txt" || true)
echo "seeds $FIRST..$LAST: $passed passed, $vacuous vacuous, $failed failed"

while read -r seed _ code; do
    if [[ $code == replay ]]; then
        reason="two runs of the seed reported different results"
    else
        reason=$(grep -m1 -A1 'panicked at' "$OUT_DIR/$seed.log" | tail -n1 || true)
    fi
    echo "  seed $seed ($code): $reason"
done < <(grep ' FAIL ' "$OUT_DIR/results.txt" || true)

[[ $failed -eq 0 ]]
