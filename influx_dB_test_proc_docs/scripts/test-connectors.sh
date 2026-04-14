#!/usr/bin/env bash
# test-connectors.sh — End-to-end test of all five InfluxDB connector scenarios.
#
# Run this on the iggy-server GCP VM after:
#   1. iggy-server is running on localhost:8090
#   2. connectors-runtime is running with the appropriate connector enabled
#   3. All streams/topics are created (see Section 5.3 of the deployment guide)
#
# Usage:
#   ./test-connectors.sh <influx2-internal-ip> <influx2-token> <influx3-internal-ip>
#
# Example:
#   ./test-connectors.sh 10.128.0.2 my-influx2-token 10.128.0.3

set -euo pipefail

# ── Arguments ──────────────────────────────────────────────────────────────────
INFLUX2_IP="${1:?Usage: $0 <influx2-ip> <influx2-token> <influx3-ip>}"
INFLUX2_TOKEN="${2:?Usage: $0 <influx2-ip> <influx2-token> <influx3-ip>}"
INFLUX3_IP="${3:?Usage: $0 <influx2-ip> <influx2-token> <influx3-ip>}"

# ── Tunable defaults ───────────────────────────────────────────────────────────
IGGY_DIR="${IGGY_DIR:-$HOME/iggy}"
IGGY_CLI="$IGGY_DIR/target/release/iggy-cli"
IGGY_ARGS="--transport tcp --server-address localhost:8090"
POLL_ATTEMPTS="${POLL_ATTEMPTS:-30}"
POLL_DELAY="${POLL_DELAY:-2}"

# ── Colour helpers ─────────────────────────────────────────────────────────────
OK()   { printf '\033[32m[PASS]\033[0m %s\n' "$*"; }
FAIL() { printf '\033[31m[FAIL]\033[0m %s\n' "$*" >&2; exit 1; }
INFO() { printf '\033[33m[INFO]\033[0m %s\n' "$*"; }
STEP() { printf '\n\033[1;34m══ %s ══\033[0m\n' "$*"; }

# ── Poll helper ────────────────────────────────────────────────────────────────
# Retries a command until it produces non-empty stdout or the attempt limit is hit.
poll_until() {
    local desc="$1"; shift
    for i in $(seq 1 "$POLL_ATTEMPTS"); do
        local out
        out=$("$@" 2>/dev/null) && [ -n "$out" ] && echo "$out" && return 0
        INFO "  waiting ($i/$POLL_ATTEMPTS): $desc"
        sleep "$POLL_DELAY"
    done
    return 1
}

# ── Iggy helpers ───────────────────────────────────────────────────────────────
iggy_send() {
    local stream="$1" topic="$2" payload="$3"
    "$IGGY_CLI" $IGGY_ARGS message send "$stream" "$topic" - <<< "$payload"
}

iggy_poll() {
    local stream="$1" topic="$2" count="${3:-10}"
    "$IGGY_CLI" $IGGY_ARGS message poll "$stream" "$topic" 1 0 "$count" 2>/dev/null
}

# ── Prerequisite checks ────────────────────────────────────────────────────────
STEP "Prerequisites"

INFO "Checking InfluxDB 2 ($INFLUX2_IP:8086)..."
curl -sf "http://$INFLUX2_IP:8086/health" | grep -q '"status":"pass"' \
  || FAIL "InfluxDB 2 health check failed"
OK "InfluxDB 2 healthy"

INFO "Checking InfluxDB 3 ($INFLUX3_IP:8181)..."
curl -sf "http://$INFLUX3_IP:8181/health" >/dev/null \
  || FAIL "InfluxDB 3 health check failed"
OK "InfluxDB 3 healthy"

INFO "Checking Iggy server (localhost:8090)..."
"$IGGY_CLI" $IGGY_ARGS stream list >/dev/null 2>&1 \
  || FAIL "Iggy server not reachable"
OK "Iggy server reachable"

INFO "Checking required topics exist in iggy-stream..."
TOPICS=$("$IGGY_CLI" $IGGY_ARGS topic list iggy-stream 2>/dev/null)
for t in iggy-topic iggy-topic-from-influx2 iggy-topic-from-influx3; do
    echo "$TOPICS" | grep -q "$t" \
      || FAIL "Topic '$t' missing — run Section 5.3 of deployment guide"
done
OK "All required topics present"

PASS_COUNT=0
FAIL_COUNT=0

run_test() {
    local name="$1"; shift
    STEP "$name"
    if "$@"; then
        OK "$name PASSED"
        ((PASS_COUNT++)) || true
    else
        printf '\033[31m[FAIL]\033[0m %s FAILED\n' "$name" >&2
        ((FAIL_COUNT++)) || true
    fi
}

# ── Test 1: V2 Sink ────────────────────────────────────────────────────────────
test_v2_sink() {
    INFO "Connector: influxdb_sink_v2.toml (version=v2 → /api/v2/write on InfluxDB 2)"
    INFO "Ensure this connector is the only one enabled in the runtime, then press ENTER."
    read -r

    local test_id="sink-v2-$$"
    iggy_send iggy-stream iggy-topic \
      "{\"measurement\":\"iggy_messages\",\"value\":1001,\"test_id\":\"$test_id\"}"
    INFO "Message published to Iggy. Polling InfluxDB 2..."

    local flux="from(bucket:\"iggy-sink-bucket\") |> range(start:-5m) |> filter(fn:(r)=>r._measurement==\"iggy_messages\")"
    local result
    result=$(poll_until "V2 sink point in InfluxDB 2" \
      curl -sf "http://$INFLUX2_IP:8086/api/v2/query?org=iggy-org" \
        -H "Authorization: Token $INFLUX2_TOKEN" \
        -H "Content-Type: application/json" \
        -d "{\"query\":\"$flux\",\"type\":\"flux\"}"
    ) || { INFO "No point appeared after $((POLL_ATTEMPTS * POLL_DELAY))s"; return 1; }

    echo "$result" | grep -q "iggy_messages" || { INFO "Measurement not in response"; return 1; }
}

# ── Test 2: V2 Source ──────────────────────────────────────────────────────────
test_v2_source() {
    INFO "Connector: influxdb_source_v2.toml (version=v2 → Flux queries on InfluxDB 2)"
    INFO "Ensure this connector is the only one enabled in the runtime, then press ENTER."
    read -r

    local ts_ns test_id
    ts_ns=$(date +%s%N)
    test_id="src-v2-$$"

    curl -sf "http://$INFLUX2_IP:8086/api/v2/write?org=iggy-org&bucket=iggy-sink-bucket&precision=ns" \
      -H "Authorization: Token $INFLUX2_TOKEN" \
      -H "Content-Type: text/plain; charset=utf-8" \
      --data-binary "sensor_readings,host=gcp-v2,test_id=$test_id value=22.3 $ts_ns"
    INFO "Written sensor_readings to InfluxDB 2 (ns=$ts_ns). Polling Iggy..."

    local result
    result=$(poll_until "V2 source message in Iggy" \
      iggy_poll iggy-stream iggy-topic-from-influx2 10
    ) || { INFO "No message in iggy-topic-from-influx2 after $((POLL_ATTEMPTS * POLL_DELAY))s"; return 1; }

    echo "$result" | grep -qE "sensor_readings|value|22" \
      || { INFO "Expected payload not found in message"; return 1; }
}

# ── Test 3: V3 Sink ────────────────────────────────────────────────────────────
test_v3_sink() {
    INFO "Connector: influxdb_sink_v3.toml (version=v3 → /api/v3/write_lp on InfluxDB 3)"
    INFO "Ensure this connector is the only one enabled in the runtime, then press ENTER."
    read -r

    local test_id="sink-v3-$$"
    iggy_send iggy-stream iggy-topic \
      "{\"measurement\":\"iggy_messages\",\"value\":3003,\"test_id\":\"$test_id\"}"
    INFO "Message published to Iggy. Polling InfluxDB 3..."

    local result
    result=$(poll_until "V3 sink row in InfluxDB 3" \
      curl -sf "http://$INFLUX3_IP:8181/api/v3/query_sql" \
        -H "Content-Type: application/json" \
        -d '{"db":"iggy-db","q":"SELECT * FROM iggy_messages LIMIT 5","format":"jsonl"}'
    ) || { INFO "No row in InfluxDB 3 after $((POLL_ATTEMPTS * POLL_DELAY))s"; return 1; }

    echo "$result" | grep -qE "iggy_messages|3003" \
      || { INFO "Expected row not found in InfluxDB 3"; return 1; }
}

# ── Test 4: V3 Source ──────────────────────────────────────────────────────────
test_v3_source() {
    INFO "Connector: influxdb_source_v3.toml (version=v3 → SQL queries on InfluxDB 3)"
    INFO "Ensure this connector is the only one enabled in the runtime, then press ENTER."
    read -r

    local ts_ns test_id
    ts_ns=$(date +%s%N)
    test_id="src-v3-$$"

    curl -sf "http://$INFLUX3_IP:8181/api/v3/write_lp?db=iggy-db&precision=ns" \
      -H "Content-Type: text/plain; charset=utf-8" \
      --data-binary "iggy_messages,host=gcp-v3,test_id=$test_id value=77.7 $ts_ns"
    INFO "Written iggy_messages to InfluxDB 3 (ns=$ts_ns). Polling Iggy..."

    local result
    result=$(poll_until "V3 source message in Iggy" \
      iggy_poll iggy-stream iggy-topic-from-influx3 10
    ) || { INFO "No message in iggy-topic-from-influx3 after $((POLL_ATTEMPTS * POLL_DELAY))s"; return 1; }

    echo "$result" | grep -qE "iggy_messages|77" \
      || { INFO "Expected payload not found in message"; return 1; }
}

# ── Test 5: V2 Protocol on V3 Backend ─────────────────────────────────────────
test_v2_on_v3_sink() {
    INFO "Connector: influxdb_sink_v2_on_v3.toml (version=v2 → /api/v2/write on InfluxDB 3)"
    INFO "Ensure this connector is the only one enabled in the runtime, then press ENTER."
    read -r

    local test_id="v2on3-$$"
    iggy_send iggy-stream iggy-topic \
      "{\"measurement\":\"iggy_messages_v2_compat\",\"value\":5005,\"test_id\":\"$test_id\"}"
    INFO "Message published to Iggy. Polling InfluxDB 3 for V2-compat table..."

    local result
    result=$(poll_until "V2-on-V3 row in InfluxDB 3" \
      curl -sf "http://$INFLUX3_IP:8181/api/v3/query_sql" \
        -H "Content-Type: application/json" \
        -d '{"db":"iggy-db","q":"SELECT * FROM iggy_messages_v2_compat LIMIT 5","format":"jsonl"}'
    ) || { INFO "No row in iggy_messages_v2_compat after $((POLL_ATTEMPTS * POLL_DELAY))s"; return 1; }

    echo "$result" | grep -qE "iggy_messages_v2_compat|5005" \
      || { INFO "Expected row not found in InfluxDB 3"; return 1; }
}

# ── Run all tests ──────────────────────────────────────────────────────────────
run_test "TEST 1: V2 Sink (Iggy → InfluxDB 2)"          test_v2_sink
run_test "TEST 2: V2 Source (InfluxDB 2 → Iggy)"        test_v2_source
run_test "TEST 3: V3 Sink (Iggy → InfluxDB 3)"          test_v3_sink
run_test "TEST 4: V3 Source (InfluxDB 3 → Iggy)"        test_v3_source
run_test "TEST 5: V2 Protocol Sink on InfluxDB 3"       test_v2_on_v3_sink

# ── Summary ────────────────────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════"
if [ "$FAIL_COUNT" -eq 0 ]; then
    OK "All $PASS_COUNT tests PASSED"
else
    printf '\033[32m[PASS]\033[0m %d passed\n' "$PASS_COUNT"
    printf '\033[31m[FAIL]\033[0m %d failed\n' "$FAIL_COUNT"
    exit 1
fi
echo "══════════════════════════════════════════════"
