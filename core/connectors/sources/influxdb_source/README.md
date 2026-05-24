# InfluxDB Source Connector

A source connector that polls InfluxDB and produces messages into Iggy streams. Supports both InfluxDB V2 (OSS 2.x / Cloud 2.x, Flux queries) and InfluxDB V3 (Core / Enterprise, SQL queries).

## V2 vs V3 Differences

| Aspect | InfluxDB V2 | InfluxDB V3 |
| --- | --- | --- |
| Data organisation | `org` (query param) | `db` |
| Query endpoint | `POST /api/v2/query` | `POST /api/v3/query_sql` |
| Query language | Flux | SQL |
| Response format | Annotated CSV | JSONL (one JSON object per line) |
| Auth header | `Authorization: Token {t}` | `Authorization: Bearer {t}` |
| Cursor semantics | `>= $cursor` (inclusive) | `> '$cursor'` (exclusive) |
| Default cursor field | `_time` | `time` |
| Config `version` key | `"v2"` (or omit — default) | `"v3"` |
| Payload value types | All fields as strings (CSV has no types); `parse_scalar` coerces to bool/int/float where possible | Native JSON types: numbers, booleans, and nulls preserved as-is from SQL |

> **Breaking schema note for consumers migrating V2 → V3**: a field that arrives as the string `"42"` under V2 will arrive as the integer `42` under V3. Update consumer deserialization accordingly.

## Cursor-Based Polling

Both versions use an RFC 3339 timestamp cursor stored in persistent connector state. On each poll the cursor is substituted into the query template via the `$cursor` and `$limit` placeholders; after a successful batch the cursor advances to the highest timestamp seen.

### V2 Cursor Semantics

V2 Flux queries use `>= $cursor` (inclusive). Rows at the cursor timestamp are re-delivered by the first query after a cursor advance and must be skipped. The connector tracks `cursor_row_count` in persisted state and skips exactly that many leading rows to prevent duplicate delivery.

The Flux query **must** sort by the cursor field when using `>=` semantics. The connector enforces this at startup and returns an error if `>=` is present without a `|> sort(columns: ["_time"])` call.

### V3 Cursor Semantics

V3 SQL uses strict `WHERE time > '$cursor'` — no rows are re-delivered across batch boundaries. The query **must** include `OFFSET $offset` when `stuck_batch_cap_factor > 0` (the default). The connector validates this at startup.

#### Stuck-Timestamp Handling (V3)

InfluxDB 3's DataFusion engine does not guarantee stable row ordering for rows that share the same timestamp. If a full batch is returned and all rows share the same timestamp, the cursor cannot advance. The connector handles this by:

1. Doubling `effective_batch_size` on each stuck poll, up to `stuck_batch_cap_factor × batch_size`.
2. Using `OFFSET $offset` to page through the tied rows without re-delivering earlier ones.
3. Tripping the circuit breaker when the cap is reached and resetting `effective_batch_size` to `batch_size` after the cool-down so the connector recovers cleanly.

## Configuration

### V2 — InfluxDB OSS 2.x / Cloud

```toml
version  = "v2"          # optional — omitting defaults to v2
url      = "http://localhost:8086"
org      = "my-org"
token    = "my-token"
query    = '''
  from(bucket: "telemetry")
    |> range(start: time(v: "$cursor"))
    |> filter(fn: (r) => r._measurement == "cpu")
    |> sort(columns: ["_time"])
    |> limit(n: $limit)
'''

# Optional
poll_interval   = "5s"        # how often to issue queries (default: "5s")
batch_size      = 500         # max rows per query (default: 500)
cursor_field    = "_time"     # column used as the cursor (default: "_time")
initial_offset  = "2024-01-01T00:00:00Z"  # starting cursor on first run
payload_column  = ""          # extract a single column as payload (default: whole row as JSON)
payload_format  = "json"      # json | text | raw (default: "json")
include_metadata = true       # include all row columns in the payload (default: true)
verbose_logging  = false
```

### V3 — InfluxDB 3.x Core / Enterprise

```toml
version = "v3"
url     = "http://localhost:8181"
db      = "my-db"
token   = "my-token"
query   = '''
  SELECT * FROM cpu
  WHERE time > '$cursor'
  ORDER BY time
  LIMIT $limit OFFSET $offset
'''

# Optional
poll_interval        = "5s"
batch_size           = 500
cursor_field         = "time"    # default cursor column for V3 (default: "time")
initial_offset       = "2024-01-01T00:00:00Z"
payload_column       = ""
payload_format       = "json"
include_metadata     = true
stuck_batch_cap_factor = 10      # max effective_batch = 10 × batch_size (default: 10, max: 100)
                                 # set to 0 to disable stuck-batch detection entirely;
                                 # $offset is not required in the query when cap = 0
verbose_logging      = false
```

### Resilience Fields (both versions)

```toml
timeout                   = "10s"   # per-request timeout
max_retries               = 3       # retries per query on transient errors (429/5xx)
retry_delay               = "1s"    # initial backoff
retry_max_delay           = "5s"    # backoff cap
max_open_retries          = 10      # retries during open() health check
open_retry_max_delay      = "60s"   # backoff cap for open() retries
circuit_breaker_threshold = 5       # consecutive failures before circuit trips
circuit_breaker_cool_down = "30s"   # cool-down before half-open probe
```

## Query Template Placeholders

| Placeholder | Substituted with |
| --- | --- |
| `$cursor` | Current cursor value (RFC 3339 timestamp) |
| `$limit` | Current effective batch size |
| `$offset` | Row offset within the current cursor group (V3 only; required when `stuck_batch_cap_factor > 0`) |

## Payload Formats

- **`json`** (default): Behaviour differs by version.
  - **V3**: Each row is a flat JSON object. `include_metadata = false` excludes the cursor column (`time`); all other columns are present. `include_metadata = true` (the default) includes all columns.
  - **V2**: Each row is wrapped in an envelope: `{"measurement":"..","field":"..","timestamp":"..","value":..,"row":{..}}`. The `row` object always includes `_time` and `_value`. `include_metadata = false` additionally excludes `_measurement` and `_field` from `row`; `include_metadata = true` includes them.
- **`text`**: The value of `payload_column` is written as a UTF-8 string. Requires `payload_column` to be set.
- **`raw`**: The value of `payload_column` is base64-decoded and written as raw bytes. Requires `payload_column` to be set.

## Full Configuration Example

```toml
[[sources]]
key     = "influxdb-source"
enabled = true
path    = "target/release/libiggy_connector_influxdb_source"

[[sources.streams]]
stream       = "metrics"
topic        = "cpu"
schema       = "json"
batch_length = 100
linger_time  = "10ms"

[sources.plugin_config]
version        = "v3"
url            = "http://localhost:8181"
db             = "telemetry"
token          = "my-secret-token"
query          = """
  SELECT * FROM cpu
  WHERE time > '$cursor'
  ORDER BY time
  LIMIT $limit OFFSET $offset
"""
poll_interval          = "10s"
batch_size             = 1000
initial_offset         = "2024-01-01T00:00:00Z"
stuck_batch_cap_factor = 10
circuit_breaker_threshold = 3
circuit_breaker_cool_down = "15s"
```

## Architecture Notes

The source uses a layered design with version-specific modules:

- **`v2` module**: Flux query construction, annotated-CSV response parsing, skip-N deduplication for `>=` cursor semantics.
- **`v3` module**: SQL query construction, JSONL response parsing, stuck-batch detection and cap inflation.
- **`common` module**: Shared cursor state management, payload format handling, retry/circuit-breaker logic, and the `RowContext` passed into both `process_rows` functions.

Persisted state (via the connector runtime's state store) holds the last cursor value and, for V2, the `cursor_row_count` needed to skip duplicate rows. State is versioned so V2 and V3 states are never confused on a connector restart.

### V2 vs V3 API Comparison

| Concern | InfluxDB V2 | InfluxDB V3 |
| --- | --- | --- |
| Write body format | Line Protocol | Line Protocol |
| Health check | `GET /health` | `GET /health` |
| Retry triggers | 429 / 5xx | 429 / 5xx |
| Cursor field default | `_time` | `time` |
| Timestamp format | RFC 3339 with TZ | RFC 3339 (TZ appended by connector if absent) |
| Sort guarantee | Flux <code>\|> sort(...)</code> explicit | DataFusion — no stable order for tied timestamps |
