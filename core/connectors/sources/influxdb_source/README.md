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
| Config `version` key | `"v2"` (or omit, the default) | `"v3"` |
| Payload value types | CSV cells are strings; annotation types/defaults are ignored and payload conversion infers scalars | Native JSON types: numbers, booleans, and nulls preserved as-is from SQL |

> **Schema note for consumers migrating V2 to V3**: a string field containing `"42"` becomes the number `42` in V2 whole-row JSON, but remains a string in V3. V2 also uses an envelope while V3 emits a flat row. Update consumer deserialization accordingly.

## Cursor-Based Polling

Both versions use an RFC3339 timestamp cursor and row position in persistent connector state. Polling stages a candidate checkpoint; `on_batch_result(Ack)` applies it and `Nack` discards it. The runtime acknowledges after forwarding messages and saving the checkpoint. Retries can still duplicate messages after a forwarding/checkpoint failure.

Queries must return ascending timestamps and a stable, unique order within each timestamp group. Include the necessary tag or key columns in the sort. A timestamp watermark does not track deletions and can miss late inserts or updates at or below its saved position. Changing the order of tied rows invalidates skip/offset progress.

### V2 Cursor Semantics

V2 queries should use an inclusive cursor, such as `range(start: time(v: "$cursor"))`. The connector persists `cursor_row_count` and skips that many matching rows when it queries the saved timestamp again. Flux sorting and limiting operate per table. Group compatible rows into one table before sorting and limiting; the sample selects one numeric field and sorts by time and its only tag, `host`.

Startup rejects a `>=` cursor expression without a `sort(` call, but that text check does not validate ordering and does not cover a `range()` expression. `$limit` is the base batch size plus the skip count, with the extra count capped at ten times the base. Dense timestamp groups can exhaust this bound and cause a poll error without advancing the cursor. Using strict `>` bypasses rows that the inclusive skip scheme needs to revisit.

### V3 Cursor Semantics

V3 requires strict `WHERE time > '$cursor'`; inclusive `>=` cursor expressions are rejected. With `stuck_batch_cap_factor > 0`, startup also requires `$offset` and an ascending `ORDER BY`. These checks are textual and cannot establish a stable tiebreaker order.

#### Stuck-Timestamp Handling (V3)

A full batch containing one timestamp retains the previous cursor, emits its rows, advances `$offset`, and doubles the next effective batch size up to `stuck_batch_cap_factor` times the base size. Offset pagination still requires stable ordering of tied rows.

A full batch with mixed timestamps emits only rows before the maximum timestamp. It advances to the second-highest timestamp, resets the effective size and offset, and re-fetches the deferred timestamp group on the next poll.

At the inflation cap, the connector emits no messages, resets the effective size to the base, preserves its offset and records a circuit-breaker failure. That failure opens the breaker only if its configured threshold is reached. Setting the cap factor to `0` disables both guards and can skip unseen ties.

## Configuration

Build the plugin and runtime from the same 0.9.0/edge checkout. The following snippets are plugin settings; place them under `[plugin_config]` in the full connector entry below. The example data has a numeric `usage` field and `host` as its only tag. Use an existing bucket/database and a token with query access. Unknown settings and unsupported versions are rejected.

### V2 - InfluxDB OSS 2.x / Cloud

```toml
version  = "v2"          # optional; omitting defaults to v2
url      = "http://localhost:8086"
org      = "my-org"
token    = "my-token"
query    = '''
  from(bucket: "telemetry")
    |> range(start: time(v: "$cursor"))
    |> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage")
    |> group(columns: [])
    |> sort(columns: ["_time", "host"])
    |> limit(n: $limit)
'''

# Optional
poll_interval   = "5s"        # how often to issue queries (default: "5s")
batch_size      = 500         # base query size; cursor handling can enlarge it
cursor_field    = "_time"     # column used as the cursor (default: "_time")
initial_offset  = "2024-01-01T00:00:00Z"  # starting cursor on first run
payload_format  = "json"      # json | text | raw (default: "json")
include_metadata = true       # include all row columns in the payload (default: true)
verbose_logging  = false
```

### V3 - InfluxDB 3.x Core / Enterprise

```toml
version = "v3"
url     = "http://localhost:8181"
db      = "my-db"
token   = "my-token"
query   = '''
  SELECT * FROM cpu
  WHERE time > '$cursor'
  ORDER BY time, host
  LIMIT $limit OFFSET $offset
'''

# Optional
poll_interval        = "5s"
batch_size           = 500
cursor_field         = "time"    # default cursor column for V3 (default: "time")
initial_offset       = "2024-01-01T00:00:00Z"
payload_format       = "json"
include_metadata     = true
stuck_batch_cap_factor = 10      # max effective_batch = 10 × batch_size (default: 10, max: 100)
                                 # 1 and values above 100 are rejected;
                                 # 0 disables the guards and the $offset requirement
verbose_logging      = false
```

### Resilience Fields (both versions)

```toml
timeout                   = "10s"   # per-request timeout
max_retries               = 3       # total query attempts, including the first (network errors, 429/5xx)
retry_delay               = "1s"    # initial backoff
retry_max_delay           = "5s"    # backoff cap
max_open_retries          = 10      # total open() health-check attempts, including the first
open_retry_max_delay      = "60s"   # backoff cap for open() retries
circuit_breaker_threshold = 5       # consecutive failures before circuit trips
circuit_breaker_cool_down = "30s"   # cooldown before queries resume
```

Attempt counts are clamped to at least one. Query retries use exponential backoff with jitter; integer-seconds `Retry-After` on HTTP 429 can override the delay cap. Other HTTP errors and malformed successful response bodies fail the poll. Startup retries failed authenticated `GET /health` requests. Invalid duration strings warn and fall back to `1s`, while zero durations are accepted. `batch_size = 0` is clamped to `1`. Without `initial_offset`, the cursor starts at `1970-01-01T00:00:00Z`.

## Query Template Placeholders

| Placeholder | Substituted with |
| --- | --- |
| `$cursor` | Current cursor value (RFC 3339 timestamp) |
| `$limit` | Base size plus capped skip count (V2), or effective batch size (V3) |
| `$offset` | Row offset within the current cursor group (V3 only; required when `stuck_batch_cap_factor > 0`) |

## Payload Formats

Without `payload_column`, the connector emits whole-row JSON regardless of `payload_format`:

- **V3**: a flat object with native JSON types. `include_metadata = false` removes only the cursor column.
- **V2**: an envelope containing `measurement`, `field`, `timestamp`, `value` and `row`. Metadata disabled leaves only `_time` and `_value` in `row`; the envelope remains. CSV cells are inferred as bool, integer or finite float, then string; an empty cell becomes null. Annotation datatype/default values are not applied.

With `payload_column`, the selected format applies:

- **`json`**: V2 parses the cell as JSON. V3 serializes its existing JSON value, so a string containing JSON remains a string.
- **`text`**: V2 emits the cell as UTF-8. V3 emits strings directly and serializes other JSON values as text.
- **`raw`**: decode the column's standard base64 text into bytes.

Set the destination `streams.schema` to the intended output format. Missing selected columns, invalid V2 JSON cells and invalid base64 fail the poll without advancing the acknowledged state. Omitting `payload_column` selects whole-row output; an empty column name is not a whole-row selector.

V3 requires a valid string cursor in every row and treats timestamps without a timezone suffix as UTC, preserving nanoseconds. V2 can emit an invalid/missing-cursor row if another row supplies a valid watermark, so retain a valid cursor in every result row. Keep database timestamps in the payload: the runtime does not transfer the plugin's timestamp fields into broker message metadata.

Message IDs combine timestamp nanoseconds with result position. Distinct rows can collide across batches, so these IDs are not unique database-row keys. V2 falls back to a random batch base when a timestamp cannot be represented as nanoseconds.

## Full Configuration Example

Save one connector entry in the runtime connector directory and start the runtime from the checkout root. Configure the broker connection in the main `connectors.toml` and create the `metrics` stream and `cpu` topic first.

```toml
type = "source"
key = "influxdb"
version = 0
name = "InfluxDB source"
enabled = true
path    = "target/release/libiggy_connector_influxdb_source"

[[streams]]
stream       = "metrics"
topic        = "cpu"
schema       = "json"
batch_length = 100
linger_time  = "10ms"

[plugin_config]
version        = "v3"
url            = "http://localhost:8181"
db             = "telemetry"
token          = "my-secret-token"
query          = """
  SELECT * FROM cpu
  WHERE time > '$cursor'
  ORDER BY time, host
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
- **`common` module**: Shared configuration/state types, payload-format and timestamp helpers, and the `RowContext` passed into both `process_rows` functions. Retry and circuit-breaker helpers come from the connector SDK.

The runtime checkpoint holds the cursor, processed-row count and version-specific skip/offset state. V2 accepts versioned V2 or legacy unversioned V2 state; V3 accepts only versioned V3 state. Corrupt state, invalid saved timestamps and version mismatches prevent startup. With the default file backend, the entry above uses `local_state/source_influxdb.state`. Stop the runtime before removing that checkpoint to reset progress; the runtime also supports an HTTP state backend.

Successful empty polls return checkpoints. Open-breaker polls return empty messages without a checkpoint, and cooldown expiry permits another query. Query and parsing errors preserve acknowledged progress and are logged by the SDK; they do not increment the runtime's forwarding-error metric or change its running status. V3 treats a database-not-found query response as an empty result. Both response readers cap buffered successful query bodies at 256 MiB.

### V2 vs V3 API Comparison

| Concern | InfluxDB V2 | InfluxDB V3 |
| --- | --- | --- |
| Write body format | Line Protocol | Line Protocol |
| Query body format | JSON (Flux query payload) | JSON (SQL query payload) |
| Health check | `GET /health` | `GET /health` |
| Query retry triggers | Network errors, 429 / 5xx | Network errors, 429 / 5xx |
| Cursor field default | `_time` | `time` |
| Timestamp format | RFC 3339 with TZ | RFC 3339 (TZ appended by connector if absent) |
| Sort guarantee | Group compatible rows, then sort by time and unique tiebreakers | Explicit timestamp and unique tiebreaker order required |
