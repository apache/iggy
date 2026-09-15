# InfluxDB Sink Connector

A sink connector that consumes messages from Iggy streams and writes them to InfluxDB as line-protocol points. Supports both InfluxDB V2 (OSS 2.x / Cloud 2.x) and InfluxDB V3 (Core / Enterprise).

## V2 vs V3 Differences

| Aspect | InfluxDB V2 | InfluxDB V3 |
| --- | --- | --- |
| Data organisation | `org` + `bucket` | `db` |
| Write endpoint | `POST /api/v2/write` | `POST /api/v3/write_lp` |
| Auth header | `Authorization: Token {t}` | `Authorization: Bearer {t}` |
| Write API precision values | `ns`, `us`, `ms`, `s` | `nanosecond`, `microsecond`, `millisecond`, `second` (mapped from connector short forms) |
| Config `version` key | `"v2"` (or omit, default) | `"v3"` |

The write body (InfluxDB line protocol), retry/circuit-breaker behaviour, batch chunking, and payload format handling are identical between versions.

## Configuration

Select the version with `version = "v2"` or `version = "v3"`. Omitting `version` defaults to `"v2"` for backward compatibility with existing deployments.
Unknown options are rejected, so remove `org` and `bucket` when switching to V3.
The examples in the next sections are the contents of `[plugin_config]`; the full connector
file appears below. The target organization/bucket or database must be configured and the
token must have write access. Startup probes `GET /health` with the configured token and version-specific authentication
header. A successful health check does not validate write permissions.

### V2 - InfluxDB OSS 2.x / Cloud

```toml
version    = "v2"
url        = "http://localhost:8086"
org        = "my-org"
bucket     = "my-bucket"
token      = "my-token"

# Optional
measurement = "iggy_events"      # line-protocol measurement name (default: iggy_messages)
precision   = "us"               # ns | us | ms | s  (default: "us")
batch_size  = 500                # messages per write request (default: 500)
payload_format = "json"          # json | text | base64 (default: "json")

include_metadata          = true  # include stream/topic/partition as tags or fields (default: true)
include_checksum          = false
include_origin_timestamp  = false
include_stream_tag        = false # add stream name as a line-protocol tag
include_topic_tag         = false # add topic name as a line-protocol tag
include_partition_tag     = false # add partition id as a line-protocol tag
verbose_logging           = false
```

### V3 - InfluxDB 3.x Core / Enterprise

```toml
version = "v3"
url     = "http://localhost:8181"
db      = "my-db"
token   = "my-token"

# Optional - same fields as V2 except org/bucket are replaced by db
measurement = "iggy_events"
precision   = "us"
batch_size  = 500
payload_format = "json"

include_metadata          = true
include_checksum          = false
include_origin_timestamp  = false
include_stream_tag        = false
include_topic_tag         = false
include_partition_tag     = false
verbose_logging           = false
```

### Resilience Fields (both versions)

```toml
timeout                   = "30s"   # per-request timeout
max_retries               = 3       # total write attempts, including the first (network errors/429/5xx)
retry_delay               = "1s"    # initial backoff between retries
retry_max_delay           = "5s"    # backoff cap
max_open_retries          = 10      # total open() health-check attempts, including the first
open_retry_max_delay      = "60s"   # backoff cap for open() retries
circuit_breaker_threshold = 5       # consecutive failures before circuit trips
circuit_breaker_cool_down = "30s"   # how long circuit stays open before writes resume
```

## Payload Formats

- **`json`** (default): Each message payload is validated as JSON, compact-serialized, and stored in one `payload_json` string field.
- **`text`**: Payload must be valid UTF-8 and is stored in one `payload_text` string field.
- **`base64`**: Payload bytes are base64-encoded into one `payload_base64` string field.

Formats are case-insensitive; `utf8` aliases `text`, `raw` aliases `base64`, and an unknown
format warns and falls back to JSON. Formatting happens after stream decoding and transforms.
Use `schema = "raw"` with base64 and no payload-changing transform to preserve arbitrary bytes,
or `schema = "text"` with text for UTF-8 input. Text CR/LF bytes become literal `\r`/`\n`
sequences in the stored string. Measurement names and tag values reject tabs; text fields allow them.

## Metadata and timestamps

All `include_*` options default to `true`. `message_id` is always a string field and `offset`
is always a tag. With `include_metadata = true`, stream/topic/partition are tags unless their
individual tag flags are disabled, in which case they become `iggy_stream`, `iggy_topic` and
`iggy_partition` fields. `include_metadata = false` omits those three values. Checksum and
origin timestamp have independent flags.

Point timestamps use the message timestamp, converted from microseconds to the configured
precision. Zero uses the current wall-clock time. Millisecond/second conversion truncates
finer digits. InfluxDB point identity uses measurement/table, tags and timestamp; removing
stream/topic/partition tags can collapse distinct messages with the same offset and timestamp.
Moving those values to fields does not preserve identity. See the
[V2](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/#duplicate-points) and
[V3](https://docs.influxdata.com/influxdb3/core/reference/line-protocol/#duplicate-points) duplicate-point rules.

## Full Configuration Example

```toml
type = "sink"
key = "influxdb"
enabled = true
version = 0
name = "InfluxDB sink"
path = "target/release/libiggy_connector_influxdb_sink"

[[streams]]
stream = "metrics"
topics = ["cpu"]
schema = "json"
batch_length = 100
poll_interval = "10ms"
consumer_group = "influxdb_sink"

[plugin_config]
version    = "v2"
url        = "http://localhost:8086"
org        = "acme"
bucket     = "telemetry"
token      = "my-secret-token"
measurement = "cpu_metrics"
batch_size  = 200
precision   = "ms"
include_stream_tag = true
include_topic_tag  = true
circuit_breaker_threshold = 3
circuit_breaker_cool_down = "15s"
```

## Architecture Notes

The sink uses a layered design:

- **Batch chunking**: each polled batch is split into requests of at most `batch_size` messages (`0` behaves as `1`). The final partial chunk is written immediately; messages are not accumulated across polls.
- **Retry middleware**: `iggy_connector_sdk::retry::HttpRetryMiddleware` retries 429, 5xx and network errors with exponential backoff and jitter.
- **Circuit breaker**: records at most one failure per consume call, based on its first error. Permanent HTTP errors do not increment the counter; a fully successful call resets it. While open, batches fail without writes. After the cool-down window, writes resume and the counter resets.
- **Precision mapping**: V3's `/api/v3/write_lp` endpoint requires full English words (`nanosecond`, `microsecond`, `millisecond`, `second`); the connector maps the short forms automatically.

A serialization error rejects its whole chunk. Later chunks are still attempted after failures,
and the first error is returned after the loop. `max_retries = 3` allows two retries; `0` and
`1` both allow one attempt. Backoff starts at `retry_delay`, doubles with ±20% jitter and is
capped by `retry_max_delay`. Integer-seconds `Retry-After` on a 429 overrides that cap;
HTTP-date values are ignored. Startup uses its own attempt budget/cap and retries any failed
health check. Invalid duration strings warn and fall back to `1s`.

The runtime records a plugin error and continues polling with consumer auto-commit. Failed
chunks and batches skipped by the circuit breaker are not queued for replay. Other chunks
may already be stored, so these mechanisms do not provide an end-to-end at-least-once guarantee.
