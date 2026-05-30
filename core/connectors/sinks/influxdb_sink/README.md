# InfluxDB Sink Connector

A sink connector that consumes messages from Iggy streams and writes them to InfluxDB as line-protocol points. Supports both InfluxDB V2 (OSS 2.x / Cloud 2.x) and InfluxDB V3 (Core / Enterprise).

## V2 vs V3 Differences

| Aspect | InfluxDB V2 | InfluxDB V3 |
| --- | --- | --- |
| Data organisation | `org` + `bucket` | `db` |
| Write endpoint | `POST /api/v2/write` | `POST /api/v3/write_lp` |
| Auth header | `Authorization: Token {t}` | `Authorization: Bearer {t}` |
| Precision values | `ns`, `us`, `ms`, `s` | same short forms accepted |
| Config `version` key | `"v2"` (or omit — default) | `"v3"` |

The write body (InfluxDB line protocol), retry/circuit-breaker behaviour, batch accumulation, and payload format handling are identical between versions.

## Configuration

Select the version with `version = "v2"` or `version = "v3"`. Omitting `version` defaults to `"v2"` for backward compatibility with existing deployments.

### V2 — InfluxDB OSS 2.x / Cloud

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

include_metadata          = true  # inject offset/stream/topic fields (default: true)
include_checksum          = false
include_origin_timestamp  = false
include_stream_tag        = false # add stream name as a line-protocol tag
include_topic_tag         = false # add topic name as a line-protocol tag
include_partition_tag     = false # add partition id as a line-protocol tag
verbose_logging           = false
```

### V3 — InfluxDB 3.x Core / Enterprise

```toml
version = "v3"
url     = "http://localhost:8181"
db      = "my-db"
token   = "my-token"

# Optional — same fields as V2 except org/bucket are replaced by db
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
max_retries               = 3       # retries per write on transient errors (429/5xx)
retry_delay               = "1s"    # initial backoff between retries
retry_max_delay           = "5s"    # backoff cap
max_open_retries          = 10      # retries during open() health check
open_retry_max_delay      = "60s"   # backoff cap for open() retries
circuit_breaker_threshold = 5       # consecutive failures before circuit trips
circuit_breaker_cool_down = "30s"   # how long circuit stays open before half-open probe
```

## Payload Formats

- **`json`** (default): Each message payload is parsed as JSON and its fields become line-protocol field set entries.
- **`text`**: Payload treated as a plain string; written as a single `value` field.
- **`base64`**: Payload decoded from base64; written as raw bytes (stored as a string field).

## Full Configuration Example

```toml
[[sinks]]
key     = "influxdb-sink"
enabled = true
path    = "target/release/libiggy_connector_influxdb_sink"

[[sinks.streams]]
stream       = "metrics"
topic        = "cpu"
schema       = "json"
batch_length = 100
linger_time  = "10ms"

[sinks.plugin_config]
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

- **Batch accumulator**: messages are serialised to line protocol and buffered until `batch_size` is reached, then flushed in a single HTTP POST.
- **Retry middleware**: `reqwest-retry` with exponential backoff handles 429 and 5xx responses automatically before the connector-level retry logic runs.
- **Circuit breaker**: after `circuit_breaker_threshold` consecutive failures the connector stops issuing writes and waits for the cool-down window before probing again.
- **Precision mapping**: V3's `/api/v3/write_lp` endpoint requires full English words (`nanosecond`, `microsecond`, `millisecond`, `second`); the connector maps the short forms automatically.
