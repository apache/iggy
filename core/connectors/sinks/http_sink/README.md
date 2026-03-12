# HTTP Sink Connector

Consumes messages from Iggy streams and delivers them to any HTTP endpoint — webhooks, REST APIs, Lambda functions, or SaaS integrations.

## Try It

Send a JSON message through Iggy and see it arrive at an HTTP endpoint.

**Prerequisites**: Docker running, project built (`cargo build` from repo root).

```bash
# Start iggy-server (terminal 1)
IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy ./target/debug/iggy-server

# Create stream and topic
./target/debug/iggy -u iggy -p iggy stream create demo_stream
./target/debug/iggy -u iggy -p iggy topic create demo_stream demo_topic 1

# Start a simple HTTP receiver (terminal 2)
python3 -c "
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
class H(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers['Content-Length']))
        print(json.dumps(json.loads(body), indent=2))
        self.send_response(200)
        self.end_headers()
HTTPServer(('', 9090), H).serve_forever()
"

# Setup connector config
mkdir -p /tmp/http-sink-test/connectors
cat > /tmp/http-sink-test/config.toml << 'TOML'
[iggy]
address = "localhost:8090"
username = "iggy"
password = "iggy"
[state]
path = "/tmp/http-sink-test/state"
[connectors]
config_type = "local"
config_dir = "/tmp/http-sink-test/connectors"
TOML
cat > /tmp/http-sink-test/connectors/sink.toml << 'TOML'
type = "sink"
key = "http"
enabled = true
version = 0
name = "test"
path = "target/debug/libiggy_connector_http_sink"
[[streams]]
stream = "demo_stream"
topics = ["demo_topic"]
schema = "json"
batch_length = 100
poll_interval = "100ms"
consumer_group = "test_cg"
[plugin_config]
url = "http://localhost:9090/ingest"
batch_mode = "individual"
TOML

# Start connector (terminal 3)
IGGY_CONNECTORS_CONFIG_PATH=/tmp/http-sink-test/config.toml ./target/debug/iggy-connectors

# Send a message
./target/debug/iggy -u iggy -p iggy message send demo_stream demo_topic '{"hello":"http"}'
```

Expected output on the Python receiver:

```json
{
  "metadata": {
    "iggy_id": "00000000-0000-0000-0000-000000000001",
    "iggy_offset": 0,
    "iggy_stream": "demo_stream",
    "iggy_topic": "demo_topic"
  },
  "payload": {
    "hello": "http"
  }
}
```

Cleanup: `rm -rf /tmp/http-sink-test`

## Quick Start

```toml
[[streams]]
stream = "events"
topics = ["notifications"]
schema = "json"
batch_length = 50
poll_interval = "100ms"
consumer_group = "http_sink"

[plugin_config]
url = "https://api.example.com/ingest"
batch_mode = "ndjson"
```

## Configuration

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `url` | string | **required** | Target URL for HTTP requests |
| `method` | string | `POST` | HTTP method: `GET`, `HEAD`, `POST`, `PUT`, `PATCH`, `DELETE` |
| `timeout` | string | `30s` | Request timeout (e.g., `10s`, `500ms`) |
| `max_payload_size_bytes` | u64 | `10485760` | Max body size in bytes (10MB). `0` to disable |
| `batch_mode` | string | `individual` | `individual`, `ndjson`, `json_array`, or `raw` |
| `include_metadata` | bool | `true` | Wrap payload in metadata envelope |
| `include_checksum` | bool | `false` | Add message checksum to metadata |
| `include_origin_timestamp` | bool | `false` | Add origin timestamp to metadata |
| `health_check_enabled` | bool | `false` | Send health check request in `open()` |
| `health_check_method` | string | `HEAD` | HTTP method for health check |
| `max_retries` | u32 | `3` | Retry attempts for transient errors |
| `retry_delay` | string | `1s` | Base delay between retries |
| `retry_backoff_multiplier` | f64 | `2.0` | Exponential backoff multiplier (min 1.0) |
| `max_retry_delay` | string | `30s` | Maximum retry delay cap |
| `success_status_codes` | [u16] | `[200, 201, 202, 204]` | Status codes considered successful |
| `tls_danger_accept_invalid_certs` | bool | `false` | Skip TLS certificate validation |
| `max_connections` | usize | `10` | Max idle connections per host |
| `verbose_logging` | bool | `false` | Log request/response details at debug level |
| `headers` | table | `{}` | Custom HTTP headers (e.g., `Authorization`) |

## Batch Modes

### `individual` (default)

One HTTP request per message. Best for webhooks and endpoints that accept single events.

> With `batch_length = 50`, this produces 50 sequential HTTP round trips per poll cycle.
> For production throughput, use `ndjson` or `json_array`.

```
POST /ingest  Content-Type: application/json
{"metadata": {"iggy_offset": 1, ...}, "payload": {"key": "value"}}
```

### `ndjson`

All messages in one request, [newline-delimited JSON](https://github.com/ndjson/ndjson-spec). Best for bulk ingestion endpoints.

```
POST /ingest  Content-Type: application/x-ndjson
{"metadata": {"iggy_offset": 1}, "payload": {"key": "value1"}}
{"metadata": {"iggy_offset": 2}, "payload": {"key": "value2"}}
```

### `json_array`

All messages as a single JSON array. Best for APIs expecting array payloads.

```
POST /ingest  Content-Type: application/json
[{"metadata": {"iggy_offset": 1}, "payload": {"key": "value1"}}, ...]
```

### `raw`

Raw bytes, one request per message. For non-JSON payloads (protobuf, binary). Metadata envelope is not applied in raw mode.

```
POST /ingest  Content-Type: application/octet-stream
<raw bytes>
```

## Metadata Envelope

When `include_metadata = true` (default), payloads are wrapped:

```json
{
  "metadata": {
    "iggy_id": "01234567-89ab-cdef-0123-456789abcdef",
    "iggy_offset": 42,
    "iggy_timestamp": 1710064800000000,
    "iggy_stream": "my_stream",
    "iggy_topic": "my_topic",
    "iggy_partition_id": 0
  },
  "payload": { ... }
}
```

- **`iggy_id`**: Message ID formatted as UUID hex string (not RFC 4122 compliant — positional formatting only)
- **Non-JSON payloads** (Raw, FlatBuffer, Proto): base64-encoded with `"iggy_payload_encoding": "base64"` in payload
- **JSON/Text payloads**: Embedded as-is

Set `include_metadata = false` to send the raw payload without wrapping.

## Retry Strategy

Exponential backoff with configurable parameters:

```
Attempt 1: retry_delay (1s)
Attempt 2: retry_delay * backoff_multiplier (2s)
Attempt 3: retry_delay * backoff^2 (4s)
Attempt 4: min(retry_delay * backoff^3, max_retry_delay) (8s, capped to 30s)
```

**Transient errors** (retry): Network errors, HTTP 429, 500, 502, 503, 504.

**Non-transient errors** (fail immediately): HTTP 400, 401, 403, 404, 405, etc.

**HTTP 429 `Retry-After`**: Integer-valued `Retry-After` headers are respected, capped to `max_retry_delay`.

**Partial delivery** (`individual`/`raw` modes): If a message fails after exhausting retries, subsequent messages continue processing. After 3 consecutive HTTP failures, the remaining batch is aborted to avoid hammering a dead endpoint.

## Example Configs

### Lambda Webhook

```toml
[plugin_config]
url = "https://abc123.execute-api.us-east-1.amazonaws.com/prod/ingest"
method = "POST"
batch_mode = "json_array"
timeout = "10s"
include_metadata = true

[plugin_config.headers]
x-api-key = "my-api-key"
```

### Slack Notification

```toml
[plugin_config]
url = "https://hooks.slack.com/services/T00/B00/xxx"
method = "POST"
batch_mode = "individual"
include_metadata = false
```

### High-Throughput Bulk Ingestion

```toml
[plugin_config]
url = "https://ingest.example.com/bulk"
method = "POST"
batch_mode = "ndjson"
max_connections = 20
timeout = "60s"
max_payload_size_bytes = 52428800
```

## Testing

Unit tests (no external dependencies):

```bash
cargo test -p iggy_connector_http_sink
```

## Delivery Semantics

All retry logic lives inside `consume()`. The connector runtime currently discards the `Result` returned by `consume()` and commits consumer group offsets before processing ([runtime issue #1](#known-limitations)). This means:

- Failed messages are **not retried by the runtime** — only by the sink's internal retry loop
- Messages are committed **before delivery** — a crash after commit but before delivery loses messages

The effective delivery guarantee is **at-most-once** at the runtime level. The sink's internal retries provide best-effort delivery within each `consume()` call.

## Known Limitations

1. **Runtime discards `consume()` errors**: The connector runtime (`sink.rs:585`) ignores the return value from `consume()`. Errors are logged internally but do not trigger runtime-level retry or alerting.

2. **Offsets committed before processing**: The `PollingMessages` auto-commit strategy commits consumer group offsets before `consume()` is called. Combined with limitation 1, at-least-once delivery is not achievable.

3. **`Retry-After` HTTP-date format not supported**: Only integer `Retry-After` values (delay-seconds) are parsed. HTTP-date format (RFC 7231 §7.1.3) falls back to exponential backoff. This is a v1 limitation.

4. **No dead letter queue**: Failed messages are logged at `error!` level but not persisted to a DLQ. DLQ support would be a runtime-level feature.

5. **No request signing**: AWS SigV4, HMAC, or other signing schemes are not supported. Use custom headers or an auth proxy for signed endpoints.
