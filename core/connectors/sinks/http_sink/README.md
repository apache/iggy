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
    "iggy_id": "00000000000000000000000000000001",
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
| `retry_backoff_multiplier` | u32 | `2` | Exponential backoff multiplier (min 1) |
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

```text
POST /ingest  Content-Type: application/json
{"metadata": {"iggy_offset": 1, ...}, "payload": {"key": "value"}}
```

### `ndjson`

All messages in one request, [newline-delimited JSON](https://github.com/ndjson/ndjson-spec). Best for bulk ingestion endpoints.

```text
POST /ingest  Content-Type: application/x-ndjson
{"metadata": {"iggy_offset": 1}, "payload": {"key": "value1"}}
{"metadata": {"iggy_offset": 2}, "payload": {"key": "value2"}}
```

### `json_array`

All messages as a single JSON array. Best for APIs expecting array payloads.

```text
POST /ingest  Content-Type: application/json
[{"metadata": {"iggy_offset": 1}, "payload": {"key": "value1"}}, ...]
```

### `raw`

Raw bytes, one request per message. For non-JSON payloads (protobuf, binary). Metadata envelope is not applied in raw mode.

```text
POST /ingest  Content-Type: application/octet-stream
<raw bytes>
```

## Message Flow: What Goes In vs. What Comes Out

The connector does **not** require or expect any particular message structure. It receives raw bytes from the Iggy runtime — whatever you published to the topic is what arrives in `consume()`. The `{metadata: {}, payload: {}}` envelope is something the **sink adds on the way out**, not something it expects on the way in.

```text
Your app publishes:  {"order_id": 123, "amount": 9.99}
                          |
                          v
Iggy stores:         raw bytes of that JSON
                          |
                          v
Runtime delivers:    those same raw bytes to consume()
                          |
                          v
HTTP sink wraps:     {"metadata": {"iggy_offset": 0, ...},
                      "payload": {"order_id": 123, "amount": 9.99}}
                          |
                          v
HTTP endpoint gets:  the wrapped envelope
```

With `include_metadata = false`, the sink skips wrapping — your original message goes through as-is:

```text
HTTP endpoint gets:  {"order_id": 123, "amount": 9.99}
```

The `schema` field in `[[streams]]` controls how the sink **interprets** the incoming bytes for output formatting:

| Schema | Interpretation | Payload in envelope |
| ------ | -------------- | ------------------- |
| `json` | Parses bytes as JSON | Embedded as JSON value |
| `text` | Treats bytes as UTF-8 string | Embedded as string |
| `raw` / `flatbuffer` / `proto` | Opaque binary | Base64-encoded with `"iggy_payload_encoding": "base64"` |

You can publish any struct serialized in any format (JSON, protobuf, raw bytes). Set the matching `schema` in `[[streams]]`, and choose whether you want the metadata envelope (`include_metadata`) or not.

## Metadata Envelope

When `include_metadata = true` (default), payloads are wrapped:

```json
{
  "metadata": {
    "iggy_id": "0123456789abcdef0123456789abcdef",
    "iggy_offset": 42,
    "iggy_timestamp": 1710064800000000,
    "iggy_stream": "my_stream",
    "iggy_topic": "my_topic",
    "iggy_partition_id": 0
  },
  "payload": { ... }
}
```

- **`iggy_id`**: Message ID formatted as 32-character lowercase hex string (no dashes)
- **Non-JSON payloads** (Raw, FlatBuffer, Proto): base64-encoded with `"iggy_payload_encoding": "base64"` in payload
- **JSON/Text payloads**: Embedded as-is

Set `include_metadata = false` to send the raw payload without wrapping.

## Retry Strategy

Uses `reqwest-middleware` with `RetryTransientMiddleware` for automatic exponential backoff:

```text
Initial request: no delay
Retry 1: retry_delay = 1s
Retry 2: retry_delay * backoff = 2s
Retry 3: retry_delay * backoff^2 = min(4s, 30s) = 4s
```

A custom `HttpSinkRetryStrategy` respects user-configured `success_status_codes` — codes in the success set are never retried, even if normally transient (e.g., 429 configured as "queued").

**Transient errors** (retry): Network errors, HTTP 429, 500, 502, 503, 504.

**Non-transient errors** (fail immediately): HTTP 400, 401, 403, 404, 405, etc.

**HTTP 429 `Retry-After`**: The middleware does not natively support `Retry-After` headers. When a response carries `Retry-After`, a warning is logged with the header value. The middleware uses computed exponential backoff instead.

**Partial delivery** (`individual`/`raw` modes): If a message fails after exhausting retries, subsequent messages continue processing. After 3 consecutive HTTP failures, the remaining batch is aborted to avoid hammering a dead endpoint.

## Use Cases

### Webhook Delivery

Forward stream events to webhook endpoints (Slack, PagerDuty, GitHub, custom). Use `individual` mode for one notification per event:

```toml
[plugin_config]
url = "https://hooks.slack.com/services/T00/B00/xxx"
batch_mode = "individual"
include_metadata = false    # Slack expects bare JSON payload
```

### REST API Ingestion

Push data into downstream REST APIs (analytics, CRM, data warehouse loaders). Use `ndjson` or `json_array` for bulk efficiency:

```toml
[plugin_config]
url = "https://analytics.example.com/v1/events"
batch_mode = "ndjson"
include_metadata = true     # downstream can route by iggy_stream/iggy_topic

[plugin_config.headers]
Authorization = "Bearer my-api-token"
```

### Serverless Function Trigger

Invoke AWS Lambda, Google Cloud Functions, or Azure Functions via their HTTP endpoints:

```toml
[plugin_config]
url = "https://abc123.execute-api.us-east-1.amazonaws.com/prod/ingest"
batch_mode = "json_array"
timeout = "10s"

[plugin_config.headers]
x-api-key = "my-api-key"
```

### IoT / Sensor Data Relay

Forward binary sensor payloads to processing services without JSON overhead:

```toml
[[streams]]
stream = "sensors"
topics = ["temperature", "pressure"]
schema = "raw"
batch_length = 200
poll_interval = "50ms"
consumer_group = "sensor_relay"

[plugin_config]
url = "https://iot-gateway.example.com/ingest"
batch_mode = "raw"
max_retries = 5
timeout = "5s"
```

### Multi-Service Event Fan-Out

Route different event types to their respective microservices. See [Deployment Patterns](#deployment-patterns) for how to set this up with multiple connector instances.

### Observability Pipeline

Forward structured logs or metrics from Iggy streams to external observability platforms:

```toml
[[streams]]
stream = "logs"
topics = ["application", "infrastructure", "security"]
schema = "json"
batch_length = 500
poll_interval = "200ms"
consumer_group = "log_forwarder"

[plugin_config]
url = "https://logs.example.com/api/v1/ingest"
batch_mode = "ndjson"
max_connections = 20
timeout = "60s"
max_payload_size_bytes = 52428800   # 50MB for large log batches
include_metadata = true             # iggy_stream/iggy_topic for routing

[plugin_config.headers]
Authorization = "Bearer observability-token"
```

## Authentication

The HTTP sink supports authentication via custom headers in `[plugin_config.headers]`. All headers are sent with every request, including health checks.

### Bearer Token

```toml
[plugin_config.headers]
Authorization = "Bearer eyJhbGciOiJSUzI1NiIs..."
```

### API Key

```toml
[plugin_config.headers]
x-api-key = "my-secret-api-key"
```

### Basic Auth

```toml
[plugin_config.headers]
# Base64-encoded "username:password"
Authorization = "Basic dXNlcm5hbWU6cGFzc3dvcmQ="
```

### Multiple Auth Headers

Some services require multiple authentication headers (e.g., API key + tenant ID):

```toml
[plugin_config.headers]
Authorization = "Bearer token"
X-Tenant-ID = "tenant-123"
X-Client-Version = "iggy-http-sink/0.1"
```

### Limitations

- **No OAuth2 / OIDC token refresh**: Bearer tokens are static. For services requiring token rotation, use an auth proxy (e.g., OAuth2 Proxy, Envoy with ext_authz) that handles token lifecycle and forwards requests to the upstream.
- **No AWS SigV4 signing**: For AWS services (API Gateway with IAM auth, S3, etc.), place the connector behind an API Gateway endpoint with API key auth, or use a signing proxy.
- **No mTLS client certificates**: Use `tls_danger_accept_invalid_certs` only for development. For production mTLS, terminate at a sidecar proxy.
- **Secrets in config file**: Header values (including tokens) are stored in plaintext in `config.toml`. Protect the config file with appropriate file permissions. Environment variable expansion in config values is not currently supported by the connector runtime.

## Deployment Patterns

### Connector Runtime Model

A **connector instance** is a single OS process — the `iggy-connectors` binary loading one shared library (`libiggy_connector_http_sink.so`/`.dylib`) with one config file. Each process reads exactly one `config.toml` (set via `IGGY_CONNECTORS_CONFIG_PATH`), which defines one `[plugin_config]` block — including the target `url`, authentication headers, batch mode, and retry settings.

Within that single process, the runtime spawns one async task per topic listed in `[[streams]]`. All tasks share the same plugin instance (and therefore the same HTTP client and `[plugin_config]`). There is no built-in orchestrator, no multi-connector-in-one-process mode, and no routing table that maps different topics to different URLs.

How this works in the runtime source code:

- **One consumer per topic**: `setup_sink_consumers()` in [`runtime/src/sink.rs`](../../../runtime/src/sink.rs) iterates `for topic in stream.topics.iter()` and creates a separate `IggyConsumer` for each topic.
- **One async task per consumer**: `spawn_consume_tasks()` in [`runtime/src/sink.rs`](../../../runtime/src/sink.rs) wraps each consumer in `tokio::spawn`, so topics are consumed concurrently within the same process.
- **One plugin instance per ID**: The `sink_connector!` macro in [`sdk/src/sink.rs`](../../sdk/src/sink.rs) creates a `static INSTANCES: DashMap<u32, SinkContainer>` — each `plugin_id` passed to `iggy_sink_open` gets its own entry, and all topic tasks call `consume()` on the same instance.
- **Sequential consume within each topic**: `consume_messages()` in [`runtime/src/sink.rs`](../../../runtime/src/sink.rs) awaits `consume()` before polling the next batch — there is no pipelining within a single topic task.

**"Deploying multiple instances"** means running N separate `iggy-connectors` processes — each with its own config directory, its own `[plugin_config]` (and therefore its own destination URL, headers, batch mode, etc.). In Docker or Kubernetes, this means N containers from the same image with different config mounts or environment variables. In systemd, N service units. In ECS, N task definitions.

### What's Achievable Today vs. Not

| Pattern | Achievable Today | How |
| ------- | :-: | --- |
| Single destination, single topic | Yes | One connector instance, one `[[streams]]` entry |
| Single destination, multiple topics | Yes | One connector instance, multiple topics in `[[streams]]` |
| Multiple destinations (topic-per-destination) | Yes | N connector instances, one per destination, each a separate OS process |
| Fan-out (same topic to multiple destinations) | Yes | N connector instances consuming same topic with different `consumer_group` names |
| Per-topic URL routing within one instance | **No** | Not supported — each instance has exactly one `url`. Requires N instances. See [Known Limitations](#known-limitations) item 6 |
| OAuth2 / OIDC token refresh | **No** | Static headers only. Use an auth proxy |
| mTLS client certificates | **No** | Use a sidecar proxy for mTLS termination |
| Environment variable expansion in config values | **No** | Use env var overrides at the process level (see [Environment Variable Overrides](#environment-variable-overrides)) |

### Single Destination, Multiple Topics

*Achievable today — single connector instance.*

When all topics go to the same endpoint, use one connector with multiple `[[streams]]` entries. The downstream service can distinguish topics via the `iggy_stream` and `iggy_topic` fields in the metadata envelope.

```text
┌─────────────────────────┐      ┌────────────────────────┐
│  Iggy Server            │      │  HTTP Endpoint         │
│  ├── stream: events     │      │  POST /ingest          │
│  │   ├── topic: clicks  │─────▶│  (routes internally    │
│  │   └── topic: views   │      │   by iggy_topic)       │
│  └── stream: orders     │      │                        │
│      └── topic: created │─────▶│                        │
└─────────────────────────┘      └────────────────────────┘
         connector-a (single instance)
```

**`connector-a/sink.toml`**:

```toml
type = "sink"
key = "http"
enabled = true
version = 0
name = "all_events"
path = "target/release/libiggy_connector_http_sink"

[[streams]]
stream = "events"
topics = ["clicks", "views"]
schema = "json"
batch_length = 100
poll_interval = "100ms"
consumer_group = "http_sink_events"

[[streams]]
stream = "orders"
topics = ["created"]
schema = "json"
batch_length = 50
poll_interval = "200ms"
consumer_group = "http_sink_orders"

[plugin_config]
url = "https://api.example.com/ingest"
batch_mode = "ndjson"
include_metadata = true

[plugin_config.headers]
Authorization = "Bearer shared-token"
```

### Multiple Destinations (One Connector Per Destination)

*Achievable today — requires N separate OS processes.*

When different topics need to go to different services, deploy separate connector instances. Each gets its own config directory and runs as a **separate `iggy-connectors` process** (not a config option within one process — see [Connector Runtime Model](#connector-runtime-model)).

```text
┌───────────────────┐
│  Iggy Server      │
│  └── stream: app  │
│      ├── clicks ──┼──▶  connector-analytics ──▶ analytics-api.example.com
│      ├── orders ──┼──▶  connector-billing   ──▶ billing-api.example.com
│      └── alerts ──┼──▶  connector-slack     ──▶ hooks.slack.com
└───────────────────┘
     3 separate connector instances
```

**Directory layout**:

```text
/opt/connectors/
├── analytics/
│   ├── config.toml           # shared iggy connection settings
│   └── connectors/
│       └── sink.toml         # clicks → analytics API
├── billing/
│   ├── config.toml
│   └── connectors/
│       └── sink.toml         # orders → billing API
└── slack/
    ├── config.toml
    └── connectors/
        └── sink.toml         # alerts → Slack webhook
```

**`analytics/connectors/sink.toml`**:

```toml
type = "sink"
key = "http"
enabled = true
version = 0
name = "analytics"
path = "/opt/connectors/libiggy_connector_http_sink"

[[streams]]
stream = "app"
topics = ["clicks"]
schema = "json"
batch_length = 500
poll_interval = "50ms"
consumer_group = "analytics_sink"

[plugin_config]
url = "https://analytics-api.example.com/v1/events"
batch_mode = "ndjson"
max_connections = 20

[plugin_config.headers]
Authorization = "Bearer analytics-token"
```

**`billing/connectors/sink.toml`**:

```toml
type = "sink"
key = "http"
enabled = true
version = 0
name = "billing"
path = "/opt/connectors/libiggy_connector_http_sink"

[[streams]]
stream = "app"
topics = ["orders"]
schema = "json"
batch_length = 50
poll_interval = "200ms"
consumer_group = "billing_sink"

[plugin_config]
url = "https://billing-api.example.com/v2/orders"
batch_mode = "individual"
include_metadata = false
timeout = "10s"

[plugin_config.headers]
Authorization = "Basic YmlsbGluZzpzZWNyZXQ="
X-Idempotency-Source = "iggy"
```

**`slack/connectors/sink.toml`**:

```toml
type = "sink"
key = "http"
enabled = true
version = 0
name = "slack_alerts"
path = "/opt/connectors/libiggy_connector_http_sink"

[[streams]]
stream = "app"
topics = ["alerts"]
schema = "json"
batch_length = 1
poll_interval = "500ms"
consumer_group = "slack_sink"

[plugin_config]
url = "https://hooks.slack.com/services/T00/B00/xxx"
batch_mode = "individual"
include_metadata = false
max_retries = 5
```

**Running** (3 processes, or 3 containers in Docker/ECS):

```bash
IGGY_CONNECTORS_CONFIG_PATH=/opt/connectors/analytics/config.toml iggy-connectors &
IGGY_CONNECTORS_CONFIG_PATH=/opt/connectors/billing/config.toml  iggy-connectors &
IGGY_CONNECTORS_CONFIG_PATH=/opt/connectors/slack/config.toml    iggy-connectors &
```

### Fan-Out: One Topic to Multiple Destinations

*Achievable today — requires N separate OS processes with different consumer groups.*

When a single topic needs to be delivered to multiple HTTP endpoints (e.g., send order events to both the billing service AND an analytics pipeline), deploy multiple connector instances that consume from the **same topic with different consumer groups**. Each instance is a separate `iggy-connectors` process (see [Connector Runtime Model](#connector-runtime-model)).

```text
                              connector-billing  ──▶ billing-api.example.com
                             (consumer_group: billing_sink)
┌─────────────────┐         /
│ stream: orders  │────────<
│ topic: created  │         \
└─────────────────┘          connector-analytics ──▶ analytics.example.com
                             (consumer_group: analytics_sink)
```

Each consumer group maintains its own offset, so both connectors independently receive every message. This is the standard Iggy fan-out pattern — not an antipattern.

**Key requirement**: Each connector instance MUST use a **different `consumer_group`**. If they share a consumer group, messages are load-balanced (split) across instances rather than duplicated.

**`billing/connectors/sink.toml`**:

```toml
[[streams]]
stream = "orders"
topics = ["created"]
schema = "json"
consumer_group = "billing_sink"       # unique consumer group

[plugin_config]
url = "https://billing-api.example.com/v2/orders"
batch_mode = "individual"
```

**`analytics/connectors/sink.toml`**:

```toml
[[streams]]
stream = "orders"
topics = ["created"]
schema = "json"
consumer_group = "analytics_sink"     # different consumer group = fan-out

[plugin_config]
url = "https://analytics.example.com/v1/events"
batch_mode = "ndjson"
```

### Docker / Container Deployment

*Achievable today.*

Each connector instance maps naturally to one container (one process = one container). Share the compiled `.so`/`.dylib` via a volume mount or bake it into the image:

```dockerfile
FROM rust:latest AS builder
WORKDIR /app
COPY . .
RUN cargo build -p iggy_connector_http_sink --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/libiggy_connector_http_sink.so /opt/connector/
COPY --from=builder /app/target/release/iggy-connectors /usr/local/bin/
COPY config/ /opt/connector/config/
ENV IGGY_CONNECTORS_CONFIG_PATH=/opt/connector/config/config.toml
CMD ["iggy-connectors"]
```

For multiple destinations, run multiple containers from the same image with different config mounts:

```yaml
# docker-compose.yml
services:
  connector-analytics:
    image: iggy-http-sink
    volumes:
      - ./analytics-config:/opt/connector/config
    environment:
      IGGY_CONNECTORS_CONFIG_PATH: /opt/connector/config/config.toml

  connector-billing:
    image: iggy-http-sink
    volumes:
      - ./billing-config:/opt/connector/config
    environment:
      IGGY_CONNECTORS_CONFIG_PATH: /opt/connector/config/config.toml
```

### Environment Variable Overrides

The connector runtime supports overriding any config field via environment variables using the convention `IGGY_CONNECTORS_SINK_{KEY}_<SECTION>_<FIELD>`. This is useful for keeping secrets out of config files:

```bash
# Override the URL and auth token at runtime
export IGGY_CONNECTORS_SINK_HTTP_PLUGIN_CONFIG_URL="https://prod-api.example.com/ingest"
export IGGY_CONNECTORS_SINK_HTTP_PLUGIN_CONFIG_HEADERS_AUTHORIZATION="Bearer prod-token"
iggy-connectors
```

## Performance Considerations

### Batch Mode Selection

The connector runtime calls `consume()` **sequentially** — the next poll cycle does not start until the current batch completes. Batch mode choice directly impacts throughput:

| Mode | HTTP Requests per Poll | Latency per Poll | Best For |
| ---- | ---------------------- | ----------------- | -------- |
| `individual` | N (one per message) | N × round-trip | Low-volume webhooks, order-sensitive delivery |
| `ndjson` | 1 | 1 × round-trip | High-throughput bulk ingestion |
| `json_array` | 1 | 1 × round-trip | APIs expecting array payloads |
| `raw` | N (one per message) | N × round-trip | Binary payloads (protobuf, avro) |

With `batch_length=50` in `individual` mode, each poll cycle performs 50 sequential HTTP round trips. If each takes 100ms, the poll cycle takes 5 seconds — during which no new messages are consumed from that topic. Use `ndjson` or `json_array` to collapse this to a single round trip.

### Memory

In `ndjson` and `json_array` modes, the entire batch is serialized into memory before sending. With `batch_length=1000` and 10KB messages, this allocates ~10MB per poll cycle. The `max_payload_size_bytes` check runs **after** serialization (the batch must be built to know its size). For very large batches, tune `batch_length` and `max_payload_size_bytes` together.

### Connection Pooling and Keep-Alive

The connector builds one `ClientWithMiddleware` (wrapping `reqwest::Client` with retry and tracing middleware) per plugin instance in `open()`. Because the runtime calls `consume()` sequentially within each topic task, a single-topic connector uses at most **one connection at a time**. Multi-topic connectors may use up to N concurrent connections (one per topic task), since each task calls `consume()` independently.

reqwest uses HTTP/1.1 persistent connections (keep-alive) by default. The connector configures:

- **`max_connections`** (default: 10) — Maximum idle connections retained per host. The pool creates additional connections beyond this limit as needed — this setting only controls how many idle connections are kept warm for reuse.
- **TCP keep-alive** (30s) — Sends TCP keep-alive probes on idle connections to detect silent drops by cloud load balancers. Without this, a connection silently closed by an intermediate LB (AWS ALB drops idle connections after ~60s, GCP after ~600s) would only be discovered on the next HTTP request, causing a failed attempt and retry delay.
- **Pool idle timeout** (90s) — Closes connections unused for 90 seconds to prevent stale connection accumulation in the pool.

Because `reqwest::Client` clones are cheap (they share the same connection pool via `Arc`), all topic tasks within a single connector process share one pool. This means multi-topic connectors benefit from connection reuse when all topics target the same host — a connection returned to the pool by topic A's task can be reused by topic B's task.

For multiple connector instances (separate processes), each process has its own independent `reqwest::Client` and its own connection pool. There is no cross-process connection sharing.

### Retry Impact on Throughput

Each failed message in `individual`/`raw` mode burns through the retry budget (default: 3 retries with exponential backoff up to 30s) before moving to the next message. The backoff delays are 1s + 2s + 4s = 7 seconds per message, but each attempt also incurs the request timeout (default 30s) for a dead endpoint. Worst case per message: 4 attempts × 30s timeout + 7s backoff = 127 seconds.

The consecutive failure abort (`MAX_CONSECUTIVE_FAILURES = 3`) mitigates this: after 3 consecutive HTTP failures, remaining messages in the batch are skipped. This limits worst-case blocking to: 3 × (4 × 30s + 1s + 2s + 4s) = 381 seconds with default timeout, or 3 × 7s = 21 seconds of backoff delay alone.

### Multiple Instances vs. Single Instance

Multiple connector instances (one per destination) provide:

- **Performance isolation**: A slow destination doesn't block other topics
- **Failure isolation**: One dead endpoint doesn't affect unrelated connectors
- **Independent tuning**: Different `batch_length`, `timeout`, `max_retries` per destination
- **Security isolation**: Each instance has its own credentials; compromise of one config doesn't expose others
- **Independent scaling**: Scale high-volume connectors without over-provisioning low-volume ones

The overhead of multiple processes is minimal — each connector is a lightweight async runtime with low memory footprint at idle.

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

Integration tests (requires Docker for WireMock container):

```bash
cargo test -p integration --test connectors -- http_sink
```

## Delivery Semantics

All retry logic lives inside `consume()`. The connector runtime invokes `consume()` via an FFI callback that returns an `i32` status code. The runtime does not inspect this return value (see `process_messages()` in `runtime/src/sink.rs`), so errors logged by the sink are not propagated to the runtime's retry or alerting mechanisms. Additionally, consumer group offsets are committed before processing ([runtime issue #1](#known-limitations)). This means:

- Failed messages are **not retried by the runtime** — only by the sink's internal retry loop
- Messages are committed **before delivery** — a crash after commit but before delivery loses messages

The effective delivery guarantee is **at-most-once** at the runtime level. The sink's internal retries provide best-effort delivery within each `consume()` call.

## Known Limitations

1. **Runtime ignores `consume()` status**: The connector runtime invokes `consume()` via an FFI callback returning `i32`. The `process_messages()` function in `runtime/src/sink.rs` does not inspect the return value. Errors are logged internally by the sink but do not trigger runtime-level retry or alerting. ([#2927](https://github.com/apache/iggy/issues/2927))

2. **Offsets committed before processing**: The `PollingMessages` auto-commit strategy commits consumer group offsets before `consume()` is called. Combined with limitation 1, at-least-once delivery is not achievable. ([#2928](https://github.com/apache/iggy/issues/2928))

3. **`Retry-After` header not used for backoff**: The `reqwest-middleware` retry layer uses computed exponential backoff. `Retry-After` headers are logged as warnings but do not influence retry timing.

4. **No dead letter queue**: Failed messages are logged at `error!` level but not persisted to a DLQ. DLQ support would be a runtime-level feature.

5. **No request signing**: AWS SigV4, HMAC, or other signing schemes are not supported. Use custom headers or an auth proxy for signed endpoints.

6. **No per-topic URL routing**: All topics configured in a single connector instance share the same `url`. For topic-specific routing, deploy separate connector instances (see [Deployment Patterns](#deployment-patterns)). A future enhancement could add a `[plugin_config.routing]` table for URL-per-topic within a single instance.

7. **No OAuth2 token refresh**: Bearer tokens are static. Use an auth proxy for services requiring automatic token rotation.

8. **No environment variable expansion in config values**: Secrets in `[plugin_config.headers]` are stored as plaintext. Use environment variable overrides (see [Environment Variable Overrides](#environment-variable-overrides)) or mount secrets from a secrets manager.
