# OTLP Source

Receives logs, metrics, and traces from any OpenTelemetry SDK or Collector
over gRPC (OTLP/gRPC protocol) and writes them to an Iggy stream as JSON
messages.

## How it works

The connector binds a gRPC server (default port 4317, the OTLP standard) and
implements all three collector services: `LogsService`, `MetricsService`, and
`TraceService`. Each incoming export request is deserialized from the
`opentelemetry-proto` wire format and serialized to JSON. The JSON messages are
buffered in an in-process channel and drained by the runtime via the `poll()`
call.

Gzip compression is enabled on every service. OTel SDKs and the Collector's
OTLP exporter compress payloads by default, so the connector accepts and sends
gzip on all three services.

## JSON schema

Each message has a `signal` field (`"log"`, `"metric"`, or `"trace"`) plus
signal-specific fields:

**Logs**: `timestamp_ns`, `observed_timestamp_ns`, `severity`,
`severity_text`, `body`, `trace_id`, `span_id`, `service_name`, `attributes`

**Metrics**: `name`, `description`, `unit`, `kind` (gauge/sum/histogram/...),
`data_points`, `resource`, `attributes`

**Traces**: `trace_id`, `span_id`, `parent_span_id`, `name`, `kind`,
`start_time_ns`, `end_time_ns`, `status`, `service_name`, `attributes`,
`events`, `links`

## Configuration

```toml
[plugin_config]
listen_addr = "0.0.0.0:4317"   # gRPC bind address
channel_capacity = 50000         # in-process buffer (messages)
batch_size = 1000                # max messages returned per poll()
```

Point any OTel SDK or Collector at `grpc://host:4317` (no TLS by default).
