# OTLP Source

Receives logs, metrics, and traces from any OpenTelemetry SDK or Collector
over gRPC (OTLP/gRPC protocol) and writes them to an Iggy stream.

## How it works

The connector binds a gRPC server (default port 4317, the OTLP standard) and
implements all three collector services: `LogsService`, `MetricsService`, and
`TraceService`. Each incoming export request is deserialized from the
`opentelemetry-proto` wire format and turned into messages according to
`format` (see below). The messages are buffered in an in-process channel and
drained by the runtime via the `poll()` call.

Gzip is accepted on every service, since OTel SDKs and the Collector's OTLP
exporter compress payloads by default. Responses are not gzipped: an export
response carries at most a `partial_success` and is a couple of bytes, so the
compression header would outweigh the body.

When the channel is full the connector does not fail the export. It enqueues
what fits and reports the remainder in the response's `partial_success`, so the
client can retry only the rejected records instead of the whole batch.

## Storage format

`format = "json"` (default) decodes each record into a JSON document, one
message per record. Use it when a downstream sink needs to query individual
fields.

`format = "proto"` forwards the raw `opentelemetry-proto` request bytes as a
single message, without decoding. It is roughly 4-5x smaller on the wire and
pairs zero-copy with `otlp_sink`'s `format = "proto"`. Individual fields are
not queryable in this mode.

## JSON schema

Every document has a `signal` field (`"log"`, `"metric"`, or `"trace"`). Fields
that would be empty or null are omitted, except on metrics, which always emit
the full set.

**Logs**: `timestamp_ns`, `observed_timestamp_ns`, `severity`,
`severity_text`, `body`, `trace_id`, `span_id`, `service_name`, `resource`,
`attributes`

**Metrics**: `name`, `type` (`gauge`, `sum`, `histogram`,
`exponential_histogram`, `summary`), `unit`, `timestamp_ns`, `value`,
`service_name`, `resource`, `attributes`, plus `is_monotonic` on sums.
`value` is a scalar for gauges and sums, and an object (`count`, `sum`, and
`scale` on exponential histograms) for the aggregating types. One message is
produced per data point, not per metric.

**Traces**: `trace_id`, `span_id`, `parent_span_id`, `name`, `kind`,
`start_time_ns`, `end_time_ns`, `status`, `status_message`, `service_name`,
`resource`, `attributes`

`resource` carries the resource-level attributes shared by every record in the
same export request, and is repeated on each message so downstream sinks do not
have to join.

## Configuration

```toml
[plugin_config]
listen_addr = "0.0.0.0:4317"   # gRPC bind address
channel_capacity = 50000       # in-process buffer (messages), must be > 0
batch_size = 1000              # max messages returned per poll()
format = "json"                # "json" or "proto"
```

Point any OTel SDK or Collector at `grpc://host:4317` (no TLS by default).
