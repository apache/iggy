# OpenSearch Sink Connector

A sink connector that consumes messages from Iggy streams and indexes them
into an OpenSearch index through the official Rust SDK.

## Configuration

```toml
[plugin_config]
url = "https://opensearch.example.com:9200"
index = "iggy_messages"
# username = "admin"
# password = "..."
# document_id_field = "order_id"
create_index_if_not_exists = true
include_metadata = true
batch_size = 1000
timeout = "30s"
refresh = "false"
max_retries = 3
retry_delay = "500ms"
max_retry_delay = "5s"
max_open_retries = 5
verbose_logging = false
```

- `url`: OpenSearch base URL. A path is kept and used as the base every
  request is joined onto, so OpenSearch behind a reverse-proxy subpath
  (`https://proxy.example.com/opensearch`) works. Query strings and
  fragments are ignored. Must not embed credentials
  (`https://user:pass@host`); use `username`/`password` instead, or `open()`
  fails config validation.
- `index`: Target index name.
- `username` / `password`: Optional HTTP Basic authentication. Both must be
  set together, or neither; setting only one fails config validation.
  `password` is a `SecretString` and is never logged. AWS SigV4 (for
  AWS-managed OpenSearch / OpenSearch Serverless) is not supported; basic
  auth only.
- `document_id_field`: Optional top-level payload field supplying the
  document `_id`. Nested paths (for example `"order.id"`) are not supported;
  only a direct top-level key is looked up. The value must be a string,
  number, or boolean; must not be empty or exceed 512 bytes (OpenSearch's
  `_id` limit). When absent from a message (or unconfigured), the connector
  falls back to a generated, deterministic `_id`. See
  [Behavior](#behavior) below for what each mode buys you.
- `create_index_if_not_exists`: Create the index during `open()` when
  missing. Defaults to `true`. When `false` and the index does not exist,
  `open()` fails.
- `index_mapping`: Optional OpenSearch index mapping body, applied when the
  index is created. For example:

  ```toml
  [plugin_config.index_mapping.mappings.properties.count]
  type = "integer"
  ```

- `include_metadata`: Add `iggy_*` provenance fields to each document.
  Defaults to `true`.
- `batch_size`: Maximum documents per OpenSearch `_bulk` request. Defaults
  to `1000`.
- `timeout`: Per-request timeout as a humantime string, for example `30s`.
  Defaults to `30s`. Applies to every request the connector makes, including
  reading the response body.
- `refresh`: OpenSearch bulk `refresh` parameter: `"false"` (default),
  `"true"`, or `"wait_for"`. `"wait_for"` blocks the bulk response until the
  write is visible to search, but only at the *next* scheduled OpenSearch
  refresh cycle (default interval ~1s), not immediately. Do not assume a
  fixed short delay is enough to observe a write via `_search` afterward.
- `max_retries`: Maximum transient retries for a bulk request after the
  initial attempt. Defaults to `3`.
- `retry_delay` / `max_retry_delay`: Exponential backoff bounds for
  transient retries. Defaults to `500ms` / `5s`. If configured with
  `retry_delay > max_retry_delay`, the values are swapped and a warning is
  logged.
- `max_open_retries`: Maximum transient retries for each `open()`-time
  request: the cluster health check, the index existence check, and index
  creation. Defaults to `5`. A permanent failure (for example a rejected
  index mapping) is not retried and fails `open()` immediately.
- `verbose_logging`: Log per-batch receive and index counts at `info` instead
  of `debug`. Defaults to `false`.

## Required privileges

With the security plugin enabled, the configured user needs
`cluster_composite_ops` (for `_bulk`) at cluster scope, plus `crud`,
`create_index`, and `indices:admin/get` on the target index pattern.

`open()` also probes `GET /_cluster/health`, which needs the cluster-scoped
`cluster:monitor/health` privilege. That privilege is deliberately *not*
required: a `403` on the probe still proves the cluster answered and the
credentials authenticated, so the connector logs a warning and continues. A
`401` (credentials rejected) still fails `open()`.

## Behavior

`Payload::Json` object values are indexed as documents; non-object JSON
(arrays, scalars) is wrapped under a `value` field, since OpenSearch
documents must be objects. `Payload::Raw` bytes are parsed as JSON when
possible, otherwise indexed as `{data: <base64>, data_type: "raw",
data_encoding: "base64"}`. `Payload::Text` is indexed as `{text, data_type:
"text"}`. Unsupported payload schemas (Protobuf, FlatBuffer, Avro) are
dropped with a warning and counted as sink errors. This matches the
connector runtime's per-record drop convention, and because the sink returns
success after dropping such a record, the runtime commits the consumer
offset for it. There is no dead-letter queue for these drops.

### Document ID

When `document_id_field` names a field present in the payload, that value
(stringified) becomes the document `_id`. Otherwise the connector generates
a deterministic `_id` by hashing the exact Iggy stream, topic, partition,
offset, and message ID (blake3, hex-encoded), prefixed `iggy_`. Hashing keeps
the ID a fixed length regardless of how long the stream and topic names are;
encoding them verbatim could otherwise exceed OpenSearch's 512-byte `_id`
limit for long names. Because bulk `index` upserts on a repeated `_id`, both
modes make replaying the same message idempotent rather than duplicating it:
the generated ID is stable for a given stream, topic, partition, offset, and
message ID, and the natural-key ID is stable for a given `document_id_field`
value. The natural-key path is covered end to end against a live server by
`connectors::opensearch::opensearch_sink` in the integration suite, which
resends a payload under an existing `order_id` at a different Iggy offset and
asserts the document count is unchanged.

If a user-provided `document_id_field` value collides across otherwise
distinct messages, those messages will collapse into a single document.
Operators choosing this field are responsible for its uniqueness.

### Metadata

When `include_metadata` is enabled, the connector writes reserved `iggy_*`
fields after payload parsing, overwriting any same-named payload fields so
provenance reflects the true stream, topic, partition, offset, checksum, and
timestamps: `iggy_message_id`, `iggy_offset`, `iggy_stream`, `iggy_topic`,
`iggy_partition`, `iggy_checksum` (stored as a string, since checksums
exceed the precision JSON numbers can represent exactly), `iggy_timestamp`,
`iggy_origin_timestamp`, `iggy_ingested_at`, and `iggy_headers` when the
message carries headers.

Each header becomes a field under `iggy_headers`, named after the header key.
Every value uses the same shape, `{data: <string>, data_encoding: "utf8" |
"base64"}`: raw binary values are base64-encoded, every other header kind is
stringified and marked `utf8`. The shape is uniform on purpose. OpenSearch
pins `iggy_headers.<key>` to whatever type the first indexed document uses, so
a per-kind shape would make every later message using the other kind fail with
a `mapper_parsing_exception` and be dropped.

Two hazards remain, both inherent to using header keys as field names:

- A header key containing a `.` is read as a path. A key `a.b` alongside a key
  `a` produces `object mapping for [iggy_headers.a] tried to parse field [a] as
  object, but found a concrete value`, and the second document is dropped.
- Header key cardinality is unbounded, but an index has a
  `index.mapping.total_fields.limit` (1000 by default). Past that limit,
  documents introducing a new header key fail with `illegal_argument_exception`
  and are dropped. Set `include_metadata = false`, or raise the limit through
  `index_mapping`, if messages carry high-cardinality header keys.

Both are permanent per-item failures, so they are subject to the visibility
caveat in [Delivery Semantics](#delivery-semantics).

**The timestamp fields do not share a unit.** `iggy_timestamp` and
`iggy_origin_timestamp` come from the Iggy message header and are
**microseconds** since the Unix epoch; `iggy_ingested_at` is stamped by this
connector and is **milliseconds**, matching the `elasticsearch_sink` and
`meilisearch_sink` convention. All three are indexed as `long`, so subtracting
one from another without converting first is off by a factor of 1000.

## Delivery Semantics

**A batch that fails at `open()` time is visible.** A missing index with
`create_index_if_not_exists = false`, a persistently unreachable cluster,
and similar setup failures correctly flip the connector to
`ConnectorStatus::Error`, reported via the runtime's `/sinks` endpoint.

**A batch that fails at `consume()` time is not currently visible anywhere
except this connector's own logs.** This is not specific to this connector;
it is how the shared connectors runtime invokes every sink's `consume()`
over FFI today: the plugin's returned status is not propagated to
`ConnectorStatus`, `last_error`, or the `/stats` `errors` counter, and the
runtime does not hold back or redeliver the failed batch. Verified against a
live server: a batch containing a real OpenSearch `mapper_parsing_exception`
was correctly classified and logged by this connector as a
`PermanentHttpError`, and the connector continued consuming and indexing
later messages normally, with `ConnectorStatus` staying `Running`
throughout. **Operators must monitor this connector's own `tracing` output
(`error!` at target `iggy_connector_opensearch_sink`) to detect indexing
failures; the runtime's own status and stats APIs will not show them.**

Within a single `consume()` call, a `_bulk` request can return HTTP 200 while
individual documents fail. This connector parses the per-item `items[]`
results rather than trusting the top-level status, so a batch with one bad
document among many still indexes the valid ones. Item failures are
classified as retryable (HTTP 429/5xx) or permanent (everything else,
including OpenSearch mapping/parsing errors). Retryable items are resent on
their own, under the same `max_retries` and backoff bounds that govern a
whole-request failure, so a `429` from a saturated indexing queue does not
cost those documents. Permanent item failures are never resent. Anything
still failing once retries are exhausted is reported as a failed document for
the batch, subject to the visibility caveat above. The close-time
`documents_indexed` counter counts successful bulk index operations, not
distinct documents: replaying the same batch twice counts twice even though
the document count in OpenSearch does not change.
