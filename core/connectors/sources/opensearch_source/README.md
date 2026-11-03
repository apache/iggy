# OpenSearch Source Connector

Polls documents from an OpenSearch index and publishes them to Iggy streams as JSON
messages. Incremental progress is tracked with OpenSearch `search_after` pagination
on `(timestamp_field, _id)`.

## Architecture

The connector is a cdylib source plugin loaded by the Iggy connectors runtime via FFI.

| Layer | Crate / binary | Role |
| ----- | -------------- | ---- |
| Plugin | `iggy_connector_opensearch_source` | Implements `Source` trait; talks to OpenSearch |
| SDK | `iggy_connector_sdk` | `Source`, `ProducedMessage`, `ConnectorState`, `source_connector!` macro |
| Runtime | `iggy-connectors` | Loads `.dylib`, calls `open` / `poll` / `close`, publishes to Iggy, saves `ConnectorState` |
| Server | `iggy-server` | Receives messages on configured streams/topics |

The connector is read-only. It does not write to OpenSearch.

## Configuration

```toml
type = "source"
key = "opensearch"
enabled = true
version = 0
name = "OpenSearch source"
path = "target/release/libiggy_connector_opensearch_source"
plugin_config_format = "json"

[[streams]]
stream = "opensearch_stream"
topic = "documents"
schema = "json"
batch_length = 100
linger_time = "5ms"

[plugin_config]
url = "http://localhost:9200"
index = "logs-*"
polling_interval = "10s"
batch_size = 100
timestamp_field = "@timestamp"
query = { match_all = {} }
```

### Required fields

| Field | Type | Description |
| ----- | ---- | ----------- |
| `url` | `String` | OpenSearch HTTP base URL |
| `index` | `String` | Index name or pattern |
| `timestamp_field` | `String` | Document field used for sort order and cursor; must exist on every document |

### Optional fields

| Field | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `polling_interval` | `String` | `"10s"` | Wait at the start of each poll cycle before fetching (humantime format). First poll waits one interval. |
| `batch_size` | `usize` | `100` | Documents per search request (`1` to `10000`, the OpenSearch default `index.max_result_window`) |
| `query` | JSON object | `{"match_all": {}}` | OpenSearch query DSL; applied on every poll |
| `username` / `password` | `String` | none | HTTP basic authentication |
| `verbose_logging` | `bool` | `false` | Log per-poll batch counts at `info!` instead of `debug!` |
| `max_retries` | `u32` | `3` | Total HTTP attempts per search during `poll()` |
| `retry_delay` | `String` | `"1s"` | Base backoff between HTTP retries |
| `retry_max_delay` | `String` | `"30s"` | Maximum backoff between HTTP retries |
| `max_open_retries` | `u32` | `5` | Total attempts for index-exists check during `open()` |
| `open_retry_max_delay` | `String` | `"30s"` | Maximum backoff during `open()` probes |
| `circuit_breaker_threshold` | `u32` | `5` | Consecutive poll failures before circuit opens |
| `circuit_breaker_cool_down` | `String` | `"60s"` | Duration to skip polls when circuit is open |

## How it works

### Poll cycle

Each call to `poll()`:

1. Sleeps `polling_interval` (paces fetch cadence; first poll waits one interval).
2. Issues `POST /{index}/_search` with the query below.
3. Maps each hit's `_source` to a JSON `ProducedMessage`.
4. Stages a candidate state whose `search_after` cursor is the sort tuple of the last hit.
5. Returns `ProducedMessages` containing the messages and the serialized candidate `ConnectorState`.

The runtime sends the batch, persists `ConnectorState` (msgpack), then reports the result through
`on_batch_result`. The connector commits the candidate state on `Ack` and discards it on `Nack`, so
a rejected batch is fetched again on the next poll. Delivery is at-least-once: a batch that reached
the topic but whose state save failed is replayed, so consumers must tolerate duplicates.
An empty batch moves no cursor and returns no state.

### Search request

```json
{
  "query": "<config.query or match_all>",
  "size": "<batch_size>",
  "track_total_hits": false,
  "sort": [
    { "<timestamp_field>": { "order": "asc" } },
    { "_id":              { "order": "asc" } },
    { "_index":           { "order": "asc" } }
  ],
  "search_after": ["<omitted on first poll; previous batch's sort tuple thereafter>"]
}
```

Three sort keys give stable order when timestamps collide. `_id` and then `_index` are the tiebreakers, so equal `_id`s in different indices of a pattern are not skipped.

### Per-hit processing

For each hit in the response:

1. **Missing sort tuple** - the poll fails with `Error::SchemaMismatch` and the cursor stays where it was. The request always asks for `sort`, so a missing tuple means the index or mapping is incompatible.
2. **Missing `_source`** - skip with a `warn!` that names the `_id` and `_index`, and move the cursor past the hit. The hit has no body, so nothing can be published and retrying would repeat forever.
3. **Both present** - serialize `_source` as the JSON payload and set:
   - `id`: a stable 128-bit BLAKE3 hash of the hit's concrete index (`_index`) and `_id`, so equal `_id`s in different indices of a pattern stay distinct, so a consumer can drop a document replayed after a NACK.
   - headers `opensearch.document_id` and `opensearch.index`: the original `_id` and the hit's concrete `_index`, since the hash cannot be reversed and `index` may be a pattern. Each is omitted with a `warn!` when it exceeds 255 bytes; the message `id` is still set.
   - `origin_timestamp`: the document's `timestamp_field` in microseconds (the unit Iggy uses), when it parses.

The cursor (`search_after`) advances only past published hits. An empty batch leaves the cursor unchanged.

### Timestamp parsing

The `timestamp_field` value in `_source` is parsed to populate `last_poll_timestamp` (informational only; does not affect pagination).

| `_source` value | Parsing |
| --------------- | ------- |
| RFC 3339 string | `DateTime::parse_from_rfc3339` |
| Integer `> 1e12` | Epoch milliseconds |
| Integer `≤ 1e12` | Epoch seconds |
| Other | Ignored; document still published |

## State and persistence

### Internal state fields

| Field | Purpose |
| ----- | ------- |
| `search_after` | `Option<Vec<Value>>` - OpenSearch sort tuple from last published hit; authoritative resume cursor |
| `last_poll_timestamp` | `Option<DateTime<Utc>>` - timestamp of last processed document; informational |
| `total_documents_published` | Cumulative documents emitted to Iggy |
| `poll_count` | Total search requests executed (successful + empty) |
| `error_count` / `last_error` | Search failure tracking |
| `processing_stats` | Bytes processed, empty/successful poll counts, avg latency |

**Invariant:** `search_after` is the authoritative resume cursor. `last_poll_timestamp` is
informational only and does not affect pagination.

### Persistence

The connectors runtime owns persistence. `poll()` returns the candidate `ConnectorState`
(MessagePack); the runtime sends the batch, saves the state, and reports the result through
`on_batch_result`. The connector commits the candidate on `Ack` and discards it on `Nack`.
A corrupt persisted state makes `open()` fail with `InitError` rather than silently
restarting from the beginning.

### If the runtime state is lost

With no persisted state the connector starts from the first document and republishes
everything, so duplicates are expected. To limit the damage:

- Keep the runtime `state_path` on durable storage that survives container or host replacement.
- Rely on the stable message `id` and the `opensearch.document_id` header to drop duplicates
  downstream.
- Bound the rescan by setting a time-window `query` in `plugin_config` (see
  [Backfill](#backfill)).

## Initial load and tuning

### Cursor behavior by phase

| Phase | Cursor behavior |
| ----- | --------------- |
| Fresh start (no state) | No `search_after` - reads from start of sort order |
| Steady state | `search_after` advances - only documents after cursor returned |
| Restart with saved state | Cursor restored from `ConnectorState`; resumes without re-reading |

There is no separate initial-load code path. Every poll uses the same logic.

### Throughput

With defaults (`batch_size = 100`, `polling_interval = "10s"`):

```text
100 docs / 10s ≈ 10 docs/sec
10,000,000 docs ≈ ~11.5 days to catch up
```

Aggressive config for large initial loads:

```toml
[plugin_config]
polling_interval = "100ms"
batch_size = 5000
timestamp_field = "@timestamp"
```

Optional time-window queries for manual partitioning:

```toml
[plugin_config]
query = { "range" = { "@timestamp" = { "gte" = "2024-01-01", "lt" = "2024-02-01" } } }
```

Requirements for correct operation:

- `timestamp_field` present on every document.
- Index mapping has a date-type field for `timestamp_field`.
- `_source` enabled in the index mapping (see Limitations).

## Error handling

| Error variant | When raised |
| ------------- | ----------- |
| `InitError` | Corrupt runtime state; missing index or unreachable cluster at `open()` |
| `InvalidConfigValue` | Missing `timestamp_field`; `batch_size` outside `1..=10000` |
| `Connection` | Network failure during a search; client not initialized at `poll()` |
| `HttpRequestFailed` | HTTP `429` or `5xx` after the retry budget is spent |
| `PermanentHttpError` | Other HTTP `4xx` (auth, search DSL errors); unparsable search response |
| `SchemaMismatch` | A hit lacks a sort tuple (index mapping problem) |
| `Serialization` | JSON / MessagePack failures |

## Resilience

### Retry policy

| Category | Conditions |
| -------- | ---------- |
| Transient (retry) | Network errors; HTTP `429`; HTTP `5xx` |
| Permanent (no retry) | `400`, `401`, `403`, `404`; malformed responses; search DSL errors |

Retries use the shared SDK `retry_async` helper: exponential backoff from `retry_delay`, capped at
`retry_max_delay` (`open_retry_max_delay` during `open()`), with ±20 % jitter. The delay is always
computed locally. A `Retry-After` response header is not read.

### Circuit breaker

When open, `poll()` skips the search, logs a warning, sleeps `polling_interval`, and returns an empty batch with no state update - cursor does not advance. Consecutive failures after retries exhausted increment the breaker; a successful search resets it.

### Delivery semantics

**At-least-once.** `poll()` does not move the cursor. The candidate state is committed only
when the runtime reports `Ack`, which means the batch was sent and its state persisted. On
`Nack` the candidate is discarded and the next poll fetches the same documents again.

A batch that reached the topic but whose state save failed is also NACKed, so those documents
are delivered twice. A crash between sending and saving state has the same effect.

Consumer guidance: dedupe on the message `id` (a hash of the hit's `_index` and `_id`), or on the
`opensearch.index` and `opensearch.document_id` headers.

### Backfill

Pagination is forward-only on `(timestamp_field asc, _id asc, _index asc)`. Documents indexed with `timestamp_field` values older than the current cursor are not read until state is reset.

| Mode | Use case |
| ---- | -------- |
| Forward tail (default) | Live streaming; cursor tracks ingest order |
| Bounded backfill | Set a time-window `query` in `plugin_config` |
| Full rescan | Reset runtime `ConnectorState`; duplicates possible |

## Limitations

- **Single sequential reader** - one `search_after` cursor, one batch per poll.
  No parallel shard/slice workers or dedicated bulk-ingest mode.
- **Same path for initial load and steady state** - a fresh connector walks the
  index from the oldest `(timestamp_field, _id)` upward. There is no separate
  bootstrap implementation.
- **Throughput tied to `polling_interval` and `batch_size`** - defaults (`10s`,
  `100`) yield roughly 10 documents/second. Tens of millions of documents require
  tuning both knobs and sufficient OpenSearch / Iggy capacity.
- **`search_after` only** - no Scroll API, point-in-time (PIT), or sliced
  parallel export. Offset paging (`from`/`size`) is not used.
- **At-least-once delivery** - the connector does not deduplicate. The cursor advances only on
  `Ack`, so a NACKed batch or a crash before the state save re-emits documents. Consumers can
  drop duplicates on the message `id`, or on the `opensearch.index` and `opensearch.document_id` headers.
  See [Delivery semantics](#delivery-semantics).
- **HTTP retry and circuit breaker** - transient `429`/`5xx` and network errors are retried
  per `max_retries`; consecutive failures trip a circuit breaker that skips polls until
  cool-down. Permanent errors (`4xx` except `429`) are not retried.
  See [Resilience](#resilience).
- **Backfill gap** - documents indexed with `timestamp_field` values older than
  the current cursor are not read until connector state is reset.
- **Full `_source` only** - entire document JSON is published; no field
  projection or schema variants beyond `Schema::Json`.
- **Documents without `_source` are skipped** - a hit returned without `_source` (for
  example an index mapping with `"_source": false`) is logged with `warn!` and not published.
  Keep `_source` enabled for every document you need to read.
- **Missing sort tuple** - a hit without a sort tuple fails the poll with
  `Error::SchemaMismatch`. The cursor does not move.
- **Single-node transport** - `SingleNodeConnectionPool` to `url`; no cluster
  node sniffing.

## Troubleshooting

| Symptom | Check |
| ------- | ----- |
| `open()` fails with missing index | Index name, URL, and credentials |
| `open()` fails with `state restore failed` | Delete or repair the connector runtime state file |
| No new documents after restart | `timestamp_field` mapping must match indexed documents |
| Duplicate messages | At-least-once delivery; lower `batch_size` only after confirming sort stability on `(timestamp_field, _id)` |
| Initial load too slow | Increase `batch_size`, decrease `polling_interval` |
| Backfilled docs missing | Timestamps older than cursor are skipped; reset state or adjust query |
| Repeated `warn!` about missing `_source` | Index mapping has `"_source": false`; those documents are skipped, not published |
