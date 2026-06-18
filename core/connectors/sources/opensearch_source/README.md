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
query = { "match_all": {} }
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
| `polling_interval` | `String` | `"10s"` | Delay after each completed poll cycle (humantime format). First poll runs immediately. |
| `batch_size` | `usize` | `100` | Documents per search request (minimum `1`) |
| `query` | JSON object | `{"match_all": {}}` | OpenSearch query DSL; applied on every poll |
| `username` / `password` | `String` | none | HTTP basic authentication |
| `verbose_logging` | `bool` | `false` | Log per-poll batch counts at `info!` instead of `debug!` |

### File-backed state (optional)

Runtime state is always returned from `poll()` and persisted by the connectors
runtime. To additionally mirror state to JSON files on disk:

```toml
[plugin_config.state]
enabled = true
storage_type = "file"
storage_config = { base_path = "./connector_states" }
state_id = "opensearch_logs_connector"
```

Only `storage_type = "file"` is implemented. See [State and persistence](#state-and-persistence).

## How it works

### Poll cycle

Each call to `poll()`:

1. Issues `POST /{index}/_search` with the query below.
2. Maps each hit's `_source` to a JSON `ProducedMessage`.
3. Updates the `search_after` cursor to the sort tuple of the last successfully published hit.
4. Returns `ProducedMessages` containing the messages and a serialized `ConnectorState`.
5. Sleeps `polling_interval` before returning.

The runtime persists `ConnectorState` (msgpack) after each successful `poll()` return.

### Search request

```json
{
  "query": "<config.query or match_all>",
  "size": "<batch_size>",
  "sort": [
    { "<timestamp_field>": { "order": "asc" } },
    { "_id":              { "order": "asc" } }
  ],
  "search_after": ["<omitted on first poll; previous batch's sort tuple thereafter>"]
}
```

Two sort keys give stable order when timestamps collide. `_id` is the tiebreaker.

### Per-hit processing

For each hit in the response:

1. **Missing sort tuple** — skip with `warn!`; cursor not advanced.
2. **Missing `_source`** — skip with `warn!`; cursor not advanced.
3. Both present — serialize `_source` as JSON payload; advance cursor to this hit's sort tuple.

The cursor (`search_after`) only advances for hits where **both** sort and `_source` are present.
An empty batch leaves the cursor unchanged.

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
| `search_after` | `Option<Vec<Value>>` — OpenSearch sort tuple from last published hit; authoritative resume cursor |
| `last_poll_timestamp` | `Option<DateTime<Utc>>` — timestamp of last processed document; informational |
| `total_documents_published` | Cumulative documents emitted to Iggy |
| `poll_count` | Total search requests executed (successful + empty) |
| `error_count` / `last_error` | Search failure tracking |
| `processing_stats` | Bytes processed, empty/successful poll counts, avg latency |

**Invariant:** `search_after` is the authoritative resume cursor. `last_poll_timestamp` is
informational only and does not affect pagination.

### Dual persistence

| Mechanism | Format | When written | When read | Failure mode |
| --------- | ------ | ------------ | --------- | ------------ |
| Runtime `ConnectorState` | MessagePack | Every `poll()` return | `new(id, config, Some(state))` | Corrupt → `open()` fails with `InitError` |
| File `SourceState` | JSON | `close()` if `state.enabled` and connector opened successfully | `open()` if `state.enabled` and no runtime state present | Load failure → `open()` fails with `InitError` |

File path: `{base_path}/{state_id}.json`; defaults: `base_path = "./connector_states"`,
`state_id = "opensearch_source_{id}"`.

Runtime `ConnectorState` is authoritative. When valid runtime state is restored on
startup, file state is not loaded. File mirror is written atomically (write-tmp →
fdatasync → rename → dir-fsync) on `close()`.

## Initial load and tuning

### Cursor behavior by phase

| Phase | Cursor behavior |
| ----- | --------------- |
| Fresh start (no state) | No `search_after` — reads from start of sort order |
| Steady state | `search_after` advances — only documents after cursor returned |
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
| `InitError` | Corrupt runtime state; missing index at `open()`; file state load failure |
| `InvalidConfigValue` | Missing `timestamp_field`; `batch_size = 0`; unsupported `storage_type` |
| `Storage` | Network or HTTP errors; client not initialized at `poll()` |
| `Serialization` | JSON / MessagePack failures |

## Limitations

- **Single sequential reader** — one `search_after` cursor, one batch per poll.
  No parallel shard/slice workers or dedicated bulk-ingest mode.
- **Same path for initial load and steady state** — a fresh connector walks the
  index from the oldest `(timestamp_field, _id)` upward. There is no separate
  bootstrap implementation.
- **Throughput tied to `polling_interval` and `batch_size`** — defaults (`10s`,
  `100`) yield roughly 10 documents/second. Tens of millions of documents require
  tuning both knobs and sufficient OpenSearch / Iggy capacity.
- **`search_after` only** — no Scroll API, point-in-time (PIT), or sliced
  parallel export. Offset paging (`from`/`size`) is not used.
- **At-least-once delivery** — no deduplication by `_id`. The in-memory cursor advances
  before the runtime persists `ConnectorState`; a crash can re-emit the last batch.
- **No HTTP retry** — transient OpenSearch errors fail the poll immediately. No circuit
  breaker (unlike the InfluxDB source).
- **Backfill gap** — documents indexed with `timestamp_field` values older than
  the current cursor are not read until connector state is reset.
- **Full `_source` only** — entire document JSON is published; no field
  projection or schema variants beyond `Schema::Json`.
- **Optional file state** — only `storage_type = "file"` is implemented. File mirror is
  written atomically on `close()`, not every poll. Runtime msgpack wins on restart when
  both are present. A failed file save on `close()` returns an error.
- **`_source`-disabled documents skipped permanently** — hits returned without `_source`
  (e.g., index mapping with `"_source": false`) are skipped with a `warn!`. The cursor
  advances past them. If `_source` later becomes available for a document at the same
  `(timestamp_field, _id)` sort position, it will not be re-fetched. Ensure `_source` is
  enabled in the index mapping before using this connector.
- **Missing sort tuple causes no cursor advance** — hits returned without a sort tuple
  (rare; typically deleted-doc artifacts or partial shard results) are skipped with a
  `warn!`. If such hits appear at the tail of a batch, the cursor stays at the last
  successfully published document. Subsequent polls return the same hits until OpenSearch
  stops including them.
- **Single-node transport** — `SingleNodeConnectionPool` to `url`; no cluster
  node sniffing.

## Troubleshooting

| Symptom | Check |
| ------- | ----- |
| `open()` fails with missing index | Index name, URL, and credentials |
| `open()` fails with `state restore failed` | Delete or repair the connector runtime state file |
| `open()` fails with `file state load failed` | Delete or repair the file state JSON |
| No new documents after restart | `timestamp_field` mapping must match indexed documents |
| Duplicate messages | At-least-once delivery; lower `batch_size` only after confirming sort stability on `(timestamp_field, _id)` |
| Initial load too slow | Increase `batch_size`, decrease `polling_interval` |
| Backfilled docs missing | Timestamps older than cursor are skipped; reset state or adjust query |
| Repeated `warn!` about missing `_source` | Index mapping has `"_source": false`; connector cannot publish those documents |
