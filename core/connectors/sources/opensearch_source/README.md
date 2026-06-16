# OpenSearch Source Connector

Polls documents from an OpenSearch index and publishes them to Iggy streams as JSON
messages. Incremental progress is tracked with OpenSearch `search_after` pagination
on `(timestamp_field, _id)`.

## Features

- Incremental polling via `search_after` on a configured timestamp field plus `_id`
- Optional custom OpenSearch query (`match_all` by default)
- Basic authentication (username + password)
- Runtime state persistence via the connectors runtime (`ConnectorState` / MessagePack)
- Optional supplementary file-backed state when `plugin_config.state.enabled = true`

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
polling_interval = "30s"
batch_size = 100
timestamp_field = "@timestamp"
query = { "match_all": {} }
```

### Required fields

| Field | Description |
| --- | --- |
| `url` | OpenSearch HTTP endpoint |
| `index` | Index name or pattern |
| `timestamp_field` | Document field used for sort order and incremental cursors (required) |

### Optional fields

| Field | Default | Description |
| --- | --- | --- |
| `polling_interval` | `10s` | Delay before each poll (humantime format) |
| `batch_size` | `100` | Maximum documents per search request (minimum `1`) |
| `query` | `match_all` | OpenSearch query DSL object |
| `username` / `password` | none | HTTP basic authentication |
| `verbose_logging` | `false` | Emit per-poll batch counts at `info!` instead of `debug!` |

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

Only `storage_type = "file"` is implemented. State is saved on `close()` when
`state.enabled = true`.

## State fields

The connector tracks:

- `search_after`: OpenSearch sort tuple from the last document in the previous batch
- `last_poll_timestamp`: timestamp of the last processed document
- `last_document_id`: `_id` of the last processed document
- `total_documents_fetched`, `poll_count`, error counters, and processing statistics

Corrupt runtime state causes `open()` to fail with `InitError` rather than silently
resetting the cursor.

## Timestamp formats

The configured `timestamp_field` may be an RFC 3339 string or epoch milliseconds /
seconds in the document `_source`.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| `open()` fails with missing index | Index name, URL, and credentials |
| `open()` fails with `state restore failed` | Delete or repair the connector runtime state file |
| No new documents after restart | `timestamp_field` mapping must match indexed documents |
| Duplicate messages | Lower `batch_size` only after confirming sort stability on `(timestamp_field, _id)` |
