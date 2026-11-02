# Elasticsearch Source Connector with State Management

This connector polls Elasticsearch documents and returns JSON messages with a
checkpoint to the Iggy connectors runtime.

## Features

- **Incremental Data Processing**: Use a top-level RFC3339 timestamp string as a watermark, subject to the timestamp limitations below.
- **Error Tracking**: Monitor error counts and last error messages.
- **Processing Statistics**: Track fetched documents, payload bytes and successful-poll durations, including empty polls.
- **Persistent State Storage**: Return MessagePack checkpoints to the runtime's file or HTTP state backend.
- **State Recovery**: Restore the runtime checkpoint on restart; optional plugin JSON snapshots can override it.

## Configuration

From the matching 0.9.0/edge Iggy checkout root, build the plugin:

```bash
cargo build --release -p iggy_connector_elasticsearch_source
```

Use the [source guide](https://iggy.apache.org/docs/connectors/sources/source/)
for the broker credentials and main runtime configuration. The
[Elasticsearch source walkthrough](https://iggy.apache.org/docs/connectors/sources/elasticsearch/)
provides local backend, index and Iggy CLI commands. Create a matching index
before starting the connector and start the runtime from the checkout root.

### Basic Configuration

```toml
type = "source"
key = "elasticsearch"
enabled = true
version = 0
name = "Elasticsearch source"
path = "target/release/libiggy_connector_elasticsearch_source"

[[streams]]
stream = "elasticsearch_stream"
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

[plugin_config.query.match_all]
```

| Field | Default | Behavior |
| --- | --- | --- |
| `url` | required | Elasticsearch URL. |
| `index` | required | Index expression; startup checks that it exists and is accessible. |
| `username` / `password` | none | Basic authentication is enabled only when both are present. |
| `query` | `match_all` | Structured Elasticsearch Query DSL object, represented by TOML tables. |
| `polling_interval` | `10s` | Delay before each poll; invalid strings fall back to `10s`, zero is accepted. |
| `batch_size` | `100` | Search size per poll; zero returns no hits. Elasticsearch applies its result-window limit. |
| `timestamp_field` | none | Top-level RFC3339 string field used to advance the watermark. |
| `scroll_timeout` | none | Accepted but unused; the connector does not use scroll. |
| `state` | none | Optional, separate plugin JSON snapshot configuration described below. |

The local connector file remains TOML. `plugin_config_format` selects the default
format of the HTTP API's plugin-config response; it does not change local file
parsing or the JSON passed through the FFI.

## Polling and Delivery

Each poll sleeps first, then searches with the configured query and batch size,
sorted ascending by `timestamp_field`, or `@timestamp` when it is unset. The
index mapping must support that sort. Each hit with `_source` becomes one JSON
message. Elasticsearch `_id` is not included automatically or used as an Iggy
message ID. The plugin leaves message headers and timestamps unset.

With a timestamp field, searches filter values strictly greater than the last
acknowledged watermark. The watermark is extracted only from top-level RFC3339
strings; numeric dates, date-only strings and nested paths do not advance it.
Without a usable timestamp, polls repeat the first matching batch.

There is no document-ID tiebreaker, scroll or `search_after` pagination. If a
timestamp group spans multiple batches, the remaining tied documents are skipped
after the watermark advances. Late arrivals and updates at or below the watermark
are also skipped. This is not a complete change-data-capture feed.

The candidate watermark is returned in the checkpoint and committed in memory
only after the runtime sends the batch, saves the checkpoint and returns `Ack`.
`Nack` keeps the previous watermark, making those documents eligible for another
poll. Fetched counters include these retries. Replayed messages have no stable
Iggy ID from this plugin, so consumers must allow for duplicates.

HTTP/search/JSON failures, `timed_out: true` and nonzero `_shards.failed` return a
poll error without advancing progress. The next poll retries after the configured
delay. There is no per-query retry loop or configured HTTP request timeout.
Poll errors are logged by the SDK; they do not change runtime connector status
or increment the runtime forwarding-error counter.

## State Information

The runtime checkpoint contains:

- `last_poll_timestamp`: Timestamp watermark for the returned batch.
- `total_documents_fetched`: Number of `_source` records fetched, including retries.
- `poll_count`: Successful search polls, including empty ones.
- `error_count` and `last_error`: Search failures and the latest error text.
- `processing_stats`: Payload bytes, average successful-poll elapsed milliseconds, last successful poll, empty polls and successful polls.

Successful polls include empty polls. Average duration includes the configured
polling delay, and payload bytes count serialized `_source` values, excluding
Elasticsearch response metadata. `last_document_id`, `last_scroll_id` and
`last_offset` remain in snapshots but do not drive polling.

The runtime saves the MessagePack checkpoint after sending the batch, including
empty successful polls. Its default file backend uses
`local_state/source_<key>.state`; the main runtime `[state]` configuration can
change that location or select HTTP storage. Invalid MessagePack state warns
and starts fresh. No plugin state configuration is required for this path.

### Optional Plugin JSON Snapshot

Append this configuration only when the separate plugin snapshot is needed:

```toml
[plugin_config.state]
enabled = true
storage_type = "file"
state_id = "elasticsearch_logs_connector"

[plugin_config.state.storage_config]
base_path = "./connector_states"
```

The plugin loads this JSON snapshot during `open()` and saves it during `close()`.
Its loaded values can override the already-restored runtime checkpoint. The
file name is `<state_id>.json`; without a state ID it uses
`elasticsearch_source_<numeric_plugin_id>.json`, and the default base path is
`./connector_states`. Missing directories are created when saving.

Only file storage is implemented. `storage_type = "elasticsearch"`, `"redis"` or
an unknown type warns and falls back to `./connector_states`, ignoring the
configured backend location. `auto_save_interval` and `tracked_fields` do not
affect the runtime plugin. It does not start the separately exported
`StateManager` background task.

This snapshot uses a direct JSON file write, separate from the runtime's atomic
checkpoint protocol. Snapshot read/write failures warn and do not fail startup
or close. To reset progress, stop the runtime and remove its checkpoint and any
configured plugin snapshot. Removing only the runtime file leaves the plugin
snapshot available for restoration.
