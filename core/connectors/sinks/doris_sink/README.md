# Apache Doris Sink

The Doris sink connector consumes JSON messages from Iggy streams and writes them to a pre-created Apache Doris table via Doris's [Stream Load HTTP API](https://doris.apache.org/docs/data-operate/import/import-way/stream-load-manual).

## Requirements

- The target Doris **database and table must be pre-created** before enabling the sink. The connector never issues DDL.
- Messages must arrive with `Payload::Json` (i.e. the configured stream schema is `json`). Other payload types are skipped with a warning.
- The Iggy message JSON shape must match the target table columns. Use the optional `columns` plugin setting if the column order differs from the JSON keys.

## How it works

1. For each batch of messages, the connector serializes the JSON payloads into a JSON array.
2. It computes a deterministic Stream Load `label` of the form `{label_prefix}-{stream}-{topic}-{partition}-{first_offset}-{last_offset}`. Doris dedupes loads by label inside its `label_keep_max_second` window, so a replayed batch (after restart, retry, etc.) is silently absorbed instead of producing duplicates.
3. It `PUT`s the batch to `{fe_url}/api/{database}/{table}/_stream_load` with HTTP Basic auth and the headers `format: json`, `strip_outer_array: true`, `label: <label>`.
4. The Doris frontend (FE) responds with a `307 Temporary Redirect` to a backend (BE). The connector follows the redirect manually so that the `Authorization` header is preserved across the hop (`reqwest`'s default policy strips it on cross-host redirects).
5. The HTTP body is parsed as JSON and the `Status` field decides the outcome:
   - `Success` → batch accepted.
   - `Label Already Exists` → idempotent replay, treated as success.
   - `Publish Timeout` or HTTP `5xx` → transient error (`Error::CannotStoreData`); the runtime can retry.
   - `Fail` or HTTP `4xx` → permanent error (`Error::PermanentHttpError`); retrying is not useful.

## Configuration

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `fe_url` | yes | — | Doris frontend HTTP base URL, e.g. `http://localhost:8030`. |
| `database` | yes | — | Target database. |
| `table` | yes | — | Target table. |
| `username` | yes | — | Doris user with `LOAD_PRIV` on the table. |
| `password` | yes | — | Doris user password. Stored as a `secrecy::SecretString` and never logged. |
| `label_prefix` | no | `iggy` | Prefix for the deterministic Stream Load label. |
| `batch_size` | no | `1000` | Maximum number of messages per Stream Load request. |
| `timeout_secs` | no | `30` | Per-request HTTP timeout. |
| `max_filter_ratio` | no | unset | Forwarded as the `max_filter_ratio` Stream Load header. |
| `columns` | no | unset | Forwarded as the `columns` Stream Load header. |
| `where` | no | unset | Forwarded as the `where` Stream Load header. |

### Example

```toml
type = "sink"
key = "doris"
enabled = true
version = 0
name = "Doris sink"
path = "target/release/libiggy_connector_doris_sink"
plugin_config_format = "toml"

[[streams]]
stream = "events"
topics = ["doris_events"]
schema = "json"
batch_length = 100
poll_interval = "5ms"
consumer_group = "doris_sink"

[plugin_config]
fe_url = "http://localhost:8030"
database = "iggy_demo"
table = "events"
username = "root"
password = "replace_with_secret"
label_prefix = "iggy"
batch_size = 1000
timeout_secs = 30
```

## Limitations (v1)

- JSON payload only. CSV and raw-text payloads are not supported yet.
- HTTP Basic auth only.
- No automatic table creation.
- No built-in retry middleware or circuit breaker — the runtime decides whether to redrive a failing batch. A hardening pass with `iggy_connector_sdk::retry::*` is planned as a follow-up.
