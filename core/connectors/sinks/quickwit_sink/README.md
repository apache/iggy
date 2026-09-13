# Quickwit Sink

The Quickwit connector sends data to the Quickwit API using HTTP. It checks readiness when opening, creates the index if needed, and appends messages as NDJSON. Requests are split at 8 MiB, including the newline after each document. A larger individual document is logged and rejected while other documents continue.

## Configuration

| Key | Default | Description |
| --- | --- | --- |
| `url` | Required | Quickwit base URL with an `http` or `https` scheme and host. Path prefixes and a trailing slash are supported; query strings and fragments are rejected. |
| `index` | Required | Index configuration as YAML, with a nonempty `index_id`. See the [Quickwit index configuration docs](https://quickwit.io/docs/configuration/index-config). |
| `verbose_logging` | `false` | Log received and ingested message counts at `info` instead of `debug`. |
| `max_retries` | `3` | Total HTTP attempts including the first; `1` disables retries. |
| `retry_delay` | `"1s"` | Base exponential delay for HTTP retries and readiness probes. |
| `retry_max_delay` | `"5s"` | Maximum delay between HTTP retry attempts. |
| `max_open_retries` | `10` | Total readiness probes including the first; `1` disables retries. |
| `open_retry_max_delay` | `"30s"` | Maximum delay between readiness probes. |
| `timeout` | `"30s"` | Timeout for each HTTP request. |

Duration values require units, such as `250ms` or `30s`. Invalid or zero durations prevent initialization. Unknown plugin configuration keys are rejected.

Set `plugin_config_format` in the connector TOML or with the `IGGY_CONNECTORS_SINK_QUICKWIT_PLUGIN_CONFIG_FORMAT` environment variable.

```toml
[plugin_config]
url = "http://localhost:7280"
verbose_logging = false
# Total attempts including the first; 1 disables retries.
max_retries = 3
retry_delay = "1s"
retry_max_delay = "5s"
# Total readiness probes including the first; 1 disables retries.
max_open_retries = 10
open_retry_max_delay = "30s"
timeout = "30s"
index = """
version: 0.9

index_id: events

doc_mapping:
  mode: dynamic
  field_mappings:
    - name: timestamp
      type: datetime
      input_formats: [unix_timestamp]
      output_format: unix_timestamp_nanos
      indexed: false
      fast: true
      fast_precision: milliseconds
    - name: service_name
      type: text
      tokenizer: raw
      fast: true
    - name: random_id
      type: text
      tokenizer: raw
      fast: true
    - name: user_id
      type: text
      tokenizer: raw
      fast: true
    - name: user_type
      type: u64
      fast: true
    - name: source
      type: text
      tokenizer: default
    - name: state
      type: text
      tokenizer: default
    - name: message
      type: text
      tokenizer: default

  # Enable only when every document contains timestamp.
  # timestamp_field: timestamp

indexing_settings:
  commit_timeout_secs: 10

# Retention requires timestamp_field above.
# retention:
#   period: 7 days
#   schedule: daily
"""
```

## Document shapes

The stream's `schema` selects the runtime decoder. The sink sends JSON objects using these shapes:

| Payload | Example document |
| --- | --- |
| JSON object, including an object parsed from raw bytes | `{"message":"ready"}` |
| JSON array or scalar | `{"data":[1,2],"data_type":"json"}` or `{"data":42,"data_type":"json"}` |
| Raw UTF-8 that is not a JSON object | `{"data":"ready","data_type":"raw","data_encoding":"utf8"}` |
| Raw non-UTF-8 bytes | `{"data":"/wCA","data_type":"raw","data_encoding":"base64"}` |
| Text | `{"text":"ready","data_type":"text"}` |

Raw JSON arrays and scalars remain UTF-8 strings in the raw wrapper. Malformed JSON also preserves the original bytes. `data_encoding` distinguishes literal text from base64. The sink handles `Payload::Avro` and `Payload::FlatBuffer` with the raw path, and `Payload::Proto` with the text wrapper. Runtime decoder settings determine which payload variant reaches the sink.

The examples use `mode: dynamic` to retain wrapper fields. With `mode: strict`, map every field emitted by the selected payload shape or Quickwit rejects the document during indexing, even after a successful HTTP response. Timestamp sharding and retention are optional here: raw/text wrappers have no `timestamp`, and the `add_fields` transform only enriches JSON payloads. Configure them only when every document supplies the required timestamp.

## Delivery semantics

Transient HTTP failures, including 429, can retry a request that Quickwit already accepted. Quickwit ingest has no deduplication key, so these retries can produce duplicate documents. Set `max_retries = 1` to disable HTTP retries and use at-most-once request submission.

The sink cannot guarantee at-least-once delivery. The runtime commits offsets when polling and ignores the plugin's consume return code. A permanent error or exhausted retry budget is logged, but affected messages are not redelivered. The runtime's processed-message count does not prove successful indexing. These runtime limitations are tracked in [#2927](https://github.com/apache/iggy/issues/2927) and [#2928](https://github.com/apache/iggy/issues/2928).

Chunks are independent: successful writes remain committed if another chunk fails. The sink continues later chunks and returns the last error. A successful ingest response acknowledges submission for indexing, not that every document passed the index mapping. This sink has no circuit breaker.
