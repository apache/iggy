# Quickwit Sink

The Quickwit connector allows you to send data to the Quickwit API using HTTP. This sink will ensure that the index exists (create it if it doesn't) and will append the data to the index using the same batch size as specified in the Iggy configuration.

## Configuration

- `url`: The URL of the Quickwit server.
- `index`: The index configuration using YAML, as described in the [Quickwit index configuration docs](https://quickwit.io/docs/configuration/index-config)

```toml
[plugin_config]
url = "http://localhost:7280"
index = """
version: 0.9

index_id: events

doc_mapping:
  mode: strict
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

  timestamp_field: timestamp

indexing_settings:
  commit_timeout_secs: 10

retention:
  period: 7 days
  schedule: daily
"""
```
