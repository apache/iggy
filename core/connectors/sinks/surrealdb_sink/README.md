# SurrealDB Sink Connector

Writes Apache Iggy stream messages into SurrealDB over the HTTP API.

The sink writes one SurrealQL bulk `INSERT IGNORE` per connector batch. Each
record uses a deterministic SurrealDB record id derived from stream, topic,
partition, offset and Iggy message id, so replayed batches are idempotent and
existing records are left untouched.

Persistent sink failures are at-most-once from the runtime's perspective:
messages may already be committed in Iggy before this connector exhausts its
write attempts, so failed writes are logged but not redelivered.

## Configuration

```toml
type = "sink"
key = "surrealdb"
enabled = true
version = 0
name = "SurrealDB sink"
path = "../../target/release/libiggy_connector_surrealdb_sink"
plugin_config_format = "toml"

[[streams]]
stream = "example_stream"
topics = ["example_topic"]
schema = "json"
batch_length = 1000
poll_interval = "5ms"
consumer_group = "surrealdb_sink_connector"

[plugin_config]
endpoint = "127.0.0.1:8000"
namespace = "iggy"
database = "connectors"
table = "iggy_messages"
username = "root"
password = "root"
auth_scope = "root"
use_tls = false
auto_define_table = true
define_indexes = true
batch_size = 1000
payload_format = "auto"
include_metadata = true
include_headers = true
include_checksum = true
include_origin_timestamp = true
query_timeout = "30s"
max_retries = 3
retry_delay = "100ms"
max_retry_delay = "5s"
verbose_logging = false
```

### Plugin Fields

| Field | Default | Description |
| --- | --- | --- |
| `endpoint` | required | SurrealDB HTTP host and port without scheme, for example `127.0.0.1:8000`. Full `http://` or `https://` URLs are also accepted. |
| `namespace` | required | SurrealDB namespace selected during `open()`. |
| `database` | required | SurrealDB database selected during `open()`. |
| `table` | required | Target table. Must be a safe SurrealQL identifier. |
| `username` / `password` | none | Optional credentials. |
| `auth_scope` | `root` | `root`, `namespace`, `database`, or `none`. |
| `use_tls` | `false` | Uses `https://` when true and `endpoint` has no scheme, `http://` otherwise. |
| `auto_define_table` | `false` | Runs `DEFINE TABLE IF NOT EXISTS <table> SCHEMALESS`. |
| `define_indexes` | `false` | Defines an offset index on stream/topic/partition/offset. Requires `auto_define_table`. |
| `batch_size` | `1000` | Maximum number of records per SurrealDB request. |
| `payload_format` | `auto` | `auto`, `json`, `text`, `base64`, or `binary` (`binary` is an alias for `base64`). |
| `include_metadata` | `true` | Stores stream/topic/partition/offset/timestamps/schema fields. |
| `include_headers` | `true` | Stores Iggy headers as a deterministic object. Raw headers are base64 encoded. |
| `include_checksum` | `true` | Stores `iggy_checksum`. |
| `include_origin_timestamp` | `true` | Stores `iggy_origin_timestamp`. |
| `query_timeout` | `30s` | SurrealDB HTTP request timeout. |
| `max_retries` | `3` | Total attempts for transient write failures. Values below `1` are raised to `1`. |
| `retry_delay` | `100ms` | Base retry delay. |
| `max_retry_delay` | `5s` | Capped exponential retry delay. |
| `verbose_logging` | `false` | Emits per-batch success logs at `info`. |

## Stored Shape

With metadata enabled, records contain:

- `id`: deterministic SurrealDB record id key
- `iggy_message_id`: original Iggy message id as a string
- `iggy_stream`, `iggy_topic`, `iggy_partition_id`, `iggy_offset`
- `iggy_timestamp`, `iggy_origin_timestamp`, `iggy_checksum`, `iggy_schema`
- `iggy_headers`
- `payload`
- `payload_encoding`

`payload_format = "auto"` stores decoded JSON payloads as queryable SurrealDB
values, text payloads as strings, and binary payloads as base64 strings.

The `messages_processed` counter reports valid records submitted to SurrealDB.
With `INSERT IGNORE`, duplicates can be ignored by SurrealDB while still being
counted as submitted.
