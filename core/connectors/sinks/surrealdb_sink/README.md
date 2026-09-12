# SurrealDB Sink Connector

Writes Apache Iggy stream messages into SurrealDB over the HTTP API.

The sink splits each poll into chunks of at most `batch_size` messages and writes
one SurrealQL bulk `INSERT IGNORE ... RETURN NONE` per chunk containing valid
records. Each record uses a deterministic SurrealDB record id derived from
stream, topic, partition, offset and Iggy message id. Replaying those identities
leaves existing records and their payloads untouched. There is no cross-poll buffer.

The runtime auto-commits while polling, logs/counts failed plugin callbacks, and
continues without replaying them. Deterministic IDs protect repeated writes of
the same identities, but do not ensure that a failed batch will be delivered.

## Configuration

From the matching 0.9.0/edge Iggy checkout root, build the plugin:

```bash
cargo build --release -p iggy_connector_surrealdb_sink
```

Use the [sink guide](https://iggy.apache.org/docs/connectors/sinks/sink/) for the
broker credentials and main runtime configuration. Save this connector entry in
its connector directory and start the runtime from the checkout root. The
[SurrealDB walkthrough](https://iggy.apache.org/docs/connectors/sinks/surrealdb/)
includes local backend, Iggy CLI and SQL query commands.

```toml
type = "sink"
key = "surrealdb"
enabled = true
version = 0
name = "SurrealDB sink"
path = "target/release/libiggy_connector_surrealdb_sink"
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
| `username` / `password` | none | Both required unless `auth_scope = "none"`. |
| `auth_scope` | `root` | `root`, `namespace`, `database`, or `none`. |
| `use_tls` | `false` | Uses `https://` when true and `endpoint` has no scheme, `http://` otherwise. |
| `auto_define_table` | `false` | Creates missing namespace/database and a schemaless table; requires root authentication scope. |
| `define_indexes` | `false` | Defines a non-unique stream/topic/partition/offset index; requires metadata and only runs with automatic DDL. |
| `batch_size` | `1000` | Maximum input records per insert chunk; zero is raised to one. |
| `payload_format` | `auto` | `auto`, `json`, `text`, `base64`, or `binary` (`binary` is an alias for `base64`). |
| `include_metadata` | `true` | Stores stream/topic/partition/offset/timestamp/schema fields. |
| `include_headers` | `true` | Stores Iggy headers as a deterministic object. Raw headers are base64 encoded. |
| `include_checksum` | `true` | Stores `iggy_checksum`. |
| `include_origin_timestamp` | `true` | Stores `iggy_origin_timestamp`. |
| `query_timeout` | `30s` | Timeout per HTTP request, not a total retry/reconnect budget. |
| `max_retries` | `3` | Total attempts for transient write failures. Values below `1` are raised to `1`. |
| `retry_delay` | `100ms` | Base retry delay. |
| `max_retry_delay` | `5s` | Capped exponential retry delay. |
| `verbose_logging` | `false` | Emits per-batch success logs at `info`. |

Namespace, database and table names must start with an ASCII letter or underscore
and contain only ASCII letters, digits and underscores. Endpoints reject embedded
credentials, paths beyond `/`, query strings and fragments. An explicit HTTP(S)
scheme takes precedence over `use_tls`. Authentication-scope and payload-format
names are case-insensitive; unknown values reject startup.

Root/namespace/database authentication requires both credentials. Startup checks
`/signin`; subsequent SQL uses Basic authentication with the configured scope.
Namespace/database users require pre-existing resources and automatic DDL disabled.
`auth_scope = "none"` ignores credentials and requires a server that permits the
unauthenticated operations. Automatic DDL uses `IF NOT EXISTS` and does not
migrate existing definitions. Index creation uses `<table>_iggy_offset_idx`.
Disabling metadata with indexes enabled rejects startup; disabling automatic DDL
with indexes enabled logs a warning and skips index creation.

Invalid duration strings warn and fall back to `1s`; zero is accepted. A maximum
retry delay smaller than the base delay is raised to the base delay.

## Stored Shape

Every submitted record contains:

- `id`: deterministic SurrealDB record id key
- `iggy_message_id`: original Iggy message id as a string
- `payload`
- `payload_encoding`

`include_metadata` adds `iggy_stream`, `iggy_topic`, `iggy_partition_id`,
`iggy_offset`, `iggy_timestamp` and `iggy_schema`. The checksum, origin timestamp
and headers have independent inclusion flags. `iggy_headers` is omitted when
headers are absent or empty. Non-raw headers are strings, including numeric and
boolean values; raw headers use `{"data":"AQID","iggy_header_encoding":"base64"}`.

Partition IDs, offsets, timestamps and checksums are strings. Timestamps retain
Iggy's microsecond values. Record keys encode stream/topic UTF-8 bytes as hex,
then partition, offset and the 32-digit hexadecimal message ID. Payload and
transform changes do not alter that identity.

`payload_format = "auto"` stores decoded JSON payloads as queryable SurrealDB
values, text/Proto variants as strings, and raw/Avro/FlatBuffer bytes as base64
strings, even when raw bytes contain valid JSON. Explicit `json` parses other
payload variants as JSON, `text` requires UTF-8, and `base64`/`binary` encodes the
payload bytes. Invalid conversions reject that record. Destination schema,
numeric and other value constraints still apply.

The sink's `messages_processed` counter counts records submitted in successful
SQL statements, including ignored records. On SurrealDB 3.1.4, `INSERT IGNORE`
also silently skips unique-index conflicts and field-assertion failures while
accepting valid records from the same chunk. Runtime counters instead reflect
whether the whole callback succeeded. Neither counts newly inserted rows.

## Failures and Retries

Malformed records are skipped while valid records in the same chunk are still
submitted. A failed chunk does not stop later chunks; the last error is returned.
HTTP status and every SQL statement status are checked. Successful chunks are not
rolled back when another chunk fails.

The sink retries transaction-conflict errors, connection/timeouts and HTTP 408,
429, 500, 502, 503 and 504, using capped exponential backoff with jitter.
`Retry-After` is not used. Connection errors trigger reconnection and repeat
sign-in, health and optional DDL; failed reconnection stops that chunk.
Deterministic `INSERT IGNORE` IDs preserve existing records but provide no
end-to-end at-least-once delivery guarantee.
