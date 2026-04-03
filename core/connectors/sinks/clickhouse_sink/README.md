# ClickHouse Sink Connector

The ClickHouse sink connector consumes messages from Iggy topics and inserts them into ClickHouse tables. Supports three insert formats: `json_each_row` (default), `row_binary`, and `string` passthrough.

## Features

- **Multiple Insert Formats**: Insert as `JSONEachRow`, `RowBinaryWithDefaults`, or raw string passthrough (CSV/TSV/JSON)
- **Schema Validation**: In `row_binary` mode, the table schema is fetched and validated at startup
- **Automatic Retries**: Configurable retry count and delay for transient errors
- **Batch Processing**: Insert messages in configurable batches via the stream configuration

## Configuration

```toml
type = "sink"
key = "clickhouse"
enabled = true
version = 0
name = "ClickHouse sink"
path = "target/release/libiggy_connector_clickhouse_sink"

[[streams]]
stream = "example_stream"
topics = ["example_topic"]
schema = "json"
batch_length = 1000
poll_interval = "5ms"
consumer_group = "clickhouse_sink_connector"

[plugin_config]
url = "http://localhost:8123"
database = "default"
username = "default"
password = ""
table = "events"
insert_format = "json_each_row"
timeout_seconds = 30
max_retries = 3
retry_delay = "1s"
verbose_logging = false
```

## Configuration Options

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `url` | string | required | ClickHouse HTTP endpoint |
| `table` | string | required | Target table name |
| `database` | string | `"default"` | ClickHouse database |
| `username` | string | `"default"` | ClickHouse username |
| `password` | string | `""` | ClickHouse password |
| `insert_format` | string | `"json_each_row"` | Insert format: `json_each_row`, `row_binary`, or `string` |
| `string_format` | string | `"json_each_row"` | ClickHouse format for `string` mode: `json_each_row`, `csv`, or `tsv` |
| `timeout_seconds` | u64 | `30` | HTTP request timeout |
| `max_retries` | u32 | `3` | Max retry attempts on transient errors |
| `retry_delay` | string | `"1s"` | Delay between retries (e.g. `500ms`, `2s`) |
| `verbose_logging` | bool | `false` | Log inserts at info level instead of debug |

## Insert Formats

### `json_each_row` (Default)

Accepts messages with a `Payload::Json` payload. Each message is sent as a JSON object on its own line using ClickHouse's `JSONEachRow` format. ClickHouse handles type coercion from the JSON values to the column types, so the table can have any schema.

```toml
[plugin_config]
url = "http://localhost:8123"
table = "events"
insert_format = "json_each_row"
```

### `row_binary`

Accepts messages with a `Payload::Json` payload. At startup the connector fetches the table schema from `system.columns` and validates that all column types are supported. Messages are then serialised to ClickHouse's `RowBinaryWithDefaults` binary format, which is more efficient than JSON for large volumes.

The table must already exist. Columns with a `DEFAULT` or `MATERIALIZED` expression can be omitted from the message — the connector will emit a `0x01` prefix byte to signal that the default should be used.

**Supported types:** all integer and float primitives, `String`, `FixedString(n)`, `Bool`/`Boolean`, `UUID`, `Date`, `Date32`, `DateTime`, `DateTime64(p)`, `Decimal`, `IPv4`, `IPv6`, `Enum8`, `Enum16`, and the composites `Nullable(T)`, `Array(T)`, `Map(K, V)`, `Tuple(...)`. `LowCardinality(T)` is transparently unwrapped to its inner type `T` (RowBinary serialises it identically).

**Unsupported types** (cause startup to fail): `Variant`, `JSON` (native column type), and geo types.

```toml
[plugin_config]
url = "http://localhost:8123"
table = "events"
insert_format = "row_binary"
```

### `string`

Accepts messages with a `Payload::Text` payload and passes them through to ClickHouse without modification. Use `string_format` to tell ClickHouse which format to expect.

```toml
[plugin_config]
url = "http://localhost:8123"
table = "events"
insert_format = "string"
string_format = "csv"   # or "tsv" or "json_each_row"
```

## Example Configs

### JSON Events

```toml
[[streams]]
stream = "events"
topics = ["user_events"]
schema = "json"
batch_length = 500
poll_interval = "10ms"
consumer_group = "clickhouse_sink"

[plugin_config]
url = "http://localhost:8123"
database = "analytics"
table = "user_events"
insert_format = "json_each_row"
```

### High-Throughput with RowBinary

```toml
[[streams]]
stream = "metrics"
topics = ["app_metrics"]
schema = "json"
batch_length = 5000
poll_interval = "5ms"
consumer_group = "clickhouse_sink"

[plugin_config]
url = "http://localhost:8123"
database = "telemetry"
table = "metrics"
insert_format = "row_binary"
max_retries = 5
retry_delay = "500ms"
verbose_logging = true
```

### CSV Passthrough

```toml
[[streams]]
stream = "exports"
topics = ["csv_data"]
schema = "text"
batch_length = 1000
poll_interval = "50ms"
consumer_group = "clickhouse_sink"

[plugin_config]
url = "http://localhost:8123"
table = "raw_imports"
insert_format = "string"
string_format = "csv"
```

## Reliability

The connector retries failed inserts up to `max_retries` times with a fixed delay of `retry_delay` between attempts. The delay is applied as-is on each attempt (not exponential backoff). Non-retryable errors fail immediately.

On shutdown the connector logs the total number of messages processed and errors encountered.
