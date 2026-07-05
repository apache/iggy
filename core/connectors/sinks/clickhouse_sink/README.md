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
retry_delay = 1  # seconds
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
| `retry_delay` | u64 | `1` | Delay between retries, in seconds |
| `verbose_logging` | bool | `false` | Log inserts at info level instead of debug |

> **TODO:** `database` and `table` values are interpolated directly into SQL. Currently only
> single quotes are escaped; backslashes pass through unchanged, which can misparse string
> literals if a value ends with `\`. A future improvement should validate both fields against a
> strict allowlist (`^[A-Za-z_][A-Za-z0-9_]*$`) at config load and escape backslashes in SQL
> string literals. Deferred because these sinks run in operator-controlled environments where
> config values are trusted.

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

The table must already exist. Columns with an ordinary `DEFAULT` expression can be omitted from the message — the connector emits a `0x01` prefix byte to signal that the default should be used. `MATERIALIZED`, `ALIAS`, and `EPHEMERAL` columns are not insertable and are dropped from the schema entirely.

**Supported types:** all integer and float primitives, `String`, `FixedString(n)`, `Bool`/`Boolean`, `UUID`, `Date`, `Date32`, `DateTime`, `DateTime64(p)`, `Decimal` (precision 1-38; `Decimal256` is not supported), `IPv4`, `IPv6`, `Enum8`, `Enum16`, and the composites `Nullable(T)`, `Array(T)`, `Map(K, V)`, `Tuple(...)`. `LowCardinality(T)` is transparently unwrapped to its inner type `T` (RowBinary serialises it identically).

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
retry_delay = 1  # seconds
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

The connector retries failed inserts up to `max_retries` times, starting from `retry_delay`. Retryable HTTP errors back off exponentially with full jitter; network errors back off linearly with the attempt count. Non-retryable errors fail immediately.

On shutdown the connector logs the total number of messages processed.

### Bad rows in a batch

A message whose payload type does not match the chosen format (for example a text payload in JSON mode) is always skipped with a warning. The rest of the batch is still sent.

The `rowbinary` format has one extra case. It turns each row into binary and writes it straight into the batch buffer, so a row that cannot be converted (a value that does not fit the target column) cannot be skipped cleanly — a half-written row would corrupt the rows after it. In that case the **whole batch fails** on the first bad row and is retried as a unit per the rules above.

If a single malformed row keeps failing, every retry of that batch will fail too. Fix or remove the bad message at the source, or switch to the `json` / `string` format, which skip bad rows instead of failing the batch.

### Delivery semantics: at-least-once

This connector provides **at-least-once** delivery — not exactly-once. Retries resend the full batch body without an `insert_deduplication_token`, so if the server applied a batch but the acknowledgement was lost in transit (network drop, timeout), the retry will insert the same rows again.

**Affected table engines:**

- `MergeTree` — no deduplication at all; duplicate rows will be stored.
- `ReplicatedMergeTree` — has implicit block-level deduplication based on the data checksum (controlled by `replicated_deduplication_window`, default 100 blocks), which will suppress duplicates in the common retry case as long as the window has not been exceeded.

If your workload cannot tolerate duplicate rows, either:

1. Use a `ReplicatedMergeTree` table and keep `max_retries` low enough that retries stay within the deduplication window, or
2. Use a `CollapsingMergeTree` / `ReplacingMergeTree` and apply deduplication at query time, or
3. Accept duplicates at write time and deduplicate with `DISTINCT` or `GROUP BY` in your queries.
