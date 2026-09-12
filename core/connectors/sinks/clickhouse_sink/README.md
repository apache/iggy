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
| `timeout_seconds` | u64 | `30` | HTTP request timeout in seconds |
| `max_retries` | u32 | `3` | Total attempts for startup requests and transient insert errors; at least one even when `0` |
| `retry_delay` | u64 | `1` | Base for exponential retry delay, in seconds |
| `verbose_logging` | bool | `false` | Log inserts at info level instead of debug |

## Insert Formats

### `json_each_row` (Default)

Accepts messages with a `Payload::Json` payload. Each payload is serialized on its own line using ClickHouse's `JSONEachRow` format. Send JSON objects whose fields and values are compatible with the existing table and its ClickHouse input settings. The connector does not validate JSON rows against the table schema before sending them.

```toml
[plugin_config]
url = "http://localhost:8123"
table = "events"
insert_format = "json_each_row"
```

### `row_binary`

Accepts messages with a `Payload::Json` payload. At startup the connector fetches the table schema from `system.columns` and validates that all column types are supported. Messages are then serialised to ClickHouse's `RowBinaryWithDefaults` binary format, which is more efficient than JSON for large volumes.

Requires [ClickHouse 23.7 or newer](https://presentations.clickhouse.com/2023-release-23.7/index.html), when `RowBinaryWithDefaults` was introduced. Older servers reject the format; use `json_each_row` instead.

The table must already exist. Columns with an ordinary `DEFAULT` expression can be omitted from the message - the connector emits a `0x01` prefix byte to signal that the default should be used. An explicit JSON `null` requires a nullable column and is stored as `NULL`, even when that column has a default. Missing columns without defaults must be nullable. The connector excludes `MATERIALIZED`, `ALIAS`, and `EPHEMERAL` columns from its insert schema.

The schema is captured once at startup and never refreshed. Do not `ALTER TABLE` the target while the connector runs. See [Schema changes while running](#schema-changes-while-running).

**Supported types:** the 8/16/32/64-bit integer and float primitives (`Int8`-`Int64`, `UInt8`-`UInt64`, `Float32`, `Float64`), `String`, `FixedString(n)`, `Bool`/`Boolean`, `UUID`, `Date`, `Date32`, `DateTime`, `DateTime64(p)`, `Decimal` (precision 1-38; `Decimal256` is not supported), `IPv4`, `IPv6`, `Enum8`, `Enum16`, and the composites `Nullable(T)`, `Array(T)`, `Map(K, V)`, `Tuple(...)`. `LowCardinality(T)` is transparently unwrapped to its inner type `T` (RowBinary serialises it identically).

**Unsupported types** (cause startup to fail): the 128/256-bit wide integers (`Int128`, `UInt128`, `Int256`, `UInt256`), `Variant`, `JSON` (native column type), and geo types.

```toml
[plugin_config]
url = "http://localhost:8123"
table = "events"
insert_format = "row_binary"
```

### `string`

Accepts messages with a `Payload::Text` payload and appends a newline to each payload that does not already end with one. Set the stream `schema = "text"` and use `string_format` to tell ClickHouse which format to expect.

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

Insert requests retry HTTP 408, 429, and 5xx responses, plus network and timeout errors. Other unsuccessful HTTP statuses fail immediately. `max_retries` is the total attempt limit, with at least one attempt even when set to `0`. Before retry number `n` (starting at 1), the delay is sampled from zero through `min(retry_delay * 2^n, 60)` seconds.
With the defaults, there are at most three attempts and the first retry waits between zero and two seconds. The startup ping and, in `row_binary` mode, schema fetch use the same limit and backoff but retry every error.

On shutdown the connector logs the total number of messages processed.

### Bad rows in a batch

A message whose payload type does not match the chosen format (for example a text payload in JSON mode) is skipped with an error log. The rest of the batch is still sent.

The `row_binary` format fails the whole batch on the first JSON row whose values cannot be converted to the target columns. This occurs before any insert request, so the batch does not enter the plugin's insert retry loop. No partial binary row is sent.

A batch with no serializable payloads returns success without an insert. In `json_each_row` and `string` modes, ClickHouse still validates the submitted data and can reject an insert containing a malformed row.

### Schema changes while running

In `row_binary` mode the table schema is fetched once at startup and cached for the lifetime of the connector. The insert stream is **positional**: rows are written as a bare `RowBinaryWithDefaults` body in the column order captured at startup, and ClickHouse decodes them against the table's current column order.

An `ALTER TABLE` that runs while the connector is live breaks that assumption. Adding, dropping, or reordering a column shifts the byte layout by one or more columns. Depending on how the shifted bytes decode, ClickHouse either rejects the batch as malformed or, worse, stores it silently with values landing in the wrong columns. Nothing detects this at runtime, so the corruption is easy to miss.

The `json_each_row` format, including `string` mode with `string_format = "json_each_row"`, maps values by field name, but changed column names, types, or constraints can still make inserts fail. The plain `CSV` and `TSV` string formats use the current table column order.

Until the hardening below lands, treat the `row_binary` schema as fixed for the connector's lifetime: **restart the connector after any `ALTER TABLE`** on the target table so it re-fetches the schema.

Two planned fixes remove the restriction:

1. **Explicit column list in the INSERT.** Emitting `INSERT INTO db.table (col1, col2, ...) FORMAT RowBinaryWithDefaults` binds the stream to column *names* instead of table position. ClickHouse then routes each value by name, applies `DEFAULT` for columns the connector omits, and returns a hard error (instead of silently corrupting rows) when a named column has been dropped or renamed. This makes added and reordered columns safe and turns the remaining drift into a visible failure.
2. **Refresh the schema on a failed insert.** When an insert fails with a data error, re-fetch the schema from `system.columns` and rebuild the column list before the batch is retried, letting the connector recover from a drop or rename on its own rather than failing every retry against a stale snapshot.

### Delivery semantics

The runtime uses consumer auto-commit and does not replay a failed sink batch. End-to-end at-least-once delivery is therefore not guaranteed; see [sink guide](https://iggy.apache.org/docs/connectors/sinks/sink#sample-implementation).

Plugin retries resend the same batch without an `insert_deduplication_token`, so a lost acknowledgement can also produce duplicate rows. ClickHouse deduplication depends on the table engine, query settings, identical retry data, and the retained deduplication window:

- `ReplicatedMergeTree` enables a deduplication log by default, controlled by `replicated_deduplication_window` and `replicated_deduplication_window_seconds`.
- Non-replicated `MergeTree` can also deduplicate when `non_replicated_deduplication_window` is positive; its default is zero.

See [ClickHouse insert deduplication](https://clickhouse.com/docs/concepts/features/operations/insert/deduplicating-inserts-on-retries) for the settings and limits. A small retry count alone does not ensure that a retry remains within the window, because other inserts can evict its deduplication record.

## Testing

Integration tests against a live ClickHouse container cover only the end-to-end path from Iggy to ClickHouse: messages produced to a stream land as rows in the target table. They are intentionally **not exhaustive** over the wire format. They do not, for example, round-trip a `UUID` column, a `DEFAULT` / `MATERIALIZED` / `ALIAS` column, or the `RowBinaryWithDefaults` `0x01` use-default path against the real server.

Those cases are instead pinned by unit tests that assert exact output bytes for every supported type, including `UUID` word order, the `0x01` use-default prefix byte, and the schema parser dropping `MATERIALIZED` / `ALIAS` / `EPHEMERAL` columns while flagging `DEFAULT` ones.

Byte-exact unit tests plus a minimal e2e path is a deliberate trade. Containerized integration tests are costly to run, and the residual risk they would cover, a real server disagreeing with our model of the wire format, is low: the `RowBinaryWithDefaults` format is stable and unlikely to change under us.
