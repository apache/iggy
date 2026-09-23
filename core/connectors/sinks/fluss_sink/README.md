# Apache Fluss Sink Connector

The Apache Fluss sink connector consumes messages from Apache Iggy streams and
writes them to [Apache Fluss](https://fluss.apache.org/) log or primary-key
tables. It can write every message to one configured table or route JSON
messages to tables named in their payloads.

The connector uses the
[Fluss Rust client](https://clients.fluss.apache.org/user-guide/rust/api-reference/)
and exposes its writer, connection, and security configuration.

## Features

- Appends to log tables and upserts or deletes rows in primary-key tables.
- Routes messages to one static table or multiple tables selected by a JSON field.
- Creates tables when configured, using a fixed schema for static routing or
  an inferred schema for multi-table routing.
- Supports per-table primary keys, partition keys, bucket settings, and properties.
- Flushes queued writes before each consumed batch completes.
- Supports Fluss writer retries, idempotence, buffering, and backpressure.
- Supports `PLAINTEXT` and SASL `PLAIN` client configuration.
- Optionally stores Iggy checksum, origin timestamp, and stream metadata.
- Stores payloads as Fluss `BYTES` or `STRING`.

## Build

From the repository root:

```bash
cargo build --release -p iggy_connector_fluss_sink
```

The connector runtime loads the resulting dynamic library from
`target/release/`. Adjust the `path` setting for the working directory and
operating system used by the runtime.

## Configuration

The following example uses the default `static` router. See
[`config.toml`](config.toml) for all available settings and table definitions.

```toml
type = "sink"
key = "fluss"
enabled = true
version = 0
name = "Fluss sink"
path = "target/release/libiggy_connector_fluss_sink"
verbose = false

[[streams]]
stream = "user_events"
topics = ["users", "orders"]
schema = "json"
batch_length = 100
poll_interval = "5ms"
consumer_group = "fluss_sink"

[plugin_config]
bootstrap_servers = "127.0.0.1:9123"
router_type = "static"
verbose_logging = false
target_database = "fluss"
target_table = "iggy_messages"
auto_create_table = true
include_metadata = true
include_checksum = true
include_origin_timestamp = true
payload_format = "json"
```

All plugin fields have defaults. Existing configurations remain valid when new
fields are added because missing fields use the connector defaults.

### Connector settings

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `router_type` | enum | `"static"` | `static` writes to one target table; `multi` selects a table from each JSON payload. |
| `route_key` | string | `"table"` | JSON string field containing `database.table` in `multi` mode. |
| `target_database` | string | `"fluss"` | Target database in `static` mode. The database must already exist. |
| `target_table` | string | `"iggy_messages"` | Target table in `static` mode. |
| `auto_create_table` | bool | `true` | Create missing tables. The static table is checked at startup; multi-table targets are checked when first used. |
| `include_metadata` | bool | `true` | Include Iggy offset, timestamp, stream, topic, and partition columns in an automatically created static table. |
| `include_checksum` | bool | `true` | Include the checksum column in an automatically created static table. |
| `include_origin_timestamp` | bool | `true` | Include the origin timestamp column in an automatically created static table. |
| `payload_format` | enum | `"json"` | Static-table payload column format: `bytea`, `json`, or `text`. Multi-table routing uses JSON fields as columns. |
| `verbose_logging` | bool | `false` | Emit a per-batch debug log when enabled; set the tracing filter to debug to display it. |

`target_database`, `target_table`, `include_metadata`, `include_checksum`,
`include_origin_timestamp`, and `payload_format` apply to static routing.
`route_key` applies to multi-table routing. `tables` applies to automatic
creation in either mode.

### Per-table creation settings

Add a `[plugin_config.tables."database.table"]` section for each table that
needs creation options. Entries for existing tables do not alter their schema.
An absent or empty `primary_keys` list creates a log table.

```toml
[plugin_config.tables."fluss.iggy_messages"]
primary_keys = []
bucket_keys = []
bucket_count = 1

[plugin_config.tables."fluss.iggy_messages".properties]
"table.datalake.enabled" = "false"
```

| Field | Type | Description |
| --- | --- | --- |
| `primary_keys` | string array | Column names for a primary-key table; omit or leave empty for a log table. |
| `partitioned_by` | string array | Columns used to partition the table. |
| `bucket_keys` | string array | Columns used to distribute rows across buckets. |
| `bucket_count` | integer | Bucket count passed to Fluss when `bucket_keys` is set. |
| `properties` | string map | Fluss table properties. |

Named columns must exist in the table schema. The sink creates the table, but
does not create its database or individual partition values. The
`partitioned_by` setting defines partition keys; it does not add partitions
for every value that messages may contain. Create those partitions separately
unless Fluss is configured to create them automatically.

### Fluss writer and connection settings

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `bootstrap_servers` | string | `"127.0.0.1:9123"` | Fluss coordinator address. |
| `writer_request_max_size` | i32 | `10485760` | Maximum writer request size in bytes. |
| `writer_acks` | string | `"all"` | Required acknowledgements. `"all"` waits for all required replicas. |
| `writer_retries` | i32 | `3` | Maximum retries for transient writer failures. |
| `writer_batch_size` | i32 | `2097152` | Target Fluss writer batch size in bytes. |
| `writer_batch_timeout_ms` | i64 | `100` | Maximum time to wait for a writer batch to fill before sending it. |
| `writer_bucket_no_key_assigner` | enum | `"sticky"` | Bucket selection for tables without bucket keys: `sticky` or `round_robin`. |
| `writer_enable_idempotence` | bool | `true` | Add writer IDs and per-bucket sequence numbers so Fluss can deduplicate retried batches. |
| `writer_max_inflight_requests_per_bucket` | usize | `5` | Maximum unacknowledged requests per bucket. Idempotent writes require a value no greater than `5`. |
| `writer_buffer_memory_size` | usize | `67108864` | Total memory in bytes available for buffered write batches. |
| `writer_buffer_wait_timeout_ms` | string | `"18446744073709551615"` | Maximum time to wait for writer buffer memory. This is a string because the default is `u64::MAX`, which TOML integers cannot represent. |
| `connect_timeout_ms` | u64 | `15000` | TCP connection timeout in milliseconds. |

Idempotent writes require `writer_acks = "all"` or `"-1"`,
`writer_retries > 0`, and
`writer_max_inflight_requests_per_bucket <= 5`.

### Security settings

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `security_protocol` | string | `"PLAINTEXT"` | Use `"PLAINTEXT"` without authentication or `"sasl"` for SASL authentication. Matching is case-insensitive. |
| `security_sasl_mechanism` | string | `"PLAIN"` | SASL mechanism. The pinned Fluss client supports only `PLAIN`. |
| `security_sasl_username` | string | `""` | SASL username. Required when `security_protocol = "sasl"`. |
| `security_sasl_password` | string | `""` | SASL password. Required when `security_protocol = "sasl"`; treated as a secret by the plugin. |

Example:

```toml
[plugin_config]
bootstrap_servers = "fluss.example.com:9123"
security_protocol = "sasl"
security_sasl_mechanism = "PLAIN"
security_sasl_username = "iggy"
security_sasl_password = "replace-with-secret"
```

## Routing modes

### Static table

`router_type = "static"` is the default. Every message goes to
`target_database.target_table`. The sink loads an existing table or, when
`auto_create_table = true`, creates it during startup using the schema below.
It converts each message into one row containing the configured Iggy metadata
and a payload column. Set `payload_format` to select the payload column type.

If a per-table configuration adds `primary_keys`, the sink uses a Fluss upsert
writer. Static rows have no `op` column, so this mode does not issue deletes.
Partitioned log tables are written row by row; other log tables use an Arrow
record batch.

### Multiple tables

Set `router_type = "multi"` to route each JSON message by `route_key`. The
field must contain a string in `database.table` form. For example:

```toml
[plugin_config]
bootstrap_servers = "127.0.0.1:9123"
router_type = "multi"
route_key = "table"
auto_create_table = true

[plugin_config.tables."fluss.orders"]
primary_keys = ["id"]
bucket_keys = ["id"]
bucket_count = 1
```

```json
{"table":"fluss.orders","id":42,"name":"updated","op":"u"}
```

The Iggy stream must use `schema = "json"` so the sink receives JSON payloads.
For an existing table, the sink loads its Fluss schema. For a missing table
with automatic creation enabled, it infers columns and types from the JSON
messages in the first batch for that table, then applies its `tables` settings.
Nested JSON objects and arrays become corresponding Fluss fields. Later
messages use the cached schema; the sink does not evolve an existing table.
Payload fields absent from that schema are ignored.

Tables without a primary key receive appends. Tables with a primary key
receive upserts, except when the resulting record batch has a `STRING` `op`
column with value `"d"`; those rows are deleted. Any other `op` value, or a
missing `op` column, results in an upsert. To use deletes with an existing
table, include `op` as a `STRING` column in its schema. The `table` and `op`
fields are regular columns when they are present in the table schema.

Messages without a valid route are skipped. If a table cannot be found and
`auto_create_table = false`, messages for that table are skipped. The sink
records skipped messages in its own error count, logged when it closes.

### Automatically created date partitions

The partitioned integration test uses multi-table routing so each JSON message
can supply an `event_day` string. The sink infers the table schema and creates
the partitioned log table. Fluss then creates date partitions from the table
properties; the sink does not call `create_partition`.

```toml
[plugin_config]
bootstrap_servers = "127.0.0.1:9123"
router_type = "multi"
route_key = "table"
auto_create_table = true

[plugin_config.tables."fluss.partitioned_iggy_messages"]
primary_keys = []
partitioned_by = ["event_day"]
bucket_keys = []
bucket_count = 1

[plugin_config.tables."fluss.partitioned_iggy_messages".properties]
"table.auto-partition.enabled" = "true"
"table.auto-partition.time-unit" = "DAY"
"table.auto-partition.time-format" = "yyyy-MM-dd"
"table.auto-partition.time-zone" = "UTC"
```

```json
{"table":"fluss.partitioned_iggy_messages","event_day":"2026-09-23","event":{"id":1,"name":"example"}}
```

Set `event_day` to the current UTC date in `YYYY-MM-DD` form when sending
messages. Fluss creates partitions on its
[auto-partition check interval](https://fluss.apache.org/docs/table-design/data-distribution/partitioning/#auto-partitioning),
which defaults to 10 minutes. The integration fixture shortens that interval
to one second and waits for the partition before reading rows.

## Static-table payload formats

| Value | Fluss type | Behavior |
| --- | --- | --- |
| `bytea` | `BYTES` | Stores raw payload bytes; JSON payloads are serialized to bytes. |
| `json` | `STRING` | Serializes JSON payloads, or stores text and Proto payloads as strings. |
| `text` | `STRING` | Uses the same conversion as `json`. |

`json` and `text` currently use the same Fluss schema and Arrow conversion. The
sink does not parse or validate JSON itself. Configure the Iggy stream with
`schema = "json"` when JSON validation is required before the sink receives the
message.

Any Iggy payload variant can be written with `bytea`. The `json` and `text`
formats skip raw, Avro, and FlatBuffer payloads, including valid UTF-8 bytes
in those variants.

## Generated static-table schema

With automatic creation and no `primary_keys` setting, static routing creates
a Fluss log table. Columns are generated in the following order:

| Column | Fluss type | Included when |
| --- | --- | --- |
| `id` | `STRING` | Always |
| `checksum` | `DECIMAL(20, 0)` | `include_checksum = true` |
| `iggy_offset` | `DECIMAL(20, 0)` | `include_metadata = true` |
| `iggy_timestamp` | `TIMESTAMP_LTZ(6)` | `include_metadata = true` |
| `iggy_stream` | `STRING` | `include_metadata = true` |
| `iggy_topic` | `STRING` | `include_metadata = true` |
| `iggy_partition_id` | `BIGINT` | `include_metadata = true` |
| `iggy_origin_timestamp` | `TIMESTAMP_LTZ(6)` | `include_origin_timestamp = true` |
| `payload` | `BYTES` or `STRING` | Always |

Message IDs are encoded as 32-character lowercase hexadecimal strings.
`DECIMAL(20, 0)` preserves the complete unsigned 64-bit range for offsets and
checksums. Timestamps are interpreted as microseconds since the Unix epoch.

### Manual table creation with Flink SQL

When `auto_create_table = false`, create the database and table through a
[Fluss catalog in Flink SQL](https://fluss.apache.org/docs/engine-flink/getting-started/)
before starting the connector. The following definition matches the default
`target_database = "fluss"`, `target_table = "iggy_messages"`, and
`payload_format = "json"` settings:

```sql
USE CATALOG fluss_catalog;

CREATE DATABASE IF NOT EXISTS `fluss`;
USE `fluss`;

CREATE TABLE `iggy_messages` (
    `id` STRING COMMENT 'Apache Iggy message ID',
    `checksum` DECIMAL(20, 0) COMMENT 'Apache Iggy message checksum',
    `iggy_offset` DECIMAL(20, 0) COMMENT 'Apache Iggy message offset',
    `iggy_timestamp` TIMESTAMP_LTZ(6)
        COMMENT 'Apache Iggy message timestamp',
    `iggy_stream` STRING COMMENT 'Apache Iggy stream name',
    `iggy_topic` STRING COMMENT 'Apache Iggy topic name',
    `iggy_partition_id` BIGINT COMMENT 'Apache Iggy partition ID',
    `iggy_origin_timestamp` TIMESTAMP_LTZ(6)
        COMMENT 'Apache Iggy message origin timestamp',
    `payload` STRING COMMENT 'Apache Iggy message payload'
)
COMMENT 'Stores Apache Iggy messages written by the Fluss sink connector';
```

Replace `fluss_catalog` with the name of the Fluss catalog configured in the
Flink SQL client. If `payload_format = "bytea"`, define `payload` as `BYTES`
instead of `STRING`.

Omit `checksum` when `include_checksum = false`. Omit `iggy_offset`,
`iggy_timestamp`, `iggy_stream`, `iggy_topic`, and `iggy_partition_id` when
`include_metadata = false`. Omit `iggy_origin_timestamp` when
`include_origin_timestamp = false`. Keep the remaining columns in the order
shown above.

Message headers are not stored.

The connector does not migrate or alter existing tables. A manually created
static table must use supported column names and compatible Fluss data types,
with `payload` as its final column.
Tables created by connector versions that used `STRING` for checksum, offset,
and timestamps must be recreated or migrated before using this schema.

## Write behavior

For each consumed batch, the sink converts messages into Arrow rows, opens an
append or upsert writer for each destination table, and flushes queued writes.
Synchronous row-write errors are counted by the sink and logged. A failed
flush returns an error for the batch. The sink reports its accumulated
processed and error counts when it closes, then closes the Fluss connection
with a 30-second graceful shutdown timeout.

The effective message count per call is controlled by the stream
`batch_length`. Fluss may combine those rows into byte-sized writer batches
according to `writer_batch_size` and `writer_batch_timeout_ms`.

## Limitations

- Databases must already exist. Partition values must exist before writes
  unless Fluss creates them automatically through its partitioning settings.
- Existing table schemas are not migrated.
- Multi-table routing requires JSON payloads and a string route field.

## Testing

Run the Fluss sink unit tests from the repository root:

```bash
cargo test -p iggy_connector_fluss_sink
```

The integration tests in `core/integration/tests/connectors/fluss/` exercise
table creation, partitioned writes, primary-key updates and deletes, and
multi-table schema inference against a Fluss cluster. They require Docker.
