# Apache Fluss Sink Connector

The Apache Fluss sink connector consumes messages from Apache Iggy streams and
appends them to an [Apache Fluss](https://fluss.apache.org/) log table. It can
create the target table automatically, preserve selected Iggy metadata, and
store payloads as either Fluss `BYTES` or `STRING`.

The connector uses the
[Fluss Rust client](https://clients.fluss.apache.org/user-guide/rust/api-reference/)
and exposes its writer, connection, and security configuration.

## Features

- Appends Iggy messages to a Fluss log table.
- Creates the target table on demand.
- Flushes all pending writes before each consumed batch completes.
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

The following example uses the default plugin settings explicitly. See
[`config.toml`](config.toml) for the complete configuration file.

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
| `target_database` | string | `"fluss"` | Target Fluss database. The database must already exist. |
| `target_table` | string | `"iggy_messages"` | Target Fluss table. |
| `auto_create_table` | bool | `true` | Create the target table if it does not exist before writing a batch. Existing tables are left unchanged. |
| `include_metadata` | bool | `true` | Add the Iggy offset, timestamp, stream, topic, and partition columns. |
| `include_checksum` | bool | `true` | Add the Iggy message checksum column. |
| `include_origin_timestamp` | bool | `true` | Add the Iggy origin timestamp column. |
| `payload_format` | enum | `"json"` | Payload storage format: `bytea`, `json`, or `text`. |

### Fluss writer and connection settings

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `bootstrap_servers` | string | `"127.0.0.1:9123"` | Fluss coordinator address. |
| `writer_request_max_size` | i32 | `10485760` | Maximum writer request size in bytes. |
| `writer_acks` | string | `"all"` | Required acknowledgements. `"all"` waits for all required replicas. |
| `writer_retries` | i32 | `2147483647` | Maximum retries for transient writer failures. |
| `writer_batch_size` | i32 | `2097152` | Target Fluss writer batch size in bytes. |
| `writer_batch_timeout_ms` | i64 | `100` | Maximum time to wait for a writer batch to fill before sending it. |
| `writer_bucket_no_key_assigner` | enum | `"sticky"` | Bucket selection for tables without bucket keys: `sticky` or `round_robin`. |
| `writer_enable_idempotence` | bool | `true` | Add writer IDs and per-bucket sequence numbers so Fluss can deduplicate retried batches. |
| `writer_max_inflight_requests_per_bucket` | usize | `5` | Maximum unacknowledged requests per bucket. Idempotent writes require a value no greater than `5`. |
| `writer_buffer_memory_size` | usize | `67108864` | Total memory in bytes available for buffered write batches. |
| `writer_buffer_wait_timeout_ms` | string | `"18446744073709551615"` | Maximum time to wait for writer buffer memory. This is a string because the default is `u64::MAX`, which TOML integers cannot represent. |
| `connect_timeout_ms` | u64 | `120000` | TCP connection timeout in milliseconds. |

Idempotent writes require `writer_acks = "all"` or `"-1"`,
`writer_retries > 0`, and
`writer_max_inflight_requests_per_bucket <= 5`.

### Security settings

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `security_protocol` | string | `"PLAINTEXT"` | Use `"PLAINTEXT"` without authentication or `"sasl"` for SASL authentication. Matching is case-insensitive. |
| `security_sasl_mechanism` | string | `"PLAIN"` | SASL mechanism. The pinned Fluss client supports only `PLAIN`. |
| `security_sasl_username` | string | `""` | SASL username. Required when `security_protocol = "sasl"`. |
| `security_sasl_password` | string | `""` | SASL password. Required when `security_protocol = "sasl"` and omitted from serialized connector configuration. |

Example:

```toml
[plugin_config]
bootstrap_servers = "fluss.example.com:9123"
security_protocol = "sasl"
security_sasl_mechanism = "PLAIN"
security_sasl_username = "iggy"
security_sasl_password = "replace-with-secret"
```

## Payload formats

| Value | Fluss type | Behavior |
| --- | --- | --- |
| `bytea` | `BYTES` | Serializes the Iggy payload to bytes and preserves it in a binary column. |
| `json` | `STRING` | Serializes the payload to bytes and stores the resulting UTF-8 string. |
| `text` | `STRING` | Serializes the payload to bytes and stores the resulting UTF-8 string. |

`json` and `text` currently use the same Fluss schema and row conversion. The
sink does not parse or validate JSON itself. Configure the Iggy stream with
`schema = "json"` when JSON validation is required before the sink receives the
message.

Any Iggy payload variant can be written with `bytea`. The `json` and `text`
formats reject payload bytes that are not valid UTF-8.

## Generated table schema

When `auto_create_table = true`, the connector creates an append-only Fluss log
table without a primary key. Columns are generated in the following order:

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
table must use the same column order and compatible Fluss data types.
Tables created by connector versions that used `STRING` for checksum, offset,
and timestamps must be recreated or migrated before using this schema.

## Write behavior

For every batch received from the Iggy connector runtime, the sink:

1. Creates the table if `auto_create_table` is enabled and the table is missing.
2. Opens an append writer for the target table.
3. Converts each Iggy message to a Fluss row.
4. Appends every row and flushes the writer.

The effective message count per call is controlled by the stream
`batch_length`. Fluss may combine those rows into byte-sized writer batches
according to `writer_batch_size` and `writer_batch_timeout_ms`.

## Limitations

- The connector writes append-only log tables and does not support primary-key
  upserts.
- The target database is not created automatically.
- Existing table schemas are not migrated.
- The pinned `fluss-rs` 0.1 client does not expose a public graceful connection
  shutdown method. Each consumed batch is flushed before returning, and
  connector shutdown currently releases the client by dropping it.

## Testing

Run the Fluss sink unit tests from the repository root:

```bash
cargo test -p iggy_connector_fluss_sink
```
