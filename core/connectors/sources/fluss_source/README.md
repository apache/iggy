# Apache Fluss source connector

Reads rows from an [Apache Fluss](https://fluss.apache.org/) log table and publishes them to an Apache Iggy stream as JSON.

Each Fluss row becomes one Apache Iggy message. Offsets are tracked per bucket and persisted through the runtime state API, so a restart resumes where the previous run stopped. Offsets only advance once the runtime acknowledges a delivered batch; a rejected batch rewinds the scanner to the last acknowledged offsets and is read again. The start position is persisted with the first poll even when it returns no rows, so a restart does not resolve `latest` again past rows written in between.

## Configuration

| Field | Required | Default | Description |
| ----- | -------- | ------- | ----------- |
| `bootstrap_servers` | yes | | Coordinator server address, for example `localhost:9123`. |
| `database` | yes | | Apache Fluss database name. |
| `table` | yes | | Apache Fluss table name. |
| `table_type` | no | `log` | Only `log` is accepted. See [Limitations](#limitations). |
| `starting_offset` | no | `earliest` | `earliest`, `latest` (each bucket's tail, resolved at startup), or an explicit non-negative offset. Applies only to buckets absent from the persisted state. An explicit offset is used as is for every such bucket, so it suits a table whose bucket offsets you know, such as one with a single bucket. |
| `columns` | no | all columns | Column projection pushed down to the server. Must name at least one column, and no column twice. |
| `poll_interval` | no | `1s` | Delay before each poll. |
| `poll_timeout` | no | `5s` | How long a single server poll waits for records. It and `poll_interval` cannot both be zero. |
| `batch_size` | no | `500` | Maximum records returned per poll (`scanner.log.max-poll-records`). Must be greater than 0. |
| `payload_format` | no | `json` | Only `json` is accepted. See [Limitations](#limitations). |
| `include_metadata` | no | `false` | Adds `_fluss_bucket`, `_fluss_offset` and `_fluss_timestamp` to each JSON object. While it is on, a column whose name starts with `_fluss_` is rejected at startup, since it would be overwritten. |
| `sasl_username` | no | | Enables SASL/PLAIN together with `sasl_password`. Set both or neither. |
| `sasl_password` | no | | The connector never logs it and `/stats` never shows it, but the runtime does not redact it: it logs the raw plugin config at trace level and returns it from its config API, such as `GET /sources/{key}/configs/plugin`. |
| `verbose_logging` | no | `false` | Logs per-batch counts at info instead of debug. |

## Example

```toml
type = "source"
key = "fluss"
enabled = true
version = 0
name = "Apache Fluss source"
path = "libiggy_connector_fluss_source"

[[streams]]
stream = "fluss_events"
topic = "events"
schema = "json"
batch_length = 100

[plugin_config]
bootstrap_servers = "localhost:9123"
database = "mydb"
table = "events"
poll_interval = "1s"
batch_size = 500
```

Bring up a local cluster with the [official Docker compose recipe](https://fluss.apache.org/docs/install-deploy/deploying-with-docker/) (ZooKeeper, one coordinator server, one tablet server), create the stream and topic with the Apache Iggy CLI, then start the connectors runtime.

## Type mapping

| Apache Fluss type | JSON |
| ----------------- | ---- |
| `BOOLEAN` | boolean |
| `TINYINT`, `SMALLINT`, `INT`, `BIGINT` | number |
| `FLOAT`, `DOUBLE` | number, `null` when not finite |
| `CHAR`, `STRING` | string |
| `DECIMAL` | string, to keep the full precision |
| `DATE` | string, `2024-02-29` |
| `TIME` | string, `12:34:56.789` |
| `TIMESTAMP` | string without a timezone, `2023-11-14 22:13:20.123456` |
| `TIMESTAMP_LTZ` | string in UTC, RFC 3339, `2023-11-14T22:13:20.123456+00:00` |
| `BINARY`, `BYTES` | base64 string |
| `ARRAY`, `MAP`, `ROW` | not supported, rejected at startup |

Temporal values keep every fractional digit the column holds, so the default `TIMESTAMP(6)` keeps its microseconds. They are formatted the same way the PostgreSQL source formats them: `TIMESTAMP` carries no timezone and is written without one, while `TIMESTAMP_LTZ` is an instant and is written in UTC.

Rows are decoded with the table schema read when the connector starts. A row written before a column was added carries `null` for it, and a column added after startup stays out of the output until the connector restarts.

Every message carries an `id` derived from its bucket and offset, so a consumer can spot a record replayed after an at-least-once redelivery (the Apache Iggy server does not deduplicate on it).

The connector also hands the runtime the Fluss record timestamp as `origin_timestamp`, but the runtime does not pass it on, so Apache Iggy stamps each message with its own time. Turn on `include_metadata` to keep the record timestamp in the payload as `_fluss_timestamp`.

## Limitations

These are limits of this connector, not of the `fluss-rs` client.

- **Primary-key tables are not supported yet.** `fluss-rs` 1.0 reads their changelog, but mapping its insert, update and delete records onto messages is left for a follow-up, so `table_type = "primary_key"` is rejected at startup.
- **`payload_format = "arrow_ipc"` is not implemented yet.** The client does expose an Arrow `RecordBatch` scanner, but it uses a different offset-tracking path, so it is left for a follow-up.
- **Partitioned tables are not supported.** They are detected and rejected at startup.
- **Rows that cannot be converted are skipped.** A value the JSON mapping cannot hold, such as a date beyond what `chrono` represents, fails the same way on every read. Such a row is dropped and logged at error level with its bucket and offset, the offsets move past it, and the number of skipped rows is logged when the connector closes.
- **A crash right after starting from `latest` can miss rows.** The resolved start offsets stay in memory until the first batch is acknowledged, which normally takes one poll. If the connector crashes in that window, the next start resolves `latest` again and misses the rows written in between.
- **An offset outside a bucket's range is not reset.** If a configured offset, or one restored from state after retention removed it, falls outside what the bucket still holds, polls keep failing until the offset is changed. The connector does not move to the earliest or latest offset on its own.

## Build and test

```bash
cargo build --release -p iggy_connector_fluss_source
cargo test -p iggy_connector_fluss_source
```
