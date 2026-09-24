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
| `starting_offset` | no | `earliest` | `earliest`, `latest` (each bucket's tail, resolved at startup), or an explicit non-negative offset. Applies only to buckets absent from the persisted state. |
| `columns` | no | all columns | Column projection pushed down to the server. |
| `poll_interval` | no | `1s` | Delay before each poll. |
| `poll_timeout` | no | `5s` | How long a single server poll waits for records. |
| `batch_size` | no | client default | Maximum records returned per poll (`scanner.log.max-poll-records`). Must be greater than 0. |
| `payload_format` | no | `json` | Only `json` is accepted. See [Limitations](#limitations). |
| `include_metadata` | no | `false` | Adds `_fluss_bucket`, `_fluss_offset` and `_fluss_timestamp` to each JSON object. |
| `sasl_username` | no | | Enables SASL/PLAIN together with `sasl_password`. Set both or neither. |
| `sasl_password` | no | | Stored as a secret and redacted from logs and the `/stats` endpoint. |
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

Every message carries an `id` derived from its bucket and offset, so a consumer can spot a record replayed after an at-least-once redelivery (the Apache Iggy server does not deduplicate on it), and an `origin_timestamp` taken from the Fluss record timestamp.

## Limitations

These are limits of this connector, not of the `fluss-rs` client.

- **Primary-key tables are not supported yet.** `fluss-rs` 1.0 reads their changelog, but mapping its insert, update and delete records onto messages is left for a follow-up, so `table_type = "primary_key"` is rejected at startup.
- **`payload_format = "arrow_ipc"` is not implemented yet.** The client does expose an Arrow `RecordBatch` scanner, but it uses a different offset-tracking path, so it is left for a follow-up.
- **Partitioned tables are not supported.** They are detected and rejected at startup.

## Build and test

```bash
cargo build --release -p iggy_connector_fluss_source
cargo test -p iggy_connector_fluss_source
```
