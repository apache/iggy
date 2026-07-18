# Redshift Sink Connector

Writes Apache Iggy stream messages into Amazon Redshift via S3-staged Parquet
files and a `COPY` load.

Each connector batch is serialized to a Parquet file and uploaded to the
configured S3 bucket/prefix, then loaded into the target Redshift table with a
`COPY` statement. This makes S3 a staging area rather than a destination in
its own right — Redshift is the system of record for the data.

Persistent load failures are at-most-once from the runtime's perspective:
messages may already be committed in Iggy before this connector exhausts its
write attempts, so failed loads are logged but not redelivered.

## Configuration

```toml
type = "sink"
key = "redshift"
enabled = true
version = 0
name = "Redshift sink"
path = "../../target/release/libiggy_connector_redshift_sink"
verbose = false

[[streams]]
stream = "user_events"
topics = ["users", "orders"]
schema = "json"
batch_length = 100
poll_interval = "5ms"
consumer_group = "redshift_sink"

[plugin_config]
connection_string = "postgresql://user:pass@localhost:5439/database"
target_table = "iggy_messages"
batch_size = 100
max_connections = 10
include_metadata = true
include_checksum = true
include_origin_timestamp = true
payload_format = "varbyte"
aws_access_key_id = "admin"
aws_secret_access_key = "password"
s3_bucket = "iggystaging"
s3_prefix = "iggy/messages"
s3_endpoint = "http://localhost:9000"
aws_region = "us-east-1"
archive = true
```

### Plugin Fields

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `connection_string` | yes | — | Postgres-wire connection string used to reach the Redshift cluster and issue the `COPY` command. |
| `target_table` | yes | — | Destination Redshift table that batches are copied into. |
| `batch_size` | no | `100` | Number of messages buffered per Parquet file / `COPY` operation. |
| `max_connections` | no | `5` | Size of the connection pool used against Redshift. |
| `include_metadata` | no | `false` | Stores stream/topic/partition/offset/timestamp/schema fields alongside the payload. |
| `include_checksum` | no | `false` | Stores the Iggy message checksum. |
| `include_origin_timestamp` | no | `false` | Stores the original Iggy origin timestamp. |
| `payload_format` | no | `varbyte` | Encoding used for the payload column in the Parquet file. See **Payload Format** below. |
| `verbose_logging` | no | `false` | Enables verbose logging for debugging purposes. |
| `max_retries` | no | `3` | Maximum number of retries for failed `COPY` operations. `0` disables retries (only one attempt will be made) |
| `retry_delay` | no | `1s` | Delay in seconds between retry attempts. |
| `aws_access_key_id` | yes | — | AWS access key used for S3 staging. |
| `aws_secret_access_key` | yes | — | AWS secret key used for S3 staging. |
| `s3_bucket` | yes | — | S3 bucket that Parquet batch files are staged into before the Redshift `COPY`. |
| `s3_prefix` | yes | — | Key prefix under which staged Parquet files are written, e.g. `iggy/messages`. |
| `s3_endpoint` | no | — | Override endpoint for S3-compatible stores (e.g. MinIO). Omit for AWS S3 itself. |
| `aws_region` | yes | — | AWS region for the S3 bucket. |
| `archive` | no | `false` | See **Archiving Staged Files** below. |

## Staging via S3

Redshift's `COPY` command loads from files, not from a live stream, so every
batch is first written out as a Parquet file and uploaded to
`s3://<s3_bucket>/<s3_prefix>/...` before the `COPY` into `target_table` runs.
S3 is purely a staging area in this flow — it is not queried directly by
consumers of the data, and its cost is the price of getting bulk data into
Redshift efficiently rather than row-by-row.

## Archiving Staged Files

The `archive` field controls what happens to a batch's Parquet file **after**
it has been successfully loaded into Redshift:

- `archive = true` — the file is kept, moved under an `archive` prefix
  (i.e. `s3://<s3_bucket>/archive/...`) instead of being deleted.
  Useful for replay, auditing, or downstream batch jobs that read Parquet
  directly.
- `archive = false` — the file is deleted from S3 once the `COPY` succeeds,
  since Redshift itself is now the source of truth for that data and the
  staged copy has no further purpose.

## Payload Format

`payload_format` controls how the payload column is written in the staged
Parquet file, which in turn determines its type once loaded into Redshift:

- Parquet has no dedicated JSON logical type, so a `payload_format = "json"`
  payload is written as a Parquet `VARCHAR` (string), not a structured type.
- As a result, the column lands in Redshift as `VARCHAR`, not `SUPER`.
- To query the payload as structured data downstream, use Redshift's
  `JSON_PARSE()` (or equivalent JSON functions) on the `VARCHAR` column at
  query time rather than expecting a native `SUPER` column out of the box.

## Stored Shape

With metadata enabled, records contain:

- `id`: original Iggy message id as numeric
- `iggy_stream`, `iggy_topic`, `iggy_partition_id`, `iggy_offset`
- `iggy_timestamp`, `iggy_origin_timestamp`, `iggy_checksum`,
- `payload`: encoded per `payload_format` (see above)

The `messages_processed` counter reports valid records submitted to Redshift
via `COPY`.
