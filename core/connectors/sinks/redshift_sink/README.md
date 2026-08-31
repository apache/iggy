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
aws_iam_role ="arn:aws:iam::0123456789012:role/iggyRole"
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
| `include_metadata` | no | `true` | Stores stream/topic/partition/offset/timestamp/schema fields alongside the payload. |
| `include_checksum` | no | `false` | Stores the Iggy message checksum. |
| `include_origin_timestamp` | no | `false` | Stores the original Iggy origin timestamp. |
| `payload_format` | no | `varbyte` | Encoding used for the payload column in the Parquet file. See **Payload Format** below. |
| `verbose_logging` | no | `false` | Enables verbose logging for debugging purposes. |
| `max_retries` | no | `3` | Maximum number of retries for failed `COPY` operations. `0` disables retries (only one attempt will be made) |
| `retry_delay` | no | `1s` | Delay in seconds between retry attempts. |
| `aws_iam_role` | yes | — | AWS IAM role with S3-Redshift write privileges used for S3 staging. |
| `aws_access_key_id` | no | — | AWS access key used for S3 staging. |
| `aws_secret_access_key` | no | — | AWS secret key used for S3 staging. |
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

## Test Suite Setup

Six queries validate connector behavior end-to-end. Each is shown in its **production (Redshift)** form; where the pgwire-postgres test harness diverges, the substitution is noted inline.

### 1. Connection check

```sql
SELECT 1
```

Confirms warehouse connectivity. No dialect differences.

## 2. Staging/target table creation

```sql
CREATE TABLE IF NOT EXISTS {table_name} (
    id VARCHAR(40),
    iggy_offset VARCHAR(20),
    iggy_timestamp VARCHAR(20),
    iggy_stream TEXT,
    iggy_topic TEXT,
    iggy_partition_id BIGINT,
    iggy_checksum VARCHAR,
    iggy_origin_timestamp VARCHAR(20),
    payload {payload_type},
    created_at TIMESTAMPTZ DEFAULT GETDATE()
);
```

- Staging table name = `staging_` + `{table_name}`.
- **pgwire test substitution:** `GETDATE()` → `NOW()`.
- **pgwire test substitution:** `VARBYTE(16777216)` → `BYTEA`. This affects the `column` when we have `VARBYTE` as the type.
- **pgwire test substitution:** `VARCHAR(MAX)` → `VARCHAR(65535)`. This affects the `column` when we have `VARCHAR` as the type.
- `iggy_offset`, `iggy_timestamp`, and `iggy_origin_timestamp` are u64 values in Iggy but are stored as `VARCHAR` rather than `BIGINT`. `BIGINT` is signed and tops out below `u64::MAX`, so a `VARCHAR` column sidesteps the overflow risk on the upper half of the u64 range without pulling in `DECIMAL`'s added precision/rounding handling.
- `iggy_partition_id` is u32 in Iggy but is stored as `BIGINT` rather than `INTEGER`. `INTEGER` is signed and tops out below `u32::MAX`, so a `BIGINT` column sidesteps the overflow risk.

## 3. Schema drift check

**Redshift:**

```sql
SELECT "column", type
FROM pg_table_def
WHERE tablename = 'target_table';
```

**pgwire test equivalent:**

```sql
SELECT column_name, type
FROM information_schema.columns
WHERE table_name = 'target_table';
```

Substitutions: `pg_table_def` → `information_schema.columns`, `"column"` → `column_name`, `udt_name`* → `type`.

## 4. S3 → staging load

**Redshift:**

```sql
COPY "staging_iggy_messages" (id, iggy_offset, iggy_timestamp, iggy_stream, iggy_topic, iggy_partition_id, iggy_checksum, iggy_origin_timestamp, payload, created_at) FROM 's3://iggystaging/iggy/messages/019ff3d5-a06f-7921-b084-0c67cabfefed.parquet'
CREDENTIALS 'aws_iam_role=arn:aws:iam::0123456789012:role/iggyRole'
FORMAT AS PARQUET;
```

**pgwire test equivalent:**

```sql
COPY {staging_table} ({columns})
FROM STDIN BINARY
```

The `s3_path` is parsed and used to fetch the object from the MinIO instance backing the mock container, with access key and secret key supplied to the container via environment variables rather than an IAM role. Instead of Redshift pulling directly from S3, the connector reads the object itself and streams it into the mock over `COPY ... FROM STDIN BINARY`, so the `CREDENTIALS`, `FORMAT AS PARQUET`, and `REGION` clauses have no equivalent here.

## 5. Staging → target insert (idempotent upsert)

```sql
INSERT INTO "iggy_messages" (id, iggy_offset, iggy_timestamp, iggy_stream, iggy_topic, iggy_partition_id, iggy_checksum, iggy_origin_timestamp, payload, created_at)
SELECT s.id, s.iggy_offset, s.iggy_timestamp, s.iggy_stream, s.iggy_topic, s.iggy_partition_id, s.iggy_checksum, s.iggy_origin_timestamp, s.payload, s.created_at
FROM (SELECT sm.*, ROW_NUMBER() OVER (PARTITION BY sm.id ORDER BY sm.created_at) AS rn FROM "staging_iggy_messages" sm) s
WHERE s.rn = 1
AND NOT EXISTS (SELECT 1 FROM "iggy_messages" t WHERE t.id = s.id);
```

Uniqueness enforced on `id` — no update branch by design. No dialect differences.

## 6. Staging table reset

```sql
TRUNCATE staging_target_table;
```

Clears staging ahead of the next load cycle. No dialect differences.
