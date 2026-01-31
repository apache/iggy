# Apache Iggy - Redshift Sink Connector

A sink connector that loads data from Iggy streams into Amazon Redshift using the S3 staging method. This is the recommended approach for high-volume data loading into Redshift.

## Overview

The Redshift Sink Connector:

1. **Buffers** incoming messages into batches
2. **Uploads** batches as CSV files to S3
3. **Executes** Redshift COPY command to load data from S3
4. **Cleans up** staged S3 files after successful load

This approach leverages Redshift's massively parallel processing (MPP) architecture for efficient bulk loading.

## Prerequisites

- Amazon Redshift cluster with network access
- S3 bucket for staging files
- AWS credentials with appropriate permissions:
  - S3: `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject` on the staging bucket
  - Redshift: `COPY` permission on the target table

## Configuration

Create a connector configuration file (e.g., `redshift.toml`):

```toml
type = "sink"
key = "redshift"
enabled = true
version = 0
name = "Redshift Sink"
path = "target/release/libiggy_connector_redshift_sink"
plugin_config_format = "toml"

[[streams]]
stream = "events"
topics = ["user_actions"]
schema = "json"
batch_length = 10000
poll_interval = "100ms"
consumer_group = "redshift_sink"

[plugin_config]
# Redshift connection (PostgreSQL wire protocol)
connection_string = "postgres://admin:password@my-cluster.region.redshift.amazonaws.com:5439/mydb"
target_table = "public.events"

# S3 staging configuration
s3_bucket = "my-staging-bucket"
s3_prefix = "redshift/staging/"
s3_region = "us-east-1"

# AWS authentication - use either IAM role (preferred) or access keys
iam_role = "arn:aws:iam::123456789012:role/RedshiftS3Access"

# Or use access keys instead of IAM role:
# aws_access_key_id = "AKIAIOSFODNN7EXAMPLE"
# aws_secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

# Batching settings
batch_size = 10000
flush_interval_ms = 30000

# CSV format options
csv_delimiter = ","
csv_quote = "\""

# COPY command options
max_errors = 10
# compression = "gzip"

# Cleanup and reliability
delete_staged_files = true
max_retries = 3
retry_delay_ms = 1000

# Database settings
max_connections = 5
auto_create_table = false

# Metadata columns (adds iggy_offset, iggy_timestamp, etc.)
include_metadata = false
```

## Configuration Reference

| Property | Type | Required | Default | Description |
| -------- | ---- | -------- | ------- | ----------- |
| `connection_string` | String | Yes | - | Redshift connection string in PostgreSQL format |
| `target_table` | String | Yes | - | Target table name (can include schema) |
| `s3_bucket` | String | Yes | - | S3 bucket for staging CSV files |
| `s3_region` | String | Yes | - | AWS region for the S3 bucket |
| `s3_prefix` | String | No | `""` | S3 key prefix for staged files |
| `iam_role` | String | No* | - | IAM role ARN for Redshift to access S3 |
| `aws_access_key_id` | String | No* | - | AWS access key ID |
| `aws_secret_access_key` | String | No* | - | AWS secret access key |
| `batch_size` | Integer | No | `10000` | Messages per batch before S3 upload |
| `flush_interval_ms` | Integer | No | `30000` | Max wait time before flushing partial batch |
| `csv_delimiter` | Char | No | `,` | CSV field delimiter |
| `csv_quote` | Char | No | `"` | CSV quote character |
| `max_errors` | Integer | No | `0` | Max errors before COPY fails |
| `compression` | String | No | `none` | Compression: `gzip`, `lzop`, `bzip2` |
| `delete_staged_files` | Boolean | No | `true` | Delete S3 files after successful COPY |
| `max_connections` | Integer | No | `5` | Max Redshift connections |
| `max_retries` | Integer | No | `3` | Max retry attempts for failures |
| `retry_delay_ms` | Integer | No | `1000` | Initial retry delay (exponential backoff) |
| `include_metadata` | Boolean | No | `false` | Include Iggy metadata columns |
| `auto_create_table` | Boolean | No | `false` | Auto-create table if not exists |

*Either `iam_role` or both `aws_access_key_id` and `aws_secret_access_key` must be provided.

## Table Schema

When `auto_create_table` is enabled, the connector creates a table with this schema:

```sql
CREATE TABLE IF NOT EXISTS <target_table> (
    id VARCHAR(40) PRIMARY KEY,
    payload VARCHAR(MAX),
    -- When include_metadata = true:
    iggy_offset BIGINT,
    iggy_timestamp TIMESTAMP,
    iggy_stream VARCHAR(256),
    iggy_topic VARCHAR(256),
    iggy_partition_id INTEGER,
    --
    created_at TIMESTAMP DEFAULT GETDATE()
);
```

For production use, pre-create your table with appropriate column types, sort keys, and distribution style.

## IAM Role Setup

For IAM role authentication (recommended), create a role with this trust policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "redshift.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

And attach a policy with S3 access:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:GetBucketLocation",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-staging-bucket",
        "arn:aws:s3:::my-staging-bucket/*"
      ]
    }
  ]
}
```

Then associate the role with your Redshift cluster.

## Performance Tuning

### Batch Size

- **Small batches** (1,000-5,000): Lower latency, more COPY operations
- **Large batches** (50,000-100,000): Higher throughput, more memory usage
- Recommended starting point: `10,000`

### Compression

Enable compression for large payloads to reduce S3 transfer time:

```toml
compression = "gzip"
```

### Parallelism

Increase `batch_length` in stream config to process more messages per poll:

```toml
[[streams]]
batch_length = 50000
```

## Error Handling

The connector implements retry logic with exponential backoff for transient failures:

- **S3 upload failures**: Retried up to `max_retries` times
- **COPY command failures**: Retried with backoff, failed rows logged
- **Cleanup failures**: Logged as warnings, do not block processing

Use `max_errors` to control COPY behavior:

- `0`: Fail on first error (strict mode)
- `N`: Allow up to N errors per COPY operation

## Monitoring

The connector logs statistics on close:

```text
Closing Redshift sink connector ID: 1. Stats: 150000 messages processed, 15 batches loaded, 0 errors
```

Monitor these metrics to track connector health.

## Limitations

- Payload must be convertible to string (JSON, text, or raw bytes)
- Table must exist unless `auto_create_table` is enabled
- Currently supports CSV format only (Parquet planned)

## License

Licensed under the Apache License, Version 2.0.
