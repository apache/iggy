# ClickHouse Sink Connector

The ClickHouse sink connector consumes messages from Iggy topics and stores them in ClickHouse databases. Supports multiple insert formats and flexible payload storage options optimized for high-performance analytics.

## Features

- **Multiple Insert Formats**: JSON (JSONEachRow) and RowBinary for different performance needs
- **Flexible Payload Storage**: Store structured JSON data or raw payloads as strings
- **Field Mappings**: Map specific JSON fields to ClickHouse columns
- **Metadata Storage**: Optionally store Iggy message metadata (offset, timestamp, topic, etc.)
- **Batch Processing**: Configurable batching for optimal throughput
- **Authentication Support**: None, username/password credentials, or JWT token
- **TLS/mTLS Support**: Secure connections with optional client certificates
- **Compression**: Optional LZ4 compression for network efficiency
- **Automatic Retries**: Exponential backoff for transient errors

## Configuration

```toml
[[streams]]
stream = "user_events"
topics = ["users", "orders"]
schema = "json"
batch_length = 100
poll_interval = "5ms"
consumer_group = "clickhouse_sink"

[plugin_config]
url = "http://localhost:8123"
database = "default"
table = "iggy_messages"
insert_type = "json"
auth_type = "credential"
username = "default"
password = "password"
compression_enabled = true
max_batch_size = 1000
include_metadata = true
```

## Configuration Options

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `url` | string | required | ClickHouse HTTP endpoint (e.g., `http://localhost:8123`) |
| `database` | string | `default` | Target database name |
| `table` | string | required | Target table name |
| `insert_type` | string | `json` | Insert format: `json` or `rowbinary` |
| `auth_type` | string | `none` | Authentication type: `none`, `credential`, or `jwt` |
| `username` | string | - | Username for credential authentication |
| `password` | string | - | Password for credential authentication |
| `jwt_token` | string | - | JWT token for JWT authentication |
| `role` | array | - | Optional roles for ClickHouse Cloud |
| `compression_enabled` | bool | `true` | Enable LZ4 compression |
| `tls_enabled` | bool | `false` | Enable TLS/HTTPS |
| `tls_root_ca_cert` | string | - | Path to CA certificate for server verification |
| `tls_client_cert` | string | - | Path to client certificate for mTLS |
| `tls_client_key` | string | - | Path to client private key for mTLS |
| `max_batch_size` | u32 | `1000` | Maximum messages per batch |
| `chunk_size` | usize | `1048576` | Internal buffer size in bytes (1MB default, JSON format only) |
| `retry` | bool | `true` | Enable automatic retries |
| `max_retry` | u32 | `3` | Maximum retry attempts |
| `base_delay` | u64 | `500` | Base delay in milliseconds between retries |
| `include_metadata` | bool | `false` | Include Iggy metadata columns |
| `field_mappings` | map | - | Map JSON fields to ClickHouse columns |
| `payload_data_type` | string | - | ClickHouse type for payload column (JSON format only) |
| `verbose_logging` | bool | `false` | Log at info level instead of debug |

## Insert Formats

The `insert_type` option determines how data is inserted into ClickHouse:

### JSON (JSONEachRow)

Uses ClickHouse's JSONEachRow format. Flexible and supports field mappings. Best for structured JSON data.

```toml
[plugin_config]
insert_type = "json"
```

**Advantages:**

- Automatic field mapping from JSON to columns
- Supports partial column insertion via `field_mappings`
- Human-readable format for debugging
- Dynamic schema flexibility

**Use when:**

- Messages are JSON objects
- Need to map specific fields to columns
- Schema may evolve over time

### RowBinary

Uses ClickHouse's RowBinary format. More efficient but stores entire payload as a string.

```toml
[plugin_config]
insert_type = "rowbinary"
```

**Advantages:**

- Higher throughput for large payloads
- Lower CPU overhead
- Compact binary format

**Use when:**

- Maximum performance is required
- Messages are binary, protobuf, or arbitrary bytes
- Don't need column-level access to payload fields

## Table Schema

### JSON Format without Metadata

```sql
CREATE TABLE iggy_messages (
    id        UInt64,
    name      String,
    count     UInt32,
    amount    Float64,
    active    Bool,
    timestamp Int64
) ENGINE = MergeTree() ORDER BY id;
```

### JSON Format with Metadata

```sql
CREATE TABLE iggy_messages (
    id                     UInt64,
    name                   String,
    count                  UInt32,
    amount                 Float64,
    active                 Bool,
    timestamp              Int64,
    iggy_stream            String,
    iggy_topic             String,
    iggy_partition_id      UInt32,
    iggy_id                String,
    iggy_offset            UInt64,
    iggy_checksum          UInt64,
    iggy_timestamp         UInt64,
    iggy_origin_timestamp  UInt64
) ENGINE = MergeTree() ORDER BY iggy_offset;
```

### RowBinary Format without Metadata

```sql
CREATE TABLE iggy_messages (
    payload String
) ENGINE = MergeTree() ORDER BY tuple();
```

### RowBinary Format with Metadata

```sql
CREATE TABLE iggy_messages (
    iggy_stream            String,
    iggy_topic             String,
    iggy_partition_id      UInt32,
    iggy_id                String,
    iggy_offset            UInt64,
    iggy_checksum          UInt64,
    iggy_timestamp         UInt64,
    iggy_origin_timestamp  UInt64,
    payload                String
) ENGINE = MergeTree() ORDER BY iggy_offset;
```

## Field Mappings

When using `insert_type = "json"`, you can map specific JSON fields to ClickHouse columns:

```toml
[plugin_config]
insert_type = "json"
table = "user_data"

[plugin_config.field_mappings]
id = "user_id"
name = "user_name"
amount = "total_amount"
```

This maps:

- JSON field `id` → ClickHouse column `user_id`
- JSON field `name` → ClickHouse column `user_name`
- JSON field `amount` → ClickHouse column `total_amount`

Corresponding table schema:

```sql
CREATE TABLE user_data (
    user_id      UInt64,
    user_name    String,
    total_amount Float64
) ENGINE = MergeTree() ORDER BY user_id;
```

## Authentication

### No Authentication

```toml
[plugin_config]
auth_type = "none"
```

### Username/Password Credentials

```toml
[plugin_config]
auth_type = "credential"
username = "default"
password = "secure_password"
```

**Note:** Both `username` and `password` must be set when using credential authentication.

### JWT Token

```toml
[plugin_config]
auth_type = "jwt"
jwt_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
```

**Caveat:** Do not mix credential and JWT authentication. Setting `jwt_token` while `username` or `password` is set will cause an error.

### ClickHouse Cloud with Roles

```toml
[plugin_config]
auth_type = "credential"
username = "cloud_user"
password = "cloud_password"
role = ["role1", "role2"]
```

## TLS/mTLS Configuration

### TLS (Server Verification)

```toml
[plugin_config]
url = "https://clickhouse.example.com:8443"
tls_enabled = true
tls_root_ca_cert = "/path/to/ca-cert.pem"
```

### mTLS (Client Certificates)

```toml
[plugin_config]
url = "https://clickhouse.example.com:8443"
tls_enabled = true
tls_root_ca_cert = "/path/to/ca-cert.pem"
tls_client_cert = "/path/to/client-cert.pem"
tls_client_key = "/path/to/client-key.pem"
```

**Important:** The connector uses `rustls-tls` with webpki bundle. For ClickHouse Cloud or public CAs, no custom CA certificate is needed. For self-signed certificates, provide `tls_root_ca_cert`.

## Performance

### Recommended Indexes

```sql
-- For metadata-enabled tables
CREATE INDEX idx_stream ON iggy_messages (iggy_stream);
CREATE INDEX idx_topic ON iggy_messages (iggy_topic);
CREATE INDEX idx_offset ON iggy_messages (iggy_offset);

-- For JSON columns (if using payload_data_type)
CREATE INDEX idx_payload ON iggy_messages (payload) TYPE bloom_filter GRANULARITY 1;
```

### Tuning Tips

- **Increase `max_batch_size`** for higher throughput (larger batches = fewer HTTP requests)
- **Enable `compression_enabled`** to reduce network bandwidth (enabled by default)
- **Use `insert_type = "rowbinary"`** for maximum performance with large payloads
- **Set `chunk_size` ≥ `max_batch_size * avg_message_size`** for JSON format to avoid runtime buffer extensions that slow down writes
- **For RowBinary format, keep batches under ~254KB** due to hardcoded chunk size limitation (see caveat below)
- **Use `poll_interval`** to control how often the sink checks for new messages
- **Choose appropriate `ORDER BY`** keys for query patterns (offset for time-series, id for lookups)

## Example Configs

### JSON Messages with Field Mappings

```toml
[[streams]]
stream = "events"
topics = ["user_events"]
schema = "json"
batch_length = 500
poll_interval = "10ms"
consumer_group = "ch_sink"

[plugin_config]
url = "http://localhost:8123"
table = "events"
insert_type = "json"
auth_type = "credential"
username = "default"
password = "password"
max_batch_size = 1000
include_metadata = false

[plugin_config.field_mappings]
user_id = "id"
event_name = "name"
event_time = "timestamp"
```

### RowBinary for High Throughput

```toml
[[streams]]
stream = "logs"
topics = ["app_logs", "system_logs"]
schema = "raw"
batch_length = 1000
poll_interval = "5ms"
consumer_group = "ch_sink"

[plugin_config]
url = "http://localhost:8123"
database = "logs"
table = "raw_logs"
insert_type = "rowbinary"
auth_type = "none"
compression_enabled = true
max_batch_size = 5000
include_metadata = true
```

### ClickHouse Cloud with TLS

```toml
[[streams]]
stream = "analytics"
topics = ["events"]
schema = "json"
batch_length = 100
poll_interval = "10ms"
consumer_group = "ch_cloud_sink"

[plugin_config]
url = "https://your-instance.clickhouse.cloud:8443"
database = "production"
table = "events"
insert_type = "json"
auth_type = "credential"
username = "cloud_user"
password = "cloud_password"
tls_enabled = true
compression_enabled = true
max_batch_size = 1000
include_metadata = true
```

## Reliability Features

### Automatic Retries

The connector automatically retries transient errors with exponential backoff. The retry delay is calculated as `base_delay * 2^attempt` milliseconds. Configure with:

- `retry` (default: `true`) - Enable/disable retries
- `max_retry` (default: `3`) - Maximum retry attempts; the connector will fail to open at startup if this exceeds `10`
- `base_delay` (default: `500ms`) - Base delay between retries; the computed delay is capped at 15 minutes to prevent unbounded waits

Non-transient errors fail immediately without retrying.

### Compression

LZ4 compression is enabled by default (`compression_enabled = true`), reducing network bandwidth by 60-90% for text data. Disable for local deployments if network is not a bottleneck.

### Error Handling

The sink tracks and logs:

- Total messages processed
- Insert attempt failures (retryable errors)
- Insert batch failures (permanent errors)

Check logs for detailed error messages and performance metrics on connector shutdown.

## Caveats

### Chunk Size and Batch Size Relationship

The `chunk_size` parameter controls the internal buffer capacity for the ClickHouse client's insert operations. Understanding its interaction with `max_batch_size` is important for optimal performance and data consistency:

#### JSON Format (JSONEachRow)

For JSON insert format, `chunk_size` is configurable (default: 1MB) and sets the initial buffer capacity.

**Performance Impact:** If the accumulated batch data exceeds `chunk_size` before reaching `max_batch_size`, the internal buffer will be **extended at runtime**. This dynamic reallocation slows down writes due to memory copying overhead.

**Best Practice:** Set `chunk_size ≥ max_batch_size * average_message_size` to avoid runtime buffer extensions and ensure optimal write performance.

**Example:**

```toml
[plugin_config]
max_batch_size = 1000          # 1000 messages per batch
chunk_size = 2097152           # 2MB buffer (for ~2KB avg message size)
```

**Calculation:** If average message size is 2KB, set `chunk_size ≥ 1000 * 2KB = 2MB` to prevent buffer extensions.

#### RowBinary Format

For RowBinary insert format, the chunk size is **hardcoded to 256KB** by the ClickHouse Rust client and cannot be configured. Data is auto-flushed at approximately **254KB**.

**Limitation:** If `max_batch_size * average_message_size > 254KB`, the batch will be split across multiple inserts:

- **Batch 1** (auto-flush at ~254KB): Succeeds ✓
- **Batch 2** (remainder at end of `max_batch_size`): Fails ✗
- **Retry**: Entire batch is retried, causing **duplicates** if retry succeeds

**Workaround:** Keep batch sizes under 254KB until the ClickHouse Rust client resolves this limitation (see [clickhouse-rs#389](https://github.com/ClickHouse/clickhouse-rs/issues/389)).

**Example for RowBinary:**

```toml
[plugin_config]
insert_type = "rowbinary"
max_batch_size = 100           # Keep total batch < 254KB
# chunk_size is ignored for RowBinary (hardcoded to 256KB)
```

**Calculation:** If average message size is 2KB, set `max_batch_size ≤ 120` to stay under 254KB (120 * 2KB = 240KB).
