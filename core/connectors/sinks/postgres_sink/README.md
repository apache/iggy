# PostgreSQL Sink Connector

The PostgreSQL sink connector consumes messages from Iggy topics and stores them in PostgreSQL databases. Supports multiple payload storage formats including BYTEA, JSONB, and TEXT.

## Features

- **Flexible Payload Storage**: Store payloads as BYTEA (raw bytes), JSONB, or TEXT
- **Automatic Table Creation**: Optionally create the target table on startup
- **Metadata Storage**: Store Iggy message metadata (offset, timestamp, topic, etc.)
- **Batch Processing**: Insert messages in configurable batches
- **Connection Pooling**: Efficient database connection management
- **Any Payload Type**: Works with JSON, text, binary, protobuf, or any byte format

## Configuration

```toml
[[streams]]
stream = "user_events"
topics = ["users", "orders"]
schema = "json"
batch_length = 100
poll_interval = "5ms"
consumer_group = "postgres_sink"

[plugin_config]
connection_string = "postgresql://user:pass@localhost:5432/database"
target_table = "iggy_messages"
batch_size = 100
max_connections = 10
auto_create_table = true
include_metadata = true
include_checksum = true
include_origin_timestamp = true
payload_format = "bytea"
```

## Configuration Options

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `connection_string` | string | required | PostgreSQL connection string |
| `target_table` | string | required | Target table name |
| `batch_size` | u32 | `100` | Messages per insert batch |
| `max_connections` | u32 | `10` | Max database connections |
| `auto_create_table` | bool | `false` | Create table if not exists |
| `include_metadata` | bool | `true` | Include Iggy metadata columns |
| `include_checksum` | bool | `true` | Include message checksum |
| `include_origin_timestamp` | bool | `true` | Include original timestamp |
| `payload_format` | string | `bytea` | Payload column type: `bytea`, `json`, or `text` |
| `verbose_logging` | bool | `false` | Log at info level instead of debug |
| `max_retries` | u32 | `3` | Max retry attempts for transient errors |
| `retry_delay` | string | `1s` | Base delay between retries (e.g., `500ms`, `2s`) |

## Payload Format

The `payload_format` option determines how the payload is stored in PostgreSQL:

| Format | Column Type | Description |
| ------ | ----------- | ----------- |
| `bytea` | `BYTEA` | Raw bytes (default). Preserves exact binary content. |
| `json` / `jsonb` | `JSONB` | Native JSON. Enables JSON queries and indexing. Payload must be valid JSON. |
| `text` | `TEXT` | UTF-8 text. Payload must be valid UTF-8. |

### BYTEA (Default)

Stores the exact bytes from the Iggy message. Use for binary data, protobuf, or when you want to preserve the original format.

```toml
[plugin_config]
payload_format = "bytea"
```

### JSONB

Stores payload as native PostgreSQL JSONB. Enables efficient JSON queries and GIN indexing. The incoming message payload must be valid JSON.

```toml
[plugin_config]
payload_format = "json"
```

Query example:

```sql
SELECT id, payload->>'user_id' as user_id
FROM iggy_messages
WHERE payload->>'status' = 'active';

CREATE INDEX idx_payload_gin ON iggy_messages USING GIN (payload);
```

### TEXT

Stores payload as UTF-8 text. Use for plain text messages or logs.

```toml
[plugin_config]
payload_format = "text"
```

Query example:

```sql
SELECT id, payload FROM iggy_messages WHERE payload LIKE '%error%';
```

## Table Schema

When `auto_create_table` is enabled, the table is created with the appropriate payload column type:

```sql
CREATE TABLE iggy_messages (
    id DECIMAL(39, 0) PRIMARY KEY,
    iggy_offset BIGINT,
    iggy_timestamp TIMESTAMP WITH TIME ZONE,
    iggy_stream TEXT,
    iggy_topic TEXT,
    iggy_partition_id INTEGER,
    iggy_checksum BIGINT,
    iggy_origin_timestamp TIMESTAMP WITH TIME ZONE,
    payload BYTEA,  -- or JSONB or TEXT based on payload_format
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

## Performance

### Recommended Indexes

```sql
CREATE INDEX idx_iggy_messages_stream ON iggy_messages (iggy_stream);
CREATE INDEX idx_iggy_messages_topic ON iggy_messages (iggy_topic);
CREATE INDEX idx_iggy_messages_offset ON iggy_messages (iggy_offset);
CREATE INDEX idx_iggy_messages_created_at ON iggy_messages (created_at);

-- For JSONB payload_format
CREATE INDEX idx_payload_gin ON iggy_messages USING GIN (payload);
```

### Tuning Tips

- Increase `batch_size` for higher throughput (larger batches = fewer round trips)
- Adjust `max_connections` based on PostgreSQL's `max_connections` setting
- Use `poll_interval` to control how often the sink checks for new messages
- Use `payload_format = "json"` for JSON data to enable native querying

## Example Configs

### JSON Messages with JSONB Storage

```toml
[[streams]]
stream = "events"
topics = ["user_events"]
schema = "json"
batch_length = 100
poll_interval = "10ms"
consumer_group = "pg_sink"

[plugin_config]
connection_string = "postgresql://user:pass@localhost:5432/analytics"
target_table = "events"
auto_create_table = true
batch_size = 500
payload_format = "json"
```

### Binary/Raw Messages

```toml
[[streams]]
stream = "binary_data"
topics = ["images", "files"]
schema = "raw"
batch_length = 50
poll_interval = "100ms"
consumer_group = "pg_sink"

[plugin_config]
connection_string = "postgresql://user:pass@localhost:5432/storage"
target_table = "binary_messages"
auto_create_table = true
batch_size = 100
payload_format = "bytea"
```

### Text Logs

```toml
[[streams]]
stream = "logs"
topics = ["app_logs"]
schema = "text"
batch_length = 1000
poll_interval = "5ms"
consumer_group = "pg_sink"

[plugin_config]
connection_string = "postgresql://user:pass@localhost:5432/logs"
target_table = "log_messages"
auto_create_table = true
include_checksum = false
include_origin_timestamp = false
batch_size = 1000
payload_format = "text"
```

## Reliability Features

### Automatic Retries

The connector automatically retries transient database errors (connection issues, deadlocks, serialization failures) with exponential backoff. Configure with `max_retries` (default: 3) and `retry_delay` (default: `1s`). The actual delay is `retry_delay * attempt_number`. Non-transient errors fail immediately.

### Connection Pool Management

The connection pool is properly closed when the connector shuts down, ensuring clean resource cleanup.

## Usage with Source Connector

The sink can work with the source connector for pass-through scenarios:

1. **Sink** with `payload_format = "bytea"` stores messages as raw bytes
2. **Source** with `payload_column = "payload"` and `payload_format = "bytea"` reads them back

For JSON data:

1. **Sink** with `payload_format = "json"` stores as JSONB
2. **Source** with `payload_column = "data"` and `payload_format = "json_direct"` reads JSONB directly
