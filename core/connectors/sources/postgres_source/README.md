# PostgreSQL Source Connector

The PostgreSQL source connector fetches data from PostgreSQL databases and streams it to Iggy topics. It supports table polling and Change Data Capture (CDC) modes with flexible payload extraction.

## Features

- **Table Polling**: Incrementally fetch data from PostgreSQL tables
- **Change Data Capture**: Monitor database changes using PostgreSQL logical replication
- **Flexible Payload Extraction**: Extract BYTEA, TEXT, or JSONB columns directly as payload
- **Custom Queries**: Use custom SQL queries with parameter substitution
- **Delete After Read**: Automatically delete rows after processing
- **Mark as Processed**: Mark rows as processed using a boolean column
- **Multiple Tables**: Monitor multiple tables simultaneously
- **Batch Processing**: Fetch data in configurable batch sizes
- **Offset Tracking**: Keep track of processed records to avoid duplicates

## Configuration

```toml
[plugin_config]
connection_string = "postgresql://user:pass@localhost:5432/database"
mode = "polling"
tables = ["users", "orders"]
poll_interval = "1s"
batch_size = 1000
tracking_column = "id"
initial_offset = "0"
max_connections = 10
snake_case_columns = false
include_metadata = true

# Payload extraction (optional)
payload_column = "payload"
payload_format = "bytea"

# Delete/mark processed (optional)
delete_after_read = false
processed_column = "is_processed"
primary_key_column = "id"

# Custom query (optional)
custom_query = "SELECT * FROM $table WHERE id > $offset ORDER BY id LIMIT $limit"

# CDC options (optional)
enable_wal_cdc = false
publication_name = "iggy_publication"
replication_slot = "iggy_slot"
capture_operations = ["INSERT", "UPDATE", "DELETE"]
cdc_backend = "builtin"
```

## Configuration Options

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `connection_string` | string | required | PostgreSQL connection string |
| `mode` | string | required | `polling` or `cdc` |
| `tables` | array | required | List of tables to monitor |
| `poll_interval` | string | `1s` | How often to poll (e.g., `1s`, `5m`) |
| `batch_size` | u32 | `1000` | Max rows per poll |
| `tracking_column` | string | `id` | Column for incremental updates |
| `initial_offset` | string | none | Starting value for tracking column |
| `max_connections` | u32 | `10` | Max database connections |
| `snake_case_columns` | bool | `false` | Convert column names to snake_case |
| `include_metadata` | bool | `true` | Wrap results with metadata |
| `payload_column` | string | none | Column to extract as payload |
| `payload_format` | string | `bytea` | Format of payload_column: `bytea`, `text`, or `json_direct` |
| `delete_after_read` | bool | `false` | Delete rows after reading |
| `processed_column` | string | none | Boolean column to mark as processed |
| `primary_key_column` | string | tracking_column | PK for delete/mark operations |
| `custom_query` | string | none | Custom SQL with parameter substitution |
| `enable_wal_cdc` | bool | `false` | Enable WAL-based CDC |
| `publication_name` | string | `iggy_publication` | Logical replication publication |
| `replication_slot` | string | `iggy_slot` | Replication slot name |
| `capture_operations` | array | `["INSERT","UPDATE","DELETE"]` | CDC operations to capture |
| `cdc_backend` | string | `builtin` | `builtin` or `pg_replicate` |
| `verbose_logging` | bool | `false` | Log at info level instead of debug |
| `max_retries` | u32 | `3` | Max retry attempts for transient errors |
| `retry_delay` | string | `1s` | Base delay between retries (e.g., `500ms`, `2s`) |

## Output Modes

### JSON Mode (Default)

When `payload_column` is not set, each row is wrapped in a `DatabaseRecord` JSON structure:

```json
{
  "table_name": "users",
  "operation_type": "SELECT",
  "timestamp": "2024-01-15T10:30:00Z",
  "data": {
    "id": 123,
    "name": "John Doe",
    "email": "john@example.com"
  },
  "old_data": null
}
```

The stream config should use `schema = "json"`.

### Payload Column Extraction

When `payload_column` is set, the connector extracts that column directly as the Iggy message payload. The `payload_format` option determines how the column is read:

| Format | Column Type | Schema | Description |
| ------ | ----------- | ------ | ----------- |
| `bytea` / `raw` | `BYTEA` | `raw` | Raw bytes passthrough |
| `text` | `TEXT` | `text` | UTF-8 text |
| `json_direct` / `jsonb` | `JSONB` | `json` | JSON object serialized to bytes |

## Payload Format Examples

### BYTEA (Raw Bytes)

Extract raw bytes from a BYTEA column:

```sql
CREATE TABLE message_queue (
    id SERIAL PRIMARY KEY,
    payload BYTEA NOT NULL
);
```

```toml
[[streams]]
stream = "messages"
topic = "queue"
schema = "raw"
batch_length = 100

[plugin_config]
tables = ["message_queue"]
tracking_column = "id"
payload_column = "payload"
payload_format = "bytea"
```

### TEXT

Extract text from a TEXT column:

```sql
CREATE TABLE logs (
    id SERIAL PRIMARY KEY,
    message TEXT NOT NULL
);
```

```toml
[[streams]]
stream = "logs"
topic = "app_logs"
schema = "text"
batch_length = 100

[plugin_config]
tables = ["logs"]
tracking_column = "id"
payload_column = "message"
payload_format = "text"
```

### JSONB (Direct)

Extract JSONB directly as JSON payload (without `DatabaseRecord` wrapper):

```sql
CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL
);
```

```toml
[[streams]]
stream = "events"
topic = "user_events"
schema = "json"
batch_length = 100

[plugin_config]
tables = ["events"]
tracking_column = "id"
payload_column = "data"
payload_format = "json_direct"
```

## Custom Query Parameters

When using `custom_query`, these placeholders are available:

| Placeholder | Replaced With |
| ----------- | ------------- |
| `$table` | Current table name |
| `$offset` | Last processed offset (or `initial_offset`) |
| `$limit` | `batch_size` value |
| `$now` | Current UTC timestamp (RFC3339) |
| `$now_unix` | Current Unix timestamp (seconds) |

Example:

```sql
SELECT * FROM $table
WHERE created_at > '$offset'
  AND (scheduled_at IS NULL OR scheduled_at <= '$now')
ORDER BY created_at
LIMIT $limit
```

## Delete After Read / Mark as Processed

### Delete After Read

Deletes rows from the source table after successful processing:

```toml
[plugin_config]
delete_after_read = true
primary_key_column = "id"
```

### Mark as Processed

Updates a boolean column instead of deleting:

```toml
[plugin_config]
processed_column = "is_processed"
primary_key_column = "id"
```

Your table needs the boolean column:

```sql
ALTER TABLE users ADD COLUMN is_processed BOOLEAN DEFAULT false;
```

When `processed_column` is set, the connector automatically adds a `WHERE is_processed = FALSE` filter to the polling query, so only unprocessed rows are fetched. This improves polling efficiency as the table grows.

## Supported Column Types

The connector handles these PostgreSQL types in JSON mode:

| PostgreSQL Type | JSON Output |
| --------------- | ----------- |
| `BOOL` | boolean |
| `INT2`, `INT4`, `INT8` | number |
| `FLOAT4`, `FLOAT8` | number |
| `NUMERIC` | number (parsed as f64) |
| `VARCHAR`, `TEXT`, `CHAR` | string |
| `TIMESTAMP`, `TIMESTAMPTZ` | string (RFC3339) |
| `UUID` | string |
| `JSON`, `JSONB` | object |
| `BYTEA` | base64 string |
| Other | string (fallback) |

## CDC Mode

CDC requires PostgreSQL logical replication setup:

1. Set `wal_level = logical` in `postgresql.conf`
2. Restart PostgreSQL
3. Use a direct connection (no pooler)

```toml
[plugin_config]
mode = "cdc"
enable_wal_cdc = true
tables = ["users", "orders"]
capture_operations = ["INSERT", "UPDATE", "DELETE"]
```

The `pg_replicate` backend requires the `cdc_pg_replicate` feature flag at build time.

## Example Configs

### Basic Polling (JSON Mode)

```toml
[[streams]]
stream = "user_events"
topic = "users"
schema = "json"
batch_length = 100

[plugin_config]
connection_string = "postgresql://user:pass@localhost:5432/mydb"
mode = "polling"
tables = ["users"]
poll_interval = "1s"
tracking_column = "updated_at"
```

### Raw Payload Passthrough

```toml
[[streams]]
stream = "messages"
topic = "queue"
schema = "raw"
batch_length = 100

[plugin_config]
connection_string = "postgresql://user:pass@localhost:5432/mydb"
mode = "polling"
tables = ["message_queue"]
poll_interval = "100ms"
tracking_column = "id"
payload_column = "payload"
payload_format = "bytea"
delete_after_read = true
```

### JSONB Direct Extraction

```toml
[[streams]]
stream = "events"
topic = "user_events"
schema = "json"
batch_length = 100

[plugin_config]
connection_string = "postgresql://user:pass@localhost:5432/mydb"
mode = "polling"
tables = ["events"]
poll_interval = "1s"
tracking_column = "id"
payload_column = "data"
payload_format = "json_direct"
```

### CDC with Custom Operations

```toml
[[streams]]
stream = "audit"
topic = "changes"
schema = "json"
batch_length = 100

[plugin_config]
connection_string = "postgresql://user:pass@localhost:5432/mydb"
mode = "cdc"
enable_wal_cdc = true
tables = ["users", "orders"]
capture_operations = ["INSERT", "UPDATE"]
```

## Reliability Features

### Automatic Retries

The connector automatically retries transient database errors (connection issues, deadlocks, serialization failures) with exponential backoff. Configure with `max_retries` (default: 3) and `retry_delay` (default: `1s`). The actual delay is `retry_delay * attempt_number`. Non-transient errors fail immediately.

### SQL Injection Protection

All table names, column names, and identifiers are properly quoted to prevent SQL injection attacks. User-provided values in tracking offsets are also safely escaped.

## Usage with Sink Connector

The source and sink connectors can work together for pass-through scenarios:

### Raw Bytes Pass-through

1. **Sink** with `payload_format = "bytea"` stores messages as BYTEA
2. **Source** with `payload_column = "payload"` and `payload_format = "bytea"` reads them back

### JSON Pass-through

1. **Sink** with `payload_format = "json"` stores messages as JSONB
2. **Source** with `payload_column = "payload"` and `payload_format = "json_direct"` reads JSONB directly
