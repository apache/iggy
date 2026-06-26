# MySQL Source Connector

The MySQL source connector fetches data from MySQL databases and streams it to Iggy topics. It supports incremental table polling with flexible payload extraction.

> **Note:** Only polling mode is available. Binlog CDC support is planned for a future release.

## Features

- **Table Polling**: Incrementally fetch data from MySQL tables using a tracking column
- **Flexible Payload Extraction**: Extract BLOB, TEXT, or JSON columns directly as payload
- **Custom Queries**: Use custom SQL queries with parameter substitution
- **Delete After Read**: Automatically delete rows after processing
- **Mark as Processed**: Mark rows as processed using a boolean column
- **Multiple Tables**: Monitor multiple tables simultaneously
- **Batch Processing**: Fetch data in configurable batch sizes
- **Offset Tracking**: Keep track of processed records to avoid duplicates

## Configuration

```toml
[plugin_config]
connection_string = "mysql://user:pass@localhost:3306/database"
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
```

## Configuration Options

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `connection_string` | string | required | MySQL connection string (`mysql://user:pass@host:3306/db`) |
| `tables` | array | required | List of tables to monitor |
| `poll_interval` | string | `10s` | How often to poll (e.g., `1s`, `5m`) |
| `batch_size` | u32 | `1000` | Max rows per poll |
| `tracking_column` | string | `id` | Column for incremental updates |
| `initial_offset` | string | none | Starting value for tracking column |
| `max_connections` | u32 | `10` | Max database connections |
| `snake_case_columns` | bool | `false` | Convert column names to snake_case |
| `include_metadata` | bool | `true` | Wrap results with metadata envelope |
| `payload_column` | string | none | Column to extract directly as payload |
| `payload_format` | string | `bytea` | Format of payload_column: `bytea`, `text`, or `json_direct` |
| `delete_after_read` | bool | `false` | Delete rows after reading |
| `processed_column` | string | none | Boolean column to mark as processed |
| `primary_key_column` | string | tracking_column | PK for delete/mark operations |
| `custom_query` | string | none | Custom SQL with parameter substitution |
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

When `include_metadata = false`, the envelope is omitted and the row columns are serialized directly:

```json
{
  "id": 123,
  "name": "John Doe",
  "email": "john@example.com"
}
```

### Payload Column Extraction

When `payload_column` is set, the connector extracts that column directly as the Iggy message payload. The `payload_format` option determines how the column is read:

| Format | Column Type | Schema | Description |
| ------ | ----------- | ------ | ----------- |
| `bytea` / `raw` | `BLOB`, `BINARY`, `VARBINARY` | `raw` | Raw bytes passthrough |
| `text` | `TEXT`, `VARCHAR` | `text` | UTF-8 text |
| `json_direct` / `jsonb` | `JSON` | `json` | JSON object serialized to bytes |

## Payload Format Examples

### BLOB (Raw Bytes)

Extract raw bytes from a BLOB column:

```sql
CREATE TABLE message_queue (
    id INT AUTO_INCREMENT PRIMARY KEY,
    payload BLOB NOT NULL
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
    id INT AUTO_INCREMENT PRIMARY KEY,
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

### JSON (Direct)

Extract a JSON column directly as JSON payload (without `DatabaseRecord` wrapper):

```sql
CREATE TABLE events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    data JSON NOT NULL
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

When `processed_column` is set, the connector automatically adds a `WHERE is_processed = FALSE` filter to the polling query, so only unprocessed rows are fetched.

## Supported Column Types

The connector handles these MySQL types in JSON mode:

| MySQL Type | JSON Output |
| ---------- | ----------- |
| `BOOLEAN` | boolean |
| `TINYINT` | number (i8 → i64) |
| `SMALLINT` | number (i16 → i64) |
| `MEDIUMINT`, `INT` | number (i32 → i64) |
| `BIGINT` | number (i64) |
| `TINYINT UNSIGNED` | number (u8 → u64) |
| `SMALLINT UNSIGNED` | number (u16 → u64) |
| `MEDIUMINT UNSIGNED`, `INT UNSIGNED` | number (u32 → u64) |
| `BIGINT UNSIGNED` | number (u64) |
| `FLOAT` | number (f32 → f64) |
| `DOUBLE` | number (f64) |
| `DECIMAL` | string (exact value preserved) |
| `BIT` | number (u64) |
| `YEAR` | number (u16 → u64) |
| `DATE` | string (YYYY-MM-DD) |
| `TIME` | string (HH:MM:SS) |
| `DATETIME`, `TIMESTAMP` | string (YYYY-MM-DD HH:MM:SS) |
| `CHAR`, `VARCHAR`, `TINYTEXT`, `TEXT`, `MEDIUMTEXT`, `LONGTEXT` | string |
| `ENUM`, `SET` | string |
| `BINARY`, `VARBINARY`, `TINYBLOB`, `BLOB`, `MEDIUMBLOB`, `LONGBLOB` | base64 string |
| `GEOMETRY` | base64 string |
| `JSON` | object |
| `NULL` | null |
| Unknown | string (text fallback), or base64 string (binary fallback) |

> **DECIMAL precision:** MySQL `DECIMAL` is an arbitrary-precision decimal type. The connector emits it as a JSON string verbatim (e.g. `"99999999999999999"`), preserving the exact value. Parse it as a decimal in the consumer if you need to do arithmetic; do not assume it arrives as a JSON number.

## Reliability Features

### Automatic Retries

The connector automatically retries transient database errors with linear backoff (`retry_delay * attempt_number`). Transient errors include:

| MySQL Error | Meaning |
| ----------- | ------- |
| `1213` | Deadlock, retry candidate |
| `1205` | Lock wait timeout |
| `1053` | Server shutdown in progress |
| `1152` | Connection aborted during handshake |
| `2006` | Server has gone away |
| `2013` | Lost connection during query |
| `1040` | Too many connections |
| `1041` | Out of memory |

Non-transient errors (syntax errors, constraint violations, missing tables) fail immediately without retry.

Configure with `max_retries` (default: `3`) and `retry_delay` (default: `1s`).

### SQL Injection Protection

All table names and column names are quoted with MySQL backtick syntax. Single quotes in offset values are escaped (`'` → `''`). NUL bytes in identifiers are rejected.

## Example Configs

### Basic Polling (JSON Mode)

```toml
[[streams]]
stream = "user_events"
topic = "users"
schema = "json"
batch_length = 100

[plugin_config]
connection_string = "mysql://user:pass@localhost:3306/mydb"
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
connection_string = "mysql://user:pass@localhost:3306/mydb"
tables = ["message_queue"]
poll_interval = "100ms"
tracking_column = "id"
payload_column = "payload"
payload_format = "bytea"
delete_after_read = true
```

### JSON Direct Extraction

```toml
[[streams]]
stream = "events"
topic = "user_events"
schema = "json"
batch_length = 100

[plugin_config]
connection_string = "mysql://user:pass@localhost:3306/mydb"
tables = ["events"]
poll_interval = "1s"
tracking_column = "id"
payload_column = "data"
payload_format = "json_direct"
```

### Multiple Tables with Timestamp Tracking

```toml
[[streams]]
stream = "audit"
topic = "changes"
schema = "json"
batch_length = 100

[plugin_config]
connection_string = "mysql://user:pass@localhost:3306/mydb"
tables = ["users", "orders", "products"]
poll_interval = "5s"
tracking_column = "updated_at"
initial_offset = "2024-01-01 00:00:00"
include_metadata = true
```

### Flat-Schema Sinks (Iceberg, Delta)

In the default JSON mode, each row is wrapped in a `DatabaseRecord` envelope. Sinks like Iceberg and Delta expect flat JSON matching the target schema, so the envelope must be unwrapped.

**Option A — use the `unwrap_envelope` transform** on the sink side to extract the `data` field:

```toml
[transforms.unwrap_envelope]
enabled = true
field = "data"
```

**Option B — bypass the envelope** by setting `include_metadata = false` on the source, which serializes columns directly without the wrapper.
