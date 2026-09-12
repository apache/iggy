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

Build from the matching 0.9.0/edge checkout root:

```bash
cargo build --release -p iggy_connector_postgres_sink
```

Configure the broker connection and connector directory as described in the
[runtime README](../../runtime/README.md). Save the following connector file
there. Paths are relative to the process working directory, here the checkout
root. The URI assumes a local `iggy` database and `iggy`/`iggy` credentials;
replace it for your deployment.

```toml
type = "sink"
key = "postgres-sink"
enabled = true
version = 1
name = "Postgres Sink"
path = "target/release/libiggy_connector_postgres_sink"

[[streams]]
stream = "user_events"
topics = ["users", "orders"]
schema = "json"
batch_length = 100
poll_interval = "5ms"
consumer_group = "postgres_sink"

[plugin_config]
connection_string = "postgresql://iggy:iggy@localhost:5432/iggy"
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
| `target_table` | string | required | One quoted table identifier; `schema.table` is a literal name containing a dot |
| `batch_size` | u32 | `100` | Maximum messages per insert statement; `0` behaves as `1` |
| `max_connections` | u32 | `10` | Max database connections |
| `auto_create_table` | bool | `false` | Create a missing table; existing tables are not migrated |
| `include_metadata` | bool | `true` | Include Iggy metadata columns |
| `include_checksum` | bool | `true` | Include message checksum |
| `include_origin_timestamp` | bool | `true` | Include original timestamp |
| `payload_format` | string | `bytea` | Payload column type: `bytea`, `json` (alias `jsonb`), or `text` |
| `verbose_logging` | bool | `false` | Log at info level instead of debug |
| `max_retries` | u32 | `3` | Total attempts per transiently failing insert; `0` and `1` both allow one attempt |
| `retry_delay` | string | `1s` | Base for linear retry delays; invalid values silently fall back to `1s` |

## Payload Format

The `payload_format` option determines how the payload is stored in PostgreSQL:

| Format | Column Type | Description |
| ------ | ----------- | ----------- |
| `bytea` | `BYTEA` | Bytes after stream decoding and transforms (default). |
| `json` / `jsonb` | `JSONB` | Native JSON. Enables JSON queries and indexing. Payload must be valid JSON accepted by PostgreSQL JSONB. |
| `text` | `TEXT` | UTF-8 text. Payload must be valid UTF-8 accepted by PostgreSQL TEXT. |

Format names are case-insensitive; unknown names silently use BYTEA. JSONB
accepts objects, arrays and scalars but rejects `\u0000`; TEXT rejects zero bytes.
Headers are not stored. The following format snippets change the corresponding
setting in the complete connector file above.

### BYTEA (Default)

Stores the bytes after decoding and transforms. Use `schema = "raw"` without
payload-changing transforms to preserve original binary or protobuf bytes.
JSON decoding can change the byte representation before the sink receives it.

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

With `auto_create_table = true` and all three metadata flags enabled, a missing
table gets the following structure. The payload type follows `payload_format`:

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

Disabling `include_metadata` omits offset, timestamp, stream, topic and partition
columns. Checksum and origin timestamp flags work independently. The `id`,
`payload` and `created_at` columns are always created. Existing tables are not
migrated. With automatic creation disabled, startup does not check that a
compatible target table exists.

`id` is the full unsigned 128-bit message ID, without source identifiers or
offset. Offset/checksum are cast to signed 64-bit integers and partition IDs to
signed 32-bit integers, so values beyond those signed ranges appear negative.
Message and origin timestamps are Unix microseconds; zero becomes the epoch and
values outside the date library's range fall back to the current time.
`created_at` is generated by PostgreSQL during insertion.

## Performance

### Recommended Indexes

Create only indexes needed by your queries. Metadata indexes require the
corresponding columns; the GIN index requires a JSONB payload column. The sink
automatically creates only the primary-key index.

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
- Stay within PostgreSQL's 65,535 bind-parameter limit: each row uses
  `2 + 5 * include_metadata + include_checksum + include_origin_timestamp`
  parameters. With default flags, at most 7,281 rows fit in one statement.
  Larger actual chunks fail; the runtime's `batch_length` also limits poll size.

## Example Configs

These fragments replace the `streams` and `plugin_config` sections in the
complete connector file above. Provision the named Iggy streams/topics and
PostgreSQL databases, and replace the URI credentials before starting.

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

The connector retries SQLx I/O and pool-acquisition timeout errors, plus SQLSTATEs
`40001`, `40P01`, `57P01`, `57P02`, `57P03`, `08000`, `08003` and `08006`.
`max_retries` counts total attempts, including the first (default: 3); zero still
allows one attempt. Backoff is linear: `retry_delay * attempt_number`, with a
one-based retry number. The base defaults to `1s`, including for invalid delay
strings. Other errors stop that chunk's retry loop. Startup has no plugin retry
loop.

### Failed Inserts and Replay

Each current-poll chunk becomes one multi-row `INSERT`. A bad payload, duplicate
primary key, incompatible table or other terminal failure rejects that chunk.
Later chunks are still attempted. Failures are logged and added to a private
insertion-error count, but `consume()` returns success and counts every attempted
message as processed. Runtime processed counts can include unstored messages,
and runtime error counts do not expose these insert failures.

The query has no `ON CONFLICT` or upsert clause. An ID already in the table
rejects the whole chunk, including new IDs alongside it. There is no transaction
covering a complete poll. Runtime auto-commits while polling and does not replay
failed chunks. A connection failure can leave the write outcome uncertain;
these retries do not establish an end-to-end at-least-once guarantee.

### Connection Pool Management

The connection pool is properly closed when the connector shuts down, ensuring clean resource cleanup.

## Usage with Source Connector

The sink can work with the source connector for pass-through scenarios:

1. **Sink** with `payload_format = "bytea"` stores messages as raw bytes
2. **Source** with `payload_column = "payload"` and `payload_format = "bytea"` reads them back

For JSON data:

1. **Sink** with `payload_format = "json"` stores as JSONB
2. **Source** with `payload_column = "payload"` and `payload_format = "json_direct"` reads JSONB directly

These settings transfer payloads. The source generates new message IDs and
timestamps; it does not restore the sink's original message metadata. JSONB
serialization does not preserve the original JSON bytes. See the
[source README](../../sources/postgres_source/README.md) for the remaining
required source configuration.
