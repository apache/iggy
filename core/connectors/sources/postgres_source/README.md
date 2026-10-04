# PostgreSQL Source Connector

The PostgreSQL source connector fetches data from PostgreSQL databases and streams it to Iggy topics. It supports table polling and Change Data Capture (CDC) modes with flexible payload extraction.

## Features

- **Table Polling**: Incrementally fetch data from PostgreSQL tables
- **Change Data Capture**: Monitor database changes using PostgreSQL logical replication
- **Flexible Payload Extraction**: Extract BYTEA, TEXT, or JSONB columns directly as payload
- **Custom Queries**: Use custom SQL queries with parameter substitution
- **Delete After Read**: Delete rows after delivery and checkpoint acknowledgement
- **Mark as Processed**: Mark rows after acknowledgement using a boolean column
- **Multiple Tables**: Poll multiple tables sequentially
- **Batch Processing**: Fetch data in configurable batch sizes
- **Offset Tracking**: Resume from acknowledged per-table offsets; retries can duplicate rows

## Configuration

Use the broker credentials and main runtime configuration from the
[source guide](https://iggy.apache.org/docs/connectors/sources/source/#configuration).
Build from the matching checkout root:

```bash
cargo build --release -p iggy_connector_postgres_source
```

The examples use the `iggy` database and `iggy`/`iggy` credentials. Create the
`users` and `orders` tables using the SQL on the
[Postgres source page](https://iggy.apache.org/docs/connectors/sources/postgres/).
Save one connector entry in the runtime's connector directory and run the runtime
from the checkout root. Create its destination stream and topic before starting it.

```toml
type = "source"
key = "postgres"
enabled = true
version = 0
name = "Postgres source"
path = "target/release/libiggy_connector_postgres_source"
plugin_config_format = "toml"

[[streams]]
stream = "user_events"
topic = "users"
schema = "json"
batch_length = 100

[plugin_config]
connection_string = "postgresql://iggy:iggy@localhost:5432/iggy"
mode = "polling"
tables = ["users", "orders"]
poll_interval = "1s"
batch_size = 1000
tracking_column = "id"
initial_offset = "0"
max_connections = 10
snake_case_columns = false
include_metadata = true

# Payload extraction requires a matching column and stream schema.
# payload_column = "payload"
# payload_format = "bytea"

# Delete/mark processed (optional)
delete_after_read = false
# processed_column = "is_processed"
primary_key_column = "id"

# Custom query (optional)
# custom_query = "SELECT * FROM $table WHERE id > $offset ORDER BY id LIMIT $limit"

# CDC options (only used when mode = "cdc")
replication_slot = "iggy_slot"
capture_operations = ["INSERT", "UPDATE", "DELETE"]
cdc_backend = "builtin"
```

## Configuration Options

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `connection_string` | string | required | PostgreSQL connection string |
| `mode` | string | required | `polling` or `cdc` |
| `tables` | array | required | Polling tables; empty polls none. Empty captures all tables in CDC |
| `poll_interval` | string | `10s` | Delay before each cycle; invalid values fall back to `10s` |
| `batch_size` | u32 | `1000` | Limit per table in polling mode; CDC limits are checked at transaction boundaries |
| `tracking_column` | string | `id` | Unique, non-null column for incremental updates |
| `initial_offset` | string | none | Exclusive starting value when a saved table offset is absent |
| `max_connections` | u32 | `10` | Max database connections |
| `snake_case_columns` | bool | `false` | Convert column names to snake_case |
| `include_metadata` | bool | `true` | Polling layout; false currently adds `data.data` while retaining the envelope |
| `payload_column` | string | none | Column to extract as payload |
| `payload_format` | string | `json` | Selected-column format; see the schema pairings below |
| `delete_after_read` | bool | `false` | Delete selected rows after Ack |
| `processed_column` | string | none | Boolean column to filter on FALSE and mark after Ack |
| `primary_key_column` | string | tracking_column | Unique, non-null key for delete/mark operations |
| `custom_query` | string | none | Custom SQL with parameter substitution |
| `replication_slot` | string | `iggy_slot` | Replication slot name (only used when `mode = "cdc"`) |
| `capture_operations` | array | `["INSERT","UPDATE","DELETE"]` | CDC operations to capture |
| `cdc_backend` | string | `builtin` | Only `builtin` is implemented |
| `verbose_logging` | bool | `false` | Log at info level instead of debug |
| `max_retries` | u32 | `3` | Total attempts for transient errors, including the first; zero still makes one attempt |
| `retry_delay` | string | `1s` | Base delay between retries (e.g., `500ms`, `2s`) |

## Delivery Failures

Each batch selected by the polling query is delivered at least once, so
consumers must tolerate duplicates. Complete polling capture additionally
requires transactions to become visible in tracking-column order. A transaction
that commits below an already acknowledged cursor is not selected by a later
poll, even when the tracking column is unique. A failed send NACKs the batch and
leaves its database progress uncommitted for redelivery.
After five consecutive NACKs, the source stops and requires a manual connector
restart.

Cleanup work and replication-slot advances are stored with the acknowledged
checkpoint before they run. After a restart, the connector replays that work
before polling new rows and then saves a state-only checkpoint to retire it.
Checkpoints created by older connector versions remain compatible unless they
contain unfinished row cleanup without the row-version receipt required for
safe replay. In that case, verify the affected rows and clear the connector
state before restarting.

## Output Modes

### JSON Mode (Default)

When `payload_column` is not set, polling wraps each row in a `DatabaseRecord` JSON structure:

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

The stream config should use `schema = "json"`. With `include_metadata = false`,
the envelope currently remains and the row moves to `data.data`. The timestamp
is generated during polling. The runtime does not transfer the plugin's timestamp
fields into broker message metadata.

### Payload Column Extraction

In polling mode, an existing `payload_column` bypasses the envelope. The `payload_format` option determines how the column is read:

| Format | Column Type | Schema | Description |
| ------ | ----------- | ------ | ----------- |
| `bytea` / `raw` | `BYTEA` | `raw` | Raw bytes passthrough |
| `text` | `TEXT` | `text` | UTF-8 text |
| `json_direct` / `jsonb` / `jsonb_direct` | `JSON`, `JSONB` | `json` | The JSON value serialized to bytes |
| `json` (default) | `BYTEA` | `json` | Bytes must already contain valid JSON |

Null BYTEA and TEXT payloads become empty bytes. Iggy rejects empty payloads,
so the entire batch receives Nack and five consecutive failures stop polling.
Use non-null, non-empty BYTEA/text payloads. Null JSON/JSONB becomes JSON
`null` and can be delivered. Missing selected columns fall back to the envelope while retaining the
selected format's schema. Type mismatches reject the poll. Without a selected
column, every payload-format option emits whole-row JSON.

## Payload Format Examples

Keep the connector-entry header from the configuration above, and replace its
`[[streams]]` and `[plugin_config]` sections with one variant. Run the matching
SQL first; insert rows with increasing unique IDs to produce messages.

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
connection_string = "postgresql://iggy:iggy@localhost:5432/iggy"
mode = "polling"
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
connection_string = "postgresql://iggy:iggy@localhost:5432/iggy"
mode = "polling"
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
connection_string = "postgresql://iggy:iggy@localhost:5432/iggy"
mode = "polling"
tables = ["events"]
tracking_column = "id"
payload_column = "data"
payload_format = "json_direct"
```

## Custom Query Parameters

A `custom_query` replaces the entire default query, including its ordering,
limit and processed-row filter. These placeholders use textual substitution.
The connector quotes and escapes `$offset` as a SQL value, including templates
that already wrap it in single quotes. Other substitutions are inserted directly:

| Placeholder | Replaced With |
| ----------- | ------------- |
| `$table` | Current table name |
| `$offset` | Last acknowledged offset, `initial_offset`, or an empty string if neither exists |
| `$limit` | `batch_size` value |
| `$now` | Current UTC timestamp (RFC3339) |
| `$now_unix` | Current Unix timestamp (seconds) |

This template requires a non-null, unique, increasing `created_at` column, an
optional `scheduled_at` timestamp, and an RFC3339 `initial_offset` before the
first row. Set `tracking_column = "created_at"` and save it as the
`custom_query` string:

```sql
SELECT * FROM $table
WHERE created_at > $offset
  AND (scheduled_at IS NULL OR scheduled_at <= '$now')
ORDER BY created_at
LIMIT $limit
```

Custom queries containing `$offset` advance the connector-managed offset after
the batch is acknowledged. The tracking column must be unique and non-null, and
the query must return rows ordered by that column in ascending order. Custom
queries without `$offset` do not advance the connector-managed offset. Cleanup
operations remain bounded by the selected primary keys rather than by the custom
query's cursor. When cleanup is enabled, the query result must include the
resolved cleanup key. The connector joins the result to the configured source
table in the same PostgreSQL snapshot to capture each selected row's version.

The generated polling query also uses this scalar cursor. At startup, the
connector rejects any generated query or `$offset` custom query whose tracking
column lacks a valid single-column unique index or permits null values.

## Delete After Read / Mark as Processed

Apply these options inside the existing `[plugin_config]` section.

The resolved cleanup key is `primary_key_column`, or `tracking_column` when the
former is unset. For every configured table, it must be a non-null column with
a valid single-column unique index. The connector validates this requirement
at startup before enabling delete or mark operations.
The resolved key, tracking column, cleanup action, and selected row versions are
stored in the acknowledged checkpoint. A restart rejects incompatible cleanup
configuration instead of applying old work to a different column or action.

### Delete After Read

Deletes selected rows after the runtime forwards the batch, saves its checkpoint and sends Ack:

```toml
[plugin_config]
delete_after_read = true
primary_key_column = "id"
```

### Mark as Processed

Updates a boolean column after Ack instead of deleting:

```toml
[plugin_config]
processed_column = "is_processed"
primary_key_column = "id"
```

Each configured table needs the boolean column:

```sql
ALTER TABLE users ADD COLUMN is_processed BOOLEAN DEFAULT false;
ALTER TABLE orders ADD COLUMN is_processed BOOLEAN DEFAULT false;
```

When `processed_column` is set, the default polling query adds an
`is_processed = FALSE` condition. A custom query must add its own filter.
`delete_after_read = true` takes precedence over marking. Cleanup keys are staged
during polling and discarded on Nack or a failed poll, leaving rows available
for replay. Cleanup runs after delivery and checkpointing. Unfinished cleanup
is retried before polling more rows and restored from the checkpoint after a
restart. Three consecutive cleanup failures stop the source. Cleanup across
tables is not atomic.

With the generated polling query, a row whose tracking value moves past the
batch boundary between poll and acknowledgement is left unchanged and returns
in a later poll. Custom queries do not apply this boundary. Cleanup also matches
the row version captured by the poll, so replay cannot delete or mark a
replacement row that reused the same key.

## Supported Column Types

The connector handles these PostgreSQL types in JSON mode:

| PostgreSQL Type | JSON Output |
| --------------- | ----------- |
| `BOOL` | boolean |
| `INT2`, `INT4`, `INT8` | number |
| `FLOAT4`, `FLOAT8` | number |
| `NUMERIC` | exact decimal string; non-finite values become `"NaN"`, `"Infinity"`, or `"-Infinity"` |
| `VARCHAR`, `TEXT`, `BPCHAR` (`CHAR(n)`), `NAME` | string |
| `TIMESTAMP` | string without a timezone, such as `2024-01-15 10:30:00` |
| `TIMESTAMPTZ` | RFC3339 string |
| `DATE`, `TIME`, `TIMETZ`, `INTERVAL` | formatted string |
| `UUID` | string |
| `JSON`, `JSONB` | original JSON value, including scalars, arrays and null |
| `BYTEA` | base64 string |
| Supported arrays | JSON array, preserving null elements |
| Other | UTF-8 driver bytes if valid, otherwise base64; not a general semantic conversion |

Finite NUMERIC values are decoded through BigDecimal without floating-point
conversion, preserving exact tracking boundaries. SQL NULL remains JSON null.
The array mappings cover boolean, integer/OID, floating-point, text, UUID,
JSON/JSONB, date/time/timestamp and interval arrays.

## CDC Mode

CDC requires PostgreSQL 11 or newer and logical replication setup:

1. Set `wal_level = logical` in `postgresql.conf`
2. Restart PostgreSQL
3. Allow the login to use logical-decoding SQL functions and ensure `test_decoding` is installed and allowed

The builtin backend uses ordinary SQL connections. A proxy must support its SQL
and replication-slot operations; the connector does not open a replication-protocol
connection. It does not create or require a publication.

```toml
[plugin_config]
mode = "cdc"
tables = ["users", "orders"]
capture_operations = ["INSERT", "UPDATE", "DELETE"]
```

The `pg_replicate` backend is not implemented. Without `cdc_pg_replicate`, startup
rejects it; with the feature, polling returns an unimplemented-backend error.
`capture_operations` accepts uppercase INSERT/UPDATE/DELETE, and an empty array
emits none. An empty `tables` array captures all tables. Filters apply after the
slot read, so acknowledged slot advances also pass excluded changes.

CDC emits a JSON envelope rather than applying polling payload extraction.
Quoted `test_decoding` values, including JSONB and arrays, remain strings.
Deletes carry replica-identity columns; updates can include `old_data` from
an old-key tuple. Unchanged TOAST values become null, which does not establish
that the stored database value is null. The generated timestamp is polling time,
not a transaction commit timestamp.

The connector peeks at logical changes and advances the replication slot only
after Iggy acknowledges the batch. A failed delivery leaves the slot unchanged
so the next poll can read the same changes again.

Advancing the slot fast-forwards through the WAL range that was just peeked, so
each acknowledged batch is decoded twice. Poll and decode errors do not change
the connector's runtime status. Monitor `confirmed_flush_lsn`, retained WAL, and
replication slot lag in PostgreSQL to detect a stuck CDC poller.

### Slot Naming

Each CDC connector must use a unique `replication_slot`. Setup accepts any
pre-existing `test_decoding` slot, so two connectors pointed at the same
database with the default `replication_slot = "iggy_slot"` will silently
share one slot. Each connector peeks from and advances the same slot after
delivery, so one connector can move the shared position past changes that the
other has not processed.
Set an explicit, distinct `replication_slot` per connector instance.

### Decommissioning

A replication slot retains WAL for as long as it exists, regardless of
whether a connector is still consuming it (`max_slot_wal_keep_size` defaults
to `-1`, i.e. unbounded). Dropping a connector without dropping its slot
leaves an orphaned slot that accumulates WAL indefinitely and can fill the
disk. When decommissioning a CDC connector, drop its slot:

```sql
SELECT pg_drop_replication_slot('iggy_slot');
```

## Example Configs

### Basic Polling (JSON Mode)

```toml
[[streams]]
stream = "user_events"
topic = "users"
schema = "json"
batch_length = 100

[plugin_config]
connection_string = "postgresql://iggy:iggy@localhost:5432/iggy"
mode = "polling"
tables = ["users"]
poll_interval = "1s"
tracking_column = "id"
```

### Raw Payload Passthrough

```toml
[[streams]]
stream = "messages"
topic = "queue"
schema = "raw"
batch_length = 100

[plugin_config]
connection_string = "postgresql://iggy:iggy@localhost:5432/iggy"
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
connection_string = "postgresql://iggy:iggy@localhost:5432/iggy"
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
connection_string = "postgresql://iggy:iggy@localhost:5432/iggy"
mode = "cdc"
tables = ["users", "orders"]
capture_operations = ["INSERT", "UPDATE"]
```

## Reliability Features

### Automatic Retries

The connector automatically retries transient database errors (connection issues, deadlocks, serialization failures) with linear backoff. Configure with `max_retries` (default: 3) and `retry_delay` (default: `1s`). The actual delay is `retry_delay * attempt_number`. Non-transient errors fail immediately.

Polling operations use the complete configured retry schedule. Database work performed after an ACK, such as deleting or marking rows and advancing a replication slot, uses the same schedule but shares a 10-second deadline across the batch. When the configured schedule exceeds that window, unfinished ACK operations remain staged and are retried before the connector polls new rows.

ACK statements use a transaction-local 9-second server-side timeout so a stalled backend is cancelled before the 10-second ACK callback backstop expires. Polling and CDC reads retain the PostgreSQL connection's configured timeout.

The connector stops after three consecutive row-cleanup or replication-slot advance failures. This prevents permanent cleanup errors from stalling polling while the connector continues to report healthy progress, and prevents repeated CDC delivery while WAL continues to grow.

### SQL Injection Protection

The generated polling and cleanup queries quote table/column identifiers and
escape value literals. PostgreSQL infers value types from the compared columns,
so numeric-looking TEXT keys keep their exact values. Custom queries use raw
text substitution except for the quoted and escaped `$offset` value. Keep
templates and configured names trusted and quote other substitutions as required
by their SQL context.

### Cursor and Checkpoint Limits

The default polling query uses `tracking_column > last_offset`, ascending order
and a per-table limit. Tracking values must be non-null, unique and increasing
in the database's sort order, with a valid single-column unique index checked
at startup. Transactions must become visible in tracking-column order; late
inserts or updates at or below the watermark can be missed. Polling does not
capture deletions.

Polling stages its per-table offsets, last-poll time and processed-row
count until the runtime forwards the batch, saves its MessagePack checkpoint
and sends Ack. Nack discards that candidate. Delivery followed by a failed
checkpoint can duplicate messages; each poll assigns fresh random UUIDs.
The default file checkpoint is `local_state/source_postgres.state`; the runtime
also supports HTTP state storage. Missing or undecodable plugin state starts
fresh, while storage access errors prevent startup. Stop the connector before
resetting its checkpoint. Five consecutive Nacks stop the SDK polling loop.
Delete/mark operations and CDC slot advances run after Ack, as described above.

## Usage with Sink Connector

The source and sink connectors can work together for pass-through scenarios.
For incremental reads of a sink-created table, use one input stream/topic/partition,
keep `include_metadata = true` on the sink, and set
`tracking_column = "iggy_offset"` with `initial_offset = "-1"` on the source,
so the first message at offset zero is included. Message IDs are not an increasing
cursor, and offsets from different partitions are not globally unique.
The source requires `iggy_offset` to be non-null and backed by a single-column
unique index; enforce both constraints on the sink-created table before starting
the source.

### Raw Bytes Pass-through

1. **Sink** with `payload_format = "bytea"` stores messages as BYTEA
2. **Source** with `payload_column = "payload"` and `payload_format = "bytea"` reads them back

### JSON Pass-through

1. **Sink** with `payload_format = "json"` stores messages as JSONB
2. **Source** with `payload_column = "payload"` and `payload_format = "json_direct"` reads JSONB directly

### Flat-Schema Sinks (Iceberg, Delta)

In the default JSON mode (no `payload_column`), the Postgres source wraps each row in a
`DatabaseRecord` envelope containing `table_name`, `operation_type`, `timestamp`, `data`, and
`old_data`. Sinks like Iceberg and Delta expect JSON matching the target table schema.
For a table whose columns match the source row, unwrap the envelope before delivery.

**Option A - use the `unwrap_envelope` transform** on the sink side to extract the `data` field:

```toml
[transforms.unwrap_envelope]
enabled = true
field = "data"
```

**Option B - bypass the envelope** by configuring the source with `payload_column` and
`payload_format = "json_direct"` to emit raw JSONB directly (see Payload Column Extraction above).
