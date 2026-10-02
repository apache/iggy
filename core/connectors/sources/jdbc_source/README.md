# JDBC Source Connector

A JDBC source connector for Iggy designed to work with JDBC-compliant relational databases. PostgreSQL is covered by the runtime integration suite; the other driver examples below are configuration guides and are not yet exercised in CI.

## Overview

This connector reads data from relational databases using JDBC (Java Database Connectivity) and publishes it as messages to Iggy streams. It supports both bulk and incremental data synchronization modes.

## Features

- **JDBC Driver Support**: Uses a supplied JDBC driver without database-specific connector code
- **Incremental Sync**: Track changes using timestamps or auto-increment IDs
- **Bulk Mode**: Re-runs the query each poll for snapshots (capped at `batch_size` rows; see limitations)
- **Type Mapping**: Automatic conversion of SQL types to JSON
- **Configurable Polling**: Control how frequently data is fetched
- **State Management**: Tracks offsets so rows below the cursor are not re-read, committing the cursor only once the runtime acknowledges delivery (at-least-once, not exactly-once)
- **Flexible Queries**: Support for custom SQL queries with placeholders

## Supported Databases

PostgreSQL bulk and incremental modes are covered by end-to-end tests with a
real database and the PostgreSQL JDBC driver. The connector is designed around
standard JDBC APIs, so the following databases are expected to work with a
compatible driver, but they are not currently part of the integration test
matrix:

- MySQL / MariaDB
- Oracle Database
- Microsoft SQL Server
- H2 Database
- Apache Derby
- IBM DB2
- SQLite (via JDBC)
- SAP HANA
- Teradata
- Snowflake
- Amazon Redshift
- Google BigQuery
- Other JDBC-compliant relational databases

Driver behavior and SQL syntax vary. Validate the query, type mappings, timeout
behavior, and cursor semantics against the exact driver version before using an
untested database in production.

## Prerequisites

1. **Java Runtime Environment (JRE)**: JRE 8 or later must be installed
2. **JDBC Driver**: Download the appropriate JDBC driver JAR for your database

### Downloading JDBC Drivers

**MySQL:**

```bash
wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar
```

**PostgreSQL:**

```bash
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
```

**Oracle:**

- Download from [Oracle JDBC Driver Downloads](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html)

**SQL Server:**

```bash
wget https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.4.1.jre11/mssql-jdbc-12.4.1.jre11.jar
```

**H2:**

```bash
wget https://repo1.maven.org/maven2/com/h2database/h2/2.2.224/h2-2.2.224.jar
```

## Configuration

### Basic Configuration (Incremental Sync)

```toml
type = "source"
key = "jdbc_mysql_source"
enabled = true

[plugin_config]
jdbc_url = "jdbc:mysql://localhost:3306/ecommerce"
driver_class = "com.mysql.cj.jdbc.Driver"
driver_jar_path = "/opt/jdbc-drivers/mysql-connector-j-8.0.33.jar"
username = "iggy_user"
password = "secret_password"
query = "SELECT * FROM orders WHERE updated_at > {last_offset} ORDER BY updated_at ASC"
poll_interval = "30s"
batch_size = 1000
# updated_at is a timestamp and may not be unique. If equal timestamps cross a
# batch boundary, the poll fails closed (see "Unique / strictly increasing"
# below). Prefer a unique auto-increment key, or keep batch_size larger than any
# same-timestamp group.
tracking_column = "updated_at"
initial_offset = "2024-01-01 00:00:00"
mode = "incremental"
snake_case_columns = true
include_metadata = true
verbose_logging = false

[[streams]]
stream = "ecommerce"
topic = "orders"
```

### Bulk Mode Configuration

```toml
type = "source"
key = "jdbc_bulk_source"
enabled = true

[plugin_config]
jdbc_url = "jdbc:postgresql://localhost:5432/warehouse"
driver_class = "org.postgresql.Driver"
driver_jar_path = "/opt/jdbc-drivers/postgresql-42.6.0.jar"
username = "warehouse_user"
password = "secret"
query = "SELECT * FROM product_catalog"
poll_interval = "1h"
batch_size = 5000
mode = "bulk"
snake_case_columns = false
include_metadata = true

[[streams]]
stream = "warehouse"
topic = "products"
```

### Oracle Database Example

```toml
type = "source"
key = "jdbc_oracle_source"
enabled = true

[plugin_config]
jdbc_url = "jdbc:oracle:thin:@localhost:1521:XE"
driver_class = "oracle.jdbc.OracleDriver"
driver_jar_path = "/opt/jdbc-drivers/ojdbc11.jar"
username = "system"
password = "oracle"
query = "SELECT * FROM CUSTOMERS WHERE ID > {last_offset} ORDER BY ID"
poll_interval = "1m"
batch_size = 500
tracking_column = "ID"
initial_offset = "0"
mode = "incremental"
jvm_options = ["-Xmx256m", "-Xms128m"]

[[streams]]
stream = "crm"
topic = "customers"
```

### SQL Server Example

```toml
type = "source"
key = "jdbc_sqlserver_source"
enabled = true

[plugin_config]
jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=Sales;encrypt=false"
driver_class = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
driver_jar_path = "/opt/jdbc-drivers/mssql-jdbc-12.4.1.jre11.jar"
username = "sa"
password = "YourPassword123"
query = "SELECT * FROM Orders WHERE OrderDate > {last_offset} ORDER BY OrderDate"
poll_interval = "15s"
batch_size = 2000
# OrderDate is a timestamp and may not be unique; see the uniqueness note on the
# MySQL example above. Prefer a unique key, or keep batch_size above any
# same-timestamp group.
tracking_column = "OrderDate"
initial_offset = "2024-01-01"
mode = "incremental"

[[streams]]
stream = "sales"
topic = "orders"
```

## Configuration Parameters

| Parameter | Type | Required | Default | Description |
| ----------- | ------ | ---------- | --------- | ------------- |
| `jdbc_url` | string | Yes | - | JDBC connection URL (can include credentials) |
| `driver_class` | string | Yes | - | JDBC driver class name |
| `driver_jar_path` | string | Yes | - | Path to the JDBC driver JAR (checked to exist at startup; passed to the embedded JVM as `-Djava.class.path`) |
| `username` | string | No | - | Database username (optional if in jdbc_url) |
| `password` | string | No | - | Database password (optional if in jdbc_url) |
| `query` | string | Yes | - | SQL query to execute. Incremental mode requires `{last_offset}`; `{tracking_column}` is also supported |
| `poll_interval` | string (duration) | No | 5s | Positive polling interval as a humantime string (e.g., "30s", "5m", "1h"); zero is rejected |
| `batch_size` | u32 | No | 1000 | Maximum rows to fetch per poll |
| `tracking_column` | string | Incremental | - | Column to track for incremental reads (required in incremental mode; the query must also `ORDER BY` it) |
| `initial_offset` | string | No | - | Starting offset value for first poll |
| `mode` | string | No | "incremental" | Sync mode: "incremental" or "bulk" |
| `connection_timeout_ms` | u64 | No | 5000 | Timeout (ms) for the per-poll `isValid` liveness check; converted to seconds and capped at 5s |
| `login_timeout_ms` | u64 | No | 30000 | Bound on establishing the connection (`DriverManager.setLoginTimeout`); rounded up to whole seconds. Stops an unreachable database from hanging startup |
| `query_timeout_ms` | u64 | No | 30000 | Positive bound on each query execution (`Statement.setQueryTimeout`); rounded up to whole seconds |
| `jvm_options` | array | No | [] | Custom JVM options (e.g., ["-Xmx1g"]) |
| `snake_case_columns` | bool | No | false | Convert column names to snake_case |
| `include_metadata` | bool | No | true | Wrap each row with metadata (operation type, timestamp). `table_name` is a reserved field and is currently always null |
| `verbose_logging` | bool | No | false | Log per-poll row and column counts at info instead of debug |

## Query Placeholders

The `query` parameter supports placeholders for dynamic queries:

- `{last_offset}`: Replaced with a JDBC `PreparedStatement` parameter and bound using the parameter's reported SQL type
- `{tracking_column}`: Replaced with the configured `tracking_column` (validated as a plain SQL identifier)

Incremental mode is validated at `open()` and enforces the following (the
connector refuses to start otherwise):

- **`tracking_column` is required.** Without it the offset can never advance and
  every poll re-reads the same rows.
- **The query must contain `{last_offset}`.** Without it each poll would execute
  the same query and re-read the first batch while the stored cursor had no
  effect.
- **Exactly one result column must match `tracking_column`.** The connector
  rejects a missing match and duplicate matching labels. Use explicit, unique
  aliases when a query joins tables that expose the same column name.
- **The query must order by the tracking column, ascending, as the first
  `ORDER BY` term.** Row limiting uses `setMaxRows`, so an unordered (or
  otherwise-ordered) query returns an arbitrary subset; advancing the offset to
  that subset's max would permanently skip the unread lower keys. The validator
  therefore requires the tracking column to be the **first** ordering term of the
  outer `ORDER BY` and rejects a descending (`DESC`) direction. Write either
  `ORDER BY {tracking_column}` or the column name (optionally table-qualified,
  e.g. `ORDER BY t.updated_at`). A composite order such as `ORDER BY other, id`
  (tracking column not first) or a `DESC` order is rejected at `open()`.
  The `ORDER BY` check is a lexical heuristic, not a full SQL parser: it inspects
  the last `ORDER BY` at parenthesis depth zero, so an `ORDER BY` inside a
  subquery, CTE, or a window function's `OVER (...)` is ignored rather than
  mistaken for the outer ordering (a query whose only ordering sits inside such a
  construct is rejected, since it has no outer `ORDER BY`). A surrounding
  identifier quote (`"OrderDate"`, `` `col` ``, `[col]`) and, when
  `snake_case_columns` is set, snake_case folding are both accounted for, so the
  same `tracking_column` that matches a row at read time also passes validation.
  The check does not interpret `UNION`; for a multi-branch `UNION` verify the
  result ordering yourself.

The connector takes the tracking value of the **last row** of each ordered batch
as the next cursor, so the cursor always matches the database's own `ORDER BY`.
The tracking column must also be:

- **Unique / strictly increasing.** The next poll resumes with a strict
  `> {last_offset}`. The connector probes one row past `batch_size`; if that row
  has the same tracking value as the last row in the batch, the entire poll
  fails before emitting messages or advancing the checkpoint. This prevents
  silent skips, but the source cannot progress until `batch_size` exceeds that
  equal-value group or the query uses a unique, strictly-increasing key. An
  auto-increment ID is ideal. Keyset pagination with a tie-break is a planned
  follow-up.
- **Monotonic under the database's own ordering.** Because the cursor is the last
  ordered row and is fed back as a bound parameter in `WHERE {tracking_column} >
  ?`, the column
  must increase monotonically under the same ordering the database applies to that
  `>` (including its collation, for text). Prefer an auto-increment ID or a
  timestamp; a case-insensitively-collated text key can order differently than its
  bytes and skip or re-read rows.
- **`NOT NULL`.** A NULL tracking value cannot be watermarked, so the connector
  errors the poll if it reads one and keeps erroring (making no progress) until
  the query is fixed. Exclude NULLs in the query, e.g. `AND {tracking_column} IS
  NOT NULL`.
- **Round-trippable string form, for timestamps.** Timestamp columns are read as
  the driver's string form and rebound through JDBC using the parameter's SQL
  type; ensure the driver emits a form it can convert back (ISO-8601 is safe; a
  locale format such as `MM/DD/YYYY` may not be).

**Example:**

```sql
-- Configuration
tracking_column = "id"
query = "SELECT * FROM users WHERE id > {last_offset} ORDER BY id"
initial_offset = "0"

-- Prepared SQL for the first poll; JDBC binds parameter 1 to 0 as the type of id
SELECT * FROM users WHERE id > ? ORDER BY id

-- After processing rows up to id=100, the SQL stays the same and parameter 1 is 100
SELECT * FROM users WHERE id > ? ORDER BY id
```

## Output Format

Each database row is converted to a JSON message:

### With Metadata (default)

```json
{
  "table_name": null,
  "operation_type": "SELECT",
  "timestamp": "2024-01-09T10:30:00Z",
  "data": {
    "id": 123,
    "name": "John Doe",
    "email": "john@example.com",
    "created_at": "2024-01-08T15:20:00"
  }
}
```

`table_name` is always `null` today: it is a reserved field, not derived
from the query. `operation_type` is always `"SELECT"`.

### Without Metadata

```json
{
  "id": 123,
  "name": "John Doe",
  "email": "john@example.com",
  "created_at": "2024-01-08T15:20:00"
}
```

## Type Mapping

JDBC SQL types are automatically mapped to JSON:

| SQL Type | JSON Type | Notes |
| ---------- | ----------- | ------- |
| BIT, BOOLEAN | boolean | - |
| TINYINT, SMALLINT, INTEGER | number | Integer |
| BIGINT | string | Emitted as a string to preserve full 64-bit precision (many JSON consumers parse numbers as f64 and would lose precision above 2^53) |
| FLOAT, REAL | number | Float |
| DOUBLE | number | Double |
| NUMERIC, DECIMAL | string | Emitted as a string to preserve arbitrary precision (e.g. money) |
| CHAR, VARCHAR, TEXT | string | - |
| DATE, TIME, TIMESTAMP | string | Driver string form |
| BINARY, VARBINARY, LONGVARBINARY | string | Base64 encoded |
| NULL | null | - |

## Runtime notes & limitations

- **Embedded JVM, one per process.** JNI permits a single `JavaVM` per OS
  process. All JDBC *source* instances in the connectors runtime share one JVM
  and must configure the same `driver_jar_path` and `jvm_options`; a later source
  with different values is rejected instead of silently using the first
  source's classpath. A JDBC source and a JDBC sink are separate shared libraries
  and **cannot both create a JVM in the same runtime process**. Run them in
  separate connectors-runtime processes.
- **Blocking I/O.** JDBC calls go through JNI and are synchronous. The fetch in
  `poll()` (and the close in `close()`) runs under `tokio::task::block_in_place`
  so it does not monopolize a shared async-runtime worker, but the work is still
  blocking; size the runtime and `poll_interval`/`batch_size` accordingly.
- **Bulk mode has no pagination beyond `batch_size`, and fails closed.** Row
  limiting uses JDBC `setMaxRows`. In bulk mode the connector probes with
  `batch_size + 1` rows and, if the result set is larger than `batch_size`,
  **errors the poll instead of syncing a truncated subset**. For tables larger
  than a batch, raise `batch_size` to cover the full result, or use incremental
  mode with an ordered `tracking_column`. (Full cross-database OFFSET pagination
  is a planned follow-up.)
- **Incremental boundaries fail closed on tied tracking values.** The connector
  fetches one probe row beyond `batch_size`. If the probe and last in-batch row
  share a tracking value, no messages are emitted and no cursor is staged. Raise
  `batch_size` above the tie group or use a unique tracking column.
- **Fetched-batch delivery is at-least-once.** The offset advanced by a poll is
  only *staged*; it is committed after the runtime reports that the batch was
  both sent and its checkpoint durably persisted (`SourceBatchResult::Ack`). If
  either step fails (`Nack`), the staged offset is discarded and the next poll
  rebuilds the same query from the committed offset, so the batch is **re-read
  rather than skipped** - for a transient in-process send failure as well as for
  a crash or restart. Rows can therefore be delivered more than once (message
  IDs are assigned by the producer and are not stable across a replay, so
  downstream consumers must dedupe on a business key if they need exactly-once);
  send or checkpoint failures do not silently drop an already-fetched batch.
  Five consecutive Nacks stop the source; an operator must restart it after the
  underlying send or checkpoint failure is fixed. The separate tracking-column
  uniqueness requirement above still applies while fetching rows from the
  database.
- **Connection recovery.** The connection is validated with `Connection.isValid`
  each poll and transparently re-established (closing the old handle) if it has
  dropped. The check runs on the shared `block_in_place` worker, so its timeout
  (`connection_timeout_ms`) is intentionally converted to whole seconds and
  capped at 5s: a dead connection must not block the worker for tens of seconds.
- **Bounded query execution.** Every prepared statement receives
  `query_timeout_ms` through `Statement.setQueryTimeout`. JDBC drivers implement
  cancellation differently, so verify timeout behavior for drivers outside the
  PostgreSQL integration matrix.
- **`SQLState` classification is informational today.** Query failures are
  classified into transient vs permanent error variants, but the runtime does
  not yet apply differentiated backoff based on that distinction; it currently
  shapes the error variant and the log message only.
- **Credentials reach the JVM heap unzeroed.** `password` is held as a
  `SecretString` on the Rust side, but the JDBC API takes a `java.lang.String`,
  so the password is copied onto the JVM heap as an ordinary (non-zeroed) string
  for the lifetime of the connection. This is inherent to the JDBC surface and
  is an accepted risk.
- **`SecretString` redaction is not end-to-end.** `jdbc_url`/`password` are typed
  as `SecretString`, and this connector's `Debug` output and startup logs redact
  them. That redaction does **not** cover the connectors runtime's generic
  config surface: the `GET /sources/{key}/configs/plugin` control-API endpoint
  and the runtime's `trace`-level config logging emit the raw, untyped
  `plugin_config` (credentials included), the same as every other connector. Do
  not expose the control API to untrusted callers and do not run the runtime at
  `trace` level in production. This is a known runtime-wide limitation shared by
  all connectors, not something this connector can address on its own.

### Credential precedence

Provide credentials **either** via `username` + `password` **or** embedded in the
`jdbc_url`, not both:

- `username` and `password` must be **both set** (separate-credential auth) or
  **both unset** (URL-embedded credentials). A half-set pair is rejected at
  `open()`.
- When both `username`/`password` and URL-embedded credentials are present, the
  driver decides precedence (typically the explicit `getConnection(url, user,
  pass)` arguments win). Avoid the ambiguity by using only one method.

## Troubleshooting

### Connection Failures

**Error**: "Failed to create JDBC connection"

**Solution**:

- Verify JDBC URL format for your database
- Check username/password
- Ensure database server is accessible
- Verify firewall rules

### Driver Not Found

**Error**: "Failed to find driver class"

**Solution**:

- Verify `driver_jar_path` points to correct JAR file
- Check `driver_class` name matches your JDBC driver
- Ensure JAR file has read permissions

### JVM Issues

**Error**: "Failed to create JVM"

**Solution**:

- Ensure Java is installed: `java -version`
- Increase JVM memory:

  ```toml
  jvm_options = ["-Xmx1g", "-Xms512m"]
  ```

### No Data Being Fetched

**Check**:

- Verify query returns results when run directly in database
- Check `initial_offset` value
- Review connector logs for errors
- Ensure `tracking_column` exists in query result

## Performance Tuning

### Optimize Batch Size

```toml
# Small batches for low latency
batch_size = 100
poll_interval = "5s"

# Large batches for throughput
batch_size = 10000
poll_interval = "1m"
```

### JVM Memory Tuning

```toml
jvm_options = [
    "-Xmx1g",           # Maximum heap size
    "-Xms512m",         # Initial heap size
    "-XX:+UseG1GC"      # Use G1 garbage collector
]
```

### Query Optimization

- Add indexes on tracking columns
- Use efficient WHERE clauses
- Avoid SELECT * in production (specify columns)
- Consider database-specific optimizations

## Connection String Formats

### MySQL

```toml
# Option 1: Separate credentials
jdbc_url = "jdbc:mysql://localhost:3306/mydb"
username = "user"
password = "pass"

# Option 2: Embedded in URL
jdbc_url = "jdbc:mysql://user:pass@localhost:3306/mydb"
```

### PostgreSQL

```toml
# Option 1: Separate credentials
jdbc_url = "jdbc:postgresql://localhost:5432/mydb"
username = "user"
password = "pass"

# Option 2: Embedded in URL
jdbc_url = "jdbc:postgresql://localhost:5432/mydb?user=myuser&password=mypass"
```

### Oracle

```toml
# Option 1: Separate credentials
jdbc_url = "jdbc:oracle:thin:@localhost:1521:XE"
username = "system"
password = "oracle"

# Option 2: Embedded in URL (Oracle uses @ for host)
jdbc_url = "jdbc:oracle:thin:system/oracle@localhost:1521:XE"
```

### SQL Server

```toml
# Option 1: Separate credentials
jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=mydb"
username = "sa"
password = "YourPassword123"

# Option 2: Embedded in URL
jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=mydb;user=sa;password=YourPassword123"
```

### H2 (In-Memory)

```toml
# No credentials needed for in-memory
jdbc_url = "jdbc:h2:mem:testdb"

# Or with file-based
jdbc_url = "jdbc:h2:file:/data/mydb;USER=sa;PASSWORD=sa"
```

## Mode Comparison

### Incremental Mode

Uses standard JDBC prepared statements and requires an orderable tracking
column. PostgreSQL is integration-tested; validate this mode with other drivers.

```toml
mode = "incremental"
tracking_column = "updated_at"  # or "id", "created_at", etc.
query = "SELECT * FROM table WHERE {tracking_column} > {last_offset} ORDER BY {tracking_column}"
initial_offset = "2024-01-01 00:00:00"  # value appropriate for the column type
```

**Benefits:**

- Avoids re-reading rows below the tracked offset (at-least-once, not exactly-once)
- Tracks offset automatically
- Efficient for large tables
- Works with timestamps, IDs, or other orderable, non-null columns; a unique
  strictly increasing value avoids fail-closed tie boundaries

**Database Examples** (the query must order by the tracking column; a unique,
strictly-increasing key like an auto-increment ID is safest, see the tracking
column requirements above):

- MySQL: `WHERE updated_at > {last_offset} ORDER BY updated_at` (timestamp; if `batch_size` splits a same-timestamp group, the poll fails closed until the batch is enlarged or a unique id is tracked)
- Oracle: `WHERE id > {last_offset} ORDER BY id` (use a monotonic key; `ROWNUM` is not a valid tracking column)
- SQL Server: `WHERE updated_at > {last_offset} ORDER BY updated_at` (timestamp; same fail-closed boundary behavior as MySQL)
- PostgreSQL: `WHERE id > {last_offset} ORDER BY id`

### Bulk Mode

Uses standard JDBC result-set APIs and requires no tracking column. PostgreSQL
is integration-tested; validate query and type behavior with other drivers.

```toml
mode = "bulk"
query = "SELECT * FROM table"  # Any valid SELECT query
```

**Benefits:**

- No tracking column needed
- Works with any SELECT query
- Good for snapshots
- Supports complex queries with JOINs, aggregations, etc.

**Limitation:** the result set is capped at `batch_size` rows (`setMaxRows`) with
no pagination beyond it. Rather than sync a truncated subset, bulk mode **fails
closed**: if the result set is larger than `batch_size` the poll errors. Raise
`batch_size` to cover the full table, or use incremental mode, for large tables.

**Use Cases:**

- Initial data load
- Periodic full snapshots (that fit within `batch_size`)
- Complex analytical queries
- Tables without tracking columns
