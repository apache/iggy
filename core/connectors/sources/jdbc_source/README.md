# JDBC Source Connector

A generic JDBC source connector for Iggy that supports any JDBC-compliant database including MySQL, PostgreSQL, Oracle, SQL Server, H2, Derby, and more.

## Overview

This connector reads data from relational databases using JDBC (Java Database Connectivity) and publishes it as messages to Iggy streams. It supports both bulk and incremental data synchronization modes.

## Features

- **Universal Database Support**: Works with any database that has a JDBC driver
- **Incremental Sync**: Track changes using timestamps or auto-increment IDs
- **Bulk Mode**: Full table scans for initial loads or snapshots
- **Type Mapping**: Automatic conversion of SQL types to JSON
- **Configurable Polling**: Control how frequently data is fetched
- **State Management**: Automatically tracks offsets to prevent duplicate reads
- **Flexible Queries**: Support for custom SQL queries with placeholders

## Supported Databases

**ALL JDBC-compliant databases are supported for both bulk and incremental modes:**

- MySQL / MariaDB
- PostgreSQL
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
- Any other JDBC-compliant database

**Key Point:** The JDBC connector provides a **single, universal implementation** that works with all these databases. You don't need separate connectors for MySQL, Oracle, etc. Just swap the JDBC driver JAR and connection string!

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
tracking_column = "updated_at"
initial_offset = "2024-01-01 00:00:00"
mode = "incremental"
snake_case_columns = true
include_metadata = true

[[streams]]
stream = "ecommerce"
topic = "orders"
partition_id = 1
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
| `driver_jar_path` | string | Yes | - | Path to JDBC driver JAR file |
| `username` | string | No | - | Database username (optional if in jdbc_url) |
| `password` | string | No | - | Database password (optional if in jdbc_url) |
| `query` | string | Yes | - | SQL query to execute (supports `{last_offset}` placeholder) |
| `poll_interval` | duration | Yes | - | How often to poll (e.g., "30s", "5m", "1h") |
| `batch_size` | u32 | No | 1000 | Maximum rows to fetch per poll |
| `tracking_column` | string | No | - | Column to track for incremental reads |
| `initial_offset` | string | No | - | Starting offset value for first poll |
| `mode` | string | No | "incremental" | Sync mode: "incremental" or "bulk" (bulk works with ALL databases) |
| `enable_connection_pool` | bool | No | false | Enable HikariCP connection pooling |
| `max_pool_size` | u32 | No | 10 | Maximum connections in pool |
| `min_idle` | u32 | No | 2 | Minimum idle connections |
| `connection_timeout_ms` | u64 | No | 30000 | Connection timeout in milliseconds |
| `jvm_options` | array | No | [] | Custom JVM options (e.g., ["-Xmx1g"]) |
| `snake_case_columns` | bool | No | false | Convert column names to snake_case |
| `include_metadata` | bool | No | true | Include metadata (table, operation, timestamp) |

## Query Placeholders

The `query` parameter supports placeholders for dynamic queries:

- `{last_offset}`: Replaced with the last tracked offset value
- Automatically wrapped in quotes for string types

**Example:**

```sql
-- Configuration
tracking_column = "id"
query = "SELECT * FROM users WHERE id > {last_offset} ORDER BY id"

-- First poll (no offset yet)
SELECT * FROM users WHERE id > '0' ORDER BY id

-- After processing rows up to id=100
SELECT * FROM users WHERE id > '100' ORDER BY id
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
| BIGINT | number | Long integer (values above 2^53 may lose precision in JSON consumers that parse numbers as f64) |
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
  (the first instance's `jvm_options`/classpath win). A JDBC source and a JDBC
  sink are separate shared libraries and **cannot both create a JVM in the same
  runtime process** — run them in separate connectors-runtime processes.
- **Blocking I/O.** JDBC calls go through JNI and are synchronous; each `poll()`
  runs blocking work on the runtime worker thread. Size the runtime and
  `poll_interval`/`batch_size` accordingly.
- **Connection recovery.** In direct (non-pooled) mode the connection is
  validated with `Connection.isValid` each poll and transparently re-established
  if it has dropped. In pooled mode each poll borrows and returns a connection
  from HikariCP, which validates connections itself.

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

### Incremental Mode (Universal)

**Works with ALL databases** - requires only a tracking column:

```toml
mode = "incremental"
tracking_column = "updated_at"  # or "id", "created_at", etc.
query = "SELECT * FROM table WHERE {tracking_column} > {last_offset} ORDER BY {tracking_column}"
```

**Benefits:**

- Prevents duplicate reads
- Tracks offset automatically
- Efficient for large tables
- Works with timestamps, IDs, or any orderable column

**Database Examples:**

- MySQL: `WHERE updated_at > {last_offset}`
- Oracle: `WHERE ROWNUM > {last_offset}` or use ID
- SQL Server: `WHERE updated_at > {last_offset}`
- PostgreSQL: `WHERE id > {last_offset}`

### Bulk Mode (Universal)

**Works with ALL databases** - no special requirements:

```toml
mode = "bulk"
query = "SELECT * FROM table"  # Any valid SELECT query
```

**Benefits:**

- No tracking column needed
- Works with any SELECT query
- Good for snapshots
- Supports complex queries with JOINs, aggregations, etc.

**Use Cases:**

- Initial data load
- Periodic full snapshots
- Complex analytical queries
- Tables without tracking columns
