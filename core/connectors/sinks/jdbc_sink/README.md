<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# JDBC Sink Connector

A generic sink connector that consumes messages from Iggy topics and writes
them into any JDBC-compliant database (PostgreSQL, MySQL, Oracle, SQL Server,
H2, and others). It is the write-side counterpart to the
[JDBC source connector](../../sources/jdbc_source/README.md) and uses the same
embedded-JVM/JNI bridge, so a single JDBC driver JAR serves both directions.

## How it works

The connector starts an embedded JVM (loading your JDBC driver JAR onto the
classpath), opens a connection (optionally via a HikariCP pool), and on every
batch of consumed messages performs a **batched `INSERT`** using a JDBC
`PreparedStatement` (`addBatch` / `executeBatch`).

### Write semantics

**INSERT-only.** Each message becomes one row. This matches the behaviour of
the other Iggy sink connectors (PostgreSQL, MongoDB, Doris, Elasticsearch) —
none of which perform upserts or deletes. If you need upsert/merge semantics,
pre-create the table with an appropriate constraint and handle conflicts at the
database level, or open a feature request.

### Row layout

Each row contains the message `id` plus the payload, and (optionally) Iggy
metadata columns:

| Column                  | Type (auto-create)   | Included when                     |
|-------------------------|----------------------|-----------------------------------|
| `id`                    | `VARCHAR(40)`        | always (message id as decimal)    |
| `iggy_offset`           | `BIGINT`             | `include_metadata = true`         |
| `iggy_timestamp`        | `BIGINT` (µs epoch)  | `include_metadata = true`         |
| `iggy_stream`           | `VARCHAR(255)`       | `include_metadata = true`         |
| `iggy_topic`            | `VARCHAR(255)`       | `include_metadata = true`         |
| `iggy_partition_id`     | `INTEGER`            | `include_metadata = true`         |
| `iggy_checksum`         | `BIGINT`             | `include_checksum = true`         |
| `iggy_origin_timestamp` | `BIGINT` (µs epoch)  | `include_origin_timestamp = true` |
| `payload`               | `TEXT` / `VARBINARY` | always                            |

Timestamps are stored as epoch microseconds (`BIGINT`) for cross-database
portability — `BIGINT` and `setLong` behave identically across every JDBC
driver (including SQL Server, where the `TIMESTAMP` keyword is a rowversion
type rather than a datetime), and the value is lossless with no timezone
coercion.

To read the timestamp columns as native dates, convert at query time:

```sql
-- PostgreSQL
SELECT to_timestamp(iggy_timestamp / 1000000.0) AS ts FROM iggy_events;

-- MySQL
SELECT FROM_UNIXTIME(iggy_timestamp / 1000000) AS ts FROM iggy_events;

-- H2
SELECT TIMESTAMP_WITH_TIME_ZONE FROM (
    SELECT DATEADD('MICROSECOND', iggy_timestamp, TIMESTAMP '1970-01-01 00:00:00') FROM iggy_events
);
```

## Configuration

| Field                     | Type           | Required | Default     | Description                                                  |
|---------------------------|----------------|----------|-------------|--------------------------------------------------------------|
| `jdbc_url`                | string         | yes      | —           | JDBC connection URL (masked in logs)                         |
| `driver_class`            | string         | yes      | —           | JDBC driver class, e.g. `org.postgresql.Driver`              |
| `driver_jar_path`         | string         | yes      | —           | Absolute path to the driver JAR                              |
| `target_table`            | string         | yes      | —           | Destination table                                            |
| `username`                | string         | no       | —           | DB username (or embed in URL)                                |
| `password`                | string         | no       | —           | DB password (masked in logs)                                 |
| `batch_size`              | int            | no       | `100`       | Messages per `INSERT` batch                                  |
| `auto_create_table`       | bool           | no       | `false`     | Create the table on open if missing                          |
| `include_metadata`        | bool           | no       | `true`      | Add offset/timestamp/stream/topic/partition columns          |
| `include_checksum`        | bool           | no       | `true`      | Add `iggy_checksum` column                                   |
| `include_origin_timestamp`| bool           | no       | `true`      | Add `iggy_origin_timestamp` column                           |
| `payload_format`          | string         | no       | `"text"`    | `text`, `json` (validated), or `bytes`                       |
| `payload_column`          | string         | no       | `"payload"` | Name of the payload column                                   |
| `verbose_logging`         | bool           | no       | `false`     | Log per-batch progress at INFO                               |
| `max_retries`             | int            | no       | `3`         | Retry attempts for a failing batch                           |
| `retry_delay`             | string         | no       | `"1s"`      | Delay between retries (humantime)                            |
| `jvm_options`             | array<string>  | no       | `[]`        | Extra JVM flags, e.g. `["-Xmx512m"]`                         |
| `enable_connection_pool`  | bool           | no       | `false`     | Use a HikariCP pool                                          |
| `max_pool_size`           | int            | no       | `10`        | Max pool size (pool mode)                                    |
| `min_idle`                | int            | no       | `2`         | Min idle connections (pool mode)                             |
| `connection_timeout_ms`   | int            | no       | `30000`     | Connection timeout (pool mode)                               |

### Example (PostgreSQL)

```toml
type = "sink"
key = "jdbc_sink_pg"
enabled = true
name = "JDBC PostgreSQL Sink"
path = "target/release/libiggy_connector_jdbc_sink"
plugin_config_format = "toml"

[[streams]]
stream = "test"
topic = "events"
schema = "json"

[plugin_config]
jdbc_url = "jdbc:postgresql://localhost:5432/mydb"
driver_class = "org.postgresql.Driver"
driver_jar_path = "/tmp/jdbc-drivers/postgresql-42.7.1.jar"
username = "postgres"
password = "postgres"
target_table = "iggy_events"
auto_create_table = true
payload_format = "json"
batch_size = 100
```

See [`example_config/connectors`](../../runtime/example_config/connectors) for
ready-to-use PostgreSQL and MySQL configurations.

## Security notes

- Never commit credentials. Prefer environment-variable overrides
  (`IGGY_CONNECTORS_SINK_<KEY>_PLUGIN_CONFIG_*`) or embed credentials in a
  secret-managed `jdbc_url`.
- `jdbc_url` and `password` are wrapped in `SecretString`; the connector masks
  passwords in all log output and never serializes secrets back out.

## Building

```bash
cargo build --release -p iggy_connector_jdbc_sink
# produces target/release/libiggy_connector_jdbc_sink.{so,dylib,dll}
```

The connector requires a JVM (JDK 8+) at runtime. Point `driver_jar_path` at the
JDBC driver for your database; for pooled mode the JAR (or classpath) must also
provide HikariCP.

## Runtime notes & limitations

- **Embedded JVM, one per process.** JNI permits a single `JavaVM` per OS
  process. All JDBC *sink* instances in the connectors runtime share one JVM
  (the first instance's `jvm_options`/classpath win). A JDBC source and a JDBC
  sink are separate shared libraries and **cannot both create a JVM in the same
  runtime process** — run them in separate connectors-runtime processes.
- **Blocking I/O.** JDBC calls go through JNI and are synchronous; each
  `consume()` runs blocking work on the runtime worker thread (the same model as
  the JDBC source). Size the runtime / batch sizes accordingly.
- **Error handling.** Transient failures (SQLState class `08`/`40`/`53`/`57`/`58`
  — connectivity, deadlock, resource/operator) are retried up to `max_retries`
  and then surfaced so a restart can re-process the batch. Permanent failures
  (constraint/syntax/data errors, e.g. SQLState `22`/`23`/`42`) are logged,
  counted, and the offending batch is **skipped** so one poison batch does not
  permanently halt the sink.
