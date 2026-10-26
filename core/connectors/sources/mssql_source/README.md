# Apache Iggy MSSQL Source Connector (`iggy_connector_mssql_source`)

The **MSSQL source connector** streams row changes from Microsoft SQL Server into Apache Iggy. It reads SQL Server
[change data capture (CDC)](https://learn.microsoft.com/sql/relational-databases/track-changes/about-change-data-capture-sql-server)
change tables with the [`tiberius`](https://crates.io/crates/tiberius) driver, merges every configured table into one
stream in commit order, and emits one JSON event per insert, update or delete.

---

## Architecture Overview

```text
  [ Applications ] ── INSERT / UPDATE / DELETE ──► [ SQL Server tables ]
                                                          │
                                                          │ transaction log
                                                          ▼
                                     [ CDC capture job (SQL Server Agent) ]
                                                          │
                                                          ▼
                                     [ cdc.<capture_instance>_CT change tables ]
                                                          │
                                                          │ fn_cdc_get_all_changes_<instance>
                                                          ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                     IGGY CONNECTORS RUNTIME PROCESS                      │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │  MSSQL source plugin (libiggy_connector_mssql_source)              │  │
│  │  • Reads change keys from every table, cuts one batch in commit    │  │
│  │    order, then reads only the rows inside the batch                │  │
│  │  • Pairs update before and after images into one event             │  │
│  │  • Stages its checkpoint and commits it only after Iggy ACKs       │  │
│  └─────────────────────────────────┬──────────────────────────────────┘  │
│                                    │ C-ABI FFI boundary                  │
│  ┌─────────────────────────────────▼──────────────────────────────────┐  │
│  │  Runtime core: transforms, state storage, Iggy producer            │  │
│  └─────────────────────────────────┬──────────────────────────────────┘  │
└────────────────────────────────────┼─────────────────────────────────────┘
                                     ▼
                     [ Iggy stream / topic, one partition ]
```

The connector never writes to SQL Server. It does not enable CDC, create capture instances or run cleanup.

### Availability groups

Availability group replicas share one recovery fork and one LSN sequence, so after a failover the connector resumes
on the new primary without a state reset. Point `Server=` at the listener. CDC jobs live in `msdb` and do not fail
over: run `sys.sp_cdc_add_job` for `capture` and `cleanup` on every replica that can become primary. Without a capture
job the connector stays `Running` but sees no new changes. The integration tests do not cover availability groups.

---

## Features

- **Commit order across tables.** Changes from all configured tables are merged by commit LSN, then by sequence
  value. A transaction that touches two tables arrives in the order SQL Server applied it.
- **At-least-once delivery with stable ids.** The checkpoint advances only after Iggy accepts a batch. Every event
  carries a deterministic message id, so a batch read again after a failure has the same ids.
- **Bounded batches.** `batch_size` limits the change rows per poll. A large transaction is split across batches
  without loss.
- **Update pairing.** The before and after images of an update become one event. A primary key update stays a
  delete plus an insert, as SQL Server records it.
- **Honest before images.** Values that SQL Server did not record are listed in `unavailable_columns` instead of
  being reported as `null`.
- **Loss detection.** The connector stops instead of skipping changes when CDC cleanup has deleted rows it has not
  read, when a capture instance disappears, or when the database was restored or re-created.
- **Capture instance rotation.** A second capture instance created for a schema change is picked up at restart
  without loss, if the order under [Capture Instance Rotation](#capture-instance-rotation) is followed.

---

## CDC Setup and Permissions

A DBA enables CDC before the connector starts. The connector checks the setup at startup and fails with a message
that names the problem.

```sql
USE shop;
EXEC sys.sp_cdc_enable_db;
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'users',
    @role_name     = NULL;   -- or a gating role, see below
```

SQL Server Agent must be running. Without it the capture job never runs and the change tables stay empty. In the
official container image, set `MSSQL_AGENT_ENABLED=true`.

The connector login needs:

- `SELECT` on the captured columns of every source table. `fn_cdc_get_all_changes_<instance>` requires it for users
  outside `db_owner`.
- Membership in the capture instance's gating role, when `@role_name` was set.
- `SELECT` on the `cdc` schema, to read `cdc.change_tables` and `cdc.captured_columns`.
- `VIEW DATABASE STATE`, to read `sys.database_recovery_status` for the restore check.

The integration tests run as `sa`. This minimal set has not been verified against a restricted login yet.

---

## Configuration Options (`[plugin_config]`)

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `connection_string` | string | required | ADO.NET connection string, including `Database=`. Never logged. |
| `tables` | array of strings | required | Unquoted `schema.table` names, for example `["dbo.users", "sales.order items"]`. |
| `poll_interval` | duration | `"1s"` | Delay between polls. Also the reconnect backoff. |
| `batch_size` | integer | `1000` | Target change rows per batch. A batch can exceed it to finish one change. |
| `query_timeout` | duration | `"30s"` | Upper bound for one query. The connection is dropped when it expires. |
| `start_position` | `"earliest"` or `"latest"` | `"earliest"` | Where to start when there is no saved checkpoint. |
| `verbose_logging` | boolean | `false` | Logs every batch at `info` instead of `debug`. |

Without a port the connection uses 1433. A named instance (`Server=host\instance`) needs its port, as in
`Server=tcp:host,port`. Without one it needs a SQL Browser lookup, which is not supported, so startup rejects it.

`tables` must not use brackets. Names are resolved by SQL Server, so `dbo.Users` and `dbo.users` are the same table
under a case-insensitive collation, and listing both fails startup.

`start_position = "earliest"` reads every change still retained by CDC. `"latest"` skips retained history and starts
with changes committed after the first start. Both apply only when no checkpoint is saved.

The `latest` position is saved with the first acknowledged batch. If the connector stops before that, the next start
picks a new `latest` position, and changes committed in between are skipped.

---

## Configuration Examples

### 1. Basic configuration

```toml
type = "source"
key = "mssql"
enabled = true
version = 1
name = "MSSQL source"
path = "target/release/libiggy_connector_mssql_source"
plugin_config_format = "json"

[[streams]]
stream = "cdc"
topic = "mssql"
schema = "json"
batch_length = 1000
linger_time = "5ms"

[plugin_config]
connection_string = "Server=tcp:sql.example.com,1433;Database=shop;User Id=iggy_cdc;Password=replace-me;Encrypt=true"
tables = ["dbo.users", "dbo.orders"]
poll_interval = "1s"
batch_size = 1000
start_position = "earliest"
```

### 2. Skip retained history on the first start

```toml
[plugin_config]
connection_string = "Server=tcp:sql.example.com,1433;Database=shop;User Id=iggy_cdc;Password=replace-me;Encrypt=true"
tables = ["dbo.users"]
start_position = "latest"
```

### 3. Local development against a self-signed certificate

```toml
[plugin_config]
connection_string = "Server=tcp:localhost,1433;Database=shop;User Id=sa;Password=replace-me;TrustServerCertificate=true"
tables = ["dbo.users"]
poll_interval = "100ms"
```

---

## TLS

TLS is set in the connection string. `Encrypt=true` encrypts the connection and verifies the server certificate.
`Encrypt=strict` uses TDS 8.0 strict encryption, which the build supports. `TrustServerCertificate=true` turns off
certificate verification. Use it only for local development, never in production.

The driver uses rustls. It does not link OpenSSL or the platform TLS library.

---

## Topic and Ordering

Events are in commit order only on a **one-partition topic**. The runtime producer balances messages across
partitions, so a topic with more partitions reorders events. A connector config has no partition setting. Let the
runtime create the topic, which creates one partition. A topic you create yourself needs one partition to keep the
order, and `durability=persisted`. The runtime refuses to start the source without `durability=persisted`, but it
does not check the partition count.

---

## Event Format

Each message payload is one JSON object (`schema = "json"`):

```json
{
  "source": { "database": "shop", "schema": "dbo", "table": "users" },
  "op": "update",
  "lsn": "0x0000002C000002400003",
  "seqval": "0x0000002C000002400002",
  "commit_time": "2026-10-20T22:42:52.530+00:00",
  "before": { "id": 1, "balance": "10.50" },
  "after": { "id": 1, "balance": "11.00", "note": "vip" },
  "unavailable_columns": ["note"]
}
```

| Field | Meaning |
| --- | --- |
| `source` | Database from `DB_NAME()`, and the canonical schema and table names. |
| `op` | `insert`, `update` or `delete`. |
| `lsn` | Commit LSN. Every change in one transaction shares it. |
| `seqval` | Order of the change inside its transaction. |
| `commit_time` | Commit time from `sys.fn_cdc_map_lsn_to_time`, with the UTC offset in effect at commit time. |
| `before` | Row before the change. `null` for inserts. |
| `after` | Row after the change. `null` for deletes. |
| `unavailable_columns` | Columns SQL Server did not record. They are left out of `before`. |

`unavailable_columns` lists:

- in an update's before image, every LOB column (`(max)` types, `xml`, `geography`, `geometry`, `text`, `ntext`,
  `image`) that the update did not change. SQL Server stores NULL for them.
- in every before image and every delete, `text`, `ntext` and `image` columns.

NULL column values are explicit in the images. The connector sets no message headers.

The message id is a hash of the database, table, commit LSN, sequence value and operation. Consumers can use it to
drop duplicates after a redelivery.

### Value mapping

| SQL Server type | JSON |
| --- | --- |
| `tinyint`, `smallint`, `int`, `bigint` | number |
| `bit` | boolean |
| `decimal`, `numeric`, `money`, `smallmoney` | string with full scale, for example `"10.50"` |
| `real`, `float` | number |
| `char`, `varchar`, `nchar`, `nvarchar`, `text`, `ntext`, `xml` | string, padding kept |
| `date` | `"2026-10-20"` |
| `time`, `datetime`, `datetime2`, `smalldatetime` | ISO 8601 without offset, up to 7 fraction digits |
| `datetimeoffset` | ISO 8601 with offset |
| `uniqueidentifier` | lowercase string |
| `binary`, `varbinary`, `image`, `rowversion` | base64 string |
| `geography`, `geometry`, `hierarchyid` | base64 string of the serialized value |

Computed columns are left out, because CDC always records them as NULL. A captured column of any other type fails
startup with a message that names it. That includes `sql_variant`, because a string cast truncates long values and
loses float and datetime precision. Leave such columns out with `@captured_column_list`.

### Using `unwrap_envelope`

The `unwrap_envelope` transform with `field = "after"` gives a sink the flat row, but it drops `op`, and a delete
becomes a JSON `null` payload. A sink that must apply deletes needs the full event.

---

## Environment Variable Overrides

Every `[plugin_config]` option can be set through an environment variable. Use this for the connection string, so
the password stays out of configuration files:

```bash
export IGGY_CONNECTORS_SOURCE_MSSQL_PLUGIN_CONFIG_CONNECTION_STRING="Server=tcp:sql,1433;Database=shop;User Id=iggy_cdc;Password=secret"
export IGGY_CONNECTORS_SOURCE_MSSQL_PLUGIN_CONFIG_TABLES='[dbo.users,dbo.orders]'
export IGGY_CONNECTORS_SOURCE_MSSQL_PLUGIN_CONFIG_BATCH_SIZE=500
```

---

## Redelivery and Acknowledgment

1. `poll()` reads one batch and stages a candidate checkpoint. It never commits it.
2. The runtime sends the batch to Iggy and saves the staged state.
3. On ACK the connector commits the candidate. On NACK it drops the candidate, and the next poll reads the same
   changes again with the same message ids.

The NACK limit is turned off. CDC rows stay in SQL Server until retention cleanup, so an Iggy outage only pauses the
connector. It resumes when Iggy is back, without a restart. The same applies to a transform or encode error that
fails every time: that batch is NACKed and read again forever, so watch the runtime's error count. So does a
state save that keeps failing, and then the same batch reaches Iggy again on every attempt, as duplicates.

When the connector is caught up, it moves its checkpoint to the newest LSN in the database, even with no changes for
its tables. This keeps the checkpoint ahead of retention cleanup on quiet tables.

Delivery is at least once. A transaction larger than `batch_size` is split across batches, so consumers can see part
of a transaction before the rest. After a crash between two such batches, the first part arrives again.

---

## Failure Modes

The source stops in two ways: `open()` fails at startup, or a running source stages a stop and the runtime shows the
connector in the `Error` state. The log names the cause.

| Failure | Result |
| --- | --- |
| Invalid configuration, login failure, CDC not enabled, table missing or not captured, unsupported column type, two names for one table | Startup fails |
| Saved state cannot be decoded | Startup fails |
| CDC cleanup deleted changes the connector has not read | Startup fails, or a running source stops |
| Capture instance dropped | Startup fails if no other instance holds every needed change, or a running source stops |
| Database restored or re-created (new `recovery_fork_guid`, or checkpoint above the newest LSN) | Startup fails, or a running source stops |
| Database renamed | Startup fails. The saved state names the old database, so a rename needs a state reset |
| CDC disabled on the database | Startup fails, or a running source stops |
| Connection lost, query timeout | Logged. The next poll reconnects after `poll_interval`. If timeouts keep repeating, raise `query_timeout` or lower `batch_size` |
| Deadlock, permission error, other server error | Logged. The next poll retries |
| A value cannot be converted or decoded, for example an unpaired UTF-16 surrogate in an `nvarchar` | Logged with the table and LSN. Progress waits at that change, and the status stays `Running` |

### Recovering from a stop

A gap or a restored database cannot be repaired in place. Reset the connector's saved state and restart. With the file
state backend, that means deleting `source_<key>.state` in the runtime state directory. With `start_position =
"earliest"`, the connector then reads all retained history again, so consumers see changes they already have.

### A change that cannot be converted

The connector reads it again once a minute and logs it with its age at `error` each time. Change-table rows never
change, so faster retries would not help. The status stays `Running`, so alert on that log line. The whole batch that
holds the bad change waits, not only that change. A change whose JSON is larger than Iggy's 64 MB message limit is
handled the same way. That check covers the JSON the connector produces, before any transforms. A transform that makes a
payload larger is not covered. Nothing is lost until CDC retention passes that change (three days by default, set with
`sys.sp_cdc_change_job @retention`). Then cleanup deletes it, and the next retry, within a minute, stops the source with
a gap. Fix the data or the connector before the retention deadline.

---

## Capture Instance Rotation

A schema change needs a new capture instance. SQL Server allows two per table. To switch without loss:

1. Create `_v2` with `sys.sp_cdc_enable_table @capture_instance = N'dbo_users_v2'`.
2. Commit at least one captured change in the database, and let the connector acknowledge a batch after it.
   Also make sure that no transaction on the table is still open from before `_v2` was created.
3. Stop the connector. A running connector stops in the `Error` state when its instance is dropped.
4. Drop `_v1` with `sys.sp_cdc_disable_table`.
5. Start the connector. It switches to `_v2` at startup.

Dropping `_v1` before step 2 leaves no way to prove that no change was missed, and startup fails. The connector
switches instances only at startup. Event shapes can change at the switch, because `_v2` can capture other columns.

---

## Limitations

- Columns outside `@captured_column_list`, and columns added after the capture instance was created, are not
  captured. A captured column dropped from the table reads as NULL from then on.
- A column widened to `(max)` after the instance was created can be reported wrongly in `unavailable_columns`.
- A table added to `tables` later starts at the current checkpoint. Its older retained history is skipped.
- On SQL Server 2019 and older, `commit_time` uses today's UTC offset for every change, so it is off by the DST
  difference for a change committed on the other side of a DST switch. SQL Server 2022 applies the offset in effect
  at commit time. Run older servers in UTC to avoid this.
- One database per connector. Use one connector per database.
- A schema or table name that contains a dot cannot be configured, because `tables` splits `schema.table` on the dot.
- A batch holds up to `batch_size` full rows in memory. Lower `batch_size` for tables with wide `(max)` columns.
- No initial snapshot of existing rows. Only changes captured by CDC are streamed.
- Azure SQL Database redirects are not supported.
