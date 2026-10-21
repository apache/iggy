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

# QuestDB Sink Connector

Writes Apache Iggy messages to [QuestDB](https://questdb.com) over QWP,
QuestDB's native binary protocol, on a WebSocket transport. The same build
works with QuestDB open source and QuestDB Enterprise. Enterprise features are
enabled through the connect string.

Requires QuestDB 10.0 or newer.

## Configuration

```toml
[plugin_config]
connection_string = "ws::addr=localhost:9000;"
table = "iggy_events"
timestamp_source = "message"
timestamp_unit = "auto"
symbol_columns = ["region", "device"]
uuid_columns = []
integer_columns = []
include_stream_column = true
include_topic_column = true
include_partition_column = false
include_offset_column = false
include_headers = false
ack_level = "ok"
flush_timeout = "30s"
batch_size = 1000
max_flush_bytes = 1000000
log_rejected_payload = false
numbers_as_double = true
verbose_logging = false
```

| Field | Default | Description |
| ----- | ------- | ----------- |
| `connection_string` | required | QuestDB connect string, `ws::` or `wss::`. Treated as a secret. |
| `table` | required | Target table. |
| `timestamp_source` | `message` | `message`, `origin`, `payload`, or `server`. |
| `timestamp_field` | none | Payload field holding the timestamp. Required when `timestamp_source = "payload"`. |
| `timestamp_unit` | `auto` | Unit of the payload timestamp: `auto`, `seconds`, `millis`, `micros`, `nanos`. `auto` infers the unit from the magnitude. Used only when `timestamp_source = "payload"`, but an invalid value is rejected with any source. |
| `symbol_columns` | `[]` | Payload fields stored as `SYMBOL`. A record with an object or array in one of these fields is rejected. |
| `uuid_columns` | `[]` | Payload fields holding RFC-4122 strings, stored as `UUID`. A record with a non-string value there is rejected. |
| `integer_columns` | `[]` | Payload fields stored as `LONG`. A fractional, non-numeric or out-of-range value there is rejected. |
| `include_stream_column` | `true` | Write the stream name as a `SYMBOL` column named `stream`. |
| `include_topic_column` | `true` | Write the topic name as a `SYMBOL` column named `topic`. |
| `include_partition_column` | `false` | Write `partition_id` as a `LONG`. |
| `include_offset_column` | `false` | Write `offset` as a `LONG`. |
| `include_headers` | `false` | Write each header as a `VARCHAR` column named `header_<key>`. Binary header values are base64-encoded. |
| `ack_level` | `ok` | `ok` waits for the server to accept a flush. `durable` waits for the Enterprise durable ack. |
| `flush_timeout` | `30s` | How long a flush waits for its ack without progress. `0s` with `ack_level = "ok"` means fire and forget. `0s` is not allowed with `durable`. |
| `batch_size` | `1000` | Maximum rows per flush. |
| `max_flush_bytes` | `1000000` | Flush when the buffer reaches this size, even before `batch_size` rows. |
| `numbers_as_double` | `true` | Write every JSON number as a `DOUBLE`. See [Numbers](#numbers). |
| `log_rejected_payload` | `false` | Add a truncated payload to the log line of a rejected record. |
| `verbose_logging` | `false` | Log per-batch details at `info` instead of `debug`. |

Field names in the column lists match without regard to case, as in QuestDB.

Credentials, TLS, store-and-forward, reconnect and multi-host failover are set
in the connect string. See the
[connect string reference](https://questdb.com/docs/connect/clients/connect-string/).

### Batch sizing

A QWP frame has a size cap, about 4 MiB by default, and the client refuses a
frame over the cap with all of its rows. `max_flush_bytes` flushes before a
batch of wide rows reaches it. The 1 MB default leaves room, because the buffer
size is an estimate and a frame can also carry the symbol dictionary. The cap
is the smaller of `max_buf_size` (100 MiB by default) and the segment size
`sf_max_segment_bytes` (4 MiB by default) in the connect string, so with the
defaults you raise the cap through `sf_max_segment_bytes`. The server's own
maximum batch size still applies.

The check runs before each row is added. A message larger than the frame cap
minus `max_flush_bytes` can therefore push one frame over the cap. That flush
fails, and the connector does not write the rows buffered with the message or
the rest of its chunk. It logs their offsets.

## Store-and-forward

Set `sf_dir` in the connect string to persist outgoing frames on disk. Frames
the server has not acknowledged are sent again after a disconnect or a runtime
restart.

```toml
connection_string = "ws::addr=localhost:9000;sf_dir=/var/lib/iggy/questdb-sf;sender_id=iggy;"
```

Use it in production. The connectors runtime commits offsets before
`consume()` runs ([#2928](https://github.com/apache/iggy/issues/2928)) and does
not replay a failed batch ([#2927](https://github.com/apache/iggy/issues/2927)),
so rows lost in the sink cannot be read again from Iggy.

- The parent directory of `sf_dir` must exist. `~` is not expanded.
- Give each connector instance its own `sender_id` or its own `sf_dir`.
  Otherwise one instance can replay frames the other queued.
- A frame that QuestDB rejects terminally, for example on a schema mismatch,
  stays on disk and is sent again after a restart. To recover, delete the slot
  directory under `<sf_dir>/<sender_id>-ingest-*/`.
- If a symbol dictionary fills up, the slot carries it to the next connection,
  and flushes keep failing until the slot drains. This usually means a
  `symbol_columns` field with unbounded cardinality. Store it as a `VARCHAR`.

## Connection pool

The connector borrows a pooled connection for each chunk it writes, and the
runtime writes each topic in its own task. `sender_pool_max` caps the pool and
defaults to 4. When the pool is full, a borrow waits `acquire_timeout_ms`
(5 s by default) and then fails, and the rest of that batch is lost. Set
`sender_pool_max` to at least the number of topics the connector consumes:

```toml
connection_string = "ws::addr=localhost:9000;sender_pool_max=16;"
```

## Enterprise

- **Multi-host failover**: list several endpoints, for example
  `addr=node-a:9000,node-b:9000`. The client moves to another endpoint on
  reconnect and keeps buffering while a replica is promoted.
- **TLS**: use `wss::`. For a private CA, point `tls_roots` at its certificate
  file, and add `tls_roots_password` if that file is a JKS or PKCS#12 keystore.
  `tls_verify=unsafe_off` turns off certificate checks. A default build refuses
  it. Build with the `insecure-skip-verify` feature to use it against a test
  server.
- **Durable ack**: set `request_durable_ack=on` in the connect string and
  `ack_level = "durable"`. The connector does not start with `durable` alone,
  because the client would refuse every wait. The server also needs replication
  configured. Without it no flush is ever acknowledged.

### Choosing an ack level

`ok` returns when the server accepts a flush. `durable` also waits until the
write-ahead log is shipped. With `ok`, a primary that fails before shipping can
lose rows it already acknowledged. Use `durable` when losing that tail matters.

`durable` costs one round trip per flush, so larger batches reduce the cost. In
one test that flushed 10 rows every 250 ms, `durable` reached about 8 rows/s and
`ok` about 40 rows/s.

## Types

QuestDB creates the table and its columns from the first rows it receives.
Create the table yourself if you need partitioning, `TTL`, `DEDUP UPSERT KEYS`,
symbol capacities or a specific designated timestamp name.

| JSON | QuestDB |
| ---- | ------- |
| string | `VARCHAR`, or `SYMBOL` / `UUID` when listed in `symbol_columns` / `uuid_columns` |
| number | `DOUBLE`, or `LONG` when listed in `integer_columns` |
| boolean | `BOOLEAN` |
| array of numbers | `DOUBLE[]`, up to 3 dimensions. An empty array is rejected. |
| object | `VARCHAR` holding the JSON text |
| `null` | column omitted |

Integer arrays are stored as `DOUBLE[]`, because QuestDB has no other array
type. `BOOLEAN` has no null, so an omitted boolean reads back as `false`.

A payload that is not a JSON object goes into one `VARCHAR` column named
`payload`. That covers `Raw`, `Text`, `FlatBuffer` and `Avro` payloads, proto
text that is not JSON, and JSON arrays or scalars.

### Numbers

JSON has one number type, so a field can arrive as `20` in one record and
`20.5` in the next. With `numbers_as_double = true`, the default, every number
is written as a `DOUBLE`, and the column type does not depend on which value
came first.

A `DOUBLE` holds integers exactly only up to 2^53 (9007199254740992). Values
such as nanosecond epochs, snowflake ids and hashes are larger. List those
fields in `integer_columns` to store them as `LONG`:

```toml
integer_columns = ["trade_id", "event_nanos"]
```

A whole value written as `20.0` is accepted in an `integer_columns` field. A
fractional value, a non-number or a value outside the `LONG` range there is
rejected. When the connector rounds a
number past 2^53 to a `DOUBLE`, it logs one warning per kind of value with the
column name.

With `numbers_as_double = false`, each value takes its type from its JSON shape:
integers become `LONG` and fractions `DOUBLE`. Two problems follow:

- The client fixes a column's type from the first row of each flush. Later rows
  in that flush with the other number shape are rejected.
- If the first rows of an auto-created column are all whole numbers, QuestDB
  creates it as `LONG` for good.

Turn it off only if every numeric field is a true integer and you create the
table with explicit column types.

### Timestamps

An auto-created table names its designated timestamp `timestamp`. A payload
field with the same name collides with it, and the server rejects the whole
flush. Rename the field with a transform, or create the table with
`timestamp(<name>)`. The field named by `timestamp_field` is used only as the
designated timestamp.

`message` and `origin` timestamps are in microseconds, so an auto-created table
gets a `TIMESTAMP` column. `payload` timestamps are converted to nanoseconds and
give a `TIMESTAMP_NS` column. With `server`, QuestDB stamps each row on arrival.
Use one source per table.

## Delivery semantics

Delivery is at least once, except in the loss cases listed below. A reconnect,
a failover or a replay of queued frames can send the same rows again. Declare
[`DEDUP UPSERT KEYS(...)`](https://questdb.com/docs/concepts/deduplication/) on
tables that must not have duplicates.

The client queues unacknowledged frames in memory, or on disk when `sf_dir` is
set. Rows can still be lost in three cases:

- With `ack_level = "ok"` on a cluster, a primary that fails before it ships
  its write-ahead log can lose rows it accepted. See
  [Choosing an ack level](#choosing-an-ack-level).
- Without `sf_dir`, a crash, or a shutdown that outlasts
  `close_flush_timeout_millis` in the connect string, loses the queue even
  though `consume()` already reported success.
- Rows the sink does not write, such as a rejected record or a failed chunk,
  are lost, as the rest of this section describes.

A batch fails when:

- QuestDB rejects a frame terminally. The client reports some rejections after
  the flush that caused them returned, without saying which topic sent the
  frame. Such a rejection fails the next batch, of any topic on this connector,
  that has no error of its own.
- A chunk fails to write. If the cause is the connection, the connector skips
  the remaining chunks of the batch. It logs the offsets and message ids of the
  messages it did not write.
- 10 batches in a row on one topic each leave a flush unacknowledged within
  `flush_timeout`. A single late ack is only a warning, because the frames stay
  queued and are delivered later.
- The client's inbox of rejection reports overflows and drops events. A terminal
  rejection may then be unreported, so the next batch fails although its own
  rows were written.

The runtime counts a failed batch in `iggy_connector_errors_total`. It does not
retry it ([#2927](https://github.com/apache/iggy/issues/2927)).

`flush_timeout` also bounds shutdown. The runtime does not interrupt a flush in
progress, so shutdown can wait up to `flush_timeout` per chunk in flight.

### Rejected records

A rejected record is lost, and its log line is the only record of it. The
runtime has already committed its offset, and there is no dead-letter queue.

Each rejection is logged with its stream, topic, partition, offset and message
id. The first 20 per batch are logged at `error` and the rest at `debug`.

Rejections are not counted in the runtime metrics. A batch that writes some
rows and rejects others counts as a success, so `iggy_connector_errors_total`
stays at zero. The plugin cannot report per-row counts through the current SDK.
Alert on the connector's logs instead.

A record is rejected when:

- **The connector rejects it before writing**: a binary payload that is not
  UTF-8, a malformed UUID, a bad array, a value of the wrong type in a listed
  column, a missing or out-of-range timestamp field, an invalid or duplicate
  column name, or a row with no columns. The rest of the batch is written.
- **The client rejects it while writing**, usually because a column already has
  another type in this flush. The rest of the batch is written.
- **The server rejects the frame.** QuestDB rejects whole frames, so the
  connector fails the batch and cannot name the record.

Set `log_rejected_payload = true` to add a truncated payload to these log lines.
This writes user data to the log.

### Reserved column names

The connector writes these columns itself. A record with a payload field of the
same name, ignoring case, is rejected.

| Column | Written when |
| ------ | ------------ |
| `stream` | `include_stream_column = true`, the default |
| `topic` | `include_topic_column = true`, the default |
| `partition_id` | `include_partition_column = true` |
| `offset` | `include_offset_column = true` |
| `header_<key>` | `include_headers = true` |

With the defaults, every record with its own `stream` or `topic` field is
rejected. Turn the flag off or rename the field with a transform.

## Testing

Unit tests:

```bash
cargo test -p iggy_connector_questdb_sink
```

The integration tests start a QuestDB container and need Docker. Build the
plugin and the binaries first. The tests run the server and the connectors
runtime from `target/`, the runtime loads the plugin from there, and
`cargo test` builds none of them:

```bash
cargo build -p iggy_connector_questdb_sink
cargo build --bin iggy-server --bin iggy-connectors
cargo test -p integration -- connectors::questdb::
```

If the plugin is missing or older than its sources, the tests fail at once with
the build command to run.

### Enterprise

Failover, TLS and durable ack need a QuestDB Enterprise cluster, and the image
is not public, so CI does not cover them. They were tested by hand on a
three-node cluster with token auth over `wss::`: an unreachable first endpoint,
a primary stopped and restarted, graceful promotions, a period with no primary,
and a hard stop of two nodes. Rows were written throughout, and every run ended
with no gaps and no duplicates.
