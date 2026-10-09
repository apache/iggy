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

Writes Apache Iggy messages to [QuestDB](https://questdb.com) over **QWP**,
QuestDB's native binary columnar protocol, on a WebSocket transport. Works
against QuestDB open source and QuestDB Enterprise from the same build; the
Enterprise-only behaviour is selected entirely through connect-string keys.

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
| `timestamp_field` | none | Payload field carrying the timestamp. Required when `timestamp_source = "payload"`. |
| `timestamp_unit` | `auto` | `auto`, `seconds`, `millis`, `micros`, `nanos`. `auto` infers from magnitude. |
| `symbol_columns` | `[]` | Payload fields stored as `SYMBOL` instead of `VARCHAR`. A listed field must hold a scalar, and a record carrying an object or an array there is rejected rather than losing the value. Matched without regard to case. |
| `uuid_columns` | `[]` | Payload fields holding canonical RFC-4122 strings, stored as `UUID`. A listed field must hold a string, and a record carrying another type there is rejected. Matched without regard to case, as QuestDB resolves column names. |
| `integer_columns` | `[]` | Payload fields kept as `LONG` while `numbers_as_double` is on, for values a `DOUBLE` cannot hold exactly. A fractional value in a listed field is rejected. Matched without regard to case. |
| `include_stream_column` | `true` | Write the Iggy stream name as a `SYMBOL` named `stream`. |
| `include_topic_column` | `true` | Write the Iggy topic name as a `SYMBOL` named `topic`. |
| `include_partition_column` | `false` | Write `partition_id` as a `LONG`. |
| `include_offset_column` | `false` | Write `offset` as a `LONG`. |
| `include_headers` | `false` | Write each message header as a `header_<key>` `VARCHAR`. A binary (`Raw`) header value is base64-encoded. |
| `ack_level` | `ok` | `ok` waits for server acceptance; `durable` waits for the Enterprise durable-ACK barrier. |
| `flush_timeout` | `30s` | How long one flush waits for the configured ack level, measured as time without progress rather than as a total budget. `0s` at `ack_level = "ok"` means fire and forget: the sink publishes and does not wait. `0s` is refused at `ack_level = "durable"`, because a zero timeout never expires. A batch that flushes several times can therefore wait this long more than once. |
| `batch_size` | `1000` | Maximum rows per flush. |
| `max_flush_bytes` | `1000000` | Flush once the encoded buffer reaches this many bytes, regardless of `batch_size`. See below. |
| `numbers_as_double` | `true` | Write every JSON number as a `DOUBLE`, so a column holding a measurement is always a `DOUBLE` rather than taking its type from the first value that defined it. Turn it off only on a pre-created table whose every numeric field is a genuine integer. See [Numbers](#numbers). |
| `log_rejected_payload` | `false` | Include a truncated payload in the log line for a rejected message. Off by default because a rejected payload is still user data. |
| `verbose_logging` | `false` | Raise per-batch logs from `debug` to `info`. |

Everything else, including credentials, TLS, store-and-forward, reconnect, and
multi-host failover, is configured through the connect string. See the
[connect string reference](https://questdb.com/docs/connect/clients/connect-string/).

### Batch sizing

`batch_size` bounds rows; `max_flush_bytes` bounds bytes. Both matter, because
QWP caps a single frame at the smallest of `max_buf_size`, the maximum batch
size the server advertises, and the store-and-forward segment payload capacity,
roughly 4 MiB with the default 4 MiB segments. The row API does not split an
oversized buffer, so a batch of wide rows that exceeds the cap is rejected whole.
The byte bound flushes early to keep that from happening.

The margin between the 1 MB default and that cap is deliberate and wider than
one row. The buffer length the bound is measured against is a local estimate on
this transport, not the encoded frame size: it does not account for the
connection-scoped symbol dictionary, which a frame may have to carry in full.
Raise the bound only alongside `sf_max_segment_bytes` in the connect string, and
expect a server advertising a smaller batch size to refuse a frame before the
bound fires.

## Store-and-forward

Setting `sf_dir` in the connect string turns on QuestDB's client-side
store-and-forward: outgoing frames are persisted to disk before being sent, and
whatever the server has not acknowledged is replayed after a disconnect or after
the connectors runtime restarts.

```toml
connection_string = "ws::addr=localhost:9000;sf_dir=/var/lib/iggy/questdb-sf;sender_id=iggy;"
```

This matters more here than it does for other hosts. The connectors runtime
commits consumer offsets before `consume()` runs
([#2928](https://github.com/apache/iggy/issues/2928)) and does not replay a
batch the sink reports as failed
([#2927](https://github.com/apache/iggy/issues/2927)), so a failed batch cannot
be recovered from Apache Iggy. Once `flush()` returns, store-and-forward owns
the rows independently of the consumer offset.

The parent of `sf_dir` must already exist. The connector does not create paths
recursively and does not expand `~`.

Give each connector instance its own `sender_id`, or its own `sf_dir`. Two pools
that share a directory under one `sender_id` adopt each other's slots, so one
instance can replay frames the other queued.

**A full symbol dictionary persists in the slot too.** The dictionary belongs to
one connection, and a connection whose dictionary filled is retired when the
connector returns it, so the next flush borrows a fresh one. With `sf_dir` set,
the slot re-seeds the next connection's dictionary at the same size until the
slot drains, so each flush window fails once more until it does. A
`symbol_columns` field with unbounded cardinality is the usual cause; store it
as a `VARCHAR` instead.

**A terminal rejection persists in the store-and-forward log.** If QuestDB
rejects a frame with a terminal error such as a schema mismatch, restarting the
runtime replays that same frame from disk and latches the sender again before
any new data can flow. Recovering means clearing the affected slot directory
under `<sf_dir>/<sender_id>-ingest-*/`.

## Connection pool

The client pools connections, and the connector borrows one for each chunk it
writes. The runtime runs one `consume()` per stream and topic pair against the
same connector instance, so the number of borrows held at once is the number of
topic tasks writing at that moment.

`sender_pool_max` in the connect string caps the pool, and defaults to 4. Past
the cap a borrow waits `acquire_timeout_ms`, five seconds by default, and then
fails. The batch is lost at offsets the runtime already committed, so **set
`sender_pool_max` to at least the number of topics this connector consumes**:

```toml
connection_string = "ws::addr=localhost:9000;sender_pool_max=16;"
```

## Enterprise

- **Multi-host failover**: list several endpoints, `addr=node-a:9000,node-b:9000`.
  The client rotates endpoints on reconnect and keeps buffering across a replica
  promotion.
- **TLS**: use `wss://` with `tls_roots` / `tls_roots_password` for a private CA.
  `tls_verify=unsafe_off` disables verification and is for controlled test
  environments only. A default build refuses it: the key needs the connector's
  `insecure-skip-verify` feature, which is off so that a released plugin cannot
  be redirected to another server by configuration alone.
- **Durable ACK**: `request_durable_ack=on` in the connect string plus
  `ack_level = "durable"`. Both are required. The connector refuses to start on
  `ack_level = "durable"` without the connect-string key, because QuestDB rejects
  a durable wait the connect string did not ask for, which would fail every batch
  while the rows themselves landed. The opposite pairing starts: the key without
  `ack_level = "durable"` asks the server for durable acknowledgement and then
  waits only for acceptance.
  This also needs replication configured on the server: on a node without WAL
  shipping the rows are accepted but the durable watermark never advances, so no
  flush is ever acknowledged within `flush_timeout`.

### Choosing an ack level

`ok` returns once the server accepts a flush. `durable` additionally waits for
the server to confirm the write-ahead log has shipped, which closes a real
window: a primary that dies before shipping can lose rows it already
acknowledged.

That window is load-dependent. A moderate writer against healthy replication
may survive a hard failover with no loss at all, which does not mean the window
is absent. Choose `durable` when a lost tail would matter, not when a test
happens to come out clean.

The cost is a round trip per flush, so it is paid per *flush* rather than per
row: larger batches amortise it. A run flushing ten rows every 250 ms measured
roughly 8 rows/s under `durable` against 40 rows/s under `ok`, which is close to
the worst case for the comparison.

## Types

QuestDB creates the table and infers column types from the rows it receives.
Pre-create the table when you care about partitioning, `TTL`, `DEDUP UPSERT
KEYS`, Parquet settings, symbol capacities, or the designated timestamp name,
since none of those can be inferred from a JSON payload.

| JSON | QuestDB |
| ---- | ------- |
| string | `VARCHAR`, or `SYMBOL` / `UUID` when listed in `symbol_columns` / `uuid_columns` |
| number | `DOUBLE`, or `LONG` when listed in `integer_columns` |
| boolean | `BOOLEAN` |
| array of numbers | `DOUBLE[]`, nesting up to 3 dimensions. An empty array has no element type, so it is rejected rather than omitted like `null` |
| object | `VARCHAR` holding the JSON text |
| `null` | column omitted |

QuestDB stores only `DOUBLE` arrays, so integer arrays are widened. `BOOLEAN`
has no null representation: an omitted boolean reads back as `false`.

Payloads with no field structure land in a single `payload` `VARCHAR` column:
`Raw`, `Text`, `FlatBuffer` and `Avro`, proto text that does not parse as JSON,
and a JSON document that is an array or a scalar rather than an object. Proto
text that holds a JSON object takes the field path instead, so its fields become
columns.

### Numbers

JSON has one number type, and a serializer writes `2.0` as `2`. A field holding
20.0 and 20.5 therefore arrives as an integer and as a float, for the same
column. That is ordinary data, not a producer mistake, and the connector must
not let the difference decide the column's type.

**`numbers_as_double` is on by default. Every number is written as a `DOUBLE`,
so a field that holds a measurement is always stored as a `DOUBLE`,** whatever
its values happen to look like and however the batches fall. This is the
guarantee the default exists to give.

The cost is exactness. A `DOUBLE` carries integers exactly only up to 2^53,
which is 9007199254740992. Below that an integer survives the round trip bit for
bit: the conversion to a `DOUBLE` is lossless, and QuestDB coerces the value to
the column's declared type on the way in, so a `LONG` column stores the integer.
Above 2^53 the value is rounded in the connector, before the frame is built, so
the column type cannot recover it. In practice:

| Value | Magnitude | Safe as a `DOUBLE` |
| ----- | --------- | ------------------ |
| counts, row ids | up to ~9.0e15 | yes |
| millisecond epoch | ~1.8e12 | yes |
| microsecond epoch | ~1.8e15 | yes |
| nanosecond epoch | ~1.8e18 | **no** |
| snowflake id, hash | ~7e17 and up | **no** |

**Name those columns in `integer_columns` and they stay `LONG`:**

```toml
numbers_as_double = true
integer_columns = ["trade_id", "event_nanos"]
```

A whole value written as `20.0` is accepted in an `integer_columns` field, since
JSON gives no way to tell it from `20`. A genuinely fractional value there is
rejected per record, because rounding it would store something the producer never
sent and sending it as a `DOUBLE` would break the type the declaration pinned.
A string, boolean, array or object in a listed field is rejected for the same
reason. Both rejections apply whatever `numbers_as_double` is set to.

When a number past 2^53 is widened to a `DOUBLE`, the connector logs a warning
naming the column, once per connector, so the rounding is not silent.

#### Turning it off

`numbers_as_double = false` types each value by its own JSON shape: an integer
becomes a `LONG` and a float a `DOUBLE`. **It forfeits the guarantee above, so a
column holding a measurement is no longer certain to be a `DOUBLE`.** Two things
go wrong, both driven by values the producer never meant to distinguish:

- **Records are lost inside a flush window.** The client pins a column's type to
  the first record that defines it and refuses every later record of that window
  that disagrees. The refusal happens in the client, before anything reaches the
  server, so an existing `DOUBLE` column does not help: that record is never
  sent. Which shape survives depends on which one the window saw first, so the
  same stream loses different records when the batching changes.
- **An auto-created column can be created as the wrong type.** If the first
  window to define a column happens to carry only whole values, the connector
  sends a `LONG` and QuestDB creates a `LONG` column. A field that was always
  meant to be a `DOUBLE` is then a `LONG` for the lifetime of the table.

Turn it off only when both of these hold:

1. **Every numeric field is a genuine integer**, so no column needs fractional
   values at all. A field that is sometimes fractional does not qualify, even if
   it is usually whole.
2. **The table is pre-created with explicit column types**, so no column's type
   is inferred from whichever values arrived first.

If only the exactness matters to you, leave `numbers_as_double` on and list the
columns in `integer_columns` instead. That keeps the guarantee for every other
column.

Across flush windows there is no type conflict either way, because QuestDB
coerces an incoming value to the column's declared type.

Both coercion directions and the rounding above 2^53 are pinned by
`given_a_declared_table_when_consumed_should_coerce_both_number_directions` in
`core/integration/tests/connectors/questdb/questdb_sink.rs`, against a table
whose declared types are the opposite of what the connector sends.

### Timestamps

Every QuestDB table has a designated timestamp and the client cannot name it, so
an auto-created table calls it `timestamp`. A payload field named `timestamp` is
therefore written as an ordinary column beside it, which the server can refuse.
That refusal is of the whole frame, so it costs every row of the flush window
that carried the field, not just the record.
Rename that field with a transform, or pre-create the table with an explicit
`timestamp(<name>)` clause. The field named by
`timestamp_field` is written only as the designated timestamp, never also as a
data column.

The designated timestamp's resolution follows `timestamp_source`. `message`
and `origin` carry microseconds, so an auto-created table gets a `TIMESTAMP`
column. `payload` converts the field to nanoseconds through `timestamp_unit`,
so the column is `TIMESTAMP_NS`. `server` sends no timestamp at all: QuestDB
stamps the row on arrival and creates its default `TIMESTAMP` column.
Pre-create the table with the matching type when it has to be one or the
other, and keep one source per table: a table created by one source does not
take the other's resolution.

## Delivery semantics

**At-least-once.** A reconnect, an unplanned failover, or a store-and-forward
replay can re-send rows QuestDB already committed. Declare
[`DEDUP UPSERT KEYS(...)`](https://questdb.com/docs/concepts/deduplication/) on
tables that cannot tolerate duplicates.

Transport failures are returned as retryable errors and server rejections as
permanent, because a rejection is deterministic: the server refused the frame,
so the rows were never stored and the same bytes would be refused again.

Two further rules keep a retry from duplicating data:

- **A failure while waiting for the acknowledgement is never retryable**, even
  with a retryable error code. By then the frame is already published and the
  rows may be committed, so re-sending would duplicate them. A durable-ACK stall
  against a server without replication configured is exactly this case: the rows
  land and only the watermark fails to advance.
- **A retryable code is not sufficient on its own.** The client sets `in_doubt`
  when delivery is unknown, so the connector requires `in_doubt() == false` as
  well. On the row API this sink uses, a publish failure is always reported as
  not delivered, so the guard is a safeguard against a future change rather than
  a condition that fires today.

Three further conditions fail a batch:

- **A terminal server rejection.** The client reports one on its own thread,
  where it cannot know which batch caused it, so the connector counts the
  rejection and fails one batch for each. When a chunk's own flush failed on
  that rejection, that chunk is the one, and the count is retired for it rather
  than failing a later healthy batch. The retire can run before the client's
  report lands, so the count is allowed to go negative and nets to zero when
  the report arrives. A transport failure retires nothing. The client's report
  inbox is bounded and drops its oldest entry when full; the connector reads
  the client's dropped-event count each batch, and when it grew it forgives any
  negative balance and fails that batch, because a terminal rejection may have
  gone unreported.
- **A run of unacknowledged flushes.** One flush whose acknowledgement does not
  arrive within `flush_timeout` is delivery lag, and the frames stay queued, so
  it is a warning. After 10 consecutive such batches on one topic the connector
  reports an error instead, because nothing is being committed and reporting
  success would leave the runtime's metrics showing a healthy connector.
- **A chunk that fails.** `batch_size` splits a runtime batch into chunks. A
  failure that belongs to the frame, such as a server rejection, does not stop
  the chunks behind it. A failure that belongs to the connection does: a
  transport error, or a borrow the pool could not satisfy, which includes an
  exhausted `sender_pool_max` and a refused dial. Each remaining chunk would
  otherwise spend its own timeout finding that out. A full symbol dictionary
  is the exception: it belongs to the connection, but the next borrow replaces
  that connection, so the chunks behind it go on. Two `error` lines name what
  was lost, by offset and message ID, since the runtime committed them
  already: the failed chunk's messages from its last acknowledged window
  onward, which is the whole chunk under fire-and-forget or after a pending
  acknowledgement, and the chunks behind it that were never attempted.

  A publish can also fail for a fault that is not the frame's own: a terminal
  rejection of an earlier frame latches the connection, and a full dictionary
  belongs to it. The buffer is intact in both cases, so the connector borrows
  a fresh connection and re-flushes that window once before giving it up.

`flush_timeout` also sets the worst-case shutdown delay. The runtime allows a
sink five seconds to stop, and does not interrupt a flush that is already in
progress, so a 30 second timeout can hold shutdown open for 30 seconds per
chunk still in flight. Lower it if shutdown latency matters more than ack
confirmation.

The classification decides how a batch is reported: whether it is counted as
failed, and how loudly it is logged. The runtime reads the return value, logs
the failure and counts it in `iggy_connector_errors_total`, and leaves the
run out of `messages_processed`. It does not yet decide whether the batch is
retried, because the runtime does not replay a failed batch
([#2927](https://github.com/apache/iggy/issues/2927)). Once that is fixed the
same classification starts driving retries, with no change needed here.

### Rejected records

**A rejected record is lost, and its log line is the only trace of it.** The
runtime commits the consumer offset before `consume()` runs
([#2928](https://github.com/apache/iggy/issues/2928)) and does not replay a
batch the sink reports as failed
([#2927](https://github.com/apache/iggy/issues/2927)), so the connector can
neither replay the record nor stop the pipeline. There is no dead-letter queue:
the connectors SDK gives a sink no way to produce back into Apache Iggy, and by
the time a message reaches the plugin the runtime has already decoded it and run
the transform chain, so the original bytes no longer exist to preserve. A
dead-letter topic belongs in the runtime, where those bytes are still available
and one implementation would serve every sink.

**Per-record rejections are not visible in the runtime's Prometheus metrics.**
The runtime increments `iggy_connector_errors_total` for drops it performs
itself, such as decode and transform failures, and for a batch whose FFI call
returns a failure status. A batch that flushes successfully after dropping some
rows returns success, so every message in it counts as processed: `errors_total`
stays at zero while `messages_processed` overcounts by the number rejected.

This is not fixable from the plugin. A sink is a separate shared library with
no handle on the runtime's metrics, and its `consume()` return value is a single
status for the whole batch, with no room for per-row counts. Closing it needs a
change in `core/connectors/sdk`, either a metrics callback alongside the
existing `LogCallback`, or an FFI return carrying the written and rejected
counts.

Until then, **alert on the connector's logs rather than on
`iggy_connector_errors_total`**. A rejection is logged with the stream, topic,
partition, offset and message ID. The first 20 of a batch are logged at `error`
and the rest at `debug`, so that one bad producer cannot flood the error log.
Raise the log level to `debug` to see every record of a batch that rejects more
than 20.

### Reserved column names

The connector writes these columns itself, and a payload field of the same name
is rejected rather than silently overwritten. Names match without regard to
case, as QuestDB resolves them.

| Column | Written when |
| ------ | ------------ |
| `stream` | `include_stream_column = true`, the default |
| `topic` | `include_topic_column = true`, the default |
| `partition_id` | `include_partition_column = true` |
| `offset` | `include_offset_column = true` |
| `header_<key>` | `include_headers = true` |

A producer whose records carry their own `stream` or `topic` field therefore
loses every record under the defaults. Turn the matching flag off, or rename the
field with a transform.

Two names are deliberately not reserved. `payload` is written only for a record
with no field structure, which by definition carries no field that could collide
with it, so a JSON field named `payload` is stored as an ordinary column.
`timestamp` is not reserved either, because it is a valid data column on a table
whose designated timestamp was pre-created under another name; see
[Timestamps](#timestamps) for the auto-created case.

Rejections come in three kinds, and only the first two name a record:

- **Validated before the wire.** A payload that is not a JSON object, a
  malformed UUID, an empty, non-rectangular or too deeply nested array, a
  non-numeric value in an `integer_columns` field, a missing or
  out-of-range timestamp field, a name QuestDB will not accept or that collides
  with another column of the same row, a non-scalar value in a `symbol_columns`
  field, or a row that would have no columns at all. Each record is validated in
  full before anything is written, so the exact record is known and the rest of
  the batch is unaffected. Each is logged at `error` with its stream, topic,
  partition, offset and message ID. At most 20 per batch are logged individually,
  followed by one summary line.
- **Refused by the client at write time.** Some rules are only knowable to the
  buffer. The important one is column type: QuestDB pins a column's type to
  whatever the first row defined it as, so a later record disagreeing cannot be
  judged from that record alone. The client rolls the half-written row back
  before reporting the error, so the cost stays at that one record and the rest
  of the batch is written. The default `numbers_as_double = true` keeps mixed
  numbers from reaching this at all.
- **Rejected by the server, at flush.** QuestDB acknowledges and rejects whole
  frames, not rows: a rejection carries frame sequence numbers
  (`from_fsn` / `to_fsn`), not a row index. The connector reports the failure for
  the **batch** and cannot attribute it to a record. The structured detail is
  logged separately, including the rejections the client retries by itself, which
  are the only early warning that a server is pushing back.

Set `log_rejected_payload = true` to include a truncated payload in the
client-side log lines, at the cost of writing user data to the log.

## Testing

Unit tests:

```bash
cargo test -p iggy_connector_questdb_sink
```

Integration tests start a QuestDB container and drive the connector through the
connectors runtime. **Build the plugin first**: the runtime `dlopen`s it from a
path at runtime, so Cargo has no dependency edge to it and `cargo test` will not
build it for you.

```bash
cargo build -p iggy_connector_questdb_sink
cargo test -p integration -- connectors::questdb::
```

`cargo nextest run -p integration -E 'test(/connectors::questdb::/)'` runs the
same tests. The harness resolves the server and runtime binaries by path under
`target/`, so both runners find them, and both need those binaries built first.

The fixture checks that the plugin exists and is newer than its sources before
starting anything, and fails in well under a second naming the build command.
`cargo test` rebuilds the test binary but never the plugin, so without that
check an edit to the sink left the runtime loading the previous build.

They need Docker.

### Enterprise behaviour

Multi-host failover, TLS and durable ACK have no single-node equivalent and the
Enterprise image is not publicly pullable, so they cannot be covered here. They
were verified by hand against a three-node cluster: token auth over `wss://`,
an unreachable endpoint at the head of the address list, a primary stopped and
restarted, two graceful promotions including one across zones, a window with no
primary at all, and a hard stop of two nodes with a watchdog promotion. Rows
were written continuously throughout with `DEDUP UPSERT KEYS` on the target
table, and every run finished with a contiguous sequence: no gaps and no
duplicates.
