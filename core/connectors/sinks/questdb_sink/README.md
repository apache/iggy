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
| `include_stream_column` | `true` | Write the Iggy stream name as a `SYMBOL`. |
| `include_topic_column` | `true` | Write the Iggy topic name as a `SYMBOL`. |
| `include_partition_column` | `false` | Write `partition_id` as a `LONG`. |
| `include_offset_column` | `false` | Write `offset` as a `LONG`. |
| `include_headers` | `false` | Write each message header as a `header_<key>` `VARCHAR`. |
| `ack_level` | `ok` | `ok` waits for server acceptance; `durable` waits for the Enterprise durable-ACK barrier. |
| `flush_timeout` | `30s` | How long to wait for the configured ack level, for the whole batch. `0s` at `ack_level = "ok"` means fire and forget: the sink publishes and does not wait. `0s` is refused at `ack_level = "durable"`, because a zero timeout never expires. |
| `batch_size` | `1000` | Maximum rows per flush. |
| `max_flush_bytes` | `1000000` | Flush once the encoded buffer reaches this many bytes, regardless of `batch_size`. See below. |
| `numbers_as_double` | `false` | Write every JSON number as a `DOUBLE`. See the note below. |
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

**A terminal rejection persists in the store-and-forward log.** If QuestDB
rejects a frame with a terminal error such as a schema mismatch, restarting the
runtime replays that same frame from disk and latches the sender again before
any new data can flow. Recovering means clearing the affected slot directory
under `<sf_dir>/<sender_id>-ingest-*/`.

## Enterprise

- **Multi-host failover**: list several endpoints, `addr=node-a:9000,node-b:9000`.
  The client rotates endpoints on reconnect and keeps buffering across a replica
  promotion.
- **TLS**: use `wss://` with `tls_roots` / `tls_roots_password` for a private CA.
  `tls_verify=unsafe_off` disables verification and is for controlled test
  environments only.
- **Durable ACK**: `request_durable_ack=on` in the connect string plus
  `ack_level = "durable"`. Both are required and the connector refuses to start
  with only one, because QuestDB rejects a durable wait that the connect string
  did not ask for, which would fail every batch while the rows themselves landed.
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
| integer | `LONG` |
| float | `DOUBLE` |
| boolean | `BOOLEAN` |
| array of numbers | `DOUBLE[]`, nesting up to 3 dimensions |
| object | `VARCHAR` holding the JSON text |
| `null` | column omitted |

QuestDB stores only `DOUBLE` arrays, so integer arrays are widened. `BOOLEAN`
has no null representation: an omitted boolean reads back as `false`.

JSON carries a single number type, so a producer that writes `2` for a whole
value and `2.5` for a fractional one gives the same field two different QuestDB
types. QuestDB pins a column's type to whichever record created it and refuses
the records that disagree, which the connector reports per record. Set
`numbers_as_double = true` to make the type follow the column name instead.
Integers are then stored as `DOUBLE`, which is exact only below 2^53.

Every QuestDB table has a designated timestamp and the client cannot name it, so
an auto-created table calls it `timestamp`. A payload field named `timestamp` is
therefore written as an ordinary column beside it, which the server can refuse.
Rename that field with a transform, or pre-create the table with an explicit
`timestamp(<name>)` clause. For a different name, pre-create the
table with an explicit `timestamp(<name>)` clause. The field named by
`timestamp_field` is written only as the designated timestamp, never also as a
data column.

Payloads that are not JSON objects (`Raw`, `Text`, `Proto`, `FlatBuffer`,
`Avro`) land in a single `payload` `VARCHAR` column.

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
`iggy_connector_errors_total`**. Every rejection is logged at `error` with the
stream, topic, partition, offset and message ID.

Rejections come in three kinds, and only the first two name a record:

- **Validated before the wire.** A payload that is not a JSON object, a
  malformed UUID, a non-rectangular or too deeply nested array, a missing or
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
  judged from that record alone. JSON has one number type, so a producer writing
  `2` and `2.5` for the same field reaches this on ordinary data. The connector
  rebuilds the rows buffered since the last flush without the offending record
  and flushes them, which keeps the cost to that one record. Past a few
  recoveries in one batch it stops and fails the batch instead, on the grounds
  that the records are fighting each other rather than one being bad.
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
cargo nextest run -p integration -E 'test(/connectors::questdb::/)'
```

The fixture checks for the built plugin before starting anything and fails in
well under a second naming the build command, rather than starting a container
and timing out waiting for rows that were never going to arrive.

They need Docker. `cargo nextest` rather than `cargo test` because the harness
resolves the server and runtime binaries through `CARGO_BIN_EXE_*`, which
plain `cargo test` does not set for another package's binaries.

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
