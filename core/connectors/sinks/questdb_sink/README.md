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
verbose_logging = false
```

| Field | Default | Description |
| ----- | ------- | ----------- |
| `connection_string` | required | QuestDB connect string, `ws::` or `wss::`. Treated as a secret. |
| `table` | required | Target table. |
| `timestamp_source` | `message` | `message`, `origin`, `payload`, or `server`. |
| `timestamp_field` | none | Payload field carrying the timestamp. Required when `timestamp_source = "payload"`. |
| `timestamp_unit` | `auto` | `auto`, `seconds`, `millis`, `micros`, `nanos`. `auto` infers from magnitude. |
| `symbol_columns` | `[]` | Payload fields stored as `SYMBOL` instead of `VARCHAR`. |
| `uuid_columns` | `[]` | Payload fields holding canonical RFC-4122 strings, stored as `UUID`. |
| `include_stream_column` | `true` | Write the Iggy stream name as a `SYMBOL`. |
| `include_topic_column` | `true` | Write the Iggy topic name as a `SYMBOL`. |
| `include_partition_column` | `false` | Write `partition_id` as a `LONG`. |
| `include_offset_column` | `false` | Write `offset` as a `LONG`. |
| `include_headers` | `false` | Write each message header as a `header_<key>` `VARCHAR`. |
| `ack_level` | `ok` | `ok` waits for server acceptance; `durable` waits for the Enterprise durable-ACK barrier. |
| `flush_timeout` | `30s` | How long to wait for the configured ack level. |
| `batch_size` | `1000` | Rows per flush. |
| `log_rejected_payload` | `false` | Include a truncated payload in the log line for a rejected message. Off by default because a rejected payload is still user data. |
| `verbose_logging` | `false` | Raise per-batch logs from `debug` to `info`. |

Everything else, including credentials, TLS, store-and-forward, reconnect, and
multi-host failover, is configured through the connect string. See the
[connect string reference](https://questdb.com/docs/connect/clients/connect-string/).

## Store-and-forward

Setting `sf_dir` in the connect string turns on QuestDB's client-side
store-and-forward: outgoing frames are persisted to disk before being sent, and
whatever the server has not acknowledged is replayed after a disconnect or after
the connectors runtime restarts.

```toml
connection_string = "ws::addr=localhost:9000;sf_dir=/var/lib/iggy/questdb-sf;sender_id=iggy;"
```

This matters more here than it does for other hosts. The connectors runtime
commits consumer offsets before `consume()` runs and discards its return value
([#2928](https://github.com/apache/iggy/issues/2928),
[#2927](https://github.com/apache/iggy/issues/2927)), so a failed batch cannot
be replayed from Apache Iggy. Once `flush()` returns, store-and-forward owns the
rows independently of the consumer offset.

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
  `ack_level = "durable"`. This needs replication configured on the server. On a
  node without WAL shipping the rows are accepted but the durable watermark
  never advances, so every flush waits out `flush_timeout`.

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

Every QuestDB table has a designated timestamp and the client cannot name it, so
an auto-created table calls it `timestamp`. For a different name, pre-create the
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
  when delivery is unknown and documents that `FailoverRetry` can carry it, so
  the connector requires `in_doubt() == false` as well.

Note that the runtime currently discards `consume()`'s return value at the FFI
boundary ([#2927](https://github.com/apache/iggy/issues/2927)), so this
classification affects logging only until that is fixed.

### Rejected records

**A rejected record is lost, and its log line is the only trace of it.** The
runtime commits the consumer offset before `consume()` runs
([#2928](https://github.com/apache/iggy/issues/2928)) and discards the result
([#2927](https://github.com/apache/iggy/issues/2927)), so the connector can
neither replay the record nor stop the pipeline. There is no dead-letter queue:
the connectors SDK gives a sink no way to produce back into Apache Iggy, and by
the time a message reaches the plugin the runtime has already decoded it and run
the transform chain, so the original bytes no longer exist to preserve. A
dead-letter topic belongs in the runtime, where those bytes are still available
and one implementation would serve every sink.

**Rejections are not visible in the runtime's Prometheus metrics.** The runtime
counts every message it hands across the FFI as processed, and increments
`iggy_connector_errors_total` only for drops it performs itself, such as decode
and transform failures. A record the sink rejects is therefore counted as a
success: `errors_total` stays at zero while `messages_processed` overcounts.

This is not fixable from the plugin. A sink is a separate shared library with no
handle on the runtime's metrics, and its `consume()` return value is discarded
at the FFI boundary in any case. Closing it needs a change in
`core/connectors/sdk` — either a metrics callback alongside the existing
`LogCallback`, or an FFI return carrying the written and rejected counts.

Until then, **alert on the connector's logs rather than on
`iggy_connector_errors_total`**. Every rejection is logged at `error` with the
stream, topic, partition, offset and message ID.

Rejections come in two granularities, and only one of them names a record:

- **Client-side, before the wire.** A payload that is not a JSON object, a
  malformed UUID string, an array nested past three dimensions, a missing
  timestamp field. These are caught while the row is being built, so the exact
  record is known. Each one is logged at `error` with its stream, topic,
  partition, offset, and message ID, and the row is rewound out of the buffer so
  the rest of the batch still flushes. At most 20 per batch are logged
  individually, followed by one summary line.
- **Server-side, at flush.** QuestDB acknowledges and rejects whole frames, not
  rows: a rejection carries frame sequence numbers (`from_fsn` / `to_fsn`), not a
  row index. When the server refuses a frame the connector reports the failure
  for the **batch** and cannot attribute it to a record. Isolating the offending
  row would require bisecting the batch across fresh connections, since a
  terminal rejection latches the sender. That is not implemented.

Set `log_rejected_payload = true` to include a truncated payload in the
client-side log lines, at the cost of writing user data to the log.

## Testing

Unit tests:

```bash
cargo test -p iggy_connector_questdb_sink
```

A developer harness against a live server is ignored by default:

```bash
QUESTDB_CONF='ws::addr=127.0.0.1:9000;' \
  cargo test -p iggy_connector_questdb_sink --test live_smoke -- --ignored --nocapture
```
