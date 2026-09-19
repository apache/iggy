# Kafka to Iggy record mapping

Status: proposed. Direction agreed with @spetz and @hubcio on 2026-09-16 ("messages should be
stored in iggy format"); the escape hatches below still need sign-off. Closes the last open
scope item of [#3533](https://github.com/apache/iggy/issues/3533) and blocks
[#3535](https://github.com/apache/iggy/issues/3535) (Produce) and
[#3536](https://github.com/apache/iggy/issues/3536) (Fetch).

## Decision

One Kafka record becomes one Iggy message, in Iggy's own format: the record value is the
message payload, the key and the Kafka headers become Iggy user headers. The gateway rebuilds
a Kafka record batch on Fetch.

Two properties drive this.

A Kafka consumer must be able to read a topic an Iggy producer wrote. This is the staged
migration the maintainers described: rewrite producers to the Iggy SDK first, leave consumers
on the gateway until later. The gateway can only encode an arbitrary Iggy message as a Kafka
record if the stored form has no Kafka framing in it.

Kafka offsets must line up with Iggy offsets. A Kafka record batch carries N records under one
base offset, while an Iggy message consumes exactly one offset. Storing a batch whole makes
every offset the gateway reports wrong by the batch size, and fixing that inside Iggy means
teaching the server to count records inside an opaque payload.

Storing the Kafka payload and headers as a dump inside the Iggy payload was the alternative
raised in the same thread. It does not satisfy the first property on its own: the gateway would
still need a native path for Iggy-written messages, so it would carry two storage formats
instead of one. Native storage with a narrow fallback (see below) keeps that to one.

## Field mapping

Produce, per record:

| Kafka | Iggy |
| ------- | ------ |
| record value | `payload` |
| record key | `kafka.key` user header, `Raw` |
| record header `name` | `kafka.h.<name>` user header, `Raw` |
| record timestamp (CreateTime), milliseconds | `origin_timestamp`, microseconds |
| record offset | partition offset, assigned by Iggy |
| partition index | partition index, both 0-based |
| topic | stream and topic per `TopicMapping` |

Fetch reverses it. A message with no `kafka.*` headers is a message an Iggy client wrote, and
encodes as a Kafka record with a null key, its Iggy user headers as Kafka headers, and
`origin_timestamp` as the record timestamp (falling back to the server-assigned `timestamp`
when the origin timestamp is zero). Iggy header kinds other than `Raw` and `String` are emitted
as their raw value bytes.

### Timestamps

Kafka carries a record timestamp in milliseconds. Iggy carries `origin_timestamp` in
microseconds (`core/common/src/types/message/iggy_message.rs:191`). Produce multiplies by 1000.
Fetch divides by 1000 and truncates toward zero.

A record that arrived through Produce survives the round trip exactly, because its microsecond
value is always a whole number of milliseconds. A message an Iggy client wrote does not. Its
sub-millisecond digits are lost on the way out, and Kafka has no field to keep them in.

Kafka sends `-1` for a record with no timestamp. That is stored as `0`, and Fetch already reads
a zero origin timestamp as an instruction to use the server-assigned timestamp instead. A real
broker does the same thing under `LogAppendTime`, so the two agree.

## Records Iggy cannot hold natively

Iggy rejects an empty payload (`core/common/src/types/message/iggy_message.rs:169`), caps a
user header value at 255 bytes (`core/common/src/types/message/user_headers.rs:631`), keys
headers in a `BTreeMap` so a name cannot repeat, and caps all user headers of one message at
100 KB (`MAX_USER_HEADERS_SIZE`, `iggy_message.rs:58`). Kafka allows all of the shapes those
rules exclude, so two mechanisms cover them.

None of those four numbers is a gateway setting. They are fixed constants on the server's
message type, with no configuration knob and no recorded rationale. The header budget works out
to roughly 350 headers at the 255-byte value cap, so in practice the key-length rule below is
what sends a record into the fallback and the budget is not.

### Null and empty values

A record with a null value (a tombstone) or a zero-length value is stored with a single `0x00`
byte payload and a `kafka.value` header holding `null` or `empty`. Fetch reads that header and
restores the original, discarding the placeholder byte.

This keeps tombstones on the fast path rather than pushing them into the fallback, because they
are ordinary traffic on compacted Kafka topics. Iggy has no compaction, so a tombstone is stored
and served like any other record and nothing acts on it.

### Everything else: the envelope fallback

A record takes the fallback when any of these hold:

- the key is present and zero-length, which is not the same as a null key. Iggy rejects an
  empty header value on the same rule that caps it at 255 bytes, so `kafka.key` cannot carry it
- the key is longer than 255 bytes
- a header name, prefixed with `kafka.h.`, is longer than 255 bytes
- a header value is null, empty, or longer than 255 bytes
- the headers together would exceed the 100 KB user-header budget

A repeated header name belonged on that list and is not reachable. Kafka allows one, but
`kafka_protocol` decodes headers into an `IndexMap` and inserts each in turn (`records.rs:919`),
so a repeat overwrites its earlier entry before any gateway code runs. The last value wins and
the record takes the native path. Catching the case needs a decoder this gateway does not have.

Such a record is stored with a `kafka.envelope` header whose value is one byte, the format
version, currently `1`. The payload holds the key, the value and the headers in the layout
below. Fetch checks for that header first and takes the plain path only when it is absent.

The layout is fixed here rather than left to the implementation, because `kafka_protocol` hands
a handler a decoded `Record` and never a raw slice of the record, so there is no verbatim body
to copy. All integers are little-endian.

```text
u8   flags           bit 0 key present, bit 1 value present
u32  key_len         0 when the key is absent
..   key
u32  value_len       0 when the value is absent
..   value
u32  header_count
     repeated header_count times:
       u32  name_len
       ..   name
       u8   value_present
       u32  value_len   0 when the header value is absent
       ..   value
```

That is 13 bytes of fixed overhead plus 9 bytes per header. Re-encoding the record as a
one-record Kafka batch would also work and would cost less code, but it puts batch framing back
into storage, which is the thing this document decided against.

The cost is that these messages are opaque to Iggy consumers and connectors. That is the point
of confining the fallback to record shapes that are rare in practice, rather than making it the
default storage form.

## Batch-level fields

Per-record storage drops what the Kafka record batch header carries: producer id, producer
epoch, base sequence, the transactional flag, compression and the batch CRC. Fetch synthesizes
a batch with producer id `-1`, epoch `-1`, base sequence `-1`, no compression, `CreateTime`
timestamps, and a recomputed CRC32C.

Two consequences worth stating before they surprise someone:

- Idempotent-producer deduplication cannot be reconstructed from stored data later. If
  [#3545](https://github.com/apache/iggy/issues/3545) ever grows past a stub, producer id,
  epoch and sequence need their own tracking.
- The bytes a consumer receives are not the bytes the producer sent, so anything comparing
  batches byte for byte across the gateway will differ.

Whether producer id `-1` is what Fetch actually sends depends on the InitProducerId decision in
[`IDEMPOTENCE.md`](IDEMPOTENCE.md). Allocating producer ids does not change what is stored, only
what Produce accepts, so this section holds under either answer.

Produce decompresses gzip, snappy, lz4 and zstd batches, which means turning those features back
on for the `kafka-protocol` dependency (`Cargo.toml:217` currently builds it with
`default-features = false, features = ["broker"]`). Fetch emits uncompressed batches.

Decompression needs its own bound, and the bound is per request rather than per batch.
`max_frame_size` bounds the frame a client sent, which is the compressed size, and zstd reaches
1000 to 1 on repetitive input without being asked, so an 8 MiB frame can expand to gigabytes. One
frame also carries many batches: the request holds up to `MAX_REQUEST_ELEMENTS` (4096) topic and
partition entries, each with its own records blob. A cap applied to one batch at a time would
therefore still admit 4096 times that much output.

Produce keeps a single decompression budget for the whole request, set to `max_frame_size`, so a
compressed request can never yield more than the same client could have sent uncompressed. A
batch that exhausts the budget is rejected with `MESSAGE_TOO_LARGE` (10). Each decompressed
record value has to clear Iggy's own `MAX_PAYLOAD_SIZE` (64 MB, `iggy_message.rs:44`) separately,
since one record becomes one message.

The budget bounds what a request accumulates, not what one batch allocates. `kafka_protocol`'s
decompressors write the whole stream out before they hand it over (`compression/gzip.rs:46` and
its three siblings), so a single batch reaches its full decompressed size in memory and the
budget rejects it one step later. Bounding the peak needs a size-limited reader per codec, which
means owning Kafka's snappy and lz4 framing rather than borrowing it. That is worth doing and it
is not done here.

Nothing decompresses today. The record batch stays an opaque `Bytes` on both paths, so the bound
above is a requirement on [#3535](https://github.com/apache/iggy/issues/3535) rather than a
description of current behavior.

## Offsets

Kafka offset and Iggy offset are the same number for the same record, and both partition spaces
are 0-based, so neither direction converts.

Produce takes the base offset from the send confirmation
(`SendMessagesConfirmationResponse::base_offset`). The server may return no confirmation, for
example for a request it classifies as a duplicate, in which case the response carries `-1`
rather than a guessed offset; Kafka clients surface that as an unknown offset.

ListOffsets LATEST is the high watermark from `IggyBridge::high_watermarks`. EARLIEST has no
server-side field today (`Partition` carries no log start offset), so it reads the first
retained message instead, and the `(messages_count, current_offset) == (0, 0)` ambiguity
documented on `high_watermarks` applies to both.

### Header order

Kafka carries record headers as an ordered list. Iggy keys them in a `BTreeMap`, so Fetch emits
them sorted by name and the producer's order is gone.

There is no fallback for this, because the gateway cannot tell whether a record's header order
carries meaning. The envelope does preserve order, since it stores the headers as a list, but a
record only reaches the envelope for one of the reasons above. A consumer that depends on header
order therefore sees a different order through the gateway than a real broker would give it.

## Partitioning

Both systems number partitions from 0, so the partition index passes through unchanged in each
direction and neither side converts.

Produce sends to the partition the request names, `Partitioning::partition_id(index)`. A Kafka
producer resolves the partition itself before it builds the request, so every partition index in
a `ProduceRequest` is a real one and `Partitioning::balanced()` has no trigger on this path. The
`-1` that `SCOPE.md` refers to belongs to CreateTopics, where it means "use the broker default
partition count", and it is handled there rather than here.

Kafka consumer groups are not mapped onto Iggy consumer groups. The gateway assigns partitions
to group members the way Kafka does, in the client, and polls every partition by explicit offset.
Iggy's group registry is used as an offset key and for nothing else, which
[`OFFSET_STORAGE.md`](OFFSET_STORAGE.md) covers.

## Reserved header namespace

`kafka.` is reserved on messages the gateway writes and reads. An Iggy producer that sets a
header in that namespace on a topic a Kafka consumer reads will have it interpreted as gateway
metadata.

## Open questions

Four questions need an answer before Produce ([#3535](https://github.com/apache/iggy/issues/3535))
is written. Each one carries a default. If no answer lands by 2026-09-22, the default is taken,
this document is updated to record that it was decided by default, and the work proceeds.

### 1. Envelope fallback, or reject the record?

A record that Iggy cannot hold natively goes into the envelope described above. The alternative
is to reject it with `MESSAGE_TOO_LARGE` (10), so that nothing an Iggy consumer cannot read ever
reaches a stream.

Rejecting is the stricter guarantee and the worse compatibility story: a Kafka producer that
sends a 300-byte key works against a real broker and fails against the gateway.

Default: keep the envelope.

### 2. Is `kafka.` the right prefix?

Every Kafka header name is stored as `kafka.h.<name>`, which spends 8 of the 255 bytes an Iggy
header name has, on every header of every record. A shorter prefix buys those bytes back and
costs readability for anyone reading a stream by hand.

Default: keep `kafka.`.

### 3. Is the placeholder byte acceptable for tombstones?

A null or empty value is stored as one `0x00` byte plus a `kafka.value` marker header. The
payload a native Iggy consumer sees is therefore a byte the producer never sent.

The alternative is the envelope, which costs a tombstone the fast path. Tombstones are ordinary
traffic on compacted Kafka topics, and Iggy has no compaction, so they are stored and served
like any other record.

Default: keep the placeholder byte.

### 4. Recompress on Fetch, or always emit uncompressed?

Produce decompresses, and Fetch currently rebuilds an uncompressed batch. Recompressing per
topic costs CPU on the read path and saves bytes on the wire to the consumer.

Default: always emit uncompressed, and revisit when a benchmark says it matters.

### Not asked here

A Kafka `retention.ms` topic config could map onto Iggy's `message_expiry` at creation time.
`ensure_stream_and_topic` leaves topics on the server default, which never expires. That belongs
to CreateTopics ([#3538](https://github.com/apache/iggy/issues/3538)), which owns topic
configuration, rather than to the record mapping.

## References

- Scope and phases: [`SCOPE.md`](SCOPE.md)
- Bridge API: `gateways/kafka/src/bridge/iggy_bridge.rs`
- Iggy message limits: `core/common/src/types/message/iggy_message.rs`,
  `core/common/src/types/message/user_headers.rs`
- Produce confirmations: `core/binary_protocol/src/responses/messages/send_messages.rs`
