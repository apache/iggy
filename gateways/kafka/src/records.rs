// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! One Kafka record to and from one Iggy message.
//!
//! `docs/BRIDGE_MAPPING.md` is the specification. This module implements it and nothing else:
//! no Iggy calls, no handler wiring.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::io::{self, Write};
use std::sync::OnceLock;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use iggy::prelude::{HeaderKey, HeaderValue, IggyError, IggyMessage, MAX_PAYLOAD_SIZE};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, Record,
    RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use thiserror::Error;

/// Iggy header whose one-byte value is the storage mapping version.
///
/// Written on every message this gateway produces and on no other, which is what lets the read
/// path tell its own messages from an Iggy client's. The `kafka.` namespace is reserved by
/// convention only, so presence of one namespaced header proves nothing on its own.
pub const VERSION_HEADER: &str = "kafka.v";
/// Storage mapping version this build writes and reads.
pub const MAPPING_VERSION: u8 = 1;

/// Iggy header carrying the Kafka record key.
pub const KEY_HEADER: &str = "kafka.key";
/// Iggy header naming which of null or empty a placeholder payload stands for.
pub const VALUE_MARKER_HEADER: &str = "kafka.value";
/// Iggy header marking a record stamped at the Unix epoch.
pub const TIMESTAMP_MARKER_HEADER: &str = "kafka.ts";
/// Prefix every Kafka record header name is stored under.
pub const HEADER_PREFIX: &str = "kafka.h.";
/// Iggy header whose one-byte value is the envelope byte layout version.
pub const ENVELOPE_HEADER: &str = "kafka.envelope";
/// Envelope byte layout version this build writes and reads.
pub const ENVELOPE_VERSION: u8 = 1;

/// Kafka sends this for a record with no timestamp.
const NO_TIMESTAMP: i64 = -1;
/// The one Kafka timestamp an `origin_timestamp` of zero cannot be told apart from.
const EPOCH_TIMESTAMP: i64 = 0;
/// Stored in place of a null or empty value, discarded on the way back.
const PLACEHOLDER: &[u8] = &[0x00];
/// Iggy caps one header name and one header value at this many bytes.
const MAX_FIELD: usize = 255;

const MARKER_NULL: &[u8] = b"null";
const MARKER_EMPTY: &[u8] = b"empty";
const MARKER_EPOCH: &[u8] = b"epoch";

const FLAG_KEY: u8 = 0b01;
const FLAG_VALUE: u8 = 0b10;

/// Flags byte, key length, value length and header count, per `BRIDGE_MAPPING.md`.
const ENVELOPE_OVERHEAD: usize = 13;
/// Name length, value-present byte and value length, before either field's own bytes.
const ENVELOPE_HEADER_OVERHEAD: usize = 9;

/// Record batch version this gateway writes. v2 is the only shape `kafka_protocol` encodes.
const BATCH_VERSION: i8 = 2;

/// Smallest v2 record: a length, an attributes byte, two deltas, two field lengths and a header
/// count, each a one-byte varint at least.
const MIN_RECORD_BYTES: usize = 7;
/// Smallest v2 record header: a name length varint and a value length varint, both empty.
const MIN_HEADER_BYTES: usize = 2;
/// Bytes a zigzag varint occupies at most, which is what `kafka_protocol` reads.
const MAX_VARINT_BYTES: usize = 5;
/// Bytes a zigzag varlong occupies at most, on the same terms.
const MAX_VARLONG_BYTES: usize = 10;
/// Base offset, batch length, leader epoch, magic, CRC, attributes, last offset delta, first and
/// max timestamp, producer id, producer epoch, base sequence and record count.
const BATCH_HEADER_BYTES: usize = 61;
/// Widest v2 record framing: five varints at five bytes each, an attributes byte, and the header
/// count varint, before the key, the value and the header bytes.
const RECORD_FRAMING_BYTES: usize = 31;
/// Widest per-header framing inside a v2 record: a name length and a value length varint.
const HEADER_FRAMING_BYTES: usize = 10;

/// Marks a snappy stream written by Kafka's own framing rather than raw snappy.
///
/// Kafka producers write xerial-framed snappy, which raw snappy decoders reject, and the Java
/// broker falls back to raw when the magic is absent. Both shapes therefore reach a broker.
const SNAPPY_MAGIC: &[u8; 16] = b"\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01";

/// Why a record or a batch could not cross.
#[derive(Debug, Error)]
pub enum RecordCodecError {
    #[error("Iggy rejected the message: {0}")]
    Iggy(#[from] IggyError),
    #[error("record timestamp {0} ms does not fit Iggy's microsecond field")]
    TimestampOutOfRange(i64),
    #[error("{0} bytes of stored user headers did not parse")]
    UserHeadersUnreadable(u32),
    #[error("stored mapping version {0} is not {MAPPING_VERSION}")]
    MappingVersion(u8),
    #[error("value marker {0:?} is neither null nor empty")]
    ValueMarker(Bytes),
    #[error("two stored header keys both name {0}")]
    HeaderNameCollision(String),
    #[error("timestamp marker {0:?} is not epoch")]
    TimestampMarker(Bytes),
    #[error("envelope for this record is {size} bytes, over Iggy's {MAX_PAYLOAD_SIZE} byte limit")]
    EnvelopeTooLarge { size: usize },
    #[error("envelope is truncated: needed {needed} bytes, {remaining} remain")]
    EnvelopeTruncated { needed: usize, remaining: usize },
    #[error("envelope has {0} bytes left after its last header")]
    EnvelopeTrailingBytes(usize),
    #[error("envelope format version {0} is not {ENVELOPE_VERSION}")]
    EnvelopeVersion(u8),
    #[error("envelope header name is not UTF-8")]
    EnvelopeHeaderName,
    #[error("record batch is malformed: {0}")]
    Batch(String),
    #[error("{0} record batches in one partition, Kafka allows 1")]
    SeveralBatches(usize),
    #[error("batch declares {count} records, and {limit} bytes can hold fewer")]
    RecordCountTooLarge { count: i32, limit: usize },
    #[error("batch declares {declared} records and holds {walked}")]
    RecordCountMismatch { declared: usize, walked: usize },
    #[error("record declares {count} headers, and {limit} bytes can hold fewer")]
    HeaderCountTooLarge { count: i32, limit: usize },
    #[error("record batch ends inside a record")]
    RecordTruncated,
    #[error("record field declares {0} bytes")]
    RecordFieldLength(i32),
    #[error("transactional batches are not supported")]
    TransactionalBatch,
    #[error("control batches are not supported")]
    ControlBatch,
    #[error("idempotent batches are not supported, set enable.idempotence=false")]
    IdempotentBatch,
    #[error("partition decompresses to {size} bytes, over the {limit} byte request budget")]
    BudgetExceeded { size: usize, limit: usize },
    #[error("partition needs {count} record slots, over the {limit} slot request budget")]
    RecordBudgetExceeded { count: usize, limit: usize },
    #[error("earlier partitions used the request budget, retry")]
    RequestBudgetSpent,
    #[error("zstd batch in a request older than Produce v7")]
    ZstdTooEarly,
    #[error("record repeats header name {0}")]
    RepeatedHeaderName(String),
}

type Result<T> = std::result::Result<T, RecordCodecError>;

/// Encodes one Kafka record as one Iggy message.
///
/// Takes the native path when Iggy can hold every field, and the envelope otherwise. A caller
/// cannot tell which from the return value, which is the point: `from_iggy` reverses both.
///
/// # Errors
///
/// Returns an error when the timestamp does not fit, when the envelope would exceed
/// `MAX_PAYLOAD_SIZE`, or when Iggy rejects the message for a reason the envelope does not fix.
pub fn to_iggy(record: &Record) -> Result<IggyMessage> {
    if let Some(value) = plain_value(record) {
        return plain_message(value.clone(), record.timestamp);
    }
    if needs_envelope(record) {
        return envelope_message(record);
    }
    let (payload, marker) = split_value(record.value.as_ref());
    let mut headers = gateway_headers(record.timestamp);
    if let Some(marker) = marker {
        headers.insert(header_key(VALUE_MARKER_HEADER), header_value(marker));
    }
    if let Some(key) = record.key.as_ref() {
        headers.insert(header_key(KEY_HEADER), header_value(key));
    }
    for (name, value) in &record.headers {
        // `needs_envelope` rejected the shapes that cannot be built here, so both are infallible.
        let Some(value) = value.as_ref() else {
            continue;
        };
        headers.insert(
            header_key(&format!("{HEADER_PREFIX}{}", name.as_str())),
            header_value(value),
        );
    }

    // The only limit left is the 100 KB budget over all headers together, which no per-field
    // check can see. Let the constructor rule on it rather than duplicating its arithmetic.
    build(payload, headers, record.timestamp)?.map_or_else(|| envelope_message(record), Ok)
}

/// Decodes one Iggy message as one Kafka record at `offset`.
///
/// A message without `kafka.v` was written by an Iggy client, not through this gateway. It gets a
/// null key, its own user headers under their own names, and its payload as the record value.
/// None of the `kafka.` headers carries meaning on such a message, because the namespace is
/// reserved by `BRIDGE_MAPPING.md` and by nothing the server enforces.
///
/// A message with `kafka.v` is taken as written here, so every marker on it is authoritative and
/// one this build does not recognize is an error rather than a guess. Nothing on the server
/// enforces the namespace, so that is a claim the message makes and not one the server keeps: an
/// Iggy writer can set `kafka.v` to a version this build does not implement, and every read of
/// that message then fails. Fetch cannot serve a record it cannot decode and a Kafka consumer
/// cannot step over one, so the handler in #3536 owns the skip-or-quarantine policy for a message
/// that fails here. `BRIDGE_MAPPING.md` records that as the open end of the provenance design.
///
/// # Errors
///
/// Returns an error when the stored user headers do not parse, when `kafka.v` names a mapping
/// version this build does not implement, or when a marker or an envelope is malformed.
pub fn from_iggy(message: &IggyMessage, offset: i64) -> Result<Record> {
    let stored = user_headers(message)?;
    let Some(version) = stored.get(&header_key(VERSION_HEADER)) else {
        let (key, value, headers) = foreign_fields(message, &stored);
        return Ok(record(key, value, headers, offset, timestamp_out(message)));
    };
    if version.as_bytes() != [MAPPING_VERSION] {
        let version = version.as_bytes().first().copied().unwrap_or_default();
        return Err(RecordCodecError::MappingVersion(version));
    }

    let (key, value, headers) = match stored.get(&header_key(ENVELOPE_HEADER)) {
        Some(envelope) => decode_envelope(envelope.as_bytes(), &message.payload)?,
        None => gateway_fields(message, &stored)?,
    };
    let timestamp = match stored.get(&header_key(TIMESTAMP_MARKER_HEADER)) {
        None => timestamp_out(message),
        Some(marker) => match marker.as_bytes() {
            MARKER_EPOCH => EPOCH_TIMESTAMP,
            _ => return Err(RecordCodecError::TimestampMarker(marker.value())),
        },
    };
    Ok(record(key, value, headers, offset, timestamp))
}

/// The stored user headers, with an unreadable block told apart from an absent one.
///
/// `IggyMessage::user_headers_map` folds a header block it cannot parse into `Ok(None)`, which
/// reads the same as a message that carries no headers at all. Taken at face value that turns an
/// enveloped message into its own envelope bytes served as the record value.
fn user_headers(message: &IggyMessage) -> Result<BTreeMap<HeaderKey, HeaderValue>> {
    match message.user_headers_map()? {
        Some(stored) => Ok(stored),
        None if message.header.user_headers_length > 0 => Err(
            RecordCodecError::UserHeadersUnreadable(message.header.user_headers_length),
        ),
        None => Ok(BTreeMap::new()),
    }
}

/// Kafka counts milliseconds, Iggy counts microseconds, and `-1` means the broker assigns one.
fn timestamp_in(millis: i64) -> Result<u64> {
    if millis == NO_TIMESTAMP {
        return Ok(0);
    }
    millis
        .checked_mul(1000)
        .and_then(|micros| u64::try_from(micros).ok())
        .ok_or(RecordCodecError::TimestampOutOfRange(millis))
}

/// Zero means the producer sent no timestamp, so the server-assigned one stands in.
///
/// A record stamped at the epoch stores that same zero, and `from_iggy` reads the `kafka.ts`
/// marker before it calls this, because Iggy has no other way to hold the difference.
fn timestamp_out(message: &IggyMessage) -> i64 {
    let micros = if message.header.origin_timestamp == 0 {
        message.header.timestamp
    } else {
        message.header.origin_timestamp
    };
    i64::try_from(micros / 1000).unwrap_or(NO_TIMESTAMP)
}

/// Whether any field of `record` is one Iggy refuses to hold natively.
///
/// A repeated header name never reaches here: `scan_records` refuses it.
fn needs_envelope(record: &Record) -> bool {
    let key_unholdable = record
        .key
        .as_ref()
        .is_some_and(|key| key.is_empty() || key.len() > MAX_FIELD);
    if key_unholdable {
        return true;
    }
    record.headers.iter().any(|(name, value)| {
        HEADER_PREFIX.len() + name.as_str().len() > MAX_FIELD
            || value
                .as_ref()
                .is_none_or(|value| value.is_empty() || value.len() > MAX_FIELD)
    })
}

/// The value, when `kafka.v` is the only header the record needs.
fn plain_value(record: &Record) -> Option<&Bytes> {
    if record.key.is_some() || !record.headers.is_empty() || record.timestamp == EPOCH_TIMESTAMP {
        return None;
    }
    record.value.as_ref().filter(|value| !value.is_empty())
}

/// Reuses one encoded `kafka.v` header block, so the common record skips a map and an encode.
///
/// Static bytes: a clone touches no refcount shared across workers.
fn plain_message(value: Bytes, timestamp: i64) -> Result<IggyMessage> {
    static VERSION_ONLY: OnceLock<&'static [u8]> = OnceLock::new();
    let headers = *VERSION_ONLY.get_or_init(|| {
        let block = IggyMessage::builder()
            .payload(Bytes::from_static(PLACEHOLDER))
            .user_headers(gateway_headers(NO_TIMESTAMP))
            .build()
            .ok()
            .and_then(|message| message.user_headers)
            .unwrap_or_else(|| unreachable!("the kafka.v header always fits"));
        Box::leak(block.to_vec().into_boxed_slice())
    });
    let mut message = IggyMessage::builder().payload(value).build()?;
    message.header.user_headers_length =
        u32::try_from(headers.len()).unwrap_or_else(|_| unreachable!("the kafka.v header is tiny"));
    message.user_headers = Some(Bytes::from_static(headers));
    message.header.origin_timestamp = timestamp_in(timestamp)?;
    Ok(message)
}

/// Payload to store, and the marker naming what the original value was when it is not the payload.
fn split_value(value: Option<&Bytes>) -> (Bytes, Option<&'static [u8]>) {
    match value {
        None => (Bytes::from_static(PLACEHOLDER), Some(MARKER_NULL)),
        Some(value) if value.is_empty() => (Bytes::from_static(PLACEHOLDER), Some(MARKER_EMPTY)),
        Some(value) => (value.clone(), None),
    }
}

/// The headers every gateway-written message carries, whichever path it takes.
///
/// Iggy reads an `origin_timestamp` of zero as no timestamp at all, so a record that really was
/// stamped at the epoch needs a marker to hold the difference.
fn gateway_headers(timestamp: i64) -> BTreeMap<HeaderKey, HeaderValue> {
    let mut headers = BTreeMap::new();
    headers.insert(header_key(VERSION_HEADER), header_value(&[MAPPING_VERSION]));
    if timestamp == EPOCH_TIMESTAMP {
        headers.insert(
            header_key(TIMESTAMP_MARKER_HEADER),
            header_value(MARKER_EPOCH),
        );
    }
    headers
}

/// `Ok(None)` when the headers together pass Iggy's budget, which the envelope then carries.
fn build(
    payload: Bytes,
    headers: BTreeMap<HeaderKey, HeaderValue>,
    timestamp: i64,
) -> Result<Option<IggyMessage>> {
    let mut message = match IggyMessage::builder()
        .payload(payload)
        .user_headers(headers)
        .build()
    {
        Ok(message) => message,
        Err(IggyError::TooBigUserHeaders) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    message.header.origin_timestamp = timestamp_in(timestamp)?;
    Ok(Some(message))
}

fn envelope_message(record: &Record) -> Result<IggyMessage> {
    // The envelope moves the key and the headers into the payload, so a record whose value alone
    // clears `MAX_PAYLOAD_SIZE` can be one the fallback cannot hold. Say so before spending the
    // allocation, since the native path has already been ruled out and nothing else is left.
    let size = envelope_size(record);
    if size > MAX_PAYLOAD_SIZE as usize {
        return Err(RecordCodecError::EnvelopeTooLarge { size });
    }

    let mut headers = gateway_headers(record.timestamp);
    headers.insert(
        header_key(ENVELOPE_HEADER),
        header_value(&[ENVELOPE_VERSION]),
    );
    build(encode_envelope(record, size), headers, record.timestamp)?
        .ok_or(IggyError::TooBigUserHeaders)
        .map_err(Into::into)
}

/// Exactly what `encode_envelope` writes for `record`.
fn envelope_size(record: &Record) -> usize {
    let field = |field: Option<&Bytes>| field.map_or(0, Bytes::len);
    ENVELOPE_OVERHEAD
        + field(record.key.as_ref())
        + field(record.value.as_ref())
        + record
            .headers
            .iter()
            .map(|(name, value)| {
                ENVELOPE_HEADER_OVERHEAD + name.as_str().len() + field(value.as_ref())
            })
            .sum::<usize>()
}

/// 13 bytes of fixed overhead plus 9 per header, little-endian throughout.
fn encode_envelope(record: &Record, size: usize) -> Bytes {
    let mut flags = 0u8;
    if record.key.is_some() {
        flags |= FLAG_KEY;
    }
    if record.value.is_some() {
        flags |= FLAG_VALUE;
    }

    let mut buf = BytesMut::with_capacity(size);
    buf.put_u8(flags);
    put_field(&mut buf, record.key.as_ref());
    put_field(&mut buf, record.value.as_ref());
    buf.put_u32_le(u32::try_from(record.headers.len()).unwrap_or(u32::MAX));
    for (name, value) in &record.headers {
        let name = name.as_str().as_bytes();
        buf.put_u32_le(u32::try_from(name.len()).unwrap_or(u32::MAX));
        buf.put_slice(name);
        buf.put_u8(u8::from(value.is_some()));
        put_field(&mut buf, value.as_ref());
    }
    buf.freeze()
}

type RecordFields = (
    Option<Bytes>,
    Option<Bytes>,
    IndexMap<StrBytes, Option<Bytes>>,
);

fn decode_envelope(version: &[u8], payload: &Bytes) -> Result<RecordFields> {
    match version.first() {
        Some(&ENVELOPE_VERSION) => {}
        Some(&other) => return Err(RecordCodecError::EnvelopeVersion(other)),
        None => return Err(RecordCodecError::EnvelopeVersion(0)),
    }

    let mut buf = payload.clone();
    let flags = take(&mut buf, 1)?[0];
    let key = take_field(&mut buf)?;
    let value = take_field(&mut buf)?;
    let count =
        u32::from_le_bytes(take(&mut buf, 4)?.as_ref().try_into().unwrap_or_default()) as usize;

    // The count is four bytes of a payload anyone can write and every header costs at least nine,
    // so reserving before reading lets a 13-byte message ask for four billion entries. Charge the
    // floor against what is left and the reserve below is bounded by the input.
    let needed = count.saturating_mul(ENVELOPE_HEADER_OVERHEAD);
    if buf.remaining() < needed {
        return Err(RecordCodecError::EnvelopeTruncated {
            needed,
            remaining: buf.remaining(),
        });
    }

    let mut headers = IndexMap::with_capacity(count);
    for _ in 0..count {
        let name = take_field(&mut buf)?;
        let name =
            String::from_utf8(name.to_vec()).map_err(|_| RecordCodecError::EnvelopeHeaderName)?;
        let present = take(&mut buf, 1)?[0] != 0;
        let value = take_field(&mut buf)?;
        headers.insert(StrBytes::from_string(name), present.then_some(value));
    }

    // Every byte of an envelope is accounted for above, so a leftover means this payload is not
    // one. Accepting it would turn a stray `kafka.envelope` header plus junk into a null record.
    if buf.has_remaining() {
        return Err(RecordCodecError::EnvelopeTrailingBytes(buf.remaining()));
    }

    Ok((
        (flags & FLAG_KEY != 0).then_some(key),
        (flags & FLAG_VALUE != 0).then_some(value),
        headers,
    ))
}

/// The native reading of a message this gateway wrote, so every marker on it is authoritative.
fn gateway_fields(
    message: &IggyMessage,
    stored: &BTreeMap<HeaderKey, HeaderValue>,
) -> Result<RecordFields> {
    let key = stored.get(&header_key(KEY_HEADER)).map(HeaderValue::value);
    let value = match stored.get(&header_key(VALUE_MARKER_HEADER)) {
        None => Some(message.payload.clone()),
        Some(marker) => match marker.as_bytes() {
            MARKER_NULL => None,
            MARKER_EMPTY => Some(Bytes::new()),
            // A marker this build does not write, on a message that says this build wrote it.
            // Guessing loses a payload or invents one, and a later version that adds a third
            // marker is read wrongly here rather than refused.
            _ => return Err(RecordCodecError::ValueMarker(marker.value())),
        },
    };

    let mut headers = IndexMap::new();
    for (name, stored_value) in stored {
        let Some(name) = header_name(name).and_then(|name| name.strip_prefix(HEADER_PREFIX)) else {
            continue;
        };
        // Two stored keys can hold the same bytes under different kinds, because an Iggy header
        // key orders on kind before bytes (`user_headers.rs:209`). `to_iggy` writes one kind, so
        // on a message this build wrote the names cannot collide, and a collision here says the
        // message is not what its `kafka.v` claims. Overwriting would drop a header quietly.
        let name = StrBytes::from_string(name.to_string());
        if headers
            .insert(name.clone(), Some(stored_value.value()))
            .is_some()
        {
            return Err(RecordCodecError::HeaderNameCollision(name.to_string()));
        }
    }
    Ok((key, value, headers))
}

/// The reading of a message an Iggy client wrote, which no marker on it can change.
///
/// Every header passes through under its own name, including one in the `kafka.` namespace. The
/// alternative was to treat any namespaced header as gateway metadata, which made a single
/// `kafka.`-prefixed header hide every other header on the message.
///
/// Two keys holding the same bytes under different kinds are two stored headers and one Kafka
/// header, and the later one in key order wins. Kafka carries headers as a list and would hold
/// both, but a `Record` keys them in an `IndexMap`, so there is no shape here that keeps the
/// pair. Refusing the message instead would let one Iggy writer stall a partition for every
/// Kafka consumer of it, which is the worse of the two. `BRIDGE_MAPPING.md` records the loss.
fn foreign_fields(
    message: &IggyMessage,
    stored: &BTreeMap<HeaderKey, HeaderValue>,
) -> RecordFields {
    let mut headers = IndexMap::new();
    for (name, stored_value) in stored {
        let Some(name) = header_name(name) else {
            continue;
        };
        headers.insert(
            StrBytes::from_string(name.to_string()),
            Some(stored_value.value()),
        );
    }
    (None, Some(message.payload.clone()), headers)
}

/// A Kafka header name for an Iggy header key, or `None` when the key is not text.
///
/// An Iggy key is bytes plus a kind, and `HeaderField::as_str` refuses every kind but `String`
/// (`user_headers.rs:372`). Other SDKs hand out `Raw` and `Int32` key constructors, so a key
/// holding a perfectly good Kafka name arrives under a kind this one would reject. Read the bytes
/// and let UTF-8 decide.
fn header_name(key: &HeaderKey) -> Option<&str> {
    std::str::from_utf8(key.as_bytes()).ok()
}

const fn record(
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: IndexMap<StrBytes, Option<Bytes>>,
    offset: i64,
    timestamp: i64,
) -> Record {
    Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id: NO_PRODUCER_ID,
        producer_epoch: NO_PRODUCER_EPOCH,
        timestamp_type: TimestampType::Creation,
        offset,
        sequence: NO_SEQUENCE,
        timestamp,
        key,
        value,
        headers,
    }
}

fn put_field(buf: &mut BytesMut, field: Option<&Bytes>) {
    let field = field.map_or(&[][..], |field| field.as_ref());
    buf.put_u32_le(u32::try_from(field.len()).unwrap_or(u32::MAX));
    buf.put_slice(field);
}

fn take(buf: &mut Bytes, needed: usize) -> Result<Bytes> {
    if buf.remaining() < needed {
        return Err(RecordCodecError::EnvelopeTruncated {
            needed,
            remaining: buf.remaining(),
        });
    }
    Ok(buf.split_to(needed))
}

/// Length-prefixed bytes, possibly empty. Presence is the flags byte's job, not the length's,
/// so that an empty key stays distinct from a null one.
fn take_field(buf: &mut Bytes) -> Result<Bytes> {
    let len = u32::from_le_bytes(take(buf, 4)?.as_ref().try_into().unwrap_or_default()) as usize;
    take(buf, len)
}

/// Both are infallible for the names and values this module builds: every one is non-empty and
/// within `MAX_FIELD`, which `needs_envelope` guarantees for caller-supplied bytes.
fn header_key(name: &str) -> HeaderKey {
    HeaderKey::try_from(name).unwrap_or_else(|_| unreachable!("header name {name} is out of range"))
}

fn header_value(value: &[u8]) -> HeaderValue {
    HeaderValue::try_from(value).unwrap_or_else(|_| unreachable!("header value is out of range"))
}

/// What one Produce request may decompress to, in total.
///
/// Charged across every partition in the request, so many partitions cannot each take it all.
/// Writes go through `BudgetedWriter`, so an over-budget frame never inflates whole in memory.
pub struct DecompressionBudget {
    bytes: usize,
    remaining: Cell<usize>,
    /// Records the request may decode. Each one costs a `Record` and an `IggyMessage` in memory.
    records: usize,
    records_remaining: Cell<usize>,
    /// What the current partition charged. Tells "too large" (10) from "retry" (6).
    entry_bytes: Cell<usize>,
    entry_records: Cell<usize>,
    /// Why this module refused a batch, when it did. Everything this module reports from inside
    /// the decoder's decompression hook leaves as an `io::Error` or an `anyhow::Error`, which the
    /// decoder stringifies, so the typed reason is parked here and taken by `decode_batch`.
    reason: Cell<Option<RecordCodecError>>,
}

impl DecompressionBudget {
    #[must_use]
    pub const fn new(bytes: usize, records: usize) -> Self {
        Self {
            bytes,
            remaining: Cell::new(bytes),
            records,
            records_remaining: Cell::new(records),
            entry_bytes: Cell::new(0),
            entry_records: Cell::new(0),
            reason: Cell::new(None),
        }
    }

    fn start_entry(&self) {
        self.entry_bytes.set(0);
        self.entry_records.set(0);
    }

    /// Takes `len` bytes if the request has them left.
    fn take(&self, len: usize) -> bool {
        let Some(remaining) = self.remaining.get().checked_sub(len) else {
            return false;
        };
        self.remaining.set(remaining);
        self.entry_bytes.set(self.entry_bytes.get() + len);
        true
    }

    /// Takes `len` bytes at once, or refuses.
    fn charge(&self, len: usize) -> io::Result<()> {
        if self.take(len) {
            return Ok(());
        }
        self.check_alone(len)?;
        Err(self.refuse(RecordCodecError::RequestBudgetSpent))
    }

    /// Refuses once this partition alone passes the budget. No retry can help it.
    fn check_alone(&self, spilled: usize) -> io::Result<()> {
        let size = self.entry_bytes.get().saturating_add(spilled);
        if size > self.bytes {
            return Err(self.refuse(RecordCodecError::BudgetExceeded {
                size,
                limit: self.bytes,
            }));
        }
        Ok(())
    }

    fn charge_records(&self, count: usize) -> Result<()> {
        let entry = self.entry_records.get().saturating_add(count);
        let Some(remaining) = self.records_remaining.get().checked_sub(count) else {
            return Err(if entry > self.records {
                RecordCodecError::RecordBudgetExceeded {
                    count: entry,
                    limit: self.records,
                }
            } else {
                RecordCodecError::RequestBudgetSpent
            });
        };
        self.records_remaining.set(remaining);
        self.entry_records.set(entry);
        Ok(())
    }

    /// Parks the typed reason and returns the error the decoder stringifies.
    fn refuse(&self, reason: RecordCodecError) -> io::Error {
        let error = io::Error::other(reason.to_string());
        self.reason.set(Some(reason));
        error
    }

    /// The typed reason for a decoder error, when this module is what caused it.
    ///
    /// Taken rather than read, because the budget outlives one batch. Left in place, the first
    /// refusal would reclassify every later error on the same request as that same refusal.
    fn reason(&self, error: &str) -> RecordCodecError {
        self.reason
            .take()
            .unwrap_or_else(|| RecordCodecError::Batch(error.to_string()))
    }
}

/// An `io::Write` sink that stops at the budget rather than after it.
///
/// Charging the output once it exists is too late: the decompressor has already allocated it.
/// Past the budget it counts and drops the bytes, to tell "too large" (10) from "retry" (6).
struct BudgetedWriter<'a> {
    out: BytesMut,
    budget: &'a DecompressionBudget,
    spilled: usize,
}

impl<'a> BudgetedWriter<'a> {
    fn new(budget: &'a DecompressionBudget) -> Self {
        Self {
            out: BytesMut::new(),
            budget,
            spilled: 0,
        }
    }

    /// Charges `len` bytes. `false` once past the budget: counted, not kept.
    fn admit(&mut self, len: usize) -> io::Result<bool> {
        if self.spilled == 0 && self.budget.take(len) {
            return Ok(true);
        }
        self.spilled = self.spilled.saturating_add(len);
        self.budget.check_alone(self.spilled)?;
        Ok(false)
    }

    /// The output, or a refusal if any of it spilled.
    fn into_output(self) -> io::Result<Bytes> {
        if self.spilled > 0 {
            return Err(self.budget.refuse(RecordCodecError::RequestBudgetSpent));
        }
        Ok(self.out.freeze())
    }
}

impl Write for BudgetedWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.admit(buf.len())? {
            self.out.extend_from_slice(buf);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Whether a partition may hold zstd. Produce allows it from v7.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Zstd {
    Allowed,
    Refused,
}

/// Whether the first batch in `blob` is compressed. Byte 22 is the low byte of its attributes.
#[must_use]
pub fn is_compressed(blob: &[u8]) -> bool {
    blob.get(22)
        .is_some_and(|attributes| attributes & 0b111 != 0)
}

/// Decodes the one record batch a Produce partition carries.
///
/// # Errors
///
/// Returns an error when the blob holds more than one batch, when the batch is malformed or holds
/// a record count other than the one it declares, when it is a control, transactional or
/// idempotent batch, when it is zstd and `zstd` refuses it, or when it passes `budget`.
pub fn decode_batch(
    buf: &mut Bytes,
    budget: &DecompressionBudget,
    zstd: Zstd,
) -> Result<Vec<Record>> {
    budget.start_entry();
    preflight(buf, budget, zstd)?;
    if !buf.has_remaining() {
        return Ok(Vec::new());
    }

    let walked = Cell::new(0);
    let set = RecordBatchDecoder::decode_with_custom_compression(
        buf,
        Some(
            |compressed: &mut Bytes, compression| -> anyhow::Result<Bytes> {
                let (records, count) = decompress(compressed, compression, budget)?;
                walked.set(count);
                Ok(records)
            },
        ),
    )
    .map_err(|error| budget.reason(&error.to_string()))?;
    // The decoder stops at the declared count and drops the rest.
    if set.records.len() != walked.get() {
        return Err(RecordCodecError::RecordCountMismatch {
            declared: set.records.len(),
            walked: walked.get(),
        });
    }
    Ok(set.records)
}

/// Checks the batch header before anything decodes a record.
///
/// - One batch per partition, as Kafka requires from Produce v3.
/// - The decoder reserves from `record_count` (`kafka-protocol-0.18.0/src/records.rs:517`), so
///   the count must fit the blob, plus the full budget when compressed.
/// - Control, transactional and idempotent batches are refused: a stored message cannot carry
///   their flags, and nothing here stops a duplicate.
fn preflight(buf: &Bytes, budget: &DecompressionBudget, zstd: Zstd) -> Result<()> {
    let infos = RecordBatchDecoder::decode_batch_info(&mut buf.clone())
        .map_err(|error| RecordCodecError::Batch(error.to_string()))?;
    let info = match infos.as_slice() {
        [] => return Ok(()),
        [info] => info,
        several => return Err(RecordCodecError::SeveralBatches(several.len())),
    };
    if info.transactional {
        return Err(RecordCodecError::TransactionalBatch);
    }
    if info.control {
        return Err(RecordCodecError::ControlBatch);
    }
    if info.producer_id != NO_PRODUCER_ID {
        return Err(RecordCodecError::IdempotentBatch);
    }
    if zstd == Zstd::Refused && info.compression == Compression::Zstd {
        return Err(RecordCodecError::ZstdTooEarly);
    }

    let limit = if info.compression == Compression::None {
        buf.len()
    } else {
        buf.len().saturating_add(budget.bytes)
    };
    let declared = usize::try_from(info.record_count).unwrap_or(0);
    if declared.saturating_mul(MIN_RECORD_BYTES) > limit {
        return Err(RecordCodecError::RecordCountTooLarge {
            count: info.record_count,
            limit,
        });
    }
    budget.charge_records(declared)
}

/// Encodes records as one uncompressed v2 batch.
///
/// Fetch always emits uncompressed, so the read path spends no CPU on a codec the client did
/// not ask for. `BRIDGE_MAPPING.md` records that as a default open to revisiting.
///
/// Takes the records by mutable reference to normalize `sequence`. The encoder groups records
/// while `offset - sequence` holds (`kafka-protocol-0.18.0/src/records.rs:277`), and `record()`
/// pins `sequence` at `NO_SEQUENCE` while offsets advance, which breaks the group on every record
/// and spends a 61-byte batch header on each one. Numbering each sequence from the first offset
/// keeps one batch and leaves the encoded `base_sequence` at `NO_SEQUENCE`. Fetch reads a
/// partition forward, so the first record carries the lowest offset, and that is what makes the
/// encoded `base_sequence` come out at `NO_SEQUENCE` rather than at an offset.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the batch.
pub fn encode_batch(records: &mut [Record]) -> Result<Bytes> {
    let Some(base) = records.first().map(|record| record.offset) else {
        return Ok(Bytes::new());
    };
    for record in records.iter_mut() {
        let delta = record.offset.saturating_sub(base);
        record.sequence = i32::try_from(delta)
            .unwrap_or(i32::MAX)
            .saturating_add(NO_SEQUENCE);
    }

    let mut buf = BytesMut::with_capacity(batch_size(records));
    let options = RecordEncodeOptions {
        version: BATCH_VERSION,
        compression: Compression::None,
    };
    RecordBatchEncoder::encode(&mut buf, records.iter(), &options)
        .map_err(|error| RecordCodecError::Batch(error.to_string()))?;
    Ok(buf.freeze())
}

/// An upper bound on the encoded batch, so the Fetch path reserves once instead of doubling.
///
/// `kafka_protocol` never reserves: it writes every field with `put_slice` into whatever buffer
/// it is handed. Every length needed here is already in hand.
fn batch_size(records: &[Record]) -> usize {
    let field = |field: Option<&Bytes>| field.map_or(0, Bytes::len);
    BATCH_HEADER_BYTES
        + records
            .iter()
            .map(|record| {
                RECORD_FRAMING_BYTES
                    + field(record.key.as_ref())
                    + field(record.value.as_ref())
                    + record
                        .headers
                        .iter()
                        .map(|(name, value)| {
                            HEADER_FRAMING_BYTES + name.as_str().len() + field(value.as_ref())
                        })
                        .sum::<usize>()
            })
            .sum::<usize>()
}

/// Decompresses one batch, refusing the write that would pass the request budget.
///
/// `kafka_protocol`'s own decompressors write the whole stream into a growing buffer before they
/// hand it over, so the four codecs are driven from here instead. Each one writes through
/// `BudgetedWriter`, which bounds the peak rather than reporting an overrun after the fact.
///
/// The decoder calls this for every batch, uncompressed ones included, which is what makes it the
/// one place that sees the record bytes before anything reserves from what they declare.
///
/// Returns the records and how many there are.
fn decompress(
    compressed: &mut Bytes,
    compression: Compression,
    budget: &DecompressionBudget,
) -> anyhow::Result<(Bytes, usize)> {
    let body = compressed.copy_to_bytes(compressed.remaining());
    let mut writer = BudgetedWriter::new(budget);
    let records = match compression {
        Compression::None => {
            budget.charge(body.len())?;
            body
        }
        Compression::Gzip => {
            let mut decoder = flate2::write::GzDecoder::new(&mut writer);
            decoder.write_all(&body)?;
            decoder.finish()?;
            writer.into_output()?
        }
        Compression::Zstd => {
            zstd::stream::copy_decode(body.as_ref(), &mut writer)?;
            writer.into_output()?
        }
        Compression::Lz4 => {
            let mut decoder = lz4::Decoder::new(body.as_ref())?;
            io::copy(&mut decoder, &mut writer)?;
            decoder.finish().1?;
            writer.into_output()?
        }
        Compression::Snappy => {
            inflate_snappy(&body, &mut writer)?;
            writer.into_output()?
        }
    };

    match scan_records(&records, budget) {
        Ok(count) => Ok((records, count)),
        Err(reason) => Err(budget.refuse(reason).into()),
    }
}

/// Headers that cost one record slot. A decoded header takes about a third of a slot's memory.
const HEADERS_PER_SLOT: usize = 3;

/// Walks the records of one batch before the decoder reserves for them.
///
/// - Refuses a header count the record cannot hold. The decoder reserves an `IndexMap` from it
///   (`kafka-protocol-0.18.0/src/records.rs:896`), and that memory is resident.
/// - Charges headers to the record budget, [`HEADERS_PER_SLOT`] per slot.
/// - Returns the record count, which `decode_batch` checks against the batch header.
///
/// The framing mirrors `Record::decode_new`, field for field.
fn scan_records(records: &Bytes, budget: &DecompressionBudget) -> Result<usize> {
    let mut blob = records.as_ref();
    let mut walked = 0;
    let mut slots = 0;
    let mut names = Vec::new();
    while !blob.is_empty() {
        let size = take_varint(&mut blob)?;
        let size = usize::try_from(size).map_err(|_| RecordCodecError::RecordFieldLength(size))?;
        let mut record = take_bytes(&mut blob, size)?;

        take_bytes(&mut record, 1)?; // attributes
        skip_varint(&mut record, MAX_VARLONG_BYTES)?; // timestamp delta
        skip_varint(&mut record, MAX_VARINT_BYTES)?; // offset delta
        skip_field(&mut record)?; // key
        skip_field(&mut record)?; // value

        let count = take_varint(&mut record)?;
        let limit = record.len();
        let headers = usize::try_from(count)
            .ok()
            .filter(|headers| headers.saturating_mul(MIN_HEADER_BYTES) <= limit)
            .ok_or(RecordCodecError::HeaderCountTooLarge { count, limit })?;
        if headers > 1 {
            check_header_names(&mut record, headers, &mut names)?;
        }
        slots += headers / HEADERS_PER_SLOT;
        walked += 1;
    }
    budget.charge_records(slots)?;
    Ok(walked)
}

/// Refuses a record that repeats a header name. `kafka_protocol` keeps only the last value.
///
/// `names` is scratch space, reused across records.
fn check_header_names<'a>(
    record: &mut &'a [u8],
    count: usize,
    names: &mut Vec<&'a [u8]>,
) -> Result<()> {
    names.clear();
    for _ in 0..count {
        let len = take_varint(record)?;
        let len = usize::try_from(len).map_err(|_| RecordCodecError::RecordFieldLength(len))?;
        names.push(take_bytes(record, len)?);
        skip_field(record)?; // value
    }
    names.sort_unstable();
    if let Some(pair) = names.windows(2).find(|pair| pair[0] == pair[1]) {
        let name = String::from_utf8_lossy(pair[0]).into_owned();
        return Err(RecordCodecError::RepeatedHeaderName(name));
    }
    Ok(())
}

/// Reads a zigzag varint, five bytes at most, the way `kafka_protocol` reads one.
fn take_varint(buf: &mut &[u8]) -> Result<i32> {
    let mut value = 0u32;
    for index in 0..MAX_VARINT_BYTES {
        let byte = take_bytes(buf, 1)?[0];
        value |= u32::from(byte & 0x7f) << (index * 7);
        if byte < 0x80 {
            break;
        }
    }
    Ok((value >> 1).cast_signed() ^ -(value & 1).cast_signed())
}

/// Steps over a zigzag varint of up to `most` bytes, for the fields the scan does not read.
fn skip_varint(buf: &mut &[u8], most: usize) -> Result<()> {
    for _ in 0..most {
        if take_bytes(buf, 1)?[0] < 0x80 {
            break;
        }
    }
    Ok(())
}

fn take_bytes<'a>(buf: &mut &'a [u8], len: usize) -> Result<&'a [u8]> {
    let (head, rest) = buf
        .split_at_checked(len)
        .ok_or(RecordCodecError::RecordTruncated)?;
    *buf = rest;
    Ok(head)
}

/// A length-prefixed record field, where `-1` is the absent one Kafka writes for a null.
fn skip_field(buf: &mut &[u8]) -> Result<()> {
    let len = take_varint(buf)?;
    if len < -1 {
        return Err(RecordCodecError::RecordFieldLength(len));
    }
    if len > 0 {
        take_bytes(buf, usize::try_from(len).unwrap_or_default())?;
    }
    Ok(())
}

/// Kafka's snappy, and raw snappy for the producers that send it.
///
/// Snappy is the one codec that states its output size up front, which `snap` reads without
/// allocating. Charge that number before the decoder runs, because the decoder needs the whole
/// block laid out to write into and cannot be fed a bounded sink. Past the budget, blocks are
/// counted and not inflated.
fn inflate_snappy(body: &Bytes, writer: &mut BudgetedWriter<'_>) -> anyhow::Result<()> {
    let mut decoder = snap::raw::Decoder::new();
    let mut inflate = |block: &[u8], writer: &mut BudgetedWriter<'_>| -> anyhow::Result<()> {
        let declared = snap::raw::decompress_len(block)?;
        if writer.admit(declared)? {
            let start = writer.out.len();
            writer.out.resize(start.saturating_add(declared), 0);
            decoder.decompress(block, &mut writer.out[start..])?;
        }
        Ok(())
    };

    let Some(mut blocks) = body.strip_prefix(SNAPPY_MAGIC) else {
        return inflate(body.as_ref(), writer);
    };
    while !blocks.is_empty() {
        let (length, rest) = blocks
            .split_at_checked(4)
            .ok_or_else(|| anyhow::anyhow!("snappy block length is truncated"))?;
        let length = u32::from_be_bytes(length.try_into()?) as usize;
        let (block, rest) = rest
            .split_at_checked(length)
            .ok_or_else(|| anyhow::anyhow!("snappy block of {length} bytes is truncated"))?;
        inflate(block, writer)?;
        blocks = rest;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy::prelude::HeaderKind;

    const CREATE_TIME: i64 = 1_700_000_000_123;

    fn record_with(
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[(&str, Option<&[u8]>)],
    ) -> Record {
        let headers = headers
            .iter()
            .map(|(name, value)| {
                (
                    StrBytes::from_string((*name).to_string()),
                    value.map(Bytes::copy_from_slice),
                )
            })
            .collect();
        record(
            key.map(Bytes::copy_from_slice),
            value.map(Bytes::copy_from_slice),
            headers,
            0,
            CREATE_TIME,
        )
    }

    fn record_at(timestamp: i64) -> Record {
        record(
            Some(Bytes::from_static(b"k")),
            Some(Bytes::from_static(b"v")),
            IndexMap::new(),
            0,
            timestamp,
        )
    }

    fn message_with(payload: &'static [u8], headers: &[(&str, &[u8])]) -> IggyMessage {
        let headers = headers
            .iter()
            .map(|(name, value)| (header_key(name), header_value(value)))
            .collect();
        IggyMessage::builder()
            .payload(Bytes::from_static(payload))
            .user_headers(headers)
            .build()
            .unwrap()
    }

    /// The same, plus the version header that marks a message as gateway-written.
    fn gateway_message(payload: &'static [u8], headers: &[(&str, &[u8])]) -> IggyMessage {
        let mut all = vec![(VERSION_HEADER, &[MAPPING_VERSION][..])];
        all.extend_from_slice(headers);
        message_with(payload, &all)
    }

    #[test]
    fn given_a_value_only_record_when_encoded_should_match_the_general_path() {
        let plain = to_iggy(&record_with(None, Some(b"v"), &[])).unwrap();
        let general = gateway_message(b"v", &[]);

        assert_eq!(plain.user_headers, general.user_headers);
        assert_eq!(
            plain.header.user_headers_length,
            general.header.user_headers_length
        );
        assert_eq!(plain.header.origin_timestamp, 1_700_000_000_123_000);
        let back = from_iggy(&plain, 0).unwrap();
        assert_eq!(back.value.as_deref(), Some(&b"v"[..]));
        assert_eq!(back.timestamp, CREATE_TIME);
    }

    fn envelope_bytes(count: u32, trailing: &[u8]) -> Bytes {
        let mut payload = BytesMut::new();
        payload.put_u8(0);
        payload.put_u32_le(0);
        payload.put_u32_le(0);
        payload.put_u32_le(count);
        payload.put_slice(trailing);
        payload.freeze()
    }

    fn is_enveloped(message: &IggyMessage) -> bool {
        message
            .user_headers_map()
            .unwrap()
            .unwrap_or_default()
            .contains_key(&header_key(ENVELOPE_HEADER))
    }

    #[test]
    fn given_a_plain_record_when_round_tripped_should_keep_key_value_and_headers() {
        let original = record_with(Some(b"k"), Some(b"v"), &[("trace", Some(b"abc"))]);
        let message = to_iggy(&original).unwrap();
        assert!(!is_enveloped(&message));
        assert_eq!(message.payload.as_ref(), b"v");

        let back = from_iggy(&message, 7).unwrap();
        assert_eq!(back.key.as_deref(), Some(&b"k"[..]));
        assert_eq!(back.value.as_deref(), Some(&b"v"[..]));
        assert_eq!(back.offset, 7);
        assert_eq!(
            back.headers.get(&StrBytes::from_static_str("trace")),
            Some(&Some(Bytes::from_static(b"abc")))
        );
        assert_eq!(back.headers.len(), 1, "no gateway header reaches Kafka");
    }

    #[test]
    fn given_a_null_value_when_round_tripped_should_stay_null() {
        let message = to_iggy(&record_with(Some(b"k"), None, &[])).unwrap();
        assert!(
            !is_enveloped(&message),
            "a tombstone stays on the fast path"
        );
        assert_eq!(message.payload.as_ref(), PLACEHOLDER);
        assert_eq!(from_iggy(&message, 0).unwrap().value, None);
    }

    #[test]
    fn given_an_empty_value_when_round_tripped_should_stay_empty_and_not_null() {
        let message = to_iggy(&record_with(Some(b"k"), Some(b""), &[])).unwrap();
        assert_eq!(message.payload.as_ref(), PLACEHOLDER);
        assert_eq!(
            from_iggy(&message, 0).unwrap().value.as_deref(),
            Some(&[][..])
        );
    }

    #[test]
    fn given_an_empty_key_when_stored_should_take_the_envelope() {
        let original = record_with(Some(b""), Some(b"v"), &[]);
        let message = to_iggy(&original).unwrap();
        assert!(is_enveloped(&message));
        let back = from_iggy(&message, 0).unwrap();
        assert_eq!(back.key.as_deref(), Some(&[][..]), "empty, not null");
        assert_eq!(back.value.as_deref(), Some(&b"v"[..]));
    }

    #[test]
    fn given_an_oversized_key_when_stored_should_take_the_envelope() {
        let key = vec![b'x'; MAX_FIELD + 1];
        let message = to_iggy(&record_with(Some(&key), Some(b"v"), &[])).unwrap();
        assert!(is_enveloped(&message));
        assert_eq!(
            from_iggy(&message, 0).unwrap().key.as_deref(),
            Some(&key[..])
        );
    }

    #[test]
    fn given_a_null_header_value_when_stored_should_take_the_envelope() {
        let message = to_iggy(&record_with(Some(b"k"), Some(b"v"), &[("flag", None)])).unwrap();
        assert!(is_enveloped(&message));
        assert_eq!(
            from_iggy(&message, 0)
                .unwrap()
                .headers
                .get(&StrBytes::from_static_str("flag")),
            Some(&None),
            "a null header value survives the envelope as null"
        );
    }

    #[test]
    fn given_an_oversized_header_name_when_stored_should_take_the_envelope() {
        let name = "n".repeat(MAX_FIELD - HEADER_PREFIX.len() + 1);
        let message =
            to_iggy(&record_with(Some(b"k"), Some(b"v"), &[(&name, Some(b"v"))])).unwrap();
        assert!(is_enveloped(&message));
        assert!(
            from_iggy(&message, 0)
                .unwrap()
                .headers
                .contains_key(&StrBytes::from_string(name))
        );
    }

    #[test]
    fn given_an_iggy_written_message_when_encoded_should_have_a_null_key_and_its_own_headers() {
        let message = message_with(b"{}", &[("source", b"connector")]);

        let record = from_iggy(&message, 3).unwrap();
        assert_eq!(record.key, None);
        assert_eq!(record.value.as_deref(), Some(&b"{}"[..]));
        assert_eq!(
            record.headers.get(&StrBytes::from_static_str("source")),
            Some(&Some(Bytes::from_static(b"connector")))
        );
    }

    #[test]
    fn given_an_iggy_message_with_a_reserved_header_when_read_should_keep_every_header() {
        // No `kafka.v`, so nothing here was written by the gateway and the reserved namespace
        // carries no meaning. The payload is the value and `own` must not disappear.
        let message = message_with(
            b"payload",
            &[(VALUE_MARKER_HEADER, MARKER_NULL), ("own", b"1")],
        );

        let record = from_iggy(&message, 0).unwrap();
        assert_eq!(
            record.value.as_deref(),
            Some(&b"payload"[..]),
            "not a tombstone"
        );
        assert_eq!(
            record.headers.get(&StrBytes::from_static_str("own")),
            Some(&Some(Bytes::from_static(b"1")))
        );
        assert!(
            record
                .headers
                .contains_key(&StrBytes::from_static_str(VALUE_MARKER_HEADER)),
            "a reserved name an Iggy client chose passes through under that name"
        );
    }

    #[test]
    fn given_a_non_string_key_kind_when_read_should_use_the_bytes_as_the_name() {
        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::from_raw(HeaderKind::Raw, b"trace").unwrap(),
            header_value(b"abc"),
        );
        let message = IggyMessage::builder()
            .payload(Bytes::from_static(b"v"))
            .user_headers(headers)
            .build()
            .unwrap();

        assert_eq!(
            from_iggy(&message, 0)
                .unwrap()
                .headers
                .get(&StrBytes::from_static_str("trace")),
            Some(&Some(Bytes::from_static(b"abc"))),
            "other SDKs hand out Raw key constructors, and the bytes are a valid Kafka name"
        );
    }

    #[test]
    fn given_unreadable_user_headers_when_read_should_fail() {
        let mut message = to_iggy(&record_with(Some(b"k"), Some(b"v"), &[])).unwrap();
        message.user_headers = Some(Bytes::from_static(b"not a header block"));

        assert!(
            matches!(
                from_iggy(&message, 0),
                Err(RecordCodecError::UserHeadersUnreadable(_))
            ),
            "a block Iggy cannot parse must not read as a message with no headers"
        );
    }

    #[test]
    fn given_an_unknown_mapping_version_when_read_should_fail() {
        let message = message_with(b"v", &[(VERSION_HEADER, &[MAPPING_VERSION + 1])]);
        assert!(matches!(
            from_iggy(&message, 0),
            Err(RecordCodecError::MappingVersion(2))
        ));
    }

    #[test]
    fn given_an_unknown_value_marker_on_a_gateway_message_when_read_should_fail() {
        let message = gateway_message(b"v", &[(VALUE_MARKER_HEADER, b"neither")]);
        assert!(
            matches!(
                from_iggy(&message, 0),
                Err(RecordCodecError::ValueMarker(_))
            ),
            "a marker this build does not write cannot stand for null or empty"
        );
    }

    #[test]
    fn given_an_unknown_timestamp_marker_on_a_gateway_message_when_read_should_fail() {
        let message = gateway_message(b"v", &[(TIMESTAMP_MARKER_HEADER, b"later")]);
        assert!(matches!(
            from_iggy(&message, 0),
            Err(RecordCodecError::TimestampMarker(_))
        ));
    }

    #[test]
    fn given_a_create_time_when_round_tripped_should_come_back_unchanged() {
        let message = to_iggy(&record_at(CREATE_TIME)).unwrap();
        assert_eq!(
            message.header.origin_timestamp,
            CREATE_TIME.cast_unsigned() * 1000
        );
        assert_eq!(
            from_iggy(&message, 0).unwrap().timestamp,
            CREATE_TIME,
            "a record that arrived through Produce survives the round trip exactly"
        );
    }

    #[test]
    fn given_no_timestamp_when_stored_should_store_zero() {
        assert_eq!(timestamp_in(NO_TIMESTAMP).unwrap(), 0);
    }

    #[test]
    fn given_an_out_of_range_timestamp_when_stored_should_fail() {
        assert!(matches!(
            timestamp_in(i64::MAX),
            Err(RecordCodecError::TimestampOutOfRange(_))
        ));
    }

    #[test]
    fn given_an_epoch_timestamp_when_round_tripped_should_stay_at_the_epoch() {
        let mut message = to_iggy(&record_at(EPOCH_TIMESTAMP)).unwrap();
        message.header.timestamp = 5_000_000;
        assert_eq!(from_iggy(&message, 0).unwrap().timestamp, EPOCH_TIMESTAMP);
    }

    #[test]
    fn given_an_epoch_timestamp_in_an_envelope_when_read_should_stay_at_the_epoch() {
        let original = record(
            Some(Bytes::new()),
            Some(Bytes::from_static(b"v")),
            IndexMap::new(),
            0,
            EPOCH_TIMESTAMP,
        );
        let mut message = to_iggy(&original).unwrap();
        assert!(is_enveloped(&message));
        message.header.timestamp = 5_000_000;
        assert_eq!(from_iggy(&message, 0).unwrap().timestamp, EPOCH_TIMESTAMP);
    }

    #[test]
    fn given_no_timestamp_when_read_should_use_the_server_timestamp() {
        let mut message = to_iggy(&record_at(NO_TIMESTAMP)).unwrap();
        message.header.timestamp = 5_000_000;
        assert_eq!(from_iggy(&message, 0).unwrap().timestamp, 5_000);
    }

    #[test]
    fn given_a_truncated_envelope_when_decoded_should_fail() {
        let message = to_iggy(&record_with(Some(b""), Some(b"v"), &[])).unwrap();
        assert!(matches!(
            decode_envelope(&[ENVELOPE_VERSION], &message.payload.slice(0..3)),
            Err(RecordCodecError::EnvelopeTruncated { .. })
        ));
    }

    #[test]
    fn given_more_headers_than_the_payload_holds_when_decoded_should_fail() {
        // Thirteen bytes claiming four billion headers. Reserving for them is the whole risk.
        assert!(matches!(
            decode_envelope(&[ENVELOPE_VERSION], &envelope_bytes(u32::MAX, b"")),
            Err(RecordCodecError::EnvelopeTruncated { .. })
        ));
    }

    #[test]
    fn given_bytes_after_the_last_header_when_decoded_should_fail() {
        assert!(matches!(
            decode_envelope(&[ENVELOPE_VERSION], &envelope_bytes(0, b"junk")),
            Err(RecordCodecError::EnvelopeTrailingBytes(4))
        ));
    }

    #[test]
    fn given_a_gateway_envelope_that_does_not_parse_when_read_should_fail() {
        let message = gateway_message(
            b"not an envelope",
            &[(ENVELOPE_HEADER, &[ENVELOPE_VERSION])],
        );
        assert!(
            from_iggy(&message, 0).is_err(),
            "the gateway writes only envelopes that parse, so this one is corrupt"
        );
    }

    #[test]
    fn given_an_envelope_header_without_a_version_header_when_read_should_read_it_as_iggy() {
        let message = message_with(
            b"not an envelope",
            &[(ENVELOPE_HEADER, &[ENVELOPE_VERSION])],
        );

        let record = from_iggy(&message, 0).unwrap();
        assert_eq!(record.key, None, "no Kafka producer wrote this");
        assert_eq!(record.value.as_deref(), Some(&b"not an envelope"[..]));
    }

    #[test]
    fn given_a_value_filling_the_payload_when_the_envelope_is_needed_should_fail() {
        // An empty key is the cheapest field Iggy cannot hold, so this record has to take the
        // envelope, and the envelope has to carry the key alongside a value already at the cap.
        let oversized = record(
            Some(Bytes::new()),
            Some(Bytes::from(vec![b'x'; MAX_PAYLOAD_SIZE as usize])),
            IndexMap::new(),
            0,
            CREATE_TIME,
        );
        assert!(matches!(
            to_iggy(&oversized),
            Err(RecordCodecError::EnvelopeTooLarge { size })
                if size == MAX_PAYLOAD_SIZE as usize + ENVELOPE_OVERHEAD
        ));
    }

    fn encode_with(records: &[Record], compression: Compression) -> Bytes {
        let mut buf = BytesMut::new();
        let options = RecordEncodeOptions {
            version: BATCH_VERSION,
            compression,
        };
        RecordBatchEncoder::encode(&mut buf, records, &options).unwrap();
        buf.freeze()
    }

    fn record_at_offset(offset: i64, value: &[u8]) -> Record {
        record(
            Some(Bytes::from_static(b"k")),
            Some(Bytes::copy_from_slice(value)),
            IndexMap::new(),
            offset,
            CREATE_TIME,
        )
    }

    fn batch_count(batch: &Bytes) -> usize {
        RecordBatchDecoder::decode_batch_info(&mut batch.clone())
            .unwrap()
            .len()
    }

    /// Rewrites a batch header field and repairs the CRC, so the decoder reaches the field.
    ///
    /// The CRC covers every byte after it, and it is checked before the record count is read, so
    /// a patched count without a repaired CRC only ever tests CRC verification.
    fn patch_header(batch: &Bytes, offset: usize, value: &[u8]) -> Bytes {
        let mut bytes = batch.to_vec();
        bytes[offset..offset + value.len()].copy_from_slice(value);
        let crc = crc32c::crc32c(&bytes[21..]);
        bytes[17..21].copy_from_slice(&crc.to_be_bytes());
        Bytes::from(bytes)
    }

    #[test]
    fn given_an_uncompressed_batch_when_round_tripped_should_keep_every_record() {
        let mut records = vec![record_at_offset(0, b"1"), record_at_offset(1, b"2")];
        let mut encoded = encode_batch(&mut records).unwrap();
        let budget = DecompressionBudget::new(1024, usize::MAX);
        let decoded = decode_batch(&mut encoded, &budget, Zstd::Allowed).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[1].value.as_deref(), Some(&b"2"[..]));
    }

    #[test]
    fn given_records_at_distinct_offsets_when_encoded_should_make_one_batch() {
        let mut records = (0..4)
            .map(|offset| record_at_offset(offset, b"v"))
            .collect::<Vec<_>>();
        let encoded = encode_batch(&mut records).unwrap();

        assert_eq!(
            batch_count(&encoded),
            1,
            "a batch header per record costs 61 bytes of framing on every Fetch"
        );
        assert_eq!(
            records[0].sequence, NO_SEQUENCE,
            "the encoded base sequence follows the first record's"
        );
    }

    #[test]
    fn given_no_records_when_encoded_should_write_nothing() {
        assert!(encode_batch(&mut []).unwrap().is_empty());
    }

    #[test]
    fn given_two_batches_in_one_blob_when_decoded_should_reject() {
        let mut blob = BytesMut::new();
        blob.extend_from_slice(&encode_batch(&mut [record_at_offset(0, b"1")]).unwrap());
        blob.extend_from_slice(&encode_batch(&mut [record_at_offset(1, b"2")]).unwrap());

        let budget = DecompressionBudget::new(1024, usize::MAX);
        assert!(
            matches!(
                decode_batch(&mut blob.freeze(), &budget, Zstd::Allowed),
                Err(RecordCodecError::SeveralBatches(2))
            ),
            "Kafka allows one batch per partition from Produce v3"
        );
    }

    #[test]
    fn given_more_records_than_the_batch_declares_when_decoded_should_reject() {
        let batch =
            encode_batch(&mut [record_at_offset(0, b"1"), record_at_offset(1, b"2")]).unwrap();
        let mut patched = patch_header(&batch, 57, &1i32.to_be_bytes());
        let budget = DecompressionBudget::new(1024, usize::MAX);

        assert!(
            matches!(
                decode_batch(&mut patched, &budget, Zstd::Allowed),
                Err(RecordCodecError::RecordCountMismatch {
                    declared: 1,
                    walked: 2
                })
            ),
            "the decoder drops the second record, so storing the rest would lose it"
        );
    }

    #[test]
    fn given_a_batch_when_checked_for_compression_should_read_its_attributes() {
        let records = [record_at_offset(0, b"v")];
        assert!(is_compressed(&encode_with(&records, Compression::Gzip)));
        assert!(!is_compressed(&encode_with(&records, Compression::None)));
        assert!(!is_compressed(&[]));
    }

    #[test]
    fn given_a_gzip_batch_when_decoded_should_read_it() {
        let records = vec![record_at_offset(0, b"compressed")];
        let mut encoded = encode_with(&records, Compression::Gzip);
        let budget = DecompressionBudget::new(1024, usize::MAX);
        let decoded = decode_batch(&mut encoded, &budget, Zstd::Allowed).unwrap();
        assert_eq!(decoded[0].value.as_deref(), Some(&b"compressed"[..]));
    }

    #[test]
    fn given_a_snappy_batch_when_decoded_should_read_it() {
        let records = vec![record_at_offset(0, b"compressed")];
        let mut encoded = encode_with(&records, Compression::Snappy);
        let budget = DecompressionBudget::new(1024, usize::MAX);
        let decoded = decode_batch(&mut encoded, &budget, Zstd::Allowed).unwrap();
        assert_eq!(decoded[0].value.as_deref(), Some(&b"compressed"[..]));
    }

    #[test]
    fn given_an_lz4_batch_when_decoded_should_read_it() {
        let records = vec![record_at_offset(0, b"compressed")];
        let mut encoded = encode_with(&records, Compression::Lz4);
        let budget = DecompressionBudget::new(1024, usize::MAX);
        let decoded = decode_batch(&mut encoded, &budget, Zstd::Allowed).unwrap();
        assert_eq!(decoded[0].value.as_deref(), Some(&b"compressed"[..]));
    }

    #[test]
    fn given_a_zstd_batch_when_decoded_should_read_it() {
        let records = vec![record_at_offset(0, b"compressed")];
        let mut encoded = encode_with(&records, Compression::Zstd);
        let budget = DecompressionBudget::new(1024, usize::MAX);
        let decoded = decode_batch(&mut encoded, &budget, Zstd::Allowed).unwrap();
        assert_eq!(decoded[0].value.as_deref(), Some(&b"compressed"[..]));
    }

    #[test]
    fn given_a_budget_smaller_than_the_batch_when_decoded_should_reject() {
        let records = vec![record_at_offset(0, &[b'x'; 512])];
        let mut encoded = encode_with(&records, Compression::Gzip);
        let budget = DecompressionBudget::new(8, usize::MAX);
        assert!(matches!(
            decode_batch(&mut encoded, &budget, Zstd::Allowed),
            Err(RecordCodecError::BudgetExceeded { .. })
        ));
    }

    #[test]
    fn given_a_spent_budget_when_a_partition_fits_alone_should_ask_for_a_retry() {
        // Enough for one partition, not for both: the budget is per request.
        let budget = DecompressionBudget::new(400, usize::MAX);
        let big = || encode_batch(&mut [record_at_offset(0, &[b'x'; 256])]).unwrap();

        assert!(decode_batch(&mut big(), &budget, Zstd::Allowed).is_ok());
        assert!(matches!(
            decode_batch(&mut big(), &budget, Zstd::Allowed),
            Err(RecordCodecError::RequestBudgetSpent)
        ));
    }

    #[test]
    fn given_a_spent_budget_when_a_partition_is_too_large_alone_should_refuse_it() {
        // gzip writes 32 KiB at a time, so the refusal comes several writes before the end.
        let budget = DecompressionBudget::new(64 * 1024, usize::MAX);
        let mut first = encode_with(
            &[record_at_offset(0, &vec![b'a'; 60 * 1024])],
            Compression::Gzip,
        );
        let mut second = encode_with(
            &[record_at_offset(0, &vec![b'b'; 200 * 1024])],
            Compression::Gzip,
        );

        assert!(decode_batch(&mut first, &budget, Zstd::Allowed).is_ok());
        assert!(
            matches!(
                decode_batch(&mut second, &budget, Zstd::Allowed),
                Err(RecordCodecError::BudgetExceeded { limit, .. }) if limit == 64 * 1024
            ),
            "a retry cannot help a partition over the whole budget"
        );
    }

    #[test]
    fn given_a_compression_bomb_when_decoded_should_stop_before_it_is_whole() {
        let bomb = vec![0u8; 4 * 1024 * 1024];
        let mut compressed = Bytes::from(encode_gzip(&bomb));
        let budget = DecompressionBudget::new(1024, usize::MAX);

        let Err(error) = decompress(&mut compressed, Compression::Gzip, &budget) else {
            panic!("a 4 MB output against a 1 KB budget has to be refused");
        };
        drop(error);
        let Some(RecordCodecError::BudgetExceeded { size, limit }) = budget.reason.take() else {
            panic!("the budget is what refused it");
        };
        assert_eq!(limit, 1024);
        assert!(
            size < bomb.len(),
            "refused at the write that passed the budget: {size} of {} bytes",
            bomb.len()
        );
    }

    #[test]
    fn given_a_snappy_block_declaring_more_than_the_budget_when_decoded_should_reject() {
        // A raw snappy stream whose leading varint claims u32::MAX bytes of output. `snap` reads
        // that length without allocating, so the declared size is checkable before the decode.
        let mut compressed = Bytes::from_static(&[0xff, 0xff, 0xff, 0xff, 0x0f, 0x00]);
        let budget = DecompressionBudget::new(1024, usize::MAX);
        assert!(decompress(&mut compressed, Compression::Snappy, &budget).is_err());
        assert!(matches!(
            budget.reason.take(),
            Some(RecordCodecError::BudgetExceeded { size, .. }) if size == u32::MAX as usize
        ));
    }

    #[test]
    fn given_a_record_count_past_the_frame_when_decoded_should_reject() {
        let batch = encode_batch(&mut [record_at_offset(0, b"v")]).unwrap();
        let mut patched = patch_header(&batch, 57, &i32::MAX.to_be_bytes());
        let budget = DecompressionBudget::new(1024, usize::MAX);

        assert!(
            matches!(
                decode_batch(&mut patched, &budget, Zstd::Allowed),
                Err(RecordCodecError::RecordCountTooLarge { .. })
            ),
            "the decoder reserves from this count before it reads a record"
        );
    }

    #[test]
    fn given_a_compressed_batch_when_preflighting_should_grant_it_the_whole_budget() {
        // 100 tiny records declare more than the blob and the bytes left can hold, and fit the
        // whole budget. Earlier partitions spending it must not make them look malformed.
        // One offset for all, so the encoder writes one batch.
        let records: Vec<_> = (0..100).map(|_| record_at_offset(0, b"")).collect();
        let mut batch = encode_with(&records, Compression::Gzip);
        let budget = DecompressionBudget::new(4096, usize::MAX);
        let mut spend = encode_batch(&mut [record_at_offset(0, &[b'x'; 4000])]).unwrap();

        assert!(decode_batch(&mut spend, &budget, Zstd::Allowed).is_ok());
        assert!(matches!(
            decode_batch(&mut batch, &budget, Zstd::Allowed),
            Err(RecordCodecError::RequestBudgetSpent)
        ));
    }

    #[test]
    fn given_many_headers_when_decoded_should_charge_them_as_records() {
        let headers: Vec<_> = (0..6)
            .map(|index| (format!("h{index}"), Some(&b"v"[..])))
            .collect();
        let headers: Vec<_> = headers
            .iter()
            .map(|(name, value)| (name.as_str(), *value))
            .collect();
        let mut batch = encode_batch(&mut [record_with(None, Some(b"v"), &headers)]).unwrap();

        // One slot for the record and two for its six headers.
        let tight = DecompressionBudget::new(1024, 2);
        assert!(matches!(
            decode_batch(&mut batch.clone(), &tight, Zstd::Allowed),
            Err(RecordCodecError::RecordBudgetExceeded { count: 3, limit: 2 })
        ));
        let enough = DecompressionBudget::new(1024, 3);
        assert!(decode_batch(&mut batch, &enough, Zstd::Allowed).is_ok());
    }

    /// A one-record v2 batch whose record body declares `headers` headers and carries none.
    ///
    /// Built by hand because no encoder writes that, and it is the shape that reaches
    /// `IndexMap::with_capacity` inside kafka-protocol.
    fn batch_declaring_headers(headers: i32) -> Bytes {
        fn put_varint(buf: &mut BytesMut, value: i32) {
            let mut zigzag = ((value << 1) ^ (value >> 31)).cast_unsigned();
            while zigzag >= 0x80 {
                buf.put_u8(u8::try_from(zigzag & 0x7f).unwrap() | 0x80);
                zigzag >>= 7;
            }
            buf.put_u8(u8::try_from(zigzag).unwrap());
        }

        let mut record = BytesMut::new();
        record.put_u8(0); // attributes
        put_varint(&mut record, 0); // timestamp delta
        put_varint(&mut record, 0); // offset delta
        put_varint(&mut record, -1); // null key
        put_varint(&mut record, -1); // null value
        put_varint(&mut record, headers);

        let mut records = BytesMut::new();
        put_varint(&mut records, i32::try_from(record.len()).unwrap());
        records.extend_from_slice(&record);

        // Everything from the attributes field on, which is what the CRC covers.
        let mut body = BytesMut::new();
        body.put_i16(0); // attributes: no compression, CreateTime
        body.put_i32(0); // last offset delta
        body.put_i64(CREATE_TIME); // first timestamp
        body.put_i64(CREATE_TIME); // max timestamp
        body.put_i64(NO_PRODUCER_ID);
        body.put_i16(NO_PRODUCER_EPOCH);
        body.put_i32(NO_SEQUENCE);
        body.put_i32(1); // record count
        body.extend_from_slice(&records);

        let mut batch = BytesMut::new();
        batch.put_i64(0); // base offset
        batch.put_i32(i32::try_from(body.len() + 9).unwrap()); // batch length, from leader epoch on
        batch.put_i32(NO_PARTITION_LEADER_EPOCH);
        batch.put_i8(BATCH_VERSION);
        batch.put_u32(crc32c::crc32c(&body));
        batch.extend_from_slice(&body);
        batch.freeze()
    }

    #[test]
    fn given_a_repeated_header_name_when_decoded_should_reject() {
        let headers = [("a", Some(&b"x"[..])), ("b", Some(&b"y"[..]))];
        let mut records = [record_with(None, Some(b"v"), &headers)];
        let batch = encode_batch(&mut records).unwrap();
        // Rename header "b" to "a". Same length, so only the CRC needs repair.
        let at = batch
            .windows(4)
            .position(|window| window == [0x02, b'b', 0x02, b'y'])
            .unwrap();
        let mut batch = patch_header(&batch, at + 1, b"a");
        let budget = DecompressionBudget::new(1024, usize::MAX);

        assert!(matches!(
            decode_batch(&mut batch, &budget, Zstd::Allowed),
            Err(RecordCodecError::RepeatedHeaderName(name)) if name == "a"
        ));
    }

    #[test]
    fn given_a_header_count_past_the_record_when_decoded_should_reject() {
        // The batch header says one record, so the record count bound passes. The count that
        // matters is the one inside the record, and no batch header reports it.
        let mut batch = batch_declaring_headers(i32::MAX);
        let budget = DecompressionBudget::new(8 * 1024 * 1024, usize::MAX);

        assert!(
            matches!(
                decode_batch(&mut batch, &budget, Zstd::Allowed),
                Err(RecordCodecError::HeaderCountTooLarge { count, .. }) if count == i32::MAX
            ),
            "this reserve is resident memory, not address space"
        );
    }

    #[test]
    fn given_a_hand_built_batch_when_its_header_count_fits_should_decode() {
        // The same builder with a count the record can hold, so the scan cannot be passing the
        // test above by rejecting every hand-built batch.
        let mut batch = batch_declaring_headers(0);
        let budget = DecompressionBudget::new(1024, usize::MAX);
        assert_eq!(
            decode_batch(&mut batch, &budget, Zstd::Allowed)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn given_records_carrying_headers_when_round_tripped_should_keep_them() {
        // Exercises the header framing the scan walks, on a batch an encoder wrote.
        let mut with_headers = vec![record_with(
            Some(b"k"),
            Some(b"v"),
            &[("trace", Some(b"abc"))],
        )];
        let mut encoded = encode_batch(&mut with_headers).unwrap();
        let budget = DecompressionBudget::new(1024, usize::MAX);

        let decoded = decode_batch(&mut encoded, &budget, Zstd::Allowed).unwrap();
        assert_eq!(
            decoded[0].headers.get(&StrBytes::from_static_str("trace")),
            Some(&Some(Bytes::from_static(b"abc")))
        );
    }

    #[test]
    fn given_an_uncompressed_batch_when_preflighting_should_not_grant_it_the_budget() {
        // One record's worth of bytes, declaring far more records than it holds. The budget is
        // the documented 8 MiB, which an uncompressed batch has no claim on.
        let batch = encode_batch(&mut [record_at_offset(0, b"v")]).unwrap();
        let mut patched = patch_header(&batch, 57, &100_000i32.to_be_bytes());
        let budget = DecompressionBudget::new(8 * 1024 * 1024, usize::MAX);

        assert!(matches!(
            decode_batch(&mut patched, &budget, Zstd::Allowed),
            Err(RecordCodecError::RecordCountTooLarge { limit, .. }) if limit == batch.len()
        ));
    }

    #[test]
    fn given_two_key_kinds_naming_one_header_on_a_gateway_message_when_read_should_fail() {
        let mut headers = BTreeMap::new();
        headers.insert(header_key(VERSION_HEADER), header_value(&[MAPPING_VERSION]));
        headers.insert(header_key("kafka.h.trace"), header_value(b"string"));
        headers.insert(
            HeaderKey::from_raw(HeaderKind::Raw, b"kafka.h.trace").unwrap(),
            header_value(b"raw"),
        );
        let message = IggyMessage::builder()
            .payload(Bytes::from_static(b"v"))
            .user_headers(headers)
            .build()
            .unwrap();

        assert!(
            matches!(
                from_iggy(&message, 0),
                Err(RecordCodecError::HeaderNameCollision(_))
            ),
            "to_iggy writes one key kind, so a collision means the message is not what it claims"
        );
    }

    #[test]
    fn given_two_key_kinds_naming_one_header_on_an_iggy_message_when_read_should_keep_one() {
        let mut headers = BTreeMap::new();
        headers.insert(header_key("trace"), header_value(b"string"));
        headers.insert(
            HeaderKey::from_raw(HeaderKind::Raw, b"trace").unwrap(),
            header_value(b"raw"),
        );
        let message = IggyMessage::builder()
            .payload(Bytes::from_static(b"v"))
            .user_headers(headers)
            .build()
            .unwrap();

        let record = from_iggy(&message, 0).unwrap();
        assert_eq!(
            record.headers.len(),
            1,
            "a Record keys headers in an IndexMap, so the pair cannot both survive"
        );
        assert!(
            record
                .headers
                .contains_key(&StrBytes::from_static_str("trace")),
            "refusing the message instead would stall the partition for every Kafka consumer"
        );
    }

    #[test]
    fn given_a_transactional_batch_when_decoded_should_reject() {
        let mut transactional = record_at_offset(0, b"v");
        transactional.transactional = true;
        let mut encoded = encode_with(&[transactional], Compression::None);
        let budget = DecompressionBudget::new(1024, usize::MAX);

        assert!(matches!(
            decode_batch(&mut encoded, &budget, Zstd::Allowed),
            Err(RecordCodecError::TransactionalBatch)
        ));
    }

    #[test]
    fn given_a_control_batch_when_decoded_should_reject() {
        let mut control = record_at_offset(0, b"v");
        control.control = true;
        let mut encoded = encode_with(&[control], Compression::None);
        let budget = DecompressionBudget::new(1024, usize::MAX);

        assert!(
            matches!(
                decode_batch(&mut encoded, &budget, Zstd::Allowed),
                Err(RecordCodecError::ControlBatch)
            ),
            "consumers filter control records by a flag no stored message can carry"
        );
    }

    #[test]
    fn given_a_truncated_batch_when_decoded_should_reject() {
        let batch = encode_batch(&mut [record_at_offset(0, b"value")]).unwrap();
        let mut truncated = batch.slice(0..batch.len() - 4);
        let budget = DecompressionBudget::new(1024, usize::MAX);
        assert!(decode_batch(&mut truncated, &budget, Zstd::Allowed).is_err());
    }

    #[test]
    fn given_a_corrupt_batch_crc_when_decoded_should_reject() {
        let batch = encode_batch(&mut [record_at_offset(0, b"value")]).unwrap();
        let mut bytes = batch.to_vec();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        let mut corrupt = Bytes::from(bytes);

        let budget = DecompressionBudget::new(1024, usize::MAX);
        assert!(matches!(
            decode_batch(&mut corrupt, &budget, Zstd::Allowed),
            Err(RecordCodecError::Batch(_))
        ));
    }

    #[test]
    fn given_an_earlier_overrun_when_a_later_batch_fails_should_not_reuse_the_budget_reason() {
        // A failed charge deducts nothing, so the budget still has room for the batch below.
        let budget = DecompressionBudget::new(64, usize::MAX);
        let mut bomb = encode_with(&[record_at_offset(0, &[b'x'; 512])], Compression::Gzip);
        assert!(matches!(
            decode_batch(&mut bomb, &budget, Zstd::Allowed),
            Err(RecordCodecError::BudgetExceeded { .. })
        ));

        // One record more than the batch holds. The count clears the preflight bound, so the
        // failure comes out of the record decoder, which is the path that reads the overflow.
        let batch = encode_batch(&mut [record_at_offset(0, b"value")]).unwrap();
        let mut short = patch_header(&batch, 57, &2i32.to_be_bytes());

        assert!(
            matches!(
                decode_batch(&mut short, &budget, Zstd::Allowed),
                Err(RecordCodecError::Batch(_))
            ),
            "a stale overflow reports a malformed batch as MESSAGE_TOO_LARGE"
        );
    }

    fn encode_gzip(body: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(body).unwrap();
        encoder.finish().unwrap()
    }
}
