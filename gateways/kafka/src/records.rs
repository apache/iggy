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
    #[error("batch declares {count} records, and {limit} bytes can hold fewer")]
    RecordCountTooLarge { count: i32, limit: usize },
    #[error("{0} batches are out of scope")]
    UnsupportedBatch(&'static str),
    #[error("decompressed {produced} bytes with {remaining} left in the request budget")]
    BudgetExceeded { produced: usize, remaining: usize },
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
/// A message with `kafka.v` was written here, so every marker on it is authoritative and one this
/// build does not recognize is an error rather than a guess.
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
/// A repeated header name is on the list in `BRIDGE_MAPPING.md` and is absent here, because
/// `kafka_protocol` decodes headers into an `IndexMap` (`records.rs:919`). A repeat overwrites
/// its earlier entry before this code runs, so the case cannot be observed.
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
        headers.insert(
            StrBytes::from_string(name.to_string()),
            Some(stored_value.value()),
        );
    }
    Ok((key, value, headers))
}

/// The reading of a message an Iggy client wrote, which no marker on it can change.
///
/// Every header passes through under its own name, including one in the `kafka.` namespace. The
/// alternative was to treat any namespaced header as gateway metadata, which made a single
/// `kafka.`-prefixed header hide every other header on the message.
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
/// Charged across every batch in the request, because one frame carries many batches and a cap
/// applied to each on its own admits as many multiples of it as the frame holds entries.
///
/// The budget bounds the peak, not just the total. Every decompressor here writes through
/// `BudgetedWriter`, which refuses the write that would pass the budget, so an over-budget frame
/// never reaches its full decompressed size in memory.
pub struct DecompressionBudget {
    remaining: Cell<usize>,
    /// What the charge was when it first tripped. Decompression reports failure through
    /// `io::Error` and `anyhow`, which the decoder stringifies, so the typed reason is kept here.
    overflow: Cell<Option<(usize, usize)>>,
}

impl DecompressionBudget {
    #[must_use]
    pub const fn new(bytes: usize) -> Self {
        Self {
            remaining: Cell::new(bytes),
            overflow: Cell::new(None),
        }
    }

    fn charge(&self, produced: usize) -> io::Result<()> {
        let remaining = self.remaining.get();
        if produced > remaining {
            self.overflow.set(Some((produced, remaining)));
            return Err(io::Error::other(format!(
                "decompressed {produced} bytes with {remaining} left in the budget"
            )));
        }
        self.remaining.set(remaining - produced);
        Ok(())
    }

    /// The typed reason for a decoder error, when the budget is what caused it.
    ///
    /// Taken rather than read, because the budget outlives one batch. Left in place, the first
    /// overrun would reclassify every later error on the same request as a budget overrun.
    fn overflow(&self, error: &str) -> RecordCodecError {
        self.overflow.take().map_or_else(
            || RecordCodecError::Batch(error.to_string()),
            |(produced, remaining)| RecordCodecError::BudgetExceeded {
                produced,
                remaining,
            },
        )
    }
}

/// An `io::Write` sink that stops at the budget rather than after it.
///
/// Charging the output once it exists is too late: the decompressor has already allocated it.
struct BudgetedWriter<'a> {
    out: BytesMut,
    budget: &'a DecompressionBudget,
}

impl<'a> BudgetedWriter<'a> {
    fn new(budget: &'a DecompressionBudget) -> Self {
        Self {
            out: BytesMut::new(),
            budget,
        }
    }
}

impl Write for BudgetedWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.budget.charge(buf.len())?;
        self.out.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Decodes every record batch a Produce partition entry carries.
///
/// A partition's `records` field is one blob that holds one or more batches back to back, so
/// this drains `buf` rather than reading a single batch.
///
/// # Errors
///
/// Returns an error when a batch is malformed, when it declares more records than the request
/// can hold, when it is a control or transactional batch, or when the request decompresses to
/// more than `budget` allows.
pub fn decode_batches(buf: &mut Bytes, budget: &DecompressionBudget) -> Result<Vec<Record>> {
    preflight(buf, budget)?;

    let mut records = Vec::new();
    while buf.has_remaining() {
        let set = RecordBatchDecoder::decode_with_custom_compression(
            buf,
            Some(|compressed: &mut Bytes, compression| decompress(compressed, compression, budget)),
        )
        .map_err(|error| budget.overflow(&error.to_string()))?;
        records.extend(set.records);
    }
    Ok(records)
}

/// Reads the batch headers before anything decodes a record.
///
/// `RecordBatchDecoder` reserves from the batch header's record count before it reads the first
/// record (`kafka-protocol-0.18.0/src/records.rs:517`), and that count is checked for sign only.
/// A 61-byte batch can therefore ask for `i32::MAX` records. The frame plus what the budget still
/// allows is the most the records of the whole blob can come to, so the counts are charged against
/// that together rather than one batch at a time: a blob holds many batches, and each reserve
/// lands in the same `Vec`. Reading the headers costs one `Bytes` clone, which is a refcount.
///
/// What is left is the ratio between a minimal wire record and a decoded `Record`, which one
/// legitimate request pays as well. This bounds the count, not that ratio.
///
/// Control and transactional batches are refused in the same pass. `record()` cannot carry either
/// flag, so a control batch admitted here would reach consumers as ordinary data, and consumers
/// filter control records by exactly that flag.
fn preflight(buf: &Bytes, budget: &DecompressionBudget) -> Result<()> {
    let mut headers = buf.clone();
    let infos = RecordBatchDecoder::decode_batch_info(&mut headers)
        .map_err(|error| RecordCodecError::Batch(error.to_string()))?;

    let limit = buf.len().saturating_add(budget.remaining.get());
    let mut declared = 0usize;
    for info in &infos {
        if info.transactional {
            return Err(RecordCodecError::UnsupportedBatch("transactional"));
        }
        if info.control {
            return Err(RecordCodecError::UnsupportedBatch("control"));
        }
        declared = declared.saturating_add(usize::try_from(info.record_count).unwrap_or(0));
        if declared.saturating_mul(MIN_RECORD_BYTES) > limit {
            return Err(RecordCodecError::RecordCountTooLarge {
                count: info.record_count,
                limit,
            });
        }
    }
    Ok(())
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
fn decompress(
    compressed: &mut Bytes,
    compression: Compression,
    budget: &DecompressionBudget,
) -> anyhow::Result<Bytes> {
    let body = compressed.copy_to_bytes(compressed.remaining());
    let mut writer = BudgetedWriter::new(budget);
    match compression {
        Compression::None => {
            budget.charge(body.len())?;
            return Ok(body);
        }
        Compression::Gzip => {
            let mut decoder = flate2::write::GzDecoder::new(&mut writer);
            decoder.write_all(&body)?;
            decoder.finish()?;
        }
        Compression::Zstd => zstd::stream::copy_decode(body.as_ref(), &mut writer)?,
        Compression::Lz4 => {
            let mut decoder = lz4::Decoder::new(body.as_ref())?;
            io::copy(&mut decoder, &mut writer)?;
            decoder.finish().1?;
        }
        Compression::Snappy => inflate_snappy(&body, &mut writer)?,
    }
    Ok(writer.out.freeze())
}

/// Kafka's snappy, and raw snappy for the producers that send it.
///
/// Snappy is the one codec that states its output size up front, which `snap` reads without
/// allocating. Charge that number before the decoder runs, because the decoder needs the whole
/// block laid out to write into and cannot be fed a bounded sink.
fn inflate_snappy(body: &Bytes, writer: &mut BudgetedWriter<'_>) -> anyhow::Result<()> {
    let mut decoder = snap::raw::Decoder::new();
    let mut inflate = |block: &[u8], writer: &mut BudgetedWriter<'_>| -> anyhow::Result<()> {
        let declared = snap::raw::decompress_len(block)?;
        writer.budget.charge(declared)?;
        let start = writer.out.len();
        writer.out.resize(start.saturating_add(declared), 0);
        decoder.decompress(block, &mut writer.out[start..])?;
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
        let budget = DecompressionBudget::new(1024);
        let decoded = decode_batches(&mut encoded, &budget).unwrap();
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
    fn given_two_batches_in_one_blob_when_decoded_should_drain_both() {
        let mut blob = BytesMut::new();
        blob.extend_from_slice(&encode_batch(&mut [record_at_offset(0, b"1")]).unwrap());
        blob.extend_from_slice(&encode_batch(&mut [record_at_offset(1, b"2")]).unwrap());

        let budget = DecompressionBudget::new(1024);
        let decoded = decode_batches(&mut blob.freeze(), &budget).unwrap();
        assert_eq!(decoded.len(), 2, "a partition blob can hold many batches");
    }

    #[test]
    fn given_a_gzip_batch_when_decoded_should_read_it() {
        let records = vec![record_at_offset(0, b"compressed")];
        let mut encoded = encode_with(&records, Compression::Gzip);
        let budget = DecompressionBudget::new(1024);
        let decoded = decode_batches(&mut encoded, &budget).unwrap();
        assert_eq!(decoded[0].value.as_deref(), Some(&b"compressed"[..]));
    }

    #[test]
    fn given_a_snappy_batch_when_decoded_should_read_it() {
        let records = vec![record_at_offset(0, b"compressed")];
        let mut encoded = encode_with(&records, Compression::Snappy);
        let budget = DecompressionBudget::new(1024);
        let decoded = decode_batches(&mut encoded, &budget).unwrap();
        assert_eq!(decoded[0].value.as_deref(), Some(&b"compressed"[..]));
    }

    #[test]
    fn given_an_lz4_batch_when_decoded_should_read_it() {
        let records = vec![record_at_offset(0, b"compressed")];
        let mut encoded = encode_with(&records, Compression::Lz4);
        let budget = DecompressionBudget::new(1024);
        let decoded = decode_batches(&mut encoded, &budget).unwrap();
        assert_eq!(decoded[0].value.as_deref(), Some(&b"compressed"[..]));
    }

    #[test]
    fn given_a_zstd_batch_when_decoded_should_read_it() {
        let records = vec![record_at_offset(0, b"compressed")];
        let mut encoded = encode_with(&records, Compression::Zstd);
        let budget = DecompressionBudget::new(1024);
        let decoded = decode_batches(&mut encoded, &budget).unwrap();
        assert_eq!(decoded[0].value.as_deref(), Some(&b"compressed"[..]));
    }

    #[test]
    fn given_a_budget_smaller_than_the_batch_when_decoded_should_reject() {
        let records = vec![record_at_offset(0, &[b'x'; 512])];
        let mut encoded = encode_with(&records, Compression::Gzip);
        let budget = DecompressionBudget::new(8);
        assert!(matches!(
            decode_batches(&mut encoded, &budget),
            Err(RecordCodecError::BudgetExceeded { .. })
        ));
    }

    #[test]
    fn given_two_batches_when_the_second_passes_the_budget_should_reject() {
        let big = record_at_offset(0, &[b'x'; 256]);
        let mut blob = BytesMut::new();
        blob.extend_from_slice(&encode_batch(&mut [big.clone()]).unwrap());
        blob.extend_from_slice(&encode_batch(&mut [big]).unwrap());

        // Enough for one batch, not for both: the budget is per request, not per batch.
        let budget = DecompressionBudget::new(400);
        assert!(matches!(
            decode_batches(&mut blob.freeze(), &budget),
            Err(RecordCodecError::BudgetExceeded { .. })
        ));
    }

    #[test]
    fn given_a_compression_bomb_when_decoded_should_stop_before_it_is_whole() {
        let bomb = vec![0u8; 4 * 1024 * 1024];
        let mut compressed = Bytes::from(encode_gzip(&bomb));
        let budget = DecompressionBudget::new(1024);

        let Err(error) = decompress(&mut compressed, Compression::Gzip, &budget) else {
            panic!("a 4 MB output against a 1 KB budget has to be refused");
        };
        drop(error);
        let Some((produced, remaining)) = budget.overflow.take() else {
            panic!("the budget is what refused it");
        };
        assert_eq!(remaining, 1024);
        assert!(
            produced < bomb.len(),
            "the write that passed the budget was refused, not charged afterwards: \
             {produced} of {} bytes",
            bomb.len()
        );
    }

    #[test]
    fn given_a_snappy_block_declaring_more_than_the_budget_when_decoded_should_reject() {
        // A raw snappy stream whose leading varint claims u32::MAX bytes of output. `snap` reads
        // that length without allocating, so the declared size is checkable before the decode.
        let mut compressed = Bytes::from_static(&[0xff, 0xff, 0xff, 0xff, 0x0f, 0x00]);
        let budget = DecompressionBudget::new(1024);
        assert!(decompress(&mut compressed, Compression::Snappy, &budget).is_err());
        assert_eq!(
            budget.overflow.take().map(|(produced, _)| produced),
            Some(u32::MAX as usize)
        );
    }

    #[test]
    fn given_a_record_count_past_the_frame_when_decoded_should_reject() {
        let batch = encode_batch(&mut [record_at_offset(0, b"v")]).unwrap();
        let mut patched = patch_header(&batch, 57, &i32::MAX.to_be_bytes());
        let budget = DecompressionBudget::new(1024);

        assert!(
            matches!(
                decode_batches(&mut patched, &budget),
                Err(RecordCodecError::RecordCountTooLarge { .. })
            ),
            "the decoder reserves from this count before it reads a record"
        );
    }

    #[test]
    fn given_counts_that_only_pass_one_at_a_time_when_decoded_should_reject() {
        // One large batch raises what the blob can hold, and every small batch then declares a
        // count that clears the bound on its own. The reserves land in one `Vec` all the same.
        let big = encode_batch(&mut [record_at_offset(0, &[b'x'; 2048])]).unwrap();
        let small = patch_header(
            &encode_batch(&mut [record_at_offset(0, b"v")]).unwrap(),
            57,
            &200i32.to_be_bytes(),
        );

        let mut blob = BytesMut::new();
        blob.extend_from_slice(&big);
        for _ in 0..8 {
            blob.extend_from_slice(&small);
        }

        let budget = DecompressionBudget::new(0);
        assert!(matches!(
            decode_batches(&mut blob.freeze(), &budget),
            Err(RecordCodecError::RecordCountTooLarge { .. })
        ));
    }

    #[test]
    fn given_a_transactional_batch_when_decoded_should_reject() {
        let mut transactional = record_at_offset(0, b"v");
        transactional.transactional = true;
        let mut encoded = encode_with(&[transactional], Compression::None);
        let budget = DecompressionBudget::new(1024);

        assert!(matches!(
            decode_batches(&mut encoded, &budget),
            Err(RecordCodecError::UnsupportedBatch("transactional"))
        ));
    }

    #[test]
    fn given_a_control_batch_when_decoded_should_reject() {
        let mut control = record_at_offset(0, b"v");
        control.control = true;
        let mut encoded = encode_with(&[control], Compression::None);
        let budget = DecompressionBudget::new(1024);

        assert!(
            matches!(
                decode_batches(&mut encoded, &budget),
                Err(RecordCodecError::UnsupportedBatch("control"))
            ),
            "consumers filter control records by a flag no stored message can carry"
        );
    }

    #[test]
    fn given_a_truncated_batch_when_decoded_should_reject() {
        let batch = encode_batch(&mut [record_at_offset(0, b"value")]).unwrap();
        let mut truncated = batch.slice(0..batch.len() - 4);
        let budget = DecompressionBudget::new(1024);
        assert!(decode_batches(&mut truncated, &budget).is_err());
    }

    #[test]
    fn given_a_corrupt_batch_crc_when_decoded_should_reject() {
        let batch = encode_batch(&mut [record_at_offset(0, b"value")]).unwrap();
        let mut bytes = batch.to_vec();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        let mut corrupt = Bytes::from(bytes);

        let budget = DecompressionBudget::new(1024);
        assert!(matches!(
            decode_batches(&mut corrupt, &budget),
            Err(RecordCodecError::Batch(_))
        ));
    }

    #[test]
    fn given_an_earlier_overrun_when_a_later_batch_fails_should_not_reuse_the_budget_reason() {
        // A failed charge deducts nothing, so the budget still has room for the batch below.
        let budget = DecompressionBudget::new(64);
        let mut bomb = encode_with(&[record_at_offset(0, &[b'x'; 512])], Compression::Gzip);
        assert!(matches!(
            decode_batches(&mut bomb, &budget),
            Err(RecordCodecError::BudgetExceeded { .. })
        ));

        // One record more than the batch holds. The count clears the preflight bound, so the
        // failure comes out of the record decoder, which is the path that reads the overflow.
        let batch = encode_batch(&mut [record_at_offset(0, b"value")]).unwrap();
        let mut short = patch_header(&batch, 57, &2i32.to_be_bytes());

        assert!(
            matches!(
                decode_batches(&mut short, &budget),
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
