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
//! no batch framing, no Iggy calls, no handler wiring.

use std::cell::Cell;
use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use iggy::prelude::{HeaderKey, HeaderValue, IggyError, IggyMessage};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, Record,
    RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use thiserror::Error;

/// Iggy header carrying the Kafka record key.
pub const KEY_HEADER: &str = "kafka.key";
/// Iggy header naming which of null or empty a placeholder payload stands for.
pub const VALUE_MARKER_HEADER: &str = "kafka.value";
/// Prefix every Kafka record header name is stored under.
pub const HEADER_PREFIX: &str = "kafka.h.";
/// Iggy header whose one-byte value is the envelope format version.
pub const ENVELOPE_HEADER: &str = "kafka.envelope";
/// Envelope format version this build writes and reads.
pub const ENVELOPE_VERSION: u8 = 1;

/// Kafka sends this for a record with no timestamp.
const NO_TIMESTAMP: i64 = -1;
/// Stored in place of a null or empty value, discarded on the way back.
const PLACEHOLDER: &[u8] = &[0x00];
/// Iggy caps one header name and one header value at this many bytes.
const MAX_FIELD: usize = 255;

const MARKER_NULL: &[u8] = b"null";
const MARKER_EMPTY: &[u8] = b"empty";

const FLAG_KEY: u8 = 0b01;
const FLAG_VALUE: u8 = 0b10;

/// Record batch version this gateway writes. v2 is the only shape `kafka_protocol` encodes.
const BATCH_VERSION: i8 = 2;

#[derive(Debug, Error)]
pub enum RecordCodecError {
    #[error("Iggy rejected the message: {0}")]
    Iggy(#[from] IggyError),
    #[error("record timestamp {0} ms does not fit Iggy's microsecond field")]
    TimestampOutOfRange(i64),
    #[error("envelope is truncated: needed {needed} bytes, {remaining} remain")]
    EnvelopeTruncated { needed: usize, remaining: usize },
    #[error("envelope format version {0} is not {ENVELOPE_VERSION}")]
    EnvelopeVersion(u8),
    #[error("envelope header name is not UTF-8")]
    EnvelopeHeaderName,
    #[error("record batch is malformed: {0}")]
    Batch(String),
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
/// Returns an error when the timestamp does not fit, or when Iggy rejects the message for a
/// reason the envelope does not fix, such as a payload over `MAX_PAYLOAD_SIZE`.
pub fn to_iggy(record: &Record) -> Result<IggyMessage> {
    if needs_envelope(record) {
        return envelope_message(record);
    }
    let (payload, marker) = split_value(record.value.as_ref());
    let mut headers = BTreeMap::new();
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
/// A message with no `kafka.` headers was written by an Iggy client, not through this gateway.
/// It gets a null key and its own user headers.
///
/// # Errors
///
/// Returns an error when the message carries a malformed envelope.
pub fn from_iggy(message: &IggyMessage, offset: i64) -> Result<Record> {
    let stored = message.user_headers_map()?.unwrap_or_default();
    let envelope = stored.get(&header_key(ENVELOPE_HEADER));
    let (key, value, headers) = if let Some(version) = envelope {
        decode_envelope(version.value().as_ref(), &message.payload)?
    } else {
        native_fields(&stored, message)
    };
    Ok(record(key, value, headers, offset, timestamp_out(message)))
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
    let mut headers = BTreeMap::new();
    headers.insert(
        header_key(ENVELOPE_HEADER),
        header_value(&[ENVELOPE_VERSION]),
    );
    build(encode_envelope(record), headers, record.timestamp)?
        .ok_or(IggyError::TooBigUserHeaders)
        .map_err(Into::into)
}

/// 13 bytes of fixed overhead plus 9 per header, little-endian throughout.
fn encode_envelope(record: &Record) -> Bytes {
    let mut flags = 0u8;
    if record.key.is_some() {
        flags |= FLAG_KEY;
    }
    if record.value.is_some() {
        flags |= FLAG_VALUE;
    }

    let mut buf = BytesMut::new();
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

type EnvelopeFields = (
    Option<Bytes>,
    Option<Bytes>,
    IndexMap<StrBytes, Option<Bytes>>,
);

fn decode_envelope(version: &[u8], payload: &Bytes) -> Result<EnvelopeFields> {
    match version.first() {
        Some(&ENVELOPE_VERSION) => {}
        Some(&other) => return Err(RecordCodecError::EnvelopeVersion(other)),
        None => return Err(RecordCodecError::EnvelopeVersion(0)),
    }

    let mut buf = payload.clone();
    let flags = take(&mut buf, 1)?[0];
    let key = take_field(&mut buf)?;
    let value = take_field(&mut buf)?;
    let count = u32::from_le_bytes(take(&mut buf, 4)?.as_ref().try_into().unwrap_or_default());

    let mut headers = IndexMap::with_capacity(count as usize);
    for _ in 0..count {
        let name = take_field(&mut buf)?;
        let name =
            String::from_utf8(name.to_vec()).map_err(|_| RecordCodecError::EnvelopeHeaderName)?;
        let present = take(&mut buf, 1)?[0] != 0;
        let value = take_field(&mut buf)?;
        headers.insert(StrBytes::from_string(name), present.then_some(value));
    }

    Ok((
        (flags & FLAG_KEY != 0).then_some(key),
        (flags & FLAG_VALUE != 0).then_some(value),
        headers,
    ))
}

fn native_fields(
    stored: &BTreeMap<HeaderKey, HeaderValue>,
    message: &IggyMessage,
) -> EnvelopeFields {
    let gateway_written = stored
        .keys()
        .any(|key| key.as_str().is_ok_and(|key| key.starts_with("kafka.")));
    let key = stored.get(&header_key(KEY_HEADER)).map(HeaderValue::value);
    let value = match stored
        .get(&header_key(VALUE_MARKER_HEADER))
        .map(HeaderValue::value)
    {
        Some(marker) if marker.as_ref() == MARKER_NULL => None,
        Some(_) => Some(Bytes::new()),
        None => Some(message.payload.clone()),
    };

    let mut headers = IndexMap::new();
    for (name, stored_value) in stored {
        // A name Iggy holds but Kafka cannot carry. Only an Iggy producer can write one.
        let Ok(name) = name.as_str() else {
            continue;
        };
        let name = if gateway_written {
            match name.strip_prefix(HEADER_PREFIX) {
                Some(name) => name,
                None => continue,
            }
        } else {
            name
        };
        headers.insert(
            StrBytes::from_string(name.to_string()),
            Some(stored_value.value()),
        );
    }
    (key, value, headers)
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
/// This bounds accumulation, not peak. `kafka_protocol`'s decompressors write the whole stream
/// out before handing it over (`compression/gzip.rs:46` and its siblings), so one batch can still
/// allocate past the budget and be rejected only afterwards. Bounding peak needs a size-limited
/// reader per codec, which means owning Kafka's snappy and lz4 framing rather than borrowing it.
pub struct DecompressionBudget {
    remaining: Cell<usize>,
    /// What the charge was when it first tripped. The decompression hook reports failure through
    /// `anyhow`, which the decoder stringifies, so the typed reason is kept here instead.
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

    fn charge(&self, produced: usize) -> anyhow::Result<()> {
        let remaining = self.remaining.get();
        if produced > remaining {
            self.overflow.set(Some((produced, remaining)));
            anyhow::bail!("decompressed {produced} bytes with {remaining} left in the budget");
        }
        self.remaining.set(remaining - produced);
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
/// Returns an error when a batch is malformed, or when the request decompresses to more than
/// `budget` allows.
pub fn decode_batches(buf: &mut Bytes, budget: &DecompressionBudget) -> Result<Vec<Record>> {
    let mut records = Vec::new();
    while buf.has_remaining() {
        let set = RecordBatchDecoder::decode_with_custom_compression(
            buf,
            Some(|compressed: &mut Bytes, compression| decompress(compressed, compression, budget)),
        )
        .map_err(|error| {
            budget.overflow.get().map_or_else(
                || RecordCodecError::Batch(error.to_string()),
                |(produced, remaining)| RecordCodecError::BudgetExceeded {
                    produced,
                    remaining,
                },
            )
        })?;
        records.extend(set.records);
    }
    Ok(records)
}

/// Encodes records as one uncompressed v2 batch.
///
/// Fetch always emits uncompressed, so the read path spends no CPU on a codec the client did
/// not ask for. `BRIDGE_MAPPING.md` records that as a default open to revisiting.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the batch.
pub fn encode_batch(records: &[Record]) -> Result<Bytes> {
    let mut buf = BytesMut::new();
    let options = RecordEncodeOptions {
        version: BATCH_VERSION,
        compression: Compression::None,
    };
    RecordBatchEncoder::encode(&mut buf, records, &options)
        .map_err(|error| RecordCodecError::Batch(error.to_string()))?;
    Ok(buf.freeze())
}

/// Decompresses one batch and charges what it produced against the request budget.
fn decompress(
    compressed: &mut Bytes,
    compression: Compression,
    budget: &DecompressionBudget,
) -> anyhow::Result<Bytes> {
    use kafka_protocol::compression::{Decompressor, Gzip, Lz4, Snappy, Zstd};

    let take =
        |buf: &mut Bytes| -> anyhow::Result<Bytes> { Ok(buf.copy_to_bytes(buf.remaining())) };
    let produced = match compression {
        Compression::None => take(compressed)?,
        Compression::Gzip => Gzip::decompress(compressed, take)?,
        Compression::Snappy => Snappy::decompress(compressed, take)?,
        Compression::Lz4 => Lz4::decompress(compressed, take)?,
        Compression::Zstd => Zstd::decompress(compressed, take)?,
    };
    budget.charge(produced.len())?;
    Ok(produced)
}

#[cfg(test)]
mod tests {
    use super::*;

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
            1_700_000_000_000,
        )
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
        let mut headers = BTreeMap::new();
        headers.insert(header_key("source"), header_value(b"connector"));
        let message = IggyMessage::builder()
            .payload(Bytes::from_static(b"{}"))
            .user_headers(headers)
            .build()
            .unwrap();

        let record = from_iggy(&message, 3).unwrap();
        assert_eq!(record.key, None);
        assert_eq!(record.value.as_deref(), Some(&b"{}"[..]));
        assert_eq!(
            record.headers.get(&StrBytes::from_static_str("source")),
            Some(&Some(Bytes::from_static(b"connector")))
        );
    }

    #[test]
    fn given_a_create_time_when_round_tripped_should_convert_between_milliseconds_and_micros() {
        let millis = 1_700_000_000_123;
        let message = to_iggy(&record_with(Some(b"k"), Some(b"v"), &[])).unwrap();
        assert_eq!(message.header.origin_timestamp, 1_700_000_000_000 * 1000);
        assert_eq!(timestamp_in(millis).unwrap(), millis.cast_unsigned() * 1000);
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
    fn given_a_truncated_envelope_when_decoded_should_fail() {
        let message = to_iggy(&record_with(Some(b""), Some(b"v"), &[])).unwrap();
        let mut truncated = message;
        truncated.payload = truncated.payload.slice(0..3);
        assert!(matches!(
            from_iggy(&truncated, 0),
            Err(RecordCodecError::EnvelopeTruncated { .. })
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

    #[test]
    fn given_an_uncompressed_batch_when_round_tripped_should_keep_every_record() {
        let records = vec![
            record_with(Some(b"a"), Some(b"1"), &[]),
            record_with(Some(b"b"), Some(b"2"), &[]),
        ];
        let mut encoded = encode_batch(&records).unwrap();
        let budget = DecompressionBudget::new(1024);
        let decoded = decode_batches(&mut encoded, &budget).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[1].value.as_deref(), Some(&b"2"[..]));
    }

    #[test]
    fn given_two_batches_in_one_blob_when_decoded_should_drain_both() {
        let mut blob = BytesMut::new();
        blob.extend_from_slice(&encode_batch(&[record_with(Some(b"a"), Some(b"1"), &[])]).unwrap());
        blob.extend_from_slice(&encode_batch(&[record_with(Some(b"b"), Some(b"2"), &[])]).unwrap());

        let budget = DecompressionBudget::new(1024);
        let decoded = decode_batches(&mut blob.freeze(), &budget).unwrap();
        assert_eq!(decoded.len(), 2, "a partition blob can hold many batches");
    }

    #[test]
    fn given_a_gzip_batch_when_decoded_should_read_it() {
        let records = vec![record_with(Some(b"a"), Some(b"compressed"), &[])];
        let mut encoded = encode_with(&records, Compression::Gzip);
        let budget = DecompressionBudget::new(1024);
        let decoded = decode_batches(&mut encoded, &budget).unwrap();
        assert_eq!(decoded[0].value.as_deref(), Some(&b"compressed"[..]));
    }

    #[test]
    fn given_a_budget_smaller_than_the_batch_when_decoded_should_reject() {
        let records = vec![record_with(Some(b"a"), Some(&[b'x'; 512]), &[])];
        let mut encoded = encode_with(&records, Compression::Gzip);
        let budget = DecompressionBudget::new(8);
        assert!(matches!(
            decode_batches(&mut encoded, &budget),
            Err(RecordCodecError::BudgetExceeded { .. })
        ));
    }

    #[test]
    fn given_two_batches_when_the_second_passes_the_budget_should_reject() {
        let big = record_with(Some(b"a"), Some(&[b'x'; 256]), &[]);
        let mut blob = BytesMut::new();
        blob.extend_from_slice(&encode_batch(std::slice::from_ref(&big)).unwrap());
        blob.extend_from_slice(&encode_batch(std::slice::from_ref(&big)).unwrap());

        // Enough for one batch, not for both: the budget is per request, not per batch.
        let budget = DecompressionBudget::new(400);
        assert!(matches!(
            decode_batches(&mut blob.freeze(), &budget),
            Err(RecordCodecError::BudgetExceeded { .. })
        ));
    }
}
