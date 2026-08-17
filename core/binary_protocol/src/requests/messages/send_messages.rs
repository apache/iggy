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

//! Encoder for the `SendMessages` wire format.
//!
//! The body is the routing metadata section followed by one canonical message
//! batch (see [`crate::batch`]) - the same record the server replicates,
//! persists, and serves back to polls.

use crate::batch::{
    BATCH_HEADER_SIZE, BATCH_MESSAGE_HEADER_SIZE, BatchHeader, MAX_TIMESTAMP_DELTA_MICROS,
    calculate_batch_checksum,
};
use crate::codec::{WireDecode, WireEncode, read_u32_le};
use crate::error::WireError;
use crate::primitives::identifier::WireIdentifier;
use crate::primitives::partitioning::WirePartitioning;
use bytes::{BufMut, BytesMut};
use twox_hash::XxHash3_64;

/// Borrowed message data for encoding. No allocation needed - the caller
/// owns the payload and headers buffers.
pub struct RawMessage<'a> {
    pub id: u128,
    pub origin_timestamp: u64,
    pub headers: Option<&'a [u8]>,
    pub payload: &'a [u8],
}

impl RawMessage<'_> {
    fn frame_size(&self) -> usize {
        BATCH_MESSAGE_HEADER_SIZE + self.payload.len() + self.headers.map_or(0, <[u8]>::len)
    }
}

/// Encoder for the `SendMessages` command payload.
///
/// Wire layout:
/// ```text
/// [metadata_length:u32_le]
/// [stream_id:variable]
/// [topic_id:variable]
/// [partitioning:variable]
/// [messages_count:u32_le]
/// [batch: 256-byte batch header + message frames]
/// ```
///
/// The producer leaves `partition_id`, `base_offset`, and `base_timestamp`
/// zero in the batch header; the server stamps them. Every checksum is
/// producer-computed and verified at admission.
pub struct SendMessagesEncoder;

impl SendMessagesEncoder {
    #[must_use]
    pub fn encoded_size(
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
        partitioning: &WirePartitioning,
        messages: &[RawMessage<'_>],
    ) -> usize {
        let metadata_inner =
            stream_id.encoded_size() + topic_id.encoded_size() + partitioning.encoded_size() + 4;
        let blob_total: usize = messages.iter().map(RawMessage::frame_size).sum();
        4 + metadata_inner + BATCH_HEADER_SIZE + blob_total
    }

    /// Encode the full `SendMessages` body into `buf`.
    ///
    /// # Errors
    /// [`WireError::Validation`] on an empty message batch;
    /// [`WireError::InvalidMessageTimestampDelta`] when a message's
    /// `origin_timestamp` runs more than [`MAX_TIMESTAMP_DELTA_MICROS`] ahead
    /// of the batch's earliest one; [`WireError::PayloadTooLarge`] when a
    /// section length or the batch length overflows its wire field.
    pub fn encode(
        buf: &mut BytesMut,
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
        partitioning: &WirePartitioning,
        messages: &[RawMessage<'_>],
    ) -> Result<(), WireError> {
        if messages.is_empty() {
            return Err(WireError::Validation(std::borrow::Cow::Borrowed(
                "cannot encode an empty message batch",
            )));
        }

        let metadata_inner =
            stream_id.encoded_size() + topic_id.encoded_size() + partitioning.encoded_size() + 4;

        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32_le(metadata_inner as u32);

        stream_id.encode(buf);
        topic_id.encode(buf);
        partitioning.encode(buf);

        let message_count =
            u32::try_from(messages.len()).map_err(|_| WireError::PayloadTooLarge {
                size: messages.len(),
                max: u32::MAX as usize,
            })?;
        buf.put_u32_le(message_count);

        let origin_timestamp = messages
            .iter()
            .map(|message| message.origin_timestamp)
            .min()
            .unwrap_or(0);

        // The batch header depends on the frame checksums written below, so
        // reserve its slot and backpatch once the blob is in place.
        let header_start = buf.len();
        buf.resize(header_start + BATCH_HEADER_SIZE, 0);

        let blob_start = buf.len();
        for (index, message) in messages.iter().enumerate() {
            let timestamp_delta = message.origin_timestamp - origin_timestamp;
            if timestamp_delta > MAX_TIMESTAMP_DELTA_MICROS {
                return Err(WireError::InvalidMessageTimestampDelta(timestamp_delta));
            }
            let headers = message.headers.unwrap_or_default();
            let user_headers_length =
                u32::try_from(headers.len()).map_err(|_| WireError::PayloadTooLarge {
                    size: headers.len(),
                    max: u32::MAX as usize,
                })?;
            let payload_length =
                u32::try_from(message.payload.len()).map_err(|_| WireError::PayloadTooLarge {
                    size: message.payload.len(),
                    max: u32::MAX as usize,
                })?;

            let frame_start = buf.len();
            buf.put_u64_le(0); // checksum, backpatched below
            buf.put_u128_le(message.id);
            #[allow(clippy::cast_possible_truncation)]
            {
                buf.put_u32_le(index as u32); // offset_delta
                buf.put_u32_le(timestamp_delta as u32);
            }
            buf.put_u32_le(user_headers_length);
            buf.put_u32_le(payload_length);
            buf.put_u64_le(0); // reserved
            buf.put_slice(message.payload);
            buf.put_slice(headers);

            let checksum = XxHash3_64::oneshot(&buf[frame_start + 8..]);
            buf[frame_start..frame_start + 8].copy_from_slice(&checksum.to_le_bytes());
        }

        // Request framing carries the body size in a u32, so a batch past
        // u32::MAX cannot ride any request frame.
        let batch_size = BATCH_HEADER_SIZE + (buf.len() - blob_start);
        if batch_size > u32::MAX as usize {
            return Err(WireError::PayloadTooLarge {
                size: batch_size,
                max: u32::MAX as usize,
            });
        }
        let batch_length = batch_size as u64;
        let mut batch_header = BatchHeader::new(0, origin_timestamp, batch_length, message_count);
        batch_header.batch_checksum = calculate_batch_checksum(&batch_header, &buf[blob_start..]);
        let header_bytes: &mut [u8] = &mut buf[header_start..header_start + BATCH_HEADER_SIZE];
        batch_header.encode_into(header_bytes);
        Ok(())
    }
}

/// Metadata-only decoder for the `SendMessages` command.
///
/// Parses routing metadata (stream, topic, partitioning, message count)
/// without touching the batch. The server uses this to resolve the namespace
/// before validating the batch bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendMessagesHeader {
    pub stream_id: WireIdentifier,
    pub topic_id: WireIdentifier,
    pub partitioning: WirePartitioning,
    pub messages_count: u32,
}

impl SendMessagesHeader {
    /// Size of the encoded metadata fields (the value written as the
    /// `metadata_length` prefix on the wire).
    #[must_use]
    pub fn metadata_length(&self) -> usize {
        self.stream_id.encoded_size()
            + self.topic_id.encoded_size()
            + self.partitioning.encoded_size()
            + 4
    }
}

impl WireEncode for SendMessagesHeader {
    fn encoded_size(&self) -> usize {
        self.metadata_length()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.stream_id.encode(buf);
        self.topic_id.encode(buf);
        self.partitioning.encode(buf);
        buf.put_u32_le(self.messages_count);
    }
}

impl WireDecode for SendMessagesHeader {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (stream_id, mut pos) = WireIdentifier::decode(buf)?;
        let (topic_id, consumed) = WireIdentifier::decode(&buf[pos..])?;
        pos += consumed;
        let (partitioning, consumed) = WirePartitioning::decode(&buf[pos..])?;
        pos += consumed;
        let messages_count = read_u32_le(buf, pos)?;
        pos += 4;
        Ok((
            Self {
                stream_id,
                topic_id,
                partitioning,
                messages_count,
            },
            pos,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::decode_batch_slice;

    fn numeric_id(id: u32) -> WireIdentifier {
        WireIdentifier::numeric(id)
    }

    fn encode(
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
        partitioning: &WirePartitioning,
        messages: &[RawMessage<'_>],
    ) -> BytesMut {
        let size = SendMessagesEncoder::encoded_size(stream_id, topic_id, partitioning, messages);
        let mut buf = BytesMut::with_capacity(size);
        SendMessagesEncoder::encode(&mut buf, stream_id, topic_id, partitioning, messages)
            .expect("encodes");
        assert_eq!(buf.len(), size);
        buf
    }

    #[test]
    fn encoded_batch_decodes_and_verifies() {
        let stream_id = numeric_id(1);
        let topic_id = numeric_id(2);
        let partitioning = WirePartitioning::Balanced;
        let messages = [
            RawMessage {
                id: 100,
                origin_timestamp: 1_000,
                headers: None,
                payload: b"first",
            },
            RawMessage {
                id: 200,
                origin_timestamp: 2_000,
                headers: Some(b"hdr"),
                payload: b"second",
            },
        ];

        let buf = encode(&stream_id, &topic_id, &partitioning, &messages);

        let metadata_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        let (header, consumed) = SendMessagesHeader::decode(&buf[4..4 + metadata_len]).unwrap();
        assert_eq!(consumed, metadata_len);
        assert_eq!(header.messages_count, 2);

        let batch = decode_batch_slice(&buf[4 + metadata_len..]).unwrap();
        assert_eq!(batch.header.partition_id, 0);
        assert_eq!(batch.header.base_offset, 0);
        assert_eq!(batch.header.base_timestamp, 0);
        assert_eq!(batch.header.origin_timestamp, 1_000);
        assert_eq!(batch.header.total_size(), buf.len() - 4 - metadata_len);
        assert_eq!(batch.message_count(), 2);

        let views: Vec<_> = batch.iter().collect();
        assert_eq!(views[0].header.id, 100);
        assert_eq!(views[0].header.offset_delta, 0);
        assert_eq!(views[0].header.timestamp_delta, 0);
        assert_eq!(views[0].payload, b"first");
        assert_eq!(views[0].user_headers, b"");
        assert_eq!(views[1].header.id, 200);
        assert_eq!(views[1].header.offset_delta, 1);
        assert_eq!(views[1].header.timestamp_delta, 1_000);
        assert_eq!(views[1].payload, b"second");
        assert_eq!(views[1].user_headers, b"hdr");
    }

    #[test]
    fn verify_metadata_length_field() {
        let stream_id = numeric_id(1);
        let topic_id = numeric_id(2);
        let partitioning = WirePartitioning::Balanced;
        let messages = [RawMessage {
            id: 1,
            origin_timestamp: 0,
            headers: None,
            payload: b"x",
        }];

        let buf = encode(&stream_id, &topic_id, &partitioning, &messages);

        let metadata_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        let expected =
            stream_id.encoded_size() + topic_id.encoded_size() + partitioning.encoded_size() + 4;
        assert_eq!(metadata_len, expected);
    }

    #[test]
    fn empty_payload_message() {
        let stream_id = numeric_id(1);
        let topic_id = numeric_id(1);
        let partitioning = WirePartitioning::Balanced;
        let messages = [RawMessage {
            id: 7,
            origin_timestamp: 0,
            headers: None,
            payload: b"",
        }];

        let buf = encode(&stream_id, &topic_id, &partitioning, &messages);
        let metadata_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        let batch = decode_batch_slice(&buf[4 + metadata_len..]).unwrap();
        assert_eq!(batch.message_count(), 1);
    }

    #[test]
    fn timestamp_delta_overflow_rejected() {
        let stream_id = numeric_id(1);
        let topic_id = numeric_id(1);
        let partitioning = WirePartitioning::Balanced;
        let messages = [
            RawMessage {
                id: 1,
                origin_timestamp: 0,
                headers: None,
                payload: b"a",
            },
            RawMessage {
                id: 2,
                origin_timestamp: MAX_TIMESTAMP_DELTA_MICROS + 1,
                headers: None,
                payload: b"b",
            },
        ];

        let mut buf = BytesMut::new();
        let result =
            SendMessagesEncoder::encode(&mut buf, &stream_id, &topic_id, &partitioning, &messages);
        assert!(matches!(
            result,
            Err(WireError::InvalidMessageTimestampDelta(_))
        ));
    }

    // -- SendMessagesHeader --

    #[test]
    fn send_messages_header_roundtrip() {
        let header = SendMessagesHeader {
            stream_id: numeric_id(1),
            topic_id: numeric_id(2),
            partitioning: WirePartitioning::Balanced,
            messages_count: 5,
        };
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), header.encoded_size());
        let (decoded, consumed) = SendMessagesHeader::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, header);
    }

    #[test]
    fn send_messages_header_roundtrip_with_string_ids() {
        let header = SendMessagesHeader {
            stream_id: WireIdentifier::named("my-stream").unwrap(),
            topic_id: WireIdentifier::named("my-topic").unwrap(),
            partitioning: WirePartitioning::MessagesKey(b"key-1".to_vec()),
            messages_count: 10,
        };
        let bytes = header.to_bytes();
        let (decoded, consumed) = SendMessagesHeader::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, header);
    }

    #[test]
    fn send_messages_header_truncation() {
        let header = SendMessagesHeader {
            stream_id: numeric_id(1),
            topic_id: numeric_id(2),
            partitioning: WirePartitioning::Balanced,
            messages_count: 1,
        };
        let bytes = header.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                SendMessagesHeader::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn send_messages_header_metadata_length() {
        let header = SendMessagesHeader {
            stream_id: numeric_id(1),
            topic_id: numeric_id(2),
            partitioning: WirePartitioning::Balanced,
            messages_count: 3,
        };
        // numeric_id(6) + numeric_id(6) + balanced(2) + count(4) = 18
        assert_eq!(header.metadata_length(), 18);
        assert_eq!(header.encoded_size(), 18);
    }
}
