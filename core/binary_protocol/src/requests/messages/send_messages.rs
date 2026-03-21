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

//! Zero-copy encoder for `SendMessages` wire format.
//!
//! Messages are written directly to the buffer without intermediate allocation.

use crate::codec::{WireDecode, WireEncode, read_u32_le};
use crate::error::WireError;
use crate::message_layout::{WIRE_MESSAGE_HEADER_SIZE, WIRE_MESSAGE_INDEX_SIZE};
use crate::primitives::identifier::WireIdentifier;
use crate::primitives::partitioning::WirePartitioning;
use bytes::{BufMut, BytesMut};

/// Borrowed message data for encoding. No allocation needed - the caller
/// owns the payload and headers buffers.
pub struct RawMessage<'a> {
    pub id: u128,
    pub origin_timestamp: u64,
    pub headers: Option<&'a [u8]>,
    pub payload: &'a [u8],
}

impl RawMessage<'_> {
    fn wire_size(&self) -> usize {
        WIRE_MESSAGE_HEADER_SIZE + self.payload.len() + self.headers.map_or(0, <[u8]>::len)
    }

    /// Write the 64-byte message header to `buf`. Payload and `user_headers` are NOT
    /// written - the caller sends them separately via vectored I/O.
    pub fn encode_header(&self, buf: &mut BytesMut) {
        let headers_len = self.headers.map_or(0, <[u8]>::len);
        buf.put_u64_le(0); // checksum (server-computed)
        buf.put_u128_le(self.id);
        buf.put_u64_le(0); // offset (server-assigned)
        buf.put_u64_le(0); // timestamp (server-assigned)
        buf.put_u64_le(self.origin_timestamp);
        #[allow(clippy::cast_possible_truncation)]
        {
            buf.put_u32_le(headers_len as u32);
            buf.put_u32_le(self.payload.len() as u32);
        }
        buf.put_u64_le(0); // reserved
    }
}

/// Zero-copy encoder for the `SendMessages` command payload.
///
/// Wire layout:
/// ```text
/// [metadata_length:u32_le]
/// [stream_id:variable]
/// [topic_id:variable]
/// [partitioning:variable]
/// [messages_count:u32_le]
/// [index_array: messages_count * 16 bytes]
/// [message_data: variable]
/// ```
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
        let index_total = messages.len() * WIRE_MESSAGE_INDEX_SIZE;
        let messages_total: usize = messages.iter().map(RawMessage::wire_size).sum();
        4 + metadata_inner + index_total + messages_total
    }

    pub fn encode(
        buf: &mut BytesMut,
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
        partitioning: &WirePartitioning,
        messages: &[RawMessage<'_>],
    ) {
        let metadata_inner =
            stream_id.encoded_size() + topic_id.encoded_size() + partitioning.encoded_size() + 4;

        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32_le(metadata_inner as u32);

        stream_id.encode(buf);
        topic_id.encode(buf);
        partitioning.encode(buf);

        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32_le(messages.len() as u32);

        // Index array: cumulative sizes for each message
        let mut cumulative_size: u32 = 0;
        for msg in messages {
            #[allow(clippy::cast_possible_truncation)]
            {
                cumulative_size += msg.wire_size() as u32;
            }
            // bytes 0-3: zero
            buf.put_u32_le(0);
            // bytes 4-7: cumulative size
            buf.put_u32_le(cumulative_size);
            // bytes 8-15: zero
            buf.put_u64_le(0);
        }

        // Message data: header(64) + payload + optional user_headers
        for msg in messages {
            let headers_len = msg.headers.map_or(0, <[u8]>::len);

            buf.put_u64_le(0); // checksum (server-computed)
            buf.put_u128_le(msg.id);
            buf.put_u64_le(0); // offset (server-assigned)
            buf.put_u64_le(0); // timestamp (server-assigned)
            buf.put_u64_le(msg.origin_timestamp);
            #[allow(clippy::cast_possible_truncation)]
            {
                buf.put_u32_le(headers_len as u32);
                buf.put_u32_le(msg.payload.len() as u32);
            }
            buf.put_u64_le(0); // reserved
            buf.put_slice(msg.payload);
            if let Some(headers) = msg.headers {
                buf.put_slice(headers);
            }
        }
    }
}

/// Metadata-only decoder for the `SendMessages` command.
///
/// Parses routing metadata (stream, topic, partitioning, message count)
/// without touching message payloads. The server uses this to extract
/// routing info before reading message data into pooled buffers.
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

/// Vectored I/O encoder for `SendMessages`.
///
/// Writes metadata + index array but stops before message frame data,
/// enabling the caller to compose `[header_buf | msg_frames...]` via writev.
pub struct SendMessagesMetadataEncoder;

impl SendMessagesMetadataEncoder {
    /// Exact size of metadata + indexes (everything except message frame data).
    #[must_use]
    pub fn header_size(
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
        partitioning: &WirePartitioning,
        messages_count: usize,
    ) -> usize {
        let metadata_inner =
            stream_id.encoded_size() + topic_id.encoded_size() + partitioning.encoded_size() + 4;
        4 + metadata_inner + messages_count * WIRE_MESSAGE_INDEX_SIZE
    }

    /// Encode `metadata_length` + metadata fields + index array into `buf`.
    ///
    /// Does NOT write message frame data - the caller sends that via vectored I/O.
    pub fn encode_header(
        buf: &mut BytesMut,
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
        partitioning: &WirePartitioning,
        messages: &[RawMessage<'_>],
    ) {
        let metadata_inner =
            stream_id.encoded_size() + topic_id.encoded_size() + partitioning.encoded_size() + 4;

        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32_le(metadata_inner as u32);

        stream_id.encode(buf);
        topic_id.encode(buf);
        partitioning.encode(buf);

        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32_le(messages.len() as u32);

        let mut cumulative_size: u32 = 0;
        for msg in messages {
            #[allow(clippy::cast_possible_truncation)]
            {
                cumulative_size += msg.wire_size() as u32;
            }
            buf.put_u32_le(0);
            buf.put_u32_le(cumulative_size);
            buf.put_u64_le(0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn numeric_id(id: u32) -> WireIdentifier {
        WireIdentifier::numeric(id)
    }

    #[test]
    fn encode_single_message_no_headers() {
        let stream_id = numeric_id(1);
        let topic_id = numeric_id(2);
        let partitioning = WirePartitioning::Balanced;
        let payload = b"hello";
        let messages = [RawMessage {
            id: 100,
            origin_timestamp: 999,
            headers: None,
            payload: payload.as_slice(),
        }];

        let size =
            SendMessagesEncoder::encoded_size(&stream_id, &topic_id, &partitioning, &messages);
        let mut buf = BytesMut::with_capacity(size);
        SendMessagesEncoder::encode(&mut buf, &stream_id, &topic_id, &partitioning, &messages);

        assert_eq!(buf.len(), size);
    }

    #[test]
    fn encode_single_message_with_headers() {
        let stream_id = numeric_id(1);
        let topic_id = numeric_id(2);
        let partitioning = WirePartitioning::PartitionId(5);
        let payload = b"world";
        let headers = b"key:val";
        let messages = [RawMessage {
            id: 200,
            origin_timestamp: 1000,
            headers: Some(headers.as_slice()),
            payload: payload.as_slice(),
        }];

        let size =
            SendMessagesEncoder::encoded_size(&stream_id, &topic_id, &partitioning, &messages);
        let mut buf = BytesMut::with_capacity(size);
        SendMessagesEncoder::encode(&mut buf, &stream_id, &topic_id, &partitioning, &messages);

        assert_eq!(buf.len(), size);
    }

    #[test]
    fn encode_multiple_messages() {
        let stream_id = numeric_id(10);
        let topic_id = numeric_id(20);
        let partitioning = WirePartitioning::MessagesKey(b"user-1".to_vec());
        let messages = [
            RawMessage {
                id: 1,
                origin_timestamp: 100,
                headers: None,
                payload: b"msg-1",
            },
            RawMessage {
                id: 2,
                origin_timestamp: 200,
                headers: Some(b"h2"),
                payload: b"msg-2",
            },
            RawMessage {
                id: 3,
                origin_timestamp: 300,
                headers: None,
                payload: b"msg-3",
            },
        ];

        let size =
            SendMessagesEncoder::encoded_size(&stream_id, &topic_id, &partitioning, &messages);
        let mut buf = BytesMut::with_capacity(size);
        SendMessagesEncoder::encode(&mut buf, &stream_id, &topic_id, &partitioning, &messages);

        assert_eq!(buf.len(), size);
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

        let mut buf = BytesMut::with_capacity(256);
        SendMessagesEncoder::encode(&mut buf, &stream_id, &topic_id, &partitioning, &messages);

        let metadata_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        let expected =
            stream_id.encoded_size() + topic_id.encoded_size() + partitioning.encoded_size() + 4;
        assert_eq!(metadata_len, expected);
    }

    #[test]
    fn verify_index_entries() {
        let stream_id = numeric_id(1);
        let topic_id = numeric_id(2);
        let partitioning = WirePartitioning::Balanced;
        let messages = [
            RawMessage {
                id: 1,
                origin_timestamp: 0,
                headers: None,
                payload: b"aaaa", // wire_size = 64 + 4 = 68
            },
            RawMessage {
                id: 2,
                origin_timestamp: 0,
                headers: Some(b"hh"), // wire_size = 64 + 3 + 2 = 69
                payload: b"bbb",
            },
        ];

        let mut buf = BytesMut::with_capacity(512);
        SendMessagesEncoder::encode(&mut buf, &stream_id, &topic_id, &partitioning, &messages);

        // Index starts after: 4 (metadata_len) + stream_id(6) + topic_id(6) + partitioning(2) + 4 (msg_count)
        let index_start = 4 + 6 + 6 + 2 + 4;

        // First index entry: cumulative = 68
        let first_zero = u32::from_le_bytes(buf[index_start..index_start + 4].try_into().unwrap());
        let first_cum =
            u32::from_le_bytes(buf[index_start + 4..index_start + 8].try_into().unwrap());
        assert_eq!(first_zero, 0);
        assert_eq!(first_cum, 68);

        // Second index entry: cumulative = 68 + 69 = 137
        let second_start = index_start + WIRE_MESSAGE_INDEX_SIZE;
        let second_zero =
            u32::from_le_bytes(buf[second_start..second_start + 4].try_into().unwrap());
        let second_cum =
            u32::from_le_bytes(buf[second_start + 4..second_start + 8].try_into().unwrap());
        assert_eq!(second_zero, 0);
        assert_eq!(second_cum, 137);
    }

    #[test]
    fn verify_message_header_layout() {
        let stream_id = numeric_id(1);
        let topic_id = numeric_id(2);
        let partitioning = WirePartitioning::Balanced;
        let messages = [RawMessage {
            id: 42,
            origin_timestamp: 777,
            headers: Some(b"hdr"),
            payload: b"pay",
        }];

        let mut buf = BytesMut::with_capacity(256);
        SendMessagesEncoder::encode(&mut buf, &stream_id, &topic_id, &partitioning, &messages);

        // Message data starts after: 4 + 6 + 6 + 2 + 4 + 16 (one index entry)
        let msg_start = 4 + 6 + 6 + 2 + 4 + 16;
        let msg = &buf[msg_start..];

        // Header layout matches WIRE_MESSAGE_HEADER_SIZE (64 bytes):
        // checksum(8) + id(16) + offset(8) + timestamp(8) + origin_ts(8) + hdrs_len(4) + payload_len(4) + reserved(8)
        let checksum = u64::from_le_bytes(msg[0..8].try_into().unwrap());
        assert_eq!(checksum, 0);

        let id = u128::from_le_bytes(msg[8..24].try_into().unwrap());
        assert_eq!(id, 42);

        let offset = u64::from_le_bytes(msg[24..32].try_into().unwrap());
        assert_eq!(offset, 0);

        let timestamp = u64::from_le_bytes(msg[32..40].try_into().unwrap());
        assert_eq!(timestamp, 0);

        let origin_ts = u64::from_le_bytes(msg[40..48].try_into().unwrap());
        assert_eq!(origin_ts, 777);

        let headers_len = u32::from_le_bytes(msg[48..52].try_into().unwrap());
        assert_eq!(headers_len, 3);

        let payload_len = u32::from_le_bytes(msg[52..56].try_into().unwrap());
        assert_eq!(payload_len, 3);

        let reserved = u64::from_le_bytes(msg[56..64].try_into().unwrap());
        assert_eq!(reserved, 0);

        // After the 64-byte header: payload then headers
        assert_eq!(&msg[64..67], b"pay");
        assert_eq!(&msg[67..70], b"hdr");
    }

    #[test]
    fn empty_payload_message() {
        let stream_id = numeric_id(1);
        let topic_id = numeric_id(1);
        let partitioning = WirePartitioning::Balanced;
        let messages = [RawMessage {
            id: 0,
            origin_timestamp: 0,
            headers: None,
            payload: b"",
        }];

        let size =
            SendMessagesEncoder::encoded_size(&stream_id, &topic_id, &partitioning, &messages);
        let mut buf = BytesMut::with_capacity(size);
        SendMessagesEncoder::encode(&mut buf, &stream_id, &topic_id, &partitioning, &messages);
        assert_eq!(buf.len(), size);
    }

    // -- RawMessage::encode_header --

    #[test]
    fn raw_message_encode_header_is_64_bytes() {
        let msg = RawMessage {
            id: 42,
            origin_timestamp: 999,
            headers: Some(b"hdr"),
            payload: b"pay",
        };
        let mut buf = BytesMut::with_capacity(WIRE_MESSAGE_HEADER_SIZE);
        msg.encode_header(&mut buf);
        assert_eq!(buf.len(), WIRE_MESSAGE_HEADER_SIZE);
    }

    #[test]
    fn raw_message_encode_header_field_values() {
        let msg = RawMessage {
            id: 0x1234_5678_9ABC_DEF0_1234_5678_9ABC_DEF0,
            origin_timestamp: 0xCAFE_BABE,
            headers: Some(b"hdr"),
            payload: b"pay",
        };
        let mut buf = BytesMut::with_capacity(WIRE_MESSAGE_HEADER_SIZE);
        msg.encode_header(&mut buf);

        assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 0);
        assert_eq!(
            u128::from_le_bytes(buf[8..24].try_into().unwrap()),
            0x1234_5678_9ABC_DEF0_1234_5678_9ABC_DEF0
        );
        assert_eq!(u64::from_le_bytes(buf[24..32].try_into().unwrap()), 0);
        assert_eq!(u64::from_le_bytes(buf[32..40].try_into().unwrap()), 0);
        assert_eq!(
            u64::from_le_bytes(buf[40..48].try_into().unwrap()),
            0xCAFE_BABE
        );
        assert_eq!(u32::from_le_bytes(buf[48..52].try_into().unwrap()), 3);
        assert_eq!(u32::from_le_bytes(buf[52..56].try_into().unwrap()), 3);
        assert_eq!(u64::from_le_bytes(buf[56..64].try_into().unwrap()), 0);
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

    #[test]
    fn send_messages_header_cross_validate_with_encoder() {
        let stream_id = numeric_id(1);
        let topic_id = numeric_id(2);
        let partitioning = WirePartitioning::Balanced;
        let messages = [RawMessage {
            id: 42,
            origin_timestamp: 777,
            headers: Some(b"hdr"),
            payload: b"pay",
        }];

        let mut buf = BytesMut::with_capacity(256);
        SendMessagesEncoder::encode(&mut buf, &stream_id, &topic_id, &partitioning, &messages);

        let metadata_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        let (header, consumed) = SendMessagesHeader::decode(&buf[4..4 + metadata_len]).unwrap();
        assert_eq!(consumed, metadata_len);
        assert_eq!(header.stream_id, stream_id);
        assert_eq!(header.topic_id, topic_id);
        assert_eq!(header.partitioning, partitioning);
        assert_eq!(header.messages_count, 1);
    }

    // -- SendMessagesMetadataEncoder --

    #[test]
    fn metadata_encoder_header_size_matches_encoded() {
        let stream_id = numeric_id(1);
        let topic_id = numeric_id(2);
        let partitioning = WirePartitioning::Balanced;
        let messages = [
            RawMessage {
                id: 1,
                origin_timestamp: 100,
                headers: None,
                payload: b"msg-1",
            },
            RawMessage {
                id: 2,
                origin_timestamp: 200,
                headers: Some(b"h"),
                payload: b"msg-2",
            },
        ];

        let expected_size = SendMessagesMetadataEncoder::header_size(
            &stream_id,
            &topic_id,
            &partitioning,
            messages.len(),
        );
        let mut buf = BytesMut::with_capacity(expected_size);
        SendMessagesMetadataEncoder::encode_header(
            &mut buf,
            &stream_id,
            &topic_id,
            &partitioning,
            &messages,
        );
        assert_eq!(buf.len(), expected_size);
    }

    #[test]
    fn metadata_encoder_concat_matches_full_encoder() {
        let stream_id = numeric_id(1);
        let topic_id = numeric_id(2);
        let partitioning = WirePartitioning::Balanced;
        let messages = [
            RawMessage {
                id: 1,
                origin_timestamp: 100,
                headers: None,
                payload: b"aaa",
            },
            RawMessage {
                id: 2,
                origin_timestamp: 200,
                headers: Some(b"hh"),
                payload: b"bbb",
            },
        ];

        let full_size =
            SendMessagesEncoder::encoded_size(&stream_id, &topic_id, &partitioning, &messages);
        let mut full_buf = BytesMut::with_capacity(full_size);
        SendMessagesEncoder::encode(
            &mut full_buf,
            &stream_id,
            &topic_id,
            &partitioning,
            &messages,
        );

        let mut vec_buf = BytesMut::with_capacity(full_size);
        SendMessagesMetadataEncoder::encode_header(
            &mut vec_buf,
            &stream_id,
            &topic_id,
            &partitioning,
            &messages,
        );
        for msg in &messages {
            msg.encode_header(&mut vec_buf);
            vec_buf.put_slice(msg.payload);
            if let Some(headers) = msg.headers {
                vec_buf.put_slice(headers);
            }
        }

        assert_eq!(vec_buf.len(), full_buf.len());
        assert_eq!(&vec_buf[..], &full_buf[..]);
    }

    // -- Cross-validation: encoder -> header decoder -> iterator --

    #[test]
    fn cross_validation_encoder_to_iterator() {
        use crate::message_view::WireMessageIterator;

        let stream_id = WireIdentifier::numeric(1);
        let topic_id = WireIdentifier::numeric(2);
        let partitioning = WirePartitioning::Balanced;
        let messages = [
            RawMessage {
                id: 100,
                origin_timestamp: 1000,
                headers: None,
                payload: b"first",
            },
            RawMessage {
                id: 200,
                origin_timestamp: 2000,
                headers: Some(b"hdr"),
                payload: b"second",
            },
        ];

        let size =
            SendMessagesEncoder::encoded_size(&stream_id, &topic_id, &partitioning, &messages);
        let mut buf = BytesMut::with_capacity(size);
        SendMessagesEncoder::encode(&mut buf, &stream_id, &topic_id, &partitioning, &messages);

        let metadata_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        let (header, _) = SendMessagesHeader::decode(&buf[4..4 + metadata_len]).unwrap();
        assert_eq!(header.messages_count, 2);

        let data_offset =
            4 + metadata_len + (header.messages_count as usize) * WIRE_MESSAGE_INDEX_SIZE;
        let message_data = &buf[data_offset..];

        let views: Vec<_> = WireMessageIterator::new(message_data, header.messages_count)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(views.len(), 2);

        assert_eq!(views[0].id(), 100);
        assert_eq!(views[0].origin_timestamp(), 1000);
        assert_eq!(views[0].payload(), b"first");
        assert_eq!(views[0].user_headers(), b"");

        assert_eq!(views[1].id(), 200);
        assert_eq!(views[1].origin_timestamp(), 2000);
        assert_eq!(views[1].payload(), b"second");
        assert_eq!(views[1].user_headers(), b"hdr");
    }
}
