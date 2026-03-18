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

use crate::codec::{WireDecode, WireEncode, read_u32_le, read_u64_le};
use crate::error::WireError;
use crate::message_view::WireMessageIterator;
use bytes::{BufMut, BytesMut};

/// Size of the `PollMessages` response header: `partition_id(4) + current_offset(8) + count(4)`.
const POLL_RESPONSE_HEADER_SIZE: usize = 16;

/// The 16-byte metadata prefix of a `PollMessages` response.
///
/// Layout: `partition_id(4) + current_offset(8) + messages_count(4)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollMessagesResponseHeader {
    pub partition_id: u32,
    pub current_offset: u64,
    pub messages_count: u32,
}

impl WireEncode for PollMessagesResponseHeader {
    fn encoded_size(&self) -> usize {
        POLL_RESPONSE_HEADER_SIZE
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.partition_id);
        buf.put_u64_le(self.current_offset);
        buf.put_u32_le(self.messages_count);
    }
}

impl WireDecode for PollMessagesResponseHeader {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let partition_id = read_u32_le(buf, 0)?;
        let current_offset = read_u64_le(buf, 4)?;
        let messages_count = read_u32_le(buf, 12)?;
        Ok((
            Self {
                partition_id,
                current_offset,
                messages_count,
            },
            POLL_RESPONSE_HEADER_SIZE,
        ))
    }
}

/// Borrowed `PollMessages` response. Does not own message data.
///
/// Does NOT implement `WireDecode` (trait returns owned data, we borrow).
/// Use [`PollMessagesResponse::decode`] instead.
pub struct PollMessagesResponse<'a> {
    pub header: PollMessagesResponseHeader,
    pub messages: WireMessageIterator<'a>,
}

impl<'a> PollMessagesResponse<'a> {
    /// Decode from a response payload buffer. Borrows the buffer.
    ///
    /// Reads the 16-byte header then creates an iterator over the remaining
    /// message frames. Messages are validated lazily during iteration.
    ///
    /// # Errors
    /// Returns `WireError` if the buffer is too short for the response header.
    pub fn decode(buf: &'a [u8]) -> Result<Self, WireError> {
        let (header, _) = PollMessagesResponseHeader::decode(buf)?;
        let messages =
            WireMessageIterator::new(&buf[POLL_RESPONSE_HEADER_SIZE..], header.messages_count);
        Ok(Self { header, messages })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_layout::{
        MSG_ID_OFFSET, MSG_ORIGIN_TIMESTAMP_OFFSET, MSG_PAYLOAD_LEN_OFFSET,
        WIRE_MESSAGE_HEADER_SIZE,
    };

    fn make_frame(payload: &[u8], id: u128, origin_ts: u64) -> Vec<u8> {
        let total = WIRE_MESSAGE_HEADER_SIZE + payload.len();
        let mut frame = vec![0u8; total];
        frame[MSG_ID_OFFSET..MSG_ID_OFFSET + 16].copy_from_slice(&id.to_le_bytes());
        frame[MSG_ORIGIN_TIMESTAMP_OFFSET..MSG_ORIGIN_TIMESTAMP_OFFSET + 8]
            .copy_from_slice(&origin_ts.to_le_bytes());
        #[allow(clippy::cast_possible_truncation)]
        frame[MSG_PAYLOAD_LEN_OFFSET..MSG_PAYLOAD_LEN_OFFSET + 4]
            .copy_from_slice(&(payload.len() as u32).to_le_bytes());
        frame[WIRE_MESSAGE_HEADER_SIZE..].copy_from_slice(payload);
        frame
    }

    #[test]
    fn response_header_roundtrip() {
        let header = PollMessagesResponseHeader {
            partition_id: 7,
            current_offset: 12345,
            messages_count: 3,
        };
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), POLL_RESPONSE_HEADER_SIZE);
        let (decoded, consumed) = PollMessagesResponseHeader::decode(&bytes).unwrap();
        assert_eq!(consumed, POLL_RESPONSE_HEADER_SIZE);
        assert_eq!(decoded, header);
    }

    #[test]
    fn response_header_truncation() {
        let header = PollMessagesResponseHeader {
            partition_id: 1,
            current_offset: 0,
            messages_count: 0,
        };
        let bytes = header.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                PollMessagesResponseHeader::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn response_decode_with_messages() {
        let frame1 = make_frame(b"hello", 1, 100);
        let frame2 = make_frame(b"world", 2, 200);

        let mut buf = BytesMut::new();
        buf.put_u32_le(42);
        buf.put_u64_le(999);
        buf.put_u32_le(2);
        buf.put_slice(&frame1);
        buf.put_slice(&frame2);

        let resp = PollMessagesResponse::decode(&buf).unwrap();
        assert_eq!(resp.header.partition_id, 42);
        assert_eq!(resp.header.current_offset, 999);
        assert_eq!(resp.header.messages_count, 2);

        let views: Vec<_> = resp.messages.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(views.len(), 2);
        assert_eq!(views[0].id(), 1);
        assert_eq!(views[0].payload(), b"hello");
        assert_eq!(views[1].id(), 2);
        assert_eq!(views[1].payload(), b"world");
    }

    #[test]
    fn response_decode_zero_messages() {
        let mut buf = BytesMut::new();
        buf.put_u32_le(1);
        buf.put_u64_le(0);
        buf.put_u32_le(0);

        let resp = PollMessagesResponse::decode(&buf).unwrap();
        assert_eq!(resp.header.messages_count, 0);
        assert_eq!(resp.messages.count(), 0);
    }
}
