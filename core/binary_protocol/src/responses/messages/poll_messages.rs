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

//! Decoder for the `PollMessages` response.
//!
//! The body after the 16-byte prefix is a stream of canonical batch records
//! (see [`crate::batch`]) served as stored: each record's header carries the
//! stamped `base_offset` / `base_timestamp`, and each frame's deltas resolve
//! against them. A record may be a server-sliced view of a larger stored
//! batch, so `base_offset + offset_delta` of the first frame is the first
//! polled offset, not necessarily `base_offset` itself.

use crate::batch::{BatchHeader, BatchIntegrity, BatchIterator, decode_batch_slice_with};
use crate::codec::{WireDecode, WireEncode, read_u32_le, read_u64_le};
use crate::error::WireError;
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

/// One polled message with its deltas resolved to absolute values.
#[derive(Debug, Clone, Copy)]
pub struct PolledMessageView<'a> {
    /// Stored per-message checksum, passed through as served.
    pub checksum: u64,
    pub id: u128,
    pub offset: u64,
    /// Broker append time: the flat batch `base_timestamp` (the per-message
    /// delta applies to `origin_timestamp` only).
    pub timestamp: u64,
    pub origin_timestamp: u64,
    pub payload: &'a [u8],
    pub user_headers: &'a [u8],
}

/// Iterator over every message in a stream of served batch records.
///
/// Walks records by `batch_length` and flattens their frames. Each record's
/// layout is proven up front (frames must tile `message_count` exactly), so
/// framing errors, including frame-level corruption inside a record, surface
/// as `Some(Err(_))` and end the iteration. Checksums are passed through
/// unverified, as served.
pub struct PolledBatchesIterator<'a> {
    batches: &'a [u8],
    position: usize,
    current_header: Option<BatchHeader>,
    frames: Option<BatchIterator<'a>>,
    failed: bool,
}

impl<'a> PolledBatchesIterator<'a> {
    #[must_use]
    pub const fn new(batches: &'a [u8]) -> Self {
        Self {
            batches,
            position: 0,
            current_header: None,
            frames: None,
            failed: false,
        }
    }

    fn advance_batch(&mut self) -> Result<bool, WireError> {
        if self.position >= self.batches.len() {
            return Ok(false);
        }
        // Layout-only decode proves the frames tile `message_count` exactly
        // before any of them are yielded, so a corrupt record errors instead
        // of silently truncating. Poll replies pass checksums through as
        // served, so no integrity verification here.
        let batch =
            decode_batch_slice_with(&self.batches[self.position..], BatchIntegrity::LayoutOnly)?;
        self.position += batch.header.total_size();
        self.frames = Some(batch.iter());
        self.current_header = Some(batch.header);
        Ok(true)
    }
}

impl<'a> Iterator for PolledBatchesIterator<'a> {
    type Item = Result<PolledMessageView<'a>, WireError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        loop {
            if let Some(frames) = &mut self.frames
                && let Some(view) = frames.next()
            {
                let header = self.current_header.expect("frames imply a current header");
                return Some(Ok(PolledMessageView {
                    checksum: view.header.checksum,
                    id: view.header.id,
                    offset: header.base_offset + u64::from(view.header.offset_delta),
                    timestamp: header.base_timestamp,
                    origin_timestamp: header.origin_timestamp
                        + u64::from(view.header.timestamp_delta),
                    payload: view.payload,
                    user_headers: view.user_headers,
                }));
            }
            match self.advance_batch() {
                Ok(true) => {}
                Ok(false) => return None,
                Err(error) => {
                    self.failed = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

/// Borrowed `PollMessages` response. Does not own message data.
///
/// Does NOT implement `WireDecode` (trait returns owned data, we borrow).
/// Use [`PollMessagesResponse::decode`] instead.
pub struct PollMessagesResponse<'a> {
    pub header: PollMessagesResponseHeader,
    pub messages: PolledBatchesIterator<'a>,
}

impl<'a> PollMessagesResponse<'a> {
    /// Decode from a response payload buffer. Borrows the buffer.
    ///
    /// Reads the 16-byte header then creates an iterator over the batch
    /// records that follow. Records are validated lazily during iteration.
    ///
    /// # Errors
    /// Returns `WireError` if the buffer is too short for the response header.
    pub fn decode(buf: &'a [u8]) -> Result<Self, WireError> {
        let (header, _) = PollMessagesResponseHeader::decode(buf)?;
        let messages = PolledBatchesIterator::new(&buf[POLL_RESPONSE_HEADER_SIZE..]);
        Ok(Self { header, messages })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::{BATCH_HEADER_SIZE, BATCH_MESSAGE_HEADER_SIZE, calculate_batch_checksum};
    use twox_hash::XxHash3_64;

    #[allow(clippy::cast_possible_truncation)]
    fn frame(id: u128, offset_delta: u32, timestamp_delta: u32, payload: &[u8]) -> Vec<u8> {
        let mut bytes = vec![0u8; BATCH_MESSAGE_HEADER_SIZE];
        bytes[8..24].copy_from_slice(&id.to_le_bytes());
        bytes[24..28].copy_from_slice(&offset_delta.to_le_bytes());
        bytes[28..32].copy_from_slice(&timestamp_delta.to_le_bytes());
        bytes[36..40].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(payload);
        let checksum = XxHash3_64::oneshot(&bytes[8..]);
        bytes[0..8].copy_from_slice(&checksum.to_le_bytes());
        bytes
    }

    #[allow(clippy::cast_possible_truncation)]
    fn batch_record(
        base_offset: u64,
        base_timestamp: u64,
        origin_timestamp: u64,
        frames: &[Vec<u8>],
    ) -> Vec<u8> {
        let blob: Vec<u8> = frames.concat();
        let mut header = BatchHeader::new(
            1,
            origin_timestamp,
            (BATCH_HEADER_SIZE + blob.len()) as u64,
            frames.len() as u32,
        );
        header.base_offset = base_offset;
        header.base_timestamp = base_timestamp;
        header.batch_checksum = calculate_batch_checksum(&header, &blob);
        let mut bytes = vec![0u8; BATCH_HEADER_SIZE];
        header.encode_into(&mut bytes);
        bytes.extend_from_slice(&blob);
        bytes
    }

    fn response_body(current_offset: u64, count: u32, batches: &[Vec<u8>]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&7u32.to_le_bytes());
        body.extend_from_slice(&current_offset.to_le_bytes());
        body.extend_from_slice(&count.to_le_bytes());
        for batch in batches {
            body.extend_from_slice(batch);
        }
        body
    }

    #[test]
    fn response_header_roundtrip() {
        let header = PollMessagesResponseHeader {
            partition_id: 3,
            current_offset: 42,
            messages_count: 7,
        };
        let bytes = header.to_bytes();
        let (decoded, consumed) = PollMessagesResponseHeader::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, header);
    }

    #[test]
    fn response_header_truncation() {
        let bytes = [0u8; POLL_RESPONSE_HEADER_SIZE - 1];
        assert!(PollMessagesResponseHeader::decode(&bytes).is_err());
    }

    #[test]
    fn decodes_messages_across_batches() {
        let first = batch_record(
            100,
            5_000,
            1_000,
            &[frame(11, 0, 0, b"a"), frame(12, 1, 10, b"b")],
        );
        let second = batch_record(102, 6_000, 2_000, &[frame(13, 0, 0, b"c")]);
        let body = response_body(102, 3, &[first, second]);

        let response = PollMessagesResponse::decode(&body).unwrap();
        assert_eq!(response.header.partition_id, 7);
        assert_eq!(response.header.current_offset, 102);
        assert_eq!(response.header.messages_count, 3);

        let messages: Vec<_> = response.messages.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(messages.len(), 3);

        assert_eq!(messages[0].id, 11);
        assert_eq!(messages[0].offset, 100);
        assert_eq!(messages[0].timestamp, 5_000);
        assert_eq!(messages[0].origin_timestamp, 1_000);
        assert_eq!(messages[0].payload, b"a");

        assert_eq!(messages[1].id, 12);
        assert_eq!(messages[1].offset, 101);
        assert_eq!(messages[1].timestamp, 5_000);
        assert_eq!(messages[1].origin_timestamp, 1_010);
        assert_eq!(messages[1].payload, b"b");

        assert_eq!(messages[2].id, 13);
        assert_eq!(messages[2].offset, 102);
        assert_eq!(messages[2].timestamp, 6_000);
        assert_eq!(messages[2].payload, b"c");
    }

    #[test]
    fn sliced_record_resolves_leading_delta() {
        // A server-sliced record keeps the stored base_offset; the first
        // frame's delta positions it inside the original batch.
        let record = batch_record(50, 9_000, 0, &[frame(1, 3, 0, b"tail")]);
        let body = response_body(53, 1, &[record]);

        let response = PollMessagesResponse::decode(&body).unwrap();
        let messages: Vec<_> = response.messages.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].offset, 53);
    }

    #[test]
    fn empty_body_yields_no_messages() {
        let body = response_body(0, 0, &[]);
        let response = PollMessagesResponse::decode(&body).unwrap();
        assert_eq!(response.messages.count(), 0);
    }

    #[test]
    fn truncated_record_surfaces_error() {
        let record = batch_record(0, 0, 0, &[frame(1, 0, 0, b"payload")]);
        let mut body = response_body(0, 1, &[record]);
        body.truncate(body.len() - 1);

        let response = PollMessagesResponse::decode(&body).unwrap();
        let result: Result<Vec<_>, _> = response.messages.collect();
        assert!(result.is_err());
    }
}
