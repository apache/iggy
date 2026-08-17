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

//! The message batch: the one layout a `SendMessages` body, the replicated
//! prepare, the persisted segment record, and the poll reply all share.
//!
//! ```text
//! [batch header: 256 bytes][blob: message frames]
//! frame = [header: 48 bytes][payload][user_headers]
//! ```
//!
//! Producers encode this batch directly on the wire (after the routing
//! metadata section); the server stamps `partition_id`, `base_offset`, and
//! `base_timestamp` at ingestion/persistence and replicates the bytes
//! verbatim; polls serve the stored records back. There is no other message
//! encoding.

use crate::WireError;
use std::hash::Hasher;
use twox_hash::XxHash3_64;

/// Size of the batch header. The remainder past the fields below is reserved
/// and must be zero.
pub const BATCH_HEADER_SIZE: usize = 256;

/// Size of a message frame header inside the blob.
pub const BATCH_MESSAGE_HEADER_SIZE: usize = 48;

/// Byte offset of `batch_checksum` inside the batch header.
pub const BATCH_CHECKSUM_OFFSET: usize = 40;

/// Byte offset of `message_count` inside the batch header.
pub const BATCH_MESSAGE_COUNT_OFFSET: usize = 48;

/// Byte offset where the reserved region of the batch header starts.
pub const BATCH_RESERVED_OFFSET: usize = 52;

/// Upper bound on a message's `timestamp_delta`: the field is a `u32`
/// microsecond delta against the batch `origin_timestamp`, so a single batch
/// spans at most ~71.6 minutes of producer clock.
pub const MAX_TIMESTAMP_DELTA_MICROS: u64 = u32::MAX as u64;

/// The batch header.
///
/// A producer encodes it with `partition_id`, `base_offset`, and
/// `base_timestamp` zero; the server owns those three fields and stamps them
/// at ingestion (`partition_id`) and persistence (`base_offset`,
/// `base_timestamp`, plus the `batch_checksum` recompute).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchHeader {
    pub partition_id: u64,
    pub base_offset: u64,
    pub base_timestamp: u64,
    pub origin_timestamp: u64,
    /// Total batch size: `BATCH_HEADER_SIZE` + blob length.
    pub batch_length: u64,
    pub batch_checksum: u64,
    pub message_count: u32,
}

impl BatchHeader {
    #[must_use]
    pub const fn new(
        partition_id: u64,
        origin_timestamp: u64,
        batch_length: u64,
        message_count: u32,
    ) -> Self {
        Self {
            partition_id,
            base_offset: 0,
            base_timestamp: 0,
            origin_timestamp,
            batch_length,
            batch_checksum: 0,
            message_count,
        }
    }

    /// # Errors
    /// [`WireError::UnexpectedEof`] on a short buffer;
    /// [`WireError::Validation`] on a `batch_length` smaller than the header
    /// or nonzero reserved bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, WireError> {
        if bytes.len() < BATCH_HEADER_SIZE {
            return Err(WireError::UnexpectedEof {
                offset: 0,
                need: BATCH_HEADER_SIZE,
                have: bytes.len(),
            });
        }

        let batch_length = read_u64(bytes, 32);
        if batch_length < BATCH_HEADER_SIZE as u64 {
            return Err(WireError::Validation(std::borrow::Cow::Borrowed(
                "batch length must cover the batch header",
            )));
        }

        // Every encoder zeroes the reserved region. Admitting nonzero bytes
        // would let unchecksummed data ride the header to disk and replicas.
        if bytes[BATCH_RESERVED_OFFSET..BATCH_HEADER_SIZE]
            .iter()
            .any(|&reserved_byte| reserved_byte != 0)
        {
            return Err(WireError::Validation(std::borrow::Cow::Borrowed(
                "batch header reserved bytes must be zero",
            )));
        }

        Ok(Self {
            partition_id: read_u64(bytes, 0),
            base_offset: read_u64(bytes, 8),
            base_timestamp: read_u64(bytes, 16),
            origin_timestamp: read_u64(bytes, 24),
            batch_length,
            batch_checksum: read_u64(bytes, BATCH_CHECKSUM_OFFSET),
            message_count: read_u32(bytes, BATCH_MESSAGE_COUNT_OFFSET),
        })
    }

    /// # Panics
    /// Panics if `bytes` is shorter than [`BATCH_HEADER_SIZE`].
    pub fn encode_into(&self, bytes: &mut [u8]) {
        assert!(bytes.len() >= BATCH_HEADER_SIZE);
        bytes[..BATCH_HEADER_SIZE].fill(0);
        bytes[0..8].copy_from_slice(&self.partition_id.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.base_offset.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.base_timestamp.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.origin_timestamp.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.batch_length.to_le_bytes());
        bytes[BATCH_CHECKSUM_OFFSET..BATCH_CHECKSUM_OFFSET + 8]
            .copy_from_slice(&self.batch_checksum.to_le_bytes());
        bytes[BATCH_MESSAGE_COUNT_OFFSET..BATCH_MESSAGE_COUNT_OFFSET + 4]
            .copy_from_slice(&self.message_count.to_le_bytes());
    }

    /// Total batch size in bytes (header + blob).
    ///
    /// # Panics
    /// Panics if `batch_length` exceeds `usize::MAX`.
    #[must_use]
    pub fn total_size(&self) -> usize {
        usize::try_from(self.batch_length).expect("batch length exceeds usize::MAX")
    }

    /// # Errors
    /// [`WireError::Validation`] if `batch_length` does not cover the header
    /// or exceeds `usize`.
    pub fn blob_len(&self) -> Result<usize, WireError> {
        self.batch_length
            .checked_sub(BATCH_HEADER_SIZE as u64)
            .and_then(|len| usize::try_from(len).ok())
            .ok_or(WireError::Validation(std::borrow::Cow::Borrowed(
                "batch length must cover the batch header",
            )))
    }

    #[must_use]
    pub fn checksum_for_blob(&self, blob: &[u8]) -> u64 {
        calculate_batch_checksum(self, blob)
    }
}

/// A decoded batch borrowing its blob.
#[derive(Debug, Clone, Copy)]
pub struct BatchRef<'a> {
    pub header: BatchHeader,
    blob: &'a [u8],
}

impl<'a> BatchRef<'a> {
    #[must_use]
    pub const fn new(header: BatchHeader, blob: &'a [u8]) -> Self {
        Self { header, blob }
    }

    #[must_use]
    pub const fn iter(&self) -> BatchIterator<'a> {
        BatchIterator {
            blob: self.blob,
            position: 0,
        }
    }

    #[must_use]
    pub const fn iter_with_offsets(&self) -> BatchIteratorWithOffsets<'a> {
        BatchIteratorWithOffsets {
            blob: self.blob,
            position: 0,
        }
    }

    #[must_use]
    pub const fn blob(&self) -> &'a [u8] {
        self.blob
    }

    #[must_use]
    pub const fn message_count(&self) -> u32 {
        self.header.message_count
    }
}

impl<'a> IntoIterator for &BatchRef<'a> {
    type Item = BatchMessageView<'a>;
    type IntoIter = BatchIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// A message frame header inside the blob.
#[derive(Debug, Clone, Copy)]
pub struct BatchMessageHeader {
    /// `XxHash3_64` over `frame[8..48] || payload || user_headers`.
    pub checksum: u64,
    pub id: u128,
    pub offset_delta: u32,
    /// Microsecond delta from [`BatchHeader::origin_timestamp`].
    pub timestamp_delta: u32,
    pub user_headers_length: u32,
    pub payload_length: u32,
}

impl BatchMessageHeader {
    /// # Errors
    /// [`WireError::UnexpectedEof`] on a short buffer; [`WireError::Validation`]
    /// on nonzero reserved bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, WireError> {
        if bytes.len() < BATCH_MESSAGE_HEADER_SIZE {
            return Err(WireError::UnexpectedEof {
                offset: 0,
                need: BATCH_MESSAGE_HEADER_SIZE,
                have: bytes.len(),
            });
        }

        if read_u64(bytes, 40) != 0 {
            return Err(WireError::Validation(std::borrow::Cow::Borrowed(
                "message frame reserved bytes must be zero",
            )));
        }

        Ok(Self {
            checksum: read_u64(bytes, 0),
            id: read_u128(bytes, 8),
            offset_delta: read_u32(bytes, 24),
            timestamp_delta: read_u32(bytes, 28),
            user_headers_length: read_u32(bytes, 32),
            payload_length: read_u32(bytes, 36),
        })
    }

    /// Frame size: header + payload + user headers.
    #[must_use]
    pub const fn total_size(&self) -> usize {
        BATCH_MESSAGE_HEADER_SIZE + self.user_headers_length as usize + self.payload_length as usize
    }
}

/// A message frame view borrowing payload and user headers from the blob.
#[derive(Debug, Clone, Copy)]
pub struct BatchMessageView<'a> {
    pub header: BatchMessageHeader,
    pub user_headers: &'a [u8],
    pub payload: &'a [u8],
}

/// Infallible frame walk over a blob whose layout has already been proven by
/// [`decode_batch_slice_with`]: on an unvalidated blob it stops at the first
/// malformed frame instead of erroring.
pub struct BatchIterator<'a> {
    blob: &'a [u8],
    position: usize,
}

impl<'a> Iterator for BatchIterator<'a> {
    type Item = BatchMessageView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.blob.len() {
            return None;
        }

        let header = BatchMessageHeader::decode(&self.blob[self.position..]).ok()?;
        let start = self.position + BATCH_MESSAGE_HEADER_SIZE;
        let payload_end = start + header.payload_length as usize;
        let headers_end = payload_end + header.user_headers_length as usize;
        let payload = self.blob.get(start..payload_end)?;
        let user_headers = self.blob.get(payload_end..headers_end)?;
        self.position += header.total_size();
        Some(BatchMessageView {
            header,
            user_headers,
            payload,
        })
    }
}

/// A frame view plus its byte range inside the blob.
#[derive(Debug, Clone, Copy)]
pub struct BatchMessageViewWithOffsets<'a> {
    pub message: BatchMessageView<'a>,
    pub start: usize,
    pub end: usize,
}

pub struct BatchIteratorWithOffsets<'a> {
    blob: &'a [u8],
    position: usize,
}

impl<'a> Iterator for BatchIteratorWithOffsets<'a> {
    type Item = BatchMessageViewWithOffsets<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.blob.len() {
            return None;
        }

        let start = self.position;
        let header = BatchMessageHeader::decode(&self.blob[self.position..]).ok()?;
        let message_start = self.position + BATCH_MESSAGE_HEADER_SIZE;
        let payload_end = message_start + header.payload_length as usize;
        let headers_end = payload_end + header.user_headers_length as usize;
        let payload = self.blob.get(message_start..payload_end)?;
        let user_headers = self.blob.get(payload_end..headers_end)?;
        self.position += header.total_size();
        Some(BatchMessageViewWithOffsets {
            message: BatchMessageView {
                header,
                user_headers,
                payload,
            },
            start,
            end: self.position,
        })
    }
}

/// How much of a batch record [`decode_batch_slice_with`] proves before returning it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchIntegrity {
    /// Re-hash the batch and reject it unless it matches its own `batch_checksum`.
    Verify,
    /// Check the framing only, and hand back whatever it describes. The caller is
    /// accepting bytes that may not be the ones written.
    LayoutOnly,
}

/// Decode one batch record (`[256B header][blob]`), verifying the batch
/// checksum and every per-message checksum.
///
/// `body` may extend past the batch: readers walking a stream of records hand
/// in the rest of the buffer and step by `batch_length`. Callers whose buffer
/// is meant to BE the batch must reject the surplus themselves.
///
/// # Errors
/// [`WireError`] on a short or self-inconsistent record, and
/// [`WireError::InvalidBatchChecksum`] / [`WireError::InvalidMessageChecksum`]
/// on an integrity mismatch.
pub fn decode_batch_slice(body: &[u8]) -> Result<BatchRef<'_>, WireError> {
    decode_batch_slice_with(body, BatchIntegrity::Verify)
}

/// [`decode_batch_slice`] with the integrity level chosen by the caller.
///
/// Layout checks are not optional either way: a short or self-inconsistent
/// record is rejected regardless, because the caller would otherwise index
/// past it.
///
/// # Errors
/// See [`decode_batch_slice`]; [`BatchIntegrity::LayoutOnly`] skips only the
/// checksum comparison.
pub fn decode_batch_slice_with(
    body: &[u8],
    integrity: BatchIntegrity,
) -> Result<BatchRef<'_>, WireError> {
    let header = BatchHeader::decode(body)?;
    let blob_len = header.blob_len()?;
    if body.len() < header.total_size() {
        return Err(WireError::UnexpectedEof {
            offset: 0,
            need: header.total_size(),
            have: body.len(),
        });
    }

    let blob = &body[BATCH_HEADER_SIZE..BATCH_HEADER_SIZE + blob_len];
    let batch = BatchRef { header, blob };
    match integrity {
        BatchIntegrity::Verify => {
            let expected_checksum = verify_and_recompute_batch_checksum(&batch)?;
            if header.batch_checksum != expected_checksum {
                return Err(WireError::InvalidBatchChecksum {
                    stored: header.batch_checksum,
                    computed: expected_checksum,
                    base_offset: header.base_offset,
                });
            }
        }
        BatchIntegrity::LayoutOnly => validate_batch_layout(&batch)?,
    }

    Ok(batch)
}

/// Batch checksum: streaming `XxHash3_64` over the six batch header meta
/// fields followed by each message's stored 8-byte checksum field in message
/// order - NOT the message bodies.
///
/// Bodies are bound only transitively: each per-message checksum already covers
/// `frame[8..48] || payload || user_headers`, so hashing the checksum fields
/// binds every body byte IFF a reader also re-verifies the per-message
/// checksums. Stamping hashes `N * 8` bytes instead of the whole blob;
/// validating decoders pay the one body pass as the per-message verify in
/// [`verify_and_recompute_batch_checksum`], which hashes the checksum-field
/// bytes in the same order so its recompute matches a compute here.
///
/// Assumes a well-formed blob whose frames tile exactly; every compute site
/// builds the blob and satisfies this.
#[must_use]
pub fn calculate_batch_checksum(header: &BatchHeader, blob: &[u8]) -> u64 {
    let mut hasher = XxHash3_64::new();
    write_batch_header_fields(&mut hasher, header);
    let batch = BatchRef {
        header: *header,
        blob,
    };
    for framed in batch.iter_with_offsets() {
        hasher.write(&blob[framed.start..framed.start + 8]);
    }
    hasher.finish()
}

fn write_batch_header_fields(hasher: &mut XxHash3_64, header: &BatchHeader) {
    hasher.write(&header.partition_id.to_le_bytes());
    hasher.write(&header.base_offset.to_le_bytes());
    hasher.write(&header.base_timestamp.to_le_bytes());
    hasher.write(&header.origin_timestamp.to_le_bytes());
    hasher.write(&header.batch_length.to_le_bytes());
    hasher.write(&header.message_count.to_le_bytes());
}

/// Verify every per-message checksum in `batch` and return the recomputed
/// batch checksum (see [`calculate_batch_checksum`]) from a single frame walk.
///
/// The per-message pass is the equal-integrity half of the scheme: the batch
/// value binds bodies only through the checksum fields, so a validating decode
/// must re-verify each message here or body corruption that leaves the
/// checksum field intact would pass. This is the one full-body pass a
/// validating decode pays; the caller then compares the returned value against
/// the stored `batch_checksum`.
///
/// # Errors
/// [`WireError::InvalidMessageChecksum`] on the first per-message mismatch;
/// [`WireError::Validation`] if the frames do not tile `message_count` exactly.
pub fn verify_and_recompute_batch_checksum(batch: &BatchRef<'_>) -> Result<u64, WireError> {
    let blob = batch.blob();
    let mut hasher = XxHash3_64::new();
    write_batch_header_fields(&mut hasher, &batch.header);
    let mut verified = 0u32;
    let mut covered = 0usize;
    for framed in batch.iter_with_offsets() {
        // Cover (`frame[8..48] || payload || user_headers`) hashed raw from the
        // blob, byte-exact with the encoder's, so a flipped body byte fails even
        // when the stored checksum field is left intact.
        let stored = framed.message.header.checksum;
        let expected = XxHash3_64::oneshot(&blob[framed.start + 8..framed.end]);
        if expected != stored {
            return Err(WireError::InvalidMessageChecksum {
                stored,
                computed: expected,
                offset: batch
                    .header
                    .base_offset
                    .saturating_add(u64::from(framed.message.header.offset_delta)),
            });
        }
        hasher.write(&blob[framed.start..framed.start + 8]);
        verified += 1;
        covered = framed.end;
    }
    if verified != batch.message_count() || covered != blob.len() {
        return Err(WireError::Validation(std::borrow::Cow::Borrowed(
            "batch frames do not tile message_count exactly",
        )));
    }
    Ok(hasher.finish())
}

/// Layout-only twin of [`verify_and_recompute_batch_checksum`]: prove the
/// frames tile `message_count` exactly without touching any checksum.
///
/// # Errors
/// [`WireError::Validation`] if the frames do not tile `message_count` exactly.
fn validate_batch_layout(batch: &BatchRef<'_>) -> Result<(), WireError> {
    let blob = batch.blob();
    let mut counted = 0u32;
    let mut covered = 0usize;
    for framed in batch.iter_with_offsets() {
        counted += 1;
        covered = framed.end;
    }
    if counted != batch.message_count() || covered != blob.len() {
        return Err(WireError::Validation(std::borrow::Cow::Borrowed(
            "batch frames do not tile message_count exactly",
        )));
    }
    Ok(())
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("4-byte slice"))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("8-byte slice"))
}

fn read_u128(bytes: &[u8], offset: usize) -> u128 {
    u128::from_le_bytes(
        bytes[offset..offset + 16]
            .try_into()
            .expect("16-byte slice"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn batch_bytes(frames: &[Vec<u8>]) -> Vec<u8> {
        let blob: Vec<u8> = frames.concat();
        let mut header = BatchHeader::new(
            7,
            1_000,
            (BATCH_HEADER_SIZE + blob.len()) as u64,
            frames.len() as u32,
        );
        header.batch_checksum = calculate_batch_checksum(&header, &blob);
        let mut bytes = vec![0u8; BATCH_HEADER_SIZE];
        header.encode_into(&mut bytes);
        bytes.extend_from_slice(&blob);
        bytes
    }

    #[test]
    fn header_roundtrip() {
        let mut header = BatchHeader::new(1, 2, 300, 4);
        header.base_offset = 10;
        header.base_timestamp = 20;
        header.batch_checksum = 30;
        let mut bytes = vec![0u8; BATCH_HEADER_SIZE];
        header.encode_into(&mut bytes);
        let decoded = BatchHeader::decode(&bytes).unwrap();
        assert_eq!(decoded, header);
    }

    #[test]
    fn decode_verifies_checksums() {
        let bytes = batch_bytes(&[frame(1, 0, 0, b"first"), frame(2, 1, 5, b"second")]);
        let batch = decode_batch_slice(&bytes).unwrap();
        assert_eq!(batch.message_count(), 2);
        let views: Vec<_> = batch.iter().collect();
        assert_eq!(views[0].payload, b"first");
        assert_eq!(views[1].payload, b"second");
        assert_eq!(views[1].header.offset_delta, 1);
    }

    #[test]
    fn decode_rejects_flipped_body_byte() {
        let mut bytes = batch_bytes(&[frame(1, 0, 0, b"payload")]);
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        assert!(matches!(
            decode_batch_slice(&bytes),
            Err(WireError::InvalidMessageChecksum { .. })
        ));
    }

    #[test]
    fn decode_rejects_flipped_batch_checksum() {
        let mut bytes = batch_bytes(&[frame(1, 0, 0, b"payload")]);
        bytes[BATCH_CHECKSUM_OFFSET] ^= 0xFF;
        assert!(matches!(
            decode_batch_slice(&bytes),
            Err(WireError::InvalidBatchChecksum { .. })
        ));
    }

    #[test]
    fn layout_only_accepts_zero_checksums() {
        let mut bytes = batch_bytes(&[frame(1, 0, 0, b"payload")]);
        bytes[BATCH_CHECKSUM_OFFSET..BATCH_CHECKSUM_OFFSET + 8].fill(0);
        let batch = decode_batch_slice_with(&bytes, BatchIntegrity::LayoutOnly).unwrap();
        assert_eq!(batch.message_count(), 1);
    }

    #[test]
    fn decode_rejects_miscounted_batch() {
        let mut bytes = batch_bytes(&[frame(1, 0, 0, b"payload")]);
        bytes[BATCH_MESSAGE_COUNT_OFFSET..BATCH_MESSAGE_COUNT_OFFSET + 4]
            .copy_from_slice(&2u32.to_le_bytes());
        assert!(decode_batch_slice_with(&bytes, BatchIntegrity::LayoutOnly).is_err());
    }

    #[test]
    fn decode_rejects_truncated_batch() {
        let bytes = batch_bytes(&[frame(1, 0, 0, b"payload")]);
        assert!(decode_batch_slice(&bytes[..bytes.len() - 1]).is_err());
    }

    #[test]
    fn decode_rejects_nonzero_reserved() {
        let mut bytes = batch_bytes(&[frame(1, 0, 0, b"payload")]);
        bytes[BATCH_HEADER_SIZE + 40] = 1;
        assert!(decode_batch_slice_with(&bytes, BatchIntegrity::LayoutOnly).is_err());
    }

    #[test]
    fn decode_rejects_nonzero_header_reserved() {
        let mut bytes = batch_bytes(&[frame(1, 0, 0, b"payload")]);
        bytes[BATCH_RESERVED_OFFSET] = 1;
        assert!(matches!(
            BatchHeader::decode(&bytes),
            Err(WireError::Validation(_))
        ));
        bytes[BATCH_RESERVED_OFFSET] = 0;
        bytes[BATCH_HEADER_SIZE - 1] = 1;
        assert!(BatchHeader::decode(&bytes).is_err());
    }
}
