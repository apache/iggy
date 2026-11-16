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

//! The placed batch: a message batch whose blob lives in the object store.
//!
//! In a partition that places its batches, it is the body of a `SendMessages`
//! prepare and the record the partition keeps of it.
//!
//! ```text
//! [batch header: 256 bytes, flags bit 1 set][placement: 64 bytes]
//!
//! placement:
//! [0..16]  object_id       the object that holds the blob
//! [16..24] position        where the blob starts in the object
//! [24..32] length          the blob's length
//! [32..40] blob_checksum   XXH3 of the blob
//! [40..44] version         1
//! [44..56] zero
//! [56..64] checksum        XXH3 of the batch header and of bytes 0..56
//! ```
//!
//! The batch header is the one of the batch the blob belongs to, with the
//! fields the server stamps. Its `batch_checksum` is zero: that checksum
//! covers the stamped fields and the blob together, and whoever stamps a
//! placed batch does not hold the blob. A reader that fetched the blob checks
//! it against `blob_checksum`, and computes `batch_checksum` when it puts the
//! batch back together.
//!
//! A replica cannot check bytes that it does not hold, so the placed batch
//! carries a checksum of its own.
//!
//! Only the server writes one, and none ever reaches a client: the flag is
//! one that [`BatchHeader::decode`] refuses, so a batch from a producer that
//! carries it is refused, and so is a placed batch that reaches code which
//! expects the blob to follow the header.

use crate::WireError;
use crate::batch::{BATCH_CHECKSUM_OFFSET, BATCH_HEADER_SIZE, BATCH_PRODUCER_OFFSET, BatchHeader};
use std::hash::Hasher;
use twox_hash::XxHash3_64;

/// Size of the placement that follows the batch header.
pub const PLACEMENT_SIZE: usize = 64;

/// Size of a placed batch: its batch header and its placement.
pub const PLACED_BATCH_SIZE: usize = BATCH_HEADER_SIZE + PLACEMENT_SIZE;

/// The bit of the batch header's flags byte that marks a placed batch.
pub const BATCH_PLACED: u8 = 1 << 1;

const VERSION: u32 = 1;
const OBJECT_ID_OFFSET: usize = 0;
const POSITION_OFFSET: usize = 16;
const LENGTH_OFFSET: usize = 24;
const BLOB_CHECKSUM_OFFSET: usize = 32;
const VERSION_OFFSET: usize = 40;
const RESERVED_OFFSET: usize = 44;
const CHECKSUM_OFFSET: usize = 56;

/// Where the blob of a batch sits in the object store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Placement {
    /// The object that holds the blob. The partition's cluster turns it into
    /// a key.
    pub object_id: u128,
    /// Where the blob starts in the object.
    pub position: u64,
    /// The blob's length, which the batch header's `batch_length` states as
    /// well.
    pub length: u64,
    /// XXH3 of the blob.
    pub blob_checksum: u64,
}

/// A decoded placed batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlacedBatch {
    pub header: BatchHeader,
    pub placement: Placement,
}

impl Placement {
    /// The placement of `blob`, stored at `position` of the object
    /// `object_id`.
    #[must_use]
    pub fn of(object_id: u128, position: u64, blob: &[u8]) -> Self {
        Self {
            object_id,
            position,
            length: blob.len() as u64,
            blob_checksum: blob_checksum(blob),
        }
    }

    /// Whether `blob` is the one this placement names.
    #[must_use]
    pub fn matches(&self, blob: &[u8]) -> bool {
        blob.len() as u64 == self.length && blob_checksum(blob) == self.blob_checksum
    }
}

impl PlacedBatch {
    /// The placed batch of `header`, whose blob is at `placement`.
    #[must_use]
    pub const fn new(header: BatchHeader, placement: Placement) -> Self {
        Self { header, placement }
    }

    /// Write the placed batch into the first [`PLACED_BATCH_SIZE`] bytes of
    /// `out`, with a zero `batch_checksum` and a fresh checksum of its own.
    ///
    /// # Panics
    /// When `out` is shorter than [`PLACED_BATCH_SIZE`].
    pub fn encode_into(&self, out: &mut [u8]) {
        let out = &mut out[..PLACED_BATCH_SIZE];
        let (header, placement) = out.split_at_mut(BATCH_HEADER_SIZE);
        BatchHeader {
            batch_checksum: 0,
            ..self.header
        }
        .encode_into(header);
        header[BATCH_PRODUCER_OFFSET] |= BATCH_PLACED;
        placement.fill(0);
        placement[OBJECT_ID_OFFSET..POSITION_OFFSET]
            .copy_from_slice(&self.placement.object_id.to_le_bytes());
        placement[POSITION_OFFSET..LENGTH_OFFSET]
            .copy_from_slice(&self.placement.position.to_le_bytes());
        placement[LENGTH_OFFSET..BLOB_CHECKSUM_OFFSET]
            .copy_from_slice(&self.placement.length.to_le_bytes());
        placement[BLOB_CHECKSUM_OFFSET..VERSION_OFFSET]
            .copy_from_slice(&self.placement.blob_checksum.to_le_bytes());
        placement[VERSION_OFFSET..RESERVED_OFFSET].copy_from_slice(&VERSION.to_le_bytes());
        seal(out);
    }

    /// # Errors
    /// [`WireError::UnexpectedEof`] on a short buffer.
    /// [`WireError::Validation`] on a checksum that does not match, a batch
    /// header without the placed flag, a version this build does not read,
    /// nonzero reserved bytes, a nonzero `batch_checksum`, or a length other
    /// than the batch header's blob length. Whatever [`BatchHeader::decode`]
    /// refuses of the header without its placed flag.
    pub fn decode(bytes: &[u8]) -> Result<Self, WireError> {
        if bytes.len() < PLACED_BATCH_SIZE {
            return Err(WireError::UnexpectedEof {
                offset: 0,
                need: PLACED_BATCH_SIZE,
                have: bytes.len(),
            });
        }
        let bytes = &bytes[..PLACED_BATCH_SIZE];
        let (header_bytes, placement) = bytes.split_at(BATCH_HEADER_SIZE);
        if u64_at(placement, CHECKSUM_OFFSET) != checksum(bytes) {
            return Err(invalid("placed batch checksum does not match"));
        }
        let version = u32::from_le_bytes(field(placement, VERSION_OFFSET));
        if version != VERSION {
            return Err(invalid("placed batch version is not supported"));
        }
        if placement[RESERVED_OFFSET..CHECKSUM_OFFSET]
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(invalid("placed batch reserved bytes are not zero"));
        }
        let header = placed_header(header_bytes)?;
        if header.batch_checksum != 0 {
            return Err(invalid("placed batch carries a batch checksum"));
        }
        let placement = Placement {
            object_id: u128::from_le_bytes(field(placement, OBJECT_ID_OFFSET)),
            position: u64_at(placement, POSITION_OFFSET),
            length: u64_at(placement, LENGTH_OFFSET),
            blob_checksum: u64_at(placement, BLOB_CHECKSUM_OFFSET),
        };
        if header.blob_len()? as u64 != placement.length {
            return Err(invalid("placed batch length differs from its batch header"));
        }
        if placement.position.checked_add(placement.length).is_none() {
            return Err(invalid("placed batch ends past the largest object"));
        }
        Ok(Self { header, placement })
    }
}

/// The batch header of a placed batch, from its first [`BATCH_HEADER_SIZE`]
/// bytes. It checks neither the placement nor the checksum.
///
/// # Errors
/// [`WireError::UnexpectedEof`] on a short buffer.
/// [`WireError::Validation`] on a header without the placed flag, and
/// whatever [`BatchHeader::decode`] refuses of the header without it.
pub fn placed_header(bytes: &[u8]) -> Result<BatchHeader, WireError> {
    if bytes.len() < BATCH_HEADER_SIZE {
        return Err(WireError::UnexpectedEof {
            offset: 0,
            need: BATCH_HEADER_SIZE,
            have: bytes.len(),
        });
    }
    let mut header = [0; BATCH_HEADER_SIZE];
    header.copy_from_slice(&bytes[..BATCH_HEADER_SIZE]);
    if header[BATCH_PRODUCER_OFFSET] & BATCH_PLACED == 0 {
        return Err(invalid("batch header does not mark a placed batch"));
    }
    header[BATCH_PRODUCER_OFFSET] &= !BATCH_PLACED;
    BatchHeader::decode(&header)
}

/// The batch header in the first bytes of `bytes`, whole batch or placed.
///
/// For server code that reads only what both carry, such as the offsets, the
/// message count and the producer.
///
/// # Errors
/// What [`BatchHeader::decode`] or [`placed_header`] refuses.
pub fn stored_header(bytes: &[u8]) -> Result<BatchHeader, WireError> {
    if is_placed(bytes) {
        placed_header(bytes)
    } else {
        BatchHeader::decode(bytes)
    }
}

/// Whether the batch header in the first bytes of `bytes` marks a placed
/// batch. False for a buffer too short to hold a flags byte.
#[must_use]
pub fn is_placed(bytes: &[u8]) -> bool {
    bytes
        .get(BATCH_PRODUCER_OFFSET)
        .is_some_and(|flags| flags & BATCH_PLACED != 0)
}

/// Recompute the checksum of the placed batch in the first
/// [`PLACED_BATCH_SIZE`] bytes of `bytes`, after its batch header changed, as
/// when the server stamps the offsets.
///
/// # Panics
/// When `bytes` is shorter than [`PLACED_BATCH_SIZE`].
pub fn seal(bytes: &mut [u8]) {
    let bytes = &mut bytes[..PLACED_BATCH_SIZE];
    bytes[BATCH_CHECKSUM_OFFSET..BATCH_CHECKSUM_OFFSET + size_of::<u64>()].fill(0);
    let checksum = checksum(bytes);
    bytes[BATCH_HEADER_SIZE + CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_le_bytes());
}

/// The checksum that the placed batch in the first [`PLACED_BATCH_SIZE`]
/// bytes of `bytes` carries. It tells one placed batch from another, as the
/// batch checksum tells whole batches apart.
///
/// # Panics
/// When `bytes` is shorter than [`PLACED_BATCH_SIZE`].
#[must_use]
pub fn checksum_of(bytes: &[u8]) -> u64 {
    u64_at(
        &bytes[BATCH_HEADER_SIZE..PLACED_BATCH_SIZE],
        CHECKSUM_OFFSET,
    )
}

/// XXH3 of a blob, as a placement records it.
#[must_use]
pub fn blob_checksum(blob: &[u8]) -> u64 {
    let mut hasher = XxHash3_64::new();
    hasher.write(blob);
    hasher.finish()
}

/// XXH3 of a placed batch up to its own checksum field.
fn checksum(placed: &[u8]) -> u64 {
    let mut hasher = XxHash3_64::new();
    hasher.write(&placed[..BATCH_HEADER_SIZE + CHECKSUM_OFFSET]);
    hasher.finish()
}

fn field<const N: usize>(bytes: &[u8], offset: usize) -> [u8; N] {
    let mut field = [0; N];
    field.copy_from_slice(&bytes[offset..offset + N]);
    field
}

fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(field(bytes, offset))
}

const fn invalid(reason: &'static str) -> WireError {
    WireError::Validation(std::borrow::Cow::Borrowed(reason))
}

#[cfg(test)]
mod tests {
    use super::*;

    const BLOB: &[u8] = b"sixteen byte blob";

    fn placed() -> PlacedBatch {
        let mut header = BatchHeader::new(3, 1_700_000_000_000_000, 0, 2);
        header.batch_length = (BATCH_HEADER_SIZE + BLOB.len()) as u64;
        header.base_offset = 40;
        header.base_timestamp = 1_700_000_000_000_500;
        PlacedBatch::new(header, Placement::of(0x0123_4567_89ab_cdef, 4096, BLOB))
    }

    fn encoded(placed: &PlacedBatch) -> [u8; PLACED_BATCH_SIZE] {
        let mut bytes = [0xff; PLACED_BATCH_SIZE];
        placed.encode_into(&mut bytes);
        bytes
    }

    #[test]
    fn given_a_placed_batch_when_encoding_and_decoding_should_give_it_back() {
        let placed = placed();

        assert_eq!(PlacedBatch::decode(&encoded(&placed)), Ok(placed));
    }

    #[test]
    fn given_a_batch_checksum_when_encoding_should_leave_it_out() {
        let mut placed = placed();
        placed.header.batch_checksum = 77;

        let decoded = PlacedBatch::decode(&encoded(&placed)).unwrap();

        assert_eq!(decoded.header.batch_checksum, 0);
        assert_eq!(decoded.placement, placed.placement);
    }

    #[test]
    fn given_a_blob_when_matching_should_accept_only_the_placed_one() {
        let placement = placed().placement;

        assert!(placement.matches(BLOB));
        assert!(!placement.matches(b"sixteen byte blot"));
        assert!(!placement.matches(&BLOB[1..]));
    }

    #[test]
    fn given_any_flipped_bit_when_decoding_should_refuse_the_placed_batch() {
        let bytes = encoded(&placed());

        for byte in 0..bytes.len() {
            let mut damaged = bytes;
            damaged[byte] ^= 1;
            assert!(
                PlacedBatch::decode(&damaged).is_err(),
                "a flipped bit in byte {byte} went unnoticed"
            );
        }
    }

    #[test]
    fn given_a_cut_placed_batch_when_decoding_should_report_the_missing_bytes() {
        let bytes = encoded(&placed());

        assert_eq!(
            PlacedBatch::decode(&bytes[..PLACED_BATCH_SIZE - 1]),
            Err(WireError::UnexpectedEof {
                offset: 0,
                need: PLACED_BATCH_SIZE,
                have: PLACED_BATCH_SIZE - 1,
            })
        );
    }

    #[test]
    fn given_a_placed_batch_when_decoding_its_header_as_a_batch_should_refuse_it() {
        let bytes = encoded(&placed());

        assert!(is_placed(&bytes));
        assert!(BatchHeader::decode(&bytes).is_err());
        assert_eq!(placed_header(&bytes), Ok(placed().header));
    }

    #[test]
    fn given_a_batch_header_without_the_flag_when_decoding_should_refuse_it() {
        let mut bytes = encoded(&placed());
        bytes[BATCH_PRODUCER_OFFSET] &= !BATCH_PLACED;
        seal(&mut bytes);

        assert!(!is_placed(&bytes));
        assert!(placed_header(&bytes).is_err());
        assert!(PlacedBatch::decode(&bytes).is_err());
    }

    #[test]
    fn given_an_idempotent_producer_when_encoding_and_decoding_should_keep_it() {
        let mut placed = placed();
        placed.header.producer = crate::batch::BatchProducer::new(9, 2, 100);

        let bytes = encoded(&placed);

        assert_eq!(PlacedBatch::decode(&bytes), Ok(placed));
        assert!(BatchHeader::decode(&bytes).is_err());
    }

    #[test]
    fn given_stamped_offsets_when_sealing_should_decode_with_them() {
        let mut bytes = encoded(&placed());
        let mut header = placed_header(&bytes).unwrap();
        header.base_offset = 900;
        header.base_timestamp = 1_700_000_000_999_999;
        header.encode_into(&mut bytes[..BATCH_HEADER_SIZE]);
        bytes[BATCH_PRODUCER_OFFSET] |= BATCH_PLACED;
        assert!(PlacedBatch::decode(&bytes).is_err());

        seal(&mut bytes);

        let decoded = PlacedBatch::decode(&bytes).unwrap();
        assert_eq!(decoded.header.base_offset, 900);
        assert_eq!(decoded.header.base_timestamp, 1_700_000_000_999_999);
        assert_eq!(decoded.placement, placed().placement);
    }

    #[test]
    fn given_a_placement_of_another_length_when_decoding_should_refuse_it() {
        let mut placed = placed();
        placed.placement.length += 1;

        assert!(PlacedBatch::decode(&encoded(&placed)).is_err());
    }

    #[test]
    fn given_a_later_version_when_decoding_should_refuse_it() {
        let mut bytes = encoded(&placed());
        bytes[BATCH_HEADER_SIZE + VERSION_OFFSET] = 2;
        let checksum = checksum(&bytes);
        bytes[BATCH_HEADER_SIZE + CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_le_bytes());

        assert!(PlacedBatch::decode(&bytes).is_err());
    }
}
