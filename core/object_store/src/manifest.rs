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

//! The manifest object of a partition incarnation: what the store holds of
//! it, for a reader that has the bucket and nothing else.
//!
//! The partition's primary writes it after every object it names is stored.
//! The cluster that writes it never reads it back as a source of truth: its
//! own record is the chain file of each replica.
//!
//! All integers are little-endian:
//!
//! ```text
//! magic | stream_id | topic_id | partition_id | created_revision |
//! published_at | flags | floor | trim_point | segment_count        10 x u64
//! per segment:
//!   start_offset | end_offset | start_timestamp | end_timestamp |
//!   max_timestamp | size                                           6 x u64
//!   crc32c u32 | 4 zero bytes | blake3                             40 bytes
//! crc32c of every byte before it                                   u32
//! ```
//!
//! The magic names the version. A reader refuses a magic it does not know.

use crate::layout::ObjectPrefix;
use crate::segment_metadata::SEGMENT_HASH_LEN;

const MAGIC: u64 = u64::from_le_bytes(*b"IGGYMAN1");
const HEADER_LEN: usize = 10 * size_of::<u64>();
const SEGMENT_LEN: usize = 6 * size_of::<u64>() + 2 * size_of::<u32>() + SEGMENT_HASH_LEN;
const CHECKSUM_LEN: usize = size_of::<u32>();

const HAS_FLOOR: u64 = 1;
const HAS_TRIM_POINT: u64 = 1 << 1;

/// A segment whose `.log` and `.index` objects the store holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestSegment {
    pub start_offset: u64,
    /// The offset of the segment's last message.
    pub end_offset: u64,
    /// Microseconds since the Unix epoch, like the other two timestamps.
    pub start_timestamp: u64,
    pub end_timestamp: u64,
    pub max_timestamp: u64,
    /// The size of the `.log` object.
    pub size: u64,
    /// CRC32C of the `.log` object's bytes.
    pub crc32c: u32,
    /// BLAKE3 of the `.log` object's bytes.
    pub hash: [u8; SEGMENT_HASH_LEN],
}

/// What the store holds of one partition incarnation.
///
/// A later manifest of the same incarnation never has a lower floor or a
/// lower trim point, so a reader can tell an older one from a newer one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionManifest {
    pub prefix: ObjectPrefix,
    /// When the primary built it, in microseconds since the Unix epoch.
    pub published_at: u64,
    /// The last offset the store holds, `None` while it holds no segment.
    pub floor: Option<u64>,
    /// Every offset at or below it is gone for good, `None` while retention
    /// removed nothing.
    pub trim_point: Option<u64>,
    /// In offset order. Each one ends at or below the floor and above the
    /// trim point.
    pub segments: Vec<ManifestSegment>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ManifestError {
    #[error("the manifest is {0} bytes, too short for its header and checksum")]
    Truncated(usize),
    #[error("the manifest starts with {0:#018x}, a version this build does not read")]
    UnsupportedVersion(u64),
    #[error("the manifest is {len} bytes, and {segments} segments take {expected}")]
    WrongLength {
        len: usize,
        segments: u64,
        expected: u64,
    },
    #[error("the manifest's checksum is {stored:#010x}, and its bytes hash to {computed:#010x}")]
    ChecksumMismatch { stored: u32, computed: u32 },
    #[error("the manifest's segment at offset {start_offset} is out of order or outside its range")]
    InvalidSegment { start_offset: u64 },
}

impl PartitionManifest {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes =
            Vec::with_capacity(HEADER_LEN + self.segments.len() * SEGMENT_LEN + CHECKSUM_LEN);
        let flags = if self.floor.is_some() { HAS_FLOOR } else { 0 }
            | if self.trim_point.is_some() {
                HAS_TRIM_POINT
            } else {
                0
            };
        for field in [
            MAGIC,
            self.prefix.stream_id,
            self.prefix.topic_id,
            self.prefix.partition_id,
            self.prefix.created_revision,
            self.published_at,
            flags,
            self.floor.unwrap_or_default(),
            self.trim_point.unwrap_or_default(),
            self.segments.len() as u64,
        ] {
            bytes.extend_from_slice(&field.to_le_bytes());
        }
        for segment in &self.segments {
            for field in [
                segment.start_offset,
                segment.end_offset,
                segment.start_timestamp,
                segment.end_timestamp,
                segment.max_timestamp,
                segment.size,
            ] {
                bytes.extend_from_slice(&field.to_le_bytes());
            }
            bytes.extend_from_slice(&segment.crc32c.to_le_bytes());
            bytes.extend_from_slice(&[0; size_of::<u32>()]);
            bytes.extend_from_slice(&segment.hash);
        }
        let checksum = crc32c::crc32c(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        bytes
    }

    /// # Errors
    /// See [`ManifestError`]. A manifest that fails here must not be used in
    /// part.
    pub fn decode(bytes: &[u8]) -> Result<Self, ManifestError> {
        if bytes.len() < HEADER_LEN + CHECKSUM_LEN {
            return Err(ManifestError::Truncated(bytes.len()));
        }
        let mut reader = Reader(bytes);
        let magic = reader.u64();
        if magic != MAGIC {
            return Err(ManifestError::UnsupportedVersion(magic));
        }
        let prefix = ObjectPrefix {
            stream_id: reader.u64(),
            topic_id: reader.u64(),
            partition_id: reader.u64(),
            created_revision: reader.u64(),
        };
        let published_at = reader.u64();
        let flags = reader.u64();
        let floor = reader.u64();
        let trim_point = reader.u64();
        let count = reader.u64();
        let expected = count
            .saturating_mul(SEGMENT_LEN as u64)
            .saturating_add((HEADER_LEN + CHECKSUM_LEN) as u64);
        if bytes.len() as u64 != expected {
            return Err(ManifestError::WrongLength {
                len: bytes.len(),
                segments: count,
                expected,
            });
        }
        let (body, checksum) = bytes.split_at(bytes.len() - CHECKSUM_LEN);
        let stored = u32::from_le_bytes(checksum.try_into().unwrap_or_default());
        let computed = crc32c::crc32c(body);
        if stored != computed {
            return Err(ManifestError::ChecksumMismatch { stored, computed });
        }
        let floor = (flags & HAS_FLOOR != 0).then_some(floor);
        let trim_point = (flags & HAS_TRIM_POINT != 0).then_some(trim_point);
        let mut segments = Vec::with_capacity(usize::try_from(count).unwrap_or_default());
        let mut above = trim_point;
        for _ in 0..count {
            let segment = ManifestSegment {
                start_offset: reader.u64(),
                end_offset: reader.u64(),
                start_timestamp: reader.u64(),
                end_timestamp: reader.u64(),
                max_timestamp: reader.u64(),
                size: reader.u64(),
                crc32c: reader.u32(),
                hash: {
                    reader.u32();
                    reader.hash()
                },
            };
            let in_order = above.is_none_or(|above| segment.start_offset > above)
                && segment.end_offset >= segment.start_offset
                && floor.is_some_and(|floor| segment.end_offset <= floor);
            if !in_order {
                return Err(ManifestError::InvalidSegment {
                    start_offset: segment.start_offset,
                });
            }
            above = Some(segment.end_offset);
            segments.push(segment);
        }
        Ok(Self {
            prefix,
            published_at,
            floor,
            trim_point,
            segments,
        })
    }
}

/// Reads fields off the front of bytes whose length the caller checked.
struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> [u8; N] {
        let (field, rest) = self.0.split_at(N);
        self.0 = rest;
        field.try_into().unwrap_or([0; N])
    }

    fn u64(&mut self) -> u64 {
        u64::from_le_bytes(self.take())
    }

    fn u32(&mut self) -> u32 {
        u32::from_le_bytes(self.take())
    }

    fn hash(&mut self) -> [u8; SEGMENT_HASH_LEN] {
        self.take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PREFIX: ObjectPrefix = ObjectPrefix {
        stream_id: 1,
        topic_id: 22,
        partition_id: 0,
        created_revision: 4096,
    };

    fn segment(start_offset: u64, end_offset: u64) -> ManifestSegment {
        ManifestSegment {
            start_offset,
            end_offset,
            start_timestamp: 1_000 + start_offset,
            end_timestamp: 1_000 + end_offset,
            max_timestamp: 1_000 + end_offset,
            size: (end_offset - start_offset + 1) * 100,
            crc32c: u32::try_from(start_offset).unwrap() ^ 0xdead_beef,
            hash: [u8::try_from(start_offset % 251).unwrap(); SEGMENT_HASH_LEN],
        }
    }

    fn manifest() -> PartitionManifest {
        PartitionManifest {
            prefix: PREFIX,
            published_at: 1_700_000_000_000_000,
            floor: Some(29),
            trim_point: Some(9),
            segments: vec![segment(10, 19), segment(20, 29)],
        }
    }

    #[test]
    fn given_a_manifest_when_encoding_and_decoding_should_give_it_back() {
        let manifest = manifest();

        assert_eq!(PartitionManifest::decode(&manifest.encode()), Ok(manifest));
    }

    #[test]
    fn given_no_segments_when_encoding_and_decoding_should_keep_the_absent_floor_and_trim_point() {
        let manifest = PartitionManifest {
            prefix: PREFIX,
            published_at: 7,
            floor: None,
            trim_point: None,
            segments: Vec::new(),
        };

        assert_eq!(PartitionManifest::decode(&manifest.encode()), Ok(manifest));
    }

    #[test]
    fn given_a_floor_of_zero_when_decoding_should_tell_it_from_no_floor() {
        let manifest = PartitionManifest {
            floor: Some(0),
            trim_point: None,
            segments: vec![segment(0, 0)],
            ..manifest()
        };

        assert_eq!(PartitionManifest::decode(&manifest.encode()), Ok(manifest));
    }

    #[test]
    fn given_any_flipped_bit_when_decoding_should_refuse_the_manifest() {
        let encoded = manifest().encode();

        for byte in 0..encoded.len() {
            let mut damaged = encoded.clone();
            damaged[byte] ^= 1;
            assert!(
                PartitionManifest::decode(&damaged).is_err(),
                "a flipped bit in byte {byte} went unnoticed"
            );
        }
    }

    #[test]
    fn given_a_cut_manifest_when_decoding_should_refuse_every_length() {
        let encoded = manifest().encode();

        for len in 0..encoded.len() {
            assert!(
                PartitionManifest::decode(&encoded[..len]).is_err(),
                "a manifest cut to {len} bytes decoded"
            );
        }
    }

    #[test]
    fn given_another_magic_when_decoding_should_report_an_unsupported_version() {
        let mut encoded = manifest().encode();
        encoded[..8].copy_from_slice(b"IGGYMAN2");

        assert_eq!(
            PartitionManifest::decode(&encoded),
            Err(ManifestError::UnsupportedVersion(u64::from_le_bytes(
                *b"IGGYMAN2"
            )))
        );
    }

    #[test]
    fn given_segments_outside_their_range_when_decoding_should_refuse_the_manifest() {
        let cases = [
            (
                "overlapping segments",
                vec![segment(10, 19), segment(19, 29)],
            ),
            (
                "segments out of order",
                vec![segment(20, 29), segment(10, 19)],
            ),
            ("a segment at the trim point", vec![segment(9, 19)]),
            ("a segment above the floor", vec![segment(10, 30)]),
        ];

        for (case, segments) in cases {
            let encoded = PartitionManifest {
                segments,
                ..manifest()
            }
            .encode();

            assert!(
                matches!(
                    PartitionManifest::decode(&encoded),
                    Err(ManifestError::InvalidSegment { .. })
                ),
                "{case}"
            );
        }
    }

    #[test]
    fn given_segments_without_a_floor_when_decoding_should_refuse_the_manifest() {
        let encoded = PartitionManifest {
            floor: None,
            ..manifest()
        }
        .encode();

        assert_eq!(
            PartitionManifest::decode(&encoded),
            Err(ManifestError::InvalidSegment { start_offset: 10 })
        );
    }
}
