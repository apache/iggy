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

//! Partition-plane state transfer: the offer a serving primary builds, the
//! receiver session with its disk-spilled segment staging, and the wire
//! codec for the consumer-offset artifact.
//!
//! A rejoining replica whose journal repair proved the gap below the commit
//! floor is unrepairable (`RepairConclusion::FloorRefused`) pulls this
//! partition's retained segments plus its consumer-offset table from the
//! group's caught-up primary, installs them, and hands the live tail back to
//! ordinary journal repair. Artifacts ride the plane-agnostic manifest/chunk
//! protocol from `core/consensus`; everything in this module is the
//! partition-specific payload handling on either end.

use consensus::le_cursor::{LeCursor, Truncated, split_verified_trailer};
use consensus::{ArtifactProgress, state_artifact_checksum};
use std::fmt;
use std::mem::size_of;
use std::path::PathBuf;

/// Framing marker for the consumer-offsets wire artifact, "ICO1".
pub const CONSUMER_OFFSETS_MAGIC: [u8; 4] = *b"ICO1";

/// Version byte following the magic.
///
/// Bump when the layout changes shape in a way appending fields cannot
/// express; a decoder rejects versions it does not know instead of
/// misreading trailing bytes as `Truncated`.
pub const CONSUMER_OFFSETS_VERSION: u8 = 1;

/// Per-section entry ceiling for the consumer-offsets artifact.
///
/// A corruption guard, not a target: it bounds the allocation `decode`
/// makes from a length field a peer sent, exactly like the manifest's own
/// entry ceiling.
pub const CONSUMER_OFFSETS_ENTRIES_MAX: u32 = 1 << 20;

/// One in-flight partition state transfer on the receiving replica.
///
/// Mirrors the metadata plane's session, plus `staged`: completed
/// `SEGMENT_LOG` artifacts are validated and spilled to `.staging` files as
/// they finish (bounding receiver memory to one in-flight artifact), and the
/// walk metadata recorded here is what the install consumes. NO retry budget
/// lives in here -- three of four metadata arming sites re-minted the
/// session, so a per-session counter bounded nothing; the budget is
/// [`crate::IggyPartition::transfer_attempts`], reset only on real progress.
#[derive(Debug)]
pub struct PartitionTransferSession {
    pub nonce: u128,
    /// Serving primary; also the stall re-request target.
    pub peer: u8,
    /// Serving peer's applied frontier from the accepted descriptor. Doubles
    /// as the decode-budget generation: segments are append-only, so a new
    /// commit frontier genuinely means new bytes.
    pub commit_op: u64,
    /// One slot per offered artifact, in manifest order. A slot moves from
    /// `Pending` to `Staged` when its segment payload is validated and
    /// spilled (freeing the buffer); the single enum makes a
    /// progress/spilled/staged desync unrepresentable.
    pub artifacts: Vec<TransferArtifact>,
    /// Whether a descriptor has been accepted (an accepted EMPTY manifest is
    /// distinguishable from "still waiting").
    pub target_accepted: bool,
    /// Ticks with no frame progress; at the configured repair-retry
    /// threshold the missing piece is re-requested.
    pub idle_ticks: u32,
}

/// A scheduled transfer re-arm (see `IggyPartition::transfer_rearm`).
///
/// The shard tick sweep counts `after_ticks` down and arms a fresh session
/// against `peer` when it reaches zero, provided nothing else armed one in
/// the meantime.
#[derive(Debug, Clone, Copy)]
pub struct PendingTransferRearm {
    pub peer: u8,
    pub after_ticks: u32,
}

/// One artifact slot of an in-flight partition transfer.
///
/// `SEGMENT_LOG` artifacts pass through both states; the consumer-offsets
/// artifact stays `Pending` until the install consumes its buffer.
#[derive(Debug)]
pub enum TransferArtifact {
    /// Still pulling: manifest entry plus the bytes received so far.
    Pending(ArtifactProgress),
    /// Validated and spilled to `.staging` files; the buffer is freed and
    /// the walk metadata is what the install consumes.
    Staged(StagedSegmentMeta),
}

impl TransferArtifact {
    #[must_use]
    pub const fn pending(&self) -> Option<&ArtifactProgress> {
        match self {
            Self::Pending(progress) => Some(progress),
            Self::Staged(_) => None,
        }
    }

    pub const fn pending_mut(&mut self) -> Option<&mut ArtifactProgress> {
        match self {
            Self::Pending(progress) => Some(progress),
            Self::Staged(_) => None,
        }
    }

    #[must_use]
    pub const fn complete(&self) -> bool {
        match self {
            Self::Pending(progress) => progress.complete(),
            Self::Staged(_) => true,
        }
    }
}

impl consensus::ChunkProgress for TransferArtifact {
    fn declared_len(&self) -> u64 {
        match self {
            Self::Pending(progress) => progress.entry.len,
            Self::Staged(meta) => meta.size,
        }
    }

    fn received_len(&self) -> u64 {
        match self {
            Self::Pending(progress) => progress.buf.len() as u64,
            // Staged == validated == every declared byte arrived; the chunk
            // cursor then skips it, subsuming the old `spilled` flags.
            Self::Staged(meta) => meta.size,
        }
    }

    fn extend_from_chunk(&mut self, payload: &[u8]) {
        match self {
            Self::Pending(progress) => progress.buf.extend_from_slice(payload),
            // Unreachable through `append_chunk`: a staged slot reports
            // itself complete, so no in-window offset can address it.
            Self::Staged(_) => debug_assert!(false, "chunk appended to a staged artifact"),
        }
    }
}

/// What the receiver learned walking one validated, staged segment artifact:
/// everything the install needs to rebuild the in-memory `Segment` without
/// re-reading the file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedSegmentMeta {
    pub start_offset: u64,
    pub end_offset: u64,
    /// Payload byte length == the manifest entry's `len` == the final `.log`
    /// file size.
    pub size: u64,
    pub start_timestamp: u64,
    pub end_timestamp: u64,
    pub max_timestamp: u64,
    /// `{start_offset:020}.log.staging` in the partition directory. The
    /// `.staging` extension is invisible to boot recovery, which filters on
    /// `extension == "log"`.
    pub log_staging: PathBuf,
    /// The locally rebuilt sparse index for the staged log, one entry per
    /// batch (denser than the origin's per-flush-chunk index; recovery is
    /// sparse-tolerant either way).
    pub index_staging: PathBuf,
}

/// The consumer-offset artifact: both offset maps plus the applied purge
/// generation, at the offer's `commit_op`.
///
/// The purge generation rides here because a receiver that missed a
/// `PurgeTopic` would otherwise install post-purge data at a stale local
/// generation and the reconciler would immediately re-wipe it, costing a
/// full extra transfer.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConsumerOffsetsWire {
    pub purge_generation: u64,
    /// The origin group's message-offset frontier: the offset the NEXT
    /// append will mint, `0` for a partition that never appended. Segments
    /// alone cannot carry this -- retention can GC every sealed segment
    /// while the counter stands at N, and installing such an offer without
    /// this field would restart the receiver's offset space at 0, forking
    /// every future batch stamp from the rest of the group.
    pub next_offset: u64,
    /// `(consumer id, offset)`, ascending by id.
    pub consumers: Vec<(u32, u64)>,
    /// `(consumer group id, offset)`, ascending by id.
    pub groups: Vec<(u32, u64)>,
}

impl ConsumerOffsetsWire {
    /// Encode: `magic | version u8 | purge_generation u64 | next_offset u64 |
    /// consumer_count u32 | group_count u32 | {id u32, offset u64}xN |
    /// {id u32, offset u64}xM | XxHash3_64 trailer`. Little-endian
    /// throughout.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        // Size exactly rather than guess; the reservation assert keeps the
        // arithmetic honest as fields are added.
        let reserved = CONSUMER_OFFSETS_MAGIC.len()
            + size_of::<u8>()
            + 2 * size_of::<u64>()
            + 2 * size_of::<u32>()
            + (self.consumers.len() + self.groups.len()) * (size_of::<u32>() + size_of::<u64>())
            + size_of::<u64>();
        let mut out = Vec::with_capacity(reserved);
        out.extend_from_slice(&CONSUMER_OFFSETS_MAGIC);
        out.push(CONSUMER_OFFSETS_VERSION);
        out.extend_from_slice(&self.purge_generation.to_le_bytes());
        out.extend_from_slice(&self.next_offset.to_le_bytes());
        #[allow(clippy::cast_possible_truncation)]
        out.extend_from_slice(&(self.consumers.len() as u32).to_le_bytes());
        #[allow(clippy::cast_possible_truncation)]
        out.extend_from_slice(&(self.groups.len() as u32).to_le_bytes());
        for (id, offset) in self.consumers.iter().chain(self.groups.iter()) {
            out.extend_from_slice(&id.to_le_bytes());
            out.extend_from_slice(&offset.to_le_bytes());
        }
        debug_assert_eq!(out.len() + size_of::<u64>(), reserved, "encode reservation");
        let trailer = state_artifact_checksum(&out);
        out.extend_from_slice(&trailer.to_le_bytes());
        out
    }

    /// Decode and validate a peer's consumer-offset artifact.
    ///
    /// The artifact checksum already verified transit; these validations are
    /// about the PEER's encoder (duplicate ids, count fields, trailing
    /// bytes), which the transit checksum cannot vouch for. Offset-value
    /// sanity is deliberately NOT here: it needs the installed end offset,
    /// so the install clamps, mirroring boot recovery.
    ///
    /// # Errors
    /// Any [`ConsumerOffsetsWireError`]; the input is never partially
    /// trusted.
    pub fn decode(bytes: &[u8]) -> Result<Self, ConsumerOffsetsWireError> {
        let content = split_verified_trailer(bytes).map_err(|mismatch| match mismatch {
            None => ConsumerOffsetsWireError::Truncated,
            Some((expected, actual)) => {
                ConsumerOffsetsWireError::ChecksumMismatch { expected, actual }
            }
        })?;
        let mut cursor = LeCursor::new(content);
        let magic = cursor.take(CONSUMER_OFFSETS_MAGIC.len())?;
        if magic != CONSUMER_OFFSETS_MAGIC {
            return Err(ConsumerOffsetsWireError::BadMagic);
        }
        let version = cursor.u8()?;
        if version != CONSUMER_OFFSETS_VERSION {
            return Err(ConsumerOffsetsWireError::UnsupportedVersion { version });
        }
        let purge_generation = cursor.u64()?;
        let next_offset = cursor.u64()?;
        let consumer_count = cursor.u32()?;
        let group_count = cursor.u32()?;
        let consumers = Self::decode_section(&mut cursor, "consumers", consumer_count)?;
        let groups = Self::decode_section(&mut cursor, "groups", group_count)?;
        if !cursor.remaining().is_empty() {
            return Err(ConsumerOffsetsWireError::Truncated);
        }
        Ok(Self {
            purge_generation,
            next_offset,
            consumers,
            groups,
        })
    }

    fn decode_section(
        cursor: &mut LeCursor<'_>,
        section: &'static str,
        count: u32,
    ) -> Result<Vec<(u32, u64)>, ConsumerOffsetsWireError> {
        // Ceiling BEFORE the reservation: `count` is peer input and this is
        // the only check between it and an eager allocation.
        if count > CONSUMER_OFFSETS_ENTRIES_MAX {
            return Err(ConsumerOffsetsWireError::TooManyEntries {
                section,
                count,
                max: CONSUMER_OFFSETS_ENTRIES_MAX,
            });
        }
        let mut entries = Vec::with_capacity(count as usize);
        let mut previous: Option<u32> = None;
        for _ in 0..count {
            let id = cursor.u32()?;
            let offset = cursor.u64()?;
            // Ascending-strict doubles as the duplicate reject and makes the
            // encoding canonical: one table, one byte sequence.
            if previous.is_some_and(|previous| id <= previous) {
                return Err(ConsumerOffsetsWireError::DuplicateConsumerId { section, id });
            }
            previous = Some(id);
            entries.push((id, offset));
        }
        Ok(entries)
    }
}

/// Failure decoding the consumer-offsets WIRE artifact (state transfer).
/// Named for the format: the on-disk offset files are a different codec with
/// different trust (this node's own bytes vs a peer's).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerOffsetsWireError {
    Truncated,
    BadMagic,
    UnsupportedVersion {
        version: u8,
    },
    ChecksumMismatch {
        expected: u64,
        actual: u64,
    },
    TooManyEntries {
        section: &'static str,
        count: u32,
        max: u32,
    },
    DuplicateConsumerId {
        section: &'static str,
        id: u32,
    },
}

impl From<Truncated> for ConsumerOffsetsWireError {
    fn from(_: Truncated) -> Self {
        Self::Truncated
    }
}

impl fmt::Display for ConsumerOffsetsWireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => write!(f, "consumer-offsets artifact is truncated"),
            Self::BadMagic => write!(f, "consumer-offsets artifact carries a foreign magic"),
            Self::UnsupportedVersion { version } => write!(
                f,
                "consumer-offsets artifact version {version} is not understood \
                 (this build speaks {CONSUMER_OFFSETS_VERSION})"
            ),
            Self::ChecksumMismatch { expected, actual } => write!(
                f,
                "consumer-offsets artifact checksum mismatch: expected {expected}, got {actual}"
            ),
            Self::TooManyEntries {
                section,
                count,
                max,
            } => write!(
                f,
                "consumer-offsets artifact {section} count {count} exceeds the {max} ceiling"
            ),
            Self::DuplicateConsumerId { section, id } => write!(
                f,
                "consumer-offsets artifact {section} id {id} is duplicated or out of order"
            ),
        }
    }
}

impl std::error::Error for ConsumerOffsetsWireError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> ConsumerOffsetsWire {
        ConsumerOffsetsWire {
            purge_generation: 3,
            next_offset: 43,
            consumers: vec![(1, 10), (7, 42)],
            groups: vec![(2, 5)],
        }
    }

    #[test]
    fn given_offset_table_when_encoded_should_round_trip() {
        let encoded = table().encode();
        assert_eq!(
            ConsumerOffsetsWire::decode(&encoded).expect("round trip"),
            table()
        );
    }

    #[test]
    fn given_empty_table_when_encoded_should_round_trip() {
        let empty = ConsumerOffsetsWire {
            purge_generation: 0,
            next_offset: 0,
            consumers: Vec::new(),
            groups: Vec::new(),
        };
        let encoded = empty.encode();
        assert_eq!(
            ConsumerOffsetsWire::decode(&encoded).expect("round trip"),
            empty
        );
    }

    #[test]
    fn given_flipped_bit_when_decoded_should_reject_checksum() {
        let mut encoded = table().encode();
        encoded[6] ^= 1;
        assert!(matches!(
            ConsumerOffsetsWire::decode(&encoded),
            Err(ConsumerOffsetsWireError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn given_truncated_bytes_when_decoded_should_reject() {
        let encoded = table().encode();
        for len in 0..encoded.len() {
            assert!(
                ConsumerOffsetsWire::decode(&encoded[..len]).is_err(),
                "strict prefix of {len} bytes must fail closed"
            );
        }
    }

    #[test]
    fn given_unknown_version_when_decoded_should_reject() {
        let mut wrong = table().encode();
        // Bump the version byte and re-seal so only the version check fires.
        wrong[CONSUMER_OFFSETS_MAGIC.len()] = CONSUMER_OFFSETS_VERSION + 1;
        let content_len = wrong.len() - size_of::<u64>();
        let trailer = state_artifact_checksum(&wrong[..content_len]);
        wrong[content_len..].copy_from_slice(&trailer.to_le_bytes());
        assert_eq!(
            ConsumerOffsetsWire::decode(&wrong),
            Err(ConsumerOffsetsWireError::UnsupportedVersion {
                version: CONSUMER_OFFSETS_VERSION + 1,
            })
        );
    }

    #[test]
    fn given_foreign_magic_when_decoded_should_reject() {
        let mut wrong = table().encode();
        // Rewrite the magic and re-seal so only the magic check can fire.
        wrong[0] = b'X';
        let content_len = wrong.len() - size_of::<u64>();
        let trailer = state_artifact_checksum(&wrong[..content_len]);
        wrong[content_len..].copy_from_slice(&trailer.to_le_bytes());
        assert_eq!(
            ConsumerOffsetsWire::decode(&wrong),
            Err(ConsumerOffsetsWireError::BadMagic)
        );
    }

    #[test]
    fn given_count_past_ceiling_when_decoded_should_reject_before_allocating() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&CONSUMER_OFFSETS_MAGIC);
        bytes.push(CONSUMER_OFFSETS_VERSION);
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&(CONSUMER_OFFSETS_ENTRIES_MAX + 1).to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        let trailer = state_artifact_checksum(&bytes);
        bytes.extend_from_slice(&trailer.to_le_bytes());
        assert_eq!(
            ConsumerOffsetsWire::decode(&bytes),
            Err(ConsumerOffsetsWireError::TooManyEntries {
                section: "consumers",
                count: CONSUMER_OFFSETS_ENTRIES_MAX + 1,
                max: CONSUMER_OFFSETS_ENTRIES_MAX,
            })
        );
    }

    #[test]
    fn given_duplicate_or_unordered_ids_when_decoded_should_reject() {
        let duplicate = ConsumerOffsetsWire {
            purge_generation: 0,
            next_offset: 0,
            consumers: vec![(5, 1), (5, 2)],
            groups: Vec::new(),
        };
        assert_eq!(
            ConsumerOffsetsWire::decode(&duplicate.encode()),
            Err(ConsumerOffsetsWireError::DuplicateConsumerId {
                section: "consumers",
                id: 5,
            })
        );
        let unordered = ConsumerOffsetsWire {
            purge_generation: 0,
            next_offset: 0,
            consumers: Vec::new(),
            groups: vec![(9, 1), (4, 2)],
        };
        assert_eq!(
            ConsumerOffsetsWire::decode(&unordered.encode()),
            Err(ConsumerOffsetsWireError::DuplicateConsumerId {
                section: "groups",
                id: 4,
            })
        );
    }

    #[test]
    fn given_trailing_bytes_when_decoded_should_reject() {
        let mut padded = table().encode();
        let content_len = padded.len() - size_of::<u64>();
        padded.truncate(content_len);
        padded.push(0);
        let trailer = state_artifact_checksum(&padded);
        padded.extend_from_slice(&trailer.to_le_bytes());
        assert_eq!(
            ConsumerOffsetsWire::decode(&padded),
            Err(ConsumerOffsetsWireError::Truncated),
            "bytes past the last section must fail closed"
        );
    }
}

/// What a full validation walk over one segment payload derived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentWalkStats {
    pub end_offset: u64,
    pub start_timestamp: u64,
    pub end_timestamp: u64,
    pub max_timestamp: u64,
}

/// Failure validating a transferred segment payload.
///
/// The artifact checksum already proved transit; these are about the bytes
/// themselves (the peer's disk, or its encoder), which transit integrity
/// cannot vouch for.
#[derive(Debug)]
pub enum SegmentWalkError {
    /// Batch header/checksum rejected at `position`.
    Batch {
        position: u64,
        source: iggy_common::IggyError,
    },
    /// First batch does not start at the artifact's declared base offset.
    BaseOffsetMismatch { expected: u64, actual: u64 },
    /// A batch's base offset does not continue the previous batch.
    NonContiguous { expected: u64, actual: u64 },
    /// A batch's offset arithmetic overflows `u64`; the operands are
    /// peer-controlled, so this is a rejection, not a clamp.
    OffsetOverflow { position: u64 },
    /// The payload holds no batches; empty segments are never offered.
    Empty,
}

impl fmt::Display for SegmentWalkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Batch { position, source } => {
                write!(f, "segment batch at byte {position} rejected: {source}")
            }
            Self::BaseOffsetMismatch { expected, actual } => write!(
                f,
                "segment first batch starts at offset {actual}, manifest says {expected}"
            ),
            Self::NonContiguous { expected, actual } => write!(
                f,
                "segment batch starts at offset {actual}, expected {expected}"
            ),
            Self::OffsetOverflow { position } => {
                write!(
                    f,
                    "segment batch at byte {position} overflows the offset space"
                )
            }
            Self::Empty => write!(f, "segment payload holds no batches"),
        }
    }
}

impl std::error::Error for SegmentWalkError {}

/// Walk every batch of a transferred `.log` payload.
///
/// Validates each header and `batch_checksum` (`decode_batch_slice`),
/// proves offset continuity from the manifest's declared base, and derives
/// the segment metadata plus a locally rebuilt sparse index (one 24-byte
/// entry per batch -- denser than the origin's per-flush-chunk index, which
/// recovery tolerates).
///
/// # Errors
/// [`SegmentWalkError`] on the first invalid byte; nothing is partially
/// trusted.
pub fn walk_segment_payload(
    base_offset: u64,
    bytes: &[u8],
) -> Result<(SegmentWalkStats, Vec<u8>), SegmentWalkError> {
    use server_common::send_messages2::decode_batch_slice;

    if bytes.is_empty() {
        return Err(SegmentWalkError::Empty);
    }
    let mut position = 0usize;
    let mut next_offset = base_offset;
    let mut stats: Option<SegmentWalkStats> = None;
    let mut index_bytes = Vec::new();
    while position < bytes.len() {
        let batch =
            decode_batch_slice(&bytes[position..]).map_err(|source| SegmentWalkError::Batch {
                position: position as u64,
                source,
            })?;
        let header = batch.header;
        if stats.is_none() && header.base_offset != base_offset {
            return Err(SegmentWalkError::BaseOffsetMismatch {
                expected: base_offset,
                actual: header.base_offset,
            });
        }
        if header.base_offset != next_offset {
            return Err(SegmentWalkError::NonContiguous {
                expected: next_offset,
                actual: header.base_offset,
            });
        }
        if header.message_count == 0 {
            return Err(SegmentWalkError::Batch {
                position: position as u64,
                source: iggy_common::IggyError::InvalidMessagesCount,
            });
        }
        // Peer-controlled operands under a reject-on-first-invalid contract:
        // checked, not saturating -- a clamp would misdirect the diagnostic.
        let Some(batch_end) = header
            .base_offset
            .checked_add(u64::from(header.message_count) - 1)
        else {
            return Err(SegmentWalkError::OffsetOverflow {
                position: position as u64,
            });
        };
        // The append-time canonical stamp, exactly what the flush path writes
        // into index entries and segment bounds; `origin_timestamp` is
        // client-supplied and would give the installed replica a divergent
        // timestamp column (polls and retention keyed differently per node).
        let timestamp = header.base_timestamp;
        // One sparse-index entry per batch, pointing at the batch start.
        index_bytes.extend_from_slice(&header.base_offset.to_le_bytes());
        index_bytes.extend_from_slice(&timestamp.to_le_bytes());
        index_bytes.extend_from_slice(&(position as u64).to_le_bytes());
        stats = Some(stats.map_or(
            SegmentWalkStats {
                end_offset: batch_end,
                start_timestamp: timestamp,
                end_timestamp: timestamp,
                max_timestamp: timestamp,
            },
            |previous| SegmentWalkStats {
                end_offset: batch_end,
                start_timestamp: previous.start_timestamp,
                end_timestamp: timestamp,
                max_timestamp: previous.max_timestamp.max(timestamp),
            },
        ));
        next_offset = batch_end
            .checked_add(1)
            .ok_or(SegmentWalkError::OffsetOverflow {
                position: position as u64,
            })?;
        // No trailing-bytes guard: the header decode floors `batch_length`
        // at the 256-byte command header (no zero-step loop is possible)
        // and `decode_batch_slice` already rejects a body shorter than
        // `total_size()`.
        position += header.total_size();
    }
    stats.map_or(Err(SegmentWalkError::Empty), |stats| {
        Ok((stats, index_bytes))
    })
}

/// One offered segment: its manifest entry plus WHERE its bytes live.
///
/// The offer deliberately holds paths, not payloads -- the serving side
/// loads one artifact at a time at chunk-serve time, bounding its memory to
/// one segment per requester regardless of how much the partition retains.
#[derive(Debug, Clone)]
pub struct SegmentArtifactSource {
    pub entry: consensus::StateArtifact,
    pub log_path: String,
}

/// One artifact of an offer, addressed by manifest index: segment payloads
/// live on disk (loaded at chunk-serve time), the offsets table is resident.
#[derive(Debug)]
pub enum PartitionArtifactSource<'a> {
    Segment(&'a SegmentArtifactSource),
    Offsets(&'a std::rc::Rc<Vec<u8>>),
}

/// A built partition state-transfer offer: everything at `commit_op`, with
/// segment payloads addressed by path and only the (small) offsets artifact
/// resident.
#[derive(Debug)]
pub struct PartitionStateTransferOffer {
    /// `== commit_min == commit_max` at build (caught-up primary gate).
    pub commit_op: u64,
    /// Ascending base offset; one artifact per non-empty retained segment.
    pub segments: Vec<SegmentArtifactSource>,
    /// The consumer-offsets artifact, resident (a few KB at most).
    pub offsets: (consensus::StateArtifact, std::rc::Rc<Vec<u8>>),
}

impl PartitionStateTransferOffer {
    /// Manifest order: segments ascending, then the offsets artifact last,
    /// so a receiver spills every segment before it holds the table.
    #[must_use]
    pub fn manifest(&self) -> Vec<consensus::StateArtifact> {
        let mut entries: Vec<_> = self.segments.iter().map(|source| source.entry).collect();
        entries.push(self.offsets.0);
        entries
    }

    /// Never zero: the offsets artifact is always present.
    #[must_use]
    pub const fn artifact_count(&self) -> usize {
        self.segments.len() + 1
    }

    /// The artifact at `index` in [`Self::manifest`] order (segments
    /// ascending, offsets last) without materialising the manifest vec.
    #[must_use]
    pub fn artifact_at(&self, index: usize) -> Option<PartitionArtifactSource<'_>> {
        match index.cmp(&self.segments.len()) {
            std::cmp::Ordering::Less => {
                Some(PartitionArtifactSource::Segment(&self.segments[index]))
            }
            std::cmp::Ordering::Equal => Some(PartitionArtifactSource::Offsets(&self.offsets.1)),
            std::cmp::Ordering::Greater => None,
        }
    }

    #[must_use]
    pub fn total_len(&self) -> u64 {
        self.segments
            .iter()
            .map(|source| source.entry.len)
            .sum::<u64>()
            + self.offsets.0.len
    }
}

/// Why a partition cannot serve a state transfer right now.
///
/// Distinct variants because the operator responses differ: "not the
/// caught-up primary" is routine (requester retries elsewhere), an
/// unreadable segment is a local fault on THIS node.
#[derive(Debug)]
pub enum PartitionTransferUnavailable {
    NotCaughtUpPrimary,
    /// In-memory / simulated partition: nothing on disk to serve.
    NoPartitionDir,
    RepairInProgress,
    FlushFailed(iggy_common::IggyError),
    SegmentUnreadable {
        start_offset: u64,
        source: std::io::Error,
    },
}

impl fmt::Display for PartitionTransferUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotCaughtUpPrimary => write!(f, "not the caught-up primary of this group"),
            Self::NoPartitionDir => write!(f, "partition has no on-disk directory"),
            Self::RepairInProgress => write!(f, "partition is itself mid-repair"),
            Self::FlushFailed(source) => {
                write!(f, "flushing the committed prefix failed: {source}")
            }
            Self::SegmentUnreadable {
                start_offset,
                source,
            } => write!(f, "segment {start_offset:0>20}.log is unreadable: {source}"),
        }
    }
}

impl std::error::Error for PartitionTransferUnavailable {}

/// Outcome of a completed install. A degraded install is a SUCCESS: the
/// segments and floor landed; only some consumer-offset file writes failed,
/// and the next offset commit blind-writes those files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionInstallOutcome {
    pub applied_frontier: u64,
    /// Every transferred offset file was WRITTEN (and the offset
    /// directories fsynced, so the old files' unlinks stick). Not a
    /// durability claim for the file contents: `persist_offset` fsyncs only
    /// under `consumer_offset_enforce_fsync`, matching the normal
    /// offset-commit path -- shipped default off.
    pub offsets_written: bool,
}

/// Failure installing a transferred partition state. Every `check`-phase
/// variant means NOTHING was mutated.
#[derive(Debug)]
pub enum PartitionInstallError {
    NoPartitionDir,
    /// `commit_op` fell below this replica's commit frontier; installing
    /// would rewind `commit_min` (the anti-rewind assert, as a refusal).
    StaleTransfer {
        commit_op: u64,
        commit_min: u64,
    },
    Offsets(ConsumerOffsetsWireError),
    /// Duplicate base offset in the staged set.
    DuplicateSegment {
        start_offset: u64,
    },
    /// A hole between consecutive staged segments.
    SegmentSetHole {
        previous_end: u64,
        next_start: u64,
    },
    /// Filesystem failure at/after the swap; disk holds a contiguous prefix
    /// of the new state and a crash-restart recovers it (see the module
    /// crash-window notes). The IN-MEMORY partition is converged to an
    /// empty, honestly-lagging state before this returns, so the live
    /// process stays serviceable and the normal triggers re-transfer.
    SwapIo {
        path: String,
        source: std::io::Error,
    },
    /// Re-opening an installed segment failed; disk holds the full new
    /// state, a restart boot-recovers it.
    SegmentOpen {
        path: String,
        source: iggy_common::IggyError,
    },
    /// The post-failure convergence itself failed: the partition holds no
    /// serviceable segment chain and every append or poll would panic. The
    /// caller must fence this one partition (tear it down for the
    /// reconciler to rebuild from disk) instead of leaving a live handle
    /// whose first use kills the whole shard.
    ConvergeFailed {
        source: iggy_common::IggyError,
    },
}

impl fmt::Display for PartitionInstallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoPartitionDir => write!(f, "partition has no on-disk directory"),
            Self::StaleTransfer {
                commit_op,
                commit_min,
            } => write!(
                f,
                "transfer frontier {commit_op} is below the local commit frontier {commit_min}"
            ),
            Self::Offsets(source) => write!(f, "consumer-offsets artifact rejected: {source}"),
            Self::DuplicateSegment { start_offset } => {
                write!(f, "duplicate staged segment at base offset {start_offset}")
            }
            Self::SegmentSetHole {
                previous_end,
                next_start,
            } => write!(
                f,
                "staged segment set holds a hole: previous ends at {previous_end}, next starts at {next_start}"
            ),
            Self::SwapIo { path, source } => write!(f, "swap io failed at {path}: {source}"),
            Self::SegmentOpen { path, source } => {
                write!(f, "re-opening installed segment {path} failed: {source}")
            }
            Self::ConvergeFailed { source } => write!(
                f,
                "post-failure convergence failed, the partition must be fenced: {source}"
            ),
        }
    }
}

impl std::error::Error for PartitionInstallError {}

impl From<ConsumerOffsetsWireError> for PartitionInstallError {
    fn from(source: ConsumerOffsetsWireError) -> Self {
        Self::Offsets(source)
    }
}

use crate::messages_writer::MessagesWriter;
use crate::offset_storage::{delete_persisted_offset, persist_offset};
use crate::segment::Segment;
use crate::types::PartitionsConfig;
use crate::{IggyIndexWriter, IggyPartition};
use consensus::Sequencer as _;
use consensus::state_manifest::artifact_kind;
use iggy_common::{ConsumerGroupId, ConsumerKind, ConsumerOffset, IggyByteSize};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use server_common::SegmentStorage;
use std::rc::Rc;
use std::sync::atomic::Ordering;

/// Staging-file names inside the partition directory. The `.staging`
/// extension is provably invisible to boot recovery, which filters on
/// `extension == "log"`.
fn staging_paths(partition_dir: &str, start_offset: u64) -> (PathBuf, PathBuf) {
    (
        PathBuf::from(format!("{partition_dir}/{start_offset:0>20}.log.staging")),
        PathBuf::from(format!("{partition_dir}/{start_offset:0>20}.index.staging")),
    )
}

fn final_paths(partition_dir: &str, start_offset: u64) -> (String, String) {
    (
        format!("{partition_dir}/{start_offset:0>20}.log"),
        format!("{partition_dir}/{start_offset:0>20}.index"),
    )
}

/// fsync the partition directory so a rename made durable stays durable.
/// Async so the wait parks the task instead of the whole shard reactor;
/// every other future on the pump keeps running through it.
async fn fsync_dir(partition_dir: &str) -> std::io::Result<()> {
    compio::fs::File::open(partition_dir)
        .await?
        .sync_all()
        .await
}

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// Build (or serve from cache) this group's state-transfer offer.
    ///
    /// Force-flushes the committed prefix first so the segments cover every
    /// committed `SendMessages` op and the offset table covers every
    /// committed offset op; `commit_op = commit_min` then names the exact
    /// state the artifacts represent. Segment bytes are NOT loaded here: the
    /// offer records `(entry, path)` and the serving side loads one artifact
    /// at a time, so building costs one streaming checksum pass per segment
    /// and the resident footprint is just the offsets table.
    ///
    /// # Errors
    /// [`PartitionTransferUnavailable`]; the requester falls back to journal
    /// repair or retries after the next trigger.
    pub async fn state_transfer_offer(
        &mut self,
        config: &PartitionsConfig,
    ) -> Result<Rc<PartitionStateTransferOffer>, PartitionTransferUnavailable> {
        if !consensus::is_caught_up_primary(self.consensus()) {
            return Err(PartitionTransferUnavailable::NotCaughtUpPrimary);
        }
        if self.partition_dir.is_none() {
            // Also defuses the in-memory trap where `segment.size` grows with
            // no bytes on disk ("simulated in-memory batch persistence").
            return Err(PartitionTransferUnavailable::NoPartitionDir);
        }
        if self.repair.is_some() {
            return Err(PartitionTransferUnavailable::RepairInProgress);
        }
        self.flush_committed_messages(config)
            .await
            .map_err(PartitionTransferUnavailable::FlushFailed)?;
        let commit_op = self.consensus().commit_min();
        if let Some(cached) = self.transfer_offer_cache.borrow().as_ref()
            && cached.commit_op == commit_op
        {
            return Ok(Rc::clone(cached));
        }

        // Under the write lock so GC (`remove_sealed_segments_up_to`, also
        // write-locked) cannot unlink a file between enumeration and read.
        let write_lock = self.write_lock.clone();
        let _guard = write_lock.lock().await;
        let mut segments = Vec::with_capacity(self.log.segments().len());
        for (segment, storage) in self.log.segments().iter().zip(self.log.storages()) {
            let size = segment.size.as_bytes_u64();
            if size == 0 {
                continue;
            }
            let (log_path, _) = storage.segment_and_index_paths();
            let Some(log_path) = log_path else {
                return Err(PartitionTransferUnavailable::SegmentUnreadable {
                    start_offset: segment.start_offset,
                    source: std::io::Error::other("segment holds bytes but no backing file"),
                });
            };
            // Checksum pass over exactly `segment.size` bytes: the file can
            // be LONGER after a failed-index-save rewind, and the size
            // counter is what readers bound by. Chunked, with an await per
            // chunk: this runs on the pump task, and a whole-file read +
            // hash of a large segment would stall every consensus tick on
            // this core long enough to trip view-change timers. Resident
            // footprint is one chunk; only the manifest entry is retained.
            let checksum = hash_segment_prefix(&log_path, size)
                .await
                .map_err(|source| PartitionTransferUnavailable::SegmentUnreadable {
                    start_offset: segment.start_offset,
                    source,
                })?;
            segments.push(SegmentArtifactSource {
                entry: consensus::StateArtifact {
                    kind: artifact_kind::SEGMENT_LOG,
                    frontier: segment.start_offset,
                    len: size,
                    checksum,
                },
                log_path: log_path.clone(),
            });
        }

        let offsets_wire = self.offsets_wire_snapshot();
        let offsets_bytes = Rc::new(offsets_wire.encode());
        let offsets_entry = consensus::StateArtifact::for_bytes(
            artifact_kind::CONSUMER_OFFSETS,
            commit_op,
            &offsets_bytes,
        );
        let offer = Rc::new(PartitionStateTransferOffer {
            commit_op,
            segments,
            offsets: (offsets_entry, offsets_bytes),
        });
        *self.transfer_offer_cache.borrow_mut() = Some(Rc::clone(&offer));
        Ok(offer)
    }

    /// Release the cached offer once no requester holds one (the shard's
    /// offer-expiry sweep).
    pub fn clear_state_transfer_offer_cache(&self) {
        self.transfer_offer_cache.borrow_mut().take();
    }

    /// Snapshot the live offset maps + purge generation into the wire shape.
    /// Eagerly auto-committed offsets can run slightly ahead of committed
    /// state; that is safe because their covering ops sit in
    /// `(commit_op, commit_max]`, which the receiver's tail repair replays,
    /// and offset applies converge (monotone auto-commit, verbatim stores).
    fn offsets_wire_snapshot(&self) -> ConsumerOffsetsWire {
        // Every key is minted from a u32 wire id, so the narrowing filter is
        // an invariant, not a policy: say so out loud instead of silently
        // shrinking the snapshot when it ever breaks.
        let mut consumers: Vec<(u32, u64)> = self
            .consumer_offsets
            .pin()
            .iter()
            .filter_map(|(id, offset)| {
                let narrowed = u32::try_from(*id).ok();
                debug_assert!(narrowed.is_some(), "consumer offset key {id} exceeds u32");
                narrowed.map(|id| (id, offset.offset.load(Ordering::Acquire)))
            })
            .collect();
        consumers.sort_unstable_by_key(|(id, _)| *id);
        let mut groups: Vec<(u32, u64)> = self
            .consumer_group_offsets
            .pin()
            .iter()
            .filter_map(|(id, offset)| {
                let narrowed = u32::try_from(id.0).ok();
                debug_assert!(
                    narrowed.is_some(),
                    "consumer group offset key {} exceeds u32",
                    id.0
                );
                narrowed.map(|id| (id, offset.offset.load(Ordering::Acquire)))
            })
            .collect();
        groups.sort_unstable_by_key(|(id, _)| *id);
        // The append counter, not the segment end: retention can GC every
        // sealed segment while the counter stands at N, and the receiver
        // must resume minting at N either way.
        let next_offset = if self.should_increment_offset {
            self.offset.load(Ordering::Acquire) + 1
        } else {
            0
        };
        ConsumerOffsetsWire {
            purge_generation: self.applied_purge_generation,
            next_offset,
            consumers,
            groups,
        }
    }

    /// Validate one completed `SEGMENT_LOG` artifact and spill it to staging
    /// files, returning the walk metadata. Frees receiver memory as it goes:
    /// after this the session drops the artifact's buffer.
    ///
    /// # Errors
    /// `Err(walk error description)` when the payload fails validation (the
    /// caller charges the decode budget), or a staging-write failure
    /// description.
    pub async fn spill_transfer_segment(
        &self,
        entry: &consensus::StateArtifact,
        bytes: Vec<u8>,
    ) -> Result<StagedSegmentMeta, SpillError> {
        let Some(partition_dir) = self.partition_dir.clone() else {
            return Err(SpillError::NoPartitionDir);
        };
        // Artifact-level integrity FIRST, exactly as the metadata plane
        // verifies every artifact before decoding: the walk's per-batch
        // checksums prove batch bodies, not that these are the bytes the
        // manifest promised (length alone is implied by completion).
        if !consensus::verify_state_artifact(entry, &bytes) {
            return Err(SpillError::ManifestChecksum {
                frontier: entry.frontier,
            });
        }
        let (stats, index_bytes) =
            walk_segment_payload(entry.frontier, &bytes).map_err(SpillError::Walk)?;
        let (log_staging, index_staging) = staging_paths(&partition_dir, entry.frontier);
        // Two writes, not a loop: each moves its buffer into compio's
        // owned-buffer API, so a segment-sized payload is never copied.
        write_staging_file(&log_staging, bytes)
            .await
            .map_err(|source| SpillError::StagingIo {
                path: log_staging.clone(),
                source,
            })?;
        write_staging_file(&index_staging, index_bytes)
            .await
            .map_err(|source| SpillError::StagingIo {
                path: index_staging.clone(),
                source,
            })?;
        fsync_dir(&partition_dir)
            .await
            .map_err(|source| SpillError::StagingIo {
                path: PathBuf::from(&partition_dir),
                source,
            })?;
        Ok(StagedSegmentMeta {
            start_offset: entry.frontier,
            end_offset: stats.end_offset,
            size: entry.len,
            start_timestamp: stats.start_timestamp,
            end_timestamp: stats.end_timestamp,
            max_timestamp: stats.max_timestamp,
            log_staging,
            index_staging,
        })
    }

    /// Adopt an already-verified staged log without rewriting it: walk the
    /// payload once (validation + a rebuilt sparse index), write only the
    /// index sidecar, and return the walk metadata. The reuse scan calls
    /// this after `verify_state_artifact` proved the bytes match the
    /// manifest; rewriting the byte-identical log (and re-verifying a third
    /// time) is exactly the work reuse exists to skip. The index write
    /// stays: the scan never checks `.index.staging`, and a missing sidecar
    /// would hand the install a missing rename source.
    async fn adopt_staged_segment(
        &self,
        entry: &consensus::StateArtifact,
        bytes: &[u8],
    ) -> Result<StagedSegmentMeta, SpillError> {
        let Some(partition_dir) = self.partition_dir.clone() else {
            return Err(SpillError::NoPartitionDir);
        };
        let (stats, index_bytes) =
            walk_segment_payload(entry.frontier, bytes).map_err(SpillError::Walk)?;
        let (log_staging, index_staging) = staging_paths(&partition_dir, entry.frontier);
        write_staging_file(&index_staging, index_bytes)
            .await
            .map_err(|source| SpillError::StagingIo {
                path: index_staging.clone(),
                source,
            })?;
        fsync_dir(&partition_dir)
            .await
            .map_err(|source| SpillError::StagingIo {
                path: PathBuf::from(&partition_dir),
                source,
            })?;
        Ok(StagedSegmentMeta {
            start_offset: entry.frontier,
            end_offset: stats.end_offset,
            size: entry.len,
            start_timestamp: stats.start_timestamp,
            end_timestamp: stats.end_timestamp,
            max_timestamp: stats.max_timestamp,
            log_staging,
            index_staging,
        })
    }

    /// Scan the partition directory for staging files left by an earlier
    /// session and adopt every one that matches a manifest entry byte-for-
    /// byte (length + artifact checksum + full re-walk). Sealed segments are
    /// immutable, so on a retry or peer re-target typically only the active
    /// segment and the offsets artifact re-pull. Staging strays matching no
    /// entry are swept.
    pub async fn reuse_staged_segments(
        &self,
        manifest: &[consensus::StateArtifact],
    ) -> Vec<(u32, StagedSegmentMeta)> {
        let Some(partition_dir) = self.partition_dir.clone() else {
            return Vec::new();
        };
        let mut adopted = Vec::new();
        let mut matched_paths = Vec::new();
        for (index, entry) in manifest.iter().enumerate() {
            if entry.kind != artifact_kind::SEGMENT_LOG {
                continue;
            }
            let (log_staging, _) = staging_paths(&partition_dir, entry.frontier);
            let Ok(bytes) = compio::fs::read(&log_staging).await else {
                continue;
            };
            if !consensus::verify_state_artifact(entry, &bytes) {
                continue;
            }
            if let Ok(meta) = self.adopt_staged_segment(entry, &bytes).await {
                matched_paths.push(meta.log_staging.clone());
                matched_paths.push(meta.index_staging.clone());
                #[allow(clippy::cast_possible_truncation)]
                adopted.push((index as u32, meta));
            }
        }
        // Sweep strays: anything `.staging` that no adopted meta claims.
        if let Ok(entries) = std::fs::read_dir(&partition_dir) {
            for dir_entry in entries.flatten() {
                let path = dir_entry.path();
                let is_staging = path.to_str().is_some_and(|path| path.ends_with(".staging"));
                if is_staging && !matched_paths.contains(&path) {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
        adopted
    }

    /// Install a fully transferred partition state: swap the staged segment
    /// files in, rebuild the in-memory log over them, replace the consumer
    /// offset tables, clear the journal, and lift the commit floor to
    /// `commit_op`. The live tail `(commit_op, commit_max]` is left to
    /// ordinary journal repair.
    ///
    /// Two-phase: every validation runs before any mutation. The mutate
    /// phase's crash windows all recover as an honestly-shorter partition
    /// (see the swap ordering comments); no durable completeness claim
    /// exists anywhere, so boot re-derives from whatever files survive.
    ///
    /// # Errors
    /// [`PartitionInstallError`]; check-phase variants mutate nothing.
    #[allow(clippy::too_many_lines)]
    pub async fn install_state_transfer(
        &mut self,
        config: &PartitionsConfig,
        commit_op: u64,
        mut staged: Vec<StagedSegmentMeta>,
        offsets_bytes: &[u8],
    ) -> Result<PartitionInstallOutcome, PartitionInstallError> {
        // ---- check phase: nothing below may mutate ----
        let Some(partition_dir) = self.partition_dir.clone() else {
            return Err(PartitionInstallError::NoPartitionDir);
        };
        let commit_min = self.consensus().commit_min();
        if commit_op < commit_min {
            // The receiver's commit walk is frozen while transferring (the
            // `is_transferring` dispatch gates), and install runs on the
            // single pump task, so this is a refusal of a genuinely stale
            // offer, not a race.
            return Err(PartitionInstallError::StaleTransfer {
                commit_op,
                commit_min,
            });
        }
        let offsets_wire = ConsumerOffsetsWire::decode(offsets_bytes)?;
        staged.sort_unstable_by_key(|meta| meta.start_offset);
        for pair in staged.windows(2) {
            if pair[1].start_offset == pair[0].start_offset {
                return Err(PartitionInstallError::DuplicateSegment {
                    start_offset: pair[1].start_offset,
                });
            }
            if pair[1].start_offset != pair[0].end_offset + 1 {
                return Err(PartitionInstallError::SegmentSetHole {
                    previous_end: pair[0].end_offset,
                    next_start: pair[1].start_offset,
                });
            }
        }

        // ---- mutate phase ----
        // The write lock spans the convergence too: a mutate failure leaves
        // the segment vectors drained, and a concurrent replicated append
        // indexing `segments().len() - 1` on the emptied vec is exactly the
        // race every other segment-vec mutator takes this lock against.
        let write_lock = self.write_lock.clone();
        let _guard = write_lock.lock().await;
        let outcome = self
            .apply_checked_install(config, commit_op, staged, &offsets_wire, &partition_dir)
            .await;
        if outcome.is_err() {
            // A mutate-phase failure can leave the log drained or half
            // rebuilt while the disk already holds any prefix of the new
            // chain. Converge BOTH the live state and the disk to an empty,
            // honestly-lagging partition, so the next flush or poll cannot
            // hit an empty segment vec and no stray chain can resurrect at
            // boot; the normal triggers re-transfer the rest. The offset
            // counter still seeds from the artifact frontier: a replica
            // that resumed minting at 0 would fork its batch stamps from
            // the group. A convergence failure outranks the install error:
            // the partition cannot serve and the caller must fence it.
            self.converge_to_empty_after_failed_install(config, offsets_wire.next_offset)
                .await
                .map_err(|source| PartitionInstallError::ConvergeFailed { source })?;
        }
        outcome
    }

    /// The install's mutate phase; every early return is a failure the
    /// caller converges from. Split out so the convergence handling cannot
    /// be forgotten on a new error path. Runs under the caller's write-lock
    /// guard.
    #[allow(clippy::too_many_lines)]
    async fn apply_checked_install(
        &mut self,
        config: &PartitionsConfig,
        commit_op: u64,
        staged: Vec<StagedSegmentMeta>,
        offsets_wire: &ConsumerOffsetsWire,
        partition_dir: &str,
    ) -> Result<PartitionInstallOutcome, PartitionInstallError> {
        // Sweep staging strays a dead earlier attempt left behind, keeping
        // only what THIS install is about to rename. Bounded disk hygiene;
        // the reuse-scan sweeps too, but an abandoned-to-repair transfer
        // that never re-arms would otherwise leak a partition copy forever.
        let keep: Vec<&PathBuf> = staged
            .iter()
            .flat_map(|meta| [&meta.log_staging, &meta.index_staging])
            .collect();
        if let Ok(entries) = std::fs::read_dir(partition_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let is_staging = path.to_str().is_some_and(|path| path.ends_with(".staging"));
                if is_staging && !keep.iter().any(|kept| **kept == path) {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }

        // Unlink the old segment chain oldest-first (a crash mid-loop leaves
        // the NEWEST suffix, which is contiguous) and drop the in-memory
        // vectors in lockstep, exactly as `purge` does.
        let namespace_raw = self.consensus().namespace();
        let segment_count = self.log.segments().len();
        for _ in 0..segment_count {
            self.log.segments_mut().remove(0);
            let mut storage = self.log.storages_mut().remove(0);
            self.log.indexes_mut().remove(0);
            self.log.messages_writers_mut().remove(0);
            self.log.index_writers_mut().remove(0);
            let (messages_path, index_path) = storage.segment_and_index_paths();
            let _ = storage.shutdown();
            drop(storage);
            for path in messages_path.into_iter().chain(index_path) {
                match compio::fs::remove_file(&path).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        // Propagated, not shrugged off: a surviving old
                        // `.log` outside the staged set resurrects at boot
                        // (recovery takes every `.log` stem), and can push
                        // the recovered chain past the installed one. The
                        // converge path re-sweeps the directory.
                        warn_unlink(namespace_raw, &path, &error);
                        return Err(PartitionInstallError::SwapIo {
                            path,
                            source: error,
                        });
                    }
                }
            }
        }
        fsync_dir(partition_dir)
            .await
            .map_err(|source| PartitionInstallError::SwapIo {
                path: partition_dir.to_owned(),
                source,
            })?;

        // Rename staged -> final: ALL indexes first, one directory fsync,
        // then each log rename with its own fsync (N+1 total, down from 2N).
        // Every index is durable before any log rename is issued -- strictly
        // stronger than index_i-before-log_i -- because boot recovery
        // derives segment bounds from the index and treats a `.log` without
        // its `.index` as fatal, while an orphaned `.index` is invisible
        // (recovery keys on `.log` stems). Each log rename remains its
        // segment's commit point, and a crash mid-loop leaves a strict
        // PREFIX of the new chain visible, which boots as a shorter
        // contiguous partition and re-triggers transfer for the rest. Not
        // fewer fsyncs than this: one-per-pair would lean on intra-directory
        // rename ordering POSIX does not grant.
        for meta in &staged {
            let (_, index_final) = final_paths(partition_dir, meta.start_offset);
            compio::fs::rename(&meta.index_staging, &index_final)
                .await
                .map_err(|source| PartitionInstallError::SwapIo {
                    path: index_final.clone(),
                    source,
                })?;
        }
        fsync_dir(partition_dir)
            .await
            .map_err(|source| PartitionInstallError::SwapIo {
                path: partition_dir.to_owned(),
                source,
            })?;
        for meta in &staged {
            let (log_final, _) = final_paths(partition_dir, meta.start_offset);
            compio::fs::rename(&meta.log_staging, &log_final)
                .await
                .map_err(|source| PartitionInstallError::SwapIo {
                    path: log_final.clone(),
                    source,
                })?;
            fsync_dir(partition_dir)
                .await
                .map_err(|source| PartitionInstallError::SwapIo {
                    path: partition_dir.to_owned(),
                    source,
                })?;
        }

        // Rebuild the in-memory log over the installed files: sealed
        // segments with metadata from the validation walk, real storage over
        // the final paths, and writers on the LAST segment only (the
        // hydrate pattern; earlier segments are sealed and never written).
        for meta in &staged {
            let (log_final, index_final) = final_paths(partition_dir, meta.start_offset);
            let index_len = compio::fs::metadata(&index_final)
                .await
                .map_or(0, |metadata| metadata.len());
            let storage = SegmentStorage::new(
                &log_final,
                &index_final,
                meta.size,
                index_len,
                config.enforce_fsync,
                config.enforce_fsync,
                true,
            )
            .await
            .map_err(|source| PartitionInstallError::SegmentOpen {
                path: log_final.clone(),
                source,
            })?;
            let mut segment = Segment::new(meta.start_offset, config.segment_size);
            segment.sealed = true;
            segment.start_timestamp = meta.start_timestamp;
            segment.end_timestamp = meta.end_timestamp;
            segment.max_timestamp = meta.max_timestamp;
            segment.end_offset = meta.end_offset;
            segment.size = IggyByteSize::from(meta.size);
            segment.current_position = meta.size;
            self.log.add_persisted_segment(segment, storage, None, None);
        }
        if staged.is_empty() {
            // An empty offered set (everything GC'd behind the consumer
            // barrier on the sender) installs a fresh segment at the
            // artifact's frontier, exactly where rotation would have put
            // it, so post-install traffic lands in a segment named for the
            // offsets it holds.
            self.install_empty_segment(config, offsets_wire.next_offset)
                .await
                .map_err(|source| PartitionInstallError::SegmentOpen {
                    path: partition_dir.to_owned(),
                    source,
                })?;
        } else {
            let last = self.log.segments().len() - 1;
            let storage = self.log.storages()[last].clone();
            if let (Some(messages_reader), Some(index_reader), Some(messages_w), Some(index_w)) = (
                storage.messages_reader.as_ref(),
                storage.index_reader.as_ref(),
                storage.messages_writer.as_ref(),
                storage.index_writer.as_ref(),
            ) {
                let messages_writer = MessagesWriter::new(
                    &messages_reader.path(),
                    messages_w.size_counter(),
                    config.enforce_fsync,
                    true,
                )
                .await
                .map_err(|source| PartitionInstallError::SegmentOpen {
                    path: messages_reader.path(),
                    source,
                })?;
                let index_writer = IggyIndexWriter::new(
                    &index_reader.path(),
                    index_w.size_counter(),
                    config.enforce_fsync,
                    true,
                )
                .await
                .map_err(|source| PartitionInstallError::SegmentOpen {
                    path: index_reader.path(),
                    source,
                })?;
                self.log.messages_writers_mut()[last] = Some(Rc::new(messages_writer));
                self.log.index_writers_mut()[last] = Some(Rc::new(index_writer));
            }
            self.log.segments_mut()[last].sealed = false;
        }

        // The installed segments supersede every journaled op; stale
        // residents (below OR above the floor) would collide with the new
        // view's prepares. Memory-only journal, so a full clear IS the
        // suffix truncation.
        self.log.journal().inner.clear_all();
        // The wrapper's flush accounting too, or thresholds and tail-repair
        // appends fold onto a pre-install base until the first real evict.
        self.log.journal_mut().info = crate::log::JournalInfo::default();

        // Consumer offsets: replace both maps through the SAME Arcs (the
        // data plane holds clones), unlink the old files, install the
        // transferred entries with locally minted paths, clamped to the
        // installed end like boot recovery clamps.
        let installed_end = staged.last().map(|meta| meta.end_offset);
        let mut offsets_written = true;
        // A key that fails the u32 narrowing would strand its old offset
        // file's delete, which boot can then resurrect: unreachable while
        // keys are minted from u32 wire ids, so assert it.
        let old_consumer_paths: Vec<String> = {
            let guard = self.consumer_offsets.pin();
            let paths = guard
                .iter()
                .filter_map(|(key, _)| {
                    let narrowed = u32::try_from(*key).ok();
                    debug_assert!(narrowed.is_some(), "consumer offset key {key} exceeds u32");
                    narrowed.and_then(|id| self.persisted_offset_path(ConsumerKind::Consumer, id))
                })
                .collect();
            guard.clear();
            paths
        };
        let old_group_paths: Vec<String> = {
            let guard = self.consumer_group_offsets.pin();
            let paths = guard
                .iter()
                .filter_map(|(key, _)| {
                    let narrowed = u32::try_from(key.0).ok();
                    debug_assert!(
                        narrowed.is_some(),
                        "consumer group offset key {} exceeds u32",
                        key.0
                    );
                    narrowed
                        .and_then(|id| self.persisted_offset_path(ConsumerKind::ConsumerGroup, id))
                })
                .collect();
            guard.clear();
            paths
        };
        for path in old_consumer_paths.into_iter().chain(old_group_paths) {
            let _ = delete_persisted_offset(&path).await;
        }
        self.persisted_offsets.borrow_mut().clear();
        self.pending_consumer_offset_commits.clear();
        self.last_polled_offsets.pin().clear();

        let clamp = |offset: u64| installed_end.map_or(0, |end| offset.min(end));
        if self.consumer_offsets_path.is_none() || self.consumer_group_offsets_path.is_none() {
            // Nothing to write the transferred table into: unreachable via
            // the server boot paths (they always configure storage), but if
            // it ever fires the table was dropped and the flag must say so.
            offsets_written = false;
        }
        if let Some(dir) = self.consumer_offsets_path.clone() {
            for (id, offset) in &offsets_wire.consumers {
                let value = clamp(*offset);
                let entry = ConsumerOffset::default_for_consumer(*id, &dir);
                entry.offset.store(value, Ordering::Release);
                let path = entry.path.clone();
                self.consumer_offsets.pin().insert(*id as usize, entry);
                if persist_offset(&path, value, self.consumer_offset_enforce_fsync)
                    .await
                    .is_ok()
                {
                    self.persisted_offsets
                        .borrow_mut()
                        .insert((ConsumerKind::Consumer, *id), value);
                } else {
                    offsets_written = false;
                }
            }
        }
        if let Some(dir) = self.consumer_group_offsets_path.clone() {
            for (id, offset) in &offsets_wire.groups {
                let value = clamp(*offset);
                let group_id = ConsumerGroupId(*id as usize);
                let entry = ConsumerOffset::default_for_consumer_group(group_id, &dir);
                entry.offset.store(value, Ordering::Release);
                let path = entry.path.clone();
                self.consumer_group_offsets.pin().insert(group_id, entry);
                if persist_offset(&path, value, self.consumer_offset_enforce_fsync)
                    .await
                    .is_ok()
                {
                    self.persisted_offsets
                        .borrow_mut()
                        .insert((ConsumerKind::ConsumerGroup, *id), value);
                } else {
                    offsets_written = false;
                }
            }
        }
        // Directory fsync so the OLD files' unlinks stick: without it a
        // crash right after install resurrects the pre-transfer offset
        // files at boot. The per-file content durability stays governed by
        // `consumer_offset_enforce_fsync` like every other offset commit.
        for dir in self
            .consumer_offsets_path
            .clone()
            .into_iter()
            .chain(self.consumer_group_offsets_path.clone())
        {
            if fsync_dir(&dir).await.is_err() {
                offsets_written = false;
            }
        }

        // Counters and stats. The offset counter seeds from the ARTIFACT's
        // frontier, not the installed segments: base offsets are minted
        // locally per replica, so a counter behind the group (all sealed
        // segments GC'd at the origin, empty active one skipped) would stamp
        // the next replicated batch differently from the primary -- same op,
        // different persisted bytes, different checksum. Segments can only
        // trail the artifact (both were built at `commit_op`), so take the
        // max defensively. The stats mutate through the EXISTING Arc:
        // partition counters are never snapshotted, and the data plane's
        // registered handle must keep reading the same cells.
        let next_offset = offsets_wire
            .next_offset
            .max(installed_end.map_or(0, |end| end + 1));
        let end = next_offset.saturating_sub(1);
        self.offset.store(end, Ordering::Release);
        self.dirty_offset.store(end, Ordering::Relaxed);
        self.should_increment_offset = next_offset > 0;
        self.recovered_durable_offset = installed_end;
        self.stats.zero_out_all();
        #[allow(clippy::cast_possible_truncation)]
        self.stats
            .increment_segments_count(self.log.segments().len() as u32);
        self.stats
            .increment_size_bytes(staged.iter().map(|meta| meta.size).sum());
        self.stats.increment_messages_count(
            staged
                .iter()
                .map(|meta| meta.end_offset - meta.start_offset + 1)
                .sum(),
        );
        self.stats.set_current_offset(end);

        // A receiver that missed a purge must not be re-wiped by the
        // reconciler right after installing post-purge data.
        self.applied_purge_generation = self
            .applied_purge_generation
            .max(offsets_wire.purge_generation);

        let consensus = self.consensus();
        if commit_op > consensus.commit_min() {
            consensus.set_commit_floor(commit_op);
        }
        if commit_op > consensus.sequencer().current_sequence() {
            consensus.sequencer().set_sequence(commit_op);
        }
        consensus.advance_commit_max(commit_op);
        self.observed_view = self.consensus().view();
        self.repair = None;
        self.transfer_offer_cache.borrow_mut().take();

        Ok(PartitionInstallOutcome {
            applied_frontier: commit_op,
            offsets_written,
        })
    }
}

/// See `install_state_transfer`'s failure arm.
impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// Converge the live partition AND its directory to an empty,
    /// honestly-lagging shape after a failed install: no segment files at
    /// all (the failure can land anywhere from "old chain unlinked" to
    /// "new chain fully renamed in", and any survivor would either be
    /// truncated in place by the empty plant or resurrect at boot in front
    /// of / behind a later install), a fresh empty segment at the group's
    /// offset frontier, an empty journal, and boot-equivalent counters.
    /// Consensus state (commit floor, view) is left alone; the replica is
    /// simply behind, and the normal triggers re-transfer.
    ///
    /// # Errors
    /// [`iggy_common::IggyError`] when the sweep or the empty plant fails;
    /// the partition then has no serviceable chain and the caller must
    /// fence it (see [`PartitionInstallError::ConvergeFailed`]).
    async fn converge_to_empty_after_failed_install(
        &mut self,
        config: &PartitionsConfig,
        minted_next_offset: u64,
    ) -> Result<(), iggy_common::IggyError> {
        let segment_count = self.log.segments().len();
        for _ in 0..segment_count {
            self.log.segments_mut().remove(0);
            let mut storage = self.log.storages_mut().remove(0);
            self.log.indexes_mut().remove(0);
            self.log.messages_writers_mut().remove(0);
            self.log.index_writers_mut().remove(0);
            let _ = storage.shutdown();
        }
        self.log.journal().inner.clear_all();
        self.log.journal_mut().info = crate::log::JournalInfo::default();

        // Sweep EVERY segment file, not the in-memory count's worth: after
        // a late failure the renamed-in new chain is on disk while the
        // in-memory vectors were already drained, so only the directory
        // itself knows what needs unlinking.
        if let Some(partition_dir) = self.partition_dir.clone() {
            let swept: Vec<PathBuf> = match std::fs::read_dir(&partition_dir) {
                Ok(entries) => entries
                    .flatten()
                    .map(|entry| entry.path())
                    .filter(|path| {
                        path.to_str().is_some_and(|path| {
                            [".log", ".index", ".staging"]
                                .iter()
                                .any(|extension| path.ends_with(extension))
                        })
                    })
                    .collect(),
                Err(error) => {
                    tracing::error!(
                        target: "iggy.partitions.diag",
                        plane = "partitions",
                        namespace_raw = self.consensus().namespace(),
                        partition_dir,
                        %error,
                        "converge sweep cannot list the partition directory"
                    );
                    return Err(iggy_common::IggyError::CannotReadPartitions);
                }
            };
            for path in swept {
                match compio::fs::remove_file(&path).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        warn_unlink(
                            self.consensus().namespace(),
                            &path.display().to_string(),
                            &error,
                        );
                        return Err(iggy_common::IggyError::CannotDeleteFile);
                    }
                }
            }
            fsync_dir(&partition_dir)
                .await
                .map_err(|_| iggy_common::IggyError::CannotSyncFile)?;
        }

        self.install_empty_segment(config, minted_next_offset)
            .await?;
        // Empty data, but NOT offset zero: the artifact already proved the
        // group's frontier, and a counter reset would stamp the next
        // replicated batch differently from the primary. Only the durable
        // claim goes back to None; the partition is honestly lagging.
        let end = minted_next_offset.saturating_sub(1);
        self.offset.store(end, Ordering::Release);
        self.dirty_offset.store(end, Ordering::Relaxed);
        self.should_increment_offset = minted_next_offset > 0;
        self.recovered_durable_offset = None;
        self.stats.zero_out_all();
        self.stats.increment_segments_count(1);
        self.repair = None;
        self.transfer_offer_cache.borrow_mut().take();
        Ok(())
    }
}

/// Failure validating or staging one transferred segment artifact.
#[derive(Debug)]
pub enum SpillError {
    NoPartitionDir,
    /// The received bytes are not what the manifest promised.
    ManifestChecksum {
        frontier: u64,
    },
    /// The payload failed its format validation walk.
    Walk(SegmentWalkError),
    /// Writing or syncing a staging file failed.
    StagingIo {
        path: PathBuf,
        source: std::io::Error,
    },
}

impl fmt::Display for SpillError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoPartitionDir => write!(f, "partition has no on-disk directory"),
            Self::ManifestChecksum { frontier } => write!(
                f,
                "segment artifact at base offset {frontier} fails its manifest checksum"
            ),
            Self::Walk(source) => write!(f, "{source}"),
            Self::StagingIo { path, source } => {
                write!(f, "staging io failed at {}: {source}", path.display())
            }
        }
    }
}

impl std::error::Error for SpillError {}

/// Chunk size for the offer build's streaming checksum pass. Large enough
/// that per-chunk overhead is noise, small enough that the pump yields to
/// the reactor many times per segment.
const OFFER_HASH_CHUNK_LEN: usize = 1 << 20;

/// `XxHash3_64` over the first `len` bytes of `path`, read in
/// [`OFFER_HASH_CHUNK_LEN`] chunks with one reactor yield per chunk. Errors
/// on a file shorter than `len`: the segment accounts bytes the disk does
/// not hold.
async fn hash_segment_prefix(path: &str, len: u64) -> std::io::Result<u64> {
    use compio::io::AsyncReadAtExt;
    let file = compio::fs::File::open(path).await?;
    let mut hasher = consensus::state_manifest::StateArtifactHasher::new();
    let mut position = 0u64;
    while position < len {
        #[allow(clippy::cast_possible_truncation)]
        let want = OFFER_HASH_CHUNK_LEN.min((len - position) as usize);
        let compio::BufResult(read, buf) = file.read_exact_at(vec![0u8; want], position).await;
        read.map_err(|_| {
            std::io::Error::other(format!(
                "file ends at {position}+, segment accounts {len} bytes"
            ))
        })?;
        hasher.update(&buf);
        position += want as u64;
    }
    Ok(hasher.finish())
}

async fn write_staging_file(path: &PathBuf, payload: Vec<u8>) -> std::io::Result<()> {
    use compio::io::AsyncWriteAtExt;
    let mut file = compio::fs::File::create(path).await?;
    let (result, _) = file.write_all_at(payload, 0).await.into();
    result?;
    file.sync_data().await?;
    Ok(())
}

fn warn_unlink(namespace_raw: u64, path: &str, error: &std::io::Error) {
    tracing::warn!(
        target: "iggy.partitions.diag",
        plane = "partitions",
        namespace_raw,
        path = %path,
        %error,
        "failed to unlink segment file during state-transfer install"
    );
}
