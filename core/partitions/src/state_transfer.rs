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

use crate::IggyPartition;
use crate::offset_storage::{
    PURGE_GENERATION_FILE, discard_offset_replacement, stage_offset_replacement,
};
use crate::segment_anchor::ANCHOR_SUFFIX;
use crate::types::PartitionsConfig;
use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use consensus::le_cursor::{LeCursor, Truncated, split_verified_trailer};
use consensus::state_manifest::artifact_kind;
use consensus::{
    ArtifactProgress, DedupWatermark, Sequencer as _, StateArtifactHasher, state_artifact_checksum,
};
use iggy_binary_protocol::{Operation, PrepareHeader};
use iggy_common::{ConsumerGroupId, ConsumerKind, ConsumerOffset};
use journal::durable_storage::{DiskStorage, DurableStorage};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use server_common::Message;
use server_common::iobuf::Owned;
use server_common::send_messages::{decode_batch_slice, decode_prepare_slice};
use server_common::sharding::IggyNamespace;
use server_common::yield_to_reactor;
use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::Ordering;

/// Current state-transfer offsets format, including the prepare-chain anchor.
pub(crate) const CONSUMER_OFFSETS_MAGIC: [u8; 4] = *b"ICO1";
pub(crate) const CONSUMER_OFFSETS_VERSION: u8 = 1;

/// Per-section entry ceiling for the consumer-offsets artifact.
///
/// A corruption guard, not a target: it bounds the allocation `decode`
/// makes from a length field a peer sent, exactly like the manifest's own
/// entry ceiling.
pub const CONSUMER_OFFSETS_ENTRIES_MAX: u32 = 1 << 20;

/// Wire stride of one dedup entry: client u128 + watermark u64 + commit u64 +
/// user u32 + committed window u128.
const DEDUP_ENTRY_LEN: usize = 2 * size_of::<u128>() + 2 * size_of::<u64>() + size_of::<u32>();

/// One in-flight partition state transfer on the receiving replica.
///
/// Mirrors the metadata plane's session, plus `staged`: completed
/// `SEGMENT_LOG` artifacts are validated and spilled to `.staging` files as
/// they finish (bounding receiver memory to one in-flight artifact), and the
/// walk metadata recorded here is what the install consumes. NO retry budget
/// lives in here -- three of four metadata arming sites re-minted the
/// session, so a per-session counter bounded nothing. The partition plane's
/// budgets live on the partition: the stall budget
/// (`transfer_attempts`, reset on received chunks) and the consecutive
/// failure count driving the re-arm backoff (reset only by a completed
/// install; deliberately NOT generation-keyed -- a committing origin
/// advances its generation every round). A deterministically undecodable
/// artifact therefore re-pulls once per backed-off round, capped at 1024x
/// the base interval, rather than being refused outright.
#[derive(Debug)]
pub struct PartitionTransferSession {
    pub nonce: u128,
    /// Serving primary; also the stall re-request target.
    pub peer: u8,
    /// Serving peer's applied frontier from the accepted descriptor.
    ///
    /// Not a decode-budget generation: this plane keeps no such budget (see the
    /// struct doc), it counts consecutive failures on the partition instead.
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
            // Delegated, not re-implemented: the two must agree about how a
            // buffer grows, and the reservation below only fires if this arm
            // routes through the same impl.
            Self::Pending(progress) => progress.extend_from_chunk(payload),
            // Unreachable through `append_chunk`: a staged slot reports
            // itself complete, so no in-window offset can address it.
            Self::Staged(_) => debug_assert!(false, "chunk appended to a staged artifact"),
        }
    }

    fn reserve_declared(&mut self) {
        match self {
            Self::Pending(progress) => progress.reserve_declared(),
            Self::Staged(_) => {}
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
    /// Byte length of the locally rebuilt sparse index sidecar, recorded at
    /// the walk so the install does not re-stat the renamed file.
    pub index_size: u64,
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

/// Memoized streaming checksum state for one segment file: the hasher fed
/// exactly `hashed_len` of its bytes, plus the stamp at that length.
///
/// Keyed per segment rather than per `(base offset, size)` pair so the ACTIVE
/// segment extends its own hasher as it grows, instead of missing the memo on
/// every byte it gained and re-reading from byte zero. Every path that plants a
/// new file at an existing base offset (purge, install, converge) clears the
/// whole map, which is what makes "same base offset, longer file, same leading
/// bytes" hold.
pub(crate) struct SegmentChecksumMemo {
    hashed_len: u64,
    /// The stamp is NOT cached alongside: `StateArtifactHasher::finish` takes
    /// `&self`, so it is a read of this hasher, and a second copy is just a
    /// field that can drift.
    hasher: StateArtifactHasher,
}

impl SegmentChecksumMemo {
    fn new() -> Self {
        Self {
            hashed_len: 0,
            hasher: StateArtifactHasher::new(),
        }
    }
}

/// The result of the last staged-segment reuse scan.
///
/// A scan reads every length-matching staged file whole, verifies it, and
/// re-walks every batch -- sequentially, on the pump. Peer rotation and stall
/// re-arms mint a fresh session against the same segment set, so without this
/// the full cost is re-paid per arm. `digest` covers every `SEGMENT_LOG`
/// manifest entry, so a hit means the new offer expects byte-identical staged
/// files; any write to a staging file, or any unlink of one, drops the memo, so
/// a hit can never describe bytes that were replaced meanwhile.
pub(crate) struct ReuseScanMemo {
    digest: u64,
    adopted: Vec<(u32, StagedSegmentMeta)>,
}

impl StagedSegmentMeta {
    /// Assemble the metadata a completed walk produced. Shared by the spill and
    /// the reuse-adopt path, which differ only in whether they also wrote the
    /// payload.
    const fn from_walk(
        entry: &consensus::StateArtifact,
        stats: SegmentWalkStats,
        index_size: u64,
        log_staging: PathBuf,
        index_staging: PathBuf,
    ) -> Self {
        Self {
            start_offset: entry.frontier,
            end_offset: stats.end_offset,
            size: entry.len,
            index_size,
            start_timestamp: stats.start_timestamp,
            end_timestamp: stats.end_timestamp,
            max_timestamp: stats.max_timestamp,
            log_staging,
            index_staging,
        }
    }
}

/// The consumer-offset artifact: both offset maps plus the applied purge
/// generation, at the offer's `commit_op`.
///
/// The purge generation rides here because a receiver that missed a
/// `PurgeTopic` would otherwise install post-purge data at a stale local
/// generation and the reconciler would immediately re-wipe it, costing a
/// full extra transfer.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct ConsumerOffsetsWire {
    pub purge_generation: u64,
    /// Checksum of the prepare at the offer's committed operation.
    pub prepare_checksum: Option<u128>,
    pub checkpoint_prepare: Vec<u8>,
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
    /// This group's dedup slice, ascending by client. Carried so a replica
    /// rejoining behind the repair floor can absorb a replay of what the group
    /// already committed instead of re-executing it.
    pub dedup: Vec<DedupWatermark>,
}

impl ConsumerOffsetsWire {
    /// Encode: `magic | version u8 | purge_generation u64 | next_offset u64 |
    /// consumer_count u32 | group_count u32 | dedup_count u32 |
    /// {id u32, offset u64}xN | {id u32, offset u64}xM |
    /// {client u128, watermark u64, latest_commit u64, user_id u32,
    /// committed_window u128}xD | checksum_present u8 | prepare_checksum u128 |
    /// prepare_length u32 | checkpoint_prepare bytes | XxHash3_64 trailer`. Little-endian throughout.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        // Size exactly rather than guess; the reservation assert keeps the
        // arithmetic honest as fields are added.
        let reserved = CONSUMER_OFFSETS_MAGIC.len()
            + size_of::<u8>()
            + 2 * size_of::<u64>()
            + 3 * size_of::<u32>()
            + (self.consumers.len() + self.groups.len()) * (size_of::<u32>() + size_of::<u64>())
            + self.dedup.len() * DEDUP_ENTRY_LEN
            + size_of::<u8>()
            + size_of::<u128>()
            + size_of::<u32>()
            + self.checkpoint_prepare.len()
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
        #[allow(clippy::cast_possible_truncation)]
        out.extend_from_slice(&(self.dedup.len() as u32).to_le_bytes());
        for (id, offset) in self.consumers.iter().chain(self.groups.iter()) {
            out.extend_from_slice(&id.to_le_bytes());
            out.extend_from_slice(&offset.to_le_bytes());
        }
        for entry in &self.dedup {
            out.extend_from_slice(&entry.client.to_le_bytes());
            out.extend_from_slice(&entry.watermark.to_le_bytes());
            out.extend_from_slice(&entry.latest_commit.to_le_bytes());
            out.extend_from_slice(&entry.user_id.to_le_bytes());
            out.extend_from_slice(&entry.committed_window.to_le_bytes());
        }
        out.push(u8::from(self.prepare_checksum.is_some()));
        out.extend_from_slice(&self.prepare_checksum.unwrap_or(0).to_le_bytes());
        out.extend_from_slice(
            &u32::try_from(self.checkpoint_prepare.len())
                .expect("bounded checkpoint prepare")
                .to_le_bytes(),
        );
        out.extend_from_slice(&self.checkpoint_prepare);
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
        let version = cursor.u8()?;
        if magic != CONSUMER_OFFSETS_MAGIC {
            return Err(ConsumerOffsetsWireError::BadMagic);
        }
        if version != CONSUMER_OFFSETS_VERSION {
            return Err(ConsumerOffsetsWireError::UnsupportedVersion { version });
        }
        let purge_generation = cursor.u64()?;
        let next_offset = cursor.u64()?;
        let consumer_count = cursor.u32()?;
        let group_count = cursor.u32()?;
        let dedup_count = cursor.u32()?;
        let consumers = Self::decode_section(&mut cursor, "consumers", consumer_count)?;
        let groups = Self::decode_section(&mut cursor, "groups", group_count)?;
        let dedup = Self::decode_dedup_section(&mut cursor, dedup_count)?;
        let present = cursor.u8()?;
        let checksum = cursor.u128()?;
        let prepare_checksum = match present {
            0 if checksum == 0 => None,
            1 => Some(checksum),
            _ => return Err(ConsumerOffsetsWireError::InvalidPrepareChecksum),
        };
        let prepare_length = cursor.u32()? as usize;
        if prepare_length > journal::partition_journal::PREPARE_BYTES_MAX {
            return Err(ConsumerOffsetsWireError::InvalidPrepareChecksum);
        }
        let checkpoint_prepare = cursor.take(prepare_length)?.to_vec();
        if !cursor.remaining().is_empty() {
            // Distinct from `Truncated`: extra bytes point at a NEWER
            // encoder, and telling the operator the artifact is short would
            // send them the wrong way.
            return Err(ConsumerOffsetsWireError::TrailingBytes {
                extra: cursor.remaining().len(),
            });
        }
        Ok(Self {
            purge_generation,
            prepare_checksum,
            checkpoint_prepare,
            next_offset,
            consumers,
            groups,
            dedup,
        })
    }

    /// Same guards as [`Self::decode_section`] at the dedup stride: peer count
    /// against the ceiling, then against the bytes actually present, then
    /// ascending-strict client order so the encoding stays canonical. Client
    /// zero is the reserved id no ingress admits, so an artifact carrying it is
    /// a peer bug and fails closed rather than being silently dropped at
    /// install.
    fn decode_dedup_section(
        cursor: &mut LeCursor<'_>,
        count: u32,
    ) -> Result<Vec<DedupWatermark>, ConsumerOffsetsWireError> {
        if count > CONSUMER_OFFSETS_ENTRIES_MAX {
            return Err(ConsumerOffsetsWireError::TooManyEntries {
                section: "dedup",
                count,
                max: CONSUMER_OFFSETS_ENTRIES_MAX,
            });
        }
        if count as usize * DEDUP_ENTRY_LEN > cursor.remaining().len() {
            return Err(ConsumerOffsetsWireError::Truncated);
        }
        let mut entries = Vec::with_capacity(count as usize);
        let mut previous: Option<u128> = None;
        for _ in 0..count {
            let client = cursor.u128()?;
            let watermark = cursor.u64()?;
            let latest_commit = cursor.u64()?;
            let user_id = cursor.u32()?;
            let committed_window = cursor.u128()?;
            if client == 0 {
                return Err(ConsumerOffsetsWireError::ReservedClient);
            }
            if previous.is_some_and(|previous| client <= previous) {
                return Err(ConsumerOffsetsWireError::NonAscendingClient { client });
            }
            previous = Some(client);
            entries.push(DedupWatermark {
                client,
                user_id,
                watermark,
                latest_commit,
                committed_window,
            });
        }
        Ok(entries)
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
        // The count is peer input and the reservation is 12 bytes per element
        // after alignment, so it is checked against the bytes actually present
        // before allocating: a ~30 byte artifact could otherwise ask for tens of
        // megabytes across the two sections. 12 is the wire stride below, and
        // the guard is a lower bound on purpose: what trails a section varies
        // (groups trails consumers, the dedup section trails groups and is
        // absent from a v1 artifact), so only "at least this many bytes
        // present" holds for both calls. Demanding that `12 * count` be all
        // that remains would reject valid input.
        if count as usize * (size_of::<u32>() + size_of::<u64>()) > cursor.remaining().len() {
            return Err(ConsumerOffsetsWireError::Truncated);
        }
        let mut entries = Vec::with_capacity(count as usize);
        let mut previous: Option<u32> = None;
        for _ in 0..count {
            let id = cursor.u32()?;
            let offset = cursor.u64()?;
            // Ascending-strict doubles as the duplicate reject and makes the
            // encoding canonical: one table, one byte sequence.
            if previous.is_some_and(|previous| id <= previous) {
                return Err(ConsumerOffsetsWireError::NonAscendingId { section, id });
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
    InvalidPrepareChecksum,
    MissingPrepareChecksum,
    Truncated,
    BadMagic,
    UnsupportedVersion {
        version: u8,
    },
    /// Bytes remained after this version's field set: a newer encoder.
    TrailingBytes {
        extra: usize,
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
    /// Ids in a section are not strictly ascending: a duplicate, or an
    /// out-of-order entry. Both are the same encoder bug and both break the
    /// canonical form the encoding promises.
    NonAscendingId {
        section: &'static str,
        id: u32,
    },
    /// Dedup clients are not strictly ascending. Same encoder bug as
    /// [`Self::NonAscendingId`], on the u128-keyed section.
    NonAscendingClient {
        client: u128,
    },
    /// A dedup entry carries client id zero, which is reserved and refused at
    /// every ingress: a peer encoder bug.
    ReservedClient,
}

impl From<Truncated> for ConsumerOffsetsWireError {
    fn from(_: Truncated) -> Self {
        Self::Truncated
    }
}

impl fmt::Display for ConsumerOffsetsWireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPrepareChecksum => write!(f, "invalid state-transfer prepare checksum"),
            Self::MissingPrepareChecksum => {
                write!(f, "durable state transfer requires a prepare checksum")
            }
            Self::Truncated => write!(f, "consumer-offsets artifact is truncated"),
            Self::BadMagic => write!(
                f,
                "consumer-offsets artifact must use {} version {CONSUMER_OFFSETS_VERSION}",
                String::from_utf8_lossy(&CONSUMER_OFFSETS_MAGIC)
            ),
            Self::TrailingBytes { extra } => write!(
                f,
                "consumer-offsets artifact carries {extra} trailing bytes past this \
                 version's field set (a newer encoder?)"
            ),
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
            Self::NonAscendingId { section, id } => write!(
                f,
                "consumer-offsets artifact {section} id {id} does not ascend \
                 (duplicate, or out of order)"
            ),
            Self::NonAscendingClient { client } => write!(
                f,
                "consumer-offsets artifact dedup client {client} does not ascend \
                 (duplicate, or out of order)"
            ),
            Self::ReservedClient => {
                write!(
                    f,
                    "consumer-offsets artifact dedup entry carries reserved client 0"
                )
            }
        }
    }
}

impl std::error::Error for ConsumerOffsetsWireError {}

const fn validate_consumer_offset_transfer_count(
    kind: ConsumerKind,
    count: usize,
    max: usize,
) -> Result<(), PartitionTransferUnavailable> {
    if count <= max {
        return Ok(());
    }
    Err(PartitionTransferUnavailable::ConsumerOffsetsTooLarge { kind, count, max })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[compio::test]
    async fn given_transient_offset_io_failure_when_retried_should_succeed_without_exhausting_budget()
     {
        let attempts = std::cell::Cell::new(0);
        let result = retry_offset_mutation(|| {
            attempts.set(attempts.get() + 1);
            std::future::ready(if attempts.get() == 1 { Err(()) } else { Ok(7) })
        })
        .await;
        assert_eq!(result, Ok(7));
        assert_eq!(attempts.get(), 2);
    }

    #[compio::test]
    async fn given_persistent_offset_io_failure_when_retried_should_stop_at_attempt_limit() {
        let attempts = std::cell::Cell::new(0);
        let result = retry_offset_mutation(|| {
            attempts.set(attempts.get() + 1);
            std::future::ready(Err::<(), _>(7))
        })
        .await;
        assert_eq!(result, Err(7));
        assert_eq!(attempts.get(), OFFSET_IO_ATTEMPTS);
    }

    fn table() -> ConsumerOffsetsWire {
        ConsumerOffsetsWire {
            prepare_checksum: None,
            checkpoint_prepare: Vec::new(),
            purge_generation: 3,
            next_offset: 43,
            consumers: vec![(1, 10), (7, 42)],
            groups: vec![(2, 5)],
            dedup: vec![
                dedup_entry(11, 4, 90),
                dedup_entry(usize::MAX as u128 + 5, 9, 91),
            ],
        }
    }

    fn dedup_entry(client: u128, watermark: u64, latest_commit: u64) -> DedupWatermark {
        DedupWatermark {
            client,
            user_id: 1,
            watermark,
            latest_commit,
            committed_window: 0b1011,
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
    fn transferred_prepare_checksum_is_covered_by_the_artifact() {
        let mut table = table();
        table.prepare_checksum = Some(u128::MAX - 7);
        let bytes = table.encode();
        assert_eq!(
            ConsumerOffsetsWire::decode(&bytes)
                .unwrap()
                .prepare_checksum,
            table.prepare_checksum
        );
        let mut corrupt = bytes;
        let position = corrupt.len() - 9;
        corrupt[position] ^= 1;
        assert!(matches!(
            ConsumerOffsetsWire::decode(&corrupt),
            Err(ConsumerOffsetsWireError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn given_empty_table_when_encoded_should_round_trip() {
        let empty = ConsumerOffsetsWire {
            prepare_checksum: None,
            checkpoint_prepare: Vec::new(),
            purge_generation: 0,
            next_offset: 0,
            consumers: Vec::new(),
            groups: Vec::new(),
            dedup: Vec::new(),
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
    fn given_unordered_dedup_clients_when_decoded_should_reject() {
        let unordered = ConsumerOffsetsWire {
            prepare_checksum: None,
            checkpoint_prepare: Vec::new(),
            purge_generation: 0,
            next_offset: 0,
            consumers: Vec::new(),
            groups: Vec::new(),
            dedup: vec![dedup_entry(9, 1, 1), dedup_entry(4, 2, 2)],
        };
        assert_eq!(
            ConsumerOffsetsWire::decode(&unordered.encode()),
            Err(ConsumerOffsetsWireError::NonAscendingClient { client: 4 })
        );
    }

    #[test]
    fn given_reserved_client_in_dedup_when_decoded_should_reject() {
        let reserved = ConsumerOffsetsWire {
            prepare_checksum: None,
            checkpoint_prepare: Vec::new(),
            purge_generation: 0,
            next_offset: 0,
            consumers: Vec::new(),
            groups: Vec::new(),
            dedup: vec![dedup_entry(0, 1, 1), dedup_entry(4, 2, 2)],
        };
        assert_eq!(
            ConsumerOffsetsWire::decode(&reserved.encode()),
            Err(ConsumerOffsetsWireError::ReservedClient)
        );
    }

    #[test]
    fn given_unknown_artifact_formats_when_decoded_should_reject() {
        for magic in [b"BAD1", b"ICO9"] {
            let mut bytes = table().encode();
            bytes[..4].copy_from_slice(magic);
            let length = bytes.len() - 8;
            let checksum = state_artifact_checksum(&bytes[..length]);
            bytes[length..].copy_from_slice(&checksum.to_le_bytes());
            assert_eq!(
                ConsumerOffsetsWire::decode(&bytes),
                Err(ConsumerOffsetsWireError::BadMagic)
            );
        }
    }

    #[test]
    fn given_dedup_count_past_ceiling_when_decoded_should_reject_before_allocating() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&CONSUMER_OFFSETS_MAGIC);
        bytes.push(CONSUMER_OFFSETS_VERSION);
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&(CONSUMER_OFFSETS_ENTRIES_MAX + 1).to_le_bytes());
        let trailer = state_artifact_checksum(&bytes);
        bytes.extend_from_slice(&trailer.to_le_bytes());
        assert_eq!(
            ConsumerOffsetsWire::decode(&bytes),
            Err(ConsumerOffsetsWireError::TooManyEntries {
                section: "dedup",
                count: CONSUMER_OFFSETS_ENTRIES_MAX + 1,
                max: CONSUMER_OFFSETS_ENTRIES_MAX,
            })
        );
    }

    #[test]
    fn given_dedup_count_exceeding_bytes_when_decoded_should_reject_as_truncated() {
        // Under the ceiling but past the bytes present: the stride guard is
        // what stops a ~30 byte artifact reserving megabytes.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&CONSUMER_OFFSETS_MAGIC);
        bytes.push(CONSUMER_OFFSETS_VERSION);
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&1_000u32.to_le_bytes());
        let trailer = state_artifact_checksum(&bytes);
        bytes.extend_from_slice(&trailer.to_le_bytes());
        assert_eq!(
            ConsumerOffsetsWire::decode(&bytes),
            Err(ConsumerOffsetsWireError::Truncated)
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
            prepare_checksum: None,
            checkpoint_prepare: Vec::new(),
            purge_generation: 0,
            next_offset: 0,
            consumers: vec![(5, 1), (5, 2)],
            groups: Vec::new(),
            dedup: Vec::new(),
        };
        assert_eq!(
            ConsumerOffsetsWire::decode(&duplicate.encode()),
            Err(ConsumerOffsetsWireError::NonAscendingId {
                section: "consumers",
                id: 5,
            })
        );
        let unordered = ConsumerOffsetsWire {
            prepare_checksum: None,
            checkpoint_prepare: Vec::new(),
            purge_generation: 0,
            next_offset: 0,
            consumers: Vec::new(),
            groups: vec![(9, 1), (4, 2)],
            dedup: Vec::new(),
        };
        assert_eq!(
            ConsumerOffsetsWire::decode(&unordered.encode()),
            Err(ConsumerOffsetsWireError::NonAscendingId {
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
            Err(ConsumerOffsetsWireError::TrailingBytes { extra: 1 }),
            "bytes past the last section must fail closed"
        );
    }

    #[test]
    fn given_offset_count_above_transfer_ceiling_when_validated_should_reject() {
        assert!(validate_consumer_offset_transfer_count(ConsumerKind::Consumer, 4, 4).is_ok());
        assert!(matches!(
            validate_consumer_offset_transfer_count(ConsumerKind::ConsumerGroup, 5, 4),
            Err(PartitionTransferUnavailable::ConsumerOffsetsTooLarge {
                kind: ConsumerKind::ConsumerGroup,
                count: 5,
                max: 4,
            })
        ));
        let error = validate_consumer_offset_transfer_count(ConsumerKind::Consumer, 5, 4)
            .expect_err("count above ceiling");
        assert!(
            !error.transient(),
            "an artifact that cannot fit the decoder will not heal by retrying"
        );
    }
}

/// What a full validation walk over one segment payload derived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentWalkStats {
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
pub(crate) async fn walk_segment_payload(
    base_offset: u64,
    bytes: &[u8],
) -> Result<(SegmentWalkStats, Vec<u8>), SegmentWalkError> {
    let mut position = 0usize;
    let mut next_offset = base_offset;
    let mut stats: Option<SegmentWalkStats> = None;
    let mut index_bytes = Vec::new();
    let mut indexed_position: Option<usize> = None;
    let mut since_yield = 0usize;
    while position < bytes.len() {
        // The walk re-hashes every message (`decode_batch_slice` verifies
        // `batch_checksum`), so a multi-GiB artifact is a long CPU pass on the
        // pump task. What these yields buy is NOT tick liveness: the consensus
        // tick is a sibling `select_biased!` arm of this same task and arms are
        // not polled while another arm's body awaits, so every group's tick and
        // heartbeat on this shard stay frozen for the duration either way (see
        // the tick-starvation TODO in `shard::router`). They buy the reactor:
        // detached tasks and io_uring completions make progress instead of
        // waiting out the whole pass. Moving the verify + walk off the pump is
        // what would fix the tick, and the nonce re-check after the spill is
        // already shaped for that.
        if since_yield >= OFFER_HASH_CHUNK_LEN {
            since_yield = 0;
            yield_to_reactor().await;
        }
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
        // STRIDED, not one entry per batch: the origin writes one entry per
        // flush chunk, and a per-batch index is dense enough that a transferred
        // segment never fits the sealed-index residency cap
        // (`poll_plan::SEALED_INDEX_RESIDENT_MAX_BYTES`), so every sealed poll
        // would fall back to binary-searching the file with single-entry preads
        // -- a slow path `poll_plan` reserves for a
        // `messages_required_to_save = 1` misconfiguration, which a transfer
        // would otherwise produce unconditionally. Both consumers do lower-bound
        // lookups and recovery walks forward from the last entry by design, so
        // sparser is correct; the first batch always gets one.
        let stride_reached = indexed_position
            .is_none_or(|indexed| position.saturating_sub(indexed) >= INDEX_STRIDE_BYTES);
        if stride_reached {
            indexed_position = Some(position);
            index_bytes.extend_from_slice(&header.base_offset.to_le_bytes());
            index_bytes.extend_from_slice(&timestamp.to_le_bytes());
            index_bytes.extend_from_slice(&(position as u64).to_le_bytes());
        }
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
        since_yield += header.total_size();
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
/// segment payloads addressed by path and the offsets artifact resident.
///
/// The offsets artifact can include a full checkpoint prepare.
#[derive(Debug)]
pub struct PartitionStateTransferOffer {
    /// `== commit_min == commit_max` at build (caught-up primary gate).
    pub commit_op: u64,
    /// Ascending base offset; one artifact per non-empty retained segment.
    pub segments: Vec<SegmentArtifactSource>,
    /// Resident consumer offsets, dedup state, and an optional checkpoint prepare.
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
    MissingPrepareChecksum {
        op: u64,
    },
    ConsumerOffsetsTooLarge {
        kind: ConsumerKind,
        count: usize,
        max: usize,
    },
    ConsumerOffsetStateInconsistent {
        kind: ConsumerKind,
        consumer_id: u32,
    },
    /// Primary-by-index of a group that has committed nothing: an empty group
    /// is trivially "caught up", so this is the only thing separating a real
    /// primary from a view-0 phantom whose directory vanished.
    NothingCommitted,
    /// More retained segments than the manifest can carry entries for.
    ManifestTooLarge {
        entries: usize,
        max: usize,
    },
    /// The segment chain changed while the offer's checksum passes ran, so the
    /// stamps no longer describe the bytes the offer addresses.
    SegmentSetChanged,
    /// This round's share of the checksum pass ran out with retained bytes
    /// still unhashed. Progress is memoized, so the next request resumes where
    /// this one stopped rather than starting the pass again.
    OfferBuildInProgress {
        /// Bytes hashed so far over the chain AS THIS ROUND SEES IT. Carries
        /// across rounds through the memo rather than resetting per round, but
        /// retention GC dropping an already-hashed segment lowers it and
        /// `remaining` together, so it tracks the live chain, not a monotone
        /// total.
        hashed: u64,
        /// Bytes of it still unhashed. The pair is the only signal that
        /// separates a converging multi-round build from a stalled one.
        remaining: u64,
    },
    FlushPending,
    FlushFailed(iggy_common::IggyError),
    SegmentUnreadable {
        start_offset: u64,
        source: std::io::Error,
    },
}

impl PartitionTransferUnavailable {
    /// Whether the refusal says "not right now" rather than "this node is
    /// broken". A requester charges its consecutive-failure count (and the
    /// exponential re-arm backoff behind it) only for the latter: a primary
    /// that is momentarily behind its own frontier is the common case under
    /// produce load, and charging it pins the backoff at its ceiling while
    /// nothing else recovers the partition.
    #[must_use]
    pub const fn transient(&self) -> bool {
        match self {
            Self::NotCaughtUpPrimary
            | Self::RepairInProgress
            | Self::NothingCommitted
            | Self::SegmentSetChanged
            | Self::FlushPending
            | Self::OfferBuildInProgress { .. } => true,
            Self::NoPartitionDir
            | Self::MissingPrepareChecksum { .. }
            | Self::ConsumerOffsetsTooLarge { .. }
            | Self::ConsumerOffsetStateInconsistent { .. }
            | Self::ManifestTooLarge { .. }
            | Self::FlushFailed(_)
            | Self::SegmentUnreadable { .. } => false,
        }
    }
}

impl fmt::Display for PartitionTransferUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotCaughtUpPrimary => write!(f, "not the caught-up primary of this group"),
            Self::MissingPrepareChecksum { op } => {
                write!(f, "partition has no checksum for committed op {op}")
            }
            Self::NoPartitionDir => write!(f, "partition has no on-disk directory"),
            Self::RepairInProgress => write!(f, "partition is itself mid-repair"),
            Self::ConsumerOffsetsTooLarge { kind, count, max } => write!(
                f,
                "partition has {count} {kind:?} offset entries, past the {max} transfer ceiling"
            ),
            Self::ConsumerOffsetStateInconsistent { kind, consumer_id } => write!(
                f,
                "durable {kind:?} offset {consumer_id} is missing from the live map"
            ),
            Self::NothingCommitted => write!(
                f,
                "primary by index at view 0 with nothing committed; refusing to serve an empty offer"
            ),
            Self::ManifestTooLarge { entries, max } => write!(
                f,
                "offer needs {entries} manifest entries, past the {max} ceiling"
            ),
            Self::SegmentSetChanged => {
                write!(f, "segment chain changed while the offer was being built")
            }
            Self::OfferBuildInProgress { hashed, remaining } => write!(
                f,
                "offer checksum pass has hashed {hashed} bytes with {remaining} to go; \
                 resuming on the next request"
            ),
            Self::FlushPending => write!(f, "committed partition flush is pending"),
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

/// Outcome of a completed install. Offset files must land before the installed
/// commit floor advances. A failed purge-generation record remains retryable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionInstallOutcome {
    /// The consensus op the install applied. Named for what it holds: every
    /// other `frontier` in this module is a MESSAGE OFFSET
    /// (`VsrState::offset_frontier`, `StateArtifact::frontier`,
    /// `installed_frontier`), and op-vs-offset confusion is what produced this
    /// PR's durability defects.
    pub applied_commit_op: u64,
    /// The offered purge generation was already recorded or persisted during
    /// this install. False means a restart may repeat the purge and transfer.
    pub purge_generation_recorded: bool,
}

/// Failure installing a transferred partition state.
///
/// Validation failures mutate nothing. Pre-swap offset failures can leave
/// ignored replacement siblings when best-effort cleanup also fails, but never
/// alter live files.
#[derive(Debug)]
pub enum PartitionInstallError {
    NoPartitionDir,
    NoOffsetDir {
        kind: ConsumerKind,
    },
    /// `commit_op` fell below this replica's commit frontier; installing
    /// would rewind `commit_min` (the anti-rewind assert, as a refusal).
    StaleTransfer {
        commit_op: u64,
        commit_min: u64,
    },
    /// The incoming frontier could not be made durable before the swap, so the
    /// install refuses rather than enter a window whose only durable witness
    /// would be the segments the failure path quarantines away.
    FrontierNotDurable {
        frontier: u64,
    },
    /// The offer's offset frontier is below this replica's own offset
    /// counter, so installing it would rewind the offset space: the next
    /// replicated prepare would be re-stamped from the rewound counter and
    /// persist different bytes (and a different `batch_checksum`) here than
    /// on the rest of the group.
    OfferRewindsDurableData {
        offer_next_offset: u64,
        local_next_offset: u64,
    },
    /// Consumer-offset staging failed before the segment swap, or finalizing a
    /// staged offset failed during it.
    OffsetPersistence {
        path: String,
        source: iggy_common::IggyError,
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
        /// The offer's frontier, carried because the LIVE counter is not it on
        /// this path: a mutate failure can leave the counter at its pre-install
        /// value, and under an advancing purge generation that value is above
        /// the group's. The fence records this instead, or it would stamp the
        /// stale counter over the reset the install already made and then
        /// quarantine the segments that would have contradicted it.
        frontier: u64,
    },
}

impl fmt::Display for PartitionInstallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoPartitionDir => write!(f, "partition has no on-disk directory"),
            Self::NoOffsetDir { kind } => {
                write!(f, "partition has no {kind:?} offset directory configured")
            }
            Self::StaleTransfer {
                commit_op,
                commit_min,
            } => write!(
                f,
                "transfer frontier {commit_op} is below the local commit frontier {commit_min}"
            ),
            Self::FrontierNotDurable { frontier } => write!(
                f,
                "could not record the incoming offset frontier {frontier} before the swap"
            ),
            Self::OfferRewindsDurableData {
                offer_next_offset,
                local_next_offset,
            } => write!(
                f,
                "offer frontier {offer_next_offset} is below this replica's own next offset \
                 {local_next_offset}; installing it would rewind the offset space"
            ),
            Self::OffsetPersistence { path, source } => {
                write!(f, "consumer offset persistence failed at {path}: {source}")
            }
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
            Self::ConvergeFailed { source, frontier } => write!(
                f,
                "post-failure convergence failed at frontier {frontier}, \
                 the partition must be fenced: {source}"
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

/// Suffix marking a half-transferred file inside the partition directory.
///
/// Provably invisible to boot recovery, which filters on `extension == "log"`,
/// and swept wholesale at boot by
/// `segment_recovery::sweep_scratch_files_and_collect_offsets`.
pub const STAGING_SUFFIX: &str = ".staging";

/// Staging-file names inside the partition directory.
fn staging_paths(partition_dir: &str, start_offset: u64) -> (PathBuf, PathBuf) {
    (
        PathBuf::from(format!(
            "{partition_dir}/{start_offset:0>20}.log{STAGING_SUFFIX}"
        )),
        PathBuf::from(format!(
            "{partition_dir}/{start_offset:0>20}.index{STAGING_SUFFIX}"
        )),
    )
}

/// Every entry of one partition directory, as paths.
///
/// BLOCKING `read_dir` on the pump: compio-fs 0.12 exposes no async directory
/// walk, and `spawn_blocking` is not an escape either -- the shard executors run
/// `thread_pool_limit(0)`. Bounded by the entry count of ONE partition directory,
/// but it is a real stall (and under the write lock at the converge site), so it
/// stays recorded rather than hidden.
///
/// Enumeration only: callers keep their own predicates and their own
/// error policies (propagate / silent skip / log-and-fail), which is what
/// `sweep_staging_except`'s do-not-widen warning depends on.
fn segment_dir_entries(partition_dir: &str) -> std::io::Result<impl Iterator<Item = PathBuf>> {
    Ok(std::fs::read_dir(partition_dir)?
        .flatten()
        .map(|entry| entry.path()))
}

/// Remove physical tails outside the logical segment list after draining the WAL.
/// The owner has settled its writers and published a purge or install backup.
pub(crate) async fn remove_public_segment_files(partition_dir: &str) -> std::io::Result<()> {
    let directory = Path::new(partition_dir);
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.path();
        if !entry.file_type()?.is_dir()
            && name
                .extension()
                .is_some_and(|extension| extension == "log" || extension == "index")
            && name
                .file_stem()
                .and_then(|stem| stem.to_str())
                .is_some_and(|stem| stem.parse::<u64>().is_ok())
        {
            match DiskStorage.remove_file(&name).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
    }
    Ok(())
}

const MATERIALIZATION_MISSING: &str = "materialization.missing";

/// Fence replacement files before quarantining the authoritative materialization.
///
/// # Errors
/// Returns an error if the recovery fence cannot be published durably.
pub async fn mark_materialization_missing(directory: &str, revision: u64) -> std::io::Result<()> {
    let path = Path::new(directory).join(MATERIALIZATION_MISSING);
    let temporary = Path::new(directory).join("materialization.missing.tmp");
    let mut file = compio::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .await?;
    file.write_all_at(revision.to_le_bytes().to_vec(), 0)
        .await
        .0?;
    file.sync_all().await?;
    compio::fs::rename(temporary, path).await?;
    fsync_dir(directory).await
}

/// # Errors
/// Returns an error if the recovery fence cannot be read or validated.
pub async fn materialization_is_missing(directory: &str, revision: u64) -> std::io::Result<bool> {
    match compio::fs::read(Path::new(directory).join(MATERIALIZATION_MISSING)).await {
        Ok(bytes) => {
            let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid materialization fence",
                )
            })?;
            Ok(u64::from_le_bytes(bytes) == revision)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

pub(crate) async fn clear_materialization_missing(directory: &str) -> std::io::Result<()> {
    match compio::fs::remove_file(Path::new(directory).join(MATERIALIZATION_MISSING)).await {
        Ok(()) => fsync_dir(directory).await,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Move every segment file in `partition_dir` aside into `<dir>.fenced.<n>/`,
/// returning the directory used.
///
/// Boot recovery also supplies `wal_revision` to
/// move the refused prepare WAL. Live callers leave it unset to retain open writers.
///
/// The partition directory itself STAYS, and so do its two superblock slots:
/// they hold the group's only durable `(view, log_view)`, and moving them would
/// make the rebuild read an empty directory -- no `restore_partition_view`,
/// `consensus.init()` instead of `init_as_backup()`, no replica-identity guard --
/// so the group would re-enter view 0 after acting in view N and could answer a
/// retransmitted DVC with `(0, 0)`, letting a quorum adopt a log shorter than the
/// committed prefix.
///
/// Nothing reclaims the fenced copies: they are evidence for an operator,
/// bounded to 1000 per partition by the suffix search, and never read again
/// (recovery keys on `.log` files inside the partition directory, and the fenced
/// subdirectory is not one).
///
/// # Errors
/// The underlying `std::io::Error`. A failure is NOT recoverable by rebuilding:
/// the rebuild plants segment 0 with `file_exists = false` and truncates
/// whatever the failed quarantine left, so callers tombstone the partition and
/// leave the bytes for an operator.
pub async fn quarantine_partition_files(
    partition_dir: &str,
    wal_revision: Option<u64>,
) -> std::io::Result<String> {
    // `create_dir`, not stat-then-create: one syscall per attempt instead of
    // two, and race-free. Deliberately NOT `create_dir_all`, which succeeds on
    // an existing directory and would silently merge this fence into an earlier
    // copy.
    let mut target = None;
    for attempt in 0..1000 {
        let candidate = format!("{partition_dir}.fenced.{attempt}");
        match compio::fs::create_dir(&candidate).await {
            Ok(()) => {
                target = Some(candidate);
                break;
            }
            // Lost the race for this suffix; the next iteration probes the
            // next one.
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    let Some(target) = target else {
        return Err(std::io::Error::other(
            "a thousand fenced copies of this partition already exist",
        ));
    };
    for path in segment_dir_entries(partition_dir)? {
        let quarantined = path.to_str().is_some_and(|path| {
            [".log", ".index", STAGING_SUFFIX, ANCHOR_SUFFIX]
                .iter()
                .any(|suffix| path.ends_with(suffix))
        });
        if !quarantined {
            continue;
        }
        let Some(name) = path.file_name() else {
            continue;
        };
        compio::fs::rename(&path, &PathBuf::from(&target).join(name)).await?;
    }
    if let Some(revision) = wal_revision {
        let name = format!("prepares-{revision}");
        match compio::fs::rename(
            &Path::new(partition_dir).join(&name),
            &Path::new(&target).join(&name),
        )
        .await
        {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    // All three touched directories: the target (its new dirents), the source
    // (the removals), and the source's parent (the target directory itself is a
    // new dirent there). Without the target-side syncs a crash can leave the
    // moved files linked in neither directory -- only forensics are at stake,
    // but forensics are the whole point of the copies.
    fsync_dir(&target).await?;
    fsync_dir(partition_dir).await?;
    if let Some(parent) = Path::new(partition_dir).parent().and_then(Path::to_str) {
        fsync_dir(parent).await?;
    }
    Ok(target)
}

/// Unlink every staging file in `partition_dir` except `keep`.
///
/// Best-effort disk hygiene shared by the reuse scan and the install: a file
/// that survives is swept at the next boot, so a failed unlink is not worth
/// failing either caller for. The CONVERGE sweep is deliberately not this
/// function -- it deletes the live chain as well and must propagate its
/// errors.
/// Do NOT widen this predicate to the quarantine's four-suffix list if the two
/// are ever unified: the keep-lists callers pass hold staging paths only (purge
/// passes none), so a wider filter would unlink every live `.log` and `.index`
/// on the partition -- worst at the reuse scan, which runs at descriptor-accept
/// on a serving partition.
pub(crate) async fn sweep_staging_except(partition_dir: &str, keep: &HashSet<&Path>) {
    let Ok(entries) = segment_dir_entries(partition_dir) else {
        return;
    };
    for path in entries {
        let is_staging = path
            .to_str()
            .is_some_and(|path| path.ends_with(STAGING_SUFFIX));
        if is_staging && !keep.contains(path.as_path()) {
            let _ = compio::fs::remove_file(&path).await;
        }
    }
}

fn final_paths(partition_dir: &str, start_offset: u64) -> (String, String) {
    (
        format!("{partition_dir}/{start_offset:0>20}.log"),
        format!("{partition_dir}/{start_offset:0>20}.index"),
    )
}

/// Consumer-offset files written concurrently while installing a transfer.
/// Each is an open + write + optional fsync, so the width trades reactor queue
/// depth against how long one partition monopolises it; matches the tick's
/// superblock pre-pass.
pub(crate) const OFFSET_PERSIST_CONCURRENCY: usize = 16;
const OFFSET_IO_ATTEMPTS: usize = 3;
/// First retry delay of [`retry_offset_mutation`]; each further retry doubles it.
const OFFSET_IO_BACKOFF_BASE: std::time::Duration = std::time::Duration::from_millis(10);

pub(crate) async fn retry_offset_mutation<T, E: fmt::Debug, F: Future<Output = Result<T, E>>>(
    mut operation: impl FnMut() -> F,
) -> Result<T, E> {
    for attempt in 1..OFFSET_IO_ATTEMPTS {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                tracing::debug!(
                    attempt,
                    ?error,
                    "offset mutation failed, retrying after backoff"
                );
                compio::time::sleep(OFFSET_IO_BACKOFF_BASE * (1 << (attempt - 1))).await;
            }
        }
    }
    operation().await.inspect_err(|error| {
        tracing::debug!(
            attempt = OFFSET_IO_ATTEMPTS,
            ?error,
            "offset mutation failed on the last attempt"
        );
    })
}

/// One consumer-offset file the install is about to write. Collected before any
/// write is issued so the offset maps and the persisted-offset tracker are never
/// borrowed across a batch's await.
#[derive(Clone)]
pub struct PlannedOffsetWrite {
    pub(crate) kind: ConsumerKind,
    pub(crate) id: u32,
    pub(crate) path: String,
    pub(crate) value: u64,
}

pub(crate) struct PendingInstall {
    commit_op: u64,
    peer: u8,
    partition_dir: String,
    staged: Vec<StagedSegmentMeta>,
    offsets_wire: ConsumerOffsetsWire,
    planned_offsets: Vec<PlannedOffsetWrite>,
    next_offset: u64,
    purge_advances: bool,
    phase: InstallPhase,
    drain: Option<crate::PersistenceDrain>,
    offset_files: Option<std::fs::ReadDir>,
    directory_handle: Option<compio::fs::File>,
    offset_dirs_changed: [bool; 2],
    failure: Option<PartitionInstallError>,
    disposition: InstallDisposition,
    purge_generation_recorded: bool,
}

enum InstallDisposition {
    Prepared,
    Backup,
    Mutating,
}

enum InstallPhase {
    StageOffsets(usize),
    Drain,
    Backup,
    Frontier,
    RemoveSegments,
    Sweep,
    RenameIndexes(usize),
    IndexDirectory,
    RenameLogs(usize),
    OpenSegments(usize),
    EmptyDirectory,
    OldOffsets(usize),
    DeleteOffset { kind: usize, id: u32, path: String },
    CommitOffsets(usize),
    OffsetDirectory(usize),
    Publish,
    PurgeGeneration,
    WalReset,
    ResetDraining,
    WalCertify,
    CertifyDraining,
    FinalFrontier,
    FinishBackup,
    ClearMissing,
    DiscardOffsets(usize),
    Converge,
    Submitted(InstallFilePhase),
    Done,
}

enum InstallFilePhase {
    StageOffsets(usize),
    Backup,
    RemoveSegment,
    Sweep,
    RenameIndex(usize),
    IndexDirectory,
    RenameLog(usize),
    OpenSegment(usize),
    EmptyDirectory,
    DeleteOffset { kind: usize, id: u32 },
    CommitOffset(usize),
    OffsetDirectory(usize),
    PurgeGeneration,
    FinishBackup,
    ClearMissing,
    DiscardOffsets(usize),
    Converge,
}

pub(crate) const fn consumer_kind_index(kind: ConsumerKind) -> usize {
    match kind {
        ConsumerKind::Consumer => 0,
        ConsumerKind::ConsumerGroup => 1,
    }
}

pub(crate) async fn stage_offset_writes(
    planned: &[PlannedOffsetWrite],
) -> Result<(), PartitionInstallError> {
    for batch in planned.chunks(OFFSET_PERSIST_CONCURRENCY) {
        let writes = batch.iter().map(|write| async move {
            (
                &write.path,
                retry_offset_mutation(|| stage_offset_replacement(&write.path, write.value)).await,
            )
        });
        for (path, result) in futures::future::join_all(writes).await {
            if let Err(source) = result {
                discard_offset_writes(planned).await;
                return Err(PartitionInstallError::OffsetPersistence {
                    path: path.clone(),
                    source,
                });
            }
        }
    }
    Ok(())
}

pub(crate) async fn discard_offset_writes(planned: &[PlannedOffsetWrite]) {
    for write in planned {
        discard_offset_replacement(&write.path).await;
    }
}

/// fsync the partition directory so a rename made durable stays durable.
/// Async so the wait parks the task instead of the whole shard reactor;
/// every other future on the pump keeps running through it.
pub(crate) async fn fsync_dir(partition_dir: &str) -> std::io::Result<()> {
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
    fn plan_transfer_offset_writes(
        &self,
        offsets_wire: &ConsumerOffsetsWire,
        next_offset: u64,
    ) -> Result<Vec<PlannedOffsetWrite>, PartitionInstallError> {
        let consumer_dir =
            self.consumer_offsets_path
                .as_deref()
                .ok_or(PartitionInstallError::NoOffsetDir {
                    kind: ConsumerKind::Consumer,
                })?;
        let group_dir = self.consumer_group_offsets_path.as_deref().ok_or(
            PartitionInstallError::NoOffsetDir {
                kind: ConsumerKind::ConsumerGroup,
            },
        )?;
        let clamp = |offset: u64| next_offset.checked_sub(1).map(|last| offset.min(last));
        let mut planned =
            Vec::with_capacity(offsets_wire.consumers.len() + offsets_wire.groups.len());
        for (kind, dir, offsets) in [
            (
                ConsumerKind::Consumer,
                consumer_dir,
                &offsets_wire.consumers,
            ),
            (ConsumerKind::ConsumerGroup, group_dir, &offsets_wire.groups),
        ] {
            planned.extend(offsets.iter().filter_map(|(id, offset)| {
                clamp(*offset).map(|value| PlannedOffsetWrite {
                    kind,
                    id: *id,
                    path: format!("{dir}/{id}"),
                    value,
                })
            }));
        }
        Ok(planned)
    }

    /// Build (or serve from cache) this group's state-transfer offer.
    ///
    /// Force-flushes the committed prefix first so the segments cover every
    /// committed `SendMessages` op and the offset table covers every
    /// committed offset op; `commit_op = commit_min` then names the exact
    /// state the artifacts represent. Segment bytes are NOT loaded here: the
    /// offer records `(entry, path)` and the serving side loads one artifact
    /// at a time, so building costs one streaming checksum pass per segment
    /// and the resident footprint is the offsets artifact, including the
    /// checkpoint prepare.
    ///
    /// # Errors
    /// [`PartitionTransferUnavailable`]; the requester falls back to journal
    /// repair or retries after the next trigger.
    #[allow(clippy::too_many_lines)]
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
        // Primary-by-index at view 0 over an empty log passes every gate above
        // yet knows nothing: a group whose directory is absent boots through
        // `consensus.init()`, comes up Normal at view 0, and an empty group is
        // trivially "caught up". Its offer would be zero segments at frontier
        // 0, which makes a receiver holding real data unlink its own chain.
        //
        // A RESTARTED replica holding a full chain matches this shape too
        // (`commit_max == 0` because the partition journal is memory-only,
        // `installed_frontier == None` for a recovered non-empty chain). That
        // is the load-bearing reason this refusal is safe rather than a
        // wedge: every transfer-arm site presupposes a peer that already
        // reported commit > 0 (repair floor refusals, StartView adoption), so
        // nobody ever asks a cluster where everything still reports 0.
        // Extending the gate with `recovered_durable_offset.is_some()` would
        // be WRONG: such an offer carries `commit_op = 0`, so the receiver's
        // floor becomes a no-op while its counter jumps to the frontier.
        if self.consensus().commit_max() == 0 && self.installed_frontier.is_none() {
            return Err(PartitionTransferUnavailable::NothingCommitted);
        }
        if !self
            .flush_committed_messages(config)
            .await
            .map_err(PartitionTransferUnavailable::FlushFailed)?
        {
            return Err(PartitionTransferUnavailable::FlushPending);
        }
        let commit_op = self.consensus().commit_min();
        if let Some(cached) = self.transfer_offer_cache.borrow().as_ref()
            && cached.commit_op == commit_op
        {
            // Returns BEFORE the chain re-validation below, deliberately:
            // re-validating on every hit is the walk the cache exists to skip.
            // Retention GC on an idle partition therefore costs one wasted
            // round -- the chunk serve fails `Stale` and the eviction path
            // re-enumerates -- which is the cheaper side of the trade.
            return Ok(Rc::clone(cached));
        }

        // Enumerate under the write lock so GC (`remove_sealed_segments_up_to`,
        // also write-locked) cannot unlink a file between enumeration and read.
        // The checksum passes below run with the lock RELEASED: they are the
        // expensive part, and the same mutex serializes
        // `append_send_messages_to_journal` and `commit_messages_inner`, so
        // holding it across a multi-GiB first pass stalls this partition's
        // produce and commit for the whole pass. The chain is re-validated
        // under the lock afterwards.
        let write_lock = self.write_lock.clone();
        // Sampled with the chain: a purge inside the hash window below both
        // truncates every file and restarts the offset space, so a size that
        // grew back past its planned length would pass the size re-check while
        // the stamps describe post-purge bytes at pre-purge offsets.
        let planned_purge_generation = self.applied_purge_generation;
        let planned: Vec<(u64, u64, String)> = {
            let _guard = write_lock.lock().await;
            let mut planned = Vec::with_capacity(self.log.segments().len());
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
                planned.push((segment.start_offset, size, log_path));
            }
            planned
        };
        // One manifest entry per planned segment plus the offsets table. The
        // manifest encoder ASSERTS its entry ceiling and that assert survives
        // release builds, so a partition retaining more segments than the
        // ceiling would panic this shard the moment a peer asked it to serve.
        // A small configured `segment_size` makes a chain that long ordinary,
        // so refuse the request instead of tripping the assert.
        let manifest_entries = planned.len() + 1;
        let manifest_entries_max = consensus::state_manifest::STATE_MANIFEST_ENTRIES_MAX as usize;
        if manifest_entries > manifest_entries_max {
            return Err(PartitionTransferUnavailable::ManifestTooLarge {
                entries: manifest_entries,
                max: manifest_entries_max,
            });
        }

        // The checksum pass is the expensive part and it runs inside ONE frame
        // body: the router's tick arm is not polled while another arm's body
        // awaits, and the yields inside `hash_segment_range` move the reactor,
        // not this shard's consensus ticks. A cold pass over multi-GiB
        // retention therefore silences every group on this core for its whole
        // duration, past `heartbeat_timeout`, on the node that by construction
        // is the caught-up primary of those groups.
        //
        // Bounded per round instead. The memo carries partial progress, so a
        // refusal here is not lost work: the requester re-asks on its flat
        // transient interval and each round advances the pass by the budget
        // until the offer completes.
        let mut budget = OFFER_HASH_BUDGET_PER_ROUND_BYTES;
        let mut segments = Vec::with_capacity(planned.len());
        for (start_offset, size, log_path) in &planned {
            let Some(checksum) = self
                .segment_checksum(*start_offset, *size, log_path, &mut budget)
                .await?
            else {
                // CUMULATIVE across rounds, read back off the memo: per-round
                // figures are constant by construction (a partial round always
                // spends exactly the budget and always stops inside one
                // segment), so they render identically on round 1 and round 30
                // and an operator cannot tell a converging pass from a wedged
                // one. This is the only window onto a multi-round build.
                let hashed = self.hashed_prefix_len(&planned);
                let total = planned.iter().map(|(_, size, _)| *size).sum::<u64>();
                // The completing round's sweep is skipped on this path, so
                // prune here too: retention GC can unlink segments across a
                // long build, and their memos would otherwise accumulate until
                // some round finally runs the loop to the end.
                self.retain_segment_checksum_memos(&planned);
                return Err(PartitionTransferUnavailable::OfferBuildInProgress {
                    hashed,
                    remaining: total.saturating_sub(hashed),
                });
            };
            segments.push(SegmentArtifactSource {
                entry: consensus::StateArtifact {
                    kind: artifact_kind::SEGMENT_LOG,
                    frontier: *start_offset,
                    len: *size,
                    checksum,
                },
                log_path: log_path.clone(),
            });
        }

        // Re-validate the chain under the lock: the passes above yielded, so GC
        // could have unlinked a sealed segment or a purge could have planted a
        // fresh file at a planned path. Every stamp would then describe bytes
        // the offer no longer addresses, so refuse and let the requester ask
        // again against the chain that exists now.
        {
            let _guard = write_lock.lock().await;
            let live: std::collections::HashMap<u64, u64> = self
                .log
                .segments()
                .iter()
                .map(|segment| (segment.start_offset, segment.size.as_bytes_u64()))
                .collect();
            // Append-only within a segment instance, so a live size BELOW the
            // planned one means the file was replaced rather than extended.
            let changed = planned_purge_generation != self.applied_purge_generation
                || planned.iter().any(|(start_offset, size, _)| {
                    live.get(start_offset)
                        .is_none_or(|live_size| live_size < size)
                });
            if changed {
                return Err(PartitionTransferUnavailable::SegmentSetChanged);
            }
            // Sweep memo entries whose segment left the chain (GC), so the map
            // tracks the live chain rather than growing with history.
            self.segment_checksum_cache
                .borrow_mut()
                .retain(|start_offset, _| live.contains_key(start_offset));
        }

        // An empty chain at frontier 0 tells the receiver to unlink its own, so
        // serve it only when a recorded purge says the emptiness is the truth.
        // `install_state_transfer`'s `purge_advances` check re-decides that
        // against the metadata plane and refuses the rest.
        let offsets_wire = self.offsets_wire_snapshot()?;
        if segments.is_empty()
            && offsets_wire.next_offset == 0
            && offsets_wire.purge_generation == 0
        {
            return Err(PartitionTransferUnavailable::NothingCommitted);
        }
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

    /// Bytes of `planned` the memo already covers, clamped per segment to the
    /// planned length so a memo carrying an active segment's later growth
    /// cannot report more than this offer will hash.
    fn hashed_prefix_len(&self, planned: &[(u64, u64, String)]) -> u64 {
        let memos = self.segment_checksum_cache.borrow();
        planned
            .iter()
            .map(|(start_offset, size, _)| {
                memos
                    .get(start_offset)
                    .map_or(0, |memo| memo.hashed_len.min(*size))
            })
            .sum()
    }

    /// Drop memo entries whose segment is no longer in `planned`, which is the
    /// live chain as of this round.
    ///
    /// Set-based rather than a scan per entry: this runs on every
    /// budget-exhausted round, inside the frame body the budget exists to
    /// bound, and `planned` is capped by `STATE_MANIFEST_ENTRIES_MAX` rather
    /// than by anything an operator sized, so the quadratic form dominates the
    /// hashing it was meant to make room for.
    fn retain_segment_checksum_memos(&self, planned: &[(u64, u64, String)]) {
        let live: HashSet<u64> = planned
            .iter()
            .map(|(start_offset, _, _)| *start_offset)
            .collect();
        self.segment_checksum_cache
            .borrow_mut()
            .retain(|start_offset, _| live.contains(start_offset));
    }

    /// The artifact stamp over the first `size` bytes of a segment file,
    /// extending the memoized hasher rather than re-reading what it already
    /// covered.
    ///
    /// Sealed segments hit the memo outright. The active one pays for its delta
    /// only, which is what keeps a committing primary off a full re-read of the
    /// retained history every round: `commit_op` advances per round, so the
    /// offer cache misses even when nothing else changed.
    ///
    /// `budget` caps the bytes this call may read, charged as it goes. `None`
    /// means the budget ran out first: the memo holds everything hashed so far
    /// and the next call resumes from it.
    ///
    /// # Errors
    /// [`PartitionTransferUnavailable::SegmentUnreadable`] when the file is
    /// unreadable or shorter than `size`.
    async fn segment_checksum(
        &self,
        start_offset: u64,
        size: u64,
        log_path: &str,
        budget: &mut u64,
    ) -> Result<Option<u64>, PartitionTransferUnavailable> {
        // Taken OUT of the map for the read: the hash awaits, and a half-fed
        // hasher left visible could be extended twice by a second build.
        let memo = self
            .segment_checksum_cache
            .borrow_mut()
            .remove(&start_offset);
        let mut memo = match memo {
            // `<=`, so the already-hashed case falls through to the shared tail:
            // `hash_segment_range` returns before opening the file when
            // `from == to`, and the finish + reinsert below is the same work the
            // separate arm did.
            Some(memo) if memo.hashed_len <= size => memo,
            // Segment bytes are append-only within one segment instance (the
            // failed-index-save path rewinds the writer cursor and returns
            // BEFORE the size increment), and every path that plants a fresh
            // file at an existing base offset clears the whole map, so a
            // shrunk size means the two have drifted.
            shrunk => {
                debug_assert!(
                    shrunk.is_none(),
                    "segment {start_offset} shrank to {size} bytes below its memo"
                );
                SegmentChecksumMemo::new()
            }
        };
        // Clamped to the round's remaining budget, so a single multi-GiB
        // segment is split across rounds rather than being the granularity
        // floor. `finish` does not consume the hasher, so a partial pass is
        // simply a memo nobody stamps yet.
        let target = size.min(memo.hashed_len.saturating_add(*budget));
        let hashed = target.saturating_sub(memo.hashed_len);
        // Dropped on failure, not reinserted: the hasher is fed chunk by chunk
        // and a mid-range error leaves it holding bytes `hashed_len` does not
        // account for, so resuming from it would stamp a checksum over a
        // doubly-fed prefix. Losing the partial pass is the cheap side.
        hash_segment_range(log_path, memo.hashed_len, target, &mut memo.hasher, None)
            .await
            .map_err(|source| PartitionTransferUnavailable::SegmentUnreadable {
                start_offset,
                source,
            })?;
        memo.hashed_len = target;
        let checksum = (target == size).then(|| memo.hasher.finish());
        self.segment_checksum_cache
            .borrow_mut()
            .insert(start_offset, memo);
        *budget = budget.saturating_sub(hashed);
        Ok(checksum)
    }

    /// Release the cached offer once no requester holds one (the shard's
    /// offer-expiry sweep).
    pub fn clear_state_transfer_offer_cache(&self) {
        self.transfer_offer_cache.borrow_mut().take();
    }

    fn validate_consumer_offset_transfer_counts(&self) -> Result<(), PartitionTransferUnavailable> {
        for kind in [ConsumerKind::Consumer, ConsumerKind::ConsumerGroup] {
            let count = self.durable_consumer_offsets.count(kind);
            if let Err(error) = validate_consumer_offset_transfer_count(
                kind,
                count,
                CONSUMER_OFFSETS_ENTRIES_MAX as usize,
            ) {
                tracing::error!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    namespace_raw = self.consensus().group(),
                    ?kind,
                    count,
                    max = CONSUMER_OFFSETS_ENTRIES_MAX,
                    "consumer offset state exceeds the transfer ceiling"
                );
                return Err(error);
            }
        }
        Ok(())
    }

    /// Snapshot committed durable offsets only. Eager auto-commit progress and
    /// follower-local cursor entries stay in the live maps until a replicated
    /// store commits them, so neither can be promoted by state transfer.
    fn offsets_wire_snapshot(&self) -> Result<ConsumerOffsetsWire, PartitionTransferUnavailable> {
        self.validate_consumer_offset_transfer_counts()?;
        let consumer_map = self.consumer_offsets.pin();
        let consumers = self.snapshot_offset_kind(ConsumerKind::Consumer, |id| {
            consumer_map.contains_key(&(id as usize))
        })?;
        let group_map = self.consumer_group_offsets.pin();
        let groups = self.snapshot_offset_kind(ConsumerKind::ConsumerGroup, |id| {
            group_map.contains_key(&ConsumerGroupId(id as usize))
        })?;
        // The append counter, not the segment end: retention can GC every
        // sealed segment while the counter stands at N, and the receiver
        // must resume minting at N either way.
        let next_offset = self.offset_frontier();
        let dedup = self.dedup().watermarks_sorted();
        let commit_op = self.consensus().commit_min();
        let prepare_checksum = if commit_op == 0 {
            Some(0)
        } else {
            self.log
                .journal()
                .inner
                .repair_header(commit_op)
                .map(|header| header.checksum)
                .or_else(|| {
                    self.persistence
                        .as_ref()
                        .and_then(|persistence| persistence.checksum(commit_op))
                })
                .or_else(|| {
                    (self.consensus().sequencer().current_sequence() == commit_op)
                        .then(|| self.consensus().last_prepare_checksum())
                })
        };
        let checkpoint_prepare = self
            .log
            .journal()
            .inner
            .repair_entry(commit_op)
            .map_or_else(Vec::new, |prepare| prepare.as_slice().to_vec());
        if self.persistence.is_some()
            && (prepare_checksum.is_none() || (commit_op > 0 && checkpoint_prepare.is_empty()))
        {
            return Err(PartitionTransferUnavailable::MissingPrepareChecksum { op: commit_op });
        }
        Ok(ConsumerOffsetsWire {
            checkpoint_prepare,
            prepare_checksum,
            purge_generation: self.applied_purge_generation,
            next_offset,
            consumers,
            groups,
            dedup,
        })
    }

    fn snapshot_offset_kind(
        &self,
        kind: ConsumerKind,
        map_contains: impl Fn(u32) -> bool,
    ) -> Result<Vec<(u32, u64)>, PartitionTransferUnavailable> {
        self.durable_consumer_offsets.with_entries(kind, |entries| {
            let mut snapshot = Vec::with_capacity(entries.len());
            for (&consumer_id, state) in entries {
                if !map_contains(consumer_id) {
                    return Err(
                        PartitionTransferUnavailable::ConsumerOffsetStateInconsistent {
                            kind,
                            consumer_id,
                        },
                    );
                }
                snapshot.push((consumer_id, state.committed_offset));
            }
            snapshot.sort_unstable_by_key(|(id, _)| *id);
            Ok(snapshot)
        })
    }

    #[cfg(test)]
    pub(crate) fn offsets_wire_snapshot_for_test(
        &self,
    ) -> Result<Vec<(u32, u64)>, PartitionTransferUnavailable> {
        self.offsets_wire_snapshot().map(|wire| wire.consumers)
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
        // This write replaces whatever the reuse memo recorded for this path,
        // so the memo cannot outlive it: a later scan against an older offer
        // must re-read the file rather than trust a walk of the old bytes.
        self.reuse_scan_memo.borrow_mut().take();
        // Artifact-level integrity FIRST, exactly as the metadata plane
        // verifies every artifact before decoding: the walk's per-batch
        // checksums prove batch bodies, not that these are the bytes the
        // manifest promised (length alone is implied by completion).
        if !verify_state_artifact_yielding(entry, &bytes).await {
            return Err(SpillError::ManifestChecksum {
                frontier: entry.frontier,
            });
        }
        let (stats, index_bytes) = walk_segment_payload(entry.frontier, &bytes)
            .await
            .map_err(SpillError::Walk)?;
        let (log_staging, index_staging) = staging_paths(&partition_dir, entry.frontier);
        let index_size = index_bytes.len() as u64;
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
        Ok(StagedSegmentMeta::from_walk(
            entry,
            stats,
            index_size,
            log_staging,
            index_staging,
        ))
    }

    /// Adopt an already-verified staged log without rewriting it: walk the
    /// payload once (validation + a rebuilt sparse index), write only the
    /// index sidecar, and return the walk metadata. The reuse scan calls
    /// this after `verify_state_artifact` proved the bytes match the
    /// manifest; rewriting the byte-identical log (and re-verifying a third
    /// time) is exactly the work reuse exists to skip. The index write
    /// stays: the scan never checks `.index.staging`, and a missing sidecar
    /// would hand the install a missing rename source.
    ///
    /// No directory fsync here: every sidecar lands in the same directory, so
    /// the caller fsyncs ONCE after its loop instead of once per adoption.
    async fn adopt_staged_segment(
        &self,
        entry: &consensus::StateArtifact,
        bytes: &[u8],
    ) -> Result<StagedSegmentMeta, SpillError> {
        let Some(partition_dir) = self.partition_dir.clone() else {
            return Err(SpillError::NoPartitionDir);
        };
        let (stats, index_bytes) = walk_segment_payload(entry.frontier, bytes)
            .await
            .map_err(SpillError::Walk)?;
        let (log_staging, index_staging) = staging_paths(&partition_dir, entry.frontier);
        let index_size = index_bytes.len() as u64;
        write_staging_file(&index_staging, index_bytes)
            .await
            .map_err(|source| SpillError::StagingIo {
                path: index_staging.clone(),
                source,
            })?;
        Ok(StagedSegmentMeta::from_walk(
            entry,
            stats,
            index_size,
            log_staging,
            index_staging,
        ))
    }

    /// Scan the partition directory for staging files left by an earlier
    /// session and adopt every one that matches a manifest entry byte-for-
    /// byte (length + artifact checksum + full re-walk). Sealed segments are
    /// immutable, so on a retry or peer re-target typically only the active
    /// segment and the offsets artifact re-pull. Staging strays matching no
    /// entry are swept.
    ///
    /// A scan against a segment set this partition already scanned short-
    /// circuits through `ReuseScanMemo`: rotating to another peer would
    /// otherwise re-read and re-walk every staged file, up to 2 GiB each,
    /// sequentially, on the pump.
    pub async fn reuse_staged_segments(
        &self,
        manifest: &[consensus::StateArtifact],
    ) -> Vec<(u32, StagedSegmentMeta)> {
        let Some(partition_dir) = self.partition_dir.clone() else {
            return Vec::new();
        };
        let digest = segment_manifest_digest(manifest);
        // Cloned out of the borrow: the file re-checks below await.
        let memoized = self
            .reuse_scan_memo
            .borrow()
            .as_ref()
            .filter(|memo| memo.digest == digest)
            .map(|memo| memo.adopted.clone());
        if let Some(adopted) = memoized {
            // The memo proves the bytes were validated; only their continued
            // presence needs re-checking (an install or a converge between the
            // two scans unlinks them).
            let mut intact = true;
            for (_, meta) in &adopted {
                let log_matches = matches!(
                    compio::fs::metadata(&meta.log_staging).await,
                    Ok(metadata) if metadata.len() == meta.size
                );
                if !log_matches || compio::fs::metadata(&meta.index_staging).await.is_err() {
                    intact = false;
                    break;
                }
            }
            if intact {
                return adopted;
            }
            self.reuse_scan_memo.borrow_mut().take();
        }
        let mut adopted = Vec::new();
        let mut matched_paths = Vec::new();
        for (index, entry) in manifest.iter().enumerate() {
            if entry.kind != artifact_kind::SEGMENT_LOG {
                continue;
            }
            let (log_staging, _) = staging_paths(&partition_dir, entry.frontier);
            // Length short-circuit BEFORE reading: length is the first
            // conjunct of the artifact check anyway, and the common retry
            // case is precisely an active-segment length mismatch -- no
            // point reading up to 2 GiB just to discard it.
            match compio::fs::metadata(&log_staging).await {
                Ok(metadata) if metadata.len() == entry.len => {}
                _ => continue,
            }
            let Ok(bytes) = compio::fs::read(&log_staging).await else {
                continue;
            };
            if !verify_state_artifact_yielding(entry, &bytes).await {
                continue;
            }
            if let Ok(meta) = self.adopt_staged_segment(entry, &bytes).await {
                matched_paths.push(meta.log_staging.clone());
                matched_paths.push(meta.index_staging.clone());
                #[allow(clippy::cast_possible_truncation)]
                adopted.push((index as u32, meta));
            }
        }
        // Every rebuilt sidecar landed in the same directory, so one fsync
        // covers them all. Its failure discards EVERY adoption: the per-adopt
        // filter above no longer sees a durability failure, and an undurable
        // sidecar handed to the install is a missing rename source after a
        // crash.
        if !adopted.is_empty() && fsync_dir(&partition_dir).await.is_err() {
            adopted.clear();
            matched_paths.clear();
        }
        // Sweep strays: anything staged that no adopted meta claims.
        let keep: HashSet<&Path> = matched_paths.iter().map(PathBuf::as_path).collect();
        sweep_staging_except(&partition_dir, &keep).await;
        *self.reuse_scan_memo.borrow_mut() = Some(ReuseScanMemo {
            digest,
            adopted: adopted.clone(),
        });
        adopted
    }

    /// Install a fully transferred partition state: swap the staged segment
    /// files in, rebuild the in-memory log over them, replace the consumer
    /// offset tables, clear the journal, and lift the commit floor to
    /// `commit_op`. The live tail `(commit_op, commit_max]` is left to
    /// ordinary journal repair.
    ///
    /// Validation runs before live-state mutation. Offset replacement siblings
    /// are then written and synced while the old partition remains intact. The
    /// segment swap's crash windows recover as an honestly-shorter partition
    /// (see the swap ordering comments); boot re-derives from surviving files.
    ///
    /// # Errors
    /// [`PartitionInstallError`]; check-phase variants mutate nothing.
    #[allow(clippy::too_many_lines)]
    pub fn queue_state_transfer_install(
        &mut self,
        commit_op: u64,
        staged: Vec<StagedSegmentMeta>,
        offsets_bytes: &[u8],
        committed_purge_generation: u64,
        peer: u8,
    ) -> Result<(), PartitionInstallError> {
        let pending = self.prepare_state_transfer_install(
            commit_op,
            staged,
            offsets_bytes,
            committed_purge_generation,
            peer,
        )?;
        self.transition = Some(crate::iggy_partition::PendingPartitionTransition::Install(
            Box::new(pending),
        ));
        self.notify_io();
        Ok(())
    }

    pub(crate) fn install_io_charge(&self) -> Option<usize> {
        let Some(crate::iggy_partition::PendingPartitionTransition::Install(pending)) =
            &self.transition
        else {
            return None;
        };
        let paths = [
            Some(pending.partition_dir.as_str()),
            self.consumer_offsets_path.as_deref(),
            self.consumer_group_offsets_path.as_deref(),
        ];
        let base = crate::io::file_phase_charge::<SB>(paths.into_iter().flatten())?;
        match pending.phase {
            InstallPhase::StageOffsets(_) | InstallPhase::DiscardOffsets(_) => {
                base.checked_mul(OFFSET_PERSIST_CONCURRENCY)
            }
            InstallPhase::Sweep => pending.staged.iter().try_fold(base, |charge, segment| {
                charge
                    .checked_add(segment.log_staging.capacity())?
                    .checked_add(segment.index_staging.capacity())?
                    .checked_add(2 * (size_of::<PathBuf>() + 4 * size_of::<&Path>() + 4))
            }),
            _ => Some(base),
        }
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) async fn drive_install_io(&mut self) -> crate::PartitionIoStep {
        let Some(crate::iggy_partition::PendingPartitionTransition::Install(mut install)) =
            self.transition.take()
        else {
            return crate::PartitionIoStep::Pending;
        };
        let mut step = crate::PartitionIoStep::Progress;
        match install.phase {
            InstallPhase::StageOffsets(cursor) if cursor == install.planned_offsets.len() => {
                install.phase = InstallPhase::Drain;
            }
            InstallPhase::Drain => {
                if let Some(persistence) = &self.persistence {
                    self.start_persistence();
                    let drain = install
                        .drain
                        .get_or_insert_with(|| persistence.begin_drain());
                    match persistence.observe_drain(drain) {
                        Ok(true) => {
                            install.disposition = InstallDisposition::Backup;
                            install.phase = InstallPhase::Backup;
                        }
                        Ok(false) => step = crate::PartitionIoStep::Pending,
                        Err(source) => self.fail_install(
                            &mut install,
                            PartitionInstallError::SwapIo {
                                path: self.partition_dir.clone().unwrap_or_default(),
                                source,
                            },
                        ),
                    }
                } else {
                    install.phase = InstallPhase::Frontier;
                }
            }
            InstallPhase::Frontier => {
                let frontier = install.offsets_wire.next_offset;
                let intent = if install.purge_advances {
                    crate::iggy_partition::SuperblockIntent::Reset(frontier)
                } else {
                    crate::iggy_partition::SuperblockIntent::Install(frontier)
                };
                match self.transition_superblock(intent).await {
                    crate::PartitionIoVerdict::Pending => step = crate::PartitionIoStep::Pending,
                    crate::PartitionIoVerdict::Failed => self.fail_install(
                        &mut install,
                        PartitionInstallError::FrontierNotDurable { frontier },
                    ),
                    crate::PartitionIoVerdict::Ready => {
                        install.disposition = InstallDisposition::Mutating;
                        self.invalidate_poll_history();
                        self.log.invalidate_sealed_read_state();
                        self.segment_checksum_cache.borrow_mut().clear();
                        self.reuse_scan_memo.borrow_mut().take();
                        if let Some(persistence) = &self.persistence {
                            persistence.retire_offset_files();
                        }
                        install.phase = InstallPhase::RemoveSegments;
                    }
                }
            }
            InstallPhase::RemoveSegments if self.log.segments().is_empty() => {
                install.phase = InstallPhase::Sweep;
            }
            InstallPhase::RenameIndexes(cursor) if cursor == install.staged.len() => {
                install.phase = InstallPhase::IndexDirectory;
            }
            InstallPhase::RenameLogs(cursor) if cursor == install.staged.len() => {
                install.directory_handle = None;
                install.phase = InstallPhase::OpenSegments(0);
            }
            InstallPhase::OpenSegments(cursor) if cursor > 0 && cursor >= install.staged.len() => {
                self.clear_install_offsets();
                install.phase = InstallPhase::OldOffsets(0);
            }
            InstallPhase::OldOffsets(2) => {
                install.phase = InstallPhase::CommitOffsets(0);
            }
            InstallPhase::OldOffsets(kind) => {
                let directory = if kind == 0 {
                    self.consumer_offsets_path.as_deref()
                } else {
                    self.consumer_group_offsets_path.as_deref()
                };
                if install.offset_files.is_none() {
                    install.offset_files = directory.and_then(|path| std::fs::read_dir(path).ok());
                }
                let table = if kind == 0 {
                    &install.offsets_wire.consumers
                } else {
                    &install.offsets_wire.groups
                };
                let next = install.offset_files.as_mut().and_then(|entries| {
                    entries.find_map(|entry| {
                        let entry = entry.ok()?;
                        if !entry.file_type().ok()?.is_file() {
                            return None;
                        }
                        let name = entry.file_name();
                        let id = name.to_str()?.parse::<u32>().ok()?;
                        if install.next_offset > 0
                            && table.binary_search_by_key(&id, |(id, _)| *id).is_ok()
                        {
                            return None;
                        }
                        Some((id, entry.path().to_string_lossy().into_owned()))
                    })
                });
                if let Some((id, path)) = next {
                    install.phase = InstallPhase::DeleteOffset { kind, id, path };
                } else {
                    install.offset_files = None;
                    install.phase = InstallPhase::OldOffsets(kind + 1);
                }
            }
            InstallPhase::CommitOffsets(cursor) if cursor == install.planned_offsets.len() => {
                install.phase = InstallPhase::OffsetDirectory(0);
            }
            InstallPhase::OffsetDirectory(2) => {
                install.phase = InstallPhase::Publish;
            }
            InstallPhase::OffsetDirectory(kind) if !install.offset_dirs_changed[kind] => {
                install.phase = InstallPhase::OffsetDirectory(kind + 1);
            }
            InstallPhase::Publish => {
                self.publish_installed_state(&install);
                install.phase = InstallPhase::PurgeGeneration;
            }
            InstallPhase::PurgeGeneration
                if install.offsets_wire.purge_generation <= self.applied_purge_generation =>
            {
                install.phase = InstallPhase::WalReset;
            }
            InstallPhase::WalReset => {
                self.applied_purge_generation = self
                    .applied_purge_generation
                    .max(install.offsets_wire.purge_generation);
                self.purge_deferred = false;
                if let Some(persistence) = &self.persistence {
                    let prepare =
                        (!install.offsets_wire.checkpoint_prepare.is_empty()).then(|| {
                            Owned::<4096>::copy_from_slice(&install.offsets_wire.checkpoint_prepare)
                                .into()
                        });
                    let segment = self.log.active_segment();
                    let position = journal::partition_journal::SegmentPosition {
                        start_offset: segment.start_offset,
                        length: segment.size.as_bytes_u64(),
                        next_offset: install.next_offset,
                    };
                    persistence.reset_with_segments(
                        install.commit_op,
                        install.offsets_wire.prepare_checksum,
                        prepare,
                        Some((position, segment.max_size.as_bytes_u64())),
                    );
                    self.start_persistence();
                    install.drain = Some(persistence.begin_drain());
                    install.phase = InstallPhase::ResetDraining;
                } else {
                    install.phase = InstallPhase::WalCertify;
                }
            }
            InstallPhase::ResetDraining | InstallPhase::CertifyDraining => {
                let persistence = self.persistence.as_ref().expect("install drain has a WAL");
                match persistence
                    .observe_drain(install.drain.as_ref().expect("install drain exists"))
                {
                    Ok(true) => {
                        if matches!(install.phase, InstallPhase::ResetDraining) {
                            install.phase = InstallPhase::WalCertify;
                        } else {
                            self.publish_installed_consensus(&install);
                            install.phase = InstallPhase::FinalFrontier;
                        }
                    }
                    Ok(false) => step = crate::PartitionIoStep::Pending,
                    Err(source) => self.fail_install(
                        &mut install,
                        PartitionInstallError::SwapIo {
                            path: self.partition_dir.clone().unwrap_or_default(),
                            source,
                        },
                    ),
                }
            }
            InstallPhase::WalCertify => {
                if let Some(persistence) = &self.persistence {
                    persistence.certify_log_view(
                        self.consensus().log_view(),
                        install.commit_op,
                        install.offsets_wire.prepare_checksum.unwrap_or(0),
                    );
                    self.start_persistence();
                    install.drain = Some(persistence.begin_drain());
                    install.phase = InstallPhase::CertifyDraining;
                } else {
                    self.publish_installed_consensus(&install);
                    install.phase = InstallPhase::FinalFrontier;
                }
            }
            InstallPhase::FinalFrontier => {
                match self
                    .transition_superblock(crate::iggy_partition::SuperblockIntent::Install(
                        self.offset_frontier(),
                    ))
                    .await
                {
                    crate::PartitionIoVerdict::Pending => step = crate::PartitionIoStep::Pending,
                    verdict => {
                        if verdict == crate::PartitionIoVerdict::Failed {
                            tracing::error!(
                                "installed frontier final write failed; retaining the pre-swap record"
                            );
                        }
                        install.phase = if self.persistence.is_some() {
                            InstallPhase::FinishBackup
                        } else {
                            InstallPhase::ClearMissing
                        };
                    }
                }
            }
            InstallPhase::ClearMissing
                if install.failure.is_some() || !self.materialization_missing =>
            {
                install.phase = InstallPhase::Done;
            }
            InstallPhase::DiscardOffsets(cursor) if cursor == install.planned_offsets.len() => {
                install.phase = InstallPhase::Done;
            }
            InstallPhase::Done => {
                if let (Some(persistence), Some(drain)) = (&self.persistence, &install.drain) {
                    persistence.finish_drain(drain);
                }
                let outcome = install.failure.take().map_or_else(
                    || {
                        Ok(PartitionInstallOutcome {
                            applied_commit_op: install.commit_op,
                            purge_generation_recorded: install.purge_generation_recorded,
                        })
                    },
                    Err,
                );
                return crate::PartitionIoStep::InstallFinished {
                    peer: install.peer,
                    outcome,
                };
            }
            InstallPhase::Submitted(_) => step = crate::PartitionIoStep::Pending,
            _ => {
                self.transition = Some(crate::iggy_partition::PendingPartitionTransition::Install(
                    install,
                ));
                return self.plan_install_io();
            }
        }
        self.transition = Some(crate::iggy_partition::PendingPartitionTransition::Install(
            install,
        ));
        step
    }

    pub(crate) fn fail_install_capture(&mut self, source: iggy_common::IggyError) {
        if let Some(crate::iggy_partition::PendingPartitionTransition::Install(mut install)) =
            self.transition.take()
        {
            let error = PartitionInstallError::SegmentOpen {
                path: install.partition_dir.clone(),
                source,
            };
            self.fail_install(&mut install, error);
            self.transition = Some(crate::iggy_partition::PendingPartitionTransition::Install(
                install,
            ));
        }
    }

    fn fail_install(&mut self, install: &mut PendingInstall, error: PartitionInstallError) {
        install.failure = Some(error);
        install.phase = match install.disposition {
            InstallDisposition::Prepared => InstallPhase::DiscardOffsets(0),
            InstallDisposition::Backup | InstallDisposition::Mutating
                if self.persistence.is_some() =>
            {
                self.fence_install_failure(install.commit_op);
                InstallPhase::Done
            }
            InstallDisposition::Backup | InstallDisposition::Mutating => InstallPhase::Converge,
        };
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn capture_install_io(
        &mut self,
        config: &PartitionsConfig,
    ) -> Result<crate::PartitionIoJob<SB>, iggy_common::IggyError> {
        let Some(crate::iggy_partition::PendingPartitionTransition::Install(install)) =
            self.transition.as_ref()
        else {
            return Err(iggy_common::IggyError::CannotWriteToFile);
        };
        let file_job = |job| crate::PartitionIoJob::Transfer(job);
        let (job, phase) = match &install.phase {
            InstallPhase::StageOffsets(cursor) | InstallPhase::DiscardOffsets(cursor) => {
                let end = (*cursor + OFFSET_PERSIST_CONCURRENCY).min(install.planned_offsets.len());
                let writes = install.planned_offsets[*cursor..end].to_vec();
                if matches!(install.phase, InstallPhase::StageOffsets(_)) {
                    (
                        file_job(crate::io::TransferFileJob::StageOffsets(writes)),
                        InstallFilePhase::StageOffsets(end),
                    )
                } else {
                    (
                        file_job(crate::io::TransferFileJob::DiscardOffsets(writes)),
                        InstallFilePhase::DiscardOffsets(end),
                    )
                }
            }
            InstallPhase::Backup => (
                file_job(crate::io::TransferFileJob::Backup {
                    directory: install.partition_dir.clone(),
                    begin: true,
                }),
                InstallFilePhase::Backup,
            ),
            InstallPhase::RemoveSegments => {
                let (segment, mut storage) = self
                    .log
                    .retire_front()
                    .ok_or(iggy_common::IggyError::CannotDeleteFile)?;
                let (messages, index) = storage.segment_and_index_paths();
                let _ = storage.shutdown();
                (
                    crate::PartitionIoJob::RemoveSegment {
                        namespace: IggyNamespace::from_raw(self.consensus().group()),
                        paths: [
                            messages,
                            index,
                            self.anchor_cleanup_path(segment.start_offset),
                        ],
                        strict: true,
                    },
                    InstallFilePhase::RemoveSegment,
                )
            }
            InstallPhase::Sweep => (
                file_job(crate::io::TransferFileJob::Sweep {
                    directory: install.partition_dir.clone(),
                    keep: install
                        .staged
                        .iter()
                        .flat_map(|meta| [meta.log_staging.clone(), meta.index_staging.clone()])
                        .collect(),
                    bodies: self.persistence.is_some(),
                }),
                InstallFilePhase::Sweep,
            ),
            InstallPhase::RenameIndexes(cursor) => {
                let meta = &install.staged[*cursor];
                let (_, to) = final_paths(&install.partition_dir, meta.start_offset);
                (
                    file_job(crate::io::TransferFileJob::Rename {
                        from: meta.index_staging.clone(),
                        to,
                        directory: None,
                    }),
                    InstallFilePhase::RenameIndex(*cursor),
                )
            }
            InstallPhase::IndexDirectory => (
                file_job(crate::io::TransferFileJob::IndexDirectory(
                    install.partition_dir.clone(),
                )),
                InstallFilePhase::IndexDirectory,
            ),
            InstallPhase::RenameLogs(cursor) => {
                let meta = &install.staged[*cursor];
                let (to, _) = final_paths(&install.partition_dir, meta.start_offset);
                (
                    file_job(crate::io::TransferFileJob::Rename {
                        from: meta.log_staging.clone(),
                        to,
                        directory: install.directory_handle.clone(),
                    }),
                    InstallFilePhase::RenameLog(*cursor),
                )
            }
            InstallPhase::OpenSegments(cursor) => {
                let job = if install.staged.is_empty() {
                    crate::PartitionIoJob::EmptySegment(
                        self.prepare_empty_segment(config, install.offsets_wire.next_offset),
                    )
                } else {
                    file_job(crate::io::TransferFileJob::OpenSegment {
                        directory: install.partition_dir.clone(),
                        meta: install.staged[*cursor].clone(),
                        segment_size: self.effective_segment_size(),
                        persisted: self.durability().is_persisted(),
                        preallocate: self.effective_preallocate_segments(config),
                        bodies: self.persistence.is_some(),
                        active: *cursor + 1 == install.staged.len(),
                    })
                };
                (job, InstallFilePhase::OpenSegment(*cursor))
            }
            InstallPhase::EmptyDirectory => (
                crate::PartitionIoJob::SegmentDirectory(install.partition_dir.clone()),
                InstallFilePhase::EmptyDirectory,
            ),
            InstallPhase::DeleteOffset { kind, id, path } => (
                crate::PartitionIoJob::OffsetDelete(crate::io::OffsetDeleteIoJob {
                    path: path.clone(),
                }),
                InstallFilePhase::DeleteOffset {
                    kind: *kind,
                    id: *id,
                },
            ),
            InstallPhase::CommitOffsets(cursor) => (
                file_job(crate::io::TransferFileJob::CommitOffset(
                    install.planned_offsets[*cursor].path.clone(),
                )),
                InstallFilePhase::CommitOffset(*cursor),
            ),
            InstallPhase::OffsetDirectory(kind) => {
                let directory = if *kind == 0 {
                    &self.consumer_offsets_path
                } else {
                    &self.consumer_group_offsets_path
                };
                (
                    crate::PartitionIoJob::SegmentDirectory(
                        directory
                            .clone()
                            .ok_or(iggy_common::IggyError::CannotSyncFile)?,
                    ),
                    InstallFilePhase::OffsetDirectory(*kind),
                )
            }
            InstallPhase::PurgeGeneration => (
                crate::PartitionIoJob::PurgeGeneration {
                    path: format!("{}/{PURGE_GENERATION_FILE}", install.partition_dir),
                    generation: install.offsets_wire.purge_generation,
                    revision: self.created_revision,
                },
                InstallFilePhase::PurgeGeneration,
            ),
            InstallPhase::FinishBackup => (
                file_job(crate::io::TransferFileJob::Backup {
                    directory: install.partition_dir.clone(),
                    begin: false,
                }),
                InstallFilePhase::FinishBackup,
            ),
            InstallPhase::ClearMissing => (
                file_job(crate::io::TransferFileJob::ClearMissing(
                    install.partition_dir.clone(),
                )),
                InstallFilePhase::ClearMissing,
            ),
            InstallPhase::Converge => (
                file_job(crate::io::TransferFileJob::Converge {
                    directory: install.partition_dir.clone(),
                    offset_directories: [
                        self.consumer_offsets_path.clone(),
                        self.consumer_group_offsets_path.clone(),
                    ],
                    segment: self.prepare_empty_segment(config, install.offsets_wire.next_offset),
                }),
                InstallFilePhase::Converge,
            ),
            _ => return Err(iggy_common::IggyError::CannotWriteToFile),
        };
        let Some(crate::iggy_partition::PendingPartitionTransition::Install(install)) =
            self.transition.as_mut()
        else {
            unreachable!("install exists");
        };
        install.phase = InstallPhase::Submitted(phase);
        Ok(job)
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn accept_install_io(&mut self, result: crate::PartitionIoResult) {
        let Some(crate::iggy_partition::PendingPartitionTransition::Install(mut install)) =
            self.transition.take()
        else {
            return;
        };
        let phase = std::mem::replace(&mut install.phase, InstallPhase::Done);
        let outcome = match (phase, result) {
            (
                InstallPhase::Submitted(phase),
                crate::PartitionIoResult::Transfer(crate::io::TransferFileResult::Finished(
                    outcome,
                )),
            ) => outcome.map(|()| match phase {
                InstallFilePhase::StageOffsets(end) => InstallPhase::StageOffsets(end),
                InstallFilePhase::DiscardOffsets(end) => InstallPhase::DiscardOffsets(end),
                InstallFilePhase::Backup => InstallPhase::Frontier,
                InstallFilePhase::Sweep => InstallPhase::RenameIndexes(0),
                InstallFilePhase::RenameIndex(cursor) => InstallPhase::RenameIndexes(cursor + 1),
                InstallFilePhase::RenameLog(cursor) => InstallPhase::RenameLogs(cursor + 1),
                InstallFilePhase::CommitOffset(cursor) => {
                    let write = &install.planned_offsets[cursor];
                    self.publish_transferred_offset(write);
                    install.offset_dirs_changed[consumer_kind_index(write.kind)] = true;
                    InstallPhase::CommitOffsets(cursor + 1)
                }
                InstallFilePhase::FinishBackup => InstallPhase::ClearMissing,
                InstallFilePhase::ClearMissing => {
                    self.materialization_missing = false;
                    InstallPhase::Done
                }
                _ => unreachable!("transfer file result matches captured phase"),
            }),
            (
                InstallPhase::Submitted(InstallFilePhase::RemoveSegment),
                crate::PartitionIoResult::SegmentRemoved(outcome),
            ) => outcome
                .map(|()| InstallPhase::RemoveSegments)
                .map_err(|source| PartitionInstallError::SegmentOpen {
                    path: install.partition_dir.clone(),
                    source,
                }),
            (
                InstallPhase::Submitted(InstallFilePhase::IndexDirectory),
                crate::PartitionIoResult::Transfer(crate::io::TransferFileResult::Directory(
                    outcome,
                )),
            ) => outcome
                .map(|directory| {
                    install.directory_handle = Some(directory);
                    InstallPhase::RenameLogs(0)
                })
                .map_err(|source| PartitionInstallError::SwapIo {
                    path: install.partition_dir.clone(),
                    source,
                }),
            (
                InstallPhase::Submitted(InstallFilePhase::OpenSegment(cursor)),
                crate::PartitionIoResult::Transfer(crate::io::TransferFileResult::Opened(outcome)),
            ) => outcome.map(|segment| {
                self.accept_empty_segment(segment);
                InstallPhase::OpenSegments(cursor + 1)
            }),
            (
                InstallPhase::Submitted(InstallFilePhase::OpenSegment(_)),
                crate::PartitionIoResult::EmptySegment(outcome),
            ) => outcome
                .map(|segment| {
                    self.accept_empty_segment(segment);
                    InstallPhase::EmptyDirectory
                })
                .map_err(|source| PartitionInstallError::SegmentOpen {
                    path: install.partition_dir.clone(),
                    source,
                }),
            (
                InstallPhase::Submitted(InstallFilePhase::EmptyDirectory),
                crate::PartitionIoResult::SegmentDirectory(outcome),
            ) => outcome
                .map(|()| {
                    self.clear_install_offsets();
                    InstallPhase::OldOffsets(0)
                })
                .map_err(|source| PartitionInstallError::SwapIo {
                    path: install.partition_dir.clone(),
                    source,
                }),
            (
                InstallPhase::Submitted(InstallFilePhase::DeleteOffset { kind, id }),
                crate::PartitionIoResult::OffsetDelete(outcome),
            ) => {
                let consumer_kind = if kind == 0 {
                    ConsumerKind::Consumer
                } else {
                    ConsumerKind::ConsumerGroup
                };
                let capacity = self.consumer_offset_capacity_for(consumer_kind);
                outcome
                    .map(|removed| {
                        install.offset_dirs_changed[kind] |= removed;
                        capacity.clear_stranded(id);
                        InstallPhase::OldOffsets(kind)
                    })
                    .map_err(|source| {
                        capacity.record_stranded(id);
                        PartitionInstallError::OffsetPersistence {
                            path: self
                                .persisted_offset_path(consumer_kind, id)
                                .unwrap_or_default(),
                            source,
                        }
                    })
            }
            (
                InstallPhase::Submitted(InstallFilePhase::OffsetDirectory(kind)),
                crate::PartitionIoResult::SegmentDirectory(outcome),
            ) => outcome
                .map(|()| InstallPhase::OffsetDirectory(kind + 1))
                .map_err(|source| PartitionInstallError::SwapIo {
                    path: install.partition_dir.clone(),
                    source,
                }),
            (
                InstallPhase::Submitted(InstallFilePhase::PurgeGeneration),
                crate::PartitionIoResult::PurgeGeneration(outcome),
            ) => {
                if let Err(error) = outcome {
                    tracing::warn!(%error, "installed purge generation could not be recorded");
                    install.purge_generation_recorded = false;
                }
                Ok(InstallPhase::WalReset)
            }
            (
                InstallPhase::Submitted(InstallFilePhase::Converge),
                crate::PartitionIoResult::Transfer(crate::io::TransferFileResult::Converged {
                    segment,
                    stranded,
                }),
            ) => {
                self.clear_converged_state(&install);
                if let Some((kind, id)) = stranded {
                    self.consumer_offset_capacity_for(if kind == 0 {
                        ConsumerKind::Consumer
                    } else {
                        ConsumerKind::ConsumerGroup
                    })
                    .record_stranded(id);
                }
                match segment {
                    Ok(segment) => {
                        self.accept_empty_segment(segment);
                        Ok(InstallPhase::FinalFrontier)
                    }
                    Err(source) => {
                        install.failure = Some(PartitionInstallError::ConvergeFailed {
                            source,
                            frontier: install.offsets_wire.next_offset,
                        });
                        Ok(InstallPhase::Done)
                    }
                }
            }
            _ => Err(PartitionInstallError::SwapIo {
                path: install.partition_dir.clone(),
                source: std::io::Error::other(
                    "transfer completion did not match its captured phase",
                ),
            }),
        };
        match outcome {
            Ok(phase) => install.phase = phase,
            Err(error) => self.fail_install(&mut install, error),
        }
        self.transition = Some(crate::iggy_partition::PendingPartitionTransition::Install(
            install,
        ));
    }

    fn clear_install_offsets(&mut self) {
        self.log.journal().inner.clear_all();
        self.log.journal_mut().info = crate::log::JournalInfo::default();
        self.consumer_offsets.pin().clear();
        self.consumer_group_offsets.pin().clear();
        self.last_polled_offsets.pin().clear();
        self.durable_consumer_offsets.clear();
        self.pending_consumer_offset_commits.clear();
        for kind in [ConsumerKind::Consumer, ConsumerKind::ConsumerGroup] {
            self.consumer_offset_capacity_for(kind)
                .rebuild(&self.durable_consumer_offsets, std::iter::empty());
        }
    }

    fn publish_transferred_offset(&self, write: &PlannedOffsetWrite) {
        let entry = ConsumerOffset::new(write.kind, write.id, write.value, write.path.clone());
        match write.kind {
            ConsumerKind::Consumer => {
                self.consumer_offsets.pin().insert(write.id as usize, entry);
            }
            ConsumerKind::ConsumerGroup => {
                self.consumer_group_offsets
                    .pin()
                    .insert(ConsumerGroupId(write.id as usize), entry);
            }
        }
        self.durable_consumer_offsets.record_explicit(
            write.kind,
            write.id,
            write.value,
            write.value,
        );
        self.consumer_offset_capacity_for(write.kind)
            .clear_stranded(write.id);
    }

    fn publish_installed_state(&mut self, install: &PendingInstall) {
        self.dedup_mut()
            .install_watermarks(install.offsets_wire.dedup.iter().copied());
        let end = install.next_offset.saturating_sub(1);
        self.offset.store(end, Ordering::Release);
        self.publish_poll_visibility();
        self.dirty_offset.store(end, Ordering::Relaxed);
        self.set_offset_space_used(install.next_offset > 0);
        self.recovered_durable_offset = install.staged.last().map(|meta| meta.end_offset);
        self.installed_frontier = (install.next_offset > 0).then_some(install.next_offset);
        self.stats.zero_out_all();
        self.stats.increment_segments_count(
            u32::try_from(self.log.segments().len()).expect("manifest segment count fits u32"),
        );
        self.stats
            .increment_size_bytes(install.staged.iter().map(|meta| meta.size).sum());
        self.stats.increment_messages_count(
            install
                .staged
                .iter()
                .map(|meta| meta.end_offset - meta.start_offset + 1)
                .sum(),
        );
        self.stats.set_current_offset(end);
    }

    fn publish_installed_consensus(&mut self, install: &PendingInstall) {
        if !install.offsets_wire.checkpoint_prepare.is_empty() {
            self.log.journal().inner.restore_checkpoint_prepare(
                install.commit_op,
                Owned::<4096>::copy_from_slice(&install.offsets_wire.checkpoint_prepare).into(),
            );
        }
        let consensus = self.consensus();
        if install.commit_op > consensus.commit_min() {
            consensus.set_commit_floor(install.commit_op);
        }
        consensus.sequencer().set_sequence(install.commit_op);
        if let Some(checksum) = install.offsets_wire.prepare_checksum {
            consensus.set_last_prepare_checksum(checksum);
        }
        consensus.clear_pipeline();
        consensus.advance_commit_max(install.commit_op);
        self.observed_view = self.consensus().view();
        self.repair = None;
        self.transfer_offer_cache.borrow_mut().take();
    }

    fn clear_converged_state(&mut self, install: &PendingInstall) {
        self.invalidate_poll_history();
        self.log.invalidate_sealed_read_state();
        while let Some((_, mut storage)) = self.log.retire_front() {
            let _ = storage.shutdown();
        }
        self.clear_install_offsets();
        self.segment_checksum_cache.borrow_mut().clear();
        self.reuse_scan_memo.borrow_mut().take();
        self.dedup_mut().install_watermarks(std::iter::empty());
        let end = install.offsets_wire.next_offset.saturating_sub(1);
        self.offset.store(end, Ordering::Release);
        self.publish_poll_visibility();
        self.dirty_offset.store(end, Ordering::Relaxed);
        self.set_offset_space_used(install.offsets_wire.next_offset > 0);
        self.recovered_durable_offset = None;
        self.installed_frontier = (install.staged.is_empty()
            && install.offsets_wire.next_offset > 0)
            .then_some(install.offsets_wire.next_offset);
        self.stats.zero_out_all();
        self.stats.increment_segments_count(1);
        self.stats.set_current_offset(end);
        self.repair = None;
        self.transfer_offer_cache.borrow_mut().take();
    }

    fn prepare_state_transfer_install(
        &self,
        commit_op: u64,
        mut staged: Vec<StagedSegmentMeta>,
        offsets_bytes: &[u8],
        committed_purge_generation: u64,
        peer: u8,
    ) -> Result<PendingInstall, PartitionInstallError> {
        // ---- check phase: nothing below may mutate live state. Staging
        // writes only sibling files the install can abandon. ----
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
        // The install rewinds the sequencer to `commit_op`, which erases ops
        // this replica may already have journaled and acked. Bounding it below
        // by what this replica knows to be COMMITTED keeps the erased window to
        // ops it does not know are committed -- the checkable form of an
        // argument the rewind's own comment only asserts. Free on an honest
        // offer: only a caught-up primary can serve, so its `commit_min`
        // equals its `commit_max`, and the receiver's descriptor gate already
        // refused any peer whose `commit_max` was below this one's.
        let commit_max = self.consensus().commit_max();
        if commit_op < commit_max {
            return Err(PartitionInstallError::StaleTransfer {
                commit_op,
                commit_min: commit_max,
            });
        }
        let offsets_wire = ConsumerOffsetsWire::decode(offsets_bytes)?;
        if self.persistence.is_some()
            && (offsets_wire.prepare_checksum.is_none()
                || (commit_op > 0 && offsets_wire.checkpoint_prepare.is_empty()))
        {
            return Err(ConsumerOffsetsWireError::MissingPrepareChecksum.into());
        }
        if !offsets_wire.checkpoint_prepare.is_empty() {
            let prepare = Message::<PrepareHeader>::try_from(Owned::copy_from_slice(
                &offsets_wire.checkpoint_prepare,
            ))
            .map_err(|_| ConsumerOffsetsWireError::InvalidPrepareChecksum)?;
            let header = prepare.header();
            if header.op != commit_op
                || header.group != self.consensus().group()
                || Some(header.checksum) != offsets_wire.prepare_checksum
                || header.size as usize != offsets_wire.checkpoint_prepare.len()
                || (header.checksum != 0 && header.identity_checksum() != header.checksum)
                || (header.checksum_body != 0
                    && header.checksum_body
                        != u128::from(iggy_common::calculate_checksum(
                            &offsets_wire.checkpoint_prepare[size_of::<PrepareHeader>()..],
                        )))
                || (header.operation == Operation::SendMessages
                    && header.checksum_body == 0
                    && decode_prepare_slice(prepare.as_slice()).is_err())
            {
                return Err(ConsumerOffsetsWireError::InvalidPrepareChecksum.into());
            }
        }
        // Anti-rewind against the LOCAL OFFSET COUNTER, not the commit
        // frontier: the partition journal is memory-only and
        // `restore_partition_view` restores view/log_view alone, so `commit_min`
        // is 0 after every restart however much data sits on disk -- the
        // `StaleTransfer` refusal above is inert on exactly the canonical
        // rejoin. The counter is the one signal that is `Some`-equivalent in
        // EVERY state the offset space has advanced through (recovered bytes,
        // an installed frontier, a converge after a failed install --
        // `recovered_durable_offset` is `None` in the last two), and it is
        // precisely what a rewind corrupts: received prepares are pre-stamp,
        // `stamp_prepare_for_persistence` overwrites `base_offset` from this
        // counter and recomputes `batch_checksum` over it, so a rewound
        // counter persists different bytes and a different checksum on this
        // replica than on the rest of the group. A purge is the one
        // legitimate rewind, and the artifact carries the generation that
        // proves one happened.
        // Against the METADATA plane's committed generation, which the caller
        // reads off durable state, NOT against `self.applied_purge_generation`:
        // that one hydrates from `purge.gen`, which a kill before the purge's
        // record step leaves absent or stale, so a post-restart rejoin of an
        // ever-purged topic could see `offered > applied` and call it an
        // advancing purge. That is the canonical rejoin, and treating it as a
        // purge disables the `OfferRewindsDurableData` refusal below -- the
        // one guard standing between an offer that rewinds this replica's
        // offset space and its durable data.
        // Second disjunct: this replica has NOT applied the committed purge, so
        // its frontier still measures the pre-purge offset space and cannot be
        // compared against a post-purge offer. Restricted to `next_offset == 0`
        // -- the state a purge leaves before anything is appended -- so an
        // origin that merely lags within the same purge era still fails the
        // fence rather than rewinding this replica's durable post-purge data.
        let purge_advances = offsets_wire.purge_generation > committed_purge_generation
            || (self.applied_purge_generation < committed_purge_generation
                && offsets_wire.next_offset == 0);
        // The COMMITTED frontier, which is what an offer is comparable against:
        // `held_offset_frontier` reads 0 for a chain installed empty at frontier
        // N (its disk arm filters empty segments and the install clears the
        // journal), and a 0 skips the guard below entirely, letting a stale offer
        // rewind the counter under data this replica already claimed. The append
        // point is not usable either -- it can stand a lease block high -- but
        // only on a solo group, which never receives an offer.
        let local_next_offset = self.offset_frontier();
        if !purge_advances && local_next_offset > 0 && offsets_wire.next_offset < local_next_offset
        {
            return Err(PartitionInstallError::OfferRewindsDurableData {
                offer_next_offset: offsets_wire.next_offset,
                local_next_offset,
            });
        }
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

        let installed_end = staged.last().map(|meta| meta.end_offset);
        let next_offset = offsets_wire
            .next_offset
            .max(installed_end.map_or(0, |end| end + 1));
        let planned_offsets = self.plan_transfer_offset_writes(&offsets_wire, next_offset)?;
        Ok(PendingInstall {
            commit_op,
            peer,
            partition_dir,
            staged,
            offsets_wire,
            planned_offsets,
            next_offset,
            purge_advances,
            phase: InstallPhase::StageOffsets(0),
            drain: None,
            offset_files: None,
            directory_handle: None,
            offset_dirs_changed: [false; 2],
            failure: None,
            disposition: InstallDisposition::Prepared,
            purge_generation_recorded: true,
        })
    }

    /// Execute the install phases for an isolated, unmounted partition.
    ///
    /// # Errors
    /// Returns validation, drain, swap or convergence failures without publishing an invalid history.
    pub async fn install_state_transfer(
        &mut self,
        config: &PartitionsConfig,
        commit_op: u64,
        staged: Vec<StagedSegmentMeta>,
        offsets_bytes: &[u8],
        committed_purge_generation: u64,
    ) -> Result<PartitionInstallOutcome, PartitionInstallError> {
        let peer = self.consensus().primary_index(self.consensus().view());
        let pending = self.prepare_state_transfer_install(
            commit_op,
            staged,
            offsets_bytes,
            committed_purge_generation,
            peer,
        )?;
        self.transition = Some(crate::iggy_partition::PendingPartitionTransition::Install(
            Box::new(pending),
        ));
        loop {
            match self.drive_install_io().await {
                crate::PartitionIoStep::Progress => {}
                crate::PartitionIoStep::Ready(_) => match self.capture_install_io(config) {
                    Ok(job) => self.accept_install_io(job.execute().await),
                    Err(error) => self.fail_install_capture(error),
                },
                crate::PartitionIoStep::Pending => {
                    if let Some(persistence) = &self.persistence {
                        if let Err(source) = persistence.drain_with_timeout().await
                            && let Some(crate::iggy_partition::PendingPartitionTransition::Install(
                                mut install,
                            )) = self.transition.take()
                        {
                            let error = PartitionInstallError::SwapIo {
                                path: install.partition_dir.clone(),
                                source,
                            };
                            self.fail_install(&mut install, error);
                            self.transition = Some(
                                crate::iggy_partition::PendingPartitionTransition::Install(install),
                            );
                        }
                    } else {
                        return Err(PartitionInstallError::SwapIo {
                            path: self.partition_dir.clone().unwrap_or_default(),
                            source: std::io::Error::other(
                                "isolated install has an unresolved phase",
                            ),
                        });
                    }
                }
                crate::PartitionIoStep::InstallFinished { outcome, .. } => return outcome,
                _ => unreachable!("install driver returns only install steps"),
            }
        }
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

/// Payload bytes between rebuilt sparse-index entries.
///
/// Targets the ORIGIN's density (one entry per flush chunk), not maximum
/// sparseness: at this stride a maximum-size 1 GiB segment rebuilds ~16k
/// entries (~384 KiB), inside `poll_plan::SEALED_INDEX_RESIDENT_MAX_BYTES`, so
/// a transferred segment caches its index like any other instead of taking the
/// per-poll binary-search fallback.
const INDEX_STRIDE_BYTES: usize = 64 * 1024;

/// Chunk size for the offer build's streaming checksum pass. Large enough
/// that per-chunk overhead is noise, small enough that the pump yields to
/// the reactor many times per segment. Sized against the yield's real cost:
/// `yield_to_reactor` is ~12 us per call, so at 1 MiB (~21 us of hashing per
/// chunk) the yields would add over half the pass again; 4 MiB keeps the
/// un-yielded stretch a bounded ~80 us CPU pass at ~15% overhead, and
/// matches the recovery walk's `SCAN_WINDOW_CAPACITY`.
const OFFER_HASH_CHUNK_LEN: usize = 4 << 20;

/// Bytes one offer-build round may read and hash before it refuses and resumes
/// on the next request.
///
/// The pass holds a frame body, and this shard's consensus ticks are a sibling
/// select arm that stays unpolled for its duration, so the budget is really a
/// bound on how long every OTHER group on this core goes without a heartbeat.
///
/// A BYTE budget standing in for a time bound, so the margin is storage-class
/// specific: 256 MiB is roughly a quarter second on commodity `NVMe` against
/// the shipped 5 s `heartbeat_timeout`, but about 2 s on a throttled cloud
/// volume at 125 MB/s baseline, which is most of that window. Sized for the
/// slower case still leaving room, and large enough that ordinary retention
/// finishes in one round. An elapsed-time clamp would bound it properly on
/// every storage class.
const OFFER_HASH_BUDGET_PER_ROUND_BYTES: u64 = 256 * 1024 * 1024;

/// Feed bytes `[from, to)` of `path` into `hasher`, read in
/// [`OFFER_HASH_CHUNK_LEN`] chunks with one reactor yield per chunk, appending
/// each chunk to `sink` when one is given.
///
/// The single chunked reader for both passes over a segment file: the offer
/// build's checksum extension (no sink) and the serving side's load + re-verify
/// (sink collects the artifact). Errors on a file shorter than `to`: the segment
/// accounts bytes the disk does not hold.
async fn hash_segment_range(
    path: &str,
    from: u64,
    to: u64,
    hasher: &mut StateArtifactHasher,
    mut sink: Option<&mut Vec<u8>>,
) -> std::io::Result<()> {
    if from >= to {
        return Ok(());
    }
    let file = compio::fs::File::open(path).await?;
    let mut position = from;
    // One buffer for the whole pass; `BufResult` hands it back per read
    // precisely so the alloc + memset are not paid per chunk. Re-allocated
    // (not resized) when the tail chunk is shorter: compio reads into a
    // Vec's CAPACITY, and a shrunken length over the old 1 MiB capacity made
    // `read_exact_at` demand a full megabyte at EOF.
    #[allow(clippy::cast_possible_truncation)]
    let mut buf = vec![0u8; OFFER_HASH_CHUNK_LEN.min((to - from) as usize)];
    while position < to {
        #[allow(clippy::cast_possible_truncation)]
        let want = OFFER_HASH_CHUNK_LEN.min((to - position) as usize);
        if buf.len() != want {
            buf = vec![0u8; want];
        }
        let compio::BufResult(read, returned) = file.read_exact_at(buf, position).await;
        buf = returned;
        read.map_err(|source| {
            std::io::Error::other(format!(
                "reading segment bytes at {position} of {to} failed: {source}"
            ))
        })?;
        hasher.update(&buf);
        if let Some(sink) = sink.as_deref_mut() {
            sink.extend_from_slice(&buf);
        }
        position += want as u64;
    }
    Ok(())
}

/// Read the first `entry.len` bytes of a served segment file and re-verify them
/// against the manifest entry, chunked through `hash_segment_range` with one
/// reactor yield per chunk.
///
/// The serving side runs this on the pump to answer a single chunk request, so
/// it reads and hashes in chunks rather than in one pass. The yields keep the
/// REACTOR moving (detached tasks, `io_uring` completions); they do not keep this
/// shard's consensus ticks alive, which are a sibling select arm of the same
/// task and stay frozen for the duration.
///
/// The file may legitimately be LONGER than the entry (an active segment that
/// kept appending after the offer was built); the artifact is the prefix.
///
/// # Errors
/// [`SegmentLoadError`], which the caller maps onto the refusal it sends: a
/// collapsed `Option` here told a requester that a dying disk was a momentary
/// blip forever, because the refusal it drives is classified by cause.
pub async fn load_verified_segment_artifact(
    log_path: &str,
    entry: &consensus::StateArtifact,
) -> Result<Vec<u8>, SegmentLoadError> {
    let mut hasher = StateArtifactHasher::new();
    #[allow(clippy::cast_possible_truncation)]
    let mut bytes = Vec::with_capacity(entry.len as usize);
    hash_segment_range(log_path, 0, entry.len, &mut hasher, Some(&mut bytes))
        .await
        .map_err(SegmentLoadError::classify)?;
    if hasher.finish() != entry.checksum {
        return Err(SegmentLoadError::ChecksumMismatch);
    }
    Ok(bytes)
}

/// Why a served segment could not be handed to a requester.
///
/// The split is the whole point: a short read is what a concurrent GC
/// unlink-and-recreate legitimately produces and a checksum mismatch means the
/// offer is simply stale, but `EIO` / `EACCES` / a failed open is a fault on
/// THIS node, and telling the requester it was transient hides a dying disk
/// behind an endless peer rotation.
#[derive(Debug)]
pub enum SegmentLoadError {
    /// The file is gone, shorter than the entry, or otherwise out of step with
    /// an offer built earlier. Retryable from the requester's side.
    Stale(std::io::Error),
    /// The bytes are present but no longer hash to the manifest entry.
    ChecksumMismatch,
    /// A local fault: unreadable device, permissions, an open that failed.
    LocalFault(std::io::Error),
}

impl SegmentLoadError {
    fn classify(source: std::io::Error) -> Self {
        // `raw_os_error`, not just `kind()`: std maps EIO to
        // `ErrorKind::Uncategorized`, so a dying disk is invisible to a
        // kind-only match -- the exact case this split exists to catch.
        // Everything unrecognised stays STALE: a short read past EOF is what a
        // racing GC unlink-and-recreate legitimately produces.
        const EIO: i32 = 5;
        if source.raw_os_error() == Some(EIO) {
            return Self::LocalFault(source);
        }
        match source.kind() {
            std::io::ErrorKind::PermissionDenied => Self::LocalFault(source),
            _ => Self::Stale(source),
        }
    }

    /// Whether the requester should retry without charging a failure.
    #[must_use]
    pub const fn transient(&self) -> bool {
        matches!(self, Self::Stale(_) | Self::ChecksumMismatch)
    }
}

impl fmt::Display for SegmentLoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stale(source) => {
                write!(f, "served segment no longer matches the offer: {source}")
            }
            Self::ChecksumMismatch => {
                write!(
                    f,
                    "served segment bytes no longer hash to the manifest entry"
                )
            }
            Self::LocalFault(source) => {
                write!(f, "served segment is unreadable on this node: {source}")
            }
        }
    }
}

impl std::error::Error for SegmentLoadError {}

/// [`consensus::verify_state_artifact`] with reactor yields.
///
/// The receiver runs this on the pump for a whole artifact (up to a segment),
/// and a non-yielding hash of that size makes the node quorum-invisible for its
/// duration and starves the same-core segment cleaner.
async fn verify_state_artifact_yielding(entry: &consensus::StateArtifact, bytes: &[u8]) -> bool {
    if bytes.len() as u64 != entry.len {
        return false;
    }
    let mut hasher = StateArtifactHasher::new();
    for chunk in bytes.chunks(OFFER_HASH_CHUNK_LEN) {
        hasher.update(chunk);
        yield_to_reactor().await;
    }
    hasher.finish() == entry.checksum
}

/// The purge generation an encoded consumer-offsets artifact carries, or `0`
/// when it cannot be decoded.
///
/// Lets the shard refuse an offer built BEFORE a committed purge without
/// duplicating the wire codec: the install's own generation handling only ever
/// widens permission, so a stale offer would resurrect purged data with the
/// local applied generation left at the newer value, which the reconciler's
/// re-wipe gate then reads as "already applied".
#[must_use]
pub fn offered_purge_generation(offsets_bytes: &[u8]) -> u64 {
    ConsumerOffsetsWire::decode(offsets_bytes)
        .map(|wire| wire.purge_generation)
        .unwrap_or_default()
}

pub(crate) fn numeric_offset_id(path: &str) -> Option<u32> {
    Path::new(path).file_name()?.to_str()?.parse().ok()
}

/// Stamp over every `SEGMENT_LOG` entry of a manifest, keying
/// [`ReuseScanMemo`]. Equal digests mean the two offers expect byte-identical
/// staged files, so a scan already done for one answers the other; the offsets
/// artifact is excluded because the scan never looks at it (and it re-encodes
/// per build, so including it would defeat the memo on every rotation).
fn segment_manifest_digest(manifest: &[consensus::StateArtifact]) -> u64 {
    let mut hasher = StateArtifactHasher::new();
    for entry in manifest
        .iter()
        .filter(|entry| entry.kind == artifact_kind::SEGMENT_LOG)
    {
        hasher.update(&entry.frontier.to_le_bytes());
        hasher.update(&entry.len.to_le_bytes());
        hasher.update(&entry.checksum.to_le_bytes());
    }
    hasher.finish()
}

async fn write_staging_file(path: &Path, payload: Vec<u8>) -> std::io::Result<()> {
    let mut file = compio::fs::File::create(path).await?;
    let (result, _) = file.write_all_at(payload, 0).await.into();
    result?;
    file.sync_data().await?;
    Ok(())
}
