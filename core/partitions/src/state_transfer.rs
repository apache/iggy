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
//! group's primary, installs them, and hands the live tail back to
//! ordinary journal repair. Artifacts ride the plane-agnostic manifest/chunk
//! protocol from `core/consensus`; everything in this module is the
//! partition-specific payload handling on either end.

use crate::IggyPartition;
use crate::iggy_partition::{InstalledConsumerGroupOwner, InstalledPartitionHistory};
use crate::io::PartitionIoQuiescence;
use crate::journal::RepairTailLease;
use crate::offset_storage::{discard_offset_replacement, stage_offset_replacement};
use crate::partition_storage::{read_revision_record, write_revision_record};
use crate::segment_anchor::ANCHOR_SUFFIX;
use crate::types::{OFFSETS_DIR, PartitionsConfig};
use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use consensus::client_table::{CLIENTS_TABLE_SLOT_MAX, ClientTable, ClientTableMode};
use consensus::le_cursor::{LeCursor, Truncated, split_verified_trailer};
use consensus::state_manifest::artifact_kind;
use consensus::{
    ArtifactProgress, DedupWatermark, Sequencer as _, StateArtifactHasher, state_artifact_checksum,
};
use iggy_binary_protocol::requests::partitions::retire_consumer_group_owners::CONSUMER_GROUP_OWNERS_MAX;
use iggy_binary_protocol::requests::partitions::{
    InstallConsumerGroupOwnerRequest, RetireConsumerGroupOwnersRequest,
    TransitionPartitionHistoryRequest,
};
use iggy_binary_protocol::responses::messages::send_messages::CONFIRMATION_SIZE;
use iggy_binary_protocol::{Operation, PrepareHeader, ReplyHeader, WireDecode, WireEncode};
use iggy_common::{ConsumerGroupId, ConsumerKind, ConsumerOffset};
use journal::durable_storage::{DiskStorage, DurableStorage};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use server_common::Message;
use server_common::fatal::NoteDescriptorExhaustion;
use server_common::fs_utils::run_blocking;
use server_common::iobuf::Owned;
use server_common::send_messages::{decode_batch_slice, decode_prepare_slice};
use server_common::sharding::IggyNamespace;
use server_common::yield_to_reactor;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::future::Future;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::task::Poll;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

/// Current state-transfer offsets format, including the prepare-chain anchor.
pub(crate) const CONSUMER_OFFSETS_MAGIC: [u8; 4] = *b"ICO1";
/// Version 6 dropped the purge generation that followed the version byte.
/// Version 7 carries the installed consumer-group owners and history fence.
/// Version 8 carries the op of the delete that emptied the offset table.
pub(crate) const CONSUMER_OFFSETS_VERSION: u8 = 8;
pub(crate) const SEGMENT_SWEEP_STACK_BYTES: usize = 2 * 1024 * 1024;

const RETRY_CHECKPOINT_MAGIC: [u8; 4] = *b"IRP2";
const RETRY_CHECKPOINT_PREFIX: &str = "receipts-";
const RETRY_CHECKPOINT_SUFFIX: &str = ".checkpoint";
const RETRY_CHECKPOINT_TEMP_SUFFIX: &str = ".checkpoint.tmp";
const RETRY_CHECKPOINT_BYTES_MAX: usize = journal::partition_journal::PREPARE_BYTES_MAX
    + ConsumerKind::COUNT
        * CONSUMER_OFFSETS_ENTRIES_MAX as usize
        * (size_of::<u32>() + size_of::<u64>())
    + CLIENTS_TABLE_SLOT_MAX * (DEDUP_ENTRY_LEN + PARTITION_RECEIPT_BYTES_MAX)
    + CONSUMER_OFFSETS_ENTRIES_MAX as usize * OWNERSHIP_ENTRY_LEN
    + RetireConsumerGroupOwnersRequest::PREFIX_SIZE
    + CONSUMER_GROUP_OWNERS_MAX as usize * size_of::<u64>()
    + size_of::<PrepareHeader>();

/// Per-section entry ceiling for the consumer-offsets artifact.
///
/// A corruption guard, not a target: it bounds the allocation `decode`
/// makes from a length field a peer sent, exactly like the manifest's own
/// entry ceiling.
pub const CONSUMER_OFFSETS_ENTRIES_MAX: u32 = 1 << 20;

/// Fixed prefix of a session-qualified receipt; reply bytes follow.
const DEDUP_ENTRY_LEN: usize = 2 * size_of::<u128>() + 3 * size_of::<u64>() + 2 * size_of::<u32>();
const OWNERSHIP_ENTRY_LEN: usize =
    InstallConsumerGroupOwnerRequest::ENCODED_SIZE + size_of::<u64>();
const PARTITION_RECEIPT_BYTES_MAX: usize =
    size_of::<ReplyHeader>() + size_of::<u32>() + CONFIRMATION_SIZE;

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
    /// Ticks since the last frame progress or chunk request. At the
    /// configured repair-retry threshold, the missing piece is requested
    /// again.
    pub idle_ticks: u32,
    /// The reuse scan of the accepted manifest. The pull waits for it,
    /// because the scan writes the staging names that the pull spills to.
    pub scan: Option<LocalWork<ReuseScanned>>,
    /// The completed segment being validated and synced outside the pump.
    pub spill: Option<SegmentSpill>,
    /// The leading bytes of one segment artifact that this replica already
    /// holds, so the pull asks only for the rest.
    pub seed: Option<PrefixSeed>,
    /// Chunk payload bytes that crossed the network in this session.
    pub pulled_len: u64,
}

/// The local copy of the leading bytes of one offered segment artifact.
///
/// A lagging replica holds a prefix of the segment the primary is still
/// writing: the same base offset and fewer bytes. The pull then fetches only
/// the suffix. The spill checks the whole artifact against the manifest
/// checksum, so a prefix with other bytes costs one more pull of that
/// artifact and nothing else.
#[derive(Debug)]
pub enum PrefixSeed {
    /// Chosen when the reuse scan lands. The read starts when the pull
    /// reaches `artifact`, so the receiver holds one artifact buffer at a
    /// time.
    Planned {
        artifact: u32,
        log_path: String,
        len: u64,
    },
    /// A detached task reads, hashes and walks the prefix.
    Reading {
        artifact: u32,
        read: LocalWork<PrefixRead>,
    },
    /// The prefix bytes sit in the artifact buffer.
    Seeded { artifact: u32, prefix: SeededPrefix },
}

/// Work that a detached task runs for a transfer session.
///
/// A prefix read and a reuse scan are such work. The pump does not wait for
/// it: the task fills `result` and wakes the partition. Dropping this state
/// asks the task to stop.
#[derive(Debug)]
pub struct LocalWork<T> {
    pub result: Rc<RefCell<Option<T>>>,
    pub cancel: LocalWorkCancel,
}

impl<T> Default for LocalWork<T> {
    fn default() -> Self {
        Self {
            result: Rc::default(),
            cancel: LocalWorkCancel::default(),
        }
    }
}

impl<T> LocalWork<T> {
    /// Whether the task finished, so the session can go on.
    #[must_use]
    pub fn landed(&self) -> bool {
        self.result.borrow().is_some()
    }

    /// Asks the task to stop at its next check. Returns `false` when it was
    /// asked before.
    #[must_use]
    pub fn stop(&self) -> bool {
        !self.cancel.0.replace(true)
    }
}

/// Asks local work to stop when the state that waits for it drops.
///
/// That state drops with its session. A wait that passes its bound asks with
/// [`LocalWork::stop`] instead, and keeps the state until the work lands. The
/// task checks the flag between its steps. A prefix read then frees its
/// buffer of up to one segment early.
#[derive(Debug, Default)]
pub struct LocalWorkCancel(Rc<Cell<bool>>);

impl Drop for LocalWorkCancel {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

/// What a reuse scan staged without a pull.
#[derive(Debug, Default)]
pub struct ReuseScanned {
    /// The manifest index and the staged meta of each segment that the scan
    /// linked or adopted.
    pub reused: Vec<(u32, StagedSegmentMeta)>,
    /// Verified files that a cancelled scan did not revisit.
    pub(crate) memoized: BTreeMap<u64, StagedSegmentMeta>,
}

#[derive(Debug)]
/// Owns the completed artifact while the pull waits for its staging writer.
pub struct SegmentSpill {
    pub artifact: usize,
    pub seeded: bool,
    pub work: LocalWork<Result<SpilledSegment, SpillError>>,
}

/// A durable staging pair and the checksum state needed if it becomes active.
pub struct SpilledSegment {
    meta: StagedSegmentMeta,
    hasher: SpilledHasher,
}

impl fmt::Debug for SpilledSegment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpilledSegment")
            .field("meta", &self.meta)
            .finish_non_exhaustive()
    }
}

/// What a landed reuse scan staged, for the log of the receiver.
#[derive(Debug, Default)]
pub struct ReusedSegments {
    /// Staged copies of an earlier attempt.
    pub adopted: usize,
    pub adopted_len: u64,
    /// Sealed segments that this replica held.
    pub linked: usize,
    pub linked_len: u64,
    pub artifacts: usize,
    pub total_len: u64,
}

impl ReusedSegments {
    /// `artifacts` are the slots of the session before the scan staged any.
    fn new(artifacts: &[TransferArtifact], reused: &[(u32, StagedSegmentMeta)]) -> Self {
        let mut segments = Self {
            artifacts: artifacts.len(),
            total_len: artifacts
                .iter()
                .filter_map(TransferArtifact::pending)
                .map(|progress| progress.entry.len)
                .sum(),
            ..Self::default()
        };
        for (_, meta) in reused {
            if meta.linked {
                segments.linked += 1;
                segments.linked_len += meta.size;
            } else {
                segments.adopted += 1;
                segments.adopted_len += meta.size;
            }
        }
        segments
    }
}

impl PrefixSeed {
    const fn artifact(&self) -> u32 {
        match self {
            Self::Planned { artifact, .. }
            | Self::Reading { artifact, .. }
            | Self::Seeded { artifact, .. } => *artifact,
        }
    }

    /// Whether the prefix read finished and the pull can continue.
    #[must_use]
    pub fn landed(&self) -> bool {
        matches!(self, Self::Reading { read, .. } if read.landed())
    }
}

/// The prefix bytes of a prefix read, with the hash and walk state after them.
type PrefixRead = std::io::Result<(Vec<u8>, SeededPrefix)>;

/// The hash and walk state after the first `len` bytes of a segment artifact,
/// which the spill continues over the pulled suffix.
pub struct SeededPrefix {
    len: u64,
    hasher: Box<StateArtifactHasher>,
    /// `None` when the prefix failed its walk. The spill then walks the whole
    /// artifact, once the checksum proved that the bytes are the offered ones.
    walk: Option<SegmentWalk>,
}

impl fmt::Debug for SeededPrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SeededPrefix")
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

/// The checksum state of a spilled segment, which can extend as the segment
/// grows.
pub(crate) struct SpilledHasher {
    start_offset: u64,
    size: u64,
    hasher: Box<StateArtifactHasher>,
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
    /// The sparse index rebuilt from the verified log at one entry per
    /// `INDEX_STRIDE_BYTES` of payload.
    pub index_staging: PathBuf,
    /// The manifest checksum of the staged log, which the install keeps as
    /// the stamp of the installed segment.
    pub checksum: u64,
    /// The staged log is a hard link to a sealed segment that this replica
    /// held, so the transfer neither pulled nor rewrote its payload.
    pub linked: bool,
}

/// Memoized checksum state for one segment file.
///
/// Keyed per segment rather than per `(base offset, size)` pair so the ACTIVE
/// segment extends its own hasher as it grows, instead of missing the memo on
/// every byte it gained and re-reading from byte zero. Every path that plants a
/// new file at an existing base offset (install, converge) clears the whole
/// map, which is what makes "same base offset, longer file, same leading bytes"
/// hold.
///
/// Three paths fill it. The append feeds the bytes of every chunk it
/// materializes, so a segment that this process wrote from its first byte
/// needs no read. The install stamps each segment it installs with the
/// checksum it verified. The offer reads a file only for the bytes that no
/// memo covers, which after a restart is every segment of the first offer.
pub(crate) enum SegmentChecksumMemo {
    /// The hasher fed exactly `hashed_len` bytes of the file. A read or an
    /// append extends it.
    Hashing {
        hashed_len: u64,
        /// The stamp is NOT cached alongside: `StateArtifactHasher::finish`
        /// takes `&self`, so it is a read of this hasher, and a second copy
        /// is just a field that can drift. Boxed, because the hasher state is
        /// hundreds of bytes and every stamp in the map would pay that size.
        hasher: Box<StateArtifactHasher>,
    },
    /// The checksum of the first `len` bytes, as an install verified them or
    /// as the hasher of a sealed segment ended. Nothing can extend it, because
    /// it holds no hasher. An append drops it, and a read starts again from
    /// byte zero.
    Stamped { len: u64, checksum: u64 },
}

impl SegmentChecksumMemo {
    /// The artifact stamp over the first `len` bytes, if the memo ends there.
    pub(crate) fn checksum_of(&self, len: u64) -> Option<u64> {
        match self {
            Self::Hashing { hashed_len, hasher } => (*hashed_len == len).then(|| hasher.finish()),
            Self::Stamped {
                len: stamped_len,
                checksum,
            } => (*stamped_len == len).then_some(*checksum),
        }
    }
}

/// Verified staging files, keyed by segment start offset.
///
/// Partial scans retain their completed files across retries and advancing
/// offers. Each hit must match the offered length and checksum. Replacing a
/// staging file invalidates its entry; installs and convergence clear the map.
/// Live segments are verified and their indexes rebuilt before entering it.
pub(crate) struct ReuseScanMemo {
    adopted: BTreeMap<u64, StagedSegmentMeta>,
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
            checksum: entry.checksum,
            linked: false,
        }
    }
}

/// The consumer-offset artifact: the offsets of every kind at the offer's
/// `commit_op`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct ConsumerOffsetsWire {
    pub dedup_capacity: usize,
    pub required_metadata_frontier: u64,
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
    /// The op of the committed delete that left every offset kind empty, `0`
    /// when the origin knows of none. It lets a receiver that applied less
    /// than that op accept an empty table that a delete emptied. A donor whose
    /// boot lost offset files before that delete still sends it, as the
    /// marking rule in `IggyPartition::plan_committed_offset` notes.
    pub emptied_at: u64,
    /// `(consumer id, offset)`, ascending by id.
    pub consumers: Vec<(u32, u64)>,
    /// `(consumer group id, offset)`, ascending by id.
    pub groups: Vec<(u32, u64)>,
    /// `(consumer group id, offset)` of external groups, ascending by id.
    pub external_groups: Vec<(u32, u64)>,
    /// This group's dedup slice, ascending by client. Carried so a replica
    /// rejoining behind the repair floor can absorb a replay of what the group
    /// already committed instead of re-executing it.
    pub dedup: Vec<DedupWatermark>,
    pub owners: Vec<InstalledConsumerGroupOwner>,
    pub history_transition: Option<InstalledPartitionHistory>,
    pub owner_retirement:
        Option<iggy_binary_protocol::requests::partitions::RetireConsumerGroupOwnersRequest>,
}

impl ConsumerOffsetsWire {
    /// Encode: `magic | version u8 | next_offset u64 | emptied_at u64 |
    /// required_metadata_frontier u64 | dedup_capacity u32 |
    /// consumer_count u32 | group_count u32 | external_group_count u32 |
    /// dedup_count u32 | {id u32, offset u64}xN | {id u32, offset u64}xM |
    /// {id u32, offset u64}xE | {client u128, watermark u64, latest_commit u64,
    /// user_id u32, committed_window u128, session u64, reply_length u32,
    /// reply bytes}xD | owner_count u32 | {installation, partition_op u64}xO |
    /// history_present u8 | [history fence, partition_op u64] |
    /// retirement_length u32 | retirement bytes | checksum_present u8 |
    /// prepare_checksum u128 | prepare_length u32 | checkpoint_prepare bytes |
    /// XxHash3_64 trailer`. Little-endian throughout.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let sections = self.offset_sections();
        let reserved = self.encoded_size();
        let mut out = Vec::with_capacity(reserved);
        out.extend_from_slice(&CONSUMER_OFFSETS_MAGIC);
        out.push(CONSUMER_OFFSETS_VERSION);
        out.extend_from_slice(&self.next_offset.to_le_bytes());
        out.extend_from_slice(&self.emptied_at.to_le_bytes());
        out.extend_from_slice(&self.required_metadata_frontier.to_le_bytes());
        out.extend_from_slice(
            &u32::try_from(self.dedup_capacity)
                .expect("bounded retry capacity")
                .to_le_bytes(),
        );
        for entries in sections {
            #[allow(clippy::cast_possible_truncation)]
            out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        }
        #[allow(clippy::cast_possible_truncation)]
        out.extend_from_slice(&(self.dedup.len() as u32).to_le_bytes());
        for (id, offset) in sections.iter().copied().flatten() {
            out.extend_from_slice(&id.to_le_bytes());
            out.extend_from_slice(&offset.to_le_bytes());
        }
        for entry in &self.dedup {
            out.extend_from_slice(&entry.client.to_le_bytes());
            out.extend_from_slice(&entry.watermark.to_le_bytes());
            out.extend_from_slice(&entry.latest_commit.to_le_bytes());
            out.extend_from_slice(&entry.user_id.to_le_bytes());
            out.extend_from_slice(&entry.committed_window.to_le_bytes());
            out.extend_from_slice(&entry.session.to_le_bytes());
            out.extend_from_slice(
                &u32::try_from(entry.reply.len())
                    .expect("bounded partition receipt")
                    .to_le_bytes(),
            );
            out.extend_from_slice(&entry.reply);
        }
        out.extend_from_slice(
            &u32::try_from(self.owners.len())
                .expect("bounded ownership count")
                .to_le_bytes(),
        );
        let mut encoded_owner =
            bytes::BytesMut::with_capacity(InstallConsumerGroupOwnerRequest::ENCODED_SIZE);
        for installed in &self.owners {
            encoded_owner.clear();
            installed.installation.encode(&mut encoded_owner);
            out.extend_from_slice(&encoded_owner);
            out.extend_from_slice(&installed.partition_op.to_le_bytes());
        }
        out.push(u8::from(self.history_transition.is_some()));
        if let Some(installed) = self.history_transition {
            out.extend_from_slice(&installed.transition.to_bytes());
            out.extend_from_slice(&installed.partition_op.to_le_bytes());
        }
        let retirement_bytes = self
            .owner_retirement
            .as_ref()
            .map(WireEncode::to_bytes)
            .unwrap_or_default();
        out.extend_from_slice(
            &u32::try_from(retirement_bytes.len())
                .expect("bounded owner retirement")
                .to_le_bytes(),
        );
        out.extend_from_slice(&retirement_bytes);
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
    /// so the install clamps, mirroring boot recovery. External group values
    /// are never clamped.
    ///
    /// # Errors
    /// Any [`ConsumerOffsetsWireError`]; the input is never partially
    /// trusted.
    #[allow(clippy::too_many_lines)]
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
        let next_offset = cursor.u64()?;
        let emptied_at = cursor.u64()?;
        let required_metadata_frontier = cursor.u64()?;
        let dedup_capacity = cursor.u32()? as usize;
        let consumer_count = cursor.u32()?;
        let group_count = cursor.u32()?;
        let external_group_count = cursor.u32()?;
        let dedup_count = cursor.u32()?;
        if dedup_capacity > CLIENTS_TABLE_SLOT_MAX {
            return Err(ConsumerOffsetsWireError::InvalidCapacity);
        }
        if dedup_count as usize > dedup_capacity {
            return Err(ConsumerOffsetsWireError::TooManyEntries {
                section: "dedup",
                count: dedup_count,
                max: u32::try_from(dedup_capacity).unwrap_or(u32::MAX),
            });
        }
        let consumers = Self::decode_section(&mut cursor, "consumers", consumer_count)?;
        let groups = Self::decode_section(&mut cursor, "groups", group_count)?;
        let external_groups =
            Self::decode_section(&mut cursor, "external groups", external_group_count)?;
        let dedup = Self::decode_dedup_section(&mut cursor, dedup_count)?;
        let owners = Self::decode_ownership_section(&mut cursor, required_metadata_frontier)?;
        let history_transition = match cursor.u8()? {
            0 => None,
            1 => {
                let transition = TransitionPartitionHistoryRequest::decode_from(
                    cursor.take(TransitionPartitionHistoryRequest::ENCODED_SIZE)?,
                )
                .map_err(|_| ConsumerOffsetsWireError::InvalidOwnership)?;
                let partition_op = cursor.u64()?;
                if transition.metadata_op > required_metadata_frontier || partition_op == 0 {
                    return Err(ConsumerOffsetsWireError::InvalidOwnership);
                }
                Some(InstalledPartitionHistory {
                    transition,
                    partition_op,
                })
            }
            _ => return Err(ConsumerOffsetsWireError::InvalidOwnership),
        };
        let owner_retirement =
            Self::decode_retirement_section(&mut cursor, required_metadata_frontier)?;
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
            dedup_capacity,
            required_metadata_frontier,
            prepare_checksum,
            checkpoint_prepare,
            next_offset,
            emptied_at,
            consumers,
            groups,
            external_groups,
            dedup,
            owners,
            history_transition,
            owner_retirement,
        })
    }

    /// The offset section of each kind, in [`ConsumerKind::ALL`] order.
    pub(crate) fn offset_sections(&self) -> [&[(u32, u64)]; ConsumerKind::COUNT] {
        [&self.consumers, &self.groups, &self.external_groups]
    }

    fn encoded_size(&self) -> usize {
        CONSUMER_OFFSETS_MAGIC.len()
            + size_of::<u8>()
            + 3 * size_of::<u64>()
            + (ConsumerKind::COUNT + 2) * size_of::<u32>()
            + self
                .offset_sections()
                .iter()
                .map(|entries| entries.len())
                .sum::<usize>()
                * (size_of::<u32>() + size_of::<u64>())
            + self.dedup.len() * DEDUP_ENTRY_LEN
            + self
                .dedup
                .iter()
                .map(|entry| entry.reply.len())
                .sum::<usize>()
            + size_of::<u8>()
            + size_of::<u128>()
            + size_of::<u32>()
            + self.checkpoint_prepare.len()
            + size_of::<u32>()
            + self.owners.len() * OWNERSHIP_ENTRY_LEN
            + size_of::<u8>()
            + self.history_transition.map_or(0, |_| {
                TransitionPartitionHistoryRequest::ENCODED_SIZE + size_of::<u64>()
            })
            + size_of::<u32>()
            + self
                .owner_retirement
                .as_ref()
                .map_or(0, WireEncode::encoded_size)
            + size_of::<u64>()
    }

    fn decode_retirement_section(
        cursor: &mut LeCursor<'_>,
        metadata_frontier: u64,
    ) -> Result<Option<RetireConsumerGroupOwnersRequest>, ConsumerOffsetsWireError> {
        let retirement_length = cursor.u32()? as usize;
        if retirement_length == 0 {
            Ok(None)
        } else {
            let catalog =
                RetireConsumerGroupOwnersRequest::decode_from(cursor.take(retirement_length)?)
                    .map_err(|_| ConsumerOffsetsWireError::InvalidOwnership)?;
            if catalog.metadata_op > metadata_frontier {
                return Err(ConsumerOffsetsWireError::InvalidOwnership);
            }
            Ok(Some(catalog))
        }
    }

    fn decode_ownership_section(
        cursor: &mut LeCursor<'_>,
        metadata_frontier: u64,
    ) -> Result<Vec<InstalledConsumerGroupOwner>, ConsumerOffsetsWireError> {
        let count = cursor.u32()?;
        if count > CONSUMER_OFFSETS_ENTRIES_MAX {
            return Err(ConsumerOffsetsWireError::TooManyEntries {
                section: "owners",
                count,
                max: CONSUMER_OFFSETS_ENTRIES_MAX,
            });
        }
        if count as usize * OWNERSHIP_ENTRY_LEN > cursor.remaining().len() {
            return Err(ConsumerOffsetsWireError::Truncated);
        }
        let mut owners = Vec::with_capacity(count as usize);
        let mut previous = None;
        for _ in 0..count {
            let installation = InstallConsumerGroupOwnerRequest::decode_from(
                cursor.take(InstallConsumerGroupOwnerRequest::ENCODED_SIZE)?,
            )
            .map_err(|_| ConsumerOffsetsWireError::InvalidOwnership)?;
            let partition_op = cursor.u64()?;
            if partition_op == 0
                || installation.group_id > u64::from(u32::MAX)
                || previous.is_some_and(|id| id >= installation.group_id)
                || installation.metadata_op > metadata_frontier
            {
                return Err(ConsumerOffsetsWireError::InvalidOwnership);
            }
            previous = Some(installation.group_id);
            owners.push(InstalledConsumerGroupOwner {
                installation,
                partition_op,
            });
        }
        Ok(owners)
    }

    fn validate_owners(
        &self,
        incarnation: u64,
        through_op: u64,
    ) -> Result<(), ConsumerOffsetsWireError> {
        if self.history_transition.is_some_and(|installed| {
            installed.partition_op > through_op || installed.transition.incarnation != incarnation
        }) {
            return Err(ConsumerOffsetsWireError::InvalidOwnership);
        }
        // The delete fence is terminal, so every owner was installed before it.
        if self.owners.iter().any(|installed| {
            installed.partition_op > through_op
                || installed.installation.incarnation != incarnation
                || self
                    .history_transition
                    .is_some_and(|history| installed.partition_op >= history.partition_op)
        }) {
            return Err(ConsumerOffsetsWireError::InvalidOwnership);
        }
        if self.owner_retirement.as_ref().is_some_and(|catalog| {
            catalog.incarnation != incarnation
                || self.owners.iter().any(|installed| {
                    catalog.retires(installed.installation.group_id)
                        && !installed.installation.owner.is_unassigned()
                })
        }) {
            return Err(ConsumerOffsetsWireError::InvalidOwnership);
        }
        Ok(())
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
            let session = cursor.u64()?;
            let reply_length = cursor.u32()? as usize;
            if reply_length < size_of::<ReplyHeader>() || reply_length > PARTITION_RECEIPT_BYTES_MAX
            {
                return Err(ConsumerOffsetsWireError::InvalidReceipt);
            }
            let reply = cursor.take(reply_length)?.to_vec();
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
                session,
                reply,
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
        // megabytes across the offset sections. 12 is the wire stride below, and
        // the guard is a lower bound on purpose: other sections, the dedup
        // section and the prepare trail each one, so only "at least this many
        // bytes present" holds for every call. Demanding that `12 * count` be
        // all that remains would reject valid input.
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
    InvalidOwnership,
    InvalidCapacity,
    InvalidReceipt,
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
            Self::InvalidOwnership => write!(
                f,
                "invalid owner, history fence or retirement catalog in consumer-offsets artifact"
            ),
            Self::InvalidCapacity => write!(f, "invalid committed retry capacity"),
            Self::InvalidReceipt => {
                write!(f, "invalid partition receipt in consumer-offsets artifact")
            }
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
    use iggy_binary_protocol::primitives::partition_history::ConsumerGroupOwner;

    #[test]
    fn given_a_prefix_read_that_stops_without_a_result_should_land_an_error_and_wake() {
        let result = Rc::new(RefCell::new(None));
        let woken = Rc::new(Cell::new(false));
        let io = Rc::new(PartitionIoQuiescence::default());
        assert!(io.begin_local_work());
        let landing = Landing {
            io: Rc::clone(&io),
            result: Rc::clone(&result),
            wake: {
                let woken = Rc::clone(&woken);
                move || woken.set(true)
            },
            stopped: prefix_read_stopped,
        };

        drop(landing);

        assert!(
            matches!(result.borrow().as_ref(), Some(Err(_))),
            "the pull must see a failed read and fetch the whole artifact"
        );
        assert!(woken.get(), "the partition must resume the pull");
        assert!(
            io.is_interrupted(),
            "teardown must keep interrupted I/O fenced"
        );
    }

    #[test]
    fn given_segment_read_error_when_classifying_should_tell_local_faults_from_stale_offers() {
        const EIO: i32 = 5;

        assert!(matches!(
            SegmentLoadError::classify(std::io::Error::from_raw_os_error(EIO)),
            SegmentLoadError::LocalFault(_)
        ));
        assert!(matches!(
            SegmentLoadError::classify(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
            SegmentLoadError::LocalFault(_)
        ));
        assert!(matches!(
            SegmentLoadError::classify(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)),
            SegmentLoadError::Stale(_)
        ));
    }

    #[compio::test]
    async fn given_segment_read_error_when_loading_artifact_should_classify_the_os_error() {
        // The open succeeds on a directory, and the read then fails with EISDIR.
        let directory = tempfile::tempdir().expect("temporary directory");
        let entry = consensus::StateArtifact {
            kind: artifact_kind::SEGMENT_LOG,
            frontier: 0,
            len: 1,
            checksum: 0,
        };

        let error =
            load_verified_segment_artifact(directory.path().to_str().expect("UTF-8 path"), &entry)
                .await
                .expect_err("a directory holds no segment bytes");

        assert!(
            matches!(&error, SegmentLoadError::Stale(source) if source.raw_os_error().is_some()),
            "the read error must reach the classification with its OS code, got {error:?}"
        );
    }

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
            owner_retirement: None,
            owners: Vec::new(),
            history_transition: None,
            dedup_capacity: consensus::CLIENTS_TABLE_MAX,
            required_metadata_frontier: 0,
            prepare_checksum: None,
            checkpoint_prepare: Vec::new(),
            next_offset: 43,
            emptied_at: 0,
            consumers: vec![(1, 10), (7, 42)],
            groups: vec![(2, 5)],
            external_groups: Vec::new(),
            dedup: vec![
                dedup_entry(11, 4, 90),
                dedup_entry(usize::MAX as u128 + 5, 9, 91),
            ],
        }
    }

    fn dedup_entry(client: u128, watermark: u64, latest_commit: u64) -> DedupWatermark {
        let header = ReplyHeader {
            command: iggy_binary_protocol::Command::Reply,
            operation: Operation::StoreConsumerOffset,
            client,
            request: watermark,
            commit: latest_commit,
            size: u32::try_from(size_of::<ReplyHeader>()).unwrap(),
            ..Default::default()
        };
        DedupWatermark {
            session: 1,
            reply: bytemuck::bytes_of(&header).to_vec(),
            client,
            user_id: 1,
            watermark,
            latest_commit,
            committed_window: 0b1011,
        }
    }

    #[test]
    fn given_zero_frontier_transfer_when_capacity_is_uncommitted_should_keep_local_limit() {
        const LOCAL_CAPACITY: usize = 2;
        const FIRST_COMMITTED_CAPACITY: usize = 3;
        let wire = ConsumerOffsetsWire {
            dedup_capacity: 0,
            ..Default::default()
        };
        let decoded = ConsumerOffsetsWire::decode(&wire.encode()).unwrap();
        let mut restored = checked_retry_table(&decoded, 0, LOCAL_CAPACITY).unwrap();
        assert!(!restored.capacity_committed());
        assert_eq!(restored.capacity(), LOCAL_CAPACITY);
        restored.commit_capacity(FIRST_COMMITTED_CAPACITY).unwrap();
        assert_eq!(restored.capacity(), FIRST_COMMITTED_CAPACITY);
        assert!(checked_retry_table(&decoded, 1, LOCAL_CAPACITY).is_err());
        let with_receipt = ConsumerOffsetsWire {
            owner_retirement: None,
            dedup: vec![dedup_entry(1, 1, 1)],
            ..wire
        };
        assert!(ConsumerOffsetsWire::decode(&with_receipt.encode()).is_err());
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
    fn given_history_and_owner_evidence_when_restored_should_validate_order_and_identity() {
        const INCARNATION: u64 = 7;
        let mut wire = table();
        wire.required_metadata_frontier = 13;
        wire.history_transition = Some(InstalledPartitionHistory {
            transition: TransitionPartitionHistoryRequest {
                incarnation: INCARNATION,
                metadata_op: 12,
            },
            partition_op: 6,
        });
        wire.owners.push(InstalledConsumerGroupOwner {
            installation: InstallConsumerGroupOwnerRequest {
                incarnation: INCARNATION,
                group_id: 2,
                owner: iggy_binary_protocol::primitives::partition_history::ConsumerGroupOwner {
                    client_id: 11,
                    session: 4,
                    generation: 9,
                },
                metadata_op: 13,
            },
            partition_op: 5,
        });
        wire.owner_retirement = Some(
            iggy_binary_protocol::requests::partitions::RetireConsumerGroupOwnersRequest {
                incarnation: INCARNATION,
                metadata_op: 13,
                next_group_id: 4,
                live_group_ids: vec![2],
            },
        );
        let restored = ConsumerOffsetsWire::decode(&wire.encode()).unwrap();
        assert_eq!(wire, restored);
        assert!(restored.validate_owners(INCARNATION, 91).is_ok());
        assert_eq!(
            restored.validate_owners(INCARNATION + 1, 91),
            Err(ConsumerOffsetsWireError::InvalidOwnership)
        );
        assert_eq!(
            restored.validate_owners(INCARNATION, 5),
            Err(ConsumerOffsetsWireError::InvalidOwnership)
        );

        let mut invalid = wire.clone();
        invalid
            .owner_retirement
            .as_mut()
            .unwrap()
            .live_group_ids
            .clear();
        assert_eq!(
            invalid.validate_owners(INCARNATION, 91),
            Err(ConsumerOffsetsWireError::InvalidOwnership)
        );
        invalid = wire.clone();
        invalid.owner_retirement.as_mut().unwrap().metadata_op += 1;
        assert_eq!(
            ConsumerOffsetsWire::decode(&invalid.encode()),
            Err(ConsumerOffsetsWireError::InvalidOwnership)
        );
        // The delete fence is terminal, so no owner shares or follows its op.
        for partition_op in [6, 7] {
            invalid = wire.clone();
            invalid.owners[0].partition_op = partition_op;
            assert_eq!(
                invalid.validate_owners(INCARNATION, 91),
                Err(ConsumerOffsetsWireError::InvalidOwnership)
            );
        }
        invalid = wire.clone();
        invalid.owners[0].installation.incarnation += 1;
        assert_eq!(
            invalid.validate_owners(INCARNATION, 91),
            Err(ConsumerOffsetsWireError::InvalidOwnership)
        );
        invalid = wire.clone();
        invalid.owners.push(invalid.owners[0]);
        assert_eq!(
            ConsumerOffsetsWire::decode(&invalid.encode()),
            Err(ConsumerOffsetsWireError::InvalidOwnership)
        );
        invalid = wire;
        invalid.required_metadata_frontier = 11;
        assert_eq!(
            ConsumerOffsetsWire::decode(&invalid.encode()),
            Err(ConsumerOffsetsWireError::InvalidOwnership)
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
            owner_retirement: None,
            owners: Vec::new(),
            history_transition: None,
            dedup_capacity: consensus::CLIENTS_TABLE_MAX,
            required_metadata_frontier: 0,
            prepare_checksum: None,
            checkpoint_prepare: Vec::new(),
            next_offset: 0,
            emptied_at: 0,
            consumers: Vec::new(),
            groups: Vec::new(),
            external_groups: Vec::new(),
            dedup: Vec::new(),
        };
        let encoded = empty.encode();
        assert_eq!(
            ConsumerOffsetsWire::decode(&encoded).expect("round trip"),
            empty
        );
    }

    #[test]
    fn given_emptied_table_when_encoded_should_round_trip_the_emptied_op() {
        let emptied = ConsumerOffsetsWire {
            emptied_at: 9,
            consumers: Vec::new(),
            groups: Vec::new(),
            ..table()
        };
        assert_eq!(
            ConsumerOffsetsWire::decode(&emptied.encode()).expect("round trip"),
            emptied
        );
    }

    /// Version 7 had no emptied-at op after the next offset. Read in place, its
    /// later fields would land one word early.
    #[test]
    fn given_version_7_offsets_artifact_when_decoded_should_reject_it() {
        const RELEASED_VERSION: u8 = 7;
        const EMPTIED_AT_START: usize =
            CONSUMER_OFFSETS_MAGIC.len() + size_of::<u8>() + size_of::<u64>();
        let encoded = table().encode();
        let mut released = encoded[..encoded.len() - size_of::<u64>()].to_vec();
        released.drain(EMPTIED_AT_START..EMPTIED_AT_START + size_of::<u64>());
        released[CONSUMER_OFFSETS_MAGIC.len()] = RELEASED_VERSION;
        let trailer = state_artifact_checksum(&released);
        released.extend_from_slice(&trailer.to_le_bytes());

        assert_eq!(
            ConsumerOffsetsWire::decode(&released),
            Err(ConsumerOffsetsWireError::UnsupportedVersion {
                version: RELEASED_VERSION
            })
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
        for version in [CONSUMER_OFFSETS_VERSION - 1, CONSUMER_OFFSETS_VERSION + 1] {
            let mut wrong = table().encode();
            wrong[CONSUMER_OFFSETS_MAGIC.len()] = version;
            let content_len = wrong.len() - size_of::<u64>();
            let trailer = state_artifact_checksum(&wrong[..content_len]);
            wrong[content_len..].copy_from_slice(&trailer.to_le_bytes());
            assert_eq!(
                ConsumerOffsetsWire::decode(&wrong),
                Err(ConsumerOffsetsWireError::UnsupportedVersion { version })
            );
        }
    }

    #[test]
    fn given_entry_in_every_section_when_encoded_should_match_the_version_pins() {
        // Nothing else fails when a layout changes and its version bump is
        // forgotten, and a peer on another release then reads the artifact out
        // of place. One entry per section makes each length cover the header,
        // every count field and every entry stride. Changing a length is the
        // reminder to change its version.
        const WITHOUT_EXTERNAL_LEN: usize = 555;
        const WITH_EXTERNAL_LEN: usize = 567;

        const INCARNATION: u64 = 1;
        let with_external = ConsumerOffsetsWire {
            dedup_capacity: consensus::CLIENTS_TABLE_MAX,
            required_metadata_frontier: 3,
            prepare_checksum: Some(1),
            checkpoint_prepare: Vec::new(),
            next_offset: 0,
            emptied_at: 0,
            consumers: vec![(1, 0)],
            groups: vec![(1, 0)],
            external_groups: vec![(1, 0)],
            dedup: vec![dedup_entry(1, 0, 0)],
            owners: vec![InstalledConsumerGroupOwner {
                installation: InstallConsumerGroupOwnerRequest {
                    incarnation: INCARNATION,
                    group_id: 1,
                    owner: ConsumerGroupOwner {
                        client_id: 1,
                        session: 1,
                        generation: 1,
                    },
                    metadata_op: 2,
                },
                partition_op: 1,
            }],
            history_transition: Some(InstalledPartitionHistory {
                transition: TransitionPartitionHistoryRequest {
                    incarnation: INCARNATION,
                    metadata_op: 3,
                },
                partition_op: 2,
            }),
            owner_retirement: Some(RetireConsumerGroupOwnersRequest {
                incarnation: INCARNATION,
                metadata_op: 3,
                next_group_id: 2,
                live_group_ids: vec![1],
            }),
        };
        let without_external = ConsumerOffsetsWire {
            external_groups: Vec::new(),
            ..with_external.clone()
        };

        for (wire, len) in [
            (&without_external, WITHOUT_EXTERNAL_LEN),
            (&with_external, WITH_EXTERNAL_LEN),
        ] {
            let encoded = wire.encode();
            assert_eq!(
                (encoded[CONSUMER_OFFSETS_MAGIC.len()], encoded.len()),
                (CONSUMER_OFFSETS_VERSION, len),
                "a consumer-offsets layout changed; bump its version with it"
            );
        }
        assert_eq!(
            CONSUMER_OFFSETS_VERSION, 8,
            "a consumer-offsets version moved; confirm its layout moved with it"
        );
    }

    /// Legacy artifacts omit session-qualified receipts and cannot restore retry protection.
    #[test]
    fn given_server_0_9_0_artifact_when_decoded_should_reject() {
        let wire = ConsumerOffsetsWire {
            prepare_checksum: Some(u128::MAX - 7),
            checkpoint_prepare: vec![1, 2, 3],
            ..table()
        };
        let mut released = Vec::new();
        released.extend_from_slice(&CONSUMER_OFFSETS_MAGIC);
        released.push(1);
        // 0.9.0 wrote its purge generation here.
        released.extend_from_slice(&0u64.to_le_bytes());
        released.extend_from_slice(&wire.next_offset.to_le_bytes());
        for count in [wire.consumers.len(), wire.groups.len(), wire.dedup.len()] {
            released.extend_from_slice(&u32::try_from(count).unwrap().to_le_bytes());
        }
        for (id, offset) in wire.consumers.iter().chain(&wire.groups) {
            released.extend_from_slice(&id.to_le_bytes());
            released.extend_from_slice(&offset.to_le_bytes());
        }
        for entry in &wire.dedup {
            released.extend_from_slice(&entry.client.to_le_bytes());
            released.extend_from_slice(&entry.watermark.to_le_bytes());
            released.extend_from_slice(&entry.latest_commit.to_le_bytes());
            released.extend_from_slice(&entry.user_id.to_le_bytes());
            released.extend_from_slice(&entry.committed_window.to_le_bytes());
        }
        released.push(1);
        released.extend_from_slice(&wire.prepare_checksum.unwrap().to_le_bytes());
        released.extend_from_slice(
            &u32::try_from(wire.checkpoint_prepare.len())
                .unwrap()
                .to_le_bytes(),
        );
        released.extend_from_slice(&wire.checkpoint_prepare);
        let trailer = state_artifact_checksum(&released);
        released.extend_from_slice(&trailer.to_le_bytes());

        assert_eq!(
            ConsumerOffsetsWire::decode(&released),
            Err(ConsumerOffsetsWireError::UnsupportedVersion { version: 1 })
        );
    }

    #[test]
    fn given_external_groups_when_encoded_should_round_trip() {
        let wire = ConsumerOffsetsWire {
            external_groups: vec![(2, 0), (9, 1 << 40)],
            ..table()
        };

        assert_eq!(ConsumerOffsetsWire::decode(&wire.encode()), Ok(wire));
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
            owner_retirement: None,
            owners: Vec::new(),
            history_transition: None,
            dedup_capacity: consensus::CLIENTS_TABLE_MAX,
            required_metadata_frontier: 0,
            prepare_checksum: None,
            checkpoint_prepare: Vec::new(),
            next_offset: 0,
            emptied_at: 0,
            consumers: Vec::new(),
            groups: Vec::new(),
            external_groups: Vec::new(),
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
            owner_retirement: None,
            owners: Vec::new(),
            history_transition: None,
            dedup_capacity: consensus::CLIENTS_TABLE_MAX,
            required_metadata_frontier: 0,
            prepare_checksum: None,
            checkpoint_prepare: Vec::new(),
            next_offset: 0,
            emptied_at: 0,
            consumers: Vec::new(),
            groups: Vec::new(),
            external_groups: Vec::new(),
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
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&(u32::try_from(CLIENTS_TABLE_SLOT_MAX).unwrap()).to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
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
                max: u32::try_from(CLIENTS_TABLE_SLOT_MAX).unwrap(),
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
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&(u32::try_from(CLIENTS_TABLE_SLOT_MAX).unwrap()).to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
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
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&(u32::try_from(CLIENTS_TABLE_SLOT_MAX).unwrap()).to_le_bytes());
        bytes.extend_from_slice(&(CONSUMER_OFFSETS_ENTRIES_MAX + 1).to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
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
    fn given_owner_count_past_ceiling_when_decoded_should_reject_before_allocating() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&CONSUMER_OFFSETS_MAGIC);
        bytes.push(CONSUMER_OFFSETS_VERSION);
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&(u32::try_from(CLIENTS_TABLE_SLOT_MAX).unwrap()).to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&(CONSUMER_OFFSETS_ENTRIES_MAX + 1).to_le_bytes());
        let trailer = state_artifact_checksum(&bytes);
        bytes.extend_from_slice(&trailer.to_le_bytes());
        assert_eq!(
            ConsumerOffsetsWire::decode(&bytes),
            Err(ConsumerOffsetsWireError::TooManyEntries {
                section: "owners",
                count: CONSUMER_OFFSETS_ENTRIES_MAX + 1,
                max: CONSUMER_OFFSETS_ENTRIES_MAX,
            })
        );
    }

    #[test]
    fn given_duplicate_or_unordered_ids_when_decoded_should_reject() {
        let duplicate = ConsumerOffsetsWire {
            owner_retirement: None,
            owners: Vec::new(),
            history_transition: None,
            dedup_capacity: consensus::CLIENTS_TABLE_MAX,
            required_metadata_frontier: 0,
            prepare_checksum: None,
            checkpoint_prepare: Vec::new(),
            next_offset: 0,
            emptied_at: 0,
            consumers: vec![(5, 1), (5, 2)],
            groups: Vec::new(),
            external_groups: Vec::new(),
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
            owner_retirement: None,
            owners: Vec::new(),
            history_transition: None,
            dedup_capacity: consensus::CLIENTS_TABLE_MAX,
            required_metadata_frontier: 0,
            prepare_checksum: None,
            checkpoint_prepare: Vec::new(),
            next_offset: 0,
            emptied_at: 0,
            consumers: Vec::new(),
            groups: vec![(9, 1), (4, 2)],
            external_groups: Vec::new(),
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
    let mut walk = SegmentWalk::new(base_offset);
    walk.advance(bytes).await?;
    walk.finish()
}

/// A walk that stops at the end of the bytes it was given and resumes there,
/// so a seeded prefix and the suffix that completes it are each walked once.
pub(crate) struct SegmentWalk {
    base_offset: u64,
    /// Payload bytes walked so far.
    position: usize,
    next_offset: u64,
    stats: Option<SegmentWalkStats>,
    index_bytes: Vec<u8>,
    indexed_position: Option<usize>,
}

impl SegmentWalk {
    pub(crate) const fn new(base_offset: u64) -> Self {
        Self {
            base_offset,
            position: 0,
            next_offset: base_offset,
            stats: None,
            index_bytes: Vec::new(),
            indexed_position: None,
        }
    }

    /// Walk `bytes`, the payload from its first byte, from where the last
    /// call stopped to its end.
    async fn advance(&mut self, bytes: &[u8]) -> Result<(), SegmentWalkError> {
        let mut since_yield = 0usize;
        while self.position < bytes.len() {
            // The walk re-hashes every message (`decode_batch_slice` verifies
            // `batch_checksum`), so a multi-GiB artifact is a long CPU pass. On the
            // pump task, what these yields buy is NOT tick liveness: the consensus
            // tick is a sibling `select_biased!` arm of this same task and arms are
            // not polled while another arm's body awaits, so every group's tick and
            // heartbeat on this shard stay frozen for the duration either way (see
            // the tick-starvation TODO in `shard::router`). They buy the reactor:
            // detached tasks and io_uring completions make progress instead of
            // waiting out the whole pass. In a detached task, such as the prefix
            // seed read, the same yields let the pump run.
            if since_yield >= OFFER_HASH_CHUNK_LEN {
                since_yield = 0;
                yield_to_reactor().await;
            }
            let batch = decode_batch_slice(&bytes[self.position..]).map_err(|source| {
                SegmentWalkError::Batch {
                    position: self.position as u64,
                    source,
                }
            })?;
            let header = batch.header;
            if self.stats.is_none() && header.base_offset != self.base_offset {
                return Err(SegmentWalkError::BaseOffsetMismatch {
                    expected: self.base_offset,
                    actual: header.base_offset,
                });
            }
            if header.base_offset != self.next_offset {
                return Err(SegmentWalkError::NonContiguous {
                    expected: self.next_offset,
                    actual: header.base_offset,
                });
            }
            if header.message_count == 0 {
                return Err(SegmentWalkError::Batch {
                    position: self.position as u64,
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
                    position: self.position as u64,
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
            let stride_reached = self
                .indexed_position
                .is_none_or(|indexed| self.position.saturating_sub(indexed) >= INDEX_STRIDE_BYTES);
            if stride_reached {
                self.indexed_position = Some(self.position);
                self.index_bytes
                    .extend_from_slice(&header.base_offset.to_le_bytes());
                self.index_bytes.extend_from_slice(&timestamp.to_le_bytes());
                self.index_bytes
                    .extend_from_slice(&(self.position as u64).to_le_bytes());
            }
            self.stats = Some(self.stats.map_or(
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
            self.next_offset =
                batch_end
                    .checked_add(1)
                    .ok_or(SegmentWalkError::OffsetOverflow {
                        position: self.position as u64,
                    })?;
            // No trailing-bytes guard: the header decode floors `batch_length`
            // at the 256-byte command header (no zero-step loop is possible)
            // and `decode_batch_slice` already rejects a body shorter than
            // `total_size()`.
            self.position += header.total_size();
            since_yield += header.total_size();
        }
        Ok(())
    }

    fn finish(self) -> Result<(SegmentWalkStats, Vec<u8>), SegmentWalkError> {
        self.stats.map_or(Err(SegmentWalkError::Empty), |stats| {
            Ok((stats, self.index_bytes))
        })
    }
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
    /// `commit_min` when the offer's `TransferPlan` was pinned: the segments
    /// hold every op through it and none after it.
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

/// The state one offer describes, captured in the turn that found every
/// applied op in its segment and none after it. Later commits only append past
/// these lengths, so hash rounds that span them still describe `commit_op`.
pub(crate) struct TransferPlan {
    commit_op: u64,
    /// Each non-empty segment, in chain order.
    segments: Vec<PlannedSegment>,
    offsets: ConsumerOffsetsWire,
}

/// One segment as a plan captured it.
pub(crate) struct PlannedSegment {
    start_offset: u64,
    len: u64,
    log_path: String,
    /// The artifact stamp over `len` bytes once known. The pin takes it from
    /// the memo, or the round whose read completes the segment records it.
    /// It is kept here because appends after the pin move the memo of the
    /// active segment past `len`, so a later round cannot read it there.
    checksum: Cell<Option<u64>>,
}

/// How far a serving partition got toward the plan its offer rounds share.
pub(crate) enum TransferPlanSlot {
    /// A request waits for the flush that has to come before the capture.
    Wanted,
    Pinned(Rc<TransferPlan>),
    Failed(PartitionTransferUnavailable),
}

/// Why a partition cannot serve a state transfer right now.
///
/// Distinct variants because the operator responses differ: "not the
/// caught-up primary" is routine (requester retries elsewhere), an
/// unreadable segment is a local fault on THIS node.
#[derive(Debug)]
pub enum PartitionTransferUnavailable {
    NotCaughtUpPrimary,
    RepairTailExpired,
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
    /// exponential re-arm backoff behind it) only for the latter: a build still
    /// flushing or hashing is the common case under produce load, and charging
    /// it pins the backoff at its ceiling while nothing else recovers the
    /// partition.
    #[must_use]
    pub const fn transient(&self) -> bool {
        match self {
            Self::NotCaughtUpPrimary
            | Self::RepairInProgress
            | Self::NothingCommitted
            | Self::SegmentSetChanged
            | Self::RepairTailExpired
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

    /// Whether the next request resumes the build this round refused. Only
    /// these keep the shard's admission slot and the partition's plan.
    #[must_use]
    pub const fn resumable(&self) -> bool {
        matches!(self, Self::FlushPending | Self::OfferBuildInProgress { .. })
    }
}

impl fmt::Display for PartitionTransferUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotCaughtUpPrimary => write!(f, "not the caught-up primary of this group"),
            Self::RepairTailExpired => write!(f, "the snapshot's retained repair tail expired"),
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
/// commit floor advances.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionInstallOutcome {
    /// The consensus op the install applied. Named for what it holds: every
    /// other `frontier` in this module is a MESSAGE OFFSET
    /// (`VsrState::offset_frontier`, `StateArtifact::frontier`,
    /// `installed_frontier`), and op-vs-offset confusion is what produced this
    /// PR's durability defects.
    pub applied_commit_op: u64,
}

/// Failure installing a transferred partition state.
///
/// Validation failures mutate nothing. Pre-swap offset failures can leave
/// ignored replacement siblings when best-effort cleanup also fails, but never
/// alter live files.
#[derive(Debug)]
pub enum PartitionInstallError {
    ShuttingDown,
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
    /// The install wipes the journal, which holds an op above `commit_op` that
    /// this replica may have acknowledged: one at or below `held_max`, its
    /// accepted WAL head, or the known commit without a WAL.
    OfferErasesHeldOps {
        commit_op: u64,
        held_max: u64,
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
    /// The offer holds no segment, no message offset and no consumer offset,
    /// while this replica holds committed consumer offsets, so installing it
    /// would unlink them. Such an offer can come from a primary that lost its
    /// directory, and a receiver that only holds external group offsets has
    /// no message offset for [`Self::OfferRewindsDurableData`] to protect.
    /// An offer whose table a committed delete emptied after this replica's
    /// last applied op is installed instead.
    OfferErasesDurableOffsets {
        local_offsets: usize,
    },
    /// Consumer-offset staging failed before the segment swap, or finalizing a
    /// staged offset failed during it.
    OffsetPersistence {
        path: String,
        source: iggy_common::IggyError,
    },
    Offsets(ConsumerOffsetsWireError),
    RetryProtection(consensus::ClientTableWireError),
    /// Duplicate base offset in the staged set.
    DuplicateSegment {
        start_offset: u64,
    },
    /// A hole between consecutive staged segments.
    SegmentSetHole {
        previous_end: u64,
        next_start: u64,
    },
    /// A writer sync or backup preparation failure returns before the swap;
    /// the old in-memory partition remains in place and may be fenced.
    /// At/after the swap, disk holds a contiguous prefix of the new state
    /// and a crash-restart recovers it (see the module crash-window notes).
    /// On the storeless post-swap failure path, the IN-MEMORY partition is
    /// converged to an empty, honestly-lagging state before this returns,
    /// so the live process stays serviceable and the normal triggers re-transfer.
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
        /// value, which can lag the group's. The fence records this instead of
        /// the stale counter.
        frontier: u64,
    },
}

impl fmt::Display for PartitionInstallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ShuttingDown => write!(f, "partition is shutting down"),
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
            Self::OfferErasesHeldOps {
                commit_op,
                held_max,
            } => write!(
                f,
                "transfer commit op {commit_op} would erase held ops up to {held_max} that \
                 this replica may have acknowledged"
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
            Self::OfferErasesDurableOffsets { local_offsets } => write!(
                f,
                "offer holds no data while this replica holds {local_offsets} committed \
                 consumer offsets; installing it would unlink them"
            ),
            Self::OffsetPersistence { path, source } => {
                write!(f, "consumer offset persistence failed at {path}: {source}")
            }
            Self::Offsets(source) => write!(f, "consumer-offsets artifact rejected: {source}"),
            Self::RetryProtection(source) => write!(f, "retry protection rejected: {source}"),
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

/// Extension of the scratch name that a link takes before its rename to the
/// staging name. It ends in `STAGING_SUFFIX`, so the staging sweeps and the
/// boot sweep remove a scratch name that a crash or a failed unlink left.
const LINK_SCRATCH_EXTENSION: &str = "link.staging";

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
/// Boot recovery must refuse an incomplete listing before changing segments.
pub(crate) async fn segment_dir_entries(partition_dir: &str) -> std::io::Result<Vec<PathBuf>> {
    let partition_dir = partition_dir.to_owned();
    server_common::fs_utils::run_blocking("iggy-segment-scan", move || {
        std::fs::read_dir(partition_dir)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect()
    })
    .await
}

/// Remove physical tails outside the logical segment list after draining the WAL.
/// The owner has settled its writers and published an install backup.
pub(crate) async fn remove_public_segment_files(partition_dir: &str) -> std::io::Result<()> {
    let directory = PathBuf::from(partition_dir);
    run_blocking("iggy-segment-sweep", move || {
        remove_public_segment_files_blocking(&directory)
    })
    .await
}

fn remove_public_segment_files_blocking(directory: &Path) -> std::io::Result<()> {
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
            match std::fs::remove_file(&name) {
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
    write_revision_record(directory, MATERIALIZATION_MISSING, revision).await
}

/// # Errors
/// Returns an error if the recovery fence cannot be read or validated.
pub async fn materialization_is_missing(directory: &str, revision: u64) -> std::io::Result<bool> {
    Ok(read_revision_record(directory, MATERIALIZATION_MISSING).await? == Some(revision))
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
/// Callers supply `wal_revision` to move the refused prepare WAL, once its
/// writers are closed (boot recovery, or a live quarantine after its drain).
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
    let source = partition_dir.to_owned();
    let target = run_blocking("iggy-segment-quarantine", move || {
        for entry in std::fs::read_dir(source)? {
            let path = entry?.path();
            let quarantined = path.to_str().is_some_and(|path| {
                [".log", ".index", STAGING_SUFFIX, ANCHOR_SUFFIX]
                    .iter()
                    .any(|suffix| path.ends_with(suffix))
            });
            if quarantined && let Some(name) = path.file_name() {
                std::fs::rename(&path, Path::new(&target).join(name))?;
            }
        }
        Ok(target)
    })
    .await?;
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

/// Move the consumer offset files of `partition_dir` into the `fenced` copy
/// [`quarantine_partition_files`] created, so a rebuild starts without the
/// offsets the diverged history produced.
///
/// # Errors
/// The underlying `std::io::Error`, with the same operator-facing semantics as
/// [`quarantine_partition_files`].
pub async fn quarantine_offsets(partition_dir: &str, fenced: &str) -> std::io::Result<()> {
    match compio::fs::rename(
        &Path::new(partition_dir).join(OFFSETS_DIR),
        &Path::new(fenced).join(OFFSETS_DIR),
    )
    .await
    {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    }
    fsync_dir(fenced).await?;
    fsync_dir(partition_dir).await
}

/// Unlink every staging file in `partition_dir` except `keep`.
///
/// Best-effort disk hygiene shared by the reuse scan and the install: a file
/// that survives is swept at the next boot, so a failed unlink is not worth
/// failing either caller for. The CONVERGE sweep is deliberately not this
/// function -- it deletes the live chain as well and must propagate its
/// errors.
/// Do NOT widen this predicate to the quarantine's four-suffix list if the two
/// are ever unified: the keep-lists callers pass hold staging paths only, so a
/// wider filter would unlink every live `.log` and `.index` on the partition --
/// worst at the reuse scan, which runs at descriptor-accept on a serving
/// partition.
pub(crate) async fn sweep_staging_except(partition_dir: &str, keep: HashSet<PathBuf>) {
    let partition_dir = partition_dir.to_owned();
    let _ = run_blocking("iggy-staging-sweep", move || {
        for entry in std::fs::read_dir(partition_dir)? {
            let path = entry?.path();
            let is_staging = path
                .to_str()
                .is_some_and(|path| path.ends_with(STAGING_SUFFIX));
            if is_staging && !keep.contains(&path) {
                let _ = std::fs::remove_file(path);
            }
        }
        Ok(())
    })
    .await;
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
    /// The emptied-at op the installed table keeps, `0` for none.
    emptied_at: u64,
    planned_offsets: Vec<PlannedOffsetWrite>,
    next_offset: u64,
    phase: InstallPhase,
    drain: Option<crate::PersistenceDrain>,
    synced_files: BTreeSet<PathBuf>,
    directory_handle: Option<compio::fs::File>,
    offset_dirs_changed: [bool; ConsumerKind::COUNT],
    /// Published with the offsets, replacing the local receipt table.
    retry_table: Option<ClientTable>,
    /// The receipt file the WAL names once it restarts at `commit_op`.
    retry_checkpoint: Vec<u8>,
    failure: Option<PartitionInstallError>,
    disposition: InstallDisposition,
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
    OldOffsets(Option<ConsumerKind>),
    CommitOffsets(usize),
    OffsetDirectories,
    Publish,
    RetryCheckpoint,
    WalReset,
    ResetDraining,
    WalCertify,
    CertifyDraining,
    ReclaimRetry,
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
    OldOffsets(ConsumerKind),
    EmptiedMarker,
    CommitOffsets { start: usize, end: usize },
    OffsetDirectories,
    RetryCheckpoint,
    ReclaimRetry,
    FinishBackup,
    ClearMissing,
    DiscardOffsets(usize),
    Converge,
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
        .await
        .note_descriptor_exhaustion(|| format!("opening directory {partition_dir}"))?
        .sync_all()
        .await
}

pub(crate) struct ServedRepairTail {
    nonce: u128,
    lease: Option<RepairTailLease>,
    last_active_tick: u64,
}

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// Pin before building the manifest: flushing and hashing can span several
    /// rounds while writes keep evicting the suffix the receiver will need.
    pub fn retain_transfer_tail(&self, peer: u8, nonce: u128, tick: u64) -> bool {
        let Some(memory) = &self.proof_walk_memory else {
            return true;
        };
        let mut tails = self.served_repair_tails.borrow_mut();
        if let Some(tail) = tails.get_mut(&peer).filter(|tail| tail.nonce == nonce) {
            tail.last_active_tick = tick;
            return tail.lease.as_ref().is_some_and(RepairTailLease::is_valid);
        }
        let journal = &self.log.journal().inner;
        let from_op = journal
            .repair_retained_from()
            .unwrap_or_else(|| self.consensus().commit_min().saturating_add(1));
        // The replacement must pin before the previous lease drops.
        let lease = journal.retain_repair_from(from_op, memory.sibling());
        tails.insert(
            peer,
            ServedRepairTail {
                nonce,
                lease: Some(lease),
                last_active_tick: tick,
            },
        );
        true
    }

    /// A descriptor is usable only while its complete post-snapshot suffix is
    /// still retained. An exhausted or expired lease needs a new nonce.
    pub fn transfer_tail_available(&self, peer: u8, nonce: u128, commit_op: u64) -> bool {
        if self.proof_walk_memory.is_none() {
            return true;
        }
        let tails = self.served_repair_tails.borrow();
        let Some(lease) = tails
            .get(&peer)
            .filter(|tail| tail.nonce == nonce)
            .and_then(|tail| tail.lease.as_ref())
            .filter(|lease| lease.is_valid())
        else {
            return false;
        };
        let from_op = commit_op.saturating_add(1);
        if self
            .log
            .journal()
            .inner
            .repair_retained_from()
            .is_some_and(|oldest| oldest > from_op)
        {
            return false;
        }
        lease.advance(from_op);
        true
    }

    pub fn touch_transfer_tail(&self, peer: u8, nonce: u128, tick: u64) -> bool {
        if self.proof_walk_memory.is_none() {
            return true;
        }
        let mut tails = self.served_repair_tails.borrow_mut();
        let Some(tail) = tails.get_mut(&peer).filter(|tail| tail.nonce == nonce) else {
            return false;
        };
        tail.last_active_tick = tick;
        tail.lease.as_ref().is_some_and(RepairTailLease::is_valid)
    }

    /// Pull repair can precede a view-fenced progress reply. Its requests keep
    /// the suffix alive without claiming that any prefix has been proven.
    pub fn renew_transfer_tail_for_repair(&self, peer: u8, tick: u64) {
        if let Some(tail) = self.served_repair_tails.borrow_mut().get_mut(&peer) {
            tail.last_active_tick = tick;
        }
    }

    /// Only a correlated replication reply can release the acknowledged prefix.
    /// Busy replies renew the lease while the receiver validates and installs.
    pub fn advance_transfer_tail(
        &self,
        peer: u8,
        progress: consensus::ReplicationProgress,
        tick: u64,
    ) {
        if let Some(tail) = self.served_repair_tails.borrow_mut().get_mut(&peer) {
            tail.last_active_tick = tick;
            if let Some(lease) = &tail.lease
                && let consensus::ReplicationProgress::Ready { received_op } = progress
            {
                lease.advance(received_op.saturating_add(1));
            }
        }
    }

    pub fn expire_transfer_tails(&self, tick: u64, idle_ticks_max: u64) {
        for tail in self.served_repair_tails.borrow_mut().values_mut() {
            if tick.saturating_sub(tail.last_active_tick) >= idle_ticks_max {
                tail.lease = None;
            }
        }
    }

    fn plan_transfer_offset_writes(
        &self,
        offsets_wire: &ConsumerOffsetsWire,
        next_offset: u64,
    ) -> Result<Vec<PlannedOffsetWrite>, PartitionInstallError> {
        let sections = offsets_wire.offset_sections();
        let mut planned =
            Vec::with_capacity(sections.iter().map(|entries| entries.len()).sum::<usize>());
        for (kind, offsets) in ConsumerKind::ALL.into_iter().zip(sections) {
            let dir = self
                .consumer_offsets_dir(kind)
                .ok_or(PartitionInstallError::NoOffsetDir { kind })?;
            // An external group's value belongs to the external system, so it is never clamped.
            let clamp = kind != ConsumerKind::ExternalGroup;
            planned.extend(offsets.iter().filter_map(|&(id, offset)| {
                let value = if clamp {
                    offset.min(next_offset.checked_sub(1)?)
                } else {
                    offset
                };
                Some(PlannedOffsetWrite {
                    kind,
                    id,
                    path: format!("{dir}/{id}"),
                    value,
                })
            }));
        }
        Ok(planned)
    }

    /// Build (or serve from cache) this group's state-transfer offer.
    ///
    /// Hashes against a `TransferPlan` pinned in the turn that found every
    /// applied op in its segment, so the segments cover every committed
    /// `SendMessages` op through the plan's `commit_op`, the offset table
    /// covers every committed offset op, and commits that land between rounds
    /// cannot move the state the artifacts represent. Segment bytes are NOT
    /// loaded here: the offer records `(entry, path)` and the serving side
    /// loads one artifact at a time, so building costs one streaming checksum
    /// pass per segment and the resident footprint is the offsets artifact,
    /// including the checkpoint prepare.
    ///
    /// # Errors
    /// [`PartitionTransferUnavailable`]; the requester falls back to journal
    /// repair or retries after the next trigger.
    pub async fn state_transfer_offer(
        &mut self,
        config: &PartitionsConfig,
    ) -> Result<Rc<PartitionStateTransferOffer>, PartitionTransferUnavailable> {
        let offer = self.build_state_transfer_offer(config).await;
        if !offer
            .as_ref()
            .is_err_and(PartitionTransferUnavailable::resumable)
        {
            self.transfer_plan.borrow_mut().take();
        }
        offer
    }

    /// Whether this replica serves its group's state transfer, which only the
    /// primary does. An I/O owner pins the plan in a turn where the segments
    /// end at `commit_min`, so a primary still applying what it committed can
    /// serve. The inline flush runs through `commit_max` and needs the two
    /// equal.
    #[must_use]
    pub fn serves_state_transfer(&self) -> bool {
        if self.has_io_dispatcher() {
            consensus::is_settled_primary(self.consensus())
        } else {
            consensus::is_caught_up_primary(self.consensus())
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn build_state_transfer_offer(
        &mut self,
        config: &PartitionsConfig,
    ) -> Result<Rc<PartitionStateTransferOffer>, PartitionTransferUnavailable> {
        if !self.serves_state_transfer() {
            return Err(PartitionTransferUnavailable::NotCaughtUpPrimary);
        }
        if self.partition_dir.is_none() {
            // Also defuses the in-memory trap where `segment.size` grows with
            // no bytes on disk ("simulated in-memory batch persistence").
            // TODO: simulator cluster partitions have no directory, so the
            // simulator never serves or installs a partition state transfer.
            // A simulated replica that needs one stays behind until the
            // simulator models partition state transfer.
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
        if let Some(cached) = self.transfer_offer_cache.borrow().as_ref()
            && cached.commit_op == self.consensus().commit_min()
        {
            // Returns BEFORE the chain re-validation below, deliberately:
            // re-validating on every hit is the walk the cache exists to skip.
            // Retention GC on an idle partition therefore costs one wasted
            // round -- the chunk serve fails `Stale` and the eviction path
            // re-enumerates -- which is the cheaper side of the trade.
            return Ok(Rc::clone(cached));
        }
        let Some(plan) = self.request_transfer_plan(config).await? else {
            return Err(PartitionTransferUnavailable::FlushPending);
        };
        let planned = plan.segments.as_slice();
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
        // is the primary of those groups.
        //
        // Bounded per round instead. The memo carries partial progress, so a
        // refusal here is not lost work: the requester re-asks on its flat
        // transient interval and each round advances the pass by the budget
        // until the offer completes.
        let mut budget = OFFER_HASH_BUDGET_PER_ROUND_BYTES;
        let mut segments = Vec::with_capacity(planned.len());
        for segment in planned {
            let checksum = if let Some(checksum) = segment.checksum.get() {
                checksum
            } else {
                let Some(checksum) = self
                    .segment_checksum(
                        segment.start_offset,
                        segment.len,
                        &segment.log_path,
                        &mut budget,
                    )
                    .await?
                else {
                    // CUMULATIVE across rounds, read back off the memo:
                    // per-round figures are constant by construction (a
                    // partial round always spends exactly the budget and
                    // always stops inside one segment), so they render
                    // identically on round 1 and round 30 and an operator
                    // cannot tell a converging pass from a wedged one.
                    // This is the only window onto a multi-round build.
                    let hashed = self.hashed_prefix_len(planned);
                    let total = planned.iter().map(|segment| segment.len).sum::<u64>();
                    return Err(PartitionTransferUnavailable::OfferBuildInProgress {
                        hashed,
                        remaining: total.saturating_sub(hashed),
                    });
                };
                segment.checksum.set(Some(checksum));
                checksum
            };
            segments.push(SegmentArtifactSource {
                entry: consensus::StateArtifact {
                    kind: artifact_kind::SEGMENT_LOG,
                    frontier: segment.start_offset,
                    len: segment.len,
                    checksum,
                },
                log_path: segment.log_path.clone(),
            });
        }

        // Re-validate the chain under the lock: the plan outlives the rounds
        // that hash it, so GC could have unlinked a sealed segment or a
        // replaced file could stand at a planned path. Every stamp would then
        // describe bytes the offer no longer addresses, so refuse and let the
        // requester ask again against the chain that exists now.
        {
            let write_lock = self.write_lock.clone();
            let _guard = write_lock.lock().await;
            let live: std::collections::HashMap<u64, u64> = self
                .log
                .segments()
                .iter()
                .map(|segment| (segment.start_offset, segment.size.as_bytes_u64()))
                .collect();
            // Append-only within a segment instance, so a live size BELOW the
            // planned one means the file was replaced rather than extended.
            let changed = planned.iter().any(|segment| {
                live.get(&segment.start_offset)
                    .is_none_or(|live_size| *live_size < segment.len)
            });
            if changed {
                return Err(PartitionTransferUnavailable::SegmentSetChanged);
            }
        }

        // A completed hash can outlive the repair tail its snapshot needs. A
        // tail that only segments still hold counts as gone, because repair
        // from disk is slow. Re-pin while keeping the memos, so only newly
        // appended bytes are hashed.
        if self.consensus().commit_min() > plan.commit_op
            && self
                .log
                .journal()
                .inner
                .memory_retained_from()
                .is_none_or(|retained_from| retained_from > plan.commit_op.saturating_add(1))
        {
            *self.transfer_plan.borrow_mut() = Some(TransferPlanSlot::Wanted);
            self.notify_io();
            return Err(PartitionTransferUnavailable::FlushPending);
        }

        // An empty chain at frontier 0 is real state: `complete_repair` lets a
        // replica with no committed message jump its commit floor only past
        // ops proven to hold none, so this primary holds every message the
        // group committed. A receiver that holds data refuses an offer that
        // would erase it.
        let offsets_bytes = Rc::new(plan.offsets.encode());
        let offsets_entry = consensus::StateArtifact::for_bytes(
            artifact_kind::CONSUMER_OFFSETS,
            plan.commit_op,
            &offsets_bytes,
        );
        let offer = Rc::new(PartitionStateTransferOffer {
            commit_op: plan.commit_op,
            segments,
            offsets: (offsets_entry, offsets_bytes),
        });
        *self.transfer_offer_cache.borrow_mut() = Some(Rc::clone(&offer));
        Ok(offer)
    }

    /// The caller guarantees that the segments hold every applied op and none
    /// after it, which makes `commit_min` the op the captured state is at.
    pub(crate) fn capture_transfer_plan(
        &self,
    ) -> Result<TransferPlan, PartitionTransferUnavailable> {
        let memos = self.segment_checksum_cache.borrow();
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
            segments.push(PlannedSegment {
                start_offset: segment.start_offset,
                len: size,
                log_path,
                checksum: Cell::new(
                    memos
                        .get(&segment.start_offset)
                        .and_then(|memo| memo.checksum_of(size)),
                ),
            });
        }
        Ok(TransferPlan {
            commit_op: self.consensus().commit_min(),
            segments,
            offsets: self.offsets_wire_snapshot()?,
        })
    }

    /// Bytes of `planned` already hashed, clamped per segment to the planned
    /// length so a memo carrying an active segment's later growth cannot
    /// report more than this offer will hash.
    fn hashed_prefix_len(&self, planned: &[PlannedSegment]) -> u64 {
        let memos = self.segment_checksum_cache.borrow();
        planned
            .iter()
            .map(|segment| {
                if segment.checksum.get().is_some() {
                    return segment.len;
                }
                match memos.get(&segment.start_offset) {
                    Some(SegmentChecksumMemo::Hashing { hashed_len, .. }) => {
                        (*hashed_len).min(segment.len)
                    }
                    Some(SegmentChecksumMemo::Stamped { .. }) | None => 0,
                }
            })
            .sum()
    }

    /// Move the append's hasher into its file job. A shorter memo belongs to
    /// an offer read and stays available for that read to finish.
    pub(crate) fn take_segment_hasher(&self, chunk_start: u64) -> Option<Box<StateArtifactHasher>> {
        if self.consensus().replica_count() == 1 {
            return None;
        }
        self.partition_dir.as_ref()?;
        let start_offset = self.log.active_segment().start_offset;
        let mut memos = self.segment_checksum_cache.borrow_mut();
        match (chunk_start, memos.remove(&start_offset)) {
            (0, _) => Some(Box::default()),
            (_, Some(SegmentChecksumMemo::Hashing { hashed_len, hasher }))
                if hashed_len == chunk_start =>
            {
                Some(hasher)
            }
            (_, Some(memo @ SegmentChecksumMemo::Hashing { hashed_len, .. }))
                if hashed_len < chunk_start =>
            {
                memos.insert(start_offset, memo);
                None
            }
            _ => None,
        }
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
        let (hashed_len, mut hasher) = match memo {
            // `<=`, so the already-hashed case falls through to the shared tail:
            // `hash_segment_range` returns before opening the file when
            // `from == to`, and the finish + reinsert below is the same work the
            // separate arm did.
            Some(SegmentChecksumMemo::Hashing { hashed_len, hasher }) if hashed_len <= size => {
                (hashed_len, hasher)
            }
            // A stamp holds no hasher to resume, so the read starts at byte 0.
            Some(SegmentChecksumMemo::Stamped { .. }) => (0, Box::default()),
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
                (0, Box::default())
            }
        };
        // Clamped to the round's remaining budget, so a single multi-GiB
        // segment is split across rounds rather than being the granularity
        // floor. `finish` does not consume the hasher, so a partial pass is
        // simply a memo nobody stamps yet.
        let target = size.min(hashed_len.saturating_add(*budget));
        let spent = target.saturating_sub(hashed_len);
        // Dropped on failure, not reinserted: the hasher is fed chunk by chunk
        // and a mid-range error leaves it holding bytes `hashed_len` does not
        // account for, so resuming from it would stamp a checksum over a
        // doubly-fed prefix. Losing the partial pass is the cheap side.
        hash_segment_range(log_path, hashed_len, target, &mut hasher, None)
            .await
            .map_err(|source| PartitionTransferUnavailable::SegmentUnreadable {
                start_offset,
                source,
            })?;
        let checksum = (target == size).then(|| hasher.finish());
        self.segment_checksum_cache.borrow_mut().insert(
            start_offset,
            SegmentChecksumMemo::Hashing {
                hashed_len: target,
                hasher,
            },
        );
        self.stamp_sealed_segment(start_offset);
        *budget = budget.saturating_sub(spent);
        Ok(checksum)
    }

    /// Replace the hasher of a sealed segment with its stamp once the memo
    /// covers every byte. Nothing appends to a sealed segment, so its hasher
    /// can never take another byte, and the stamp is 16 bytes where the
    /// hasher is hundreds in two heap blocks.
    pub(crate) fn stamp_sealed_segment(&self, start_offset: u64) {
        let segments = self.log.segments();
        let Ok(position) =
            segments.binary_search_by_key(&start_offset, |segment| segment.start_offset)
        else {
            return;
        };
        let segment = &segments[position];
        let len = segment.size.as_bytes_u64();
        let mut memos = self.segment_checksum_cache.borrow_mut();
        if segment.sealed
            && let Some(memo) = memos.get_mut(&start_offset)
            && let Some(checksum) = memo.checksum_of(len)
        {
            *memo = SegmentChecksumMemo::Stamped { len, checksum };
        }
    }

    /// Release the cached offer once no requester holds one (the shard's
    /// offer-expiry sweep), and the plan of a build in progress with it: both
    /// name segments that retention may be about to unlink.
    pub fn clear_state_transfer_offer_cache(&self) {
        self.transfer_offer_cache.borrow_mut().take();
        self.transfer_plan.borrow_mut().take();
    }

    fn validate_consumer_offset_transfer_counts(&self) -> Result<(), PartitionTransferUnavailable> {
        for kind in ConsumerKind::ALL {
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
        // Durable state is the only store of external offsets, so no live map can disagree.
        let external_groups = self.snapshot_offset_kind(ConsumerKind::ExternalGroup, |_| true)?;
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
            required_metadata_frontier: self.required_metadata_frontier,
            dedup_capacity: if self.dedup().capacity_committed() {
                self.dedup().capacity()
            } else {
                0
            },
            checkpoint_prepare,
            prepare_checksum,
            next_offset,
            emptied_at: self.durable_consumer_offsets.emptied_at(),
            consumers,
            groups,
            external_groups,
            dedup,
            owners: self.consumer_group_owners.values().copied().collect(),
            history_transition: self.history_transition,
            owner_retirement: self.owner_retirement.clone(),
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

    /// Validate and sync a completed segment without holding the shard pump.
    /// Staging writers are serialized, including work of abandoned sessions.
    pub fn begin_transfer_segment_spill(
        &mut self,
        entry: &consensus::StateArtifact,
        bytes: Vec<u8>,
        prefix: Option<SeededPrefix>,
    ) -> LocalWork<Result<SpilledSegment, SpillError>> {
        let job = self.plan_segment_spill(entry, bytes, prefix);
        self.spawn_staging_work(segment_spill_stopped, move |cancelled| async move {
            job?.run(&cancelled).await
        })
    }

    /// Publish only the result owned by the current transfer session.
    pub fn accept_transfer_segment_spill(&self, spilled: SpilledSegment) -> StagedSegmentMeta {
        let SpilledSegment { meta, hasher } = spilled;
        *self.spilled_hasher.borrow_mut() = Some(hasher);
        self.reuse_scan_memo
            .borrow_mut()
            .get_or_insert_with(|| ReuseScanMemo {
                adopted: BTreeMap::new(),
            })
            .adopted
            .insert(meta.start_offset, meta.clone());
        meta
    }

    fn plan_segment_spill(
        &self,
        entry: &consensus::StateArtifact,
        bytes: Vec<u8>,
        prefix: Option<SeededPrefix>,
    ) -> Result<SegmentSpillJob, SpillError> {
        let partition_dir = self
            .partition_dir
            .clone()
            .ok_or(SpillError::NoPartitionDir)?;
        // A replacement must invalidate the proof before any writer starts.
        if let Some(memo) = self.reuse_scan_memo.borrow_mut().as_mut() {
            memo.adopted.remove(&entry.frontier);
        }
        Ok(SegmentSpillJob {
            partition_dir,
            entry: *entry,
            bytes,
            prefix,
        })
    }

    #[cfg(test)]
    pub(crate) async fn spill_transfer_segment(
        &self,
        entry: &consensus::StateArtifact,
        bytes: Vec<u8>,
        prefix: Option<SeededPrefix>,
    ) -> Result<StagedSegmentMeta, SpillError> {
        let spilled = self
            .plan_segment_spill(entry, bytes, prefix)?
            .run(&Cell::new(false))
            .await?;
        Ok(self.accept_transfer_segment_spill(spilled))
    }

    /// Start the reuse scan of `manifest` in a detached task, so that its
    /// links, syncs and reads of staged copies do not hold the shard pump.
    ///
    /// The scan stages every segment of the manifest that this replica can
    /// supply without a pull: a sealed live segment that holds the bytes of
    /// an entry, through hard links, or else the staged copy of an earlier
    /// attempt. It sweeps the staging strays that match no entry. Scans and
    /// spills wait for the preceding writer, so an abandoned task cannot race
    /// the next session on the same staging names.
    pub fn begin_reuse_scan(
        &mut self,
        manifest: &[consensus::StateArtifact],
    ) -> LocalWork<ReuseScanned> {
        let job = self.plan_reuse_scan(manifest);
        self.spawn_staging_work(ReuseScanned::default, move |cancelled| async move {
            match job {
                Some(job) => job.run(&cancelled).await,
                None => ReuseScanned::default(),
            }
        })
    }

    /// Hand a landed reuse scan to the session: stage the segments that it
    /// reused, and plan the prefix seed among the rest. Returns `Pending`
    /// while the scan runs, because the pull waits for it. The call that
    /// finds the scan landed returns what it reused.
    pub fn drive_reuse_scan(&mut self) -> Poll<Option<ReusedSegments>> {
        let Some(scan) = self
            .transfer
            .as_ref()
            .and_then(|session| session.scan.as_ref())
        else {
            return Poll::Ready(None);
        };
        let Some(scanned) = scan.result.borrow_mut().take() else {
            return Poll::Pending;
        };
        let reused = self.accept_reuse_scan(scanned);
        let Some(session) = self.transfer.as_mut() else {
            return Poll::Ready(None);
        };
        let segments = ReusedSegments::new(&session.artifacts, &reused);
        session.scan = None;
        for (index, meta) in reused {
            session.artifacts[index as usize] = TransferArtifact::Staged(meta);
        }
        let seed = self
            .transfer
            .as_ref()
            .and_then(|session| self.plan_prefix_seed(&session.artifacts));
        if let Some(session) = self.transfer.as_mut() {
            session.seed = seed;
        }
        // The I/O turn that reports a landed scan leaves the ready queue ahead
        // of any other I/O work that is due, so that work takes the next turn
        // instead of the next notification.
        self.notify_io();
        Poll::Ready(Some(segments))
    }

    /// The reuse scan of `manifest`, planned on the pump. `None` without a
    /// partition directory.
    fn plan_reuse_scan(&self, manifest: &[consensus::StateArtifact]) -> Option<ReuseScanJob> {
        let partition_dir = self.partition_dir.clone()?;
        // The scan owns the memo while it can replace staging names.
        let memoized = self
            .reuse_scan_memo
            .borrow_mut()
            .take()
            .map_or_else(BTreeMap::new, |memo| memo.adopted);
        let entries = manifest
            .iter()
            .zip(0u32..)
            .filter(|(entry, _)| entry.kind == artifact_kind::SEGMENT_LOG)
            .map(|(entry, index)| (index, *entry, self.held_segment(entry)))
            .collect();
        Some(ReuseScanJob {
            partition_dir,
            namespace_raw: self.consensus().group(),
            memoized,
            entries,
        })
    }

    /// Select a sealed segment by its append checksum. The detached scan
    /// verifies its on-disk bytes before reusing it.
    fn held_segment(&self, entry: &consensus::StateArtifact) -> Option<HeldSegment> {
        let position = self.sealed_segment_holding(entry.frontier, entry.len, entry.checksum)?;
        let (Some(log_path), _) = self.log.storages()[position].segment_and_index_paths() else {
            return None;
        };
        Some(HeldSegment { log_path })
    }

    /// The position of the sealed live segment at `start_offset` whose memo
    /// stamps its `len` bytes with `checksum`.
    fn sealed_segment_holding(&self, start_offset: u64, len: u64, checksum: u64) -> Option<usize> {
        let segments = self.log.segments();
        let position = segments
            .binary_search_by_key(&start_offset, |segment| segment.start_offset)
            .ok()?;
        let segment = &segments[position];
        let stamp = self
            .segment_checksum_cache
            .borrow()
            .get(&start_offset)
            .and_then(|memo| memo.checksum_of(len));
        (segment.sealed && segment.size.as_bytes_u64() == len && stamp == Some(checksum))
            .then_some(position)
    }

    /// Keep verified files, including progress made before a scan stopped.
    ///
    /// The pump ran while the scan did, so a linked segment must still be
    /// sealed and stamped as the manifest expects. Retention can have
    /// removed it. An adopted copy needs no check, because only a scan, its
    /// session and the install of that session write staging names, and the
    /// session neither pulls nor installs before its scan lands.
    pub(crate) fn accept_reuse_scan(
        &self,
        mut scanned: ReuseScanned,
    ) -> Vec<(u32, StagedSegmentMeta)> {
        let valid = |meta: &StagedSegmentMeta| {
            !meta.linked
                || self
                    .sealed_segment_holding(meta.start_offset, meta.size, meta.checksum)
                    .is_some()
        };
        let reused: Vec<_> = scanned
            .reused
            .into_iter()
            .filter(|(_, meta)| valid(meta))
            .collect();
        scanned.memoized.retain(|_, meta| valid(meta));
        for (_, meta) in &reused {
            scanned.memoized.insert(meta.start_offset, meta.clone());
        }
        *self.reuse_scan_memo.borrow_mut() =
            (!scanned.memoized.is_empty()).then_some(ReuseScanMemo {
                adopted: scanned.memoized,
            });
        reused
    }

    /// The segment artifact whose leading bytes this replica holds: a live
    /// segment at the base offset of an entry that no reuse claimed, and
    /// shorter than that entry. Such a segment is the one the primary kept
    /// writing after this replica fell behind. The spill proves the prefix.
    fn plan_prefix_seed(&self, artifacts: &[TransferArtifact]) -> Option<PrefixSeed> {
        let segments = self.log.segments();
        artifacts.iter().zip(0u32..).find_map(|(slot, artifact)| {
            let entry = slot.pending()?.entry;
            if entry.kind != artifact_kind::SEGMENT_LOG {
                return None;
            }
            let position = segments
                .binary_search_by_key(&entry.frontier, |segment| segment.start_offset)
                .ok()?;
            let len = segments[position].size.as_bytes_u64();
            if len == 0 || len >= entry.len {
                return None;
            }
            let (log_path, _) = self.log.storages()[position].segment_and_index_paths();
            Some(PrefixSeed::Planned {
                artifact,
                log_path: log_path?,
                len,
            })
        })
    }

    /// Start or finish the prefix read of the artifact that the pull reached.
    ///
    /// Returns `true` while the read runs. The next chunk request then waits,
    /// because that request asks for the bytes that the read supplies. The read runs
    /// in a detached task, so a read of up to a whole segment does not hold the
    /// pump. A failed read leaves the artifact to a whole pull.
    pub fn drive_prefix_seed(&mut self) -> bool {
        let namespace_raw = self.namespace().inner();
        let Some(session) = self.transfer.as_mut() else {
            return false;
        };
        let next = session
            .artifacts
            .iter()
            .position(|artifact| !consensus::ChunkProgress::complete(artifact));
        if session
            .seed
            .as_ref()
            .is_none_or(|seed| Some(seed.artifact() as usize) != next)
        {
            return false;
        }
        match session.seed.take() {
            Some(PrefixSeed::Planned {
                artifact,
                log_path,
                len,
            }) => {
                let Some(entry) = session.artifacts[artifact as usize]
                    .pending()
                    .map(|progress| progress.entry)
                else {
                    return false;
                };
                let read =
                    self.spawn_local_work(prefix_read_stopped, move |cancelled| async move {
                        read_segment_prefix(&log_path, entry, len, &cancelled).await
                    });
                if let Some(session) = self.transfer.as_mut() {
                    session.seed = Some(PrefixSeed::Reading { artifact, read });
                }
                true
            }
            Some(PrefixSeed::Reading { artifact, read }) => {
                let Some(landed) = read.result.borrow_mut().take() else {
                    session.seed = Some(PrefixSeed::Reading { artifact, read });
                    return true;
                };
                match (landed, session.artifacts[artifact as usize].pending_mut()) {
                    (Ok((bytes, prefix)), Some(progress)) if progress.buf.is_empty() => {
                        tracing::info!(
                            namespace_raw,
                            artifact,
                            seeded_len = prefix.len,
                            artifact_len = progress.entry.len,
                            "seeded a partition transfer artifact from the local segment"
                        );
                        progress.buf = bytes;
                        session.seed = Some(PrefixSeed::Seeded { artifact, prefix });
                    }
                    (Err(error), _) => tracing::info!(
                        namespace_raw,
                        artifact,
                        %error,
                        "cannot read the local segment prefix, so the pull fetches the \
                         whole artifact"
                    ),
                    _ => {}
                }
                // The I/O turn that reports a landed read leaves the ready
                // queue ahead of any other I/O work that is due, so that work
                // takes the next turn instead of the next notification.
                self.notify_io();
                false
            }
            seed => {
                session.seed = seed;
                false
            }
        }
    }

    fn spawn_staging_work<T: 'static, F: Future<Output = T> + 'static>(
        &mut self,
        stopped: fn() -> T,
        work: impl FnOnce(Rc<Cell<bool>>) -> F + 'static,
    ) -> LocalWork<T> {
        let (ended, receiver) = futures::channel::oneshot::channel::<()>();
        let previous = self.staging_work_ended.replace(receiver);
        self.spawn_local_work(stopped, move |cancelled| async move {
            let _ended = ended;
            if let Some(previous) = previous {
                let _ = previous.await;
            }
            work(cancelled).await
        })
    }

    /// Run `work` in a detached task, so that the pump does not wait for it.
    /// `work` gets the flag that asks it to stop.
    fn spawn_local_work<T: 'static, F: Future<Output = T> + 'static>(
        &self,
        stopped: fn() -> T,
        work: impl FnOnce(Rc<Cell<bool>>) -> F,
    ) -> LocalWork<T> {
        let local = LocalWork::default();
        if !self.io_active.begin_local_work() {
            *local.result.borrow_mut() = Some(stopped());
            self.notify_io();
            return local;
        }
        let landing = Landing {
            io: Rc::clone(&self.io_active),
            result: Rc::clone(&local.result),
            wake: self.io_wake(),
            stopped,
        };
        let work = work(Rc::clone(&local.cancel.0));
        self.consensus().message_bus().spawn(async move {
            landing.land(work.await);
        });
        local
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
    /// [`PartitionInstallError`]; check-phase variants mutate nothing. Shutdown
    /// refuses new installs without changing an install already in progress.
    #[allow(clippy::too_many_lines)]
    pub fn queue_state_transfer_install(
        &mut self,
        commit_op: u64,
        staged: Vec<StagedSegmentMeta>,
        offsets_bytes: &[u8],
        peer: u8,
    ) -> Result<(), PartitionInstallError> {
        if self.is_shutting_down() {
            return Err(PartitionInstallError::ShuttingDown);
        }
        let pending =
            self.prepare_state_transfer_install(commit_op, staged, offsets_bytes, peer)?;
        self.cancel_primary_transfer();
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
        let base = crate::io::file_phase_charge::<SB>(
            std::iter::once(Some(pending.partition_dir.as_str()))
                .chain(ConsumerKind::ALL.map(|kind| self.consumer_offsets_dir(kind)))
                .flatten(),
        )?;
        match pending.phase {
            InstallPhase::StageOffsets(_)
            | InstallPhase::DiscardOffsets(_)
            | InstallPhase::CommitOffsets(_) => base.checked_mul(OFFSET_PERSIST_CONCURRENCY),
            InstallPhase::Sweep => pending.staged.iter().try_fold(
                base.checked_add(SEGMENT_SWEEP_STACK_BYTES)?,
                |charge, segment| {
                    charge
                        .checked_add(segment.log_staging.capacity())?
                        .checked_add(segment.index_staging.capacity())?
                        .checked_add(2 * (size_of::<PathBuf>() + 4 * size_of::<&Path>() + 4))
                },
            ),
            InstallPhase::OldOffsets(Some(kind)) => {
                let retained = pending
                    .offsets_wire
                    .offset_sections()
                    .get(kind.index())
                    .map_or(0, |entries| entries.len());
                base.checked_add(self.offset_cleanup_charge(kind)?)?
                    .checked_add(retained.checked_mul(size_of::<u32>())?)
            }
            InstallPhase::RetryCheckpoint => base.checked_add(pending.retry_checkpoint.capacity()),
            InstallPhase::Converge => ConsumerKind::ALL.into_iter().try_fold(
                base.checked_add(size_of::<[HashSet<u32>; ConsumerKind::COUNT]>())?,
                |charge, kind| {
                    charge
                        .checked_add(self.offset_cleanup_charge(kind)?)?
                        .checked_add(
                            self.consumer_offset_capacity_for(kind)
                                .stranded_count()
                                .checked_mul(size_of::<(ConsumerKind, u32)>())?,
                        )
                },
            ),
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
                    if install.drain.is_none() {
                        // Checkpoint syncs may be skipped when the frontier is unchanged.
                        // The backup still needs proof from each original writer.
                        install.synced_files = self.barrier_install_files_locked();
                        install.disposition = InstallDisposition::Backup;
                    }
                    self.start_persistence();
                    let drain = install
                        .drain
                        .get_or_insert_with(|| persistence.begin_drain());
                    match persistence.observe_drain(drain) {
                        Ok(true) => {
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
                match self
                    .transition_superblock(crate::iggy_partition::SuperblockIntent::Install(
                        frontier,
                    ))
                    .await
                {
                    crate::PartitionIoVerdict::Pending => step = crate::PartitionIoStep::Pending,
                    crate::PartitionIoVerdict::Failed => self.fail_install(
                        &mut install,
                        PartitionInstallError::FrontierNotDurable { frontier },
                    ),
                    crate::PartitionIoVerdict::Ready => {
                        install.disposition = InstallDisposition::Mutating;
                        self.cancel_history_maintenance();
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
                self.clear_install_journal();
                install.phase = InstallPhase::OldOffsets(Some(ConsumerKind::Consumer));
            }
            InstallPhase::OldOffsets(None) if self.emptied_marker_path().is_none() => {
                self.clear_install_offsets(install.emptied_at);
                install.phase = InstallPhase::CommitOffsets(0);
            }
            InstallPhase::OldOffsets(Some(kind)) if self.consumer_offsets_dir(kind).is_none() => {
                install.phase =
                    InstallPhase::OldOffsets(crate::iggy_partition::next_consumer_kind(kind));
            }
            InstallPhase::CommitOffsets(cursor) if cursor == install.planned_offsets.len() => {
                install.phase = InstallPhase::OffsetDirectories;
            }
            InstallPhase::OffsetDirectories if !install.offset_dirs_changed.contains(&true) => {
                install.phase = InstallPhase::Publish;
            }
            InstallPhase::Publish => {
                self.publish_installed_state(&mut install);
                install.phase = InstallPhase::RetryCheckpoint;
            }
            InstallPhase::RetryCheckpoint if self.persistence.is_none() => {
                install.phase = InstallPhase::WalReset;
            }
            InstallPhase::WalReset => {
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
                            install.phase = InstallPhase::ReclaimRetry;
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
        if matches!(install.phase, InstallPhase::RetryCheckpoint) {
            let path = self
                .retry_checkpoint_path(install.commit_op)
                .map_err(|_| iggy_common::IggyError::CannotWriteToFile)?;
            let Some(crate::iggy_partition::PendingPartitionTransition::Install(install)) =
                self.transition.as_mut()
            else {
                unreachable!("install exists");
            };
            install.phase = InstallPhase::Submitted(InstallFilePhase::RetryCheckpoint);
            return Ok(crate::PartitionIoJob::RetryCheckpoint {
                path,
                bytes: std::mem::take(&mut install.retry_checkpoint),
            });
        }
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
                    synced_files: install.synced_files.clone(),
                }),
                InstallFilePhase::Backup,
            ),
            InstallPhase::RemoveSegments => {
                let (segment, storage) = self
                    .log
                    .retire_front()
                    .ok_or(iggy_common::IggyError::CannotDeleteFile)?;
                let (messages, index) = storage.segment_and_index_paths();
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
            InstallPhase::OldOffsets(Some(kind)) => {
                let consumer_kind = *kind;
                // Replacements atomically overwrite their old files, so only keys
                // absent from the plan are unlinked.
                let mut retained: Vec<u32> = install
                    .planned_offsets
                    .iter()
                    .filter(|write| write.kind == consumer_kind)
                    .map(|write| write.id)
                    .collect();
                retained.sort_unstable();
                (
                    self.capture_offset_cleanup(*kind, retained)?,
                    InstallFilePhase::OldOffsets(consumer_kind),
                )
            }
            // After the unlinks and before the renames, the order a committed
            // delete or store keeps.
            InstallPhase::OldOffsets(None) => (
                file_job(crate::io::TransferFileJob::EmptiedMarker {
                    path: self
                        .emptied_marker_path()
                        .ok_or(iggy_common::IggyError::CannotWriteToFile)?,
                    emptied_at: install.emptied_at,
                }),
                InstallFilePhase::EmptiedMarker,
            ),
            InstallPhase::CommitOffsets(cursor) => {
                let end = (*cursor + OFFSET_PERSIST_CONCURRENCY).min(install.planned_offsets.len());
                (
                    file_job(crate::io::TransferFileJob::CommitOffsets(
                        install.planned_offsets[*cursor..end]
                            .iter()
                            .map(|write| write.path.clone())
                            .collect(),
                    )),
                    InstallFilePhase::CommitOffsets {
                        start: *cursor,
                        end,
                    },
                )
            }
            // The parent syncs after every kind: a kind directory created since
            // its last sync is not durable until `offsets/` is, and once the WAL
            // restarts at this install nothing replays the files inside it.
            InstallPhase::OffsetDirectories => {
                let paths = ConsumerKind::ALL.map(|kind| {
                    install.offset_dirs_changed[kind.index()]
                        .then(|| self.consumer_offsets_dir(kind).map(str::to_owned))
                        .flatten()
                });
                if paths
                    .iter()
                    .zip(install.offset_dirs_changed)
                    .any(|(path, changed)| changed && path.is_none())
                {
                    return Err(iggy_common::IggyError::CannotSyncFile);
                }
                (
                    crate::PartitionIoJob::OffsetDirectories(crate::io::OffsetDirectoriesIoJob {
                        paths,
                        attempted: install.offset_dirs_changed,
                        #[cfg(test)]
                        fault: None,
                    }),
                    InstallFilePhase::OffsetDirectories,
                )
            }
            InstallPhase::FinishBackup => (
                file_job(crate::io::TransferFileJob::Backup {
                    directory: install.partition_dir.clone(),
                    begin: false,
                    synced_files: BTreeSet::new(),
                }),
                InstallFilePhase::FinishBackup,
            ),
            InstallPhase::ClearMissing => (
                file_job(crate::io::TransferFileJob::ClearMissing(
                    install.partition_dir.clone(),
                )),
                InstallFilePhase::ClearMissing,
            ),
            InstallPhase::ReclaimRetry => (
                crate::PartitionIoJob::ReclaimRetryCheckpoints {
                    current: self
                        .retry_checkpoint_path(install.commit_op)
                        .map_err(|_| iggy_common::IggyError::CannotDeleteFile)?,
                    keep_current: install.commit_op > 0,
                },
                InstallFilePhase::ReclaimRetry,
            ),
            InstallPhase::Converge => (
                file_job(crate::io::TransferFileJob::Converge {
                    directory: install.partition_dir.clone(),
                    offset_directories: ConsumerKind::ALL
                        .map(|kind| self.consumer_offsets_dir(kind).map(str::to_owned)),
                    emptied_marker: self.emptied_marker_path(),
                    stranded_offsets: Box::new(ConsumerKind::ALL.map(|kind| {
                        let capacity = self.consumer_offset_capacity_for(kind);
                        let mut known = HashSet::with_capacity(capacity.stranded_count());
                        capacity.extend_stranded_ids(&mut known);
                        known
                    })),
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
                InstallPhase::Submitted(InstallFilePhase::ClearMissing),
                crate::PartitionIoResult::Transfer(crate::io::TransferFileResult::Finished(Err(
                    error,
                ))),
            ) => {
                // The install is already published. Keep its recovery marker
                // without rolling back or fencing the installed data.
                install.failure = Some(error);
                Ok(InstallPhase::Done)
            }
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
                InstallFilePhase::EmptiedMarker => {
                    self.clear_install_offsets(install.emptied_at);
                    InstallPhase::CommitOffsets(0)
                }
                InstallFilePhase::CommitOffsets { start, end } => {
                    for write in &install.planned_offsets[start..end] {
                        self.publish_transferred_offset(write);
                        install.offset_dirs_changed[write.kind.index()] = true;
                    }
                    InstallPhase::CommitOffsets(end)
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
                    self.clear_install_journal();
                    InstallPhase::OldOffsets(Some(ConsumerKind::Consumer))
                })
                .map_err(|source| PartitionInstallError::SwapIo {
                    path: install.partition_dir.clone(),
                    source,
                }),
            (
                InstallPhase::Submitted(InstallFilePhase::OldOffsets(consumer_kind)),
                crate::PartitionIoResult::OffsetCleanup(outcome),
            ) => {
                let kind = consumer_kind.index();
                let capacity = self.consumer_offset_capacity_for(consumer_kind);
                install.offset_dirs_changed[kind] |= outcome.changed;
                for id in outcome.released {
                    capacity.clear_stranded(id);
                }
                for id in &outcome.failed {
                    capacity.record_stranded(*id);
                }
                if let Some(source) = outcome.scan_error {
                    Err(PartitionInstallError::SwapIo {
                        path: self
                            .consumer_offsets_dir(consumer_kind)
                            .unwrap_or_default()
                            .to_owned(),
                        source,
                    })
                } else if let Some(id) = outcome.failed.first() {
                    Err(PartitionInstallError::OffsetPersistence {
                        path: self
                            .persisted_offset_path(consumer_kind, *id)
                            .unwrap_or_default(),
                        source: iggy_common::IggyError::CannotDeleteFile,
                    })
                } else {
                    Ok(InstallPhase::OldOffsets(
                        crate::iggy_partition::next_consumer_kind(consumer_kind),
                    ))
                }
            }
            (
                InstallPhase::Submitted(InstallFilePhase::OffsetDirectories),
                crate::PartitionIoResult::OffsetDirectories(result),
            ) => ConsumerKind::ALL
                .into_iter()
                .find(|kind| result.failed[kind.index()])
                .map_or(Ok(InstallPhase::Publish), |kind| {
                    Err(PartitionInstallError::SwapIo {
                        path: self
                            .consumer_offsets_dir(kind)
                            .unwrap_or_default()
                            .to_owned(),
                        source: std::io::Error::other("consumer offset directory sync failed"),
                    })
                }),
            (
                InstallPhase::Submitted(InstallFilePhase::RetryCheckpoint),
                crate::PartitionIoResult::RetryCheckpoint(outcome),
            ) => outcome.map(|()| InstallPhase::WalReset).map_err(|source| {
                PartitionInstallError::SwapIo {
                    path: self.retry_checkpoint_path(install.commit_op).map_or_else(
                        |_| install.partition_dir.clone(),
                        |path| path.display().to_string(),
                    ),
                    source,
                }
            }),
            (
                InstallPhase::Submitted(InstallFilePhase::ReclaimRetry),
                crate::PartitionIoResult::ReclaimRetryCheckpoints(outcome),
            ) => {
                if let Err(error) = outcome {
                    tracing::warn!(%error, namespace_raw = self.consensus().group(), "cannot reclaim obsolete receipt checkpoints after install");
                }
                Ok(InstallPhase::FinalFrontier)
            }
            (
                InstallPhase::Submitted(InstallFilePhase::Converge),
                crate::PartitionIoResult::Transfer(crate::io::TransferFileResult::Converged {
                    segment,
                    stranded,
                    released,
                }),
            ) => {
                self.clear_converged_state(&install);
                for (kind, id) in released {
                    self.consumer_offset_capacity_for(kind).clear_stranded(id);
                }
                if let Some((kind, id)) = stranded {
                    self.consumer_offset_capacity_for(kind).record_stranded(id);
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

    fn clear_install_journal(&mut self) {
        self.log.journal().inner.clear_all();
        self.reset_proof_walk_memory();
        self.log.journal_mut().info = crate::log::JournalInfo::default();
        self.consensus().note_journal_mutation();
    }

    /// Clear the offset table for the installed one, which keeps `emptied_at`.
    fn clear_install_offsets(&mut self, emptied_at: u64) {
        self.consumer_offsets.pin().clear();
        self.consumer_group_offsets.pin().clear();
        self.last_polled_offsets.pin().clear();
        self.durable_consumer_offsets.clear();
        self.durable_consumer_offsets.set_emptied_at(emptied_at);
        self.explicit_offset_stores
            .clear(self.consensus().message_bus().monotonic_micros());
        self.pending_consumer_offset_commits.clear();
        for kind in ConsumerKind::ALL {
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
            // Recorded below: the durable table is the only store of an external offset.
            ConsumerKind::ExternalGroup => {}
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

    fn publish_installed_state(&mut self, install: &mut PendingInstall) {
        if let Some(retry_table) = install.retry_table.take() {
            *self.dedup_mut() = retry_table;
        }
        self.required_metadata_frontier = self
            .required_metadata_frontier
            .max(install.offsets_wire.required_metadata_frontier);
        self.consumer_group_owners = install
            .offsets_wire
            .owners
            .iter()
            .map(|installed| (installed.installation.group_id, *installed))
            .collect();
        self.consumer_group_polls.clear();
        self.history_transition = install.offsets_wire.history_transition;
        self.owner_retirement
            .clone_from(&install.offsets_wire.owner_retirement);
        let end = install.next_offset.saturating_sub(1);
        self.offset.store(end, Ordering::Release);
        self.dirty_offset.store(end, Ordering::Relaxed);
        self.set_offset_space_used(install.next_offset > 0);
        self.recovered_durable_offset = install.staged.last().map(|meta| meta.end_offset);
        self.installed_frontier = (install.next_offset > 0).then_some(install.next_offset);
        // Each staged log matched its manifest checksum, so a later offer or
        // reuse scan can trust these stamps without reading the files.
        let mut memos: std::collections::HashMap<u64, SegmentChecksumMemo> = install
            .staged
            .iter()
            .map(|meta| {
                (
                    meta.start_offset,
                    SegmentChecksumMemo::Stamped {
                        len: meta.size,
                        checksum: meta.checksum,
                    },
                )
            })
            .collect();
        // The active segment grows after the install, and its first append
        // drops a stamp, so the next transfer after it seals pulls it whole.
        // The hasher of its spill extends with each append instead.
        if let Some(spilled) = self.spilled_hasher.borrow_mut().take()
            && let Some(active) = install.staged.last()
            && spilled.start_offset == active.start_offset
            && spilled.size == active.size
            && spilled.hasher.finish() == active.checksum
        {
            memos.insert(
                active.start_offset,
                SegmentChecksumMemo::Hashing {
                    hashed_len: active.size,
                    hasher: spilled.hasher,
                },
            );
        }
        *self.segment_checksum_cache.borrow_mut() = memos;
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
            self.consensus().note_journal_mutation();
        }
        self.consensus()
            .install_checkpoint(install.commit_op, install.offsets_wire.prepare_checksum);
        // The cleared journal and pipeline held any unapplied install, and the
        // checkpoint already carries every owner installed up to commit_op.
        self.pending_owner_install.set(None);
        self.owner_install_barrier = 0;
        self.observed_view = self.consensus().view();
        self.repair = None;
        self.forget_recovered_commit();
        self.transfer_offer_cache.borrow_mut().take();
    }

    fn clear_converged_state(&mut self, install: &PendingInstall) {
        self.invalidate_poll_history();
        self.log.invalidate_sealed_read_state();
        while self.log.retire_front().is_some() {}
        self.clear_install_journal();
        // The journal wipe took the bodies of the pipeline's prepares, and its
        // queued requests were admitted under the history retired above.
        self.consensus().clear_pipeline();
        self.clear_install_offsets(0);
        self.segment_checksum_cache.borrow_mut().clear();
        self.reuse_scan_memo.borrow_mut().take();
        let end = install.offsets_wire.next_offset.saturating_sub(1);
        self.offset.store(end, Ordering::Release);
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

    #[allow(clippy::too_many_lines)]
    fn prepare_state_transfer_install(
        &self,
        commit_op: u64,
        mut staged: Vec<StagedSegmentMeta>,
        offsets_bytes: &[u8],
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
        // The install wipes the journal. The primary may have counted this
        // replica's ack before its commit reached us. Acks wait for the
        // contiguous WAL head, so protect held ops through that head. Held ops
        // above it were never acked, and repair fetches them again after
        // install. Protecting them as well refuses every offer under steady
        // writes: while the serving peer builds the offer, repair journals the
        // committed tail past the op the offer pinned. Without a WAL, protect
        // the recorded acknowledgement frontier and the known commit instead.
        let commit_max = self.consensus().commit_max();
        let held_max = self.persistence.as_ref().map_or_else(
            || commit_max.max(self.acknowledged_op.get()),
            |persistence| persistence.head(),
        );
        let journal = &self.log.journal().inner;
        if commit_op < held_max && journal.holds_op_in(commit_op + 1..=held_max) {
            return Err(PartitionInstallError::OfferErasesHeldOps {
                commit_op,
                held_max,
            });
        }
        let offsets_wire = ConsumerOffsetsWire::decode(offsets_bytes)?;
        offsets_wire.validate_owners(self.created_revision(), commit_op)?;
        if self.history_deleted() && offsets_wire.history_transition != self.history_transition {
            return Err(ConsumerOffsetsWireError::InvalidOwnership.into());
        }
        let retry_table = checked_retry_table(&offsets_wire, commit_op, self.dedup().capacity())
            .map_err(PartitionInstallError::RetryProtection)?;
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
        // replica than on the rest of the group.
        //
        // The COMMITTED frontier, which is what an offer is comparable against:
        // `held_offset_frontier` reads 0 for a chain installed empty at frontier
        // N (its disk arm filters empty segments and the install clears the
        // journal), and a 0 skips the guard below entirely, letting a stale offer
        // rewind the counter under data this replica already claimed. The append
        // point is not usable either -- it can stand a lease block high -- but
        // only on a solo group, which never receives an offer.
        let local_next_offset = self.offset_frontier();
        if local_next_offset > 0 && offsets_wire.next_offset < local_next_offset {
            return Err(PartitionInstallError::OfferRewindsDurableData {
                offer_next_offset: offsets_wire.next_offset,
                local_next_offset,
            });
        }
        // The durable offset table has no frontier to compare, so the same
        // rule keys on what each side holds. A replica that only holds external
        // group offsets reads 0 above, and an offer that holds nothing at all
        // would unlink every one of them. That is right only when a committed
        // delete this replica has not applied emptied the donor's table: every
        // offset held here was gone at that op.
        let offer_holds_nothing = staged.is_empty()
            && offsets_wire.next_offset == 0
            && offsets_wire
                .offset_sections()
                .iter()
                .all(|entries| entries.is_empty());
        let local_offsets: usize = ConsumerKind::ALL
            .into_iter()
            .map(|kind| self.durable_consumer_offsets.count(kind))
            .sum();
        // An op past the offer's own is not one the donor applied.
        let emptied_at = if offsets_wire.emptied_at <= commit_op {
            offsets_wire.emptied_at
        } else {
            0
        };
        if offer_holds_nothing && local_offsets > 0 && emptied_at <= commit_min {
            return Err(PartitionInstallError::OfferErasesDurableOffsets { local_offsets });
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
        let retry_checkpoint = if self.persistence.is_some() {
            retry_checkpoint_bytes(
                self.consensus().group(),
                self.created_revision(),
                commit_op,
                &offsets_wire,
            )
        } else {
            Vec::new()
        };
        Ok(PendingInstall {
            commit_op,
            peer,
            partition_dir,
            staged,
            offsets_wire,
            emptied_at: if planned_offsets.is_empty() {
                emptied_at
            } else {
                0
            },
            planned_offsets,
            next_offset,
            phase: InstallPhase::StageOffsets(0),
            drain: None,
            synced_files: BTreeSet::new(),
            directory_handle: None,
            offset_dirs_changed: [false; ConsumerKind::COUNT],
            retry_table: Some(retry_table),
            retry_checkpoint,
            failure: None,
            disposition: InstallDisposition::Prepared,
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
    ) -> Result<PartitionInstallOutcome, PartitionInstallError> {
        let peer = self.consensus().primary_index(self.consensus().view());
        let pending =
            self.prepare_state_transfer_install(commit_op, staged, offsets_bytes, peer)?;
        self.cancel_primary_transfer();
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
                        if let Err(source) = persistence
                            .drain_with_timeout(|timeout| {
                                self.consensus().message_bus().sleep(timeout)
                            })
                            .await
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
    Cancelled,
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
            Self::Cancelled => write!(f, "the transfer abandoned its segment spill"),
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

/// Bytes a prefix read reads between two checks of its cancel flag. A dropped
/// seed stops the read within one piece: 64 MiB, under a second on a
/// 125 MB/s cloud volume. Each piece opens the file once and allocates one
/// read chunk, which costs little next to 16 chunk reads.
const PREFIX_READ_PIECE_LEN: u64 = 16 * OFFER_HASH_CHUNK_LEN as u64;

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
        // The caller's log line shows the error but not where it happened.
        read.inspect_err(|error| {
            tracing::debug!(path, position, to, %error, "reading segment bytes failed");
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
/// The serving side caches only artifacts that fit one chunk. Larger files
/// use bounded range reads and are verified by the receiver before install.
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

/// Read a requested segment range without loading the rest of the file.
///
/// The caller bounds `len` by the manifest and chunk limit. The receiver
/// verifies the complete artifact before installing it, including its prefix.
///
/// # Errors
/// Returns the classified open or read error, including a short range.
pub async fn load_segment_chunk(
    log_path: &str,
    offset: u64,
    len: usize,
) -> Result<Vec<u8>, SegmentLoadError> {
    let file = compio::fs::File::open(log_path)
        .await
        .map_err(SegmentLoadError::classify)?;
    let compio::BufResult(result, bytes) = file.read_exact_at(vec![0; len], offset).await;
    result.map_err(SegmentLoadError::classify)?;
    Ok(bytes)
}

/// Read, hash and walk the first `len` bytes of a local segment file into a
/// buffer sized for the whole artifact, so the pulled suffix appends in place.
/// Once `cancelled` is set, the read stops before its next piece or the walk
/// with [`std::io::ErrorKind::Interrupted`] and frees the buffer.
pub(crate) async fn read_segment_prefix(
    log_path: &str,
    entry: consensus::StateArtifact,
    len: u64,
    cancelled: &Cell<bool>,
) -> std::io::Result<(Vec<u8>, SeededPrefix)> {
    let capacity = usize::try_from(entry.len).map_err(std::io::Error::other)?;
    let mut bytes = Vec::with_capacity(capacity);
    let mut hasher = Box::<StateArtifactHasher>::default();
    let mut position = 0;
    loop {
        if cancelled.get() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "the transfer gave up this prefix read",
            ));
        }
        if position == len {
            break;
        }
        let end = len.min(position.saturating_add(PREFIX_READ_PIECE_LEN));
        hash_segment_range(log_path, position, end, &mut hasher, Some(&mut bytes)).await?;
        position = end;
    }
    let mut walk = SegmentWalk::new(entry.frontier);
    let walk = walk.advance(&bytes).await.is_ok().then_some(walk);
    Ok((bytes, SeededPrefix { len, hasher, walk }))
}

/// Lands the result of local work and wakes the partition. A task that stops
/// without a result, such as one that panicked, drops this guard with an
/// empty slot. The guard then lands `stopped()`, so the session does not wait
/// forever.
struct Landing<T, W: Fn()> {
    io: Rc<PartitionIoQuiescence>,
    result: Rc<RefCell<Option<T>>>,
    wake: W,
    stopped: fn() -> T,
}

impl<T, W: Fn()> Landing<T, W> {
    fn land(self, result: T) {
        *self.result.borrow_mut() = Some(result);
    }
}

impl<T, W: Fn()> Drop for Landing<T, W> {
    fn drop(&mut self) {
        if self.result.borrow().is_none() {
            self.io.interrupt();
        }
        self.io.settle_local_work();
        self.result.borrow_mut().get_or_insert_with(self.stopped);
        (self.wake)();
    }
}

/// A prefix read that stopped without a result, so the artifact falls back to
/// a whole pull.
fn prefix_read_stopped() -> PrefixRead {
    Err(std::io::Error::other(
        "the prefix read stopped without a result",
    ))
}

const fn segment_spill_stopped() -> Result<SpilledSegment, SpillError> {
    Err(SpillError::Cancelled)
}

struct SegmentSpillJob {
    partition_dir: String,
    entry: consensus::StateArtifact,
    bytes: Vec<u8>,
    prefix: Option<SeededPrefix>,
}

impl SegmentSpillJob {
    async fn run(self, cancelled: &Cell<bool>) -> Result<SpilledSegment, SpillError> {
        if cancelled.get() {
            return Err(SpillError::Cancelled);
        }
        let Self {
            partition_dir,
            entry,
            bytes,
            prefix,
        } = self;
        let (mut hasher, walk, hashed_len) = match prefix {
            Some(prefix) => (prefix.hasher, prefix.walk, prefix.len),
            None => (Box::default(), None, 0),
        };
        // Artifact-level integrity FIRST, exactly as the metadata plane
        // verifies every artifact before decoding: the walk's per-batch
        // checksums prove batch bodies, not that these are the bytes the
        // manifest promised (length alone is implied by completion).
        let unhashed = usize::try_from(hashed_len)
            .ok()
            .and_then(|hashed_len| bytes.get(hashed_len..));
        let Some(unhashed) = unhashed.filter(|_| bytes.len() as u64 == entry.len) else {
            return Err(SpillError::ManifestChecksum {
                frontier: entry.frontier,
            });
        };
        hash_yielding(&mut hasher, [unhashed]).await;
        if cancelled.get() {
            return Err(SpillError::Cancelled);
        }
        if hasher.finish() != entry.checksum {
            return Err(SpillError::ManifestChecksum {
                frontier: entry.frontier,
            });
        }
        let mut walk = walk.unwrap_or_else(|| SegmentWalk::new(entry.frontier));
        walk.advance(&bytes).await.map_err(SpillError::Walk)?;
        let (stats, index_bytes) = walk.finish().map_err(SpillError::Walk)?;
        if cancelled.get() {
            return Err(SpillError::Cancelled);
        }
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
        if cancelled.get() {
            return Err(SpillError::Cancelled);
        }
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
        if cancelled.get() {
            return Err(SpillError::Cancelled);
        }
        Ok(SpilledSegment {
            meta: StagedSegmentMeta::from_walk(
                &entry,
                stats,
                index_size,
                log_staging,
                index_staging,
            ),
            hasher: SpilledHasher {
                start_offset: entry.frontier,
                size: entry.len,
                hasher,
            },
        })
    }
}

/// A reuse scan that a detached task runs. It holds no partition state, so
/// the pump runs while it does.
struct ReuseScanJob {
    partition_dir: String,
    namespace_raw: u64,
    memoized: BTreeMap<u64, StagedSegmentMeta>,
    /// The manifest index of each segment entry, the entry, and the live
    /// segment that holds its bytes.
    entries: Vec<(u32, consensus::StateArtifact, Option<HeldSegment>)>,
}

impl ReuseScanJob {
    /// Checks cancellation between files, retaining verified progress without
    /// sweeping files that it did not reach.
    async fn run(mut self, cancelled: &Cell<bool>) -> ReuseScanned {
        let mut reused = Vec::new();
        let mut matched_paths = Vec::new();
        let mut complete = true;
        for (index, entry, held) in &self.entries {
            if cancelled.get() {
                complete = false;
                break;
            }
            let memoized = self
                .memoized
                .remove(&entry.frontier)
                .filter(|meta| (meta.size, meta.checksum) == (entry.len, entry.checksum));
            let meta = if let Some(meta) = memoized
                && matches!(
                    compio::fs::metadata(&meta.log_staging).await,
                    Ok(metadata) if metadata.len() == meta.size
                )
                && matches!(
                    compio::fs::metadata(&meta.index_staging).await,
                    Ok(metadata) if metadata.len() == meta.index_size
                ) {
                Some(meta)
            } else {
                self.reuse(entry, held.as_ref()).await
            };
            let Some(meta) = meta else {
                continue;
            };
            matched_paths.push(meta.log_staging.clone());
            matched_paths.push(meta.index_staging.clone());
            reused.push((*index, meta));
        }
        // Every rebuilt sidecar and every link landed in the same directory,
        // so one fsync covers them all. Its failure discards EVERY adoption:
        // the per-adopt filter above no longer sees a durability failure, and
        // an undurable sidecar handed to the install is a missing rename
        // source after a crash.
        if !reused.is_empty() && fsync_dir(&self.partition_dir).await.is_err() {
            reused.clear();
            matched_paths.clear();
        }
        // A stopped scan cannot tell a stray from a staged copy of an entry
        // that it did not reach.
        if complete {
            // Sweep strays: anything staged that no adopted meta claims.
            sweep_staging_except(&self.partition_dir, matched_paths.into_iter().collect()).await;
            self.memoized.clear();
        }
        ReuseScanned {
            reused,
            memoized: self.memoized,
        }
    }

    /// Prefer a verified live match, then try the staged copy of an earlier
    /// attempt. Either path rebuilds the index from verified log bytes.
    async fn reuse(
        &self,
        entry: &consensus::StateArtifact,
        held: Option<&HeldSegment>,
    ) -> Option<StagedSegmentMeta> {
        if let Some(held) = held
            && let Some(meta) = held
                .link(&self.partition_dir, entry, self.namespace_raw)
                .await
        {
            return Some(meta);
        }
        let (log_staging, _) = staging_paths(&self.partition_dir, entry.frontier);
        // Length short-circuit BEFORE reading: length is the first
        // conjunct of the artifact check anyway, and the common retry
        // case is precisely an active-segment length mismatch -- no
        // point reading up to 2 GiB just to discard it.
        match compio::fs::metadata(&log_staging).await {
            Ok(metadata) if metadata.len() == entry.len => {}
            _ => return None,
        }
        let bytes = compio::fs::read(&log_staging).await.ok()?;
        if !verify_state_artifact_yielding(entry, &bytes).await {
            return None;
        }
        adopt_staged_segment(&self.partition_dir, entry, &bytes)
            .await
            .ok()
    }
}

/// A sealed live log selected by the pump for verification and reuse.
struct HeldSegment {
    log_path: String,
}

impl HeldSegment {
    /// Verify and hard-link the log, rebuilding its index from the same bytes.
    ///
    /// Each link is synced before it takes the staging name, because an
    /// installed segment must be as durable as a spilled one, and the live
    /// writes of this segment did not have to sync. A staging name that
    /// already names the live file is such a link from an earlier scan, so
    /// it costs nothing.
    async fn link(
        &self,
        partition_dir: &str,
        entry: &consensus::StateArtifact,
        namespace_raw: u64,
    ) -> Option<StagedSegmentMeta> {
        let log_metadata = compio::fs::metadata(&self.log_path).await.ok()?;
        // The install opens the log at exactly the manifest length.
        if log_metadata.len() != entry.len {
            return None;
        }
        let bytes = compio::fs::read(&self.log_path).await.ok()?;
        if !verify_state_artifact_yielding(entry, &bytes).await {
            return None;
        }
        let (log_staging, _) = staging_paths(partition_dir, entry.frontier);
        if !names_file_of(&log_metadata, &log_staging).await
            && let Err(error) = link_durably(Path::new(&self.log_path), &log_staging).await
        {
            tracing::warn!(
                %error,
                namespace_raw,
                path = %log_staging.display(),
                "cannot link a held segment to its staging name"
            );
            return None;
        }
        let mut meta = adopt_staged_segment(partition_dir, entry, &bytes)
            .await
            .ok()?;
        meta.linked = true;
        Some(meta)
    }
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
    partition_dir: &str,
    entry: &consensus::StateArtifact,
    bytes: &[u8],
) -> Result<StagedSegmentMeta, SpillError> {
    let (stats, index_bytes) = walk_segment_payload(entry.frontier, bytes)
        .await
        .map_err(SpillError::Walk)?;
    let (log_staging, index_staging) = staging_paths(partition_dir, entry.frontier);
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
    hash_yielding(&mut hasher, [bytes]).await;
    hasher.finish() == entry.checksum
}

pub(crate) async fn hash_yielding<'a>(
    hasher: &mut StateArtifactHasher,
    slices: impl IntoIterator<Item = &'a [u8]>,
) {
    let mut remaining = OFFER_HASH_CHUNK_LEN;
    for mut bytes in slices {
        while !bytes.is_empty() {
            let (chunk, rest) = bytes.split_at(bytes.len().min(remaining));
            hasher.update(chunk);
            bytes = rest;
            remaining -= chunk.len();
            if remaining == 0 {
                yield_to_reactor().await;
                remaining = OFFER_HASH_CHUNK_LEN;
            }
        }
    }
}

pub(crate) fn numeric_offset_id(path: &str) -> Option<u32> {
    Path::new(path).file_name()?.to_str()?.parse().ok()
}

fn checked_retry_table(
    wire: &ConsumerOffsetsWire,
    through_op: u64,
    configured_capacity: usize,
) -> Result<ClientTable, consensus::client_table::ClientTableWireError> {
    if wire.dedup_capacity == 0 {
        if through_op != 0 || !wire.dedup.is_empty() {
            return Err(
                consensus::client_table::ClientTableWireError::InvalidCapacity { capacity: 0 },
            );
        }
        return Ok(ClientTable::with_mode(
            configured_capacity,
            ClientTableMode::PartitionSlice,
        ));
    }
    if wire.dedup_capacity != configured_capacity {
        tracing::warn!(
            configured_capacity,
            committed_capacity = wire.dedup_capacity,
            "partition retry capacity is fixed by committed state; ignoring local configuration"
        );
    }
    let mut table = ClientTable::with_mode(wire.dedup_capacity, ClientTableMode::PartitionSlice);
    table.install_watermarks(wire.dedup_capacity, wire.dedup.iter().cloned())?;
    for entry in &wire.dedup {
        if entry.latest_commit > through_op {
            return Err(consensus::client_table::ClientTableWireError::InvalidReply);
        }
    }
    Ok(table)
}

fn retry_checkpoint_bytes(
    group: u64,
    incarnation: u64,
    through_op: u64,
    wire: &ConsumerOffsetsWire,
) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&RETRY_CHECKPOINT_MAGIC);
    bytes.extend_from_slice(&group.to_le_bytes());
    bytes.extend_from_slice(&incarnation.to_le_bytes());
    bytes.extend_from_slice(&through_op.to_le_bytes());
    bytes.extend_from_slice(&wire.encode());
    bytes.extend_from_slice(&state_artifact_checksum(&bytes).to_le_bytes());
    bytes
}

pub(crate) async fn write_retry_checkpoint(path: &Path, bytes: Vec<u8>) -> std::io::Result<()> {
    let temporary = path.with_extension(RETRY_CHECKPOINT_TEMP_SUFFIX.trim_start_matches('.'));
    write_staging_file(&temporary, bytes).await?;
    compio::fs::rename(&temporary, path).await?;
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "receipt checkpoint has no parent",
        )
    })?;
    compio::fs::File::open(parent).await?.sync_all().await
}

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    pub(crate) fn retry_checkpoint_path(&self, through_op: u64) -> std::io::Result<PathBuf> {
        let directory = self.partition_dir.as_ref().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "receipt checkpoint requires a partition directory",
            )
        })?;
        Ok(Path::new(directory)
            .join(format!("prepares-{}", self.created_revision()))
            .join(format!(
                "{RETRY_CHECKPOINT_PREFIX}{through_op}{RETRY_CHECKPOINT_SUFFIX}"
            )))
    }

    pub(crate) async fn persist_retry_checkpoint(
        &self,
        through_op: u64,
    ) -> std::io::Result<PathBuf> {
        let bytes = self.encode_retry_checkpoint(through_op)?;
        let path = self.retry_checkpoint_path(through_op)?;
        write_retry_checkpoint(&path, bytes).await?;
        Ok(path)
    }

    /// The receipt table answers retries of every op through `through_op`, so
    /// it is captured while the applied frontier is exactly there.
    pub(crate) fn encode_retry_checkpoint(&self, through_op: u64) -> std::io::Result<Vec<u8>> {
        if through_op != self.consensus().commit_min() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "receipt frontier differs from the applied frontier",
            ));
        }
        let wire = ConsumerOffsetsWire {
            dedup_capacity: self.dedup().capacity(),
            required_metadata_frontier: self.required_metadata_frontier,
            dedup: self.dedup().watermarks_sorted(),
            owners: self.consumer_group_owners.values().copied().collect(),
            history_transition: self.history_transition,
            owner_retirement: self.owner_retirement.clone(),
            ..Default::default()
        };
        Ok(retry_checkpoint_bytes(
            self.consensus().group(),
            self.created_revision(),
            through_op,
            &wire,
        ))
    }

    /// Restore the receipt artifact referenced by the WAL before mutating segment files.
    ///
    /// # Errors
    /// Returns an error for missing, corrupt, or incompatible retry protection.
    pub async fn restore_retry_checkpoint(&mut self, through_op: u64) -> std::io::Result<()> {
        if through_op == 0 {
            if self.partition_dir.is_some()
                && let Err(error) = self.reclaim_retry_checkpoints(through_op).await
            {
                tracing::warn!(%error, namespace_raw = self.consensus().group(), "cannot reclaim orphan receipt checkpoints during recovery");
            }
            return Ok(());
        }
        let path = self.retry_checkpoint_path(through_op)?;
        let file = compio::fs::File::open(&path).await?;
        let length = file.metadata().await?.len();
        if length > RETRY_CHECKPOINT_BYTES_MAX as u64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "receipt checkpoint exceeds the allocation limit",
            ));
        }
        let capacity = usize::try_from(length).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "receipt checkpoint length exceeds address space",
            )
        })?;
        let (result, bytes) = file
            .read_exact_at(Vec::with_capacity(capacity), 0)
            .await
            .into();
        result?;
        let content = split_verified_trailer(&bytes).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "receipt checkpoint checksum mismatch",
            )
        })?;
        let mut cursor = LeCursor::new(content);
        let identity = (|| -> Result<bool, Truncated> {
            Ok(
                cursor.take(RETRY_CHECKPOINT_MAGIC.len())? == RETRY_CHECKPOINT_MAGIC
                    && cursor.u64()? == self.consensus().group()
                    && cursor.u64()? == self.created_revision()
                    && cursor.u64()? == through_op,
            )
        })()
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "receipt checkpoint identity is truncated",
            )
        })?;
        if !identity {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "receipt checkpoint identity mismatch",
            ));
        }
        let wire = ConsumerOffsetsWire::decode(cursor.remaining())
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        wire.validate_owners(self.created_revision(), through_op)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        let table = checked_retry_table(&wire, through_op, self.dedup().capacity())
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        *self.dedup_mut() = table;
        self.history_transition = wire.history_transition;
        self.owner_retirement = wire.owner_retirement;
        self.consumer_group_polls.clear();
        self.consumer_group_owners = wire
            .owners
            .iter()
            .map(|installed| (installed.installation.group_id, *installed))
            .collect();
        self.required_metadata_frontier = self
            .required_metadata_frontier
            .max(wire.required_metadata_frontier);
        if let Err(error) = self.reclaim_retry_checkpoints(through_op).await {
            tracing::warn!(%error, namespace_raw = self.consensus().group(), "cannot reclaim obsolete receipt checkpoints during recovery");
        }
        Ok(())
    }

    // Call only after publication or validated recovery, under the partition owner.
    pub(crate) async fn reclaim_retry_checkpoints(&self, through_op: u64) -> std::io::Result<()> {
        reclaim_retry_checkpoint_files(&self.retry_checkpoint_path(through_op)?, through_op > 0)
            .await
    }
}

/// Removes every receipt checkpoint next to `current`, and `current` too unless
/// `keep_current` is set.
pub(crate) async fn reclaim_retry_checkpoint_files(
    current: &Path,
    keep_current: bool,
) -> std::io::Result<()> {
    let directory = current.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "receipt checkpoint has no parent",
        )
    })?;
    let entries = match DiskStorage.entries(directory).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    for entry in entries {
        if entry.directory {
            continue;
        }
        let Some(name) = entry.name.to_str() else {
            continue;
        };
        let Some(number) = name.strip_prefix(RETRY_CHECKPOINT_PREFIX).and_then(|tail| {
            tail.strip_suffix(RETRY_CHECKPOINT_SUFFIX)
                .or_else(|| tail.strip_suffix(RETRY_CHECKPOINT_TEMP_SUFFIX))
        }) else {
            continue;
        };
        if number.parse::<u64>().is_err() {
            continue;
        }
        let path = directory.join(&entry.name);
        if keep_current && path == current {
            continue;
        }
        match DiskStorage.remove_file(&path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                // Leave failed removals discoverable by the next checkpoint or recovery.
                tracing::warn!(%error, path = %path.display(), "cannot remove obsolete receipt checkpoint");
            }
        }
    }
    DiskStorage.sync_directory(directory).await
}

/// Write `payload` to a new file at `path`. A staging name can be a hard link
/// to a live segment that the reuse scan linked. Writing through that name in
/// place changes the live segment, so the old name goes first.
async fn write_staging_file(path: &Path, payload: Vec<u8>) -> std::io::Result<()> {
    remove_if_present(path).await?;
    let mut file = compio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
        .note_descriptor_exhaustion(|| format!("creating {}", path.display()))?;
    let (result, _) = file.write_all_at(payload, 0).await.into();
    result?;
    file.sync_data().await?;
    Ok(())
}

async fn remove_if_present(path: &Path) -> std::io::Result<()> {
    match compio::fs::remove_file(path).await {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

/// Hard-link `live` to `staged` through a scratch name, so that the staging
/// name only ever names a link whose data is durable, and a failure keeps
/// the staged copy that `staged` held. A rename does nothing when both names
/// are links to one file, so the caller must not link a file that `staged`
/// already names.
async fn link_durably(live: &Path, staged: &Path) -> std::io::Result<()> {
    let scratch = staged.with_extension(LINK_SCRATCH_EXTENSION);
    let linked = async {
        remove_if_present(&scratch).await?;
        compio::fs::hard_link(live, &scratch).await?;
        compio::fs::File::open(&scratch)
            .await
            .note_descriptor_exhaustion(|| format!("opening {}", scratch.display()))?
            .sync_data()
            .await?;
        compio::fs::rename(&scratch, staged).await
    }
    .await;
    if linked.is_err() {
        // A later sweep of the staging names removes a name that this cannot.
        let _ = compio::fs::remove_file(&scratch).await;
    }
    linked
}

/// Whether `staged` names the file that `live` describes. A link takes its
/// staging name only once its data is durable, so a scan that finds one
/// there keeps it without another link or sync.
#[cfg(unix)]
async fn names_file_of(live: &compio::fs::Metadata, staged: &Path) -> bool {
    compio::fs::metadata(staged)
        .await
        .is_ok_and(|staged| staged.dev() == live.dev() && staged.ino() == live.ino())
}

/// Without inode numbers no scan can prove an earlier link, so each one
/// links again.
#[cfg(not(unix))]
async fn names_file_of(_live: &compio::fs::Metadata, _staged: &Path) -> bool {
    false
}
