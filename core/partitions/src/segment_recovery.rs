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

//! Segment recovery: the boot-time reader for the segment files this crate's
//! writers emit.
//!
//! Previously the bootstrap path borrowed `load_segments` from the legacy
//! server implementation to hydrate persisted segments. That loader
//! reads the legacy 16-byte dense per-message index through
//! `server_common::IndexReader`, but the server persists a 24-byte sparse index
//! ([`crate::IggyIndexWriter`]: one entry per flush, absolute `offset`,
//! `timestamp`, and batch-start `position`). Reading the 24-byte file with the
//! 16-byte parser mis-strides it (the "Index data must be exactly 16 bytes"
//! recovery panic). This module is that writer's matching loader, kept in the
//! same crate and reading the same 24-byte format it emits.

use crate::iggy_index::IGGY_INDEX_SIZE;
use crate::segment_anchor::ANCHOR_EXTENSION;
use crate::state_transfer::{STAGING_SUFFIX, segment_dir_entries};
use crate::{IggyIndex, IggyIndexReader, PartitionsConfig, Segment};
use compio::buf::{IntoInner, IoBuf};
use compio::fs;
use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use iggy_common::{IggyByteSize, IggyError, MAX_MESSAGE_SIZE_UPPER_BYTES, PartitionStats};
use server_common::fatal::NoteDescriptorExhaustion;
use server_common::fs_utils::truncate_file;
use server_common::send_messages::{BatchHeader, COMMAND_HEADER_SIZE, decode_batch_slice};
use server_common::sharding::IggyNamespace;
use server_common::{SegmentStorage, yield_to_reactor};
use std::io;
use std::path::{Path, PathBuf};
use tracing::{debug, error, info, warn};

const LOG_EXTENSION: &str = "log";
const INDEX_EXTENSION: &str = "index";

/// Window for the buffered walk, probe, and index scans. One allocation per
/// partition load, refilled forward on demand; batches larger than this fall
/// back to a single direct read.
const SCAN_WINDOW_CAPACITY: usize = 4 * 1024 * 1024;

/// Byte stride between rebuilt sparse index entries, mirroring the
/// state-transfer receiver's rebuild policy: lower-bound consumers are
/// correct at ANY density, but a per-batch index over a large segment
/// overshoots the sealed-index residency cap and demotes every sealed poll
/// to on-file binary search, so entries are spaced out. The first walked
/// batch always gets one.
const REBUILT_INDEX_STRIDE_BYTES: u64 = 64 * 1024;

/// Sparse index entries either index scan steps through between reactor
/// yields, on top of the per-refill yield the anchor search shares with the
/// walks. Neither scan is bounded by disk reads: an entry whose position is
/// past the log end is rejected on arithmetic alone (an index floored back to
/// a short log is mostly those), and the consistency scan reads a whole window
/// of entries per pread and then compares them in memory. Without this a
/// megabyte-scale index would hold the shard core -- signal handling included
/// -- for its whole length on every clean boot.
const INDEX_SCAN_YIELD_STRIDE: u64 = 1024;

/// Index entries the log may legitimately fail to back under `durable_segments`.
/// Persistence writes exactly one entry per flush chunk and chunks never
/// overlap. The WAL makes a body durable before it acknowledges it, and the
/// flush that indexes that body runs later still, so an entry existing on disk
/// proves the log bytes it names were already fdatasynced. Only the chunk in
/// flight when the process died can leave an entry the log never backed. See
/// [`PartitionRecoveryRefusal::FsyncedLogLoss`] for why a deeper step-back is
/// evidence about the log rather than about the index.
const MAX_FSYNCED_INDEX_STEP_BACK_ENTRIES: u64 = 1;

/// Index entries the backward anchor search probes before giving up, at one
/// 24-byte pread each. Capping the walk cannot change a verdict: only the
/// FIRST entry probed can leave the step-back within
/// [`MAX_FSYNCED_INDEX_STEP_BACK_ENTRIES`], so past that entry the refusal is
/// already decided and deeper probes only sharpen the byte it names. The
/// refusal carries the depth actually searched, so a capped search never
/// reads as "the log backs nothing".
const MAX_INDEX_ANCHOR_PROBE_ENTRIES: u64 = 4096;

/// Units the damage probes of one partition load may spend per byte of
/// residue they are asked to classify, at one unit per candidate offset
/// examined. Candidates never outnumber residue bytes, so an honest
/// front-to-back scan always fits under this multiple regardless of residue
/// width; only a shape that re-examines offsets can exhaust it. Deriving the
/// limit from the residue actually present -- rather than from any
/// configuration knob or frozen size constant -- makes it immune to knob
/// changes between boots by construction: no legal segment can be refused
/// because a limit was derived from a value the segment was not written
/// under. Exhaustion refuses recovery; it never falls through to the
/// truncating no-survivor verdict.
const PROBE_BUDGET_UNITS_PER_RESIDUE_BYTE: u64 = 2;

/// Bytes the damage probes of one partition load may hand to checksum
/// verification per byte of residue, charged BEFORE each slice is read.
/// Candidate enumeration alone does not bound this cost: candidates advance
/// one byte at a time, so the slices claimed by neighbouring plausible
/// headers OVERLAP, and residue packed with them (admissible producer
/// payload -- nothing has to be corrupted) would otherwise drive total
/// verified bytes toward residue times [`MAX_RECOVERABLE_BATCH_BYTES`].
/// An honest torn tail verifies almost nothing against this limit: zeros and
/// garbage never decode a header, a batch torn mid-write fails the file
/// bound before any verify, and the first batch that does verify ends the
/// probe -- so the multiple is generous for every real shape while capping
/// the crafted one at linear work. Residue-derived like the candidate
/// budget, for the same knob immunity.
const PROBE_VERIFY_BUDGET_BYTES_PER_RESIDUE_BYTE: u64 = 4;

/// Largest on-disk batch record recovery treats as plausible: the frozen
/// ceiling on `message_bus.max_message_size` -- the widest wire frame any
/// legal configuration admits, validated at boot -- plus one batch header of
/// slack in case an admission path counts its cap against the blob alone. A
/// header claiming more cannot be a real batch, so rejecting it at the
/// header is verdict-identical to reading the claimed bytes and failing the
/// verify, minus a claimed-size allocation and read that a single
/// bit-flipped length field could otherwise drive up to a whole segment.
const MAX_RECOVERABLE_BATCH_BYTES: u64 = MAX_MESSAGE_SIZE_UPPER_BYTES + COMMAND_HEADER_SIZE as u64;

/// Attempts at finding a free `<partition dir>.fenced.<n>` name, mirroring
/// the partition-level quarantine's bound.
const FENCED_DIR_PROBE_LIMIT: u32 = 1000;

/// Why a partition's recovered segments cannot be served.
///
/// Every shape here is structural -- the local files contradict themselves or
/// each other -- but they are distinguished because they point at different
/// causes, and not all of them are at-rest corruption: an empty non-tail
/// segment is a failed rebuild's orphan pairing, a hole is a stray or
/// half-unlinked file, interior damage is bit rot (or a resurrected tail
/// appended over), and offsets that do not continue the chain can be minted
/// into byte-clean files by an upstream crash window as well as by damage.
///
/// An index that contradicts itself is deliberately NOT here, and neither is
/// one the log cannot back UNLESS the topic runs under `persisted durability` and the
/// gap is deeper than the single in-flight entry: entries are derived from the
/// log, so recovery drops such an index whole and rebuilds it from a byte-0
/// walk of the log rather than believing any part of it. What `persisted durability`
/// adds is evidence from serialized completed flushes: an entry above chunk N
/// means the log fdatasync covering chunk N completed before the later flush
/// began. This is independent of reply timing and turns a deeper gap into
/// evidence about the LOG. Absent that evidence the index only locates data;
/// recovery verifies the log from byte 0.
#[derive(Debug)]
pub enum PartitionRecoveryRefusal {
    /// `recoverable_bytes` on the two chain-shape refusals is the sum of
    /// walked, decodable bytes across the whole planned chain: the evidence
    /// the single-replica boot arm needs to decide whether fencing and
    /// rebuilding empty loses anything (0 means the chain provably held
    /// nothing servable; anything else is data a rebuild would hide).
    EmptyNonTailSegment {
        empty_start: u64,
        next_start: u64,
        recoverable_bytes: u64,
    },
    Hole {
        previous_start: u64,
        previous_end: u64,
        next_start: u64,
        recoverable_bytes: u64,
    },
    /// A complete, checksum-verifying batch survives PAST bytes that do not
    /// decode. A torn tail has nothing after it, so this is interior damage,
    /// and truncating at it would silently discard the surviving batches.
    InteriorDamage {
        start_offset: u64,
        damage_position: u64,
        survivor_position: u64,
    },
    /// Bytes past the walked prefix that the damage probe could not
    /// classify: it ran out of a work budget before proving or disproving a
    /// survivor. The candidate budget is sized so a front-to-back scan of
    /// every residue in the load always fits (its exhaustion means offsets
    /// were re-examined -- a probe defect); the verification budget bounds
    /// the bytes handed to checksum verifies, whose claimed slices overlap,
    /// so residue packed with plausible headers can exhaust it from an
    /// on-disk shape. The index anchor search charges the same verification
    /// budget as it steps back through entries the log cannot back, so an
    /// index packed with claims the log never proves ends here too instead
    /// of paying a verify per entry. Truncation is only ever sound for a
    /// proven torn tail, so giving up keeps the bytes. The residue width is
    /// diagnostic only; it is not a gate.
    UnverifiedResidue {
        start_offset: u64,
        damage_position: u64,
        residue_bytes: u64,
        candidates_examined: u64,
        budget_units: u64,
        verified_bytes: u64,
        verify_budget_bytes: u64,
    },
    /// A batch whose checksum verifies does not continue the offset chain,
    /// so offsets are not contiguous inside one segment file. The verify is
    /// what earns the refusal: an UNVERIFIED mismatch is damage and goes to
    /// the probe (a torn tail truncates). The cause is not necessarily
    /// at-rest damage: a crash window that leaves the durable offset
    /// frontier past the recovered end offset stamps the same shape into
    /// byte-clean files.
    OffsetDiscontinuity {
        start_offset: u64,
        expected_offset: u64,
        found_offset: u64,
        position: u64,
    },
    /// A batch whose checksum verifies carries another partition's own
    /// `partition_id` stamp: a real record that landed in the wrong file (a
    /// misdirected write, a recycled block, an operator copy), not damage.
    /// Adopting it would seed this partition's offset space from foreign
    /// data; truncating it would destroy the only evidence of the misdirect.
    ForeignBatch {
        start_offset: u64,
        batch_partition_id: u64,
        position: u64,
    },
    /// The sparse index of a topic running under `persisted durability` outruns its
    /// log by more than the one entry a crash can legitimately strand there.
    /// Persistence writes exactly one entry per flush chunk and chunks never
    /// overlap. The WAL makes a body durable before acknowledging it and the
    /// flush indexes it later, so every entry on disk names a chunk whose log
    /// bytes completed their fdatasync. A completed
    /// chunk can contain batches acknowledged before the flush threshold was
    /// reached, while the in-flight chunk can do so too. Reply timing is not
    /// the proof. Only the chunk in flight when the process died can have an
    /// entry the log never backed. A deeper step-back therefore says the LOG
    /// lost bytes it had already made durable, and rebuilding from what remains
    /// could re-mint offsets, including offsets already returned to clients.
    FsyncedLogLoss {
        start_offset: u64,
        entry_count: u64,
        provable_entries: u64,
        /// Position of the highest entry the log still proves; 0 when it
        /// proves none, which `provable_entries` disambiguates.
        provable_position: u64,
        /// Entries the backward search actually probed, which its own cap
        /// holds below `entry_count` on a long index: `provable_entries == 0`
        /// then means nothing proved in the searched window, not that the log
        /// backs nothing.
        searched_entries: u64,
    },
    /// Under `persisted durability`, the byte-0 rebuild after a dropped index proved
    /// the log only through `walked_position`, short of `durable_position`,
    /// the byte the index's own last entry proves the log had already
    /// fdatasynced through (the flush that wrote the entry began only after
    /// the previous chunk's log sync completed). The step-back gate measures
    /// loss at entry granularity; this catches the sub-chunk shape it cannot:
    /// bytes a completed flush made durable are gone mid-chunk, so truncating
    /// to the walked prefix would re-mint their offsets.
    FsyncedRebuildShortfall {
        start_offset: u64,
        entry_count: u64,
        walked_position: u64,
        durable_position: u64,
    },
    PrepareWal {
        directory: PathBuf,
        source: std::io::Error,
    },
    CheckpointSizeMismatch {
        start_offset: u64,
        validated_bytes: u64,
        expected_bytes: u64,
    },
    /// The physical file length differs from the required recovered boundary.
    StorageSizeMismatch {
        start_offset: u64,
        on_disk_bytes: u64,
        expected_bytes: u64,
    },
}

impl std::fmt::Display for PartitionRecoveryRefusal {
    // One arm per refusal shape; length tracks the enum, not complexity.
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyNonTailSegment {
                empty_start,
                next_start,
                recoverable_bytes,
            } => write!(
                f,
                "segment {empty_start} is empty yet {next_start} follows it, so the \
                 chain ({recoverable_bytes} recoverable bytes) cannot be served \
                 past it"
            ),
            Self::Hole {
                previous_start,
                previous_end,
                next_start,
                recoverable_bytes,
            } => write!(
                f,
                "segment {previous_start} ends at offset {previous_end} but the next \
                 starts at {next_start}, leaving a hole in a chain holding \
                 {recoverable_bytes} recoverable bytes"
            ),
            Self::InteriorDamage {
                start_offset,
                damage_position,
                survivor_position,
            } => write!(
                f,
                "segment {start_offset} holds undecodable bytes at {damage_position} \
                 with a complete verifying batch after them at {survivor_position}; \
                 not a torn tail, and truncating would discard durable batches"
            ),
            Self::UnverifiedResidue {
                start_offset,
                damage_position,
                residue_bytes,
                candidates_examined,
                budget_units,
                verified_bytes,
                verify_budget_bytes,
            } => write!(
                f,
                "segment {start_offset} holds {residue_bytes} bytes past the walked \
                 prefix at {damage_position} that the damage probe could not \
                 classify before exhausting its work budgets ({candidates_examined} \
                 candidate offsets examined of {budget_units} allowed; \
                 {verified_bytes} bytes handed to verification of \
                 {verify_budget_bytes} allowed); truncating unproven bytes could \
                 destroy durable batches"
            ),
            Self::OffsetDiscontinuity {
                start_offset,
                expected_offset,
                found_offset,
                position,
            } => write!(
                f,
                "segment {start_offset} holds a verified batch at byte {position} \
                 whose base offset {found_offset} does not continue the chain at \
                 {expected_offset}"
            ),
            Self::ForeignBatch {
                start_offset,
                batch_partition_id,
                position,
            } => write!(
                f,
                "segment {start_offset} holds a verified batch at byte {position} \
                 stamped for partition {batch_partition_id}; a foreign record in \
                 this log is preserved as evidence, not truncated"
            ),
            Self::FsyncedLogLoss {
                start_offset,
                entry_count,
                provable_entries,
                provable_position,
                searched_entries,
            } => write!(
                f,
                "segment {start_offset} runs under persisted durability with {entry_count} sparse \
                 index entries, but its log backs only {provable_entries} of the \
                 {searched_entries} searched from the top (up to byte {provable_position}); \
                 every entry below the last describes a log chunk whose fdatasync had \
                 completed, so the log has lost previously durable data rather than the \
                 index having outrun it"
            ),
            Self::FsyncedRebuildShortfall {
                start_offset,
                entry_count,
                walked_position,
                durable_position,
            } => write!(
                f,
                "segment {start_offset} runs under persisted durability with {entry_count} sparse \
                 index entries, and the byte-0 rebuild proved its log only through byte \
                 {walked_position}, short of byte {durable_position} which the last \
                 entry's own fdatasync ordering proves the log had already made durable; \
                 the log has lost previously durable bytes mid-chunk, so rebuilding \
                 would re-mint their offsets"
            ),
            Self::PrepareWal { directory, source } => write!(
                f,
                "prepare WAL at {} cannot be recovered: {source}",
                directory.display()
            ),
            Self::CheckpointSizeMismatch {
                start_offset,
                validated_bytes,
                expected_bytes,
            } => write!(
                f,
                "segment {start_offset} validated prefix has {validated_bytes} bytes, \
                 but the WAL checkpoint requires {expected_bytes}"
            ),
            Self::StorageSizeMismatch {
                start_offset,
                on_disk_bytes,
                expected_bytes,
            } => write!(
                f,
                "segment {start_offset} file length {on_disk_bytes} diverged from \
                 its required recovered size {expected_bytes}"
            ),
        }
    }
}

/// Why a partition could not be brought back from its on-disk state.
#[derive(Debug)]
pub enum PartitionRecoveryError {
    /// Per-partition, not fatal: the boot path fences this one group instead
    /// of taking the node down for one damaged local chain. Only STRUCTURAL
    /// refusals route here -- shapes where the local files contradict
    /// themselves, so a retried boot cannot help. Transient recovery I/O
    /// failures (stat, open, read, truncate, fsync) stay node-fatal on
    /// purpose: a retried boot can still serve that partition, while fencing
    /// it would quarantine healthy data.
    ///
    /// The Display text deliberately claims nothing about what happens to the
    /// refused files: disposition (quarantine into `.fenced.N` vs tombstone
    /// with files left in place) is decided by the caller's arms that catch
    /// this error, and only they log it -- a claim here would render beside
    /// theirs and contradict one branch or the other.
    Refused {
        dir: PathBuf,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        reason: PartitionRecoveryRefusal,
    },
    /// An offset directory could not be checked, or an existing one could not
    /// be enumerated.
    ConsumerOffsetsLoad {
        consumer_kind: &'static str,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        path: String,
        source: Box<IggyError>,
    },
    /// An offset directory that boot creates, such as the external group one
    /// for a partition made before that kind existed, could not be created.
    CreateOffsetsDirectory {
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        path: String,
        source: std::io::Error,
    },
    /// Transient I/O with no structural verdict attached. Boot fails on it
    /// rather than fencing, so a retried boot can still serve the partition.
    /// The reconciler logs it and retries the partition with backoff.
    Iggy(IggyError),
}

impl std::fmt::Display for PartitionRecoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused {
                dir,
                stream_id,
                topic_id,
                partition_id,
                reason,
            } => write!(
                f,
                "partition {stream_id}/{topic_id}/{partition_id} at {} refused storage \
                 recovery: {reason}",
                dir.display()
            ),
            Self::ConsumerOffsetsLoad {
                consumer_kind,
                stream_id,
                topic_id,
                partition_id,
                path,
                ..
            } => write!(
                f,
                "failed to load persisted {consumer_kind} offsets for stream {stream_id}, \
                 topic {topic_id}, partition {partition_id} from {path}"
            ),
            Self::CreateOffsetsDirectory {
                stream_id,
                topic_id,
                partition_id,
                path,
                source,
            } => write!(
                f,
                "failed to create offsets directory {path} for stream {stream_id}, \
                 topic {topic_id}, partition {partition_id}: {source}"
            ),
            Self::Iggy(source) => write!(f, "{source}"),
        }
    }
}

impl std::error::Error for PartitionRecoveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            // Both put the cause in the Display text, the only part boot logs.
            Self::Refused { .. } | Self::CreateOffsetsDirectory { .. } => None,
            Self::ConsumerOffsetsLoad { source, .. } => Some(source.as_ref()),
            Self::Iggy(source) => std::error::Error::source(source),
        }
    }
}

impl From<IggyError> for PartitionRecoveryError {
    fn from(source: IggyError) -> Self {
        Self::Iggy(source)
    }
}

/// A persisted segment recovered from disk: its metadata plus the storage
/// handles opened over its `.log` / `.index` files. Only the last segment of
/// a chain keeps its write cursors.
pub struct RecoveredSegment {
    pub segment: Segment,
    pub storage: SegmentStorage,
}

/// Loads every persisted segment for a partition, sorted by start offset.
///
/// Segment offsets and timestamps are recovered from the 24-byte sparse index
/// (see module docs); segment byte size comes from walking the `.log` batch
/// chain. Recovery runs in three passes: every segment is bounded first
/// without touching an existing byte (pass A's only write is staging each
/// rebuilt index in a fresh `.staging` file), then the chain guard runs over
/// those bounds, and only an accepted chain is made physical -- torn tails
/// truncated, staged indexes renamed into place, unreadable segments fenced
/// aside -- before storage opens over it. A refusal raised in pass A or B
/// therefore leaves every pre-existing file byte-identical to what boot found
/// (staged scratch is swept at the next boot or quarantined with the fence).
/// Pass C is NOT atomic across segments: its own refusals -- the storage-open
/// guard -- can land after earlier segments in the chain were already
/// truncated. The last segment is left unsealed so it can accept further
/// writes.
///
/// # Errors
///
/// Transient I/O failures (listing, stat, open, read, truncate, fsync) are
/// returned as-is and abort the boot so it can be retried. Structural
/// contradictions -- a holed chain, damage with intact batches after it,
/// residue the damage probe could not classify within its limits -- return
/// [`PartitionRecoveryError::Refused`] so the caller can fence this one
/// partition instead of taking the node down.
///
/// `durable_segments` is the topic's own effective value, not a hint: it is what
/// makes a durable index entry evidence about the log (see
/// [`PartitionRecoveryRefusal::FsyncedLogLoss`]), so passing it wrong either
/// refuses healthy chains or hides previously durable data loss.
///
/// Takes no offset ceiling. A legitimate gap is proved by the anchor the boot
/// re-anchor writes beside the segment it plants, not inferred from how far the
/// superblock's reservation happens to reach.
pub async fn load_persisted_segments(
    config: &PartitionsConfig,
    namespace: IggyNamespace,
    segment_size: IggyByteSize,
    durable_segments: bool,
    stats: &PartitionStats,
) -> Result<Vec<RecoveredSegment>, PartitionRecoveryError> {
    load_persisted_segments_with_checkpoint(
        config,
        namespace,
        segment_size,
        durable_segments,
        stats,
        None,
    )
    .await
}

/// [`load_persisted_segments`] bounded by the partition WAL's `checkpoint`.
///
/// The checkpointed segment's log is walked over exactly `checkpoint.length`
/// bytes and never truncated, segments past it are left out, and every segment
/// opens without a messages writer, because the WAL owns the bytes past the
/// checkpoint.
///
/// # Errors
///
/// As [`load_persisted_segments`], plus [`PartitionRecoveryError::Refused`]
/// with [`PartitionRecoveryRefusal::StorageSizeMismatch`] when the
/// checkpointed file is shorter than the checkpoint, or with
/// [`PartitionRecoveryRefusal::CheckpointSizeMismatch`] when the walk proves
/// a different length.
#[allow(clippy::too_many_lines)]
pub async fn load_persisted_segments_with_checkpoint(
    config: &PartitionsConfig,
    namespace: IggyNamespace,
    segment_size: IggyByteSize,
    durable_segments: bool,
    stats: &PartitionStats,
    checkpoint: Option<journal::partition_journal::SegmentPosition>,
) -> Result<Vec<RecoveredSegment>, PartitionRecoveryError> {
    let stream_id = namespace.stream_id();
    let topic_id = namespace.topic_id();
    let partition_id = namespace.partition_id();
    let partition_path = config.get_partition_path(stream_id, topic_id, partition_id);
    let identity = PartitionIdentity {
        partition_path: &partition_path,
        stream_id,
        topic_id,
        partition_id,
    };
    // ONE directory walk feeds both: the sweep only ever unlinks `.staging` and
    // orphan `.index` files, never a `.log`, so the log stems it already
    // collects ARE the post-sweep start-offset set. Note the error policy is
    // the collect side's (NotFound => empty, anything else => refuse boot); the
    // sweep's silent return would swallow an EACCES that must not be ignored.
    let mut start_offsets = sweep_scratch_files_and_collect_offsets(&partition_path).await?;
    start_offsets.sort_unstable();
    if let Some(checkpoint) = checkpoint {
        start_offsets.retain(|offset| {
            *offset < checkpoint.next_offset || *offset == checkpoint.start_offset
        });
    }

    let max_size = segment_size;
    let mut scratch = ScanScratch::default();

    // Pass A: derive every segment's bounds without touching an existing
    // byte (the only write is each rebuilt index staged to a fresh
    // `.staging` scratch file). Nothing moves until the WHOLE chain is
    // accepted, so a refusal raised by a later segment (or by the chain
    // guard) leaves the earlier segments' files byte-identical for the
    // caller's quarantine to keep.
    let mut planned = Vec::with_capacity(start_offsets.len());
    for start_offset in start_offsets {
        let messages_path =
            config.get_messages_path(stream_id, topic_id, partition_id, start_offset);
        let index_path = config.get_index_path(stream_id, topic_id, partition_id, start_offset);

        let raw_messages_size = file_len(&messages_path).await?;
        let checkpoint_segment =
            checkpoint.is_some_and(|checkpoint| checkpoint.start_offset == start_offset);
        let messages_size = checkpoint
            .filter(|checkpoint| checkpoint.start_offset == start_offset)
            .map_or(raw_messages_size, |checkpoint| checkpoint.length);
        if raw_messages_size < messages_size {
            return Err(
                identity.refusal(PartitionRecoveryRefusal::StorageSizeMismatch {
                    start_offset,
                    on_disk_bytes: raw_messages_size,
                    expected_bytes: messages_size,
                }),
            );
        }
        let bounds = if checkpoint_segment {
            let messages = open_messages_file(identity, &messages_path).await?;
            let mut scanner = FileScanner::new(&messages, messages_size, &mut scratch);
            recover_by_walking_log(
                identity,
                &mut scanner,
                &messages_path,
                start_offset,
                messages_size,
                RewriteCause::Checkpoint,
            )
            .await?
        } else {
            recover_segment_bounds(
                identity,
                &index_path,
                &messages_path,
                start_offset,
                raw_messages_size,
                durable_segments,
                &mut scratch,
            )
            .await?
        };
        if checkpoint_segment
            && bounds.as_ref().map_or(0, |bounds| bounds.messages_size) != messages_size
        {
            return Err(
                identity.refusal(PartitionRecoveryRefusal::CheckpointSizeMismatch {
                    start_offset,
                    validated_bytes: bounds.as_ref().map_or(0, |bounds| bounds.messages_size),
                    expected_bytes: messages_size,
                }),
            );
        }

        // `bounds == None` means the log holds no whole batch ANYWHERE: the
        // index-less walk tried from byte 0 and the damage probe found no
        // surviving batch deeper in the file. There is nothing to serve:
        // zeroed sizes seed fresh empty files (pass C fences the unreadable
        // originals aside rather than deleting them), where counting the
        // bytes with `end_offset == start_offset` would fabricate one
        // phantom message for the bootstrap non-empty filters and strand
        // undecodable garbage inside the readable range. Note this is NOT
        // tail-only -- the log and the index persist concurrently under
        // every config, so a torn index is reachable mid-chain, which is why
        // the walk exists rather than refusing the partition.
        let recovered_empty = bounds.is_none() && !checkpoint_segment;
        let bounds = bounds.unwrap_or_else(|| {
            if raw_messages_size > 0 && !checkpoint_segment {
                warn!(
                    stream_id,
                    topic_id,
                    partition_id,
                    start_offset,
                    messages_size = raw_messages_size,
                    "segment log holds bytes but no whole batch decodes \
                     anywhere in it (torn write); recovering the segment as \
                     empty and fencing its files aside"
                );
            }
            WalkedBounds {
                start_timestamp: 0,
                end_timestamp: 0,
                end_offset: start_offset,
                messages_size: 0,
                index_size: 0,
                rebuilt_index: None,
            }
        });

        // Staged now so pass C can install it with one atomic rename, and so
        // a long chain never holds more than one rebuilt index in memory.
        let rebuilt_index_staging = match bounds.rebuilt_index {
            Some(entries) => Some(stage_rebuilt_index(&index_path, entries).await?),
            None => None,
        };

        let mut segment = Segment::new(start_offset, max_size);
        segment.sealed = true;
        segment.start_timestamp = bounds.start_timestamp;
        segment.end_timestamp = bounds.end_timestamp;
        segment.max_timestamp = bounds.end_timestamp;
        segment.end_offset = bounds.end_offset;
        segment.size = IggyByteSize::from(bounds.messages_size);
        segment.current_position = bounds.messages_size;

        planned.push(PlannedSegment {
            segment,
            messages_path,
            index_path,
            index_size: bounds.index_size,
            rebuilt_index_staging,
            recovered_empty,
        });
    }

    if let Some(last) = planned.last_mut() {
        last.segment.sealed = false;
    }

    // Pass B: the chain guard reads the planned bounds and, for a gap, the
    // anchor beside it, so it can refuse BEFORE anything is truncated.
    ensure_contiguous_chain(identity, &planned).await?;

    // Pass C: the chain is accepted; make disk match the bounds and open
    // storage over them.
    let mut recovered = Vec::with_capacity(planned.len());
    for plan in planned {
        let messages_size = plan.segment.size.as_bytes_u64();
        if plan.recovered_empty {
            // The pair holds bytes that prove nothing, yet they are the only
            // copy of whatever the crash tore: move them aside and seed fresh
            // empty files rather than truncating them away.
            fence_unrecoverable_segment_files(
                identity,
                &plan.messages_path,
                &plan.index_path,
                plan.segment.start_offset,
            )
            .await?;
        }
        // Log first, index second: an index is kept only when a whole batch
        // verifies at its LAST entry's position, so the walked log length
        // strictly exceeds that position and every kept
        // entry still points inside the shortened log even if
        // a crash lands between the two mutations -- and a crash that lands
        // there anyway leaves an index running ahead of its log, which the
        // next boot discards and rebuilds instead of refusing. The
        // staged-rebuild install is reached from a populated index too, so
        // a crash between the truncate and the rename leaves that unbacked
        // index over the shortened log; the next boot re-discards it and
        // rebuilds from the log again, and truncation is monotone, so the
        // pair converges.
        let checkpoint_segment = checkpoint
            .is_some_and(|checkpoint| checkpoint.start_offset == plan.segment.start_offset);
        if !checkpoint_segment {
            truncate_to(&plan.messages_path, messages_size, RewriteCause::Damage).await?;
        }
        if let Some(staging_path) = &plan.rebuilt_index_staging {
            install_rebuilt_index(staging_path, &plan.index_path, identity.partition_path).await?;
        } else {
            let cause = if checkpoint_segment {
                RewriteCause::Checkpoint
            } else {
                RewriteCause::Damage
            };
            truncate_to(&plan.index_path, plan.index_size, cause).await?;
        }

        let mut storage = if checkpoint.is_some() {
            SegmentStorage::with_read_only_messages(
                &plan.messages_path,
                &plan.index_path,
                plan.index_size,
                true,
                None,
            )
            .await
        } else {
            SegmentStorage::new(
                &plan.messages_path,
                &plan.index_path,
                messages_size,
                plan.index_size,
                true,
            )
            .await
        }
        .map_err(|source| {
            error!(
                stream_id,
                topic_id,
                partition_id,
                path = %plan.messages_path,
                error = %source,
                "failed to open persisted segment storage during recovery"
            );
            // The seed-vs-stat guard refusing the open is a post-condition
            // assertion on the truncation this pass just performed: it can
            // only fire if the filesystem lied about a length or a change
            // broke the truncate-then-open contract. Kept as defense-in-depth
            // and routed as a structural refusal (fence one partition, not
            // the node) because a retried boot cannot help. Everything else
            // here is transient I/O and stays node-fatal.
            match source {
                IggyError::SegmentSizeMismatchAtOpen(on_disk_bytes, expected_bytes) => identity
                    .refusal(PartitionRecoveryRefusal::StorageSizeMismatch {
                        start_offset: plan.segment.start_offset,
                        on_disk_bytes,
                        expected_bytes,
                    }),
                transient => transient.into(),
            }
        })?;
        // Only the tail takes writes, as after a rotation.
        if plan.segment.sealed {
            storage.seal();
        }

        stats.increment_segments_count(1);
        stats.increment_size_bytes(messages_size);
        if messages_size > 0 {
            // The count is the offset span this segment now advertises, which
            // is what a consumer can ask for. A byte-0 walk proves every
            // offset in a non-fsynced or rebuilt span. An fsynced anchored
            // walk proves the final chunk and inherits the earlier prefix from
            // completed serialized log fdatasyncs. Counting the whole span
            // therefore does not widen the recovery claim. Saturating: an end
            // offset at u64::MAX must not wrap the counter.
            stats.increment_messages_count(
                plan.segment
                    .end_offset
                    .saturating_sub(plan.segment.start_offset)
                    .saturating_add(1),
            );
        }

        recovered.push(RecoveredSegment {
            segment: plan.segment,
            storage,
        });
    }

    Ok(recovered)
}

/// Identity of the partition being recovered, threaded through the walk for
/// logs and refusal construction.
#[derive(Clone, Copy)]
struct PartitionIdentity<'load> {
    partition_path: &'load str,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
}

impl PartitionIdentity<'_> {
    fn refusal(&self, reason: PartitionRecoveryRefusal) -> PartitionRecoveryError {
        PartitionRecoveryError::Refused {
            dir: PathBuf::from(self.partition_path),
            stream_id: self.stream_id,
            topic_id: self.topic_id,
            partition_id: self.partition_id,
            reason,
        }
    }
}

/// Pass A output for one segment: the recovered metadata plus what pass C
/// must make true on disk once the whole chain is accepted.
struct PlannedSegment {
    segment: Segment,
    messages_path: String,
    index_path: String,
    index_size: u64,
    /// Path of the staged rebuilt index pass C renames over `index_path`.
    rebuilt_index_staging: Option<String>,
    /// The pair holds bytes but nothing in them decodes; pass C fences the
    /// files aside and reseeds empty ones instead of truncating.
    recovered_empty: bool,
}

/// Work bounds shared by every damage probe in one partition load.
///
/// Two independent counters, because the probe pays for two different
/// things. ENUMERATION: one unit per candidate byte offset examined, growing
/// by [`PROBE_BUDGET_UNITS_PER_RESIDUE_BYTE`] per residue byte -- examining
/// is flat-cost by construction (the header decode bails on an undersized
/// length or the first nonzero reserved byte), so this only fires on a probe
/// defect that re-examines offsets. VERIFICATION: the bytes of every slice
/// handed to the checksum verify, in-window slices included (an in-window
/// verify still hashes every message up to the first bad checksum), growing
/// by [`PROBE_VERIFY_BUDGET_BYTES_PER_RESIDUE_BYTE`] per residue byte --
/// candidate slices overlap, so nothing else bounds their total. Window
/// refills are charged against neither; they advance strictly forward, so
/// they are linear in the residue on their own.
///
/// Scoped to the LOAD, not to one probe: pass A probes every segment before
/// pass B can refuse the chain, so a per-probe budget would multiply the
/// worst case by the segment count.
///
/// The limits are therefore a sum of grants, not a function of any one
/// residue: the index anchor search grows them over the log spans it steps
/// across and the damage probe grows them again over residue that can overlap
/// those spans, so a segment whose index was walked backward carries a larger
/// allowance than its own residue would buy. Exhaustion only ever refuses and
/// never truncates, so the looser bound buys boot work, not a weaker verdict.
#[derive(Default)]
struct ProbeBudget {
    limit_units: u64,
    spent_units: u64,
    verify_limit_bytes: u64,
    verify_spent_bytes: u64,
}

impl ProbeBudget {
    const fn grow_for_residue(&mut self, residue_bytes: u64) {
        self.limit_units = self
            .limit_units
            .saturating_add(residue_bytes.saturating_mul(PROBE_BUDGET_UNITS_PER_RESIDUE_BYTE));
        self.verify_limit_bytes = self.verify_limit_bytes.saturating_add(
            residue_bytes.saturating_mul(PROBE_VERIFY_BUDGET_BYTES_PER_RESIDUE_BYTE),
        );
    }

    /// Charges one candidate; `false` means the budget is exhausted and the
    /// probe must give up without a verdict.
    const fn charge_candidate(&mut self) -> bool {
        self.spent_units = self.spent_units.saturating_add(1);
        self.spent_units <= self.limit_units
    }

    /// Charges one verify slice by the bytes it would hash. Called BEFORE
    /// the slice is read, so exhaustion never pays for the slice that broke
    /// the budget; `false` means the probe must give up without a verdict.
    const fn charge_verify(&mut self, slice_bytes: u64) -> bool {
        self.verify_spent_bytes = self.verify_spent_bytes.saturating_add(slice_bytes);
        self.verify_spent_bytes <= self.verify_limit_bytes
    }
}

/// Readable bounds recovered for one segment holding data.
struct WalkedBounds {
    start_timestamp: u64,
    end_timestamp: u64,
    end_offset: u64,
    messages_size: u64,
    index_size: u64,
    rebuilt_index: Option<Vec<u8>>,
}

/// What the batch chain walked from one index entry proves.
struct AnchoredWalk {
    /// Timestamp of the first non-empty batch proved by this walk.
    start_timestamp: Option<u64>,
    end_offset: u64,
    end_timestamp: u64,
    /// First byte past the last whole batch the walk proved.
    position: u64,
}

/// Reusable buffers for the walk, probe, and index validation scans, plus the
/// probe work budget they share, allocated once per partition load.
#[derive(Default)]
struct ScanScratch {
    window: Vec<u8>,
    spill: Vec<u8>,
    probe_budget: ProbeBudget,
}

/// Contiguity guard: recovery takes every `.log` stem in the directory, so a
/// stray file (an unlink a failed state-transfer install could not finish,
/// an operator copy) would otherwise splice a hole or an overlap into the
/// chain and push `current_offset` past data this replica does not hold.
/// Refuse loudly instead of serving a holed log.
///
/// A FORWARD gap is admitted only when the far side carries a
/// [`SegmentAnchor`] naming exactly the near side, which is the record the boot
/// re-anchor writes before it plants. Nothing else legitimises a gap: an
/// overlap, a backwards pair, or a gap with no anchor is damage.
///
/// The anchors are what a monotone offset ceiling could not be. A ceiling says
/// only "some boot claimed up to N", and every plant base sits below the current
/// N -- but so does the successor of a segment that was deleted, so a lost middle
/// segment read as a plant. The anchor is written by the one component that
/// creates legitimate gaps, names which segment it sealed, and is swept as soon
/// as its segment is gone.
///
/// Retention needs no allowance: it removes a contiguous FRONT prefix, so the
/// remaining chain stays contiguous and no interior gap appears.
///
/// # Known residual
///
/// An anchor whose planted segment survived while the sealed segment it names was
/// itself lost still reads as legitimate, because the pair the anchor describes
/// is then simply absent from the chain and the guard never examines it. Catching
/// that needs a durable count of the chain, which this record does not carry.
///
/// Runs on the planned bounds alone, BEFORE any truncation, so the segment
/// files a refusal quarantines are exactly the bytes boot found. The refusal
/// names the partition and its directory so the caller can fence THAT group
/// rather than abort the node's boot: the shapes it rejects are exactly what
/// a failed quarantine leaves behind, and one damaged local chain must not
/// take the whole node down.
async fn ensure_contiguous_chain(
    identity: PartitionIdentity<'_>,
    planned: &[PlannedSegment],
) -> Result<(), PartitionRecoveryError> {
    // Walked, decodable bytes across the whole chain: the refusals carry it
    // so the single-replica boot arm can tell a shape with nothing servable
    // at stake (fence and rebuild empty) from one guarding real data
    // (tombstone). The verdict variant alone cannot: both shapes here can
    // fire over fully populated chains.
    let recoverable_bytes = planned
        .iter()
        .map(|plan| plan.segment.size.as_bytes_u64())
        .sum::<u64>();
    for pair in planned.windows(2) {
        let previous = &pair[0].segment;
        let next = &pair[1].segment;
        // A NON-tail empty segment can only be an orphan pairing: the torn-
        // tail leniency (an index-less crash tail recovered as empty) only
        // ever applies to the LAST element, and a size-0 segment followed by
        // more chain is exactly what a failed converge rebuild leaves behind.
        // Skipping it here was the guard's blind spot.
        if previous.size == IggyByteSize::default() {
            return Err(
                identity.refusal(PartitionRecoveryRefusal::EmptyNonTailSegment {
                    empty_start: previous.start_offset,
                    next_start: next.start_offset,
                    recoverable_bytes,
                }),
            );
        }
        // `checked_add`, not `+`: an end offset at u64::MAX must read as a
        // hole (no start offset can follow it), not overflow.
        if previous.end_offset.checked_add(1) == Some(next.start_offset) {
            continue;
        }
        // FORWARD only, and only with the plant's own record beside it. Start
        // offsets come off the file names so they ascend, but each end offset is
        // walked from that file's own bytes with nothing clamping it against the
        // next start, so a half-installed transfer or an operator copy can leave
        // a pair that overlaps -- and no re-anchor ever plants a segment whose
        // range a predecessor already covers.
        // Read HERE rather than collected up front: only a gap needs an anchor,
        // so a contiguous chain -- every chain that never crashed mid-block --
        // opens no file at all, and the ones that do are already walking this
        // pair. An unreadable anchor is unknown, not absent, and treating it as
        // absent would refuse a healthy chain for as long as the fault lasts.
        let read = crate::segment_anchor::read_anchor(identity.partition_path, next.start_offset)
            .await
            .map_err(|error| {
                error!(
                    partition_path = identity.partition_path,
                    start_offset = next.start_offset,
                    %error,
                    "failed to read a segment anchor during recovery"
                );
                PartitionRecoveryError::from(IggyError::CannotReadFile)
            })?;
        let anchored = previous.end_offset < next.start_offset
            && read.is_some_and(|anchor| {
                anchor.covers(
                    next.start_offset,
                    previous.start_offset,
                    previous.end_offset,
                )
            });
        if !anchored {
            return Err(identity.refusal(PartitionRecoveryRefusal::Hole {
                previous_start: previous.start_offset,
                previous_end: previous.end_offset,
                next_start: next.start_offset,
                recoverable_bytes,
            }));
        }
        info!(
            partition_path = identity.partition_path,
            previous_start = previous.start_offset,
            previous_end = previous.end_offset,
            next_start = next.start_offset,
            "admitted a gap in the recovered segment chain: the planted segment \
             carries the boot re-anchor's own record of it"
        );
    }
    Ok(())
}

/// Unlink the partition directory's scratch leftovers: every `*.staging` spill
/// file, and every `.index` with no `.log` beside it.
///
/// Boot is the one sweep that always runs. The install-time and reuse-time
/// staging sweeps only fire on the NEXT transfer attempt, so a transfer
/// abandoned for good would otherwise leak a full partition copy across
/// restarts; staging files are pure scratch (never a rename source until an
/// install owns them), so unlinking is always safe.
///
/// Orphaned indexes come from the state-transfer install, which renames ALL
/// indexes to their final names, fsyncs the directory, and only then renames the
/// logs -- a crash in that window is GUARANTEED to leave final-name `.index`
/// files with no `.log`. Recovery keys on `.log` stems, so nothing else ever
/// looks at them again: they are invisible to it and to the size stats, and
/// without this they are a permanent leak at offsets the partition may never
/// revisit. Unlinking rather than keeping them is safe because every path that
/// recreates a segment at a given base offset opens its index through
/// `SegmentStorage::new(.., file_exists = false)` first, which TRUNCATES: the
/// stale entries are never read, only overwritten.
/// Sweeps boot-time scratch (`.staging` spill, orphan `.index`) and returns the
/// start offset parsed out of every remaining zero-padded `.log` file name. A
/// missing directory means a never-persisted partition.
async fn sweep_scratch_files_and_collect_offsets(
    partition_path: &str,
) -> Result<Vec<u64>, PartitionRecoveryError> {
    // The recovery opens are read-only, but a failed one fails the partition
    // load, which stops the server at boot, so they count like write opens.
    let entries = match segment_dir_entries(partition_path)
        .await
        .note_descriptor_exhaustion(|| format!("listing directory {partition_path}"))
    {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            error!(
                partition_path,
                error = %source,
                "failed to list partition directory during recovery"
            );
            return Err(IggyError::CannotReadPartitions.into());
        }
    };
    let mut swept = Vec::new();
    let mut orphan_candidates = Vec::new();
    let mut log_stems = std::collections::HashSet::new();
    let mut start_offsets = Vec::new();
    for path in entries {
        let Some(as_str) = path.to_str() else {
            continue;
        };
        if as_str.ends_with(STAGING_SUFFIX) {
            swept.push(path);
            continue;
        }
        match path.extension().and_then(|extension| extension.to_str()) {
            Some(LOG_EXTENSION) => {
                if let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) {
                    log_stems.insert(stem.to_owned());
                    if let Ok(start_offset) = stem.parse::<u64>() {
                        start_offsets.push(start_offset);
                    }
                }
            }
            // An anchor outlives nothing: it describes the gap in front of ONE
            // segment, so once that segment is gone (retention, a failed plant
            // that never landed) the record can only mislead a later guard into
            // admitting a gap it never saw.
            Some(INDEX_EXTENSION | ANCHOR_EXTENSION) => orphan_candidates.push(path),
            _ => {}
        }
    }
    swept.extend(orphan_candidates.into_iter().filter(|path| {
        !path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .is_some_and(|stem| log_stems.contains(stem))
    }));
    for path in swept {
        if let Err(error) = fs::remove_file(&path).await {
            warn!(
                partition_path,
                path = %path.display(),
                %error,
                "failed to sweep a stale scratch file at boot"
            );
        }
    }
    Ok(start_offsets)
}

/// Byte length of a segment file, a missing file reading as empty.
///
/// Any other stat failure is fail-stop, mirroring the `NotFound`-only leniency
/// of the directory listing above: recovery physically truncates files to the
/// bounds derived from these lengths, so folding a transient `EACCES` or
/// `EIO` into 0 would route a healthy segment into recover-as-empty, fencing
/// it out of service (worst route: an index stat error floors a healthy
/// sealed index to a 0-byte target while its entries still load).
async fn file_len(path: &str) -> Result<u64, PartitionRecoveryError> {
    match fs::metadata(path).await {
        Ok(metadata) => Ok(metadata.len()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(source) => {
            error!(
                path,
                error = %source,
                "failed to stat a segment file during recovery"
            );
            Err(IggyError::CannotReadFileMetadata.into())
        }
    }
}

/// Why recovery rewrites segment bytes, which decides whether an operator must
/// see the rewrite.
#[derive(Clone, Copy)]
enum RewriteCause {
    /// A crash tore the bytes, or damage left an index that the log cannot back.
    Damage,
    /// The checkpoint segment's index is derived data that every boot replaces,
    /// because the WAL owns the bytes past the checkpoint.
    Checkpoint,
}

/// Physically truncates a segment file to its recovered byte length, so disk
/// and the seeded size counters agree before storage reopens: reopen verifies
/// the on-disk length against the recovered size and refuses a divergence,
/// and before that check existed a leftover tail silently resurrected through
/// the writers' re-stat of the raw length. Truncation also protects state
/// transfer: the sender sizes each artifact from `segment.size` and hashes
/// exactly `[0, segment.size)`, so resurrected garbage INSIDE that range
/// would poison every artifact a torn replica offers once it serves as
/// primary.
///
/// The tail being discarded was proven dead by the bounds walk: nothing past
/// the recovered size decodes (the interior-damage probe refuses recovery
/// outright when something does), so polls could never serve those bytes.
/// The checkpoint segment's index is the exception: its entries describe
/// bytes that the WAL owns, and the WAL replay indexes them again.
///
/// Stats the file fresh instead of trusting a length carried from pass A: the
/// whole chain was walked in between, and the mutation must key on what is on
/// disk now. The fsync bounds the crash window: a power cut right after `set_len` may
/// re-present the torn tail on the next boot, which only walks and truncates
/// again (idempotent), but the sync keeps the common case deterministic.
async fn truncate_to(
    path: &str,
    target_size: u64,
    cause: RewriteCause,
) -> Result<(), PartitionRecoveryError> {
    let current_size = file_len(path).await?;
    if current_size == target_size {
        return Ok(());
    }
    // Unreachable by construction (walked bounds never exceed the file they
    // were walked from); extending would fabricate a zero-filled tail, and
    // zero bytes decode as valid-looking index entries -- three bare
    // little-endian u64s with no magic to reject them -- so fail stop.
    if target_size > current_size {
        error!(
            path,
            current_size,
            target_size,
            "recovered bounds exceed the file they were walked from; \
             refusing to extend a segment file"
        );
        return Err(IggyError::CannotWriteToFile.into());
    }
    match cause {
        RewriteCause::Damage => warn!(
            path,
            current_size,
            target_size,
            "truncating a segment file to its recovered bounds; discarding \
             torn tail bytes"
        ),
        RewriteCause::Checkpoint => debug!(
            path,
            current_size,
            target_size,
            "dropping sparse index entries past the WAL checkpoint; the WAL \
             replay indexes them again"
        ),
    }
    let file = fs::OpenOptions::new()
        .write(true)
        .open(path)
        .await
        .note_descriptor_exhaustion(|| format!("opening {path}"))
        .map_err(|source| {
            error!(
                path,
                error = %source,
                "failed to open a segment file for truncation during recovery"
            );
            PartitionRecoveryError::from(IggyError::CannotWriteToFile)
        })?;
    truncate_file(&file, target_size).await.map_err(|source| {
        error!(
            path,
            target_size,
            error = %source,
            "failed to truncate a segment file to its recovered bounds"
        );
        PartitionRecoveryError::from(IggyError::CannotWriteToFile)
    })?;
    file.sync_all().await.map_err(|source| {
        error!(
            path,
            error = %source,
            "failed to fsync a segment file after truncation"
        );
        PartitionRecoveryError::from(IggyError::CannotSyncFile)
    })?;
    Ok(())
}

/// Stages the index rebuilt by the index-less walk in a scratch file beside
/// its final name. Without a rebuild a SEALED segment -- which never flushes
/// again -- would keep an empty index forever and pay a full log scan on
/// every poll.
///
/// Staged, not written in place: an in-place writeback can tear -- a crash
/// mid-write may persist a later page while an earlier one still reads
/// zeros, and 24-byte zero runs decode as valid non-monotone entries, so the
/// next boot would fence the whole partition over its own repair artifact.
/// The staging file is pure scratch until pass C renames it into place: the
/// boot sweep unlinks orphaned `*.staging` files, so a crash anywhere before
/// the rename costs nothing.
async fn stage_rebuilt_index(
    index_path: &str,
    entries: Vec<u8>,
) -> Result<String, PartitionRecoveryError> {
    let staging_path = format!("{index_path}{STAGING_SUFFIX}");
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&staging_path)
        .await
        .note_descriptor_exhaustion(|| format!("opening {staging_path}"))
        .map_err(|source| {
            error!(
                path = %staging_path,
                error = %source,
                "failed to open a sparse index staging file during recovery"
            );
            PartitionRecoveryError::from(IggyError::CannotWriteToFile)
        })?;
    file.write_all_at(entries, 0).await.0.map_err(|source| {
        error!(
            path = %staging_path,
            error = %source,
            "failed to write a rebuilt sparse index during recovery"
        );
        PartitionRecoveryError::from(IggyError::CannotWriteToFile)
    })?;
    file.sync_all().await.map_err(|source| {
        error!(
            path = %staging_path,
            error = %source,
            "failed to fsync a rebuilt sparse index after recovery"
        );
        PartitionRecoveryError::from(IggyError::CannotSyncFile)
    })?;
    Ok(staging_path)
}

/// Installs a staged rebuilt index at its final name. The rename is the
/// atomic commit point: the on-disk index is either the old one holding no
/// whole entry (whose walk re-runs the rebuild) or the complete rebuilt one,
/// never a mix of pages from both.
async fn install_rebuilt_index(
    staging_path: &str,
    index_path: &str,
    partition_path: &str,
) -> Result<(), PartitionRecoveryError> {
    fs::rename(staging_path, index_path)
        .await
        .map_err(|source| {
            error!(
                from = %staging_path,
                to = %index_path,
                error = %source,
                "failed to rename a rebuilt sparse index into place during recovery"
            );
            PartitionRecoveryError::from(IggyError::CannotWriteToFile)
        })?;
    fsync_dir(partition_path).await
}

/// Makes renames and new files in `dir` durable.
async fn fsync_dir(dir: &str) -> Result<(), PartitionRecoveryError> {
    let synced = async {
        let handle = fs::File::open(dir)
            .await
            .note_descriptor_exhaustion(|| format!("opening directory {dir}"))?;
        handle.sync_all().await
    };
    synced.await.map_err(|source| {
        error!(
            dir,
            error = %source,
            "failed to fsync a directory during recovery"
        );
        PartitionRecoveryError::from(IggyError::CannotSyncFile)
    })
}

/// Moves a segment pair that recovery proved unreadable into a fresh
/// `<partition dir>.fenced.<n>` directory -- the naming the partition-level
/// quarantine uses, so operators grep one pattern -- and seeds empty files
/// at the original names for the empty recovery to open. The bytes prove
/// nothing, yet they are the only copy of whatever the crash tore, so the
/// one verdict that would otherwise destroy data keeps it instead.
///
/// Index first, log second on the reseed: recovery keys on `.log` stems and
/// sweeps orphaned indexes, so a crash between the two creates leaves only
/// states a later boot already understands (segment absent, or one orphan
/// index).
async fn fence_unrecoverable_segment_files(
    identity: PartitionIdentity<'_>,
    messages_path: &str,
    index_path: &str,
    start_offset: u64,
) -> Result<(), PartitionRecoveryError> {
    let log_bytes = file_len(messages_path).await?;
    let index_bytes = file_len(index_path).await?;
    if log_bytes == 0 && index_bytes == 0 {
        return Ok(());
    }
    let mut fenced_dir = None;
    for attempt in 0..FENCED_DIR_PROBE_LIMIT {
        let candidate = format!("{}.fenced.{attempt}", identity.partition_path);
        // `create_dir`, not `create_dir_all`: success is the claim on this
        // suffix, and merging into an existing fence would mix evidence from
        // two incidents.
        match fs::create_dir(&candidate).await {
            Ok(()) => {
                fenced_dir = Some(candidate);
                break;
            }
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {}
            Err(source) => {
                error!(
                    path = %candidate,
                    error = %source,
                    "failed to create a fence directory during recovery"
                );
                return Err(IggyError::CannotWriteToFile.into());
            }
        }
    }
    let Some(fenced_dir) = fenced_dir else {
        error!(
            partition_path = identity.partition_path,
            "every fence directory suffix is taken; refusing to merge into one"
        );
        return Err(IggyError::CannotWriteToFile.into());
    };
    let fenced_log = fenced_target(&fenced_dir, messages_path)?;
    let fenced_index = fenced_target(&fenced_dir, index_path)?;
    rename_into_fence(messages_path, &fenced_log).await?;
    rename_into_fence(index_path, &fenced_index).await?;
    seed_empty_file(index_path).await?;
    seed_empty_file(messages_path).await?;
    // The fence directory's new dirents, the partition directory's renames
    // plus fresh files, and the parent's new fence-directory dirent.
    fsync_dir(&fenced_dir).await?;
    fsync_dir(identity.partition_path).await?;
    if let Some(parent) = Path::new(identity.partition_path)
        .parent()
        .and_then(Path::to_str)
    {
        fsync_dir(parent).await?;
    }
    warn!(
        stream_id = identity.stream_id,
        topic_id = identity.topic_id,
        partition_id = identity.partition_id,
        start_offset,
        fenced_log = %fenced_log.display(),
        fenced_index = %fenced_index.display(),
        log_bytes,
        index_bytes,
        "segment holds bytes but nothing in it decodes; moved the whole \
         .log/.index pair into the fence directory and recovered the segment \
         empty over fresh files"
    );
    Ok(())
}

/// Destination of one fenced file: the fence directory plus the file's own
/// name, so the fenced copy stays greppable by its segment stem.
fn fenced_target(fenced_dir: &str, source_path: &str) -> Result<PathBuf, PartitionRecoveryError> {
    Path::new(source_path).file_name().map_or_else(
        || {
            error!(
                source_path,
                "segment file path has no final component; cannot fence it"
            );
            Err(IggyError::CannotWriteToFile.into())
        },
        |name| Ok(Path::new(fenced_dir).join(name)),
    )
}

async fn rename_into_fence(source_path: &str, target: &Path) -> Result<(), PartitionRecoveryError> {
    match fs::rename(source_path, target).await {
        Ok(()) => Ok(()),
        // A missing index beside a present log has nothing to move.
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => {
            error!(
                from = source_path,
                to = %target.display(),
                error = %source,
                "failed to move an unreadable segment file into its fence directory"
            );
            Err(IggyError::CannotWriteToFile.into())
        }
    }
}

async fn seed_empty_file(path: &str) -> Result<(), PartitionRecoveryError> {
    let seeded = async {
        let file = fs::File::create(path)
            .await
            .note_descriptor_exhaustion(|| format!("creating {path}"))?;
        file.sync_all().await
    };
    seeded.await.map_err(|source| {
        error!(
            path,
            error = %source,
            "failed to seed a fresh empty segment file after fencing"
        );
        PartitionRecoveryError::from(IggyError::CannotWriteToFile)
    })
}

/// Index anchors for one segment: `(entry_count, first, last)`.
///
/// A `.log` with NO `.index` beside it reads exactly like a 0-byte one, and
/// both belong on the index-less walk. It is an ordinary shape:
/// `SegmentStorage::new` creates the log before the index, so a crash or a
/// failed open between the two leaves precisely that pair, as does any
/// operator restore that drops an index. The reader's open is bare
/// `read(true)` and folds ENOENT into `CannotReadFile`, which propagates as a
/// plain `PartitionRecoveryError::Iggy` -- not a `Refused` the caller
/// can fence -- so it would abort the whole boot for a segment the walk
/// rebuilds. Stat through the `NotFound`-lenient [`file_len`] first; every
/// other stat failure still fails stop there.
async fn load_index_anchors(
    identity: PartitionIdentity<'_>,
    index_path: &str,
) -> Result<(u64, Option<IggyIndex>, Option<IggyIndex>), PartitionRecoveryError> {
    if file_len(index_path).await? == 0 {
        return Ok((0, None, None));
    }
    // Opened here, not by `IggyIndexReader::new`, which drops the `io::Error`.
    let file = compio::fs::File::open(index_path)
        .await
        .note_descriptor_exhaustion(|| format!("opening {index_path}"))
        .map_err(|source| {
            error!(
                stream_id = identity.stream_id,
                topic_id = identity.topic_id,
                partition_id = identity.partition_id,
                path = %index_path,
                error = %source,
                "failed to open sparse index during recovery"
            );
            PartitionRecoveryError::from(IggyError::CannotReadFile)
        })?;
    let reader = IggyIndexReader::from_file(index_path, file);
    let entry_count = reader.entry_count().await.map_err(|source| {
        error!(
            stream_id = identity.stream_id,
            topic_id = identity.topic_id,
            partition_id = identity.partition_id,
            path = %index_path,
            error = %source,
            "failed to size sparse index during recovery"
        );
        source
    })?;
    let first = reader.load_first().await.map_err(|source| {
        error!(
            stream_id = identity.stream_id,
            topic_id = identity.topic_id,
            partition_id = identity.partition_id,
            path = %index_path,
            error = %source,
            "failed to read first sparse index entry during recovery"
        );
        source
    })?;
    let last = reader.load_last().await.map_err(|source| {
        error!(
            stream_id = identity.stream_id,
            topic_id = identity.topic_id,
            partition_id = identity.partition_id,
            path = %index_path,
            error = %source,
            "failed to read last sparse index entry during recovery"
        );
        source
    })?;
    Ok((entry_count, first, last))
}

/// Derives a segment's readable bounds. `None` when the log holds no whole
/// batch at all (the caller recovers the segment as empty).
///
/// Without `durable_segments`, a consistent index locates batches but cannot prove
/// any log page reached disk: page-cache writeback may preserve a later chunk
/// while losing an earlier one. Recovery therefore checksum-walks the log from
/// byte 0. A clean walk preserves the existing index, while a break falls
/// through to the rebuilding walk so every retained entry describes verified
/// bytes.
///
/// With `durable_segments`, completed serialized flushes prove the prefix before
/// the final index entry, so the last entry's `position` anchors the walk that
/// proves where the segment really ends. An index whose last entry the log
/// cannot back, or whose entries contradict each other, is dropped whole: the
/// log is walked from byte 0 and the index is rebuilt from the batches that
/// walk proves. The unbacked-last path first measures the step-back, and a gap
/// deeper than one entry refuses as previously durable log loss rather than
/// rebuilding. Bytes left past either walked prefix go through the damage
/// probe: a torn tail truncates, while damage with intact batches after it, or
/// residue the probe cannot classify within its limits, refuses recovery.
#[allow(clippy::too_many_lines)]
async fn recover_segment_bounds(
    identity: PartitionIdentity<'_>,
    index_path: &str,
    messages_path: &str,
    start_offset: u64,
    messages_size: u64,
    durable_segments: bool,
    scratch: &mut ScanScratch,
) -> Result<Option<WalkedBounds>, PartitionRecoveryError> {
    let (entry_count, first, last) = load_index_anchors(identity, index_path).await?;

    match (first, last) {
        (Some(first), Some(last)) => {
            // A mis-strided or foreign index decodes to garbage entries that
            // binary searches would trust, so an index that contradicts itself
            // is dropped whole and rebuilt from the log. No `durable_segments`
            // gate here, unlike the step-back below: the writer cannot emit a
            // non-ascending run, so this file is foreign or mis-strided and is
            // no witness to what the log once held.
            let validation = index_is_consistent(
                identity,
                index_path,
                messages_path,
                start_offset,
                entry_count,
                messages_size,
                scratch,
            )
            .await?;

            let messages = open_messages_file(identity, messages_path).await?;
            let mut scanner = FileScanner::new(&messages, messages_size, scratch);
            if !validation.structurally_consistent {
                return recover_by_walking_log(
                    identity,
                    &mut scanner,
                    messages_path,
                    start_offset,
                    messages_size,
                    RewriteCause::Damage,
                )
                .await;
            }

            // The sparse index holds ONE entry per flushed chunk, pointing
            // at the chunk's FIRST batch -- `last.offset` is where the last
            // chunk STARTS, not where the segment ends (a whole journal
            // flushed as one chunk indexes only its first offset). Walk the
            // batch chain from that position to the file end to recover the
            // true end offset.
            let walk = walk_chain_from_anchor(
                identity,
                &mut scanner,
                messages_path,
                start_offset,
                messages_size,
                last,
            )
            .await?;
            if walk.start_timestamp.is_none() {
                // Under `durable_segments` the DEPTH of the step-back that would
                // find a usable non-empty anchor is evidence about the LOG.
                // Flushes are serialized and each one fdatasyncs the whole log
                // file before the next one writes, so an entry existing above
                // entry N proves the log's fdatasync through chunk N completed.
                // Only the chunk in flight at the crash can strand an entry; a
                // deeper gap is the log missing bytes a completed flush had
                // already made durable. A checksum-valid empty anchor reaches
                // this path too: it carries no message range recovery may
                // advertise and the production writer never emits one.
                // Refuse and keep every byte for the operator.
                if durable_segments {
                    let search = find_provable_index_anchor(
                        identity,
                        index_path,
                        messages_path,
                        &mut scanner,
                        start_offset,
                        entry_count,
                        messages_size,
                    )
                    .await?;
                    let step_back_entries = entry_count.saturating_sub(search.provable_entries);
                    if step_back_entries > MAX_FSYNCED_INDEX_STEP_BACK_ENTRIES {
                        return Err(identity.refusal(PartitionRecoveryRefusal::FsyncedLogLoss {
                            start_offset,
                            entry_count,
                            provable_entries: search.provable_entries,
                            provable_position: search.provable_position,
                            searched_entries: search.searched_entries,
                        }));
                    }
                }
                // The log cannot back its own last entry, so this index is no
                // longer a description of this log and nothing in it is a
                // trustworthy anchor. Anchoring on the highest entry that
                // still proves would leave `[0, anchor.position)` unread, and
                // the damage probe only scans FORWARD from where a walk
                // stopped: a hole below the anchor would be invisible, while
                // `end_offset` still advertised the offsets over it. That hole
                // is exactly what the crash reaching this branch can leave --
                // without `durable_segments` writeback order is arbitrary, so an
                // unprovable tail entry is equally a torn INDEX tail and
                // evidence the LOG lost an interior page. Walk from byte 0
                // instead: it reads the damage, finds the anchor's own batch
                // as a survivor past it, and refuses with the bytes preserved,
                // and where the log is whole every rebuilt entry is one the
                // walk proved rather than one it inherited.
                warn!(
                    stream_id = identity.stream_id,
                    topic_id = identity.topic_id,
                    partition_id = identity.partition_id,
                    start_offset,
                    messages_size,
                    entry_count,
                    last_entry_offset = last.offset,
                    last_entry_position = last.position,
                    "the log does not back its last sparse index entry with a non-empty batch; \
                     discarding the index and rebuilding it from a byte-0 \
                     walk of the log"
                );
                let rebuilt = recover_by_walking_log(
                    identity,
                    &mut scanner,
                    messages_path,
                    start_offset,
                    messages_size,
                    RewriteCause::Damage,
                )
                .await?;
                if durable_segments {
                    ensure_fsynced_rebuild_reaches(
                        identity,
                        rebuilt.as_ref(),
                        start_offset,
                        entry_count,
                        last.position,
                    )?;
                }
                return Ok(rebuilt);
            }

            if !validation.mappings_match {
                let rebuilt = recover_by_walking_log(
                    identity,
                    &mut scanner,
                    messages_path,
                    start_offset,
                    messages_size,
                    RewriteCause::Damage,
                )
                .await?;
                if durable_segments {
                    ensure_fsynced_rebuild_reaches(
                        identity,
                        rebuilt.as_ref(),
                        start_offset,
                        entry_count,
                        last.position,
                    )?;
                }
                return Ok(rebuilt);
            }

            if !durable_segments {
                // The last entry proved that the index still describes this
                // log. It does not prove earlier log pages reached disk. The
                // first entry is exactly `(start_offset, position 0)` by the
                // consistency check above, so use it only to seed the expected
                // offset and checksum every batch, including unindexed spans.
                let full_walk = walk_chain_from_anchor(
                    identity,
                    &mut scanner,
                    messages_path,
                    start_offset,
                    messages_size,
                    first,
                )
                .await?;
                if full_walk.position == messages_size
                    && let Some(start_timestamp) = full_walk.start_timestamp
                {
                    return Ok(Some(WalkedBounds {
                        start_timestamp,
                        end_timestamp: full_walk.end_timestamp,
                        end_offset: full_walk.end_offset,
                        messages_size: full_walk.position,
                        index_size: entry_count * IGGY_INDEX_SIZE as u64,
                        rebuilt_index: None,
                    }));
                }

                // The full walk found damage or no non-empty batch. Re-run
                // through the rebuilding path: it probes any residue before a
                // destructive verdict and emits an index containing only the
                // batches the log itself proves.
                return recover_by_walking_log(
                    identity,
                    &mut scanner,
                    messages_path,
                    start_offset,
                    messages_size,
                    RewriteCause::Damage,
                )
                .await;
            }

            refuse_if_survivor_past_damage(
                identity,
                &mut scanner,
                messages_path,
                walk.position,
                messages_size,
                Some(walk.end_offset),
                start_offset,
            )
            .await?;
            Ok(Some(WalkedBounds {
                start_timestamp: first.timestamp,
                end_timestamp: walk.end_timestamp,
                end_offset: walk.end_offset,
                messages_size: walk.position,
                index_size: entry_count * IGGY_INDEX_SIZE as u64,
                rebuilt_index: None,
            }))
        }
        // No whole index entry, but the log holds bytes: recover the bounds by
        // WALKING the log from byte 0 instead of declaring the segment empty.
        //
        // The index is not the only self-describing copy -- batch headers carry
        // their own offsets, timestamps and lengths -- and the log and its
        // index persist concurrently under every config, so a torn index is
        // reachable for a MID-CHAIN segment too, not just the tail.
        // Recovering that as empty
        // then trips the contiguity guard and refuses the whole partition:
        // total serve loss (and offset reuse from 0) for a chain whose bytes
        // are all present. The walk keeps the torn-tail truncation the indexed
        // path performs, and rebuilds the index from the batches it proves so
        // a sealed segment does not pay a full-scan poll penalty forever.
        _ if messages_size > 0 => {
            let messages = open_messages_file(identity, messages_path).await?;
            let mut scanner = FileScanner::new(&messages, messages_size, scratch);
            recover_by_walking_log(
                identity,
                &mut scanner,
                messages_path,
                start_offset,
                messages_size,
                RewriteCause::Damage,
            )
            .await
        }
        _ => Ok(None),
    }
}

/// Refuse an fsynced rebuild that stops before the durable prefix implied by
/// the index entry whose flush could begin only after the preceding log
/// fdatasync completed.
fn ensure_fsynced_rebuild_reaches(
    identity: PartitionIdentity<'_>,
    rebuilt: Option<&WalkedBounds>,
    start_offset: u64,
    entry_count: u64,
    durable_position: u64,
) -> Result<(), PartitionRecoveryError> {
    let walked_position = rebuilt.map_or(0, |bounds| bounds.messages_size);
    if walked_position < durable_position {
        return Err(
            identity.refusal(PartitionRecoveryRefusal::FsyncedRebuildShortfall {
                start_offset,
                entry_count,
                walked_position,
                durable_position,
            }),
        );
    }
    Ok(())
}

/// Recovers a segment's bounds from the log alone, walking the batch chain
/// from byte 0 and rebuilding the sparse index from the batches it proves.
/// `None` when no whole batch decodes anywhere (the caller recovers the
/// segment as empty).
///
/// Reached both when the index holds no whole entry and when it holds
/// entries the log cannot back. Every batch is checksum-verified, as in the
/// anchored walk; here the FILENAME is the only anchor at all, and the
/// header decode checks a length, not a checksum.
#[allow(clippy::too_many_lines)]
async fn recover_by_walking_log(
    identity: PartitionIdentity<'_>,
    scanner: &mut FileScanner<'_>,
    messages_path: &str,
    start_offset: u64,
    messages_size: u64,
    cause: RewriteCause,
) -> Result<Option<WalkedBounds>, PartitionRecoveryError> {
    let mut position = 0u64;
    let mut start_timestamp = None;
    let mut end_offset = start_offset;
    let mut end_timestamp = 0;
    let mut expected_offset = start_offset;
    let mut rebuilt_index = Vec::new();
    let mut last_indexed_position: Option<u64> = None;
    while position < messages_size {
        let Some(header) = header_at(identity, scanner, messages_path, position).await? else {
            break;
        };
        let extent = position.saturating_add(header.total_size() as u64);
        if extent > messages_size {
            break;
        }
        let verifies = batch_verifies(
            identity,
            scanner,
            messages_path,
            position,
            header.total_size(),
        )
        .await?;
        if !verifies {
            break;
        }
        if header.partition_id != identity.partition_id as u64 {
            // Verified above, so this is a real record minted for another
            // partition (a misdirected write, an operator copy), not damage:
            // preserve it as evidence.
            return Err(identity.refusal(PartitionRecoveryRefusal::ForeignBatch {
                start_offset,
                batch_partition_id: header.partition_id,
                position,
            }));
        }
        if header.base_offset != expected_offset {
            // A batch that VERIFIES but does not continue the chain is
            // durable data past a hole (or a duplicated range): the offsets
            // in between are exactly what a truncation here would silently
            // erase, so refuse instead.
            return Err(
                identity.refusal(PartitionRecoveryRefusal::OffsetDiscontinuity {
                    start_offset,
                    expected_offset,
                    found_offset: header.base_offset,
                    position,
                }),
            );
        }
        if header.message_count > 0 {
            end_offset = header
                .base_offset
                .saturating_add(u64::from(header.message_count) - 1);
            end_timestamp = header.base_timestamp;
            start_timestamp.get_or_insert(header.base_timestamp);
            expected_offset = end_offset.saturating_add(1);
            if last_indexed_position.is_none_or(|indexed| {
                position.saturating_sub(indexed) >= REBUILT_INDEX_STRIDE_BYTES
            }) {
                push_index_entry(
                    &mut rebuilt_index,
                    header.base_offset,
                    header.base_timestamp,
                    position,
                );
                last_indexed_position = Some(position);
            }
        }
        position = extent;
        if scanner.take_refilled() {
            yield_to_reactor().await;
        }
    }
    refuse_if_survivor_past_damage(
        identity,
        scanner,
        messages_path,
        position,
        messages_size,
        start_timestamp.map(|_| end_offset),
        start_offset,
    )
    .await?;
    let Some(start_timestamp) = start_timestamp else {
        // Not one whole batch, and the probe above proved nothing decodable
        // follows either: the bytes really are unusable, so the caller's
        // empty recovery is right after all.
        return Ok(None);
    };
    match cause {
        RewriteCause::Damage => warn!(
            stream_id = identity.stream_id,
            topic_id = identity.topic_id,
            partition_id = identity.partition_id,
            start_offset,
            messages_size,
            walked_size = position,
            rebuilt_entries = rebuilt_index.len() / IGGY_INDEX_SIZE,
            "recovered segment bounds by walking the log and rebuilding its \
             index from the walked batches"
        ),
        RewriteCause::Checkpoint => debug!(
            stream_id = identity.stream_id,
            topic_id = identity.topic_id,
            partition_id = identity.partition_id,
            start_offset,
            walked_size = position,
            rebuilt_entries = rebuilt_index.len() / IGGY_INDEX_SIZE,
            "rebuilt the checkpoint segment's sparse index from the walked \
             log; the WAL replay indexes the bytes past the checkpoint again"
        ),
    }
    Ok(Some(WalkedBounds {
        start_timestamp,
        end_timestamp,
        end_offset,
        messages_size: position,
        index_size: rebuilt_index.len() as u64,
        rebuilt_index: Some(rebuilt_index),
    }))
}

/// Walks the batch chain forward from one sparse index entry to the end of
/// the log, proving where the segment really ends.
///
/// The anchor is only a starting byte and the offset the chain must continue
/// from; nothing about it is assumed to hold. Every batch the walk accepts
/// passes its batch checksum: an index entry proves nothing about the bytes
/// under it, because the log and the index persist concurrently and a crash
/// can leave a batch's header page durable over a body that never landed.
/// The header decode still runs first, so a header that does not fit the
/// file breaks the walk before any checksum is paid, and the verify covers
/// one flushed chunk per segment in the ordinary boot (the last entry points
/// at the last chunk's first batch). When no non-empty batch verifies at the
/// anchor, `start_timestamp` remains `None`, which tells the caller the index
/// does not describe a recoverable message range; the other bounds are then
/// the anchor's own and must not be used.
async fn walk_chain_from_anchor(
    identity: PartitionIdentity<'_>,
    scanner: &mut FileScanner<'_>,
    messages_path: &str,
    start_offset: u64,
    messages_size: u64,
    anchor: IggyIndex,
) -> Result<AnchoredWalk, PartitionRecoveryError> {
    let mut position = anchor.position;
    let mut start_timestamp = None;
    let mut end_offset = anchor.offset;
    let mut end_timestamp = anchor.timestamp;
    let mut expected_offset = anchor.offset;
    // The anchor's offset comes from the INDEX; only a batch this walk has
    // already proved makes the expectation the LOG's own.
    let mut expectation_from_log = false;
    while position < messages_size {
        let Some(header) = header_at(identity, scanner, messages_path, position).await? else {
            break;
        };
        let extent = position.saturating_add(header.total_size() as u64);
        if extent > messages_size {
            break;
        }
        // The batch checksum covers every header field and the body, and the
        // server mints every legal record with it, so it is what tells
        // damage wearing a decodable header from a real record: a batch that
        // fails it is damage whatever its fields claim -- a torn body under
        // an intact header page, a bit flip in either direction -- so break
        // and let the probe classify the residue (a torn tail truncates). A
        // batch that VERIFIES is durable evidence, and only the verified
        // contradictions below earn a refusal.
        if !batch_verifies(
            identity,
            scanner,
            messages_path,
            position,
            header.total_size(),
        )
        .await?
        {
            break;
        }
        if header.partition_id != identity.partition_id as u64 {
            // A real record minted for another partition (a misdirected or
            // copied write), not damage: adopting it would seed this
            // partition's offsets from foreign data and truncating it would
            // destroy the evidence, so refuse and keep the bytes.
            return Err(identity.refusal(PartitionRecoveryRefusal::ForeignBatch {
                start_offset,
                batch_partition_id: header.partition_id,
                position,
            }));
        }
        if header.base_offset != expected_offset {
            if !expectation_from_log {
                // Still the anchor: the offset this batch failed to match is
                // the INDEX ENTRY's, so a verifying batch here contradicts
                // the entry, not the chain. Calling that a discontinuity
                // would refuse a healthy log over a stale entry, so break and
                // leave the verdict to the byte-0 rebuild, which reads the
                // chain from the filename onward.
                break;
            }
            // A verified batch that does not continue the chain is durable
            // data past a hole or a duplicated range. Absorbing it would
            // mint a segment no peer can ever install (state transfer's own
            // walk refuses any gap) and seed current_offset with fabricated
            // offsets under an advance-only superblock persist; truncating
            // it would hide the loss. Refuse and keep the bytes.
            return Err(
                identity.refusal(PartitionRecoveryRefusal::OffsetDiscontinuity {
                    start_offset,
                    expected_offset,
                    found_offset: header.base_offset,
                    position,
                }),
            );
        }
        if header.message_count > 0 {
            start_timestamp.get_or_insert(header.base_timestamp);
            end_offset = header
                .base_offset
                .saturating_add(u64::from(header.message_count) - 1);
            end_timestamp = header.base_timestamp;
            expected_offset = end_offset.saturating_add(1);
        }
        // Any verified batch that matched the expectation makes it the LOG's
        // own, an empty batch included: its header carried the offset. Gating
        // this on message_count would let a verified empty anchor batch turn
        // a real discontinuity after it into the benign anchor-mismatch break
        // above, which truncates instead of refusing.
        expectation_from_log = true;
        position = extent;
        if scanner.take_refilled() {
            yield_to_reactor().await;
        }
    }
    Ok(AnchoredWalk {
        start_timestamp,
        end_offset,
        end_timestamp,
        position,
    })
}

/// How far the index has outrun the log, in the terms the refusal reports.
struct IndexAnchorSearch {
    /// Entries at or below the highest one the log still proves, that entry
    /// included; 0 when nothing in the searched window proved.
    provable_entries: u64,
    /// Position of that entry, or 0 when nothing proved.
    provable_position: u64,
    /// Entries actually probed, capped by
    /// [`MAX_INDEX_ANCHOR_PROBE_ENTRIES`].
    searched_entries: u64,
}

/// Independent sparse-index validation verdicts.
///
/// A structural contradiction means the index cannot be writer-produced and
/// carries no fsync evidence, so recovery drops it immediately. A mapping
/// mismatch can instead be the ordinary index-ahead-of-log crash shape, so an
/// fsynced topic must still run the step-back and durable-position gates before
/// rebuilding it.
struct IndexValidation {
    structurally_consistent: bool,
    mappings_match: bool,
}

/// Buffered log-header reader used while validating sparse index mappings.
/// Index positions ascend, so each window moves forward and every log byte is
/// read at most once even for an index with one entry per batch.
struct IndexLogScanner<'scan> {
    identity: PartitionIdentity<'scan>,
    file: &'scan fs::File,
    path: &'scan str,
    file_len: u64,
    window: &'scan mut Vec<u8>,
    window_start: u64,
}

impl<'scan> IndexLogScanner<'scan> {
    fn new(
        identity: PartitionIdentity<'scan>,
        file: &'scan fs::File,
        path: &'scan str,
        file_len: u64,
        window: &'scan mut Vec<u8>,
    ) -> Self {
        window.clear();
        Self {
            identity,
            file,
            path,
            file_len,
            window,
            window_start: 0,
        }
    }

    async fn entry_matches(
        &mut self,
        offset: u64,
        timestamp: u64,
        position: u64,
    ) -> Result<bool, PartitionRecoveryError> {
        let Some(header_end) = position.checked_add(COMMAND_HEADER_SIZE as u64) else {
            return Ok(false);
        };
        if header_end > self.file_len {
            return Ok(false);
        }
        let window_end = self.window_start + self.window.len() as u64;
        if position < self.window_start || header_end > window_end {
            let fill = usize::try_from((self.file_len - position).min(SCAN_WINDOW_CAPACITY as u64))
                .unwrap_or(SCAN_WINDOW_CAPACITY);
            self.window.resize(fill, 0);
            read_scan_window(self.file, self.window, position)
                .await
                .map_err(|source| {
                    error!(
                        stream_id = self.identity.stream_id,
                        topic_id = self.identity.topic_id,
                        partition_id = self.identity.partition_id,
                        path = %self.path,
                        position,
                        error = %source,
                        "failed to read a segment log header for sparse index validation"
                    );
                    PartitionRecoveryError::from(IggyError::CannotReadFile)
                })?;
            self.window_start = position;
        }
        let at = usize::try_from(position - self.window_start).unwrap_or(0);
        Ok(
            BatchHeader::decode(&self.window[at..at + COMMAND_HEADER_SIZE]).is_ok_and(|header| {
                header.partition_id == self.identity.partition_id as u64
                    && header.base_offset == offset
                    && header.base_timestamp == timestamp
                    && header.message_count > 0
                    && header.total_size() as u64 <= MAX_RECOVERABLE_BATCH_BYTES
                    && position.saturating_add(header.total_size() as u64) <= self.file_len
            }),
        )
    }
}

/// Highest index entry below the last one that the log can still prove,
/// reported as the number of entries at or below it plus that entry's
/// position, alongside how deep the search went.
///
/// Reached only under `durable_segments`, and only to measure how far the index
/// has outrun the log: the index is dropped whole either way, so nothing is
/// anchored on the entry this returns. What the DEPTH decides is whether the
/// gap is the one chunk a crash can strand or previously durable data the log lost
/// (see [`MAX_FSYNCED_INDEX_STEP_BACK_ENTRIES`]).
///
/// An entry proves when the log holds, at its position, the whole non-empty
/// batch it describes: decoding, fitting inside the log, carrying this
/// partition's stamp and the entry's own base offset, and passing its batch
/// checksum.
/// Entries are read one at a time from the back rather than
/// slurped: an index can run to megabytes, and the search stops at the first
/// entry that proves. The dominant shape costs no LOG read at all -- an
/// entry pointing past the log end is rejected on arithmetic alone -- so a
/// torn tail pays one header read for the entry it lands on; the index side
/// still costs a 24-byte pread per entry stepped over, which is what
/// [`MAX_INDEX_ANCHOR_PROBE_ENTRIES`] caps.
///
/// The log reads are budgeted like the damage probe's: the log span between
/// one probed entry and the one probed before it is the residue that entry
/// classifies. An honest index holds one entry per flushed chunk, so a whole
/// batch fits inside every such span and its verify always fits the residue
/// multiple; entries packed closer together than the batches they claim
/// exhaust it and refuse, rather than paying a verify per entry over an index
/// that never proves. That budget bounds hashed BYTES, so the step count is
/// capped separately.
/// Yields once per window of disk reads, like the walks, and additionally
/// every [`INDEX_SCAN_YIELD_STRIDE`] entries: an entry that overshoots the
/// log reads nothing from it, so there is no refill to key the yield on, and
/// an index that outran a truncated log is full of exactly those.
#[allow(clippy::too_many_lines)]
async fn find_provable_index_anchor(
    identity: PartitionIdentity<'_>,
    index_path: &str,
    messages_path: &str,
    scanner: &mut FileScanner<'_>,
    start_offset: u64,
    entry_count: u64,
    messages_size: u64,
) -> Result<IndexAnchorSearch, PartitionRecoveryError> {
    // The last entry is the one that just failed to prove out.
    let Some(mut entry_index) = entry_count.checked_sub(2) else {
        return Ok(IndexAnchorSearch {
            provable_entries: 0,
            provable_position: 0,
            searched_entries: 0,
        });
    };
    let file = fs::File::open(index_path)
        .await
        .note_descriptor_exhaustion(|| format!("opening {index_path}"))
        .map_err(|source| {
            error!(
                stream_id = identity.stream_id,
                topic_id = identity.topic_id,
                partition_id = identity.partition_id,
                path = %index_path,
                error = %source,
                "failed to open sparse index for anchor search during recovery"
            );
            PartitionRecoveryError::from(IggyError::CannotReadFile)
        })?;
    let mut raw = [0u8; IGGY_INDEX_SIZE];
    // Lowest log byte a probed entry has already paid for; the next probed
    // entry is budgeted by the span from its own position up to here.
    let mut budgeted_down_to = messages_size;
    let mut searched_entries = 0u64;
    loop {
        let (read, bytes) = file
            .read_exact_at(raw, entry_index * IGGY_INDEX_SIZE as u64)
            .await
            .into();
        raw = bytes;
        read.map_err(|source| {
            error!(
                stream_id = identity.stream_id,
                topic_id = identity.topic_id,
                partition_id = identity.partition_id,
                path = %index_path,
                error = %source,
                "failed to read a sparse index entry for anchor search during recovery"
            );
            PartitionRecoveryError::from(IggyError::CannotReadFile)
        })?;
        let entry = IggyIndex::new(
            read_u64_le(&raw, 0),
            read_u64_le(&raw, 8),
            read_u64_le(&raw, 16),
        );
        searched_entries += 1;
        if entry.position < messages_size {
            scanner
                .budget
                .grow_for_residue(budgeted_down_to.saturating_sub(entry.position));
            budgeted_down_to = entry.position;
            if let Some(header) =
                header_at(identity, scanner, messages_path, entry.position).await?
                && header.partition_id == identity.partition_id as u64
                && header.base_offset == entry.offset
                && header.message_count > 0
                && entry.position.saturating_add(header.total_size() as u64) <= messages_size
            {
                if !scanner.budget.charge_verify(header.total_size() as u64) {
                    return Err(unverified_residue(
                        identity,
                        scanner,
                        start_offset,
                        entry.position,
                        messages_size,
                    ));
                }
                if batch_verifies(
                    identity,
                    scanner,
                    messages_path,
                    entry.position,
                    header.total_size(),
                )
                .await?
                {
                    return Ok(IndexAnchorSearch {
                        provable_entries: entry_index + 1,
                        provable_position: entry.position,
                        searched_entries,
                    });
                }
            }
        }
        if scanner.take_refilled() || entry_index.is_multiple_of(INDEX_SCAN_YIELD_STRIDE) {
            yield_to_reactor().await;
        }
        if searched_entries == MAX_INDEX_ANCHOR_PROBE_ENTRIES {
            break;
        }
        let Some(next) = entry_index.checked_sub(1) else {
            break;
        };
        entry_index = next;
    }
    Ok(IndexAnchorSearch {
        provable_entries: 0,
        provable_position: 0,
        searched_entries,
    })
}

/// Checks every whole index entry: the first must name the segment's own
/// start -- its start offset, at byte 0 -- and offsets and positions must
/// strictly ascend (the writer appends one entry per flushed chunk over a
/// growing log, and every chunk covers at least one message and one byte).
/// Every entry must also point at a whole non-empty batch in this partition
/// whose offset and timestamp exactly match the entry. Monotonic columns alone
/// are insufficient: an ascending corrupt position can land on a different
/// valid batch, and an offset poll would then silently skip the requested
/// range instead of failing to decode.
/// Every producer of an index mints its first entry there: the writer at
/// `file_position == 0` on a fresh segment, [`recover_by_walking_log`]'s
/// rebuild, and the state-transfer install's own walk. A first entry
/// elsewhere means the file was written mis-strided or over foreign bytes
/// and describes no log at all -- and the accepted path reads that entry's
/// timestamp as the SEGMENT's start timestamp, so a head that belongs to
/// another segment is not merely unused. Logged here, where the entry is
/// known, and the caller rebuilds the index from the log.
///
/// Timestamp MONOTONICITY is deliberately not required: a primary clock rewind
/// across a restart can legitimately regress persisted `base_timestamp` today,
/// and the lower-bound searches degrade gracefully on a non-monotone run. Each
/// individual entry must still equal the batch timestamp it indexes, or a
/// timestamp lookup can seek to an unrelated valid batch and skip data.
///
/// Runs on EVERY clean boot over the whole index, and a window holds tens of
/// thousands of entries, so the per-read yield the walks rely on is far too
/// coarse here: yields every [`INDEX_SCAN_YIELD_STRIDE`] entries instead.
#[allow(clippy::too_many_lines)]
async fn index_is_consistent(
    identity: PartitionIdentity<'_>,
    index_path: &str,
    messages_path: &str,
    start_offset: u64,
    entry_count: u64,
    messages_size: u64,
    scratch: &mut ScanScratch,
) -> Result<IndexValidation, PartitionRecoveryError> {
    let index_file = fs::File::open(index_path)
        .await
        .note_descriptor_exhaustion(|| format!("opening {index_path}"))
        .map_err(|source| {
            error!(
                stream_id = identity.stream_id,
                topic_id = identity.topic_id,
                partition_id = identity.partition_id,
                path = %index_path,
                error = %source,
                "failed to open sparse index for validation during recovery"
            );
            PartitionRecoveryError::from(IggyError::CannotReadFile)
        })?;
    let messages_file = fs::File::open(messages_path)
        .await
        .note_descriptor_exhaustion(|| format!("opening {messages_path}"))
        .map_err(|source| {
            error!(
                stream_id = identity.stream_id,
                topic_id = identity.topic_id,
                partition_id = identity.partition_id,
                path = %messages_path,
                error = %source,
                "failed to open segment log for sparse index validation"
            );
            PartitionRecoveryError::from(IggyError::CannotReadFile)
        })?;
    let ScanScratch {
        window: index_window,
        spill: log_window,
        ..
    } = scratch;
    let mut log_scanner = IndexLogScanner::new(
        identity,
        &messages_file,
        messages_path,
        messages_size,
        log_window,
    );
    let per_chunk_entries = SCAN_WINDOW_CAPACITY / IGGY_INDEX_SIZE;
    let mut previous: Option<(u64, u64)> = None;
    let mut mappings_match = true;
    let mut entry_index = 0u64;
    let mut byte_position = 0u64;
    while entry_index < entry_count {
        let chunk_entries = (entry_count - entry_index).min(per_chunk_entries as u64);
        // Bounded by the window capacity, so the try_from cannot fail.
        let chunk_bytes =
            usize::try_from(chunk_entries).unwrap_or(per_chunk_entries) * IGGY_INDEX_SIZE;
        index_window.resize(chunk_bytes, 0);
        read_scan_window(&index_file, index_window, byte_position)
            .await
            .map_err(|source| {
                error!(
                    stream_id = identity.stream_id,
                    topic_id = identity.topic_id,
                    partition_id = identity.partition_id,
                    path = %index_path,
                    error = %source,
                    "failed to read sparse index entries for validation during recovery"
                );
                PartitionRecoveryError::from(IggyError::CannotReadFile)
            })?;
        for entry in index_window.as_chunks::<IGGY_INDEX_SIZE>().0 {
            let entry_offset = read_u64_le(entry, 0);
            let entry_timestamp = read_u64_le(entry, 8);
            let entry_position = read_u64_le(entry, 16);
            if let Some((previous_offset, previous_position)) = previous
                && (entry_offset <= previous_offset || entry_position <= previous_position)
            {
                warn!(
                    stream_id = identity.stream_id,
                    topic_id = identity.topic_id,
                    partition_id = identity.partition_id,
                    start_offset,
                    entry_index,
                    "sparse index entry regresses in offset or position; \
                     discarding the index and rebuilding it from the log"
                );
                return Ok(IndexValidation {
                    structurally_consistent: false,
                    mappings_match: false,
                });
            }
            if previous.is_none() && (entry_offset != start_offset || entry_position != 0) {
                warn!(
                    stream_id = identity.stream_id,
                    topic_id = identity.topic_id,
                    partition_id = identity.partition_id,
                    start_offset,
                    first_entry_offset = entry_offset,
                    first_entry_position = entry_position,
                    "sparse index does not start at the segment's own first batch; \
                     discarding the index and rebuilding it from the log"
                );
                return Ok(IndexValidation {
                    structurally_consistent: false,
                    mappings_match: false,
                });
            }

            if mappings_match
                && !log_scanner
                    .entry_matches(entry_offset, entry_timestamp, entry_position)
                    .await?
            {
                warn!(
                    stream_id = identity.stream_id,
                    topic_id = identity.topic_id,
                    partition_id = identity.partition_id,
                    start_offset,
                    entry_index,
                    entry_offset,
                    entry_timestamp,
                    entry_position,
                    "sparse index entry does not describe the log batch at its position; \
                         discarding the index and rebuilding it from the log"
                );
                mappings_match = false;
            }
            previous = Some((entry_offset, entry_position));
            entry_index += 1;
            if entry_index.is_multiple_of(INDEX_SCAN_YIELD_STRIDE) {
                yield_to_reactor().await;
            }
        }
        byte_position += chunk_bytes as u64;
    }
    Ok(IndexValidation {
        structurally_consistent: true,
        mappings_match,
    })
}

/// Opens a segment's messages file for the recovery walk. Fail-stop on any
/// failure, mirroring `file_len`: recovery truncates to the bounds the walk
/// produces, so folding an open failure into "walked nothing" would discard
/// a healthy indexed segment's index -- or route an index-less one into
/// recover-as-empty, fencing the whole log out of service.
async fn open_messages_file(
    identity: PartitionIdentity<'_>,
    messages_path: &str,
) -> Result<fs::File, PartitionRecoveryError> {
    fs::File::open(messages_path)
        .await
        .note_descriptor_exhaustion(|| format!("opening {messages_path}"))
        .map_err(|source| {
            error!(
                stream_id = identity.stream_id,
                topic_id = identity.topic_id,
                partition_id = identity.partition_id,
                path = %messages_path,
                error = %source,
                "failed to open a segment messages file during recovery"
            );
            PartitionRecoveryError::from(IggyError::CannotReadFile)
        })
}

/// The batch header at `position`, or `None` when the walk must stop there
/// (nothing decodes, or the header runs past the end of the walked file).
async fn header_at(
    identity: PartitionIdentity<'_>,
    scanner: &mut FileScanner<'_>,
    messages_path: &str,
    position: u64,
) -> Result<Option<BatchHeader>, PartitionRecoveryError> {
    scanner
        .peek_header(position)
        .await
        .map_err(|source| scan_read_failure(identity, messages_path, &source))
}

/// Whether the whole batch at `position` passes its batch checksum. `false`
/// when the claimed extent runs past the end of the walked file, which is
/// the torn-tail shape and not a read failure.
async fn batch_verifies(
    identity: PartitionIdentity<'_>,
    scanner: &mut FileScanner<'_>,
    messages_path: &str,
    position: u64,
    total_size: usize,
) -> Result<bool, PartitionRecoveryError> {
    Ok(scanner
        .slice_at(position, total_size)
        .await
        .map_err(|source| scan_read_failure(identity, messages_path, &source))?
        .is_some_and(|batch| decode_batch_slice(batch).is_ok()))
}

/// A read failure inside the walk or probe is transient I/O, not evidence
/// about the bytes: fail stop rather than classify it as a torn tail, which
/// would truncate a healthy segment on an `EIO`.
fn scan_read_failure(
    identity: PartitionIdentity<'_>,
    path: &str,
    source: &io::Error,
) -> PartitionRecoveryError {
    error!(
        stream_id = identity.stream_id,
        topic_id = identity.topic_id,
        partition_id = identity.partition_id,
        path = %path,
        error = %source,
        "failed to read a segment file during the recovery walk"
    );
    PartitionRecoveryError::from(IggyError::CannotReadFile)
}

/// Classifies bytes left past the walked prefix, porting the WAL repair's
/// rule: truncation is sound only for a torn tail, and the question that
/// decides it is whether a complete entry follows the damage. A batch that
/// decodes, checksums, and plausibly extends the chain is durable data -- it
/// can only exist because an append completed after the damaged region -- so
/// discarding it would hide real loss behind a silent boot-time repair.
///
/// Scans FORWARD from `damage_position` only, and a walk that consumed the
/// whole file leaves it nothing to do. Damage BELOW where the caller's walk
/// began is invisible to it, which is why no caller may start a walk part way
/// into a log it has reason to distrust.
///
/// The residue is deliberately NOT width-gated: a torn flush chunk is
/// bounded by the CHUNK, not by one record, and with `durable_segments = false`
/// delayed allocation routinely extends a file far past its written-back
/// pages, leaving hundreds of MiB of zeros behind one crash. That is the
/// canonical torn tail this module exists to truncate, so every residue is
/// probed whole. What bounds the probe instead are the shared work budgets
/// (candidates examined, bytes handed to verification), whose exhaustion
/// REFUSES and keeps the bytes rather than truncating: past the limits the
/// probe has proven nothing, and the cheapest input to construct must never
/// earn the destructive verdict. That holds even when the walk proved not a
/// single batch: recover-as-empty SERVES the segment and re-mints offsets
/// from the previous frontier, so a residue the probe could not finish
/// scanning may still hide the very survivor that would have refused, and
/// only the refusal keeps the partition dark until an operator looks.
async fn refuse_if_survivor_past_damage(
    identity: PartitionIdentity<'_>,
    scanner: &mut FileScanner<'_>,
    messages_path: &str,
    damage_position: u64,
    messages_size: u64,
    chain_end_offset: Option<u64>,
    start_offset: u64,
) -> Result<(), PartitionRecoveryError> {
    if damage_position >= messages_size {
        // The walk consumed the whole file: nothing to classify.
        return Ok(());
    }
    let residue_bytes = messages_size - damage_position;
    scanner.budget.grow_for_residue(residue_bytes);
    match scanner
        .probe_for_survivor(damage_position, chain_end_offset, start_offset)
        .await
        .map_err(|source| scan_read_failure(identity, messages_path, &source))?
    {
        ProbeOutcome::Survivor { position } => {
            Err(identity.refusal(PartitionRecoveryRefusal::InteriorDamage {
                start_offset,
                damage_position,
                survivor_position: position,
            }))
        }
        ProbeOutcome::BudgetExhausted => Err(unverified_residue(
            identity,
            scanner,
            start_offset,
            damage_position,
            messages_size,
        )),
        ProbeOutcome::NoSurvivor => Ok(()),
    }
}

/// Refusal for a scan that ran out of its work budget before classifying
/// the bytes from `damage_position` to the end of the walked file. The
/// counters are diagnostic: they say which budget broke and by how much.
fn unverified_residue(
    identity: PartitionIdentity<'_>,
    scanner: &FileScanner<'_>,
    start_offset: u64,
    damage_position: u64,
    messages_size: u64,
) -> PartitionRecoveryError {
    identity.refusal(PartitionRecoveryRefusal::UnverifiedResidue {
        start_offset,
        damage_position,
        residue_bytes: messages_size.saturating_sub(damage_position),
        candidates_examined: scanner.budget.spent_units,
        budget_units: scanner.budget.limit_units,
        verified_bytes: scanner.budget.verify_spent_bytes,
        verify_budget_bytes: scanner.budget.verify_limit_bytes,
    })
}

/// Verdict of the damage probe over the residue past the walked prefix.
/// `NoSurvivor` is the only verdict that permits truncation; running out of
/// budget is deliberately NOT folded into it, so a residue that is expensive
/// to scan refuses (keeping the bytes) instead of earning the destructive
/// outcome.
enum ProbeOutcome {
    /// A complete, checksum-verifying batch starts at this position.
    Survivor { position: u64 },
    /// The whole residue was scanned and nothing in it verifies.
    NoSurvivor,
    /// The scan budget ran out before the residue was classified.
    BudgetExhausted,
}

/// Buffered reads over one segment file for the recovery walks, the index
/// anchor search, and the damage probe. Parsing and checksumming happen
/// against an in-memory
/// window so neither pays a syscall per batch -- the probe advances its
/// candidate one byte at a time, and per-candidate preads would turn one
/// damaged multi-GiB segment into a boot-length stall.
///
/// Window refills use async reads so online partition recovery can share a
/// shard with request handling, including when the runtime has no blocking pool.
struct FileScanner<'scan> {
    file: &'scan fs::File,
    file_len: u64,
    window: &'scan mut Vec<u8>,
    window_start: u64,
    spill: &'scan mut Vec<u8>,
    budget: &'scan mut ProbeBudget,
    refilled: bool,
}

impl<'scan> FileScanner<'scan> {
    fn new(file: &'scan fs::File, file_len: u64, scratch: &'scan mut ScanScratch) -> Self {
        let ScanScratch {
            window,
            spill,
            probe_budget,
        } = scratch;
        window.clear();
        Self {
            file,
            file_len,
            window,
            window_start: 0,
            spill,
            budget: probe_budget,
            refilled: false,
        }
    }

    /// True when the scanner hit disk since the last call. The async scan
    /// loops yield once per window of CPU work, including when reads complete
    /// immediately, so recovery cannot monopolize the shard.
    fn take_refilled(&mut self) -> bool {
        std::mem::take(&mut self.refilled)
    }

    /// Bytes `[position, position + len)`, or `None` when they run past the
    /// end of the file.
    async fn slice_at(&mut self, position: u64, len: usize) -> io::Result<Option<&[u8]>> {
        let Some(end) = position.checked_add(len as u64) else {
            return Ok(None);
        };
        if end > self.file_len {
            return Ok(None);
        }
        if len > SCAN_WINDOW_CAPACITY {
            // A batch larger than the window: one direct read, no windowing.
            // Callers only pass lengths from headers that already passed the
            // plausibility cap, which is what bounds this resize.
            self.spill.resize(len, 0);
            read_scan_window(self.file, self.spill, position).await?;
            self.refilled = true;
            return Ok(Some(&self.spill[..]));
        }
        let window_end = self.window_start + self.window.len() as u64;
        if position < self.window_start || end > window_end {
            // A forward move anchors the window at `position`, so the walks
            // and the probe stream ahead through it. A backward move (the
            // index anchor search stepping down its entries) anchors the
            // window to END at `end` instead, so the entries below it land in
            // the same window and the refills stay linear in the file rather
            // than costing one window per entry.
            let window_start = if position < self.window_start {
                end.saturating_sub(SCAN_WINDOW_CAPACITY as u64)
            } else {
                position
            };
            let fill =
                usize::try_from((self.file_len - window_start).min(SCAN_WINDOW_CAPACITY as u64))
                    .unwrap_or(SCAN_WINDOW_CAPACITY);
            self.window.resize(fill, 0);
            read_scan_window(self.file, self.window, window_start).await?;
            self.window_start = window_start;
            self.refilled = true;
        }
        // In-window by the branch above, and the window is capacity-bounded,
        // so the try_from cannot fail.
        let start = usize::try_from(position - self.window_start).unwrap_or(0);
        Ok(Some(&self.window[start..start + len]))
    }

    /// The batch command header at `position`, or `None` when it does not fit
    /// the file, does not decode (torn header, garbage bytes), or claims a
    /// size no legal batch can reach. The size check runs BEFORE any caller
    /// slices the claimed extent: an oversized claim cannot be a real batch,
    /// so treating the header as undecodable is verdict-identical to reading
    /// the claimed bytes and failing the verify, and it keeps one bit-flipped
    /// length field from driving a claimed-size allocation and read.
    async fn peek_header(&mut self, position: u64) -> io::Result<Option<BatchHeader>> {
        let Some(bytes) = self.slice_at(position, COMMAND_HEADER_SIZE).await? else {
            return Ok(None);
        };
        Ok(BatchHeader::decode(bytes)
            .ok()
            .filter(|header| header.total_size() as u64 <= MAX_RECOVERABLE_BATCH_BYTES))
    }

    /// Probes the residue for the first complete, checksum-verifying batch
    /// starting after `damage_position`.
    ///
    /// Batch starts are byte-aligned (appends write exact-sized records with
    /// no padding) and the damaged region's own lengths cannot be trusted, so
    /// every byte offset is a candidate. Candidates are scanned inside the
    /// loaded window and the window advances sequentially -- refilled at the
    /// first candidate whose header no longer fits, re-reading at most one
    /// header of overlap -- so each residue byte is read O(1) times instead
    /// of once per candidate. The header decode pre-filters candidates
    /// cheaply (204 reserved bytes must be zero), and offset sanity plus
    /// length bounds run before a verify is paid, so the checksum only runs
    /// on byte positions that already look like a plausible chain
    /// continuation.
    ///
    /// Each candidate examined is charged one unit against the shared
    /// enumeration budget -- examining is flat-cost (zeros bail on the
    /// undersized length, garbage on the first nonzero reserved byte), so
    /// with that budget sized per residue byte an honest front-to-back scan
    /// always fits, at any residue width. Each slice handed to a verify is
    /// charged its byte length against the shared verification budget BEFORE
    /// it is read: candidates advance one byte at a time, so claimed slices
    /// overlap and neither the window advance nor the plausibility cap
    /// bounds their total. Window refills are charged against neither; they
    /// advance strictly forward and are linear in the residue on their own.
    /// Exhaustion of either budget returns
    /// [`ProbeOutcome::BudgetExhausted`], never `NoSurvivor`.
    async fn probe_for_survivor(
        &mut self,
        damage_position: u64,
        chain_end_offset: Option<u64>,
        start_offset: u64,
    ) -> io::Result<ProbeOutcome> {
        let header_len = COMMAND_HEADER_SIZE as u64;
        // The bytes AT the damage already failed to decode or verify, so the
        // first candidate starts one past them.
        let mut candidate = damage_position.saturating_add(1);
        while candidate.saturating_add(header_len) <= self.file_len {
            self.fill_window_at(candidate).await?;
            let window_end = self.window_start + self.window.len() as u64;
            while candidate.saturating_add(header_len) <= window_end {
                if !self.budget.charge_candidate() {
                    return Ok(ProbeOutcome::BudgetExhausted);
                }
                // In-window by the loop bound, and the window is
                // capacity-bounded, so the try_from cannot fail.
                let at = usize::try_from(candidate - self.window_start).unwrap_or(0);
                if let Ok(header) = BatchHeader::decode(&self.window[at..at + COMMAND_HEADER_SIZE])
                {
                    let advances_chain = chain_end_offset
                        .map_or(header.base_offset >= start_offset, |chain_end| {
                            header.base_offset > chain_end
                        });
                    let total_size = header.total_size();
                    // The plausibility cap, not just the file length: with no
                    // width gate on the residue, this is what keeps one
                    // corrupted-upward length claim from driving a
                    // claimed-size spill allocation and read.
                    let fits = total_size as u64 <= MAX_RECOVERABLE_BATCH_BYTES
                        && candidate.saturating_add(total_size as u64) <= self.file_len;
                    if advances_chain && fits && header.message_count > 0 {
                        if !self.budget.charge_verify(total_size as u64) {
                            return Ok(ProbeOutcome::BudgetExhausted);
                        }
                        let batch = self.verify_slice(candidate, total_size).await?;
                        if decode_batch_slice(batch).is_ok() {
                            return Ok(ProbeOutcome::Survivor {
                                position: candidate,
                            });
                        }
                        // Yield per spill read, not only per window: N spill
                        // verifies inside one window would otherwise land in
                        // one un-preemptible synchronous stretch. `take_refilled`
                        // is a take, so this and the outer per-window yield
                        // can never double-fire on the same read.
                        if self.take_refilled() {
                            yield_to_reactor().await;
                        }
                    }
                }
                candidate += 1;
            }
            if self.take_refilled() {
                yield_to_reactor().await;
            }
        }
        Ok(ProbeOutcome::NoSurvivor)
    }

    /// Anchors the window at `position` unless the header there already sits
    /// inside it. The probe's outer loop refills through this, so its
    /// windows advance strictly forward.
    async fn fill_window_at(&mut self, position: u64) -> io::Result<()> {
        let window_end = self.window_start + self.window.len() as u64;
        if position >= self.window_start
            && position.saturating_add(COMMAND_HEADER_SIZE as u64) <= window_end
        {
            return Ok(());
        }
        let fill = usize::try_from((self.file_len - position).min(SCAN_WINDOW_CAPACITY as u64))
            .unwrap_or(SCAN_WINDOW_CAPACITY);
        self.window.resize(fill, 0);
        read_scan_window(self.file, self.window, position).await?;
        self.window_start = position;
        self.refilled = true;
        Ok(())
    }

    /// Bytes `[position, position + len)` for one probe verification without
    /// moving the scan window: an in-window slice costs no read, anything
    /// else is one direct read into the spill buffer. The caller bounds
    /// `len` against the file and the plausibility cap before calling, which
    /// is what bounds the spill's growth.
    async fn verify_slice(&mut self, position: u64, len: usize) -> io::Result<&[u8]> {
        let window_end = self.window_start + self.window.len() as u64;
        let end = position.saturating_add(len as u64);
        if position >= self.window_start && end <= window_end {
            // In-window by the branch above, and the window is
            // capacity-bounded, so the try_from cannot fail.
            let at = usize::try_from(position - self.window_start).unwrap_or(0);
            return Ok(&self.window[at..at + len]);
        }
        self.spill.resize(len, 0);
        read_scan_window(self.file, self.spill, position).await?;
        self.refilled = true;
        Ok(&self.spill[..])
    }
}

fn push_index_entry(rebuilt_index: &mut Vec<u8>, offset: u64, timestamp: u64, position: u64) {
    rebuilt_index.extend_from_slice(&offset.to_le_bytes());
    rebuilt_index.extend_from_slice(&timestamp.to_le_bytes());
    rebuilt_index.extend_from_slice(&position.to_le_bytes());
}

async fn read_scan_window(file: &fs::File, window: &mut Vec<u8>, position: u64) -> io::Result<()> {
    let length = window.len();
    let (result, bytes) = file
        .read_exact_at(std::mem::take(window).slice(..length), position)
        .await
        .into();
    *window = bytes.into_inner();
    result
}

fn read_u64_le(bytes: &[u8], at: usize) -> u64 {
    let mut raw = [0u8; 8];
    raw.copy_from_slice(&bytes[at..at + 8]);
    u64::from_le_bytes(raw)
}

#[cfg(test)]
mod tests;
