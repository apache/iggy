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

//! Server-owned segment recovery.
//!
//! Previously the bootstrap path borrowed `load_segments` from the legacy
//! server implementation to hydrate persisted segments. That loader
//! reads the legacy 16-byte dense per-message index through
//! `server_common::IndexReader`, but the server persists a 24-byte sparse index
//! (`partitions::IggyIndexWriter`: one entry per flush, absolute `offset`,
//! `timestamp`, and batch-start `position`). Reading the 24-byte file with the
//! 16-byte parser mis-strides it (the "Index data must be exactly 16 bytes"
//! recovery panic). This module is the server-owned loader, reading the same
//! 24-byte format its writer emits.

use crate::server_error::{PartitionRecoveryRefusal, ServerError};
use configs::server::ServerConfig;
use iggy_common::{IggyByteSize, IggyError, MAX_MESSAGE_SIZE_UPPER_BYTES, PartitionStats};
use partitions::state_transfer::STAGING_SUFFIX;
use partitions::{IggyIndex, IggyIndexReader, Segment};
use server_common::send_messages::{BatchHeader, COMMAND_HEADER_SIZE, decode_batch_slice};
use server_common::{SegmentStorage, yield_to_reactor};
use std::fs;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use tracing::{error, warn};

const LOG_EXTENSION: &str = "log";
const INDEX_EXTENSION: &str = "index";

/// On-disk stride of one sparse index entry (`offset`, `timestamp`,
/// `position`, each a little-endian u64). Mirrors `IGGY_INDEX_SIZE`, which is
/// crate-private to `partitions` alongside the reader and writer that own the
/// format: the reader's `entry_count` floors by it, and this module needs it
/// to turn that count back into bytes, validate whole entries, and emit
/// rebuilt ones.
const SPARSE_INDEX_ENTRY_SIZE: usize = std::mem::size_of::<u64>() * 3;

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

/// A persisted segment recovered from disk: its metadata plus the storage
/// handles (readers/writers) opened over its `.log` / `.index` files.
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
/// contradictions -- a holed chain, an index diverging from its log, damage
/// with intact batches after it, residue the damage probe could not classify
/// within its limits -- return
/// [`ServerError::PartitionRecoveryRefused`] so the caller can fence this one
/// partition instead of taking the node down.
#[allow(clippy::too_many_lines)]
pub async fn load_persisted_segments(
    config: &ServerConfig,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    segment_size: IggyByteSize,
    stats: &PartitionStats,
) -> Result<Vec<RecoveredSegment>, ServerError> {
    let partition_path = config
        .system
        .get_partition_path(stream_id, topic_id, partition_id);
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
    let mut start_offsets = sweep_scratch_files_and_collect_offsets(&partition_path)?;
    start_offsets.sort_unstable();

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
            config
                .system
                .get_messages_file_path(stream_id, topic_id, partition_id, start_offset);
        let index_path =
            config
                .system
                .get_index_path(stream_id, topic_id, partition_id, start_offset);

        let raw_messages_size = file_len(&messages_path)?;

        let bounds = recover_segment_bounds(
            identity,
            &index_path,
            &messages_path,
            start_offset,
            raw_messages_size,
            &mut scratch,
        )
        .await?;

        // `bounds == None` means the log holds no whole batch ANYWHERE: the
        // index-less walk tried from byte 0 and the damage probe found no
        // surviving batch deeper in the file. There is nothing to serve:
        // zeroed sizes seed fresh empty files (pass C fences the unreadable
        // originals aside rather than deleting them), where counting the
        // bytes with `end_offset == start_offset` would fabricate one
        // phantom message for the bootstrap non-empty filters and strand
        // undecodable garbage inside the readable range. Note this is NOT
        // tail-only -- a torn index is reachable mid-chain on the shipped
        // `enforce_fsync = false`, which is why the walk exists rather than
        // refusing the partition.
        let recovered_empty = bounds.is_none();
        let bounds = bounds.unwrap_or_else(|| {
            if raw_messages_size > 0 {
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
        let rebuilt_index_staging = match &bounds.rebuilt_index {
            Some(entries) => Some(stage_rebuilt_index(&index_path, entries)?),
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

    // Pass B: the chain guard reads only the planned bounds, so it can refuse
    // BEFORE anything is truncated.
    ensure_contiguous_chain(identity, &planned)?;

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
            )?;
        }
        // Log first, index second: a walk only accepts bounds when a whole
        // batch decodes at the last index entry's position, so the walked log
        // length strictly exceeds that position and every surviving index
        // entry still points inside the shortened log even if a crash lands
        // between the two mutations. The staged-rebuild install keeps the
        // same property: until its rename lands, the on-disk index still
        // holds no whole entry, so a crash between the two re-runs the
        // index-less walk over the already-truncated log.
        truncate_to(&plan.messages_path, messages_size)?;
        if let Some(staging_path) = &plan.rebuilt_index_staging {
            install_rebuilt_index(staging_path, &plan.index_path, identity.partition_path)?;
        } else {
            truncate_to(&plan.index_path, plan.index_size)?;
        }

        let storage = SegmentStorage::new(
            &plan.messages_path,
            &plan.index_path,
            messages_size,
            plan.index_size,
            true,
        )
        .await
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

        stats.increment_segments_count(1);
        stats.increment_size_bytes(messages_size);
        if messages_size > 0 {
            // Offsets in a segment are contiguous (either walk refuses a
            // discontinuity), so the message count is the inclusive span
            // between the first (segment start) and last offset. Saturating:
            // an end offset at u64::MAX must not wrap the counter.
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
    fn refusal(&self, reason: PartitionRecoveryRefusal) -> ServerError {
        ServerError::PartitionRecoveryRefused {
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
/// Runs on the planned bounds alone, BEFORE any truncation, so the segment
/// files a refusal quarantines are exactly the bytes boot found. The refusal
/// names the partition and its directory so the caller can fence THAT group
/// rather than abort the node's boot: the shapes it rejects are exactly what
/// a failed quarantine leaves behind, and one damaged local chain must not
/// take the whole node down.
fn ensure_contiguous_chain(
    identity: PartitionIdentity<'_>,
    planned: &[PlannedSegment],
) -> Result<(), ServerError> {
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
        if previous.end_offset.checked_add(1) != Some(next.start_offset) {
            return Err(identity.refusal(PartitionRecoveryRefusal::Hole {
                previous_start: previous.start_offset,
                previous_end: previous.end_offset,
                next_start: next.start_offset,
                recoverable_bytes,
            }));
        }
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
fn sweep_scratch_files_and_collect_offsets(partition_path: &str) -> Result<Vec<u64>, ServerError> {
    let entries = match fs::read_dir(partition_path) {
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
    for entry in entries.flatten() {
        let path = entry.path();
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
            Some(INDEX_EXTENSION) => orphan_candidates.push(path),
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
        if let Err(error) = fs::remove_file(&path) {
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
fn file_len(path: &str) -> Result<u64, ServerError> {
    match fs::metadata(path) {
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
///
/// Stats the file fresh instead of trusting a length carried from pass A: the
/// whole chain was walked in between, and the mutation must key on what is on
/// disk now. Synchronous `std::fs` on purpose (see [`FileScanner`]). The
/// fsync bounds the crash window: a power cut right after `set_len` may
/// re-present the torn tail on the next boot, which only walks and truncates
/// again (idempotent), but the sync keeps the common case deterministic.
fn truncate_to(path: &str, target_size: u64) -> Result<(), ServerError> {
    let current_size = file_len(path)?;
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
    warn!(
        path,
        current_size,
        target_size,
        "truncating a segment file to its recovered bounds; discarding \
         torn tail bytes"
    );
    let file = fs::OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|source| {
            error!(
                path,
                error = %source,
                "failed to open a segment file for truncation during recovery"
            );
            ServerError::from(IggyError::CannotWriteToFile)
        })?;
    file.set_len(target_size).map_err(|source| {
        error!(
            path,
            target_size,
            error = %source,
            "failed to truncate a segment file to its recovered bounds"
        );
        ServerError::from(IggyError::CannotWriteToFile)
    })?;
    file.sync_all().map_err(|source| {
        error!(
            path,
            error = %source,
            "failed to fsync a segment file after truncation"
        );
        ServerError::from(IggyError::CannotSyncFile)
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
fn stage_rebuilt_index(index_path: &str, entries: &[u8]) -> Result<String, ServerError> {
    let staging_path = format!("{index_path}{STAGING_SUFFIX}");
    let file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&staging_path)
        .map_err(|source| {
            error!(
                path = %staging_path,
                error = %source,
                "failed to open a sparse index staging file during recovery"
            );
            ServerError::from(IggyError::CannotWriteToFile)
        })?;
    file.write_all_at(entries, 0).map_err(|source| {
        error!(
            path = %staging_path,
            error = %source,
            "failed to write a rebuilt sparse index during recovery"
        );
        ServerError::from(IggyError::CannotWriteToFile)
    })?;
    file.sync_all().map_err(|source| {
        error!(
            path = %staging_path,
            error = %source,
            "failed to fsync a rebuilt sparse index after recovery"
        );
        ServerError::from(IggyError::CannotSyncFile)
    })?;
    Ok(staging_path)
}

/// Installs a staged rebuilt index at its final name. The rename is the
/// atomic commit point: the on-disk index is either the old one holding no
/// whole entry (whose walk re-runs the rebuild) or the complete rebuilt one,
/// never a mix of pages from both.
fn install_rebuilt_index(
    staging_path: &str,
    index_path: &str,
    partition_path: &str,
) -> Result<(), ServerError> {
    fs::rename(staging_path, index_path).map_err(|source| {
        error!(
            from = %staging_path,
            to = %index_path,
            error = %source,
            "failed to rename a rebuilt sparse index into place during recovery"
        );
        ServerError::from(IggyError::CannotWriteToFile)
    })?;
    fsync_dir(partition_path)
}

/// Makes renames and new files in `dir` durable. Synchronous like every
/// other mutation in this module (see [`FileScanner`]).
fn fsync_dir(dir: &str) -> Result<(), ServerError> {
    fs::File::open(dir)
        .and_then(|handle| handle.sync_all())
        .map_err(|source| {
            error!(
                dir,
                error = %source,
                "failed to fsync a directory during recovery"
            );
            ServerError::from(IggyError::CannotSyncFile)
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
fn fence_unrecoverable_segment_files(
    identity: PartitionIdentity<'_>,
    messages_path: &str,
    index_path: &str,
    start_offset: u64,
) -> Result<(), ServerError> {
    let log_bytes = file_len(messages_path)?;
    let index_bytes = file_len(index_path)?;
    if log_bytes == 0 && index_bytes == 0 {
        return Ok(());
    }
    let mut fenced_dir = None;
    for attempt in 0..FENCED_DIR_PROBE_LIMIT {
        let candidate = format!("{}.fenced.{attempt}", identity.partition_path);
        // `create_dir`, not `create_dir_all`: success is the claim on this
        // suffix, and merging into an existing fence would mix evidence from
        // two incidents.
        match fs::create_dir(&candidate) {
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
    rename_into_fence(messages_path, &fenced_log)?;
    rename_into_fence(index_path, &fenced_index)?;
    seed_empty_file(index_path)?;
    seed_empty_file(messages_path)?;
    // The fence directory's new dirents, the partition directory's renames
    // plus fresh files, and the parent's new fence-directory dirent.
    fsync_dir(&fenced_dir)?;
    fsync_dir(identity.partition_path)?;
    if let Some(parent) = Path::new(identity.partition_path)
        .parent()
        .and_then(Path::to_str)
    {
        fsync_dir(parent)?;
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
fn fenced_target(fenced_dir: &str, source_path: &str) -> Result<PathBuf, ServerError> {
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

fn rename_into_fence(source_path: &str, target: &Path) -> Result<(), ServerError> {
    match fs::rename(source_path, target) {
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

fn seed_empty_file(path: &str) -> Result<(), ServerError> {
    fs::File::create(path)
        .and_then(|file| file.sync_all())
        .map_err(|source| {
            error!(
                path,
                error = %source,
                "failed to seed a fresh empty segment file after fencing"
            );
            ServerError::from(IggyError::CannotWriteToFile)
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
/// plain `ServerError::Iggy` -- not a `PartitionRecoveryRefused` the caller
/// can fence -- so it would abort the whole boot for a segment the walk
/// rebuilds. Stat through the `NotFound`-lenient [`file_len`] first; every
/// other stat failure still fails stop there.
async fn load_index_anchors(
    identity: PartitionIdentity<'_>,
    index_path: &str,
) -> Result<(u64, Option<IggyIndex>, Option<IggyIndex>), ServerError> {
    if file_len(index_path)? == 0 {
        return Ok((0, None, None));
    }
    let reader = IggyIndexReader::new(index_path).await.map_err(|source| {
        error!(
            stream_id = identity.stream_id,
            topic_id = identity.topic_id,
            partition_id = identity.partition_id,
            path = %index_path,
            error = %source,
            "failed to open sparse index during recovery"
        );
        source
    })?;
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
/// With a whole index entry present, the last entry's `position` is only the
/// last flushed chunk's START byte, so the batch chain is walked from there to
/// prove where the segment really ends -- without `enforce_fsync` there is no
/// ordering barrier between the message write and the index write, and a tail
/// torn mid-flush would otherwise pass while `end_offset` claims offsets whose
/// bytes are incomplete. Without one, the log itself is walked from byte 0 and
/// the index is rebuilt from the batches found. Either way, bytes left past
/// the walked prefix go through the damage probe: a torn tail truncates, but
/// damage with intact batches after it -- or residue the probe cannot
/// classify within its limits -- refuses recovery.
#[allow(clippy::too_many_lines)]
async fn recover_segment_bounds(
    identity: PartitionIdentity<'_>,
    index_path: &str,
    messages_path: &str,
    start_offset: u64,
    messages_size: u64,
    scratch: &mut ScanScratch,
) -> Result<Option<WalkedBounds>, ServerError> {
    let (entry_count, first, last) = load_index_anchors(identity, index_path).await?;

    match (first, last) {
        (Some(first), Some(last)) => {
            // Interior entries were never validated before: a mis-strided
            // index can decode to garbage entries that binary searches then
            // trust. Monotonicity plus the walk's own anchor bound them: the
            // walk below only accepts bounds when a whole batch decodes at
            // the LAST entry's position, so ascending positions keep every
            // surviving entry inside the truncated log.
            validate_index_entries(identity, index_path, start_offset, entry_count, scratch)?;

            let messages = open_messages_file(identity, messages_path)?;
            let mut scanner = FileScanner::new(&messages, messages_size, scratch);
            // The sparse index holds ONE entry per flushed chunk, pointing
            // at the chunk's FIRST batch -- `last.offset` is where the last
            // chunk STARTS, not where the segment ends (a whole journal
            // flushed as one chunk indexes only its first offset). Walk the
            // batch chain from that position to the file end to recover the
            // true end offset.
            let mut position = last.position;
            let mut end_offset = last.offset;
            let mut end_timestamp = last.timestamp;
            let mut expected_offset = last.offset;
            let mut walked_any = false;
            // TODO(hubcio): for batches that continue the chain exactly this
            // indexed walk trusts the header decode alone (batches that
            // contradict the walk checksum-verify below), so a torn flush
            // that persisted the header page but zeroed the body is absorbed
            // silently; the index-less walk below checksums every batch.
            // Decide whether the indexed arm should checksum too (boot cost)
            // or leave body rot to protocol-aware repair.
            while position < messages_size {
                let header = match scanner.peek_header(position) {
                    Ok(Some(header)) => header,
                    Ok(None) => break,
                    Err(source) => {
                        return Err(scan_read_failure(identity, messages_path, &source));
                    }
                };
                let extent = position.saturating_add(header.total_size() as u64);
                if extent > messages_size {
                    break;
                }
                // A header contradicting the walk -- a foreign partition_id,
                // or a base offset that does not continue the chain in
                // EITHER direction -- is either damage wearing a decodable
                // header or a real record that does not belong at this
                // position. The batch checksum tells them apart, because it
                // covers both fields and the server mints every legal record
                // with it: a batch that fails it is damage, so break and let
                // the probe below classify the residue (a bit flip in a tail
                // batch then truncates like any torn tail, regardless of the
                // flip's direction), while a batch that VERIFIES is durable
                // evidence -- of a misdirected or copied write when the
                // partition stamp is foreign, of duplicated or lost offsets
                // when the chain breaks -- and both absorbing it and
                // truncating it would hide that, so refuse and keep the
                // bytes. Refusing the verified forward gap over absorbing it
                // is deliberate: state transfer's own walk refuses any gap,
                // so an absorbed one would mint a segment no peer can ever
                // install, and the offsets it fabricates seed current_offset
                // under an advance-only superblock persist.
                let foreign_partition = header.partition_id != identity.partition_id as u64;
                if foreign_partition || header.base_offset != expected_offset {
                    let verifies = scanner
                        .slice_at(position, header.total_size())
                        .map_err(|source| scan_read_failure(identity, messages_path, &source))?
                        .is_some_and(|batch| decode_batch_slice(batch).is_ok());
                    if !verifies {
                        break;
                    }
                    if foreign_partition {
                        return Err(identity.refusal(PartitionRecoveryRefusal::ForeignBatch {
                            start_offset,
                            batch_partition_id: header.partition_id,
                            position,
                        }));
                    }
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
                    expected_offset = end_offset.saturating_add(1);
                }
                walked_any = true;
                position = extent;
                if scanner.take_refilled() {
                    yield_to_reactor().await;
                }
            }
            if !walked_any {
                // Probe before concluding: a verifying batch past the bytes
                // that broke the walk at the anchor is a survivor, and
                // refusing it as a divergence would claim the log holds
                // nothing while durable data sits in it.
                refuse_if_survivor_past_damage(
                    identity,
                    &mut scanner,
                    messages_path,
                    position,
                    messages_size,
                    Some(end_offset),
                    start_offset,
                )
                .await?;
                return Err(
                    identity.refusal(PartitionRecoveryRefusal::IndexLogDivergence {
                        start_offset,
                        end_offset: last.offset,
                        messages_size_bytes: messages_size,
                        indexed_size_bytes: last.position,
                    }),
                );
            }
            refuse_if_survivor_past_damage(
                identity,
                &mut scanner,
                messages_path,
                position,
                messages_size,
                Some(end_offset),
                start_offset,
            )
            .await?;
            Ok(Some(WalkedBounds {
                start_timestamp: first.timestamp,
                end_timestamp,
                end_offset,
                messages_size: position,
                index_size: entry_count * SPARSE_INDEX_ENTRY_SIZE as u64,
                rebuilt_index: None,
            }))
        }
        // No whole index entry, but the log holds bytes: recover the bounds by
        // WALKING the log from byte 0 instead of declaring the segment empty.
        //
        // The index is not the only self-describing copy -- batch headers carry
        // their own offsets, timestamps and lengths -- and with the shipped
        // `enforce_fsync = false` there is no write ordering between a log and
        // its index, so a torn index is reachable on default config for a
        // MID-CHAIN segment too, not just the tail. Recovering that as empty
        // then trips the contiguity guard and refuses the whole partition:
        // total serve loss (and offset reuse from 0) for a chain whose bytes
        // are all present. The walk keeps the torn-tail truncation the indexed
        // path performs, and rebuilds the index from the batches it proves so
        // a sealed segment does not pay a full-scan poll penalty forever.
        _ if messages_size > 0 => {
            let messages = open_messages_file(identity, messages_path)?;
            let mut scanner = FileScanner::new(&messages, messages_size, scratch);
            let mut position = 0u64;
            let mut start_timestamp = None;
            let mut end_offset = start_offset;
            let mut end_timestamp = 0;
            let mut expected_offset = start_offset;
            let mut rebuilt_index = Vec::new();
            let mut last_indexed_position: Option<u64> = None;
            while position < messages_size {
                let header = match scanner.peek_header(position) {
                    Ok(Some(header)) => header,
                    Ok(None) => break,
                    Err(source) => {
                        return Err(scan_read_failure(identity, messages_path, &source));
                    }
                };
                let extent = position.saturating_add(header.total_size() as u64);
                if extent > messages_size {
                    break;
                }
                // The FILENAME is the only trustworthy anchor once the index
                // is gone, and the header decode checks a length, not a
                // checksum. So the batch has to verify before its header is
                // believed, and the chain has to be contiguous from the
                // filename onward.
                let verifies = scanner
                    .slice_at(position, header.total_size())
                    .map_err(|source| scan_read_failure(identity, messages_path, &source))?
                    .is_some_and(|batch| decode_batch_slice(batch).is_ok());
                if !verifies {
                    break;
                }
                if header.partition_id != identity.partition_id as u64 {
                    // Verified above, so this is a real record minted for
                    // another partition (a misdirected write, an operator
                    // copy), not damage: preserve it as evidence.
                    return Err(identity.refusal(PartitionRecoveryRefusal::ForeignBatch {
                        start_offset,
                        batch_partition_id: header.partition_id,
                        position,
                    }));
                }
                if header.base_offset != expected_offset {
                    // A batch that VERIFIES but does not continue the chain is
                    // durable data past a hole (or a duplicated range): the
                    // offsets in between are exactly what a truncation here
                    // would silently erase, so refuse instead.
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
                &mut scanner,
                messages_path,
                position,
                messages_size,
                start_timestamp.map(|_| end_offset),
                start_offset,
            )
            .await?;
            let Some(start_timestamp) = start_timestamp else {
                // Not one whole batch, and the probe above proved nothing
                // decodable follows either: the bytes really are unusable, so
                // the caller's empty recovery is right after all.
                return Ok(None);
            };
            warn!(
                stream_id = identity.stream_id,
                topic_id = identity.topic_id,
                partition_id = identity.partition_id,
                start_offset,
                messages_size,
                walked_size = position,
                rebuilt_entries = rebuilt_index.len() / SPARSE_INDEX_ENTRY_SIZE,
                "sparse index holds no whole entry; recovered segment bounds \
                 by walking the log and rebuilding its index from the walked \
                 batches"
            );
            Ok(Some(WalkedBounds {
                start_timestamp,
                end_timestamp,
                end_offset,
                messages_size: position,
                index_size: rebuilt_index.len() as u64,
                rebuilt_index: Some(rebuilt_index),
            }))
        }
        _ => Ok(None),
    }
}

/// Validates every whole index entry: the first must not claim an offset
/// below the segment's own start, and offsets and positions must strictly
/// ascend (the writer appends one entry per flushed chunk over a growing
/// log, and every chunk covers at least one message and one byte).
///
/// Timestamps are deliberately NOT validated: a primary clock rewind across a
/// restart can legitimately regress persisted `base_timestamp` today, and the
/// lower-bound searches degrade gracefully on a non-monotone run, so refusing
/// would trade availability for nothing.
fn validate_index_entries(
    identity: PartitionIdentity<'_>,
    index_path: &str,
    start_offset: u64,
    entry_count: u64,
    scratch: &mut ScanScratch,
) -> Result<(), ServerError> {
    let file = fs::File::open(index_path).map_err(|source| {
        error!(
            stream_id = identity.stream_id,
            topic_id = identity.topic_id,
            partition_id = identity.partition_id,
            path = %index_path,
            error = %source,
            "failed to open sparse index for validation during recovery"
        );
        ServerError::from(IggyError::CannotReadFile)
    })?;
    let window = &mut scratch.window;
    let per_chunk_entries = SCAN_WINDOW_CAPACITY / SPARSE_INDEX_ENTRY_SIZE;
    let mut previous: Option<(u64, u64)> = None;
    let mut entry_index = 0u64;
    let mut byte_position = 0u64;
    while entry_index < entry_count {
        let chunk_entries = (entry_count - entry_index).min(per_chunk_entries as u64);
        // Bounded by the window capacity, so the try_from cannot fail.
        let chunk_bytes =
            usize::try_from(chunk_entries).unwrap_or(per_chunk_entries) * SPARSE_INDEX_ENTRY_SIZE;
        window.resize(chunk_bytes, 0);
        file.read_exact_at(&mut window[..], byte_position)
            .map_err(|source| {
                error!(
                    stream_id = identity.stream_id,
                    topic_id = identity.topic_id,
                    partition_id = identity.partition_id,
                    path = %index_path,
                    error = %source,
                    "failed to read sparse index entries for validation during recovery"
                );
                ServerError::from(IggyError::CannotReadFile)
            })?;
        for entry in window.chunks_exact(SPARSE_INDEX_ENTRY_SIZE) {
            let entry_offset = read_u64_le(entry, 0);
            let entry_position = read_u64_le(entry, 16);
            if let Some((previous_offset, previous_position)) = previous
                && (entry_offset <= previous_offset || entry_position <= previous_position)
            {
                return Err(
                    identity.refusal(PartitionRecoveryRefusal::IndexEntriesNotMonotone {
                        start_offset,
                        entry_index,
                    }),
                );
            }
            if previous.is_none() && entry_offset < start_offset {
                return Err(identity.refusal(
                    PartitionRecoveryRefusal::IndexEntryBeforeSegmentStart {
                        start_offset,
                        first_entry_offset: entry_offset,
                    },
                ));
            }
            previous = Some((entry_offset, entry_position));
            entry_index += 1;
        }
        byte_position += chunk_bytes as u64;
    }
    Ok(())
}

/// Opens a segment's messages file for the recovery walk. Fail-stop on any
/// failure, mirroring `file_len`: recovery truncates to the bounds the walk
/// produces, so folding an open failure into "walked nothing" would route a
/// healthy indexed segment into a divergence refusal -- or an index-less one
/// into recover-as-empty, fencing the whole log out of service.
fn open_messages_file(
    identity: PartitionIdentity<'_>,
    messages_path: &str,
) -> Result<fs::File, ServerError> {
    fs::File::open(messages_path).map_err(|source| {
        error!(
            stream_id = identity.stream_id,
            topic_id = identity.topic_id,
            partition_id = identity.partition_id,
            path = %messages_path,
            error = %source,
            "failed to open a segment messages file during recovery"
        );
        ServerError::from(IggyError::CannotReadFile)
    })
}

/// A read failure inside the walk or probe is transient I/O, not evidence
/// about the bytes: fail stop rather than classify it as a torn tail, which
/// would truncate a healthy segment on an `EIO`.
fn scan_read_failure(
    identity: PartitionIdentity<'_>,
    path: &str,
    source: &io::Error,
) -> ServerError {
    error!(
        stream_id = identity.stream_id,
        topic_id = identity.topic_id,
        partition_id = identity.partition_id,
        path = %path,
        error = %source,
        "failed to read a segment file during the recovery walk"
    );
    ServerError::from(IggyError::CannotReadFile)
}

/// Classifies bytes left past the walked prefix, porting the WAL repair's
/// rule: truncation is sound only for a torn tail, and the question that
/// decides it is whether a complete entry follows the damage. A batch that
/// decodes, checksums, and plausibly extends the chain is durable data -- it
/// can only exist because an append completed after the damaged region -- so
/// discarding it would hide real loss behind a silent boot-time repair.
///
/// The residue is deliberately NOT width-gated: a torn flush chunk is
/// bounded by the CHUNK, not by one record, and with `enforce_fsync = false`
/// delayed allocation routinely extends a file far past its written-back
/// pages, leaving hundreds of MiB of zeros behind one crash. That is the
/// canonical torn tail this module exists to truncate, so every residue is
/// probed whole. What bounds the probe instead are the shared work budgets
/// (candidates examined, bytes handed to verification), whose exhaustion
/// REFUSES and keeps the bytes rather than truncating: past the limits the
/// probe has proven nothing, and the cheapest input to construct must never
/// earn the destructive verdict. One exception folds exhaustion forward
/// instead: when the walk proved not a single batch (`chain_end_offset` is
/// `None`), the refusal and the no-survivor verdict converge on the same
/// recover-as-empty outcome -- the pair is fenced aside whole, bytes
/// preserved either way -- so exhaustion there returns `Ok` rather than
/// trading a fence for a tombstone.
async fn refuse_if_survivor_past_damage(
    identity: PartitionIdentity<'_>,
    scanner: &mut FileScanner<'_>,
    messages_path: &str,
    damage_position: u64,
    messages_size: u64,
    chain_end_offset: Option<u64>,
    start_offset: u64,
) -> Result<(), ServerError> {
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
        ProbeOutcome::BudgetExhausted if chain_end_offset.is_none() => Ok(()),
        ProbeOutcome::BudgetExhausted => Err(identity.refusal(
            PartitionRecoveryRefusal::UnverifiedResidue {
                start_offset,
                damage_position,
                residue_bytes,
                candidates_examined: scanner.budget.spent_units,
                budget_units: scanner.budget.limit_units,
                verified_bytes: scanner.budget.verify_spent_bytes,
                verify_budget_bytes: scanner.budget.verify_limit_bytes,
            },
        )),
        ProbeOutcome::NoSurvivor => Ok(()),
    }
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

/// Forward-only buffered reads over one segment file for the recovery walk
/// and the damage probe. Parsing and checksumming happen against an in-memory
/// window so neither pays a syscall per batch -- the probe advances its
/// candidate one byte at a time, and per-candidate preads would turn one
/// damaged multi-GiB segment into a boot-length stall.
///
/// Synchronous `std::fs` on purpose, like every mutation in this module: the
/// boot path's runtime sizes its blocking pool at zero and recovery must not
/// depend on `io_uring` opcode coverage. Only the sparse-index bound reads go
/// through the async `IggyIndexReader`.
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
    /// loops yield to the reactor once per window of work on it: recovery
    /// runs in front of the bootstrap barrier with the blocking pool sized
    /// at zero, so an unyielding walk over a damaged multi-GiB chain would
    /// pin the shard core -- signal handling included -- until it finishes.
    fn take_refilled(&mut self) -> bool {
        std::mem::take(&mut self.refilled)
    }

    /// Bytes `[position, position + len)`, or `None` when they run past the
    /// end of the file.
    fn slice_at(&mut self, position: u64, len: usize) -> io::Result<Option<&[u8]>> {
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
            self.file.read_exact_at(&mut self.spill[..], position)?;
            self.refilled = true;
            return Ok(Some(&self.spill[..]));
        }
        let window_end = self.window_start + self.window.len() as u64;
        if position < self.window_start || end > window_end {
            let fill = usize::try_from((self.file_len - position).min(SCAN_WINDOW_CAPACITY as u64))
                .unwrap_or(SCAN_WINDOW_CAPACITY);
            self.window.resize(fill, 0);
            self.file.read_exact_at(&mut self.window[..], position)?;
            self.window_start = position;
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
    fn peek_header(&mut self, position: u64) -> io::Result<Option<BatchHeader>> {
        let Some(bytes) = self.slice_at(position, COMMAND_HEADER_SIZE)? else {
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
            self.fill_window_at(candidate)?;
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
                        let batch = self.verify_slice(candidate, total_size)?;
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
    fn fill_window_at(&mut self, position: u64) -> io::Result<()> {
        let window_end = self.window_start + self.window.len() as u64;
        if position >= self.window_start
            && position.saturating_add(COMMAND_HEADER_SIZE as u64) <= window_end
        {
            return Ok(());
        }
        let fill = usize::try_from((self.file_len - position).min(SCAN_WINDOW_CAPACITY as u64))
            .unwrap_or(SCAN_WINDOW_CAPACITY);
        self.window.resize(fill, 0);
        self.file.read_exact_at(&mut self.window[..], position)?;
        self.window_start = position;
        self.refilled = true;
        Ok(())
    }

    /// Bytes `[position, position + len)` for one probe verification without
    /// moving the scan window: an in-window slice costs no read, anything
    /// else is one direct read into the spill buffer. The caller bounds
    /// `len` against the file and the plausibility cap before calling, which
    /// is what bounds the spill's growth.
    fn verify_slice(&mut self, position: u64, len: usize) -> io::Result<&[u8]> {
        let window_end = self.window_start + self.window.len() as u64;
        let end = position.saturating_add(len as u64);
        if position >= self.window_start && end <= window_end {
            // In-window by the branch above, and the window is
            // capacity-bounded, so the try_from cannot fail.
            let at = usize::try_from(position - self.window_start).unwrap_or(0);
            return Ok(&self.window[at..at + len]);
        }
        self.spill.resize(len, 0);
        self.file.read_exact_at(&mut self.spill[..], position)?;
        self.refilled = true;
        Ok(&self.spill[..])
    }
}

fn push_index_entry(rebuilt_index: &mut Vec<u8>, offset: u64, timestamp: u64, position: u64) {
    rebuilt_index.extend_from_slice(&offset.to_le_bytes());
    rebuilt_index.extend_from_slice(&timestamp.to_le_bytes());
    rebuilt_index.extend_from_slice(&position.to_le_bytes());
}

fn read_u64_le(bytes: &[u8], at: usize) -> u64 {
    let mut raw = [0u8; 8];
    raw.copy_from_slice(&bytes[at..at + 8]);
    u64::from_le_bytes(raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use configs::server::ServerSystemConfig;
    use server_common::send_messages::{
        IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned, calculate_batch_checksum,
    };
    use server_common::sharding::IggyNamespace;
    use std::os::unix::fs::symlink;
    use std::sync::Arc;
    use tempfile::{TempDir, tempdir};

    const STREAM_ID: usize = 1;
    const TOPIC_ID: usize = 1;
    const PARTITION_ID: usize = 1;
    const SEGMENT_MAX_SIZE: u64 = 16 * 1024 * 1024;
    const FIXTURE_TIMESTAMP: u64 = 1_700_000_000_000_000;
    // Longer than one batch header and nonzero in the header's reserved
    // region, so no prefix of it decodes as a batch.
    const GARBAGE: [u8; 384] = [0xAB; 384];

    fn test_config(tmp: &TempDir) -> ServerConfig {
        let mut config = ServerConfig::default();
        // `ServerSystemConfig` is not `Clone`; build a fresh value and swap
        // the whole `Arc`.
        let system = ServerSystemConfig {
            path: tmp.path().to_string_lossy().into_owned(),
            ..ServerSystemConfig::default()
        };
        config.system = Arc::new(system);
        config
    }

    fn prepare_partition_dir(config: &ServerConfig) -> String {
        let partition_path = config
            .system
            .get_partition_path(STREAM_ID, TOPIC_ID, PARTITION_ID);
        fs::create_dir_all(&partition_path).expect("create partition dir");
        partition_path
    }

    // Batch header wire offsets the zero-padded fixtures plant values at.
    const HEADER_BATCH_LENGTH_OFFSET: usize = 32;
    const HEADER_MESSAGE_COUNT_OFFSET: usize = 48;

    /// A zero-padded record that also plants a `base_offset`, so probe
    /// candidates that hit it look like a plausible chain continuation and
    /// pay a checksum verify over the whole claimed slice (which never
    /// verifies: the stored batch checksum stays zero).
    fn bait_record(base_offset: u64, claimed_batch_length: u64, sequence: u32) -> Vec<u8> {
        let mut record = zero_padded_record(claimed_batch_length, sequence);
        record[HEADER_BASE_OFFSET_OFFSET..HEADER_BASE_OFFSET_OFFSET + 8]
            .copy_from_slice(&base_offset.to_le_bytes());
        record
    }

    /// One fixed-width zero-padded record of the shape foreign storage
    /// formats emit: a monotone u64 where the batch header keeps
    /// `batch_length`, a nonzero u32 where it keeps `message_count`, zeros
    /// everywhere else -- so its header decodes without any of it being a
    /// batch.
    fn zero_padded_record(claimed_batch_length: u64, sequence: u32) -> Vec<u8> {
        let mut record = vec![0u8; COMMAND_HEADER_SIZE];
        record[HEADER_BATCH_LENGTH_OFFSET..HEADER_BATCH_LENGTH_OFFSET + 8]
            .copy_from_slice(&claimed_batch_length.to_le_bytes());
        record[HEADER_MESSAGE_COUNT_OFFSET..HEADER_MESSAGE_COUNT_OFFSET + 4]
            .copy_from_slice(&sequence.to_le_bytes());
        record
    }

    /// One valid on-disk batch record: real message frames with their
    /// per-message checksums, and the server-owned header fields stamped the
    /// way persistence stamps them.
    fn encoded_batch(base_offset: u64, message_count: usize) -> Vec<u8> {
        encoded_batch_with_payload(
            base_offset,
            message_count,
            &Bytes::from_static(b"segment-recovery-fixture"),
        )
    }

    fn encoded_batch_with_payload(
        base_offset: u64,
        message_count: usize,
        payload: &Bytes,
    ) -> Vec<u8> {
        encoded_batch_stamped(base_offset, message_count, payload, PARTITION_ID as u64)
    }

    /// Like [`encoded_batch`], but stamped with an arbitrary `partition_id`
    /// and re-checksummed, so a foreign record verifies while contradicting
    /// the identity of the partition being recovered.
    fn encoded_foreign_batch(base_offset: u64, message_count: usize, partition_id: u64) -> Vec<u8> {
        encoded_batch_stamped(
            base_offset,
            message_count,
            &Bytes::from_static(b"segment-recovery-fixture"),
            partition_id,
        )
    }

    fn encoded_batch_stamped(
        base_offset: u64,
        message_count: usize,
        payload: &Bytes,
        partition_id: u64,
    ) -> Vec<u8> {
        let mut messages = IggyMessages::with_capacity(message_count);
        for _ in 0..message_count {
            messages.push(IggyMessage {
                header: IggyMessageHeader {
                    origin_timestamp: FIXTURE_TIMESTAMP,
                    ..IggyMessageHeader::default()
                },
                payload: payload.clone(),
                user_headers: None,
            });
        }
        let namespace = IggyNamespace::new(STREAM_ID, TOPIC_ID, PARTITION_ID);
        let SendMessagesOwned { mut header, blob } =
            SendMessagesOwned::from_messages(namespace, &messages).expect("encode fixture batch");
        header.partition_id = partition_id;
        header.base_offset = base_offset;
        header.base_timestamp = FIXTURE_TIMESTAMP;
        header.batch_checksum = calculate_batch_checksum(&header, &blob);
        let mut record = vec![0u8; header.total_size()];
        header.encode_into(&mut record[..COMMAND_HEADER_SIZE]);
        record[COMMAND_HEADER_SIZE..].copy_from_slice(&blob);
        record
    }

    /// One sparse index entry, mirroring the `IggyIndexWriter` layout the
    /// recovery reader expects.
    fn index_entry(offset: u64, position: u64) -> Vec<u8> {
        let mut entry = Vec::new();
        entry.extend_from_slice(&offset.to_le_bytes());
        entry.extend_from_slice(&FIXTURE_TIMESTAMP.to_le_bytes());
        entry.extend_from_slice(&position.to_le_bytes());
        entry
    }

    /// Writes a segment's `.log` and `.index` fixtures and returns their
    /// paths as `(messages_path, index_path)`.
    fn write_segment(
        config: &ServerConfig,
        start_offset: u64,
        log: &[u8],
        index: &[u8],
    ) -> (String, String) {
        let messages_path =
            config
                .system
                .get_messages_file_path(STREAM_ID, TOPIC_ID, PARTITION_ID, start_offset);
        let index_path =
            config
                .system
                .get_index_path(STREAM_ID, TOPIC_ID, PARTITION_ID, start_offset);
        fs::write(&messages_path, log).expect("write log fixture");
        fs::write(&index_path, index).expect("write index fixture");
        (messages_path, index_path)
    }

    fn len_of(path: &str) -> u64 {
        fs::metadata(path).expect("stat fixture file").len()
    }

    fn bytes_of(path: &str) -> Vec<u8> {
        fs::read(path).expect("read fixture file")
    }

    async fn recover(config: &ServerConfig) -> Result<Vec<RecoveredSegment>, ServerError> {
        load_persisted_segments(
            config,
            STREAM_ID,
            TOPIC_ID,
            PARTITION_ID,
            IggyByteSize::from(SEGMENT_MAX_SIZE),
            &PartitionStats::default(),
        )
        .await
    }

    #[compio::test]
    async fn given_torn_log_tail_when_recovering_should_truncate_files_to_walked_bounds() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        let mut log = encoded_batch(0, 3);
        let valid_len = log.len() as u64;
        log.extend_from_slice(&GARBAGE);
        let (messages_path, index_path) = write_segment(&config, 0, &log, &index_entry(0, 0));

        let recovered = recover(&config).await.expect("recover torn-tail segment");

        assert_eq!(recovered.len(), 1);
        let segment = &recovered[0].segment;
        assert_eq!(segment.end_offset, 2);
        assert_eq!(segment.size, IggyByteSize::from(valid_len));
        assert_eq!(segment.current_position, valid_len);
        assert_eq!(
            len_of(&messages_path),
            valid_len,
            "torn tail bytes must be gone from disk"
        );
        assert_eq!(len_of(&index_path), SPARSE_INDEX_ENTRY_SIZE as u64);
    }

    #[compio::test]
    async fn given_torn_index_tail_when_recovering_should_floor_index_to_whole_entries() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        let log = encoded_batch(0, 2);
        let mut index = index_entry(0, 0);
        index.extend_from_slice(&GARBAGE[..10]);
        let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

        let recovered = recover(&config).await.expect("recover torn-index segment");

        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].segment.end_offset, 1);
        assert_eq!(
            len_of(&index_path),
            SPARSE_INDEX_ENTRY_SIZE as u64,
            "partial index entry must be gone from disk"
        );
        assert_eq!(len_of(&messages_path), log.len() as u64);
    }

    #[compio::test]
    async fn given_no_recoverable_bytes_when_recovering_should_fence_files_and_seed_empty() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        let partition_path = prepare_partition_dir(&config);
        let (messages_path, index_path) = write_segment(&config, 0, &GARBAGE, &GARBAGE[..10]);

        let recovered = recover(&config).await.expect("recover segment as empty");

        assert_eq!(recovered.len(), 1);
        let segment = &recovered[0].segment;
        assert_eq!(segment.size, IggyByteSize::default());
        assert_eq!(segment.end_offset, 0);
        assert_eq!(len_of(&messages_path), 0, "the served log must be empty");
        assert_eq!(len_of(&index_path), 0, "the served index must be empty");
        let fenced_dir = format!("{partition_path}.fenced.0");
        let fenced = |original: &str| {
            Path::new(&fenced_dir).join(Path::new(original).file_name().expect("fixture file name"))
        };
        assert_eq!(
            fs::read(fenced(&messages_path)).expect("read fenced log"),
            GARBAGE,
            "the unreadable log bytes must survive in the fence directory"
        );
        assert_eq!(
            fs::read(fenced(&index_path)).expect("read fenced index"),
            &GARBAGE[..10],
            "the unreadable index bytes must survive in the fence directory"
        );
    }

    #[compio::test]
    async fn given_recovered_partition_when_recovering_again_should_change_nothing() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        let mut log = encoded_batch(0, 3);
        log.extend_from_slice(&GARBAGE);
        let (messages_path, index_path) = write_segment(&config, 0, &log, &index_entry(0, 0));

        let first = recover(&config).await.expect("first recovery");
        let sizes_after_first = (len_of(&messages_path), len_of(&index_path));
        let bounds_after_first = (first[0].segment.end_offset, first[0].segment.size);
        drop(first);

        let second = recover(&config).await.expect("second recovery");

        assert_eq!(
            (len_of(&messages_path), len_of(&index_path)),
            sizes_after_first,
            "a second recovery must not move the files"
        );
        assert_eq!(
            (second[0].segment.end_offset, second[0].segment.size),
            bounds_after_first
        );
    }

    #[compio::test]
    async fn given_unopenable_index_when_recovering_should_fail_stop_without_truncating() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        let mut log = encoded_batch(0, 1);
        log.extend_from_slice(&GARBAGE);
        let messages_path =
            config
                .system
                .get_messages_file_path(STREAM_ID, TOPIC_ID, PARTITION_ID, 0);
        let index_path = config
            .system
            .get_index_path(STREAM_ID, TOPIC_ID, PARTITION_ID, 0);
        fs::write(&messages_path, &log).expect("write log fixture");
        // Self-referential symlink: every open or stat that follows it fails
        // with ELOOP, root or not (unlike permission bits, which root
        // bypasses).
        symlink(&index_path, &index_path).expect("create self-referential index symlink");

        // Recovery stats the index before opening it (a missing one routes to
        // the index-less walk), so an ELOOP surfaces from the stat.
        let error = recover(&config)
            .await
            .err()
            .expect("an unstattable index must refuse recovery");

        assert!(
            matches!(
                &error,
                ServerError::Iggy(inner) if matches!(**inner, IggyError::CannotReadFileMetadata)
            ),
            "expected CannotReadFileMetadata, got {error:?}"
        );
        assert_eq!(
            bytes_of(&messages_path),
            log,
            "fail-stop must leave the log untouched"
        );
    }

    #[compio::test]
    async fn given_unopenable_log_when_recovering_index_less_should_fail_stop_without_truncating() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        let messages_path =
            config
                .system
                .get_messages_file_path(STREAM_ID, TOPIC_ID, PARTITION_ID, 0);
        let index_path = config
            .system
            .get_index_path(STREAM_ID, TOPIC_ID, PARTITION_ID, 0);
        fs::write(&index_path, &GARBAGE[..10]).expect("write torn index fixture");
        // See the index variant above; the log stem is still collected by the
        // directory sweep, so recovery reaches the stat and must fail stop
        // there instead of recovering the segment as empty.
        symlink(&messages_path, &messages_path).expect("create self-referential log symlink");

        let error = recover(&config)
            .await
            .err()
            .expect("an unstattable log must refuse recovery");

        assert!(
            matches!(
                &error,
                ServerError::Iggy(inner) if matches!(**inner, IggyError::CannotReadFileMetadata)
            ),
            "expected CannotReadFileMetadata, got {error:?}"
        );
        assert_eq!(
            len_of(&index_path),
            10,
            "fail-stop must leave the torn index untouched"
        );
        assert!(
            fs::symlink_metadata(&messages_path)
                .expect("lstat log symlink")
                .file_type()
                .is_symlink(),
            "fail-stop must leave the log symlink in place"
        );
    }

    #[compio::test]
    async fn given_clean_segment_when_recovering_should_leave_files_untouched() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        let log = encoded_batch(0, 4);
        let (messages_path, index_path) = write_segment(&config, 0, &log, &index_entry(0, 0));

        let recovered = recover(&config).await.expect("recover clean segment");

        assert_eq!(recovered.len(), 1);
        let segment = &recovered[0].segment;
        assert_eq!(segment.end_offset, 3);
        assert!(!segment.sealed, "the tail segment must accept writes");
        assert_eq!(len_of(&messages_path), log.len() as u64);
        assert_eq!(len_of(&index_path), SPARSE_INDEX_ENTRY_SIZE as u64);
    }

    #[compio::test]
    async fn given_torn_mid_chain_segment_when_recovering_should_truncate_it_too() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        // Sealed segment holding offsets 0..=2 plus a garbage tail, then the
        // tail segment holding offsets 3..=4.
        let mut sealed_log = encoded_batch(0, 3);
        let sealed_valid_len = sealed_log.len() as u64;
        sealed_log.extend_from_slice(&GARBAGE);
        let (sealed_messages_path, _sealed_index_path) =
            write_segment(&config, 0, &sealed_log, &index_entry(0, 0));
        let tail_log = encoded_batch(3, 2);
        let (tail_messages_path, _tail_index_path) =
            write_segment(&config, 3, &tail_log, &index_entry(3, 0));

        let recovered = recover(&config).await.expect("recover two-segment chain");

        assert_eq!(recovered.len(), 2);
        assert!(recovered[0].segment.sealed);
        assert_eq!(recovered[0].segment.end_offset, 2);
        assert!(!recovered[1].segment.sealed);
        assert_eq!(recovered[1].segment.end_offset, 4);
        assert_eq!(
            len_of(&sealed_messages_path),
            sealed_valid_len,
            "a mid-chain torn tail must be truncated too"
        );
        assert_eq!(len_of(&tail_messages_path), tail_log.len() as u64);
    }

    #[compio::test]
    async fn given_valid_batch_after_damage_when_recovering_should_refuse_and_preserve_files() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        let mut log = encoded_batch(0, 3);
        log.extend_from_slice(&GARBAGE);
        log.extend_from_slice(&encoded_batch(3, 1));
        let index = index_entry(0, 0);
        let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

        let error = recover(&config)
            .await
            .err()
            .expect("a surviving batch past damage must refuse recovery");

        assert!(
            matches!(
                &error,
                ServerError::PartitionRecoveryRefused {
                    reason: PartitionRecoveryRefusal::InteriorDamage { .. },
                    ..
                }
            ),
            "expected an interior-damage refusal, got {error:?}"
        );
        assert_eq!(
            bytes_of(&messages_path),
            log,
            "a refusal must leave the log byte-identical"
        );
        assert_eq!(
            bytes_of(&index_path),
            index,
            "a refusal must leave the index byte-identical"
        );
    }

    #[compio::test]
    async fn given_garbage_head_with_valid_batch_later_when_recovering_should_refuse() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        let mut log = GARBAGE.to_vec();
        log.extend_from_slice(&encoded_batch(5, 1));
        let (messages_path, index_path) = write_segment(&config, 0, &log, &GARBAGE[..10]);

        let error = recover(&config)
            .await
            .err()
            .expect("a lost head with valid batches later must refuse recovery");

        assert!(
            matches!(
                &error,
                ServerError::PartitionRecoveryRefused {
                    reason: PartitionRecoveryRefusal::InteriorDamage { .. },
                    ..
                }
            ),
            "expected an interior-damage refusal, got {error:?}"
        );
        assert_eq!(bytes_of(&messages_path), log);
        assert_eq!(bytes_of(&index_path), &GARBAGE[..10]);
    }

    #[compio::test]
    async fn given_offset_gap_after_valid_batches_when_recovering_index_less_should_refuse() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        let mut log = encoded_batch(0, 2);
        log.extend_from_slice(&encoded_batch(5, 1));
        let (messages_path, _index_path) = write_segment(&config, 0, &log, &GARBAGE[..10]);

        let error = recover(&config)
            .await
            .err()
            .expect("an offset gap inside one segment must refuse recovery");

        assert!(
            matches!(
                &error,
                ServerError::PartitionRecoveryRefused {
                    reason: PartitionRecoveryRefusal::OffsetDiscontinuity {
                        expected_offset: 2,
                        found_offset: 5,
                        ..
                    },
                    ..
                }
            ),
            "expected an offset-discontinuity refusal, got {error:?}"
        );
        assert_eq!(bytes_of(&messages_path), log);
    }

    #[compio::test]
    async fn given_multi_batch_torn_tail_when_recovering_index_less_should_truncate_at_break() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        let mut log = encoded_batch(0, 2);
        log.extend_from_slice(&encoded_batch(2, 2));
        let valid_len = log.len() as u64;
        log.extend_from_slice(&GARBAGE);
        let (messages_path, index_path) = write_segment(&config, 0, &log, &GARBAGE[..10]);

        let recovered = recover(&config).await.expect("recover multi-batch tail");

        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].segment.end_offset, 3);
        assert_eq!(
            len_of(&messages_path),
            valid_len,
            "the walk must keep every whole batch before the tear"
        );
        assert_eq!(
            bytes_of(&index_path),
            index_entry(0, 0),
            "the index must be rebuilt from the walked batches"
        );
        assert!(
            fs::metadata(format!("{index_path}{STAGING_SUFFIX}")).is_err(),
            "the staged rebuild must be renamed into place, not copied"
        );

        let sizes_after_first = (len_of(&messages_path), len_of(&index_path));
        drop(recovered);
        let second = recover(&config).await.expect("second recovery");
        assert_eq!(second[0].segment.end_offset, 3);
        assert_eq!(
            (len_of(&messages_path), len_of(&index_path)),
            sizes_after_first,
            "recovering over a rebuilt index must be a no-op"
        );
    }

    #[compio::test]
    async fn given_holed_chain_when_recovering_should_leave_every_segment_untouched() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        // First segment carries a torn tail that WOULD truncate; the hole to
        // the next segment must refuse the chain before that happens.
        let mut first_log = encoded_batch(0, 3);
        first_log.extend_from_slice(&GARBAGE);
        let first_index = index_entry(0, 0);
        let (first_messages_path, first_index_path) =
            write_segment(&config, 0, &first_log, &first_index);
        let next_log = encoded_batch(10, 1);
        let next_index = index_entry(10, 0);
        let (next_messages_path, next_index_path) =
            write_segment(&config, 10, &next_log, &next_index);

        let error = recover(&config)
            .await
            .err()
            .expect("a holed chain must refuse recovery");

        assert!(
            matches!(
                &error,
                ServerError::PartitionRecoveryRefused {
                    reason: PartitionRecoveryRefusal::Hole { .. },
                    ..
                }
            ),
            "expected a hole refusal, got {error:?}"
        );
        assert_eq!(
            bytes_of(&first_messages_path),
            first_log,
            "a refused chain must leave even truncation candidates byte-identical"
        );
        assert_eq!(bytes_of(&first_index_path), first_index);
        assert_eq!(bytes_of(&next_messages_path), next_log);
        assert_eq!(bytes_of(&next_index_path), next_index);
    }

    #[compio::test]
    async fn given_non_monotone_index_entries_when_recovering_should_refuse() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        let batch0 = encoded_batch(0, 1);
        let batch1 = encoded_batch(1, 1);
        let batch2 = encoded_batch(2, 1);
        let mut log = batch0.clone();
        log.extend_from_slice(&batch1);
        log.extend_from_slice(&batch2);
        let last_position = (batch0.len() + batch1.len()) as u64;
        // Interior garbage entry: ascending against its predecessor, so only
        // the offset regression to the (valid) last entry exposes it.
        let mut index = index_entry(0, 0);
        index.extend_from_slice(&index_entry(50, 100));
        index.extend_from_slice(&index_entry(2, last_position));
        let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

        let error = recover(&config)
            .await
            .err()
            .expect("a non-monotone index must refuse recovery");

        assert!(
            matches!(
                &error,
                ServerError::PartitionRecoveryRefused {
                    reason: PartitionRecoveryRefusal::IndexEntriesNotMonotone {
                        entry_index: 2,
                        ..
                    },
                    ..
                }
            ),
            "expected a non-monotone index refusal, got {error:?}"
        );
        assert_eq!(bytes_of(&messages_path), log);
        assert_eq!(bytes_of(&index_path), index);
    }

    #[compio::test]
    async fn given_index_entry_below_segment_start_when_recovering_should_refuse() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        let log = encoded_batch(5, 1);
        let index = index_entry(3, 0);
        let (messages_path, index_path) = write_segment(&config, 5, &log, &index);

        let error = recover(&config)
            .await
            .err()
            .expect("an index claiming offsets below the segment start must refuse recovery");

        assert!(
            matches!(
                &error,
                ServerError::PartitionRecoveryRefused {
                    reason: PartitionRecoveryRefusal::IndexEntryBeforeSegmentStart {
                        first_entry_offset: 3,
                        ..
                    },
                    ..
                }
            ),
            "expected a below-start index refusal, got {error:?}"
        );
        assert_eq!(bytes_of(&messages_path), log);
        assert_eq!(bytes_of(&index_path), index);
    }

    #[compio::test]
    async fn given_index_less_wide_batches_when_recovering_should_rebuild_strided_index() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        // First batch alone crosses the rebuild stride, so the second batch
        // must get its own entry at the first batch's total size.
        let wide = encoded_batch_with_payload(0, 1, &Bytes::from(vec![0x42u8; 70 * 1024]));
        let narrow = encoded_batch(1, 1);
        let mut log = wide.clone();
        log.extend_from_slice(&narrow);
        let (_messages_path, index_path) = write_segment(&config, 0, &log, &GARBAGE[..10]);

        let recovered = recover(&config).await.expect("recover wide-batch segment");

        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].segment.end_offset, 1);
        let rebuilt = bytes_of(&index_path);
        assert_eq!(rebuilt.len(), 2 * SPARSE_INDEX_ENTRY_SIZE);
        let entry = |index: usize| {
            let at = index * SPARSE_INDEX_ENTRY_SIZE;
            (
                read_u64_le(&rebuilt, at),
                read_u64_le(&rebuilt, at + 8),
                read_u64_le(&rebuilt, at + 16),
            )
        };
        assert_eq!(entry(0), (0, FIXTURE_TIMESTAMP, 0));
        assert_eq!(entry(1), (1, FIXTURE_TIMESTAMP, wide.len() as u64));
    }

    #[compio::test]
    async fn given_zero_padded_records_when_probing_should_scan_whole_residue_and_recover_empty() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        let partition_path = prepare_partition_dir(&config);
        // Torn index forces the index-less walk, and the garbage head keeps
        // it from decoding anything, so the whole file is probe residue.
        // Each record's header decodes and claims an 8 KiB batch that fits,
        // so aligned candidates pay a (fast-failing) verify. Whether the
        // verify budget survives all 128 claims or gives up partway, the
        // outcome is the same by design: with no walked batch, exhaustion
        // converges with survivor-free on recover-as-empty, and the empty
        // recovery fences the pair whole.
        let mut log = GARBAGE.to_vec();
        for record in 0..128u32 {
            log.extend_from_slice(&zero_padded_record(
                8 * 1024 + u64::from(record),
                record + 1,
            ));
        }
        let (messages_path, _index_path) = write_segment(&config, 0, &log, &GARBAGE[..10]);

        let recovered = recover(&config)
            .await
            .expect("a survivor-free residue must recover as empty, not refuse");

        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].segment.size, IggyByteSize::default());
        assert_eq!(len_of(&messages_path), 0, "the served log must be empty");
        let fenced_log = Path::new(&format!("{partition_path}.fenced.0")).join(
            Path::new(&messages_path)
                .file_name()
                .expect("log file name"),
        );
        assert_eq!(
            fs::read(fenced_log).expect("read fenced log"),
            log,
            "the unclassifiable bytes must survive in the fence directory"
        );
    }

    #[compio::test]
    async fn given_wide_zeros_residue_when_recovering_should_truncate_at_break() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        // The canonical torn flush chunk: a crash under `enforce_fsync =
        // false` leaves the file extended far past its written-back pages,
        // reading as zeros -- residue bounded by the CHUNK (up to a whole
        // segment), not by one record. No survivor decodes anywhere in it,
        // so recovery must truncate to the walked prefix, at any residue
        // width and regardless of any configured message size.
        let mut log = encoded_batch(0, 3);
        let valid_len = log.len() as u64;
        log.resize(log.len() + 512 * 1024, 0);
        let (messages_path, index_path) = write_segment(&config, 0, &log, &index_entry(0, 0));

        let recovered = recover(&config)
            .await
            .expect("a zero-filled torn flush chunk must truncate, not refuse");

        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].segment.end_offset, 2);
        assert_eq!(
            len_of(&messages_path),
            valid_len,
            "the zero-filled residue must be gone from disk"
        );
        assert_eq!(len_of(&index_path), SPARSE_INDEX_ENTRY_SIZE as u64);
    }

    #[compio::test]
    async fn given_overlapping_verify_claims_when_probing_should_refuse_on_verify_budget() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        // Residue packed with back-to-back plausible headers, each claiming
        // a slice 32x wider than its own 256-byte pitch: claimed slices
        // overlap, so unbounded verification would hash close to residue x
        // claim bytes. The verify budget must give up and refuse -- with a
        // walked prefix this is a refusal, never a truncation -- and its
        // limit must be the residue-derived multiple, which is the guard
        // that keeps the bound from being silently deleted again.
        let mut log = encoded_batch(0, 2);
        for sequence in 0..64u32 {
            log.extend_from_slice(&bait_record(100, 8 * 1024, sequence + 1));
        }
        let residue = u64::from(64u32) * COMMAND_HEADER_SIZE as u64;
        let (messages_path, _index_path) = write_segment(&config, 0, &log, &GARBAGE[..10]);

        let error = recover(&config)
            .await
            .err()
            .expect("exhausting the verify budget over a walked prefix must refuse");

        assert!(
            matches!(
                &error,
                ServerError::PartitionRecoveryRefused {
                    reason: PartitionRecoveryRefusal::UnverifiedResidue {
                        residue_bytes,
                        verified_bytes,
                        verify_budget_bytes,
                        ..
                    },
                    ..
                } if *residue_bytes == residue
                    && *verify_budget_bytes
                        == residue * PROBE_VERIFY_BUDGET_BYTES_PER_RESIDUE_BYTE
                    && verified_bytes > verify_budget_bytes
            ),
            "expected a verify-budget refusal, got {error:?}"
        );
        assert_eq!(
            bytes_of(&messages_path),
            log,
            "a refusal must leave the log byte-identical"
        );
    }

    #[compio::test]
    async fn given_overlapping_verify_claims_with_no_walked_batch_when_probing_should_recover_empty()
     {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        let partition_path = prepare_partition_dir(&config);
        // Same bait with a garbage head, so the walk proves not one batch:
        // exhaustion and survivor-free converge on recover-as-empty here,
        // and the pair is fenced whole -- bytes preserved without minting a
        // tombstone for a file that provably serves nothing.
        let mut log = GARBAGE.to_vec();
        for sequence in 0..64u32 {
            log.extend_from_slice(&bait_record(100, 8 * 1024, sequence + 1));
        }
        let (messages_path, _index_path) = write_segment(&config, 0, &log, &GARBAGE[..10]);

        let recovered = recover(&config)
            .await
            .expect("verify-budget exhaustion with no walked batch must recover as empty");

        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].segment.size, IggyByteSize::default());
        assert_eq!(len_of(&messages_path), 0, "the served log must be empty");
        let fenced_log = Path::new(&format!("{partition_path}.fenced.0")).join(
            Path::new(&messages_path)
                .file_name()
                .expect("log file name"),
        );
        assert_eq!(
            fs::read(fenced_log).expect("read fenced log"),
            log,
            "the unclassifiable bytes must survive in the fence directory"
        );
    }

    #[test]
    fn probe_budget_charges_verify_bytes_before_the_read() {
        let mut budget = ProbeBudget::default();
        budget.grow_for_residue(1024);
        // 4 KiB of verify allowance: three 1 KiB slices fit, the fifth does
        // not, and the failing charge is already counted (the caller must
        // not read the slice that broke the budget).
        for _ in 0..4 {
            assert!(budget.charge_verify(1024), "in-budget verifies must pass");
        }
        assert!(
            !budget.charge_verify(1024),
            "the fifth 1 KiB slice against a 4 KiB verify budget must exhaust"
        );
        // A later probe widens the shared limit; the failed charge above
        // stays counted, so the widened budget must cover it plus the next
        // slice.
        budget.grow_for_residue(512);
        assert!(budget.charge_verify(1024));
    }

    #[test]
    fn probe_budget_charges_per_candidate_across_probes() {
        let mut budget = ProbeBudget::default();
        budget.grow_for_residue(4);
        for _ in 0..8 {
            assert!(budget.charge_candidate(), "honest scans fit the budget");
        }
        assert!(
            !budget.charge_candidate(),
            "the ninth candidate against a 4-byte residue must exhaust"
        );
        // A later probe in the same load widens the shared limit; spent
        // units carry over rather than resetting per probe.
        budget.grow_for_residue(2);
        assert!(budget.charge_candidate());
    }

    #[compio::test]
    async fn given_exhausted_probe_budget_when_classifying_residue_should_refuse() {
        let tmp = tempdir().expect("tempdir");
        let mut log = encoded_batch(0, 2);
        let valid_len = log.len() as u64;
        log.extend_from_slice(&GARBAGE);
        let messages_path = tmp.path().join("00000000000000000000.log");
        fs::write(&messages_path, &log).expect("write log fixture");
        let file = fs::File::open(&messages_path).expect("open log fixture");
        let partition_path = tmp.path().to_string_lossy().into_owned();
        let identity = PartitionIdentity {
            partition_path: &partition_path,
            stream_id: STREAM_ID,
            topic_id: TOPIC_ID,
            partition_id: PARTITION_ID,
        };
        // No once-through scan can exhaust a residue-sized budget, so the
        // exhaustion path is a tripwire for probe defects that re-examine
        // candidates. Simulate one by pre-spending the shared budget past
        // anything this residue can grow it by.
        let mut scratch = ScanScratch::default();
        scratch.probe_budget.spent_units = u64::MAX / 2;
        let mut scanner = FileScanner::new(&file, log.len() as u64, &mut scratch);

        let error = refuse_if_survivor_past_damage(
            identity,
            &mut scanner,
            &messages_path.to_string_lossy(),
            valid_len,
            log.len() as u64,
            Some(1),
            0,
        )
        .await
        .expect_err("an exhausted budget must refuse instead of truncating");

        assert!(
            matches!(
                &error,
                ServerError::PartitionRecoveryRefused {
                    reason: PartitionRecoveryRefusal::UnverifiedResidue {
                        damage_position,
                        residue_bytes,
                        candidates_examined,
                        budget_units,
                        ..
                    },
                    ..
                } if *damage_position == valid_len
                    && *residue_bytes == GARBAGE.len() as u64
                    && candidates_examined > budget_units
            ),
            "expected a budget-exhausted refusal, got {error:?}"
        );
        assert_eq!(
            bytes_of(&messages_path.to_string_lossy()),
            log,
            "a refusal must leave the log byte-identical"
        );
    }

    #[compio::test]
    async fn given_indexed_offset_regression_when_recovering_should_refuse_without_panicking() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        // A decodable batch claiming offsets below the segment start: absorbed,
        // it would regress the recovered end offset below the start and
        // underflow the message-count arithmetic.
        let mut log = encoded_batch(100, 1);
        log.extend_from_slice(&encoded_batch(5, 1));
        let index = index_entry(100, 0);
        let (messages_path, index_path) = write_segment(&config, 100, &log, &index);

        let error = recover(&config)
            .await
            .err()
            .expect("an indexed walk hitting a regressed offset must refuse recovery");

        assert!(
            matches!(
                &error,
                ServerError::PartitionRecoveryRefused {
                    reason: PartitionRecoveryRefusal::OffsetDiscontinuity {
                        expected_offset: 101,
                        found_offset: 5,
                        ..
                    },
                    ..
                }
            ),
            "expected an offset-discontinuity refusal, got {error:?}"
        );
        assert_eq!(bytes_of(&messages_path), log);
        assert_eq!(bytes_of(&index_path), index);
    }

    #[compio::test]
    async fn given_indexed_forward_offset_gap_when_recovering_should_refuse() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        // A verified batch opening a forward gap is durable data whose
        // offsets this chain never promised: absorbing it would inflate the
        // recovered message count and mint a segment state transfer can
        // never install, so recovery refuses and keeps every byte.
        let mut log = encoded_batch(0, 2);
        log.extend_from_slice(&encoded_batch(5, 1));
        let (messages_path, index_path) = write_segment(&config, 0, &log, &index_entry(0, 0));

        let error = recover(&config)
            .await
            .err()
            .expect("a verified forward offset gap in an indexed chain must refuse recovery");

        assert!(
            matches!(
                &error,
                ServerError::PartitionRecoveryRefused {
                    reason: PartitionRecoveryRefusal::OffsetDiscontinuity {
                        expected_offset: 2,
                        found_offset: 5,
                        ..
                    },
                    ..
                }
            ),
            "expected an offset-discontinuity refusal, got {error:?}"
        );
        assert_eq!(bytes_of(&messages_path), log);
        assert_eq!(bytes_of(&index_path), index_entry(0, 0));
    }

    // `base_offset` sits at header bytes 8..16, so flips inside that range
    // leave the per-message checksums clean and trip the BATCH checksum on
    // the offset field specifically -- pinning that `base_offset` is hashed.
    const HEADER_BASE_OFFSET_OFFSET: usize = 8;

    #[compio::test]
    async fn given_bit_flipped_base_offset_upward_when_recovering_should_truncate_at_break() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        // An upward flip wears the forward-gap shape (header decodes,
        // base_offset ahead of the chain) but fails the batch checksum:
        // damage, not data, so the walk breaks and the tail truncates.
        let mut log = encoded_batch(0, 2);
        let valid_len = log.len() as u64;
        let mut corrupt = encoded_batch(2, 1);
        corrupt[HEADER_BASE_OFFSET_OFFSET] ^= 0x04;
        log.extend_from_slice(&corrupt);
        let (messages_path, index_path) = write_segment(&config, 0, &log, &index_entry(0, 0));

        let recovered = recover(&config)
            .await
            .expect("an unverified gap batch with nothing verifying past it must truncate");

        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].segment.end_offset, 1);
        assert_eq!(
            len_of(&messages_path),
            valid_len,
            "the unverified gap batch must be gone from disk"
        );
        assert_eq!(len_of(&index_path), SPARSE_INDEX_ENTRY_SIZE as u64);
    }

    #[compio::test]
    async fn given_bit_flipped_base_offset_downward_when_recovering_should_truncate_at_break() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        // The same flip downward wears the regression shape. It must earn
        // the same verdict as the upward one -- verify fails, walk breaks,
        // tail truncates -- rather than a permanent refusal: one bit must
        // not get opposite verdicts by direction.
        let mut log = encoded_batch(0, 2);
        let valid_len = log.len() as u64;
        let mut corrupt = encoded_batch(2, 1);
        corrupt[HEADER_BASE_OFFSET_OFFSET] ^= 0x02;
        log.extend_from_slice(&corrupt);
        let (messages_path, index_path) = write_segment(&config, 0, &log, &index_entry(0, 0));

        let recovered = recover(&config)
            .await
            .expect("an unverified regressing batch with nothing verifying past it must truncate");

        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].segment.end_offset, 1);
        assert_eq!(
            len_of(&messages_path),
            valid_len,
            "the unverified regressing batch must be gone from disk"
        );
        assert_eq!(len_of(&index_path), SPARSE_INDEX_ENTRY_SIZE as u64);
    }

    #[compio::test]
    async fn given_bit_flipped_base_offset_before_valid_batch_when_recovering_should_refuse() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        // A verifying batch past the failing gap batch is durable data:
        // truncating there would erase it, so recovery must refuse and keep
        // every byte.
        let mut log = encoded_batch(0, 2);
        let mut corrupt = encoded_batch(2, 1);
        corrupt[HEADER_BASE_OFFSET_OFFSET] ^= 0x04;
        log.extend_from_slice(&corrupt);
        log.extend_from_slice(&encoded_batch(6, 1));
        let index = index_entry(0, 0);
        let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

        let error = recover(&config)
            .await
            .err()
            .expect("a verifying batch past the failing gap batch must refuse recovery");

        assert!(
            matches!(
                &error,
                ServerError::PartitionRecoveryRefused {
                    reason: PartitionRecoveryRefusal::InteriorDamage { .. },
                    ..
                }
            ),
            "expected an interior-damage refusal, got {error:?}"
        );
        assert_eq!(bytes_of(&messages_path), log);
        assert_eq!(bytes_of(&index_path), index);
    }

    #[compio::test]
    async fn given_failing_gap_batch_at_index_anchor_when_recovering_should_probe_for_survivors() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        // The failing gap batch is the FIRST thing past the last index
        // entry, so the walk breaks with nothing walked. The probe must
        // still run: a verifying batch past the damage is a survivor and
        // refuses as interior damage, not as a divergence claiming the log
        // holds nothing.
        let mut corrupt = encoded_batch(5, 1);
        corrupt[HEADER_BASE_OFFSET_OFFSET] ^= 0x04;
        let mut log = corrupt;
        log.extend_from_slice(&encoded_batch(6, 1));
        let index = index_entry(0, 0);
        let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

        let error = recover(&config)
            .await
            .err()
            .expect("a survivor past the anchor's failing batch must refuse recovery");

        assert!(
            matches!(
                &error,
                ServerError::PartitionRecoveryRefused {
                    reason: PartitionRecoveryRefusal::InteriorDamage { .. },
                    ..
                }
            ),
            "expected an interior-damage refusal, got {error:?}"
        );
        assert_eq!(bytes_of(&messages_path), log);
        assert_eq!(bytes_of(&index_path), index);
    }

    #[compio::test]
    async fn given_failing_gap_batch_at_index_anchor_with_no_survivor_when_recovering_should_refuse_divergence()
     {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        // Same anchor shape with nothing verifying past it: the index claims
        // a batch where none decodes and verifies, which is the divergence
        // verdict -- non-destructive, bytes preserved.
        let mut log = encoded_batch(5, 1);
        log[HEADER_BASE_OFFSET_OFFSET] ^= 0x04;
        let index = index_entry(0, 0);
        let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

        let error = recover(&config)
            .await
            .err()
            .expect("an anchor batch that fails its checksum must refuse recovery");

        assert!(
            matches!(
                &error,
                ServerError::PartitionRecoveryRefused {
                    reason: PartitionRecoveryRefusal::IndexLogDivergence { .. },
                    ..
                }
            ),
            "expected an index-log divergence refusal, got {error:?}"
        );
        assert_eq!(bytes_of(&messages_path), log);
        assert_eq!(bytes_of(&index_path), index);
    }

    #[compio::test]
    async fn given_foreign_partition_batch_when_recovering_indexed_should_refuse() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        // A verified batch stamped for partition 7 that continues the chain
        // EXACTLY: only the partition_id comparison can catch it, and
        // adopting it would serve foreign data under this partition's
        // offsets.
        let mut log = encoded_batch(0, 2);
        log.extend_from_slice(&encoded_foreign_batch(2, 1, 7));
        let index = index_entry(0, 0);
        let (messages_path, index_path) = write_segment(&config, 0, &log, &index);

        let error = recover(&config)
            .await
            .err()
            .expect("a verified foreign batch in an indexed chain must refuse recovery");

        assert!(
            matches!(
                &error,
                ServerError::PartitionRecoveryRefused {
                    reason: PartitionRecoveryRefusal::ForeignBatch {
                        batch_partition_id: 7,
                        ..
                    },
                    ..
                }
            ),
            "expected a foreign-batch refusal, got {error:?}"
        );
        assert_eq!(bytes_of(&messages_path), log);
        assert_eq!(bytes_of(&index_path), index);
    }

    #[compio::test]
    async fn given_foreign_partition_batch_when_recovering_index_less_should_refuse() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        let mut log = encoded_batch(0, 2);
        log.extend_from_slice(&encoded_foreign_batch(2, 1, 7));
        let (messages_path, _index_path) = write_segment(&config, 0, &log, &GARBAGE[..10]);

        let error = recover(&config)
            .await
            .err()
            .expect("a verified foreign batch in an index-less chain must refuse recovery");

        assert!(
            matches!(
                &error,
                ServerError::PartitionRecoveryRefused {
                    reason: PartitionRecoveryRefusal::ForeignBatch {
                        batch_partition_id: 7,
                        ..
                    },
                    ..
                }
            ),
            "expected a foreign-batch refusal, got {error:?}"
        );
        assert_eq!(bytes_of(&messages_path), log);
    }

    #[compio::test]
    async fn given_bit_flipped_batch_length_when_recovering_should_truncate_at_break() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        // A corrupted-upward length claim (the classic all-ones flip) is not
        // a plausible batch: the walk must break at the header itself, never
        // sizing an allocation or a read by what the header claims.
        let mut log = encoded_batch(0, 2);
        let valid_len = log.len() as u64;
        log.extend_from_slice(&zero_padded_record(0xFFFF_FFFF, 1));
        let (messages_path, _index_path) = write_segment(&config, 0, &log, &GARBAGE[..10]);

        let recovered = recover(&config)
            .await
            .expect("an implausible length claim in the tail must truncate, not refuse");

        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].segment.end_offset, 1);
        assert_eq!(
            len_of(&messages_path),
            valid_len,
            "the walk must break at the implausible header and truncate there"
        );
    }

    #[compio::test]
    async fn given_bit_flipped_batch_length_before_valid_batch_when_recovering_should_refuse() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        let mut log = encoded_batch(0, 2);
        let valid_len = log.len() as u64;
        log.extend_from_slice(&zero_padded_record(0xFFFF_FFFF, 1));
        log.extend_from_slice(&encoded_batch(2, 1));
        let (messages_path, _index_path) = write_segment(&config, 0, &log, &GARBAGE[..10]);

        let error = recover(&config)
            .await
            .err()
            .expect("a surviving batch past an implausible header must refuse recovery");

        assert!(
            matches!(
                &error,
                ServerError::PartitionRecoveryRefused {
                    reason: PartitionRecoveryRefusal::InteriorDamage {
                        damage_position,
                        survivor_position,
                        ..
                    },
                    ..
                } if *damage_position == valid_len
                    && *survivor_position == valid_len + COMMAND_HEADER_SIZE as u64
            ),
            "expected an interior-damage refusal, got {error:?}"
        );
        assert_eq!(bytes_of(&messages_path), log);
    }

    #[compio::test]
    async fn given_refused_chain_when_index_rebuilt_should_stage_without_touching_final() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        // The index-less first segment wants a rebuild; the hole to the next
        // segment refuses the chain in pass B, before any install.
        let first_log = encoded_batch(0, 2);
        let first_index = GARBAGE[..10].to_vec();
        let (first_messages_path, first_index_path) =
            write_segment(&config, 0, &first_log, &first_index);
        write_segment(&config, 10, &encoded_batch(10, 1), &index_entry(10, 0));

        let error = recover(&config)
            .await
            .err()
            .expect("a holed chain must refuse recovery");

        assert!(
            matches!(
                &error,
                ServerError::PartitionRecoveryRefused {
                    reason: PartitionRecoveryRefusal::Hole { .. },
                    ..
                }
            ),
            "expected a hole refusal, got {error:?}"
        );
        assert_eq!(
            bytes_of(&format!("{first_index_path}{STAGING_SUFFIX}")),
            index_entry(0, 0),
            "pass A must stage the rebuilt index beside the final one"
        );
        assert_eq!(
            bytes_of(&first_index_path),
            first_index,
            "a refusal must leave the final index byte-identical"
        );
        assert_eq!(bytes_of(&first_messages_path), first_log);
    }

    #[compio::test]
    async fn given_orphaned_index_staging_when_recovering_should_sweep_it() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        let log = encoded_batch(0, 2);
        let (messages_path, index_path) = write_segment(&config, 0, &log, &index_entry(0, 0));
        let staging_path = format!("{index_path}{STAGING_SUFFIX}");
        fs::write(&staging_path, GARBAGE).expect("write orphaned staging fixture");

        let recovered = recover(&config).await.expect("recover clean segment");

        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].segment.end_offset, 1);
        assert!(
            fs::metadata(&staging_path).is_err(),
            "an orphaned staging file must be swept at boot"
        );
        assert_eq!(len_of(&messages_path), log.len() as u64);
        assert_eq!(len_of(&index_path), SPARSE_INDEX_ENTRY_SIZE as u64);
    }
    #[compio::test]
    async fn given_absent_index_when_recovering_should_walk_index_less_and_rebuild() {
        let tmp = tempdir().expect("tempdir");
        let config = test_config(&tmp);
        prepare_partition_dir(&config);
        let log = encoded_batch(0, 4);
        let messages_path =
            config
                .system
                .get_messages_file_path(STREAM_ID, TOPIC_ID, PARTITION_ID, 0);
        let index_path = config
            .system
            .get_index_path(STREAM_ID, TOPIC_ID, PARTITION_ID, 0);
        fs::write(&messages_path, &log).expect("write log fixture");

        let recovered = recover(&config)
            .await
            .expect("a log with no index beside it must recover, not abort the boot");

        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].segment.end_offset, 3);
        assert_eq!(len_of(&messages_path), log.len() as u64);
        assert_eq!(
            len_of(&index_path),
            SPARSE_INDEX_ENTRY_SIZE as u64,
            "the walk must install a rebuilt index over the missing one"
        );
    }
}
