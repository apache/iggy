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

use crate::server_error::{PartitionChainRefusal, ServerError};
use configs::server::ServerConfig;
use iggy_common::{IggyByteSize, IggyError, PartitionStats};
use partitions::state_transfer::STAGING_SUFFIX;
use partitions::{IggyIndexReader, Segment};
use server_common::SegmentStorage;
use server_common::send_messages::{COMMAND_HEADER_SIZE, SendMessagesHeader, decode_batch_slice};
use std::fs;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use tracing::{error, warn};

const LOG_EXTENSION: &str = "log";
const INDEX_EXTENSION: &str = "index";

/// A persisted segment recovered from disk: its metadata plus the storage
/// handles (readers/writers) opened over its `.log` / `.index` files.
pub struct RecoveredSegment {
    pub segment: Segment,
    pub storage: SegmentStorage,
}

/// Loads every persisted segment for a partition, sorted by start offset.
///
/// Segment offsets and timestamps are recovered from the 24-byte sparse index
/// (see module docs); segment byte size comes from the `.log` file. The last
/// segment is left unsealed so it can accept further writes.
///
/// # Errors
///
/// Returns an error if the partition directory or a segment's files cannot be
/// read, or if a segment's index references a batch beyond the end of its
/// messages file (torn write).
pub async fn load_persisted_segments(
    config: &ServerConfig,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    segment_size: IggyByteSize,
    enforce_fsync: bool,
    stats: &PartitionStats,
) -> Result<Vec<RecoveredSegment>, ServerError> {
    let partition_path = config
        .system
        .get_partition_path(stream_id, topic_id, partition_id);
    // ONE directory walk feeds both: the sweep only ever unlinks `.staging` and
    // orphan `.index` files, never a `.log`, so the log stems it already
    // collects ARE the post-sweep start-offset set. Note the error policy is
    // the collect side's (NotFound => empty, anything else => refuse boot); the
    // sweep's silent return would swallow an EACCES that must not be ignored.
    let mut start_offsets = sweep_scratch_files_and_collect_offsets(&partition_path)?;
    start_offsets.sort_unstable();

    let max_size = segment_size;

    let mut recovered = Vec::with_capacity(start_offsets.len());
    for start_offset in start_offsets {
        let messages_path =
            config
                .system
                .get_messages_file_path(stream_id, topic_id, partition_id, start_offset);
        let index_path =
            config
                .system
                .get_index_path(stream_id, topic_id, partition_id, start_offset);

        let messages_size = file_len(&messages_path);
        let index_size = file_len(&index_path);

        let bounds = recover_segment_bounds(
            &index_path,
            &messages_path,
            start_offset,
            messages_size,
            stream_id,
            topic_id,
            partition_id,
        )
        .await?;

        // `bounds == None` now means the log holds no whole BATCH either (the
        // index-less path above already tried walking the log), so there is
        // nothing to recover: zeroed sizes make the next append overwrite the
        // torn bytes, where counting them with `end_offset == start_offset`
        // would fabricate one phantom message for the bootstrap non-empty
        // filters and strand undecodable garbage inside the readable range.
        // Note this is NOT tail-only -- a torn index is reachable mid-chain on
        // the shipped `enforce_fsync = false`, which is why the walk above
        // exists rather than refusing the partition.
        let (start_timestamp, end_timestamp, end_offset, effective_messages_size) =
            if let Some((start_timestamp, end_timestamp, end_offset, walked_size)) = bounds {
                (start_timestamp, end_timestamp, end_offset, walked_size)
            } else {
                if messages_size > 0 {
                    warn!(
                        stream_id,
                        topic_id,
                        partition_id,
                        start_offset,
                        messages_size,
                        "segment log holds bytes but its index holds no whole \
                         entry (torn write); recovering the segment as empty"
                    );
                }
                (0, 0, start_offset, 0)
            };
        let effective_index_size = if bounds.is_some() { index_size } else { 0 };

        let storage = SegmentStorage::new(
            &messages_path,
            &index_path,
            effective_messages_size,
            effective_index_size,
            enforce_fsync,
            enforce_fsync,
            true,
        )
        .await
        .map_err(|source| {
            error!(
                stream_id,
                topic_id,
                partition_id,
                path = %messages_path,
                error = %source,
                "failed to open persisted segment storage during recovery"
            );
            source
        })?;

        let mut segment = Segment::new(start_offset, max_size);
        segment.sealed = true;
        segment.start_timestamp = start_timestamp;
        segment.end_timestamp = end_timestamp;
        segment.max_timestamp = end_timestamp;
        segment.end_offset = end_offset;
        segment.size = IggyByteSize::from(effective_messages_size);
        segment.current_position = effective_messages_size;

        stats.increment_segments_count(1);
        stats.increment_size_bytes(effective_messages_size);
        if effective_messages_size > 0 {
            // Offsets in a segment are contiguous, so the message count is the
            // inclusive span between the first (segment start) and last offset.
            stats.increment_messages_count(end_offset - start_offset + 1);
        }

        recovered.push(RecoveredSegment { segment, storage });
    }

    if let Some(last) = recovered.last_mut() {
        last.segment.sealed = false;
    }

    ensure_contiguous_chain(
        &recovered,
        &partition_path,
        stream_id,
        topic_id,
        partition_id,
    )?;

    Ok(recovered)
}

/// Contiguity guard: recovery takes every `.log` stem in the directory, so a
/// stray file (an unlink a failed state-transfer install could not finish,
/// an operator copy) would otherwise splice a hole or an overlap into the
/// chain and push `current_offset` past data this replica does not hold.
/// Refuse loudly instead of serving a holed log.
///
/// The refusal names the partition and its directory so the caller can fence
/// THAT group rather than abort the node's boot: the shapes it rejects are
/// exactly what a failed quarantine leaves behind, and one damaged local chain
/// must not take the whole node down.
fn ensure_contiguous_chain(
    recovered: &[RecoveredSegment],
    partition_path: &str,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
) -> Result<(), ServerError> {
    let refused = |reason| {
        Err(ServerError::PartitionChainRefused {
            dir: PathBuf::from(partition_path),
            stream_id,
            topic_id,
            partition_id,
            reason,
        })
    };
    for pair in recovered.windows(2) {
        let previous = &pair[0].segment;
        let next = &pair[1].segment;
        // A NON-tail empty segment can only be an orphan pairing: the torn-
        // tail leniency (an index-less crash tail recovered as empty) only
        // ever applies to the LAST element, and a size-0 segment followed by
        // more chain is exactly what a failed converge rebuild leaves behind.
        // Skipping it here was the guard's blind spot.
        if previous.size == IggyByteSize::default() {
            return refused(PartitionChainRefusal::EmptyNonTailSegment {
                empty_start: previous.start_offset,
                next_start: next.start_offset,
            });
        }
        if next.start_offset != previous.end_offset + 1 {
            return refused(PartitionChainRefusal::Hole {
                previous_start: previous.start_offset,
                previous_end: previous.end_offset,
                next_start: next.start_offset,
            });
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

fn file_len(path: &str) -> u64 {
    fs::metadata(path).map_or(0, |metadata| metadata.len())
}

/// Derives `(start_timestamp, end_timestamp, end_offset)` from a segment's
/// 24-byte sparse index. `None` when the index holds no whole entry (the
/// caller recovers the segment as empty). The last entry's `position` is only
/// the last flushed batch's START byte, so the batch header is read back from
/// the messages file to prove the batch also ENDS inside it -- without
/// `enforce_fsync` there is no ordering barrier between the message write and
/// the index write, and a tail torn mid-flush would otherwise pass while
/// `end_offset` claims offsets whose bytes are incomplete.
#[allow(clippy::too_many_lines)]
async fn recover_segment_bounds(
    index_path: &str,
    messages_path: &str,
    start_offset: u64,
    messages_size: u64,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
) -> Result<Option<(u64, u64, u64, u64)>, ServerError> {
    let reader = IggyIndexReader::new(index_path).await.map_err(|source| {
        error!(
            stream_id,
            topic_id,
            partition_id,
            path = %index_path,
            error = %source,
            "failed to open sparse index during recovery"
        );
        source
    })?;
    let first = reader.load_first().await.map_err(|source| {
        error!(
            stream_id,
            topic_id,
            partition_id,
            path = %index_path,
            error = %source,
            "failed to read first sparse index entry during recovery"
        );
        source
    })?;
    let last = reader.load_last().await.map_err(|source| {
        error!(
            stream_id,
            topic_id,
            partition_id,
            path = %index_path,
            error = %source,
            "failed to read last sparse index entry during recovery"
        );
        source
    })?;

    match (first, last) {
        (Some(first), Some(last)) => {
            // The sparse index holds ONE entry per flushed chunk, pointing
            // at the chunk's FIRST batch -- `last.offset` is where the last
            // chunk STARTS, not where the segment ends (a whole journal
            // flushed as one chunk indexes only its first offset). Walk the
            // batch chain from that position to the file end to recover the
            // true end offset; a header that no longer decodes marks a torn
            // tail, which truncates the readable range to the last whole
            // batch so the next append overwrites the torn bytes.
            // Opened ONCE for the walk: the helper used to open the file per
            // batch, which is an open + pread + close for every batch in the
            // segment, synchronously, at boot. A failure to open a file that
            // just stat'd walks nothing, which lands on the divergence refusal
            // below rather than recovering an indexed segment as empty.
            let messages = fs::File::open(messages_path).ok();
            let mut position = last.position;
            let mut end_offset = last.offset;
            let mut end_timestamp = last.timestamp;
            let mut walked_any = false;
            while let Some(messages) = messages.as_ref()
                && position < messages_size
            {
                let Some(header) = read_batch_header(messages, position, messages_size) else {
                    break;
                };
                let extent = position.saturating_add(header.total_size() as u64);
                if extent > messages_size {
                    break;
                }
                if header.message_count > 0 {
                    end_offset = header
                        .base_offset
                        .saturating_add(u64::from(header.message_count) - 1);
                    end_timestamp = header.base_timestamp;
                }
                walked_any = true;
                position = extent;
            }
            if !walked_any {
                return Err(ServerError::RecoveredSegmentSizeDivergence {
                    stream_id,
                    topic_id,
                    partition_id,
                    start_offset,
                    end_offset: last.offset,
                    messages_size_bytes: messages_size,
                    indexed_size_bytes: last.position,
                });
            }
            Ok(Some((first.timestamp, end_timestamp, end_offset, position)))
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
        // are all present. The walk stops at the first header that does not
        // decode or does not fit, which keeps the torn-tail truncation the
        // indexed path performs.
        _ if messages_size > 0 => {
            // Opened once, as above. Nothing walked means no whole batch,
            // which is the `Ok(None)` the tail of this arm already returns.
            let messages = fs::File::open(messages_path).ok();
            let mut position = 0u64;
            let mut start_timestamp = None;
            let mut end_offset = start_offset;
            let mut end_timestamp = 0;
            let mut expected_offset = start_offset;
            let mut scratch = Vec::new();
            while let Some(messages) = messages.as_ref()
                && position < messages_size
            {
                let Some(header) = read_batch_header(messages, position, messages_size) else {
                    break;
                };
                let extent = position.saturating_add(header.total_size() as u64);
                if extent > messages_size {
                    break;
                }
                // The FILENAME is the only trustworthy anchor once the index is
                // gone, and `read_batch_header` checks a length, not a checksum.
                // A torn header claiming an offset below `start_offset` would
                // underflow the message count the caller derives; one claiming a
                // jump above becomes this partition's counter, and the next
                // prepare stamps a `base_offset` diverged from every peer. So
                // the chain has to be contiguous from the filename onward, and
                // the batch has to verify before its header is believed.
                if header.base_offset != expected_offset
                    || !batch_verifies(messages, position, &header, &mut scratch)
                {
                    break;
                }
                if header.message_count > 0 {
                    end_offset = header
                        .base_offset
                        .saturating_add(u64::from(header.message_count) - 1);
                    end_timestamp = header.base_timestamp;
                    start_timestamp.get_or_insert(header.base_timestamp);
                    expected_offset = end_offset.saturating_add(1);
                }
                position = extent;
            }
            let Some(start_timestamp) = start_timestamp else {
                // Not one whole batch either: the bytes really are unusable, so
                // the caller's empty recovery is right after all.
                return Ok(None);
            };
            warn!(
                stream_id,
                topic_id,
                partition_id,
                start_offset,
                messages_size,
                walked_size = position,
                "sparse index holds no whole entry; recovered segment bounds by \
                 walking the log instead of discarding it (the index repopulates \
                 on the next flush, and polls take the index-less fallback until \
                 then)"
            );
            Ok(Some((start_timestamp, end_timestamp, end_offset, position)))
        }
        _ => Ok(None),
    }
}

/// The batch command header at `position` in the messages file, or `None`
/// when the header does not fit / decode (`position` past the file, header
/// truncated, or garbage bytes).
/// Whether the batch at `position` decodes and passes its own `batch_checksum`.
///
/// The index-less recovery walk trusts nothing else: without an index the only
/// anchors are the filename and the payload's self-description, and a torn
/// header is exactly what that walk exists to survive.
fn batch_verifies(
    messages: &fs::File,
    position: u64,
    header: &SendMessagesHeader,
    scratch: &mut Vec<u8>,
) -> bool {
    scratch.clear();
    scratch.resize(header.total_size(), 0);
    if messages.read_exact_at(scratch, position).is_err() {
        return false;
    }
    decode_batch_slice(scratch).is_ok()
}

fn read_batch_header(
    messages: &fs::File,
    position: u64,
    messages_size: u64,
) -> Option<SendMessagesHeader> {
    if position.checked_add(COMMAND_HEADER_SIZE as u64)? > messages_size {
        return None;
    }
    let mut header_bytes = [0u8; COMMAND_HEADER_SIZE];
    messages.read_exact_at(&mut header_bytes, position).ok()?;
    SendMessagesHeader::decode(&header_bytes).ok()
}
