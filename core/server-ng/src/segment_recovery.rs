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

//! server-ng-owned segment recovery.
//!
//! Previously the bootstrap path borrowed `server::bootstrap::load_segments`
//! from the legacy `server` crate to hydrate persisted segments. That loader
//! reads the legacy 16-byte dense per-message index through
//! `server_common::IndexReader`, but server-ng persists a 24-byte sparse index
//! (`partitions::IggyIndexWriter`: one entry per flush, absolute `offset`,
//! `timestamp`, and batch-start `position`). Reading the 24-byte file with the
//! 16-byte parser mis-strides it (the "Index data must be exactly 16 bytes"
//! recovery panic). This module is the server-ng-owned loader, reading the same
//! 24-byte format its writer emits.

use crate::server_error::{PartitionChainRefusal, ServerNgError};
use configs::server_ng::ServerNgConfig;
use iggy_common::{IggyByteSize, IggyError, PartitionStats};
use partitions::state_transfer::STAGING_SUFFIX;
use partitions::{IggyIndexReader, Segment};
use server_common::SegmentStorage;
use server_common::send_messages2::{COMMAND_HEADER_SIZE, SendMessages2Header};
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
    config: &ServerNgConfig,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    stats: &PartitionStats,
) -> Result<Vec<RecoveredSegment>, ServerNgError> {
    let partition_path = config
        .system
        .get_partition_path(stream_id, topic_id, partition_id);
    sweep_scratch_files(&partition_path);
    let mut start_offsets = collect_segment_start_offsets(&partition_path)?;
    start_offsets.sort_unstable();

    let enforce_fsync = config.system.partition.enforce_fsync;
    let max_size = config.system.segment.size;

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
) -> Result<(), ServerNgError> {
    let refused = |reason| {
        Err(ServerNgError::PartitionChainRefused {
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
fn sweep_scratch_files(partition_path: &str) {
    let Ok(entries) = fs::read_dir(partition_path) else {
        return;
    };
    let mut swept = Vec::new();
    let mut orphan_candidates = Vec::new();
    let mut log_stems = std::collections::HashSet::new();
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
}

/// Parses the zero-padded start offset out of every `.log` file name in the
/// partition directory. A missing directory means a never-persisted partition.
fn collect_segment_start_offsets(partition_path: &str) -> Result<Vec<u64>, ServerNgError> {
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

    let mut start_offsets = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some(LOG_EXTENSION) {
            continue;
        }
        if let Some(start_offset) = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| stem.parse::<u64>().ok())
        {
            start_offsets.push(start_offset);
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
) -> Result<Option<(u64, u64, u64, u64)>, ServerNgError> {
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
            let mut position = last.position;
            let mut end_offset = last.offset;
            let mut end_timestamp = last.timestamp;
            let mut walked_any = false;
            while position < messages_size {
                let Some(header) = read_batch_header(messages_path, position, messages_size) else {
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
                return Err(ServerNgError::RecoveredSegmentSizeDivergence {
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
            let mut position = 0u64;
            let mut start_timestamp = None;
            let mut end_offset = start_offset;
            let mut end_timestamp = 0;
            while position < messages_size {
                let Some(header) = read_batch_header(messages_path, position, messages_size) else {
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
                    start_timestamp.get_or_insert(header.base_timestamp);
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
fn read_batch_header(
    messages_path: &str,
    position: u64,
    messages_size: u64,
) -> Option<SendMessages2Header> {
    if position.checked_add(COMMAND_HEADER_SIZE as u64)? > messages_size {
        return None;
    }
    let file = fs::File::open(messages_path).ok()?;
    let mut header_bytes = [0u8; COMMAND_HEADER_SIZE];
    file.read_exact_at(&mut header_bytes, position).ok()?;
    SendMessages2Header::decode(&header_bytes).ok()
}
