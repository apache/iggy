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

use crate::server_error::ServerNgError;
use configs::server_ng::ServerNgConfig;
use iggy_common::{IggyByteSize, IggyError, PartitionStats};
use partitions::{IggyIndexReader, Segment};
use server_common::SegmentStorage;
use server_common::send_messages2::{COMMAND_HEADER_SIZE, SendMessages2Header};
use std::fs;
use std::os::unix::fs::FileExt;
use tracing::{error, warn};

const LOG_EXTENSION: &str = "log";

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

        // An index without a single whole entry means any log bytes were torn
        // off mid-write (the crash landed between the message write and the
        // index write). Recover the segment as EMPTY: counting the bytes with
        // `end_offset == start_offset` would fabricate one phantom message for
        // the bootstrap non-empty filters, and appending after the torn bytes
        // would strand undecodable garbage inside the readable range. Zeroed
        // sizes make the next append overwrite the torn bytes instead.
        let (start_timestamp, end_timestamp, end_offset, effective_messages_size) =
            if let Some((start_timestamp, end_timestamp, end_offset)) = bounds {
                (start_timestamp, end_timestamp, end_offset, messages_size)
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

    Ok(recovered)
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
async fn recover_segment_bounds(
    index_path: &str,
    messages_path: &str,
    start_offset: u64,
    messages_size: u64,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
) -> Result<Option<(u64, u64, u64)>, ServerNgError> {
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
            let last_batch_extent = read_batch_extent(messages_path, last.position, messages_size)
                .ok_or(ServerNgError::RecoveredSegmentSizeDivergence {
                    stream_id,
                    topic_id,
                    partition_id,
                    start_offset,
                    end_offset: last.offset,
                    messages_size_bytes: messages_size,
                    indexed_size_bytes: last.position,
                })?;
            if last_batch_extent > messages_size {
                return Err(ServerNgError::RecoveredSegmentSizeDivergence {
                    stream_id,
                    topic_id,
                    partition_id,
                    start_offset,
                    end_offset: last.offset,
                    messages_size_bytes: messages_size,
                    indexed_size_bytes: last_batch_extent,
                });
            }
            Ok(Some((first.timestamp, last.timestamp, last.offset)))
        }
        _ => Ok(None),
    }
}

/// End byte of the batch whose command header sits at `position` in the
/// messages file, or `None` when the header itself does not fit / decode
/// (`position` past the file, header truncated, or garbage bytes).
fn read_batch_extent(messages_path: &str, position: u64, messages_size: u64) -> Option<u64> {
    if position.checked_add(COMMAND_HEADER_SIZE as u64)? > messages_size {
        return None;
    }
    let file = fs::File::open(messages_path).ok()?;
    let mut header_bytes = [0u8; COMMAND_HEADER_SIZE];
    file.read_exact_at(&mut header_bytes, position).ok()?;
    let header = SendMessages2Header::decode(&header_bytes).ok()?;
    position.checked_add(header.total_size() as u64)
}
