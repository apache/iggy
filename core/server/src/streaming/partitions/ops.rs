/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Shared partition operations that can be used by both production code and tests.
//!
//! This module provides the core logic for polling and loading messages from partitions,
//! avoiding code duplication between `IggyShard` and test harnesses.

use super::journal::Journal;
use super::local_partitions::LocalPartitions;
use crate::shard::system::messages::PollingArgs;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::segments::IggyMessagesBatchSet;
use crate::streaming::segments::IndexReader;
use crate::streaming::segments::MessagesReader;
use iggy_common::IggyPollMetadata;
use iggy_common::sharding::IggyNamespace;
use iggy_common::{IggyError, PollingKind};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::Ordering;

/// Poll messages from a partition partitions.
///
/// This is the core polling logic shared between production code and tests.
pub async fn poll_messages(
    local_partitions: &RefCell<LocalPartitions>,
    namespace: &IggyNamespace,
    consumer: PollingConsumer,
    args: PollingArgs,
) -> Result<(IggyPollMetadata, IggyMessagesBatchSet), IggyError> {
    let partition_id = namespace.partition_id();
    let count = args.count;
    let strategy = args.strategy;
    let value = strategy.value;

    // Handle timestamp polling separately - it has different logic
    if strategy.kind == PollingKind::Timestamp {
        return poll_messages_by_timestamp(local_partitions, namespace, value, count).await;
    }

    // Phase 1: Extract metadata and determine start offset
    let (metadata, start_offset) = {
        let store = local_partitions.borrow();
        let partition = store
            .get(namespace)
            .expect("local_partitions: partition must exist for poll");

        let current_offset = partition.offset.load(Ordering::Relaxed);
        let metadata = IggyPollMetadata::new(partition_id as u32, current_offset);

        let start_offset = match strategy.kind {
            PollingKind::Offset => {
                let offset = value;
                if offset > current_offset {
                    return Ok((metadata, IggyMessagesBatchSet::empty()));
                }
                offset
            }
            PollingKind::First => partition
                .log
                .segments()
                .first()
                .map(|segment| segment.start_offset)
                .unwrap_or(0),
            PollingKind::Last => {
                let mut requested_count = count as u64;
                if requested_count > current_offset + 1 {
                    requested_count = current_offset + 1;
                }
                1 + current_offset - requested_count
            }
            PollingKind::Next => {
                let stored_offset = match consumer {
                    PollingConsumer::Consumer(id, _) => partition
                        .consumer_offsets
                        .pin()
                        .get(&id)
                        .map(|item| item.offset.load(Ordering::Relaxed)),
                    PollingConsumer::ConsumerGroup(cg_id, _) => partition
                        .consumer_group_offsets
                        .pin()
                        .get(&cg_id)
                        .map(|item| item.offset.load(Ordering::Relaxed)),
                };
                match stored_offset {
                    Some(offset) => offset + 1,
                    None => partition
                        .log
                        .segments()
                        .first()
                        .map(|segment| segment.start_offset)
                        .unwrap_or(0),
                }
            }
            PollingKind::Timestamp => unreachable!("Timestamp handled above"),
        };

        if start_offset > current_offset || count == 0 {
            return Ok((metadata, IggyMessagesBatchSet::empty()));
        }

        (metadata, start_offset)
    };

    // Phase 2: Get messages using hybrid disk+journal logic
    let batches = get_messages_by_offset(local_partitions, namespace, start_offset, count).await?;
    Ok((metadata, batches))
}

/// Get messages by offset, handling the hybrid disk+journal case.
pub async fn get_messages_by_offset(
    local_partitions: &RefCell<LocalPartitions>,
    namespace: &IggyNamespace,
    start_offset: u64,
    count: u32,
) -> Result<IggyMessagesBatchSet, IggyError> {
    if count == 0 {
        return Ok(IggyMessagesBatchSet::empty());
    }

    // Get journal and in_flight metadata for routing
    let (
        is_journal_empty,
        journal_first_offset,
        journal_last_offset,
        in_flight_empty,
        in_flight_first,
        in_flight_last,
    ) = {
        let store = local_partitions.borrow();
        let partition = store
            .get(namespace)
            .expect("local_partitions: partition must exist for poll");

        let journal = partition.log.journal();
        let journal_inner = journal.inner();
        let in_flight = partition.log.in_flight();
        (
            journal.is_empty(),
            journal_inner.base_offset,
            journal_inner.current_offset,
            in_flight.is_empty(),
            in_flight.first_offset(),
            in_flight.last_offset(),
        )
    };

    let end_offset = start_offset + (count - 1).max(1) as u64;

    // Case 0: Journal is empty - check in_flight buffer or disk
    if is_journal_empty {
        if !in_flight_empty && start_offset >= in_flight_first && start_offset <= in_flight_last {
            let in_flight_batches = {
                let store = local_partitions.borrow();
                let partition = store
                    .get(namespace)
                    .expect("local_partitions: partition must exist for poll");
                partition
                    .log
                    .in_flight()
                    .get_by_offset(start_offset, count)
                    .to_vec()
            };
            if !in_flight_batches.is_empty() {
                let mut result = IggyMessagesBatchSet::empty();
                result.add_immutable_batches(&in_flight_batches);
                return Ok(result.get_by_offset(start_offset, count));
            }
        }
        return load_messages_from_disk(local_partitions, namespace, start_offset, count).await;
    }

    // Case 1: All messages are in journal
    if start_offset >= journal_first_offset && end_offset <= journal_last_offset {
        let batches = {
            let store = local_partitions.borrow();
            let partition = store
                .get(namespace)
                .expect("local_partitions: partition must exist for poll");
            partition
                .log
                .journal()
                .get(|batches| batches.get_by_offset(start_offset, count))
        };
        return Ok(batches);
    }

    // Case 2: All messages on disk (end_offset < journal_first_offset)
    if end_offset < journal_first_offset {
        return load_messages_from_disk(local_partitions, namespace, start_offset, count).await;
    }

    // Case 3: Messages span disk and journal boundary
    let disk_count = if start_offset < journal_first_offset {
        ((journal_first_offset - start_offset) as u32).min(count)
    } else {
        0
    };

    let mut combined_batch_set = IggyMessagesBatchSet::empty();

    // Load messages from disk if needed
    if disk_count > 0 {
        let disk_messages =
            load_messages_from_disk(local_partitions, namespace, start_offset, disk_count).await?;
        if !disk_messages.is_empty() {
            combined_batch_set.add_batch_set(disk_messages);
        }
    }

    // Get remaining messages from journal
    let remaining_count = count - combined_batch_set.count();
    if remaining_count > 0 {
        let journal_start_offset = std::cmp::max(start_offset, journal_first_offset);
        let journal_messages = {
            let store = local_partitions.borrow();
            let partition = store
                .get(namespace)
                .expect("local_partitions: partition must exist for poll");
            partition
                .log
                .journal()
                .get(|batches| batches.get_by_offset(journal_start_offset, remaining_count))
        };
        if !journal_messages.is_empty() {
            combined_batch_set.add_batch_set(journal_messages);
        }
    }

    Ok(combined_batch_set)
}

/// Poll messages by timestamp.
async fn poll_messages_by_timestamp(
    local_partitions: &RefCell<LocalPartitions>,
    namespace: &IggyNamespace,
    timestamp: u64,
    count: u32,
) -> Result<(IggyPollMetadata, IggyMessagesBatchSet), IggyError> {
    let partition_id = namespace.partition_id();

    // Get metadata and journal info
    let (metadata, is_journal_empty, journal_first_timestamp, journal_last_timestamp) = {
        let store = local_partitions.borrow();
        let partition = store
            .get(namespace)
            .expect("local_partitions: partition must exist for poll");

        let current_offset = partition.offset.load(Ordering::Relaxed);
        let metadata = IggyPollMetadata::new(partition_id as u32, current_offset);

        let journal = partition.log.journal();
        let journal_inner = journal.inner();
        (
            metadata,
            journal.is_empty(),
            journal_inner.first_timestamp,
            journal_inner.end_timestamp,
        )
    };

    if count == 0 {
        return Ok((metadata, IggyMessagesBatchSet::empty()));
    }

    // Case 0: Journal is empty, all messages on disk
    if is_journal_empty {
        let batches =
            load_messages_from_disk_by_timestamp(local_partitions, namespace, timestamp, count)
                .await?;
        return Ok((metadata, batches));
    }

    // Case 1: Timestamp is after journal's last timestamp - no messages
    if timestamp > journal_last_timestamp {
        return Ok((metadata, IggyMessagesBatchSet::empty()));
    }

    // Case 2: Timestamp is within journal range - get from journal
    if timestamp >= journal_first_timestamp {
        let batches = {
            let store = local_partitions.borrow();
            let partition = store
                .get(namespace)
                .expect("local_partitions: partition must exist for poll");
            partition
                .log
                .journal()
                .get(|batches| batches.get_by_timestamp(timestamp, count))
        };
        return Ok((metadata, batches));
    }

    // Case 3: Timestamp is before journal - need disk + possibly journal
    let disk_messages =
        load_messages_from_disk_by_timestamp(local_partitions, namespace, timestamp, count).await?;

    if disk_messages.count() >= count {
        return Ok((metadata, disk_messages));
    }

    // Case 4: Messages span disk and journal
    let remaining_count = count - disk_messages.count();
    let journal_messages = {
        let store = local_partitions.borrow();
        let partition = store
            .get(namespace)
            .expect("local_partitions: partition must exist for poll");
        partition
            .log
            .journal()
            .get(|batches| batches.get_by_timestamp(timestamp, remaining_count))
    };

    let mut combined_batch_set = disk_messages;
    if !journal_messages.is_empty() {
        combined_batch_set.add_batch_set(journal_messages);
    }
    Ok((metadata, combined_batch_set))
}

/// Load messages from disk by offset.
///
/// Re-resolves segments by `current_offset` on each iteration to avoid stale
/// indices across `.await` points (the message_cleaner can delete front segments
/// from the vec between cooperative yields on the compio thread).
pub async fn load_messages_from_disk(
    local_partitions: &RefCell<LocalPartitions>,
    namespace: &IggyNamespace,
    start_offset: u64,
    count: u32,
) -> Result<IggyMessagesBatchSet, IggyError> {
    if count == 0 {
        return Ok(IggyMessagesBatchSet::empty());
    }

    let mut remaining_count = count;
    let mut batches = IggyMessagesBatchSet::empty();
    let mut current_offset = start_offset;

    loop {
        if remaining_count == 0 {
            break;
        }

        // Resolve the segment containing current_offset in a fresh borrow each iteration.
        let extracted = {
            let store = local_partitions.borrow();
            let partition = store
                .get(namespace)
                .expect("local_partitions: partition must exist");

            let segments = partition.log.segments();
            if segments.is_empty() {
                None
            } else {
                segments
                    .iter()
                    .rposition(|segment| segment.start_offset <= current_offset)
                    .map(|seg_idx| {
                        let segment = &segments[seg_idx];
                        (segment.start_offset, segment.end_offset)
                    })
            }
        };

        let Some((segment_start_offset, segment_end_offset)) = extracted else {
            break;
        };

        let offset = current_offset.max(segment_start_offset);
        let mut end_offset = offset + (remaining_count - 1).max(1) as u64;
        if end_offset > segment_end_offset {
            end_offset = segment_end_offset;
        }

        let messages = load_segment_messages(
            local_partitions,
            namespace,
            offset,
            end_offset,
            remaining_count,
            segment_start_offset,
        )
        .await?;

        let loaded_count = messages.count();
        if loaded_count > 0 {
            batches.add_batch_set(messages);
            remaining_count = remaining_count.saturating_sub(loaded_count);
            current_offset = end_offset + 1;
        } else {
            break;
        }
    }

    Ok(batches)
}

/// Load messages from a specific segment identified by `segment_start_offset`.
///
/// Resolves the segment by its start_offset (stable identity) rather than by
/// vec index, which can go stale if the message_cleaner removes front segments.
/// Clones `Rc` readers so the underlying fds survive even if the segment is
/// later removed from the vec (POSIX semantics: open fds outlive unlink).
async fn load_segment_messages(
    local_partitions: &RefCell<LocalPartitions>,
    namespace: &IggyNamespace,
    start_offset: u64,
    end_offset: u64,
    count: u32,
    segment_start_offset: u64,
) -> Result<IggyMessagesBatchSet, IggyError> {
    let relative_start_offset = (start_offset - segment_start_offset) as u32;

    // Check journal first for this segment's data
    let journal_data = {
        let store = local_partitions.borrow();
        let partition = store
            .get(namespace)
            .expect("local_partitions: partition must exist");

        let journal = partition.log.journal();
        let is_journal_empty = journal.is_empty();
        let journal_inner = journal.inner();
        let journal_first_offset = journal_inner.base_offset;
        let journal_last_offset = journal_inner.current_offset;

        if !is_journal_empty
            && start_offset >= journal_first_offset
            && end_offset <= journal_last_offset
        {
            Some(journal.get(|batches| batches.get_by_offset(start_offset, count)))
        } else {
            None
        }
    };

    if let Some(batches) = journal_data {
        return Ok(batches);
    }

    // Resolve segment by start_offset and extract Rc-cloned readers
    let Some((index_reader, messages_reader, cached_indexes)) = resolve_segment_readers(
        local_partitions,
        namespace,
        segment_start_offset,
        |indexes| {
            indexes
                .slice_by_offset(relative_start_offset, count)
                .unwrap_or_default()
        },
    ) else {
        return Ok(IggyMessagesBatchSet::empty());
    };

    let indexes_to_read = match cached_indexes {
        Some(indexes) if !indexes.is_empty() => Some(indexes),
        _ => {
            index_reader
                .load_from_disk_by_offset(relative_start_offset, count)
                .await?
        }
    };

    let Some(indexes_to_read) = indexes_to_read else {
        return Ok(IggyMessagesBatchSet::empty());
    };

    let batch = messages_reader
        .load_messages_from_disk(indexes_to_read)
        .await?;

    batch.validate_checksums_and_offsets(start_offset)?;

    Ok(IggyMessagesBatchSet::from(batch))
}

/// Load messages from disk by timestamp.
///
/// Re-resolves segments each iteration using a `min_start_offset` cursor to
/// advance past processed segments, avoiding stale vec indices across `.await`.
async fn load_messages_from_disk_by_timestamp(
    local_partitions: &RefCell<LocalPartitions>,
    namespace: &IggyNamespace,
    timestamp: u64,
    count: u32,
) -> Result<IggyMessagesBatchSet, IggyError> {
    if count == 0 {
        return Ok(IggyMessagesBatchSet::empty());
    }

    let mut remaining_count = count;
    let mut batches = IggyMessagesBatchSet::empty();
    let mut min_start_offset: u64 = 0;

    loop {
        if remaining_count == 0 {
            break;
        }

        // Find next unprocessed segment whose end_timestamp covers the requested timestamp
        let extracted = {
            let store = local_partitions.borrow();
            let partition = store
                .get(namespace)
                .expect("local_partitions: partition must exist");

            let segments = partition.log.segments();
            if segments.is_empty() {
                None
            } else {
                segments
                    .iter()
                    .find(|seg| {
                        seg.end_timestamp >= timestamp && seg.end_offset >= min_start_offset
                    })
                    .map(|seg| (seg.start_offset, seg.end_offset))
            }
        };

        let Some((segment_start_offset, segment_end_offset)) = extracted else {
            break;
        };

        let messages = load_segment_messages_by_timestamp(
            local_partitions,
            namespace,
            segment_start_offset,
            timestamp,
            remaining_count,
        )
        .await?;

        min_start_offset = segment_end_offset + 1;

        let messages_count = messages.count();
        if messages_count == 0 {
            continue;
        }

        remaining_count = remaining_count.saturating_sub(messages_count);
        batches.add_batch_set(messages);
    }

    Ok(batches)
}

/// Load messages from a specific segment by timestamp.
///
/// Resolves the segment by its start_offset (stable identity) rather than by
/// vec index. Same rationale as [`load_segment_messages`].
async fn load_segment_messages_by_timestamp(
    local_partitions: &RefCell<LocalPartitions>,
    namespace: &IggyNamespace,
    segment_start_offset: u64,
    timestamp: u64,
    count: u32,
) -> Result<IggyMessagesBatchSet, IggyError> {
    if count == 0 {
        return Ok(IggyMessagesBatchSet::empty());
    }

    // Check journal first
    let journal_data = {
        let store = local_partitions.borrow();
        let partition = store
            .get(namespace)
            .expect("local_partitions: partition must exist");

        let journal = partition.log.journal();
        let is_journal_empty = journal.is_empty();
        let journal_inner = journal.inner();
        let journal_first_timestamp = journal_inner.first_timestamp;
        let journal_last_timestamp = journal_inner.end_timestamp;

        if !is_journal_empty
            && timestamp >= journal_first_timestamp
            && timestamp <= journal_last_timestamp
        {
            Some(journal.get(|batches| batches.get_by_timestamp(timestamp, count)))
        } else {
            None
        }
    };

    if let Some(batches) = journal_data {
        return Ok(batches);
    }

    // Resolve segment by start_offset and extract Rc-cloned readers
    let Some((index_reader, messages_reader, cached_indexes)) = resolve_segment_readers(
        local_partitions,
        namespace,
        segment_start_offset,
        |indexes| {
            indexes
                .slice_by_timestamp(timestamp, count)
                .unwrap_or_default()
        },
    ) else {
        return Ok(IggyMessagesBatchSet::empty());
    };

    let indexes_to_read = match cached_indexes {
        Some(indexes) if !indexes.is_empty() => Some(indexes),
        _ => {
            index_reader
                .load_from_disk_by_timestamp(timestamp, count)
                .await?
        }
    };

    let Some(indexes_to_read) = indexes_to_read else {
        return Ok(IggyMessagesBatchSet::empty());
    };

    let batch = messages_reader
        .load_messages_from_disk(indexes_to_read)
        .await?;

    Ok(IggyMessagesBatchSet::from(batch))
}

/// Resolve a segment by its `start_offset` (stable identity) and extract
/// Rc-cloned readers plus optionally-cached indexes. Returns `None` if the
/// segment no longer exists (deleted by message_cleaner).
///
/// Only clones the reader Rcs (not writers) since callers only perform reads.
/// The cloned Rcs keep the underlying fds alive even if the segment is later
/// removed from the vec (POSIX semantics: open fds outlive unlink).
fn resolve_segment_readers<F, T>(
    local_partitions: &RefCell<LocalPartitions>,
    namespace: &IggyNamespace,
    segment_start_offset: u64,
    slice_indexes: F,
) -> Option<(Rc<IndexReader>, Rc<MessagesReader>, Option<T>)>
where
    F: FnOnce(&iggy_common::IggyIndexesMut) -> T,
{
    let store = local_partitions.borrow();
    let partition = store
        .get(namespace)
        .expect("local_partitions: partition must exist");

    let segments = partition.log.segments();
    // TODO: O(n) linear scan — fine for typical segment counts but consider a
    // HashMap<start_offset, idx> if partitions accumulate many segments.
    let seg_idx = segments
        .iter()
        .position(|s| s.start_offset == segment_start_offset)?;

    let storages = partition.log.storages();
    if seg_idx >= storages.len() {
        return None;
    }

    let storage = &storages[seg_idx];
    let index_reader = storage
        .index_reader
        .as_ref()
        .expect("Index reader not initialized")
        .clone();
    let messages_reader = storage
        .messages_reader
        .as_ref()
        .expect("Messages reader not initialized")
        .clone();

    let cached_indexes = partition
        .log
        .indexes()
        .get(seg_idx)
        .and_then(|opt| opt.as_ref())
        .map(slice_indexes);

    Some((index_reader, messages_reader, cached_indexes))
}
