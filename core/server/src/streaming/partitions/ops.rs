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
//!
//! # Safety invariants
//!
//! The snapshot-then-read pattern in [`get_messages_by_offset`] and
//! [`poll_messages_by_timestamp`] is safe only under **single-threaded shard
//! execution** (compio runtime). Between the metadata snapshot and the actual
//! reads, no other shard request can mutate the partition state because the
//! message pump processes one request at a time.
//!
//! The poll + auto_commit sequence in the handler (`handlers.rs`) is likewise
//! non-atomic but safe for the same reason.
//!
//! If the architecture ever moves to multi-threaded shard processing or adds
//! compaction/message deletion, these invariants must be re-evaluated.

use super::journal::Journal;
use super::local_partitions::LocalPartitions;
use crate::shard::system::messages::PollingArgs;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::segments::IggyMessagesBatchSet;
use iggy_common::IggyPollMetadata;
use iggy_common::sharding::IggyNamespace;
use iggy_common::{IggyError, PollingKind};
use std::cell::RefCell;
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

    // Snapshot journal and in-flight metadata for routing decisions.
    let (journal_first_offset, in_flight_empty, in_flight_first, in_flight_last) = {
        let store = local_partitions.borrow();
        let partition = store
            .get(namespace)
            .expect("local_partitions: partition must exist for poll");

        let journal = partition.log.journal();
        let in_flight = partition.log.in_flight();
        (
            journal.first_offset(),
            in_flight.is_empty(),
            in_flight.first_offset(),
            in_flight.last_offset(),
        )
    };

    // Lookup ordered by ascending offset: disk -> in-flight -> journal.
    //
    // Offsets are sequential: disk < in-flight < journal. A request may span
    // multiple tiers, so we advance `current` through each sequentially.
    // See issue #2715.

    let mut combined = IggyMessagesBatchSet::empty();
    let mut remaining = count;
    let mut current = start_offset;

    // Lowest in-memory tier boundary (if any). Disk handles offsets below this.
    let in_memory_floor = if !in_flight_empty {
        in_flight_first
    } else {
        journal_first_offset.unwrap_or(u64::MAX)
    };

    // Disk (pre-tier): offsets below the lowest in-memory tier.
    if remaining > 0 && current < in_memory_floor {
        let disk_count =
            ((in_memory_floor.min(current + remaining as u64) - current) as u32).min(remaining);
        let disk_messages =
            load_messages_from_disk(local_partitions, namespace, current, disk_count).await?;
        let loaded = disk_messages.count();
        if loaded > 0 {
            current += loaded as u64;
            remaining = remaining.saturating_sub(loaded);
            combined.add_batch_set(disk_messages);
        }
    }

    // In-flight: committed data being persisted to disk.
    if remaining > 0 && !in_flight_empty && current >= in_flight_first && current <= in_flight_last
    {
        let in_flight_count = ((in_flight_last - current + 1) as u32).min(remaining);
        let in_flight_batches = {
            let store = local_partitions.borrow();
            let partition = store
                .get(namespace)
                .expect("local_partitions: partition must exist for poll");
            partition
                .log
                .in_flight()
                .get_by_offset(current, in_flight_count)
                .to_vec()
        };
        if !in_flight_batches.is_empty() {
            let mut result = IggyMessagesBatchSet::empty();
            result.add_immutable_batches(&in_flight_batches);
            let sliced = result.get_by_offset(current, in_flight_count);
            let loaded = sliced.count();
            if loaded > 0 {
                current += loaded as u64;
                remaining = remaining.saturating_sub(loaded);
                combined.add_batch_set(sliced);
            }
        }
    }

    // Journal: may hold data from recent appends.
    if remaining > 0
        && let Some(jfo) = journal_first_offset
        && current >= jfo
    {
        let journal_messages = {
            let store = local_partitions.borrow();
            let partition = store
                .get(namespace)
                .expect("local_partitions: partition must exist for poll");
            partition
                .log
                .journal()
                .get(|batches| batches.get_by_offset(current, remaining))
        };
        if !journal_messages.is_empty() {
            combined.add_batch_set(journal_messages);
        }
    }

    Ok(combined)
}

/// Poll messages by timestamp.
async fn poll_messages_by_timestamp(
    local_partitions: &RefCell<LocalPartitions>,
    namespace: &IggyNamespace,
    timestamp: u64,
    count: u32,
) -> Result<(IggyPollMetadata, IggyMessagesBatchSet), IggyError> {
    let partition_id = namespace.partition_id();

    // Snapshot metadata from journal and in-flight for routing decisions.
    let (
        metadata,
        journal_first_ts,
        journal_last_ts,
        in_flight_empty,
        in_flight_first_ts,
        in_flight_last_ts,
    ) = {
        let store = local_partitions.borrow();
        let partition = store
            .get(namespace)
            .expect("local_partitions: partition must exist for poll");

        let current_offset = partition.offset.load(Ordering::Relaxed);
        let metadata = IggyPollMetadata::new(partition_id as u32, current_offset);

        let journal = partition.log.journal();

        let in_flight = partition.log.in_flight();
        let (ife, ifts, ilts) = if in_flight.is_empty() {
            (true, 0u64, 0u64)
        } else {
            let first_ts = in_flight
                .batches()
                .first()
                .and_then(|b| b.first_timestamp())
                .unwrap_or(0);
            let last_ts = in_flight
                .batches()
                .last()
                .and_then(|b| b.last_timestamp())
                .unwrap_or(0);
            (false, first_ts, last_ts)
        };

        (
            metadata,
            journal.first_timestamp(),
            journal.last_timestamp(),
            ife,
            ifts,
            ilts,
        )
    };

    if count == 0 {
        return Ok((metadata, IggyMessagesBatchSet::empty()));
    }

    // Three-tier timestamp lookup: disk -> in-flight -> journal.
    // Same structure as offset-based polling (see issue #2715).

    let mut combined = IggyMessagesBatchSet::empty();
    let mut remaining = count;

    // Phase 1: Disk - timestamps before in-flight range.
    let disk_upper_ts = if !in_flight_empty {
        in_flight_first_ts
    } else {
        journal_first_ts.unwrap_or(u64::MAX)
    };

    if timestamp < disk_upper_ts && remaining > 0 {
        let disk_messages =
            load_messages_from_disk_by_timestamp(local_partitions, namespace, timestamp, remaining)
                .await?;
        let loaded = disk_messages.count();
        if loaded > 0 {
            remaining = remaining.saturating_sub(loaded);
            combined.add_batch_set(disk_messages);
        }
    }

    // Phase 2: In-flight - committed data being persisted.
    if remaining > 0 && !in_flight_empty && timestamp <= in_flight_last_ts {
        let in_flight_batches = {
            let store = local_partitions.borrow();
            let partition = store
                .get(namespace)
                .expect("local_partitions: partition must exist for poll");
            partition.log.in_flight().batches().to_vec()
        };
        if !in_flight_batches.is_empty() {
            let mut batch_set = IggyMessagesBatchSet::empty();
            batch_set.add_immutable_batches(&in_flight_batches);
            let filtered = batch_set.get_by_timestamp(timestamp, remaining);
            let loaded = filtered.count();
            if loaded > 0 {
                remaining = remaining.saturating_sub(loaded);
                combined.add_batch_set(filtered);
            }
        }
    }

    // Phase 3: Journal - newest appends (post-commit).
    if remaining > 0
        && let Some(jlts) = journal_last_ts
        && timestamp <= jlts
    {
        let journal_messages = {
            let store = local_partitions.borrow();
            let partition = store
                .get(namespace)
                .expect("local_partitions: partition must exist for poll");
            partition
                .log
                .journal()
                .get(|batches| batches.get_by_timestamp(timestamp, remaining))
        };
        if !journal_messages.is_empty() {
            combined.add_batch_set(journal_messages);
        }
    }

    Ok((metadata, combined))
}

/// Load messages from disk by offset.
pub async fn load_messages_from_disk(
    local_partitions: &RefCell<LocalPartitions>,
    namespace: &IggyNamespace,
    start_offset: u64,
    count: u32,
) -> Result<IggyMessagesBatchSet, IggyError> {
    if count == 0 {
        return Ok(IggyMessagesBatchSet::empty());
    }

    // Get segment range containing the requested offset
    let segment_range = {
        let store = local_partitions.borrow();
        let partition = store
            .get(namespace)
            .expect("local_partitions: partition must exist");

        let segments = partition.log.segments();
        if segments.is_empty() {
            return Ok(IggyMessagesBatchSet::empty());
        }

        let start = segments
            .iter()
            .rposition(|segment| segment.start_offset <= start_offset)
            .unwrap_or(0);
        let end = segments.len();
        start..end
    };

    let mut remaining_count = count;
    let mut batches = IggyMessagesBatchSet::empty();
    let mut current_offset = start_offset;

    for idx in segment_range {
        if remaining_count == 0 {
            break;
        }

        let (segment_start_offset, segment_end_offset) = {
            let store = local_partitions.borrow();
            let partition = store
                .get(namespace)
                .expect("local_partitions: partition must exist");

            let segment = &partition.log.segments()[idx];
            (segment.start_offset, segment.end_offset)
        };

        let offset = if current_offset < segment_start_offset {
            segment_start_offset
        } else {
            current_offset
        };

        let mut end_offset = offset + (remaining_count - 1) as u64;
        if end_offset > segment_end_offset {
            end_offset = segment_end_offset;
        }

        let messages = load_segment_messages(
            local_partitions,
            namespace,
            idx,
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

/// Load messages from a specific segment.
async fn load_segment_messages(
    local_partitions: &RefCell<LocalPartitions>,
    namespace: &IggyNamespace,
    idx: usize,
    start_offset: u64,
    end_offset: u64,
    count: u32,
    segment_start_offset: u64,
) -> Result<IggyMessagesBatchSet, IggyError> {
    let relative_start_offset = (start_offset - segment_start_offset) as u32;

    // Check journal for this segment's data (handles callers outside get_messages_by_offset).
    let journal_data = {
        let store = local_partitions.borrow();
        let partition = store
            .get(namespace)
            .expect("local_partitions: partition must exist");

        let journal = partition.log.journal();

        if let (Some(jfo), Some(jlo)) = (journal.first_offset(), journal.last_offset())
            && start_offset >= jfo
            && end_offset <= jlo
        {
            Some(journal.get(|batches| batches.get_by_offset(start_offset, count)))
        } else {
            None
        }
    };

    if let Some(batches) = journal_data {
        return Ok(batches);
    }

    // Load from disk
    let (index_reader, messages_reader, indexes) = {
        let store = local_partitions.borrow();
        let partition = store
            .get(namespace)
            .expect("local_partitions: partition must exist");

        let storages = partition.log.storages();
        if idx >= storages.len() {
            return Ok(IggyMessagesBatchSet::empty());
        }

        let index_reader = storages[idx]
            .index_reader
            .as_ref()
            .expect("Index reader not initialized")
            .clone();
        let messages_reader = storages[idx]
            .messages_reader
            .as_ref()
            .expect("Messages reader not initialized")
            .clone();
        let indexes_vec = partition.log.indexes();
        let indexes = indexes_vec
            .get(idx)
            .and_then(|opt| opt.as_ref())
            .map(|indexes| {
                indexes
                    .slice_by_offset(relative_start_offset, count)
                    .unwrap_or_default()
            });
        (index_reader, messages_reader, indexes)
    };

    let indexes_to_read = if let Some(indexes) = indexes {
        if !indexes.is_empty() {
            Some(indexes)
        } else {
            index_reader
                .as_ref()
                .load_from_disk_by_offset(relative_start_offset, count)
                .await?
        }
    } else {
        index_reader
            .as_ref()
            .load_from_disk_by_offset(relative_start_offset, count)
            .await?
    };

    if indexes_to_read.is_none() {
        return Ok(IggyMessagesBatchSet::empty());
    }

    let indexes_to_read = indexes_to_read.unwrap();
    let batch = messages_reader
        .as_ref()
        .load_messages_from_disk(indexes_to_read)
        .await?;

    batch.validate_checksums_and_offsets(start_offset)?;

    Ok(IggyMessagesBatchSet::from(batch))
}

/// Load messages from disk by timestamp.
async fn load_messages_from_disk_by_timestamp(
    local_partitions: &RefCell<LocalPartitions>,
    namespace: &IggyNamespace,
    timestamp: u64,
    count: u32,
) -> Result<IggyMessagesBatchSet, IggyError> {
    if count == 0 {
        return Ok(IggyMessagesBatchSet::empty());
    }

    // Find segment range that might contain messages >= timestamp
    let segment_range = {
        let store = local_partitions.borrow();
        let partition = store
            .get(namespace)
            .expect("local_partitions: partition must exist");

        let segments = partition.log.segments();
        if segments.is_empty() {
            return Ok(IggyMessagesBatchSet::empty());
        }

        let start = segments
            .iter()
            .position(|segment| segment.end_timestamp >= timestamp)
            .unwrap_or(segments.len());

        if start >= segments.len() {
            return Ok(IggyMessagesBatchSet::empty());
        }

        start..segments.len()
    };

    let mut remaining_count = count;
    let mut batches = IggyMessagesBatchSet::empty();

    for idx in segment_range {
        if remaining_count == 0 {
            break;
        }

        let segment_end_timestamp = {
            let store = local_partitions.borrow();
            let partition = store
                .get(namespace)
                .expect("local_partitions: partition must exist");
            partition.log.segments()[idx].end_timestamp
        };

        if segment_end_timestamp < timestamp {
            continue;
        }

        let messages = load_segment_messages_by_timestamp(
            local_partitions,
            namespace,
            idx,
            timestamp,
            remaining_count,
        )
        .await?;

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
async fn load_segment_messages_by_timestamp(
    local_partitions: &RefCell<LocalPartitions>,
    namespace: &IggyNamespace,
    idx: usize,
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

        if let (Some(jfts), Some(jlts)) = (journal.first_timestamp(), journal.last_timestamp())
            && timestamp >= jfts
            && timestamp <= jlts
        {
            Some(journal.get(|batches| batches.get_by_timestamp(timestamp, count)))
        } else {
            None
        }
    };

    if let Some(batches) = journal_data {
        return Ok(batches);
    }

    // Load from disk
    let (index_reader, messages_reader, indexes) = {
        let store = local_partitions.borrow();
        let partition = store
            .get(namespace)
            .expect("local_partitions: partition must exist");

        let storages = partition.log.storages();
        if idx >= storages.len() {
            return Ok(IggyMessagesBatchSet::empty());
        }

        let index_reader = storages[idx]
            .index_reader
            .as_ref()
            .expect("Index reader not initialized")
            .clone();
        let messages_reader = storages[idx]
            .messages_reader
            .as_ref()
            .expect("Messages reader not initialized")
            .clone();
        let indexes_vec = partition.log.indexes();
        let indexes = indexes_vec
            .get(idx)
            .and_then(|opt| opt.as_ref())
            .map(|indexes| {
                indexes
                    .slice_by_timestamp(timestamp, count)
                    .unwrap_or_default()
            });
        (index_reader, messages_reader, indexes)
    };

    let indexes_to_read = if let Some(indexes) = indexes {
        if !indexes.is_empty() {
            Some(indexes)
        } else {
            index_reader
                .as_ref()
                .load_from_disk_by_timestamp(timestamp, count)
                .await?
        }
    } else {
        index_reader
            .as_ref()
            .load_from_disk_by_timestamp(timestamp, count)
            .await?
    };

    if indexes_to_read.is_none() {
        return Ok(IggyMessagesBatchSet::empty());
    }

    let indexes_to_read = indexes_to_read.unwrap();
    let batch = messages_reader
        .as_ref()
        .load_messages_from_disk(indexes_to_read)
        .await?;

    Ok(IggyMessagesBatchSet::from(batch))
}
