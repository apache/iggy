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

use crate::{
    configs::{cache_indexes::CacheIndexesConfig, system::SystemConfig},
    slab::{
        partitions::{self, Partitions},
        traits_ext::{
            ComponentsById, Delete, EntityComponentSystem, EntityMarker, Insert, IntoComponents,
        },
    },
    streaming::{
        deduplication::message_deduplicator::MessageDeduplicator,
        partitions::{
            consumer_offset::ConsumerOffset,
            journal::Journal,
            partition::{self, PartitionRef, PartitionRefMut},
            storage,
        },
        polling_consumer::ConsumerGroupId,
        segments::{
            IggyIndexesMut, IggyMessagesBatchMut, IggyMessagesBatchSet, IndexWriter,
            MessagesWriter, storage::Storage,
        },
    },
};
use err_trail::ErrContext;
use iggy_common::{ConsumerOffsetInfo, IggyByteSize, IggyError, IggyMessagesBatch, PooledBuffer};
use std::rc::Rc;
use std::{
    ops::AsyncFnOnce,
    sync::{Arc, atomic::Ordering},
};

pub fn get_partition_ids() -> impl FnOnce(&Partitions) -> Vec<usize> {
    |partitions| {
        partitions.with_components(|components| {
            let (root, ..) = components.into_components();
            root.iter()
                .map(|(_, partition)| partition.id())
                .collect::<Vec<_>>()
        })
    }
}

pub fn delete_partitions(
    partitions_count: u32,
) -> impl FnOnce(&mut Partitions) -> Vec<partition::Partition> {
    move |partitions| {
        let current_count = partitions.len() as u32;
        let partitions_to_delete = partitions_count.min(current_count);
        let start_idx = (current_count - partitions_to_delete) as usize;
        let range = start_idx..current_count as usize;
        range
            .map(|idx| {
                let partition = partitions.delete(idx);
                assert_eq!(partition.id(), idx);
                partition
            })
            .collect()
    }
}

pub fn insert_partition(
    partition: partition::Partition,
) -> impl FnOnce(&mut Partitions) -> partitions::ContainerId {
    move |partitions| partitions.insert(partition)
}

pub fn purge_partitions_mem() -> impl FnOnce(&Partitions) {
    |partitions| {
        partitions.with_components(|components| {
            let (.., stats, _, offsets, _, _, _) = components.into_components();
            for (offset, stat) in offsets
                .iter()
                .map(|(_, o)| o)
                .zip(stats.iter().map(|(_, s)| s))
            {
                offset.store(0, Ordering::Relaxed);
                stat.zero_out_all();
            }
        })
    }
}

pub fn purge_consumer_offsets() -> impl FnOnce(&Partitions) -> (Vec<String>, Vec<String>) {
    |partitions| {
        partitions.with_components(|components| {
            let (.., consumer_offsets, cg_offsets, _) = components.into_components();

            let mut consumer_offset_paths = Vec::new();
            let mut consumer_group_offset_paths = Vec::new();

            // Collect paths and clear consumer offsets
            for (_, consumer_offset) in consumer_offsets {
                let hdl = consumer_offset.pin();
                for item in hdl.values() {
                    consumer_offset_paths.push(item.path.clone());
                }
                hdl.clear(); // Clear the hashmap
            }

            // Collect paths and clear consumer group offsets
            for (_, cg_offset) in cg_offsets {
                let hdl = cg_offset.pin();
                for item in hdl.values() {
                    consumer_group_offset_paths.push(item.path.clone());
                }
                hdl.clear(); // Clear the hashmap
            }

            (consumer_offset_paths, consumer_group_offset_paths)
        })
    }
}

pub fn get_consumer_offset(
    id: usize,
) -> impl FnOnce(ComponentsById<PartitionRef>) -> Option<ConsumerOffsetInfo> {
    move |(root, _, _, current_offset, offsets, _, _)| {
        offsets.pin().get(&id).map(|item| ConsumerOffsetInfo {
            partition_id: root.id() as u32,
            current_offset: current_offset.load(Ordering::Relaxed),
            stored_offset: item.offset.load(Ordering::Relaxed),
        })
    }
}

pub fn get_consumer_group_offset(
    consumer_group_id: ConsumerGroupId,
) -> impl FnOnce(ComponentsById<PartitionRef>) -> Option<ConsumerOffsetInfo> {
    move |(root, _, _, current_offset, _, offsets, _)| {
        offsets
            .pin()
            .get(&consumer_group_id)
            .map(|item| ConsumerOffsetInfo {
                partition_id: root.id() as u32,
                current_offset: current_offset.load(Ordering::Relaxed),
                stored_offset: item.offset.load(Ordering::Relaxed),
            })
    }
}

pub fn store_consumer_offset(
    id: usize,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    offset: u64,
    config: &SystemConfig,
) -> impl FnOnce(ComponentsById<PartitionRef>) {
    move |(.., offsets, _, _)| {
        let hdl = offsets.pin();
        let item = hdl.get_or_insert(
            id,
            ConsumerOffset::default_for_consumer(
                id as u32,
                &config.get_consumer_offsets_path(stream_id, topic_id, partition_id),
            ),
        );
        item.offset.store(offset, Ordering::Relaxed);
    }
}

pub fn delete_consumer_offset(
    id: usize,
) -> impl FnOnce(ComponentsById<PartitionRef>) -> Result<String, IggyError> {
    move |(.., offsets, _, _)| {
        let hdl = offsets.pin();
        let offset = hdl
            .remove(&id)
            .ok_or_else(|| IggyError::ConsumerOffsetNotFound(id))?;
        Ok(offset.path.clone())
    }
}

pub fn persist_consumer_offset_to_disk(
    id: usize,
) -> impl AsyncFnOnce(ComponentsById<PartitionRef>) -> Result<(), IggyError> {
    async move |(.., offsets, _, _)| {
        let hdl = offsets.pin();
        let item = hdl
            .get(&id)
            .expect("persist_consumer_offset_to_disk: offset not found");
        let offset = item.offset.load(Ordering::Relaxed);
        storage::persist_offset(&item.path, offset).await
    }
}

pub fn delete_consumer_offset_from_disk(
    id: usize,
) -> impl AsyncFnOnce(ComponentsById<PartitionRef>) -> Result<(), IggyError> {
    async move |(.., offsets, _, _)| {
        let hdl = offsets.pin();
        let item = hdl
            .get(&id)
            .expect("delete_consumer_offset_from_disk: offset not found");
        let path = &item.path;
        storage::delete_persisted_offset(path).await
    }
}

pub fn store_consumer_group_offset(
    consumer_group_id: ConsumerGroupId,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    offset: u64,
    config: &SystemConfig,
) -> impl FnOnce(ComponentsById<PartitionRef>) {
    move |(.., offsets, _)| {
        let hdl = offsets.pin();
        let item = hdl.get_or_insert(
            consumer_group_id,
            ConsumerOffset::default_for_consumer_group(
                consumer_group_id,
                &config.get_consumer_group_offsets_path(stream_id, topic_id, partition_id),
            ),
        );
        item.offset.store(offset, Ordering::Relaxed);
    }
}

pub fn delete_consumer_group_offset(
    consumer_group_id: ConsumerGroupId,
) -> impl FnOnce(ComponentsById<PartitionRef>) -> Result<String, IggyError> {
    move |(.., offsets, _)| {
        let hdl = offsets.pin();
        let offset = hdl
            .remove(&consumer_group_id)
            .ok_or_else(|| IggyError::ConsumerOffsetNotFound(consumer_group_id.0))?;
        Ok(offset.path.clone())
    }
}

pub fn persist_consumer_group_offset_to_disk(
    consumer_group_id: ConsumerGroupId,
) -> impl AsyncFnOnce(ComponentsById<PartitionRef>) -> Result<(), IggyError> {
    async move |(.., offsets, _)| {
        let hdl = offsets.pin();
        let item = hdl
            .get(&consumer_group_id)
            .expect("persist_consumer_group_offset_to_disk: offset not found");
        let offset = item.offset.load(Ordering::Relaxed);
        storage::persist_offset(&item.path, offset).await
    }
}

pub fn delete_consumer_group_offset_from_disk(
    consumer_group_id: ConsumerGroupId,
) -> impl AsyncFnOnce(ComponentsById<PartitionRef>) -> Result<(), IggyError> {
    async move |(.., offsets, _)| {
        let hdl = offsets.pin();
        let item = hdl
            .get(&consumer_group_id)
            .expect("delete_consumer_group_offset_from_disk: offset not found");
        let path = &item.path;
        storage::delete_persisted_offset(path).await
    }
}

pub fn create_message_deduplicator(config: &SystemConfig) -> Option<MessageDeduplicator> {
    if !config.message_deduplication.enabled {
        return None;
    }
    let max_entries = if config.message_deduplication.max_entries > 0 {
        Some(config.message_deduplication.max_entries)
    } else {
        None
    };
    let expiry = if !config.message_deduplication.expiry.is_zero() {
        Some(config.message_deduplication.expiry)
    } else {
        None
    };

    Some(MessageDeduplicator::new(max_entries, expiry))
}

pub fn get_segment_range_by_offset(
    offset: u64,
) -> impl FnOnce(ComponentsById<PartitionRef>) -> std::ops::Range<usize> {
    move |(.., log)| {
        let segments = log.segments();

        if segments.is_empty() {
            return 0..0;
        }

        let start = segments
            .iter()
            .rposition(|segment| segment.start_offset <= offset)
            .unwrap_or(0);

        let end = segments.len();
        start..end
    }
}

pub fn get_segment_range_by_timestamp(
    timestamp: u64,
) -> impl FnOnce(ComponentsById<PartitionRef>) -> Result<std::ops::Range<usize>, IggyError> {
    move |(.., log)| -> Result<std::ops::Range<usize>, IggyError> {
        let segments = log.segments();

        if segments.is_empty() {
            return Ok(0..0);
        }

        let start = segments
            .iter()
            .enumerate()
            .filter(|(_, segment)| segment.end_timestamp >= timestamp)
            .map(|(index, _)| index)
            .next()
            .ok_or(IggyError::TimestampOutOfRange(timestamp))?;
        let end = segments.len();
        Ok(start..end)
    }
}

pub async fn load_messages_from_disk_by_timestamp(
    storage: &Storage,
    index: &Option<IggyIndexesMut>,
    timestamp: u64,
    count: u32,
) -> Result<IggyMessagesBatchSet, IggyError> {
    let indexes_to_read = if let Some(indexes) = index {
        if !indexes.is_empty() {
            indexes.slice_by_timestamp(timestamp, count)
        } else {
            storage
                .index_reader
                .as_ref()
                .expect("Index reader not initialized")
                .load_from_disk_by_timestamp(timestamp, count)
                .await?
        }
    } else {
        storage
            .index_reader
            .as_ref()
            .expect("Index reader not initialized")
            .load_from_disk_by_timestamp(timestamp, count)
            .await?
    };

    if indexes_to_read.is_none() {
        return Ok(IggyMessagesBatchSet::empty());
    }

    let indexes_to_read = indexes_to_read.unwrap();

    let batch = storage
        .messages_reader
        .as_ref()
        .expect("Messages reader not initialized")
        .load_messages_from_disk(indexes_to_read)
        .await
        .error(|e: &IggyError| format!("Failed to load messages from disk by timestamp: {e}"))?;

    Ok(IggyMessagesBatchSet::from(batch))
}

pub fn calculate_current_offset() -> impl FnOnce(ComponentsById<PartitionRef>) -> u64 {
    |(root, _, _, offset, ..)| {
        if !root.should_increment_offset() {
            0
        } else {
            offset.load(Ordering::Relaxed) + 1
        }
    }
}

pub fn get_segment_start_offset_and_deduplicator()
-> impl FnOnce(ComponentsById<PartitionRef>) -> (u64, Option<Arc<MessageDeduplicator>>) {
    move |(.., deduplicator, _, _, _, log)| {
        let segment = log.active_segment();
        (segment.start_offset, deduplicator.clone())
    }
}

pub fn append_to_journal(
    current_offset: u64,
    batch: IggyMessagesBatchMut,
) -> impl FnOnce(ComponentsById<PartitionRefMut>) -> Result<(u32, u32), IggyError> {
    move |(root, stats, _, offset, .., log)| {
        let segment = log.active_segment_mut();

        if segment.end_offset == 0 {
            segment.start_timestamp = batch.first_timestamp().unwrap();
        }

        let batch_messages_size = batch.size();
        let batch_messages_count = batch.count();

        stats.increment_size_bytes(batch_messages_size as u64);
        stats.increment_messages_count(batch_messages_count as u64);

        segment.end_timestamp = batch.last_timestamp().unwrap();
        segment.end_offset = batch.last_offset().unwrap();

        let (journal_messages_count, journal_size) = log.journal_mut().append(batch)?;

        let last_offset = if batch_messages_count == 0 {
            current_offset
        } else {
            current_offset + batch_messages_count as u64 - 1
        };

        if root.should_increment_offset() {
            offset.store(last_offset, Ordering::Relaxed);
        } else {
            root.set_should_increment_offset(true);
            offset.store(last_offset, Ordering::Relaxed);
        }
        log.active_segment_mut().current_position += batch_messages_size;

        Ok((journal_messages_count, journal_size))
    }
}

pub fn clear_in_flight() -> impl FnOnce(ComponentsById<PartitionRefMut>) {
    |(.., log)| {
        log.clear_in_flight();
    }
}

/// Result of committing journal batches for a specific segment.
/// Captures writers, indexes, and frozen batches at commit time to prevent race conditions.
/// The in-flight buffer is set atomically during commit to ensure polls can always find data.
pub struct CommittedBatch {
    pub segment_idx: usize,
    pub messages_writer: Rc<MessagesWriter>,
    pub index_writer: Rc<IndexWriter>,
    /// Indexes captured at commit time, before any rotation can clear them.
    pub unsaved_indexes: PooledBuffer,
    /// Frozen (immutable) batches ready for disk persistence.
    /// These are also stored in the in-flight buffer for reads during async I/O.
    pub frozen: Vec<IggyMessagesBatch>,
}

/// Commits the journal and returns the batches along with the segment's writers and indexes.
/// By capturing writers, indexes, and frozen batches at commit time (within the same lock
/// acquisition), we ensure that even if segment rotation happens after this call, we still
/// have valid data to complete the persistence operation.
///
/// CRITICAL: This function also sets the in-flight buffer atomically. This prevents a race
/// condition where a poll could arrive after journal commit but before in-flight is set,
/// finding both journal and in-flight empty while data is not yet on disk.
pub fn commit_journal() -> impl FnOnce(ComponentsById<PartitionRefMut>) -> CommittedBatch {
    |(.., log)| {
        let segment_idx = log.segments().len().saturating_sub(1);
        let mut batches = log.journal_mut().commit();
        log.ensure_indexes();
        batches.append_indexes_to(log.active_indexes_mut().unwrap());

        let storage = log.active_storage();
        let messages_writer = storage
            .messages_writer
            .as_ref()
            .expect("Messages writer must exist at commit time")
            .clone();
        let index_writer = storage
            .index_writer
            .as_ref()
            .expect("Index writer must exist at commit time")
            .clone();
        let unsaved_indexes = log.active_indexes().unwrap().unsaved_slice();
        let frozen: Vec<IggyMessagesBatch> = batches.iter_mut().map(|b| b.freeze()).collect();
        log.set_in_flight(frozen.clone());

        CommittedBatch {
            segment_idx,
            messages_writer,
            index_writer,
            unsaved_indexes,
            frozen,
        }
    }
}

pub fn is_segment_full() -> impl FnOnce(ComponentsById<PartitionRef>) -> bool {
    |(.., log)| log.active_segment().is_full()
}

pub fn persist_reason(
    unsaved_messages_count_exceeded: bool,
    unsaved_messages_size_exceeded: bool,
    journal_messages_count: u32,
    journal_size: u32,
    config: &SystemConfig,
) -> impl FnOnce(ComponentsById<PartitionRef>) -> String {
    move |(.., log)| {
        if unsaved_messages_count_exceeded {
            format!(
                "unsaved messages count exceeded: {}, max from config: {}",
                journal_messages_count, config.partition.messages_required_to_save,
            )
        } else if unsaved_messages_size_exceeded {
            format!(
                "unsaved messages size exceeded: {}, max from config: {}",
                journal_size, config.partition.size_of_messages_required_to_save,
            )
        } else {
            format!(
                "segment is full, current size: {}, max from config: {}",
                log.active_segment().size,
                &config.segment.size,
            )
        }
    }
}

/// Updates segment size and marks indexes as saved for a specific segment.
/// Uses segment_idx to ensure we update the correct segment even after rotation.
pub fn update_index_and_increment_stats(
    segment_idx: usize,
    saved: IggyByteSize,
    config: &SystemConfig,
) -> impl FnOnce(ComponentsById<PartitionRefMut>) {
    move |(.., log)| {
        if let Some(segment) = log.segments_mut().get_mut(segment_idx) {
            segment.size = IggyByteSize::from(segment.size.as_bytes_u64() + saved.as_bytes_u64());
        }
        if let Some(Some(indexes)) = log.indexes_mut().get_mut(segment_idx) {
            indexes.mark_saved();
            if config.segment.cache_indexes == CacheIndexesConfig::None {
                indexes.clear();
            }
        }
    }
}
