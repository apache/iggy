use error_set::ErrContext;
use iggy_common::{ConsumerOffsetInfo, Identifier, IggyByteSize, IggyError};
use std::{
    ops::{AsyncFnOnce, Index},
    sync::atomic::Ordering,
};
use sysinfo::Component;

use crate::{
    configs::{cache_indexes::CacheIndexesConfig, system::SystemConfig},
    shard_trace,
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
            journal::{Journal, MemoryMessageJournal},
            partition2::{self, PartitionRef, PartitionRefMut},
            storage2,
        },
        segments::{IggyIndexesMut, IggyMessagesBatchMut, IggyMessagesBatchSet, storage::Storage},
    },
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
) -> impl FnOnce(&mut Partitions) -> Vec<partition2::Partition> {
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
    partition: partition2::Partition,
) -> impl FnOnce(&mut Partitions) -> partitions::ContainerId {
    move |partitions| partitions.insert(partition)
}

pub fn purge_partitions_mem() -> impl FnOnce(&mut Partitions) {
    |partitions| {
        partitions.with_components(|components| {
            let (_root, _stats, _deduplicator, _offset, _consumer_offset, _cg_offset, _log) =
                components.into_components();
            // TODO: Implement purge logic
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

pub fn get_consumer_group_member_offset(
    id: usize,
) -> impl FnOnce(ComponentsById<PartitionRef>) -> Option<ConsumerOffsetInfo> {
    move |(root, _, _, current_offset, _, offsets, _)| {
        offsets.pin().get(&id).map(|item| ConsumerOffsetInfo {
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
) -> impl FnOnce(ComponentsById<PartitionRef>) -> Result<(), IggyError> {
    move |(.., offsets, _, _)| {
        offsets
            .pin()
            .remove(&id)
            .map(|_| ())
            .ok_or_else(|| IggyError::ConsumerOffsetNotFound(id))
    }
}

pub fn persist_consumer_offset_to_disk(
    shard_id: u16,
    id: usize,
) -> impl AsyncFnOnce(ComponentsById<PartitionRef>) -> Result<(), IggyError> {
    async move |(.., offsets, _, _)| {
        let hdl = offsets.pin();
        let item = hdl
            .get(&id)
            .expect("persist_consumer_offset_to_disk: offset not found");
        let offset = item.offset.load(Ordering::Relaxed);
        storage2::persist_offset(shard_id, &item.path, offset).await
    }
}

pub fn delete_consumer_offset_from_disk(
    shard_id: u16,
    id: usize,
) -> impl AsyncFnOnce(ComponentsById<PartitionRef>) -> Result<(), IggyError> {
    async move |(.., offsets, _, _)| {
        let hdl = offsets.pin();
        let item = hdl
            .get(&id)
            .expect("delete_consumer_offset_from_disk: offset not found");
        let path = &item.path;
        storage2::delete_persisted_offset(shard_id, path).await
    }
}

pub fn store_consumer_group_member_offset(
    id: usize,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    offset: u64,
    config: &SystemConfig,
) -> impl FnOnce(ComponentsById<PartitionRef>) {
    move |(.., offsets, _)| {
        let hdl = offsets.pin();
        let item = hdl.get_or_insert(
            id,
            ConsumerOffset::default_for_consumer_group(
                id as u32,
                &config.get_consumer_group_offsets_path(stream_id, topic_id, partition_id),
            ),
        );
        item.offset.store(offset, Ordering::Relaxed);
    }
}

pub fn delete_consumer_group_member_offset(
    id: usize,
) -> impl FnOnce(ComponentsById<PartitionRef>) -> Result<(), IggyError> {
    move |(.., offsets, _)| {
        offsets
            .pin()
            .remove(&id)
            .map(|_| ())
            .ok_or_else(|| IggyError::ConsumerOffsetNotFound(id))
    }
}

pub fn persist_consumer_group_member_offset_to_disk(
    shard_id: u16,
    id: usize,
) -> impl AsyncFnOnce(ComponentsById<PartitionRef>) -> Result<(), IggyError> {
    async move |(.., offsets, _)| {
        let hdl = offsets.pin();
        let item = hdl
            .get(&id)
            .expect("persist_consumer_group_member_offset_to_disk: offset not found");
        let offset = item.offset.load(Ordering::Relaxed);
        storage2::persist_offset(shard_id, &item.path, offset).await
    }
}

pub fn delete_consumer_group_member_offset_from_disk(
    shard_id: u16,
    id: usize,
) -> impl AsyncFnOnce(ComponentsById<PartitionRef>) -> Result<(), IggyError> {
    async move |(.., offsets, _)| {
        let hdl = offsets.pin();
        let item = hdl
            .get(&id)
            .expect("delete_consumer_group_member_offset_from_disk: offset not found");
        let path = &item.path;
        storage2::delete_persisted_offset(shard_id, path).await
    }
}

pub fn purge_segments_mem() -> impl FnOnce(&mut Partitions) {
    |_partitions| {
        // TODO:
        //partitions.segments_mut()
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

pub async fn get_messages_by_offset(
    storage: &Storage,
    journal: &MemoryMessageJournal,
    index: &Option<IggyIndexesMut>,
    offset: u64,
    end_offset: u64,
    count: u32,
    segment_start_offset: u64,
) -> Result<IggyMessagesBatchSet, IggyError> {
    if count == 0 {
        return Ok(IggyMessagesBatchSet::default());
    }

    // Case 0: Accumulator is empty, so all messages have to be on disk
    if journal.is_empty() {
        return load_messages_from_disk_by_offset(
            storage,
            index,
            offset,
            count,
            segment_start_offset,
        )
        .await;
    }

    let journal_first_offset = journal.inner().base_offset;
    let journal_last_offset = journal.inner().current_offset;

    // Case 1: All messages are in accumulator buffer
    if offset >= journal_first_offset && end_offset <= journal_last_offset {
        return Ok(journal.get(|batches| batches.get_by_offset(offset, count)));
    }

    // Case 2: All messages are on disk
    if end_offset < journal_first_offset {
        return load_messages_from_disk_by_offset(
            storage,
            index,
            offset,
            count,
            segment_start_offset,
        )
        .await;
    }

    // Case 3: Messages span disk and accumulator buffer boundary
    // Calculate how many messages we need from disk
    let disk_count = if offset < journal_first_offset {
        ((journal_first_offset - offset) as u32).min(count)
    } else {
        0
    };
    let mut combined_batch_set = IggyMessagesBatchSet::empty();

    // Load messages from disk if needed
    if disk_count > 0 {
        let disk_messages = load_messages_from_disk_by_offset(storage, index, offset, disk_count, segment_start_offset)
            .await
            .with_error_context(|error| {
                format!("Failed to load messages from disk, start offset: {offset}, count: {disk_count}, error: {error}")
            })?;

        if !disk_messages.is_empty() {
            combined_batch_set.add_batch_set(disk_messages);
        }
    }

    // Calculate how many more messages we need from the accumulator
    let remaining_count = count - combined_batch_set.count();

    if remaining_count > 0 {
        let accumulator_start_offset = std::cmp::max(offset, journal_first_offset);
        let accumulator_messages =
            journal.get(|batches| batches.get_by_offset(accumulator_start_offset, remaining_count));
        if !accumulator_messages.is_empty() {
            combined_batch_set.add_batch_set(accumulator_messages);
        }
    }

    Ok(combined_batch_set)
}

async fn load_messages_from_disk_by_offset(
    storage: &Storage,
    index: &Option<IggyIndexesMut>,
    start_offset: u64,
    count: u32,
    segment_start_offset: u64,
) -> Result<IggyMessagesBatchSet, IggyError> {
    // Convert start_offset to relative offset within the segment
    let relative_start_offset = (start_offset - segment_start_offset) as u32;

    // Load indexes first
    let indexes_to_read = if let Some(indexes) = index {
        if !indexes.is_empty() {
            indexes.slice_by_offset(relative_start_offset, count)
        } else {
            storage
                .index_reader
                .as_ref()
                .expect("Index reader not initialized")
                .load_from_disk_by_offset(relative_start_offset, count)
                .await?
        }
    } else {
        storage
            .index_reader
            .as_ref()
            .expect("Index reader not initialized")
            .load_from_disk_by_offset(relative_start_offset, count)
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
        .with_error_context(|error| format!("Failed to load messages from disk: {error}"))?;

    batch
        .validate_checksums_and_offsets(start_offset)
        .with_error_context(|error| {
            format!("Failed to validate messages read from disk! error: {error}")
        })?;

    Ok(IggyMessagesBatchSet::from(batch))
}

pub fn get_messages_by_offset_range(
    offset: u64,
    count: u32,
    range: std::ops::Range<usize>,
) -> impl AsyncFnOnce(ComponentsById<PartitionRef>) -> Result<IggyMessagesBatchSet, IggyError> {
    async move |(.., log)| -> Result<IggyMessagesBatchSet, IggyError> {
        let segments = log.segments().iter();
        let storages = log.storages().iter();
        let journal = log.journal();
        let indexes = log.indexes().iter();

        let mut remaining_count = count;
        let mut batches = IggyMessagesBatchSet::empty();
        let mut current_offset = offset;

        for (segment, storage, index) in segments
            .zip(storages)
            .zip(indexes)
            .map(|((a, b), c)| (a, b, c))
            .skip(range.start)
            .take(range.end - range.start)
        {
            let start_offset = if current_offset < segment.start_offset {
                segment.start_offset
            } else {
                current_offset
            };

            let mut end_offset = start_offset + (remaining_count - 1) as u64;
            if end_offset > segment.end_offset {
                end_offset = segment.end_offset;
            }

            // Calculate the actual count to request from this segment
            let count: u32 = ((end_offset - start_offset + 1) as u32).min(remaining_count);

            let messages = get_messages_by_offset(
                storage,
                journal,
                index,
                start_offset,
                end_offset,
                count,
                segment.start_offset,
            )
            .await?;

            let messages_count = messages.count();
            if messages_count == 0 {
                current_offset = segment.end_offset + 1;
                continue;
            }

            remaining_count = remaining_count.saturating_sub(messages_count);

            if let Some(last_offset) = messages.last_offset() {
                current_offset = last_offset + 1;
            } else if messages_count > 0 {
                current_offset += messages_count as u64;
            }

            batches.add_batch_set(messages);

            if remaining_count == 0 {
                break;
            }
        }
        Ok(batches)
    }
}

pub fn get_segment_range_by_offset(
    offset: u64,
) -> impl FnOnce(ComponentsById<PartitionRef>) -> std::ops::Range<usize> {
    move |(.., log)| {
        let start = log
            .segments()
            .iter()
            .rposition(|segment| segment.start_offset <= offset)
            .expect("get_segment_range_by_offset: start segment not found");
        let end = log.segments().len();
        start..end
    }
}

pub fn get_segment_range_by_timestamp(
    timestamp: u64,
) -> impl FnOnce(ComponentsById<PartitionRef>) -> Result<std::ops::Range<usize>, IggyError> {
    move |(.., log)| -> Result<std::ops::Range<usize>, IggyError> {
        let segments = log.segments();
        let start = log
            .segments()
            .iter()
            .enumerate()
            .filter(|(_, segment)| segment.end_timestamp >= timestamp)
            .map(|(index, _)| index)
            .next()
            .ok_or(IggyError::TimestampOutOfRange(timestamp))?;
        let end = log.segments().len();
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
        .with_error_context(|error| {
            format!("Failed to load messages from disk by timestamp: {error}")
        })?;

    Ok(IggyMessagesBatchSet::from(batch))
}

pub async fn get_messages_by_timestamp(
    storage: &Storage,
    journal: &MemoryMessageJournal,
    index: &Option<IggyIndexesMut>,
    timestamp: u64,
    count: u32,
) -> Result<IggyMessagesBatchSet, IggyError> {
    if count == 0 {
        return Ok(IggyMessagesBatchSet::default());
    }

    // Case 0: Accumulator is empty, so all messages have to be on disk
    if journal.is_empty() {
        return load_messages_from_disk_by_timestamp(storage, index, timestamp, count).await;
    }

    let journal_first_timestamp = journal.inner().first_timestamp;
    let journal_last_timestamp = journal.inner().end_timestamp;

    // Case 1: All messages are in accumulator buffer
    if timestamp > journal_last_timestamp {
        return Ok(IggyMessagesBatchSet::empty());
    }

    if timestamp >= journal_first_timestamp {
        return Ok(journal.get(|batches| batches.get_by_timestamp(timestamp, count)));
    }

    // Case 2: All messages are on disk (timestamp is before journal's first timestamp)
    let disk_messages =
        load_messages_from_disk_by_timestamp(storage, index, timestamp, count).await?;

    if disk_messages.count() >= count {
        return Ok(disk_messages);
    }

    // Case 3: Messages span disk and accumulator buffer boundary
    let remaining_count = count - disk_messages.count();
    let journal_messages =
        journal.get(|batches| batches.get_by_timestamp(timestamp, remaining_count));

    let mut combined_batch_set = disk_messages;
    if !journal_messages.is_empty() {
        combined_batch_set.add_batch_set(journal_messages);
    }
    return Ok(combined_batch_set);
}

pub fn get_messages_by_timestamp_range(
    timestamp: u64,
    count: u32,
    range: std::ops::Range<usize>,
) -> impl AsyncFnOnce(ComponentsById<PartitionRef>) -> Result<IggyMessagesBatchSet, IggyError> {
    async move |(.., log)| -> Result<IggyMessagesBatchSet, IggyError> {
        let segments = log.segments().iter();
        let storages = log.storages().iter();
        let journal = log.journal();
        let indexes = log.indexes().iter();

        let mut remaining_count = count;
        let mut batches = IggyMessagesBatchSet::empty();

        for (segment, storage, index) in segments
            .zip(storages)
            .zip(indexes)
            .map(|((a, b), c)| (a, b, c))
            .skip(range.start)
            .take(range.end - range.start)
        {
            if remaining_count == 0 {
                break;
            }

            // Skip segments that end before our timestamp
            if segment.end_timestamp < timestamp {
                continue;
            }

            let messages =
                get_messages_by_timestamp(storage, journal, index, timestamp, remaining_count)
                    .await?;

            let messages_count = messages.count();
            if messages_count == 0 {
                continue;
            }

            remaining_count = remaining_count.saturating_sub(messages_count);
            batches.add_batch_set(messages);

            if remaining_count == 0 {
                break;
            }
        }

        Ok(batches)
    }
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

pub fn deduplicate_messages(
    current_offset: u64,
    current_position: u32,
    batch: &mut IggyMessagesBatchMut,
) -> impl AsyncFnOnce(ComponentsById<PartitionRef>) {
    async move |(.., deduplicator, _, _, _, log)| {
        let segment = log.active_segment();
        batch
            .prepare_for_persistence(
                segment.start_offset,
                current_offset,
                current_position,
                deduplicator.as_ref(),
            )
            .await;
    }
}

pub fn append_to_journal(
    shard_id: u16,
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

        segment.end_timestamp = batch.last_timestamp().unwrap();
        segment.end_offset = batch.last_offset().unwrap();

        let (journal_messages_count, journal_size) = log.journal_mut().append(shard_id, batch)?;

        stats.increment_messages_count(batch_messages_count as u64);
        stats.increment_size_bytes(batch_messages_size as u64);

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

        Ok((journal_messages_count, journal_size))
    }
}

pub fn commit_journal() -> impl FnOnce(ComponentsById<PartitionRefMut>) -> IggyMessagesBatchSet {
    |(.., log)| {
        let batches = log.journal_mut().commit();
        log.ensure_indexes();
        batches.append_indexes_to(log.active_indexes_mut().unwrap());
        batches
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

pub fn persist_batch(
    shard_id: u16,
    stream_id: &Identifier,
    topic_id: &Identifier,
    partition_id: usize,
    batches: IggyMessagesBatchSet,
    reason: String,
) -> impl AsyncFnOnce(ComponentsById<PartitionRef>) -> Result<(IggyByteSize, u32), IggyError> {
    async move |(.., log)| {
        shard_trace!(
            shard_id,
            "Persisting messages on disk for stream ID: {}, topic ID: {}, partition ID: {} because {}...",
            stream_id,
            topic_id,
            partition_id,
            reason
        );

        let batch_count = batches.count();
        let batch_size = batches.size();

        let storage = log.active_storage();
        let saved = storage
            .messages_writer
            .as_ref()
            .expect("Messages writer not initialized")
            .save_batch_set(batches)
            .await
            .with_error_context(|error| {
                let segment = log.active_segment();
                format!(
                    "Failed to save batch of {batch_count} messages \
                                    ({batch_size} bytes) to {segment}. {error}",
                )
            })?;

        let unsaved_indexes_slice = log.active_indexes().unwrap().unsaved_slice();
        let len = unsaved_indexes_slice.len();
        storage
            .index_writer
            .as_ref()
            .expect("Index writer not initialized")
            .save_indexes(unsaved_indexes_slice)
            .await
            .with_error_context(|error| {
                let segment = log.active_segment();
                format!("Failed to save index of {len} indexes to {segment}. {error}",)
            })?;

        shard_trace!(
            shard_id,
            "Persisted {} messages on disk for stream ID: {}, topic ID: {}, for partition with ID: {}, total bytes written: {}.",
            batch_count,
            stream_id,
            topic_id,
            partition_id,
            saved
        );

        Ok((saved, batch_count))
    }
}

pub fn update_index_and_increment_stats(
    saved: IggyByteSize,
    batch_count: u32,
    config: &SystemConfig,
) -> impl FnOnce(ComponentsById<PartitionRefMut>) {
    move |(_, stats, .., log)| {
        let segment = log.active_segment_mut();
        segment.size += saved.as_bytes_u32();
        log.active_indexes_mut().unwrap().mark_saved();
        if config.segment.cache_indexes == CacheIndexesConfig::None {
            log.active_indexes_mut().unwrap().clear();
        }
        stats.increment_size_bytes(saved.as_bytes_u64());
        stats.increment_messages_count(batch_count as u64);
    }
}
