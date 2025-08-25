use std::sync::atomic::Ordering;

use iggy_common::{ConsumerOffsetInfo, IggyError};

use crate::{
    configs::system::SystemConfig,
    slab::{
        partitions::{self, Partitions},
        traits_ext::{
            Components, ComponentsById, Delete, EntityComponentSystem, EntityMarker, Insert,
            IntoComponents,
        },
    },
    streaming::{
        deduplication::message_deduplicator::MessageDeduplicator,
        partitions::{
            consumer_offset::ConsumerOffset,
            partition2::{self, PartitionRef, PartitionRefMut},
            storage2,
        },
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
            let (root, stats, deduplicator, offset, consumer_offset, cg_offset) =
                components.into_components();
        })
    }
}

pub fn get_consumer_offset(
    id: usize,
) -> impl FnOnce(ComponentsById<PartitionRef>) -> Option<ConsumerOffsetInfo> {
    move |(root, _, _, current_offset, offsets, _)| {
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
    move |(root, _, _, current_offset, _, offsets)| {
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
    move |(.., offsets, _)| {
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
    move |(.., offsets, _)| {
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
    async move |(.., offsets)| {
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
    async move |(.., offsets, _)| {
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
    move |(.., offsets)| {
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
    move |(.., offsets)| {
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
    async move |(.., offsets)| {
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
    async move |(.., offsets)| {
        let hdl = offsets.pin();
        let item = hdl
            .get(&id)
            .expect("delete_consumer_group_member_offset_from_disk: offset not found");
        let path = &item.path;
        storage2::delete_persisted_offset(shard_id, path).await
    }
}

pub fn purge_segments_mem() -> impl FnOnce(&mut Partitions) {
    |partitions| {
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
