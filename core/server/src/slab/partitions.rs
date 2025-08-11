use crate::{
    slab::traits_ext::{
        Borrow, Components, Delete, EntityComponentSystem, EntityMarker, IndexComponents, Insert,
        IntoComponents,
    },
    streaming::{
        deduplication::message_deduplicator::MessageDeduplicator,
        partitions::{
            consumer_offset,
            partition::ConsumerOffset,
            partition2::{self, Partition, PartitionRef},
        },
        segments,
        stats::stats::PartitionStats,
    },
};
use ahash::AHashMap;
use slab::Slab;
use std::sync::{Arc, atomic::AtomicU64};

// TODO: This could be upper limit of partitions per topic, use that value to validate instead of whathever this thing is in `common` crate.
pub const PARTITIONS_CAPACITY: usize = 16384;
pub type SlabId = usize;

struct PartitionOffset {
    offset: u64,
}

#[derive(Debug)]
pub struct Partitions {
    info: Slab<partition2::PartitionInfo>,
    stats: Slab<Arc<PartitionStats>>,
    segments: Slab<Vec<segments::Segment2>>,
    message_deduplicator: Slab<Option<MessageDeduplicator>>,
    offset: Slab<Arc<AtomicU64>>,

    consumer_offset: Slab<Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>>,
    consumer_group_offset: Slab<Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>>,
}

impl<'a> From<&'a Partitions> for PartitionRef<'a> {
    fn from(value: &'a Partitions) -> Self {
        PartitionRef::new(
            &value.info,
            &value.stats,
            &value.message_deduplicator,
            &value.offset,
            &value.consumer_offset,
            &value.consumer_group_offset,
        )
    }
}

impl Insert<SlabId> for Partitions {
    type Item = Partition;

    fn insert(&mut self, item: Self::Item) -> SlabId {
        let (
            info,
            stats,
            message_deduplicator,
            offset,
            consumer_offset,
            consumer_group_offset,
        ) = item.into_components();

        let id = self.info.insert(info);
        let info = &mut self.info[id];
        info.update_id(id);
        self.stats.insert(stats);
        self.message_deduplicator.insert(message_deduplicator);
        self.offset.insert(offset);
        self.consumer_offset.insert(consumer_offset);
        self.consumer_group_offset.insert(consumer_group_offset);
        id
    }
}

impl Delete<SlabId> for Partitions {
    type Item = Partition;

    fn delete(&mut self, id: SlabId) -> Self::Item {
        todo!()
    }
}

impl EntityComponentSystem<SlabId, Borrow> for Partitions {
    type Entity = Partition;
    type EntityRef<'a> = PartitionRef<'a>;

    fn with<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityRef<'a>) -> O,
    {
        f(self.into())
    }

    async fn with_async<O, F>(&self, f: F) -> O
    where
        F: for<'a> AsyncFnOnce(Self::EntityRef<'a>) -> O,
    {
        f(self.into()).await
    }
}

impl Default for Partitions {
    fn default() -> Self {
        Self {
            info: Slab::with_capacity(PARTITIONS_CAPACITY),
            stats: Slab::with_capacity(PARTITIONS_CAPACITY),
            segments: Slab::with_capacity(PARTITIONS_CAPACITY),
            message_deduplicator: Slab::with_capacity(PARTITIONS_CAPACITY),
            offset: Slab::with_capacity(PARTITIONS_CAPACITY),
            consumer_offset: Slab::with_capacity(PARTITIONS_CAPACITY),
            consumer_group_offset: Slab::with_capacity(PARTITIONS_CAPACITY),
        }
    }
}

impl Partitions {
    pub fn count(&self) -> usize {
        self.info.len()
    }

    pub fn with_stats<T>(&self, f: impl FnOnce(&Slab<Arc<PartitionStats>>) -> T) -> T {
        let stats = &self.stats;
        f(stats)
    }

    pub fn with_stats_mut<T>(&mut self, f: impl FnOnce(&mut Slab<Arc<PartitionStats>>) -> T) -> T {
        let mut stats = &mut self.stats;
        f(&mut stats)
    }

    pub fn with_segments(&self, partition_id: usize, f: impl FnOnce(&Vec<segments::Segment2>)) {
        let segments = &self.segments[partition_id];
        f(segments);
    }

    pub fn with_segment_id(
        &self,
        partition_id: usize,
        segment_id: usize,
        f: impl FnOnce(&segments::Segment2),
    ) {
        self.with_segments(partition_id, |segments| {
            // we could binary search for that segment technically, but this is fine for now.
            if let Some(segment) = segments.iter().find(|s| s.id == segment_id) {
                f(segment);
            }
        });
    }
}
