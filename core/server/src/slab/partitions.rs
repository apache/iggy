use crate::{
    slab::traits_ext::{
        Borrow, ComponentsById, Delete, EntityComponentSystem, EntityComponentSystemMut, Insert,
        IntoComponents,
    },
    streaming::{
        deduplication::message_deduplicator::MessageDeduplicator,
        partitions::{
            consumer_offset,
            partition2::{self, Partition, PartitionRef, PartitionRefMut},
        },
        segments,
        stats::stats::PartitionStats,
    },
};
use slab::Slab;
use std::sync::{Arc, atomic::AtomicU64};

// TODO: This could be upper limit of partitions per topic, use that value to validate instead of whathever this thing is in `common` crate.
pub const PARTITIONS_CAPACITY: usize = 16384;
const SEGMENTS_CAPACITY: usize = 1024;
pub type ContainerId = usize;

#[derive(Debug, Clone)]
pub struct Partitions {
    root: Slab<partition2::PartitionRoot>,
    stats: Slab<Arc<PartitionStats>>,
    segments: Slab<Vec<segments::Segment2>>,
    message_deduplicator: Slab<Option<MessageDeduplicator>>,
    offset: Slab<Arc<AtomicU64>>,

    consumer_offset: Slab<Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>>,
    consumer_group_offset: Slab<Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>>,
}

impl Insert for Partitions {
    type Idx = ContainerId;
    type Item = Partition;

    fn insert(&mut self, item: Self::Item) -> Self::Idx {
        let (root, stats, deduplicator, offset, consumer_offset, consumer_group_offset) =
            item.into_components();

        let entity_id = self.root.insert(root);
        let id = self.stats.insert(stats);
        assert_eq!(
            entity_id, id,
            "partition_insert: id mismatch when creating stats"
        );
        let id = self.segments.insert(Vec::with_capacity(SEGMENTS_CAPACITY));
        assert_eq!(
            entity_id, id,
            "partition_insert: id mismatch when creating segments"
        );
        let id = self.message_deduplicator.insert(deduplicator);
        assert_eq!(
            entity_id, id,
            "partition_insert: id mismatch when creating message_deduplicator"
        );
        let id = self.offset.insert(offset);
        assert_eq!(
            entity_id, id,
            "partition_insert: id mismatch when creating offset"
        );
        let id = self.consumer_offset.insert(consumer_offset);
        assert_eq!(
            entity_id, id,
            "partition_insert: id mismatch when creating consumer_offset"
        );
        let id = self.consumer_group_offset.insert(consumer_group_offset);
        assert_eq!(
            entity_id, id,
            "partition_insert: id mismatch when creating consumer_group_offset"
        );
        entity_id
    }
}

impl Delete for Partitions {
    type Idx = ContainerId;
    type Item = Partition;

    fn delete(&mut self, id: Self::Idx) -> Self::Item {
        todo!()
    }
}

//TODO: those from impls could use a macro aswell.
impl<'a> From<&'a Partitions> for PartitionRef<'a> {
    fn from(value: &'a Partitions) -> Self {
        PartitionRef::new(
            &value.root,
            &value.stats,
            &value.message_deduplicator,
            &value.offset,
            &value.consumer_offset,
            &value.consumer_group_offset,
        )
    }
}

impl<'a> From<&'a mut Partitions> for PartitionRefMut<'a> {
    fn from(value: &'a mut Partitions) -> Self {
        PartitionRefMut::new(
            &mut value.root,
            &mut value.stats,
            &mut value.message_deduplicator,
            &mut value.offset,
            &mut value.consumer_offset,
            &mut value.consumer_group_offset,
        )
    }
}

impl EntityComponentSystem<Borrow> for Partitions {
    type Idx = ContainerId;
    type Entity = Partition;
    type EntityComponents<'a> = PartitionRef<'a>;

    fn with_components<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityComponents<'a>) -> O,
    {
        f(self.into())
    }

    fn with_components_async<O, F>(&self, f: F) -> impl Future<Output = O>
    where
        F: for<'a> AsyncFnOnce(Self::EntityComponents<'a>) -> O,
    {
        f(self.into())
    }
}

impl EntityComponentSystemMut for Partitions {
    type EntityComponentsMut<'a> = PartitionRefMut<'a>;

    fn with_components_mut<O, F>(&mut self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityComponentsMut<'a>) -> O,
    {
        f(self.into())
    }

    fn with_components_by_id_mut<O, F>(&mut self, id: Self::Idx, f: F) -> O
    where
        F: for<'a> FnOnce(ComponentsById<'a, Self::EntityComponentsMut<'a>>) -> O,
    {
        self.with_components_mut(|components| f(components.into_components_by_id(id)))
    }
}

impl Default for Partitions {
    fn default() -> Self {
        Self {
            root: Slab::with_capacity(PARTITIONS_CAPACITY),
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
    pub fn len(&self) -> usize {
        self.root.len()
    }

    pub fn with_partition_by_id<T>(
        &self,
        id: ContainerId,
        f: impl FnOnce(ComponentsById<PartitionRef>) -> T,
    ) -> T {
        self.with_components_by_id(id, |components| f(components))
    }

    pub fn with_partition_by_id_mut<T>(
        &mut self,
        id: ContainerId,
        f: impl FnOnce(ComponentsById<PartitionRefMut>) -> T,
    ) -> T {
        self.with_components_by_id_mut(id, |components| f(components))
    }

    pub fn with_partition_by_id_async<T>(
        &self,
        id: ContainerId,
        f: impl AsyncFnOnce(ComponentsById<PartitionRef>) -> T,
    ) -> impl Future<Output = T> {
        self.with_components_by_id_async(id, async move |components| f(components).await)
    }
}
