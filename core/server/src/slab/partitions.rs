use crate::{
    slab::traits_ext::{
        Borrow, Components, EntityComponentSystem, IndexComponents, IntoComponents,
    },
    streaming::{
        deduplication::message_deduplicator::MessageDeduplicator, partitions::partition2, segments,
        stats::stats::PartitionStats,
    },
};
use slab::Slab;
use std::{
    ops::Index,
    sync::{Arc, atomic::AtomicU64},
};

// TODO: This could be upper limit of partitions per topic, use that value to validate instead of whathever this thing is in `common` crate.
pub const PARTITIONS_CAPACITY: usize = 16384;
type Idx = usize;

#[derive(Debug)]
pub struct Partitions {
    container: Slab<partition2::Partition>,
    stats: Slab<Arc<PartitionStats>>,
    segments: Slab<Vec<segments::Segment2>>,
    message_deduplicators: Slab<Option<MessageDeduplicator>>,
    partition_offsets: Slab<Arc<AtomicU64>>,
}

pub struct Part {
    partition: partition2::Partition,
}

impl IntoComponents for Part {
    type Components = (partition2::Partition,);

    fn into_components(self) -> Self::Components {
        (self.partition,)
    }
}

pub struct PartRef<'a> {
    partition: &'a Slab<partition2::Partition>,
}

pub struct PartItemRef<'a> {
    partition: &'a partition2::Partition,
}

impl<'a> IntoComponents for PartRef<'a> {
    type Components = (&'a Slab<partition2::Partition>,);

    fn into_components(self) -> Self::Components {
        (self.partition,)
    }
}

impl<'a> IndexComponents<Idx> for PartRef<'a> {
    type Output = (&'a partition2::Partition,);

    fn index(&self, index: Idx) -> Self::Output {
        (&self.partition[index],)
    }
}

impl EntityComponentSystem<Idx, Borrow> for Partitions {
    type Entity = Part;
    type EntityRef<'a> = PartRef<'a>;

    fn with<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityRef<'a>) -> O,
    {
        todo!()
    }

    async fn with_async<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Components<Self::EntityRef<'a>>) -> O,
    {
        todo!()
    }
}

impl Default for Partitions {
    fn default() -> Self {
        Self {
            container: Slab::with_capacity(PARTITIONS_CAPACITY),
            stats: Slab::with_capacity(PARTITIONS_CAPACITY),
            segments: Slab::with_capacity(PARTITIONS_CAPACITY),
            message_deduplicators: Slab::with_capacity(PARTITIONS_CAPACITY),
            partition_offsets: Slab::with_capacity(PARTITIONS_CAPACITY),
        }
    }
}

pub struct PartitionRef<'partitions> {
    partition: &'partitions Slab<partition2::Partition>,
}

impl<'partitions> Decompose for PartitionRef<'partitions> {
    type Target = &'partitions Slab<partition2::Partition>;

    fn decompose(self) -> Self::Target {
        self.partition
    }
}

impl Project for Partitions {
    type View<'me>
        = PartitionRef<'me>
    where
        Self: 'me;

    fn project(&self) -> Self::View<'_> {
        PartitionRef {
            partition: &self.container,
        }
    }
}

pub struct PartitionRefMut<'partitions> {
    partition: &'partitions mut Slab<partition2::Partition>,
}

impl<'partitions> Decompose for PartitionRefMut<'partitions> {
    type Target = &'partitions mut Slab<partition2::Partition>;

    fn decompose(self) -> Self::Target {
        self.partition
    }
}

impl ProjectMut for Partitions {
    type ViewMut<'me>
        = PartitionRefMut<'me>
    where
        Self: 'me;

    fn project_mut(&mut self) -> Self::ViewMut<'_> {
        PartitionRefMut {
            partition: &mut self.container,
        }
    }
}

impl Partitions {
    pub fn with_stats<T>(&self, f: impl FnOnce(&Slab<Arc<PartitionStats>>) -> T) -> T {
        let stats = &self.stats;
        f(stats)
    }

    pub fn with_stats_mut<T>(&mut self, f: impl FnOnce(&mut Slab<Arc<PartitionStats>>) -> T) -> T {
        let mut stats = &mut self.stats;
        f(&mut stats)
    }

    pub fn with_message_deduplicators<T>(
        &self,
        f: impl FnOnce(&Slab<Option<MessageDeduplicator>>) -> T,
    ) -> T {
        let message_deduplicators = &self.message_deduplicators;
        f(message_deduplicators)
    }

    pub fn with_message_deduplicators_mut<T>(
        &mut self,
        f: impl FnOnce(&mut Slab<Option<MessageDeduplicator>>) -> T,
    ) -> T {
        let mut message_deduplicators = &mut self.message_deduplicators;
        f(&mut message_deduplicators)
    }

    pub fn with_partition_offsets<T>(&self, f: impl FnOnce(&Slab<Arc<AtomicU64>>) -> T) -> T {
        let partition_offsets = &self.partition_offsets;
        f(partition_offsets)
    }

    pub fn with_partition_offsets_mut<T>(
        &mut self,
        f: impl FnOnce(&mut Slab<Arc<AtomicU64>>) -> T,
    ) -> T {
        let mut partition_offsets = &mut self.partition_offsets;
        f(&mut partition_offsets)
    }

    pub fn with_partition_id<T>(
        &self,
        partition_id: usize,
        f: impl FnOnce(&partition2::Partition) -> T,
    ) -> T {
        self.with(|partitions| {
            let partition = &partitions[partition_id];
            f(partition)
        })
    }

    pub fn with_partition_by_id_mut<T>(
        &mut self,
        partition_id: usize,
        f: impl FnOnce(&mut partition2::Partition) -> T,
    ) -> T {
        self.with_mut(|partitions| {
            let partition = &mut partitions[partition_id];
            f(partition)
        })
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
