use crate::{
    configs::system::SystemConfig,
    slab::{
        partitions::{self, Partitions},
        traits_ext::{EntityMarker, IntoComponents, IntoComponentsById},
    },
    streaming::{
        deduplication::message_deduplicator::{self, MessageDeduplicator},
        partitions::consumer_offset,
        stats::stats::PartitionStats,
    },
};
use iggy_common::IggyTimestamp;
use slab::Slab;
use std::sync::{Arc, atomic::AtomicU64};

// TODO: Let's create type aliases for the fields that have maps for consumer/consumer_group offsets,
// it's really difficult to distinguish between them when using the returned tuple from `into_components`.
#[derive(Debug)]
pub struct Partition {
    root: PartitionRoot,
    stats: Arc<PartitionStats>,
    message_deduplicator: Option<MessageDeduplicator>,
    offset: Arc<AtomicU64>,
    consumer_offset: Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>,
    consumer_group_offset: Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>,
}

impl Partition {
    pub fn new(
        created_at: IggyTimestamp,
        should_increment_offset: bool,
        stats: Arc<PartitionStats>,
        message_deduplicator: Option<MessageDeduplicator>,
        offset: Arc<AtomicU64>,
        consumer_offset: Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>,
        consumer_group_offset: Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>,
    ) -> Self {
        let root = PartitionRoot::new(created_at, should_increment_offset);
        Self {
            root,
            stats,
            message_deduplicator,
            offset,
            consumer_offset,
            consumer_group_offset,
        }
    }

    pub fn stats(&self) -> &PartitionStats {
        &self.stats
    }
}

impl Clone for Partition {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            stats: Arc::clone(&self.stats),
            message_deduplicator: self.message_deduplicator.clone(),
            offset: Arc::clone(&self.offset),
            consumer_offset: Arc::clone(&self.consumer_offset),
            consumer_group_offset: Arc::clone(&self.consumer_group_offset),
        }
    }
}

impl EntityMarker for Partition {
    type Idx = partitions::ContainerId;

    fn id(&self) -> Self::Idx {
        self.root.id
    }

    fn update_id(&mut self, id: Self::Idx) {
        self.root.id = id;
    }
}

impl IntoComponents for Partition {
    type Components = (
        PartitionRoot,
        Arc<PartitionStats>,
        Option<MessageDeduplicator>,
        Arc<AtomicU64>,
        Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>,
        Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>,
    );

    fn into_components(self) -> Self::Components {
        (
            self.root,
            self.stats,
            self.message_deduplicator,
            self.offset,
            self.consumer_offset,
            self.consumer_group_offset,
        )
    }
}

#[derive(Default, Debug, Clone)]
pub struct PartitionRoot {
    id: usize,
    created_at: IggyTimestamp,
    should_increment_offset: bool,
}

impl PartitionRoot {
    pub fn new(created_at: IggyTimestamp, should_increment_offset: bool) -> Self {
        Self {
            id: 0,
            created_at,
            should_increment_offset,
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn update_id(&mut self, id: usize) {
        self.id = id;
    }

    pub fn created_at(&self) -> IggyTimestamp {
        self.created_at
    }
}

// TODO: Create a macro to impl those PartitionRef/PartitionRefMut structs and it's traits.
pub struct PartitionRef<'a> {
    root: &'a Slab<PartitionRoot>,
    stats: &'a Slab<Arc<PartitionStats>>,
    message_deduplicator: &'a Slab<Option<MessageDeduplicator>>,
    offset: &'a Slab<Arc<AtomicU64>>,
    consumer_offset: &'a Slab<Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>>,
    consumer_group_offset: &'a Slab<Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>>,
}

impl<'a> PartitionRef<'a> {
    pub fn new(
        root: &'a Slab<PartitionRoot>,
        stats: &'a Slab<Arc<PartitionStats>>,
        message_deduplicator: &'a Slab<Option<MessageDeduplicator>>,
        offset: &'a Slab<Arc<AtomicU64>>,
        consumer_offset: &'a Slab<Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>>,
        consumer_group_offset: &'a Slab<
            Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>,
        >,
    ) -> Self {
        Self {
            root,
            stats,
            message_deduplicator,
            offset,
            consumer_offset,
            consumer_group_offset,
        }
    }
}

impl<'a> IntoComponents for PartitionRef<'a> {
    type Components = (
        &'a Slab<PartitionRoot>,
        &'a Slab<Arc<PartitionStats>>,
        &'a Slab<Option<MessageDeduplicator>>,
        &'a Slab<Arc<AtomicU64>>,
        &'a Slab<Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>>,
        &'a Slab<Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>>,
    );

    fn into_components(self) -> Self::Components {
        (
            self.root,
            self.stats,
            self.message_deduplicator,
            self.offset,
            self.consumer_offset,
            self.consumer_group_offset,
        )
    }
}

impl<'a> IntoComponentsById for PartitionRef<'a> {
    type Idx = partitions::ContainerId;
    type Output = (
        &'a PartitionRoot,
        &'a Arc<PartitionStats>,
        &'a Option<MessageDeduplicator>,
        &'a Arc<AtomicU64>,
        &'a Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>,
        &'a Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>,
    );

    fn into_components_by_id(self, index: Self::Idx) -> Self::Output {
        (
            &self.root[index],
            &self.stats[index],
            &self.message_deduplicator[index],
            &self.offset[index],
            &self.consumer_offset[index],
            &self.consumer_group_offset[index],
        )
    }
}

pub struct PartitionRefMut<'a> {
    root: &'a mut Slab<PartitionRoot>,
    stats: &'a mut Slab<Arc<PartitionStats>>,
    message_deduplicator: &'a mut Slab<Option<MessageDeduplicator>>,
    offset: &'a mut Slab<Arc<AtomicU64>>,
    consumer_offset: &'a mut Slab<Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>>,
    consumer_group_offset:
        &'a mut Slab<Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>>,
}

impl<'a> PartitionRefMut<'a> {
    pub fn new(
        root: &'a mut Slab<PartitionRoot>,
        stats: &'a mut Slab<Arc<PartitionStats>>,
        message_deduplicator: &'a mut Slab<Option<MessageDeduplicator>>,
        offset: &'a mut Slab<Arc<AtomicU64>>,
        consumer_offset: &'a mut Slab<Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>>,
        consumer_group_offset: &'a mut Slab<
            Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>,
        >,
    ) -> Self {
        Self {
            root,
            stats,
            message_deduplicator,
            offset,
            consumer_offset,
            consumer_group_offset,
        }
    }
}

impl<'a> IntoComponents for PartitionRefMut<'a> {
    type Components = (
        &'a mut Slab<PartitionRoot>,
        &'a mut Slab<Arc<PartitionStats>>,
        &'a mut Slab<Option<MessageDeduplicator>>,
        &'a mut Slab<Arc<AtomicU64>>,
        &'a mut Slab<Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>>,
        &'a mut Slab<Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>>,
    );

    fn into_components(self) -> Self::Components {
        (
            self.root,
            self.stats,
            self.message_deduplicator,
            self.offset,
            self.consumer_offset,
            self.consumer_group_offset,
        )
    }
}

impl<'a> IntoComponentsById for PartitionRefMut<'a> {
    type Idx = partitions::ContainerId;
    type Output = (
        &'a mut PartitionRoot,
        &'a mut Arc<PartitionStats>,
        &'a mut Option<MessageDeduplicator>,
        &'a mut Arc<AtomicU64>,
        &'a mut Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>,
        &'a mut Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>,
    );

    fn into_components_by_id(self, index: Self::Idx) -> Self::Output {
        (
            &mut self.root[index],
            &mut self.stats[index],
            &mut self.message_deduplicator[index],
            &mut self.offset[index],
            &mut self.consumer_offset[index],
            &mut self.consumer_group_offset[index],
        )
    }
}
