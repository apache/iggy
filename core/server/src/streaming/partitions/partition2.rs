use crate::{
    slab::{
        partitions::{Partitions, SlabId},
        traits_ext::{EntityMarker, IndexComponents, IntoComponents},
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

#[derive(Debug)]
pub struct Partition {
    info: PartitionInfo,
    stats: Arc<PartitionStats>,
    message_deduplicator: Option<MessageDeduplicator>,
    offset: Arc<AtomicU64>,
    consumer_offset: Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>,
    consumer_group_offset: Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>,
}

impl Partition {
    pub fn new(
        info: PartitionInfo,
        stats: Arc<PartitionStats>,
        message_deduplicator: Option<MessageDeduplicator>,
        offset: Arc<AtomicU64>,
        consumer_offset: Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>,
        consumer_group_offset: Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>,
    ) -> Self {
        Self {
            info,
            stats,
            message_deduplicator,
            offset,
            consumer_offset,
            consumer_group_offset,
        }
    }

    pub fn update_id(&mut self, id: usize) {
        self.info.id = id;
    }

    pub fn id(&self) -> usize {
        self.info.id
    }
}

impl Clone for Partition {
    fn clone(&self) -> Self {
        Self {
            info: self.info.clone(),
            stats: Arc::clone(&self.stats),
            message_deduplicator: self.message_deduplicator.clone(),
            offset: Arc::clone(&self.offset),
            consumer_offset: Arc::clone(&self.consumer_offset),
            consumer_group_offset: Arc::clone(&self.consumer_group_offset),
        }
    }
}

impl EntityMarker for Partition {}

impl IntoComponents for Partition {
    type Components = (
        PartitionInfo,
        Arc<PartitionStats>,
        Option<MessageDeduplicator>,
        Arc<AtomicU64>,
        Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>,
        Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>,
    );

    fn into_components(self) -> Self::Components {
        (
            self.info,
            self.stats,
            self.message_deduplicator,
            self.offset,
            self.consumer_offset,
            self.consumer_group_offset,
        )
    }
}

#[derive(Default, Debug, Clone)]
pub struct PartitionInfo {
    id: usize,
    created_at: IggyTimestamp,
    should_increment_offset: bool,
}

impl PartitionInfo {
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
}

// TODO: Probably move this to the `slab` module
pub struct PartitionRef<'a> {
    info: &'a Slab<PartitionInfo>,
    stats: &'a Slab<Arc<PartitionStats>>,
    message_deduplicator: &'a Slab<Option<MessageDeduplicator>>,
    offset: &'a Slab<Arc<AtomicU64>>,
    consumer_offset: &'a Slab<Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>>,
    consumer_group_offset: &'a Slab<Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>>,
}

impl<'a> PartitionRef<'a> {
    pub fn new(
        info: &'a Slab<PartitionInfo>,
        stats: &'a Slab<Arc<PartitionStats>>,
        message_deduplicator: &'a Slab<Option<MessageDeduplicator>>,
        offset: &'a Slab<Arc<AtomicU64>>,
        consumer_offset: &'a Slab<Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>>,
        consumer_group_offset: &'a Slab<
            Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>,
        >,
    ) -> Self {
        Self {
            info,
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
        &'a Slab<PartitionInfo>,
        &'a Slab<Arc<PartitionStats>>,
        &'a Slab<Option<MessageDeduplicator>>,
        &'a Slab<Arc<AtomicU64>>,
        &'a Slab<Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>>,
        &'a Slab<Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>>,
    );

    fn into_components(self) -> Self::Components {
        (
            self.info,
            self.stats,
            self.message_deduplicator,
            self.offset,
            self.consumer_offset,
            self.consumer_group_offset,
        )
    }
}

impl<'a> IndexComponents<SlabId> for PartitionRef<'a> {
    type Output = (
        &'a PartitionInfo,
        &'a Arc<PartitionStats>,
        &'a Option<MessageDeduplicator>,
        &'a Arc<AtomicU64>,
        &'a Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>,
        &'a Arc<papaya::HashMap<usize, consumer_offset::ConsumerOffset>>,
    );

    fn index(&self, index: SlabId) -> Self::Output {
        (
            &self.info[index],
            &self.stats[index],
            &self.message_deduplicator[index],
            &self.offset[index],
            &self.consumer_offset[index],
            &self.consumer_group_offset[index],
        )
    }
}
