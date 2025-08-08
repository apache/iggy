use crate::streaming::stats::stats::PartitionStats;
use iggy_common::IggyTimestamp;
use slab::Slab;
use std::sync::{Arc, atomic::AtomicU64};

#[derive(Debug)]
pub struct SharedPartition {
    pub id: usize,
    pub stats: Arc<PartitionStats>,
    pub current_offset: Arc<AtomicU64>,
    pub consumer_group_offset: Arc<()>,
    pub consumer_offset: Arc<()>,
}

#[derive(Default, Debug)]
pub struct Partition {
    id: usize,
    created_at: IggyTimestamp,
    should_increment_offset: bool,
}

impl Partition {
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

    pub fn insert_into(self, container: &mut Slab<Self>) -> usize {
        let idx = container.insert(self);
        let partition = &mut container[idx];
        partition.id = idx;
        idx
    }
}
