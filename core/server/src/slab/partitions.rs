use std::cell::RefCell;

use iggy_common::Partition;
use slab::Slab;

use crate::streaming::{partitions::partition2, segments};

// TODO: This could be upper limit of partitions per topic, use that value to validate instead of whathever this thing is in `common` crate.
const CAPACITY: usize = 16384;

pub struct Partitions {
    container: Slab<partition2::Partition>,
    storage: (),
    stats: (),
    segments: Slab<Vec<segments::Segment2>>,
}

impl Default for Partitions {
    fn default() -> Self {
        Self {
            container: Slab::with_capacity(CAPACITY),
            storage: (),
            stats: (),
            segments: Slab::with_capacity(CAPACITY),
        }
    }
}

impl Partitions {
    pub async fn with_async(&self, f: impl AsyncFnOnce(&Slab<partition2::Partition>)) {
        let container = &self.container;
        f(&container).await;
    }

    pub fn with(&self, f: impl FnOnce(&Slab<partition2::Partition>)) {
        let container = &self.container;
        f(&container);
    }

    pub fn with_mut(&mut self, f: impl FnOnce(&mut Slab<partition2::Partition>)) {
        let mut container = &mut self.container;
        f(&mut container);
    }

    pub fn with_partition_id(&self, partition_id: usize, f: impl FnOnce(&partition2::Partition)) {
        self.with(|partitions| {
            if let Some(partition) = partitions.get(partition_id) {
                f(partition);
            }
        });
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
            if let Some(segment) = segments.iter().find(|s| s.id == segment_id) {
                f(segment);
            }
        });
    }
}
