use slab::Slab;
use std::sync::Arc;

use crate::streaming::{partitions::partition2, segments, stats::stats::PartitionStats};

// TODO: This could be upper limit of partitions per topic, use that value to validate instead of whathever this thing is in `common` crate.
const CAPACITY: usize = 16384;

#[derive(Debug)]
pub struct Partitions {
    container: Slab<partition2::Partition>,
    stats: Slab<Arc<PartitionStats>>,
    segments: Slab<Vec<segments::Segment2>>,
}

impl Default for Partitions {
    fn default() -> Self {
        Self {
            container: Slab::with_capacity(CAPACITY),
            stats: Slab::with_capacity(CAPACITY),
            segments: Slab::with_capacity(CAPACITY),
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

    pub async fn with_async<T>(&self, f: impl AsyncFnOnce(&Slab<partition2::Partition>) -> T) -> T {
        let container = &self.container;
        f(&container).await
    }

    pub fn with<T>(&self, f: impl FnOnce(&Slab<partition2::Partition>) -> T) -> T {
        let container = &self.container;
        f(&container)
    }

    pub fn with_mut<T>(&mut self, f: impl FnOnce(&mut Slab<partition2::Partition>) -> T) -> T {
        let mut container = &mut self.container;
        f(&mut container)
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
