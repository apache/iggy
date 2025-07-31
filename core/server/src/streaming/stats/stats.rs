use std::sync::{
    Arc,
    atomic::{AtomicU32, AtomicU64},
};

#[derive(Default, Debug)]
pub struct StreamStats {
    size_bytes: AtomicU64,
    messages_count: AtomicU64,
    segments_count: AtomicU32,
}

impl StreamStats {
    pub fn new() -> Self {
        Self {
            size_bytes: AtomicU64::new(0),
            messages_count: AtomicU64::new(0),
            segments_count: AtomicU32::new(0),
        }
    }
}

#[derive(Default, Debug)]
pub struct TopicStats {
    parent: Arc<StreamStats>,
    size_bytes: AtomicU64,
    messages_count: AtomicU64,
}

impl TopicStats {
    pub fn new(parent: Arc<StreamStats>) -> Self {
        Self {
            parent,
            size_bytes: AtomicU64::new(0),
            messages_count: AtomicU64::new(0),
        }
    }
}

#[derive(Default, Debug)]
pub struct PartitionStats {
    parent: Arc<TopicStats>,
    messages_count: AtomicU64,
    size_bytes: AtomicU64,
}

impl PartitionStats {
    pub fn new(parent: Arc<TopicStats>) -> Self {
        Self {
            parent,
            messages_count: AtomicU64::new(0),
            size_bytes: AtomicU64::new(0),
        }
    }
}
