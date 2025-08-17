use std::sync::{
    Arc,
    atomic::{AtomicU32, AtomicU64, Ordering},
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

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes.load(Ordering::SeqCst)
    }

    pub fn messages_count(&self) -> u64 {
        self.messages_count.load(Ordering::SeqCst)
    }

    pub fn segments_count(&self) -> u32 {
        self.segments_count.load(Ordering::SeqCst)
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
    pub fn new(parent_stats: Arc<TopicStats>) -> Self {
        Self {
            parent: parent_stats,
            messages_count: AtomicU64::new(0),
            size_bytes: AtomicU64::new(0),
        }
    }
}
