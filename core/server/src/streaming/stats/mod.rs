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
    pub fn increment_size_bytes(&self, size_bytes: u64) {
        self.size_bytes.fetch_add(size_bytes, Ordering::AcqRel);
    }

    pub fn increment_messages_count(&self, messages_count: u64) {
        self.messages_count
            .fetch_add(messages_count, Ordering::AcqRel);
    }

    pub fn increment_segments_count(&self, segments_count: u32) {
        self.segments_count
            .fetch_add(segments_count, Ordering::AcqRel);
    }

    pub fn decrement_size_bytes(&self, size_bytes: u64) {
        self.size_bytes.fetch_sub(size_bytes, Ordering::AcqRel);
    }

    pub fn decrement_messages_count(&self, messages_count: u64) {
        self.messages_count
            .fetch_sub(messages_count, Ordering::AcqRel);
    }

    pub fn decrement_segments_count(&self, segments_count: u32) {
        self.segments_count
            .fetch_sub(segments_count, Ordering::AcqRel);
    }

    pub fn size_bytes_inconsistent(&self) -> u64 {
        self.size_bytes.load(Ordering::Relaxed)
    }

    pub fn messages_count_inconsistent(&self) -> u64 {
        self.messages_count.load(Ordering::Relaxed)
    }

    pub fn segments_count_inconsistent(&self) -> u32 {
        self.segments_count.load(Ordering::Relaxed)
    }

    pub fn zero_out_size_bytes(&self) {
        self.size_bytes.store(0, Ordering::Relaxed);
    }

    pub fn zero_out_messages_count(&self) {
        self.messages_count.store(0, Ordering::Relaxed);
    }

    pub fn zero_out_segments_count(&self) {
        self.segments_count.store(0, Ordering::Relaxed);
    }

    pub fn zero_out_all(&self) {
        self.zero_out_size_bytes();
        self.zero_out_messages_count();
        self.zero_out_segments_count();
    }
}

#[derive(Default, Debug)]
pub struct TopicStats {
    parent: Arc<StreamStats>,
    size_bytes: AtomicU64,
    messages_count: AtomicU64,
    segments_count: AtomicU32,
}

impl TopicStats {
    pub fn new(parent: Arc<StreamStats>) -> Self {
        Self {
            parent,
            size_bytes: AtomicU64::new(0),
            messages_count: AtomicU64::new(0),
            segments_count: AtomicU32::new(0),
        }
    }

    pub fn parent(&self) -> Arc<StreamStats> {
        self.parent.clone()
    }

    pub fn increment_parent_size_bytes(&self, size_bytes: u64) {
        self.parent.increment_size_bytes(size_bytes);
    }

    pub fn increment_parent_messages_count(&self, messages_count: u64) {
        self.parent.increment_messages_count(messages_count);
    }

    pub fn increment_parent_segments_count(&self, segments_count: u32) {
        self.parent.increment_segments_count(segments_count);
    }

    pub fn increment_size_bytes(&self, size_bytes: u64) {
        self.size_bytes.fetch_add(size_bytes, Ordering::AcqRel);
        self.increment_parent_size_bytes(size_bytes);
    }

    pub fn increment_messages_count(&self, messages_count: u64) {
        self.messages_count
            .fetch_add(messages_count, Ordering::AcqRel);
        self.increment_parent_messages_count(messages_count);
    }

    pub fn increment_segments_count(&self, segments_count: u32) {
        self.segments_count
            .fetch_add(segments_count, Ordering::AcqRel);
        self.increment_parent_segments_count(segments_count);
    }

    pub fn decrement_parent_size_bytes(&self, size_bytes: u64) {
        self.parent.decrement_size_bytes(size_bytes);
    }

    pub fn decrement_parent_messages_count(&self, messages_count: u64) {
        self.parent.decrement_messages_count(messages_count);
    }

    pub fn decrement_parent_segments_count(&self, segments_count: u32) {
        self.parent.decrement_segments_count(segments_count);
    }

    pub fn decrement_size_bytes(&self, size_bytes: u64) {
        self.size_bytes.fetch_sub(size_bytes, Ordering::AcqRel);
        self.decrement_parent_size_bytes(size_bytes);
    }

    pub fn decrement_messages_count(&self, messages_count: u64) {
        self.messages_count
            .fetch_sub(messages_count, Ordering::AcqRel);
        self.decrement_parent_messages_count(messages_count);
    }

    pub fn decrement_segments_count(&self, segments_count: u32) {
        self.segments_count
            .fetch_sub(segments_count, Ordering::AcqRel);
        self.decrement_parent_segments_count(segments_count);
    }

    pub fn size_bytes_inconsistent(&self) -> u64 {
        self.size_bytes.load(Ordering::Relaxed)
    }

    pub fn messages_count_inconsistent(&self) -> u64 {
        self.messages_count.load(Ordering::Relaxed)
    }

    pub fn segments_count_inconsistent(&self) -> u32 {
        self.segments_count.load(Ordering::Relaxed)
    }

    pub fn zero_out_parent_size_bytes(&self) {
        self.parent.zero_out_size_bytes();
    }

    pub fn zero_out_parent_messages_count(&self) {
        self.parent.zero_out_messages_count();
    }

    pub fn zero_out_parent_segments_count(&self) {
        self.parent.zero_out_segments_count();
    }

    pub fn zero_out_parent_all(&self) {
        self.parent.zero_out_all();
    }

    pub fn zero_out_size_bytes(&self) {
        self.size_bytes.store(0, Ordering::Relaxed);
        self.zero_out_parent_size_bytes();
    }

    pub fn zero_out_messages_count(&self) {
        self.messages_count.store(0, Ordering::Relaxed);
        self.zero_out_parent_messages_count();
    }

    pub fn zero_out_segments_count(&self) {
        self.segments_count.store(0, Ordering::Relaxed);
        self.zero_out_parent_segments_count();
    }

    pub fn zero_out_all(&self) {
        self.zero_out_size_bytes();
        self.zero_out_messages_count();
        self.zero_out_segments_count();
    }
}

#[derive(Default, Debug)]
pub struct PartitionStats {
    parent: Arc<TopicStats>,
    messages_count: AtomicU64,
    size_bytes: AtomicU64,
    segments_count: AtomicU32,
}

impl PartitionStats {
    pub fn new(parent_stats: Arc<TopicStats>) -> Self {
        Self {
            parent: parent_stats,
            messages_count: AtomicU64::new(0),
            size_bytes: AtomicU64::new(0),
            segments_count: AtomicU32::new(0),
        }
    }

    pub fn parent(&self) -> Arc<TopicStats> {
        self.parent.clone()
    }

    pub fn increment_size_bytes(&self, size_bytes: u64) {
        self.size_bytes.fetch_add(size_bytes, Ordering::AcqRel);
        self.increment_parent_size_bytes(size_bytes);
    }

    pub fn increment_messages_count(&self, messages_count: u64) {
        self.messages_count
            .fetch_add(messages_count, Ordering::AcqRel);
        self.increment_parent_messages_count(messages_count);
    }

    pub fn increment_segments_count(&self, segments_count: u32) {
        self.segments_count
            .fetch_add(segments_count, Ordering::AcqRel);
        self.increment_parent_segments_count(segments_count);
    }

    pub fn increment_parent_size_bytes(&self, size_bytes: u64) {
        self.parent.increment_size_bytes(size_bytes);
    }

    pub fn increment_parent_messages_count(&self, messages_count: u64) {
        self.parent.increment_messages_count(messages_count);
    }

    pub fn increment_parent_segments_count(&self, segments_count: u32) {
        self.parent.increment_segments_count(segments_count);
    }

    pub fn decrement_size_bytes(&self, size_bytes: u64) {
        self.size_bytes.fetch_sub(size_bytes, Ordering::AcqRel);
        self.decrement_parent_size_bytes(size_bytes);
    }

    pub fn decrement_messages_count(&self, messages_count: u64) {
        self.messages_count
            .fetch_sub(messages_count, Ordering::AcqRel);
        self.decrement_parent_messages_count(messages_count);
    }

    pub fn decrement_segments_count(&self, segments_count: u32) {
        self.segments_count
            .fetch_sub(segments_count, Ordering::AcqRel);
        self.decrement_parent_segments_count(segments_count);
    }

    pub fn decrement_parent_size_bytes(&self, size_bytes: u64) {
        self.parent.decrement_size_bytes(size_bytes);
    }

    pub fn decrement_parent_messages_count(&self, messages_count: u64) {
        self.parent.decrement_messages_count(messages_count);
    }

    pub fn decrement_parent_segments_count(&self, segments_count: u32) {
        self.parent.decrement_segments_count(segments_count);
    }

    pub fn size_bytes_inconsistent(&self) -> u64 {
        self.size_bytes.load(Ordering::Relaxed)
    }

    pub fn messages_count_inconsistent(&self) -> u64 {
        self.messages_count.load(Ordering::Relaxed)
    }

    pub fn segments_count_inconsistent(&self) -> u32 {
        self.segments_count.load(Ordering::Relaxed)
    }

    pub fn zero_out_parent_size_bytes(&self) {
        self.parent.zero_out_size_bytes();
    }

    pub fn zero_out_parent_messages_count(&self) {
        self.parent.zero_out_messages_count();
    }

    pub fn zero_out_parent_segments_count(&self) {
        self.parent.zero_out_segments_count();
    }

    pub fn zero_out_parent_all(&self) {
        self.parent.zero_out_all();
    }

    pub fn zero_out_size_bytes(&self) {
        self.size_bytes.store(0, Ordering::Relaxed);
        self.zero_out_parent_size_bytes();
    }

    pub fn zero_out_messages_count(&self) {
        self.messages_count.store(0, Ordering::Relaxed);
        self.zero_out_parent_messages_count();
    }

    pub fn zero_out_segments_count(&self) {
        self.segments_count.store(0, Ordering::Relaxed);
        self.zero_out_parent_segments_count();
    }

    pub fn zero_out_all(&self) {
        self.zero_out_size_bytes();
        self.zero_out_messages_count();
        self.zero_out_segments_count();
        self.zero_out_parent_all();
    }
}
