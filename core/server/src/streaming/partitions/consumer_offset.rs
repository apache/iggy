use std::sync::atomic::AtomicU64;

use iggy_common::ConsumerKind;

#[derive(Debug)]
pub struct ConsumerOffset {
    pub kind: ConsumerKind,
    pub consumer_id: u32,
    pub offset: AtomicU64,
    pub path: String,
}

impl Clone for ConsumerOffset {
    fn clone(&self) -> Self {
        Self {
            kind: self.kind,
            consumer_id: self.consumer_id,
            offset: AtomicU64::new(0),
            path: self.path.clone(),
        }
    }
}

impl ConsumerOffset {
    pub fn default_for_consumer(consumer_id: u32, path: &str) -> Self {
        Self {
            kind: ConsumerKind::Consumer,
            consumer_id,
            offset: AtomicU64::new(0),
            path: format!("{path}/{consumer_id}"),
        }
    }

    pub fn default_for_consumer_group(consumer_id: u32, path: &str) -> Self {
        Self {
            kind: ConsumerKind::ConsumerGroup,
            consumer_id,
            offset: AtomicU64::new(0),
            path: format!("{path}/{consumer_id}"),
        }
    }

    pub fn new(kind: ConsumerKind, consumer_id: u32, offset: u64, path: String) -> Self {
        Self {
            kind,
            consumer_id,
            offset: AtomicU64::new(offset),
            path,
        }
    }
}
