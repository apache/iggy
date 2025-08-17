use std::sync::atomic::AtomicU64;

use iggy_common::ConsumerKind;

#[derive(Debug)]
pub struct ConsumerOffset {
    pub kind: ConsumerKind,
    pub consumer_id: u32,
    pub offset: AtomicU64,
    pub path: String,
}

impl ConsumerOffset {
    pub fn new(kind: ConsumerKind, consumer_id: u32, offset: u64, path: &str) -> ConsumerOffset {
        ConsumerOffset {
            kind,
            consumer_id,
            offset: AtomicU64::new(offset),
            path: format!("{path}/{consumer_id}"),
        }
    }
}
