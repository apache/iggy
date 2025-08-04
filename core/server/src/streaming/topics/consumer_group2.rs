use std::sync::{atomic::AtomicUsize, Arc};
use ahash::AHashMap;
use arcshift::ArcShift;
use slab::Slab;
use crate::slab::Keyed;

const CAPACITY: usize = 128;

#[derive(Default, Debug)]
pub struct ConsumerGroup {
    id: usize,
    name: String,
    membership: AHashMap<u32, usize>,
    members: ArcShift<Slab<Member>>
}

impl ConsumerGroup {
    pub fn new(id: usize, name: String) -> Self {
        ConsumerGroup {
            id,
            name,
            members: ArcShift::new(Slab::with_capacity(CAPACITY)),
            membership: AHashMap::with_capacity(CAPACITY),
        }
    }
}

#[derive(Debug)]
struct Member {
    id: usize,
    partitions: Vec<u32>,
    current_partition_idx: Arc<AtomicUsize>,
}


impl Keyed for ConsumerGroup {
    type Key = String;

    fn key(&self) -> &Self::Key {
        &self.name
    }
}
