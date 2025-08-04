use crate::slab::{IndexedSlab, Keyed};
use ahash::AHashMap;
use arcshift::ArcShift;
use slab::Slab;
use std::{
    sync::{Arc, atomic::AtomicUsize},
    task::Context,
};

pub const MEMBERS_CAPACITY: usize = 128;

#[derive(Default, Debug)]
pub struct ConsumerGroup {
    id: usize,
    name: String,
    members: ArcShift<Slab<Member>>,
}

impl ConsumerGroup {
    pub fn new(name: String, members: ArcShift<Slab<Member>>) -> Self {
        ConsumerGroup {
            id: 0,
            name,
            members,
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn insert_into(self, container: &mut IndexedSlab<Self>) -> usize {
        let idx = container.insert(self);
        let group = &mut container[idx];
        group.id = idx;
        idx
    }
}

#[derive(Debug)]
pub struct Member {
    id: usize,
    client_id: u32,
    partitions: Vec<u32>,
    current_partition_idx: AtomicUsize,
}

impl Member {
    pub fn id(&self) -> usize {
        self.id
    }
}

impl Keyed for ConsumerGroup {
    type Key = String;

    fn key(&self) -> &Self::Key {
        &self.name
    }
}
