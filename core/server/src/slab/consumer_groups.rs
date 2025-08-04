use crate::{slab::IndexedSlab, streaming::topics::consumer_group2};
use ahash::AHashMap;
use arcshift::ArcShift;
use slab::Slab;
use std::sync::{Arc, atomic::AtomicUsize};

const CAPACITY: usize = 1024;

#[derive(Debug)]
pub struct ConsumerGroups {
    container: IndexedSlab<consumer_group2::ConsumerGroup>,
}

impl Default for ConsumerGroups {
    fn default() -> Self {
        Self {
            container: IndexedSlab::with_capacity(CAPACITY),
        }
    }
}


