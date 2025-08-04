use crate::{slab::IndexedSlab, streaming::topics::consumer_group2};
use ahash::AHashMap;
use arcshift::ArcShift;
use iggy_common::Identifier;
use slab::Slab;
use std::sync::{Arc, atomic::AtomicUsize};

const CAPACITY: usize = 1024;

#[derive(Debug)]
pub struct ConsumerGroups {
    container: IndexedSlab<consumer_group2::ConsumerGroup>,
}

impl ConsumerGroups {
    pub fn with<T>(&self, f: impl FnOnce(&IndexedSlab<consumer_group2::ConsumerGroup>) -> T) -> T {
        f(&self.container)
    }

    pub fn with_mut<T>(
        &mut self,
        f: impl FnOnce(&mut IndexedSlab<consumer_group2::ConsumerGroup>) -> T,
    ) -> T {
        f(&mut self.container)
    }

    pub fn exists(&self, id: &Identifier) -> bool {
        match id.kind {
            iggy_common::IdKind::Numeric => {
                let id = id.get_u32_value().unwrap() as usize;
                self.container.slab.contains(id)
            }
            iggy_common::IdKind::String => {
                let key = id.get_string_value().unwrap();
                self.container.index.contains_key(&key)
            }
        }
    }
}

impl Default for ConsumerGroups {
    fn default() -> Self {
        Self {
            container: IndexedSlab::with_capacity(CAPACITY),
        }
    }
}
