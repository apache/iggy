use crate::{slab::Keyed, streaming::topics::consumer_group2};
use ahash::AHashMap;
use arcshift::ArcShift;
use iggy_common::Identifier;
use slab::Slab;
use std::sync::{Arc, atomic::AtomicUsize};

const CAPACITY: usize = 1024;

#[derive(Debug, Clone)]
pub struct ConsumerGroups {
    index: AHashMap<<consumer_group2::ConsumerGroup as Keyed>::Key, usize>,
    container: Slab<consumer_group2::ConsumerGroup>,
}

impl ConsumerGroups {
    pub fn with<T>(&self, f: impl FnOnce(&Slab<consumer_group2::ConsumerGroup>) -> T) -> T {
        f(&self.container)
    }

    pub fn with_mut<T>(
        &mut self,
        f: impl FnOnce(&mut Slab<consumer_group2::ConsumerGroup>) -> T,
    ) -> T {
        f(&mut self.container)
    }

    pub fn with_index<T>(
        &self,
        f: impl FnOnce(&AHashMap<<consumer_group2::ConsumerGroup as Keyed>::Key, usize>) -> T,
    ) -> T {
        f(&self.index)
    }

    pub fn exists(&self, id: &Identifier) -> bool {
        match id.kind {
            iggy_common::IdKind::Numeric => {
                let id = id.get_u32_value().unwrap() as usize;
                self.container.contains(id)
            }
            iggy_common::IdKind::String => {
                let key = id.get_string_value().unwrap();
                self.index.contains_key(&key)
            }
        }
    }
}

impl Default for ConsumerGroups {
    fn default() -> Self {
        Self {
            index: AHashMap::with_capacity(CAPACITY),
            container: Slab::with_capacity(CAPACITY),
        }
    }
}
