use ahash::AHashMap;
use iggy_common::Identifier;
use slab::Slab;
use std::{cell::RefCell, sync::Arc};

use crate::{
    slab::{partitions::Partitions, Keyed},
    streaming::{partitions::partition2, stats::stats::TopicStats, topics::topic2::{self, TopicRef}},
};

const CAPACITY: usize = 1024;
pub type SlabId = usize;

#[derive(Debug)]
pub struct Topics {
    index: RefCell<AHashMap<<topic2::TopicRoot as Keyed>::Key, usize>>,
    root: RefCell<Slab<topic2::TopicRoot>>,
    stats: RefCell<Slab<Arc<TopicStats>>>,
}

impl<'a> From<&'a Topics> for topic2::TopicRef<'a> {
    fn from(value: &'a Topics) -> Self {
        let xd = value.root.borrow();
        TopicRef::new()
    }
}

impl Topics {
    pub fn init() -> Self {
        Self {
            index: RefCell::new(AHashMap::with_capacity(CAPACITY)),
            container: RefCell::new(Slab::with_capacity(CAPACITY)),
            stats: RefCell::new(Slab::with_capacity(CAPACITY)),
        }
    }

    pub fn exists(&self, id: &Identifier) -> bool {
        match id.kind {
            iggy_common::IdKind::Numeric => {
                let id = id.get_u32_value().unwrap() as usize;
                self.container.borrow().contains(id)
            }
            iggy_common::IdKind::String => {
                let key = id.get_string_value().unwrap();
                self.index.borrow().contains_key(&key)
            }
        }
    }

    fn get_index(&self, id: &Identifier) -> usize {
        match id.kind {
            iggy_common::IdKind::Numeric => id.get_u32_value().unwrap() as usize,
            iggy_common::IdKind::String => {
                let key = id.get_string_value().unwrap();
                *self.index.borrow().get(&key).expect("Topic not found")
            }
        }
    }

    pub fn len(&self) -> usize {
        self.container.borrow().len()
    }

    pub fn with_partitions(&self, topic_id: &Identifier, f: impl FnOnce(&Partitions)) {
        self.with_topic_by_id(topic_id, |topic| f(topic.partitions()));
    }

    pub fn with_partitions_mut(&self, topic_id: &Identifier, f: impl FnOnce(&mut Partitions)) {
        self.with_topic_by_id_mut(topic_id, |topic| f(topic.partitions_mut()));
    }
}
