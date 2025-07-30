use iggy_common::Identifier;
use std::cell::RefCell;

use crate::{
    slab::{IndexedSlab, partitions::Partitions},
    streaming::{partitions::partition2, topics::topic2},
};

const CAPACITY: usize = 1024;

pub struct Topics {
    container: RefCell<IndexedSlab<topic2::Topic>>,
    stats: (),
}

impl Default for Topics {
    fn default() -> Self {
        Self {
            container: RefCell::new(IndexedSlab::with_capacity(CAPACITY)),
            stats: (),
        }
    }
}

impl Topics {
    pub fn init() -> Self {
        Self {
            container: RefCell::new(IndexedSlab::with_capacity(CAPACITY)),
            stats: (),
        }
    }

    pub async fn with_async(&self, f: impl AsyncFnOnce(&IndexedSlab<topic2::Topic>)) {
        let container = self.container.borrow();
        f(&container).await;
    }

    pub fn with(&self, f: impl FnOnce(&IndexedSlab<topic2::Topic>)) {
        let container = self.container.borrow();
        f(&container);
    }

    pub fn with_mut(&self, f: impl FnOnce(&mut IndexedSlab<topic2::Topic>)) {
        let mut container = self.container.borrow_mut();
        f(&mut container);
    }

    pub fn with_topic_by_id(&self, id: &Identifier, f: impl FnOnce(&topic2::Topic)) {
        self.with(|topics| {
            let topic = match id.kind {
                iggy_common::IdKind::Numeric => {
                    let id = id.get_u32_value().unwrap() as usize;
                    &topics[id]
                }
                iggy_common::IdKind::String => {
                    let key = id.get_string_value().unwrap();
                    unsafe { topics.get_by_key_unchecked(&key) }
                }
            };
            f(topic)
        });
    }

    pub fn with_topic_by_id_mut(&self, id: &Identifier, mut f: impl FnMut(&mut topic2::Topic)) {
        self.with_mut(|topics| {
            let topic = match id.kind {
                iggy_common::IdKind::Numeric => {
                    let id = id.get_u32_value().unwrap() as usize;
                    &mut topics[id]
                }
                iggy_common::IdKind::String => {
                    let key = id.get_string_value().unwrap();
                    unsafe { topics.get_by_key_mut_unchecked(&key) }
                }
            };
            f(topic)
        });
    }

    pub fn with_partitions(&self, topic_id: &Identifier, f: impl FnOnce(&Partitions)) {
        self.with(|topics| {
            let topic = match topic_id.kind {
                iggy_common::IdKind::Numeric => {
                    let id = topic_id.get_u32_value().unwrap() as usize;
                    &topics[id]
                }
                iggy_common::IdKind::String => {
                    let key = topic_id.get_string_value().unwrap();
                    unsafe { topics.get_by_key_unchecked(&key) }
                }
            };
            f(topic.partitions());
        })
    }

    pub fn with_partition_by_id(
        &self,
        id: &Identifier,
        partition_id: usize,
        f: impl FnOnce(&partition2::Partition),
    ) {
        self.with_partitions(id, |partitions| {
            partitions.with_partition_id(partition_id, f);
        });
    }
}
