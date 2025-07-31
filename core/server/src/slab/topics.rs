use iggy_common::Identifier;
use slab::Slab;
use std::{cell::RefCell, sync::Arc};

use crate::{
    slab::{IndexedSlab, partitions::Partitions},
    streaming::{partitions::partition2, stats::stats::TopicStats, topics::topic2},
};

const CAPACITY: usize = 1024;

pub struct Topics {
    container: RefCell<IndexedSlab<topic2::Topic>>,
    stats: RefCell<Slab<Arc<TopicStats>>>,
}

impl Topics {
    pub fn init() -> Self {
        Self {
            container: RefCell::new(IndexedSlab::with_capacity(CAPACITY)),
            stats: RefCell::new(Slab::with_capacity(CAPACITY)),
        }
    }

    pub fn len(&self) -> usize {
        self.container.borrow().len()
    }

    pub fn exists(&self, key: &String) -> bool {
        self.container.borrow().exists(key)
    }

    pub async fn with_async(&self, f: impl AsyncFnOnce(&IndexedSlab<topic2::Topic>)) {
        let container = self.container.borrow();
        f(&container).await;
    }

    pub fn with_stats<T>(&self, f: impl FnOnce(&Slab<Arc<TopicStats>>) -> T) -> T {
        let stats = self.stats.borrow();
        f(&stats)
    }

    pub fn with_stats_mut<T>(&self, f: impl FnOnce(&mut Slab<Arc<TopicStats>>) -> T) -> T {
        let mut stats = self.stats.borrow_mut();
        f(&mut stats)
    }

    pub fn with<T>(&self, f: impl FnOnce(&IndexedSlab<topic2::Topic>) -> T) -> T {
        let container = self.container.borrow();
        f(&container)
    }

    pub fn with_mut<T>(&self, f: impl FnOnce(&mut IndexedSlab<topic2::Topic>) -> T) -> T {
        let mut container = self.container.borrow_mut();
        f(&mut container)
    }

    pub fn with_topic_by_id<T>(&self, id: &Identifier, f: impl FnOnce(&topic2::Topic) -> T) -> T {
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
        })
    }

    pub fn with_topic_by_id_mut<T>(
        &self,
        id: &Identifier,
        f: impl FnOnce(&mut topic2::Topic) -> T,
    ) -> T {
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
        })
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
