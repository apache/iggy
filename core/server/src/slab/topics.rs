use iggy_common::Identifier;
use slab::Slab;
use std::{cell::RefCell, sync::Arc};

use crate::{
    slab::{IndexedSlab, Keyed, partitions::Partitions},
    streaming::{partitions::partition2, stats::stats::TopicStats, topics::topic2},
};

const CAPACITY: usize = 1024;

#[derive(Debug)]
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

    pub fn exists(&self, id: &Identifier) -> bool {
        match id.kind {
            iggy_common::IdKind::Numeric => {
                let id = id.get_u32_value().unwrap() as usize;
                self.container.borrow().slab.contains(id)
            }
            iggy_common::IdKind::String => {
                let key = id.get_string_value().unwrap();
                self.container.borrow().index.contains_key(&key)
            }
        }
    }

    fn get_topic_ref<'topics>(
        id: &Identifier,
        slab: &'topics IndexedSlab<topic2::Topic>,
    ) -> &'topics topic2::Topic {
        match id.kind {
            iggy_common::IdKind::Numeric => {
                let idx = id.get_u32_value().unwrap() as usize;
                &slab[idx]
            }
            iggy_common::IdKind::String => {
                let key = id.get_string_value().unwrap();
                unsafe { slab.get_by_key_unchecked(&key) }
            }
        }
    }

    fn get_topic_mut<'topics>(
        id: &Identifier,
        slab: &'topics mut IndexedSlab<topic2::Topic>,
    ) -> &'topics mut topic2::Topic {
        match id.kind {
            iggy_common::IdKind::Numeric => {
                let idx = id.get_u32_value().unwrap() as usize;
                &mut slab[idx]
            }
            iggy_common::IdKind::String => {
                let key = id.get_string_value().unwrap();
                unsafe { slab.get_by_key_mut_unchecked(&key) }
            }
        }
    }

    pub fn len(&self) -> usize {
        self.container.borrow().len()
    }

    pub async fn with_async<T>(&self, f: impl AsyncFnOnce(&IndexedSlab<topic2::Topic>) -> T) -> T {
        let container = self.container.borrow();
        f(&container).await
    }

    pub fn with<T>(&self, f: impl FnOnce(&IndexedSlab<topic2::Topic>) -> T) -> T {
        let container = self.container.borrow();
        f(&container)
    }

    pub fn with_mut<T>(&self, f: impl FnOnce(&mut IndexedSlab<topic2::Topic>) -> T) -> T {
        let mut container = self.container.borrow_mut();
        f(&mut container)
    }

    pub async fn with_topic_by_id_async<T>(
        &self,
        id: &Identifier,
        f: impl AsyncFnOnce(&topic2::Topic) -> T,
    ) -> T {
        self.with_async(async |topics| Self::get_topic_ref(id, topics).invoke_async(f).await)
            .await
    }

    pub fn with_topic_by_id<T>(&self, id: &Identifier, f: impl FnOnce(&topic2::Topic) -> T) -> T {
        self.with(|topics| Self::get_topic_ref(id, topics).invoke(f))
    }

    pub fn with_topic_by_id_mut<T>(
        &self,
        id: &Identifier,
        f: impl FnOnce(&mut topic2::Topic) -> T,
    ) -> T {
        self.with_mut(|topics| Self::get_topic_mut(id, topics).invoke_mut(f))
    }

    pub fn with_partitions(&self, topic_id: &Identifier, f: impl FnOnce(&Partitions)) {
        self.with_topic_by_id(topic_id, |topic| f(topic.partitions()));
    }

    pub fn with_partitions_mut(&self, topic_id: &Identifier, f: impl FnOnce(&mut Partitions)) {
        self.with_topic_by_id_mut(topic_id, |topic| f(topic.partitions_mut()));
    }

    pub async fn with_partitions_async<T>(
        &self,
        topic_id: &Identifier,
        f: impl AsyncFnOnce(&Partitions) -> T,
    ) -> T {
        self.with_topic_by_id_async(topic_id, async |topic| f(topic.partitions()).await)
            .await
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

    pub fn with_stats<T>(&self, f: impl FnOnce(&Slab<Arc<TopicStats>>) -> T) -> T {
        let stats = self.stats.borrow();
        f(&stats)
    }

    pub fn with_stats_mut<T>(&self, f: impl FnOnce(&mut Slab<Arc<TopicStats>>) -> T) -> T {
        let mut stats = self.stats.borrow_mut();
        f(&mut stats)
    }
}
