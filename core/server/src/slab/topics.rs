use ahash::AHashMap;
use iggy_common::Identifier;
use slab::Slab;
use std::{cell::RefCell, sync::Arc};

use crate::{
    slab::{Keyed, partitions::Partitions},
    streaming::{partitions::partition2, stats::stats::TopicStats, topics::topic2},
};

const CAPACITY: usize = 1024;

#[derive(Debug)]
pub struct Topics {
    index: RefCell<AHashMap<<topic2::Topic as Keyed>::Key, usize>>,
    container: RefCell<Slab<topic2::Topic>>,
    stats: RefCell<Slab<Arc<TopicStats>>>,
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
                tracing::error!("Getting index for topic: {key}, index: {:?}", self.index.borrow());
                *self.index.borrow().get(&key).expect("Topic not found")
            }
        }
    }

    pub fn len(&self) -> usize {
        self.container.borrow().len()
    }

    pub async fn with_async<T>(&self, f: impl AsyncFnOnce(&Slab<topic2::Topic>) -> T) -> T {
        let container = self.container.borrow();
        f(&container).await
    }

    pub fn with<T>(&self, f: impl FnOnce(&Slab<topic2::Topic>) -> T) -> T {
        let container = self.container.borrow();
        f(&container)
    }

    pub fn with_mut<T>(&self, f: impl FnOnce(&mut Slab<topic2::Topic>) -> T) -> T {
        let mut container = self.container.borrow_mut();
        f(&mut container)
    }

    pub fn with_mut_index<T>(
        &self,
        f: impl FnOnce(&mut AHashMap<<topic2::Topic as Keyed>::Key, usize>) -> T,
    ) -> T {
        let mut index = self.index.borrow_mut();
        f(&mut index)
    }

    pub async fn with_topic_by_id_async<T>(
        &self,
        id: &Identifier,
        f: impl AsyncFnOnce(&topic2::Topic) -> T,
    ) -> T {
        let id = self.get_index(id);
        self.with_async(async |topics| topics[id].invoke_async(f).await)
            .await
    }

    pub fn with_topic_stats_by_id<T>(
        &self,
        id: &Identifier,
        f: impl FnOnce(Arc<TopicStats>) -> T,
    ) -> T {
        let topic_id = self.with_topic_by_id(id, |topic| topic.id());
        self.with_stats(|stats| f(stats[topic_id].clone()))
    }

    pub fn with_topic_by_id<T>(&self, id: &Identifier, f: impl FnOnce(&topic2::Topic) -> T) -> T {
        let id = self.get_index(id);
        self.with(|topics| topics[id].invoke(f))
    }

    pub fn with_topic_by_id_mut<T>(
        &self,
        id: &Identifier,
        f: impl FnOnce(&mut topic2::Topic) -> T,
    ) -> T {
        let id = self.get_index(id);
        self.with_mut(|topics| topics[id].invoke_mut(f))
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

    pub fn with_stats<T>(&self, f: impl FnOnce(&Slab<Arc<TopicStats>>) -> T) -> T {
        let stats = self.stats.borrow();
        f(&stats)
    }

    pub fn with_stats_mut<T>(&self, f: impl FnOnce(&mut Slab<Arc<TopicStats>>) -> T) -> T {
        let mut stats = self.stats.borrow_mut();
        f(&mut stats)
    }
}
