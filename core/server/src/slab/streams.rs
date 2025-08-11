use ahash::AHashMap;
use iggy_common::Identifier;
use slab::Slab;
use std::{cell::RefCell, sync::Arc};

use crate::{
    slab::{Keyed, partitions::Partitions, topics::Topics},
    streaming::{
        partitions::partition2, stats::stats::StreamStats, streams::stream2, topics::topic2,
    },
};

const CAPACITY: usize = 1024;

pub struct Streams {
    index: RefCell<AHashMap<<stream2::Stream as Keyed>::Key, usize>>,
    container: RefCell<Slab<stream2::Stream>>,
    stats: RefCell<Slab<Arc<StreamStats>>>,
}

impl Streams {
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
                *self.index.borrow().get(&key).expect("Stream not found")
            }
        }
    }

    pub fn with_stats<T>(&self, f: impl FnOnce(&Slab<Arc<StreamStats>>) -> T) -> T {
        let stats = self.stats.borrow();
        f(&stats)
    }

    pub fn with_stats_by_id<T>(
        &self,
        id: &Identifier,
        f: impl FnOnce(&Arc<StreamStats>) -> T,
    ) -> T {
        let stream_id = self.with_stream_by_id(id, |stream| stream.id());
        self.with_stats(|stats| {
            let stats = &stats[stream_id];
            f(stats)
        })
    }

    pub fn with_stats_mut<T>(&self, f: impl FnOnce(&mut Slab<Arc<StreamStats>>) -> T) -> T {
        let mut stats = self.stats.borrow_mut();
        f(&mut stats)
    }

    pub async fn with_async<T>(&self, f: impl AsyncFnOnce(&Slab<stream2::Stream>) -> T) -> T {
        let container = self.container.borrow();
        f(&container).await
    }

    pub fn with_index<T>(
        &self,
        f: impl FnOnce(&AHashMap<<stream2::Stream as Keyed>::Key, usize>) -> T,
    ) -> T {
        let index = self.index.borrow();
        f(&index)
    }

    pub fn with_index_mut<T>(
        &self,
        f: impl FnOnce(&mut AHashMap<<stream2::Stream as Keyed>::Key, usize>) -> T,
    ) -> T {
        let mut index = self.index.borrow_mut();
        f(&mut index)
    }

    pub fn with<T>(&self, f: impl FnOnce(&Slab<stream2::Stream>) -> T) -> T {
        let container = self.container.borrow();
        f(&container)
    }

    pub fn with_mut<T>(&self, f: impl FnOnce(&mut Slab<stream2::Stream>) -> T) -> T {
        let mut container = self.container.borrow_mut();
        f(&mut container)
    }

    pub fn with_stream_by_id<T>(
        &self,
        id: &Identifier,
        f: impl FnOnce(&stream2::Stream) -> T,
    ) -> T {
        let id = self.get_index(id);
        self.with(|streams| streams[id].invoke(f))
    }

    pub async fn with_stream_by_id_async<T>(
        &self,
        id: &Identifier,
        f: impl AsyncFnOnce(&stream2::Stream) -> T,
    ) -> T {
        let id = self.get_index(id);
        self.with_async(async |streams| streams[id].invoke_async(f).await)
            .await
    }

    pub fn with_stream_by_id_mut<T>(
        &self,
        id: &Identifier,
        f: impl FnOnce(&mut stream2::Stream) -> T,
    ) -> T {
        let id = self.get_index(id);
        self.with_mut(|streams| streams[id].invoke_mut(f))
    }

    pub fn with_topics<T>(&self, stream_id: &Identifier, f: impl FnOnce(&Topics) -> T) -> T {
        self.with_stream_by_id(stream_id, |stream| f(stream.topics()))
    }

    pub async fn with_topics_async<T>(
        &self,
        stream_id: &Identifier,
        f: impl AsyncFnOnce(&Topics) -> T,
    ) -> T {
        self.with_stream_by_id_async(stream_id, async |stream| f(stream.topics()).await)
            .await
    }

    pub fn with_topics_mut<T>(
        &self,
        stream_id: &Identifier,
        f: impl FnOnce(&mut Topics) -> T,
    ) -> T {
        self.with_stream_by_id_mut(stream_id, |stream| f(stream.topics_mut()))
    }

    pub fn with_topic_by_id<T>(
        &self,
        stream_id: &Identifier,
        id: &Identifier,
        f: impl FnOnce(&topic2::Topic) -> T,
    ) -> T {
        self.with_topics(stream_id, |topics| topics.with_topic_by_id(id, f))
    }

    pub async fn with_topic_by_id_async<T>(
        &self,
        stream_id: &Identifier,
        id: &Identifier,
        f: impl AsyncFnOnce(&topic2::Topic) -> T,
    ) -> T {
        self.with_topics_async(stream_id, async |topics| {
            topics.with_topic_by_id_async(id, f).await
        })
        .await
    }

    pub fn with_topic_by_id_mut<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl FnOnce(&mut topic2::Topic) -> T,
    ) -> T {
        self.with_topics_mut(stream_id, |topics| topics.with_topic_by_id_mut(topic_id, f))
    }

    pub fn with_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl FnOnce(&Partitions),
    ) {
        self.with_topics(stream_id, |topics| {
            topics.with_partitions(topic_id, f);
        });
    }

    pub async fn with_partitions_async<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl AsyncFnOnce(&Partitions) -> T,
    ) -> T {
        self.with_topics_async(stream_id, async |topics| {
            topics.with_partitions_async(topic_id, f).await
        })
        .await
    }

    pub fn len(&self) -> usize {
        self.container.borrow().len()
    }
}
