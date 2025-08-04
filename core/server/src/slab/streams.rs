use iggy_common::Identifier;
use slab::Slab;
use std::{cell::RefCell, sync::Arc};

use crate::{
    slab::{IndexedSlab, partitions::Partitions, topics::Topics},
    streaming::{
        partitions::partition2, stats::stats::StreamStats, streams::stream2, topics::topic2,
    },
};

const CAPACITY: usize = 1024;

pub struct Streams {
    container: RefCell<IndexedSlab<stream2::Stream>>,
    stats: RefCell<Slab<Arc<StreamStats>>>,
}

impl Streams {
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

    pub async fn with_async<T>(
        &self,
        f: impl AsyncFnOnce(&IndexedSlab<stream2::Stream>) -> T,
    ) -> T {
        let container = self.container.borrow();
        f(&container).await
    }

    pub fn with<T>(&self, f: impl FnOnce(&IndexedSlab<stream2::Stream>) -> T) -> T {
        let container = self.container.borrow();
        f(&container)
    }

    pub fn with_mut<T>(&self, f: impl FnOnce(&mut IndexedSlab<stream2::Stream>) -> T) -> T {
        let mut container = self.container.borrow_mut();
        f(&mut container)
    }

    pub fn with_stream_by_id<T>(
        &self,
        id: &Identifier,
        f: impl FnOnce(&stream2::Stream) -> T,
    ) -> T {
        self.with(|streams| {
            let stream = match id.kind {
                iggy_common::IdKind::Numeric => {
                    let id = id.get_u32_value().unwrap() as usize;
                    &streams[id]
                }
                iggy_common::IdKind::String => {
                    let key = id.get_string_value().unwrap();
                    unsafe { streams.get_by_key_unchecked(&key) }
                }
            };
            f(stream)
        })
    }

    pub fn with_stream_by_id_mut(&self, id: &Identifier, f: impl FnOnce(&mut stream2::Stream)) {
        self.with_mut(|streams| {
            let stream = match id.kind {
                iggy_common::IdKind::Numeric => {
                    let id = id.get_u32_value().unwrap() as usize;
                    &mut streams[id]
                }
                iggy_common::IdKind::String => {
                    let key = id.get_string_value().unwrap();
                    unsafe { streams.get_by_key_mut_unchecked(&key) }
                }
            };
            f(stream)
        });
    }

    pub fn with_topics<T>(&self, stream_id: &Identifier, f: impl FnOnce(&Topics) -> T) -> T {
        self.with_stream_by_id(stream_id, |stream| f(stream.topics()))
    }

    pub async fn with_topics_async<T>(
        &self,
        stream_id: &Identifier,
        f: impl AsyncFnOnce(&Topics) -> T,
    ) -> T {
        self.with_async(async |streams: &IndexedSlab<stream2::Stream>| {
            let stream = match stream_id.kind {
                iggy_common::IdKind::Numeric => {
                    let id = stream_id.get_u32_value().unwrap() as usize;
                    &streams[id]
                }
                iggy_common::IdKind::String => {
                    let key = stream_id.get_string_value().unwrap();
                    unsafe { streams.get_by_key_unchecked(&key) }
                }
            };
            f(stream.topics()).await
        })
        .await
    }

    pub fn with_topics_mut<T>(
        &self,
        stream_id: &Identifier,
        f: impl FnOnce(&mut Topics) -> T,
    ) -> T {
        self.with_mut(|streams| {
            let stream = match stream_id.kind {
                iggy_common::IdKind::Numeric => {
                    let id = stream_id.get_u32_value().unwrap() as usize;
                    &mut streams[id]
                }
                iggy_common::IdKind::String => {
                    let key = stream_id.get_string_value().unwrap();
                    unsafe { streams.get_by_key_mut_unchecked(&key) }
                }
            };
            f(stream.topics_mut())
        })
    }

    pub fn with_topic_by_id<T>(
        &self,
        stream_id: &Identifier,
        id: &Identifier,
        f: impl FnOnce(&topic2::Topic) -> T,
    ) -> T {
        self.with_topics(stream_id, |topics| topics.with_topic_by_id(id, f))
    }

    pub fn with_topic_by_id_mut<T>(
        &self,
        id: &Identifier,
        topic_id: &Identifier,
        f: impl FnOnce(&mut topic2::Topic) -> T,
    ) -> T {
        self.with_topics(id, |topics| topics.with_topic_by_id_mut(topic_id, f))
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

    pub fn with_partition_by_id(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
        f: impl FnOnce(&partition2::Partition),
    ) {
        self.with_partitions(stream_id, topic_id, |partitions| {
            partitions.with_partition_id(partition_id, f);
        });
    }
}
