use ahash::AHashMap;
use iggy_common::Identifier;
use slab::Slab;
use std::{cell::RefCell, sync::Arc};
use tokio_rustls::StartHandshake;

use crate::{
    shard::stats,
    slab::{
        Keyed,
        partitions::Partitions,
        topics::Topics,
        traits_ext::{
            Delete, DeleteCell, EntityComponentSystem, EntityComponentSystemMutCell, Insert,
            InsertCell, InteriorMutability, IntoComponents,
        },
    },
    streaming::{stats::stats::StreamStats, streams::stream2, topics::topic2},
};

const CAPACITY: usize = 1024;
pub type ContainerId = usize;

pub struct Streams {
    index: RefCell<AHashMap<<stream2::StreamRoot as Keyed>::Key, ContainerId>>,
    root: RefCell<Slab<stream2::StreamRoot>>,
    stats: RefCell<Slab<Arc<StreamStats>>>,
}

impl<'a> From<&'a Streams> for stream2::StreamRef<'a> {
    fn from(value: &'a Streams) -> Self {
        let root = value.root.borrow();
        let stats = value.stats.borrow();
        stream2::StreamRef::new(root, stats)
    }
}

impl<'a> From<&'a Streams> for stream2::StreamRefMut<'a> {
    fn from(value: &'a Streams) -> Self {
        let root = value.root.borrow_mut();
        let stats = value.stats.borrow_mut();
        stream2::StreamRefMut::new(root, stats)
    }
}

impl InsertCell for Streams {
    type Idx = ContainerId;
    type Item = stream2::Stream;

    fn insert(&self, item: Self::Item) -> Self::Idx {
        let (root, stats) = item.into_components();
        let key = root.key().clone();

        let entity_id = self.root.borrow_mut().insert(root);
        let id = self.stats.borrow_mut().insert(stats);
        assert_eq!(
            entity_id, id,
            "stream_insert: id mismatch when inserting stats"
        );
        self.index.borrow_mut().insert(key, entity_id);
        entity_id
    }
}

impl DeleteCell for Streams {
    type Idx = ContainerId;
    type Item = stream2::Stream;

    fn delete(&self, id: Self::Idx) -> Self::Item {
        todo!()
    }
}

impl EntityComponentSystem<InteriorMutability> for Streams {
    type Idx = ContainerId;
    type Entity = stream2::Stream;
    type EntityRef<'a> = stream2::StreamRef<'a>;

    fn with<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityRef<'a>) -> O,
    {
        f(self.into())
    }

    async fn with_async<O, F>(&self, f: F) -> O
    where
        F: for<'a> AsyncFnOnce(Self::EntityRef<'a>) -> O,
    {
        f(self.into()).await
    }
}

impl EntityComponentSystemMutCell for Streams {
    type EntityRefMut<'a> = stream2::StreamRefMut<'a>;

    fn with_mut<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityRefMut<'a>) -> O,
    {
        f(self.into())
    }
}

impl Streams {
    pub fn init() -> Self {
        Self {
            index: RefCell::new(AHashMap::with_capacity(CAPACITY)),
            root: RefCell::new(Slab::with_capacity(CAPACITY)),
            stats: RefCell::new(Slab::with_capacity(CAPACITY)),
        }
    }

    pub fn exists(&self, id: &Identifier) -> bool {
        match id.kind {
            iggy_common::IdKind::Numeric => {
                let id = id.get_u32_value().unwrap() as usize;
                self.root.borrow().contains(id)
            }
            iggy_common::IdKind::String => {
                let key = id.get_string_value().unwrap();
                self.index.borrow().contains_key(&key)
            }
        }
    }

    pub fn get_index(&self, id: &Identifier) -> usize {
        match id.kind {
            iggy_common::IdKind::Numeric => id.get_u32_value().unwrap() as usize,
            iggy_common::IdKind::String => {
                let key = id.get_string_value().unwrap();
                *self.index.borrow().get(&key).expect("Stream not found")
            }
        }
    }

    pub fn with_root_by_id<T>(
        &self,
        id: &Identifier,
        f: impl FnOnce(&stream2::StreamRoot) -> T,
    ) -> T {
        let id = self.get_index(id);
        self.with_by_id(id, |(root, _)| f(&root))
    }

    pub async fn with_root_by_id_async<T>(
        &self,
        id: &Identifier,
        f: impl AsyncFnOnce(&stream2::StreamRoot) -> T,
    ) -> T {
        let id = self.get_index(id);
        self.with_by_id_async(id, async |(root, _)| f(&root).await)
            .await
    }

    pub fn with_root_by_id_mut<T>(
        &self,
        id: &Identifier,
        f: impl FnOnce(&mut stream2::StreamRoot) -> T,
    ) -> T {
        let id = self.get_index(id);
        self.with_by_id_mut(id, |(mut root, _)| f(&mut root))
    }

    pub fn with_stats<T>(&self, f: impl FnOnce(&Slab<Arc<StreamStats>>) -> T) -> T {
        self.with(|components| {
            let (_, stats) = components.into_components();
            f(&stats)
        })
    }

    pub fn with_stats_by_id<T>(
        &self,
        id: &Identifier,
        f: impl FnOnce(&Arc<StreamStats>) -> T,
    ) -> T {
        let id = self.get_index(id);
        self.with_by_id(id, |(_, stats)| {
            let stats = stats;
            f(&stats)
        })
    }

    pub fn with_stats_mut<T>(&self, f: impl FnOnce(&mut Slab<Arc<StreamStats>>) -> T) -> T {
        self.with_mut(|components| {
            let (_, mut stats) = components.into_components();
            f(&mut stats)
        })
    }

    pub fn with_index<T>(
        &self,
        f: impl FnOnce(&AHashMap<<stream2::StreamRoot as Keyed>::Key, usize>) -> T,
    ) -> T {
        let index = self.index.borrow();
        f(&index)
    }

    pub fn with_index_mut<T>(
        &self,
        f: impl FnOnce(&mut AHashMap<<stream2::StreamRoot as Keyed>::Key, usize>) -> T,
    ) -> T {
        let mut index = self.index.borrow_mut();
        f(&mut index)
    }

    pub fn with_topics<T>(&self, stream_id: &Identifier, f: impl FnOnce(&Topics) -> T) -> T {
        let id = self.get_index(stream_id);
        self.with_by_id(id, |(root, _)| f(root.topics()))
    }

    pub async fn with_topics_async<T>(
        &self,
        stream_id: &Identifier,
        f: impl AsyncFnOnce(&Topics) -> T,
    ) -> T {
        let id = self.get_index(stream_id);
        self.with_by_id_async(id, async |(root, _)| f(root.topics()).await)
            .await
    }

    pub fn with_topics_mut<T>(
        &self,
        stream_id: &Identifier,
        f: impl FnOnce(&mut Topics) -> T,
    ) -> T {
        let id = self.get_index(stream_id);
        self.with_by_id_mut(id, |(mut root, _)| {
            let topics = root.topics_mut();
            f(topics)
        })
    }

    pub fn with_topic_root_by_id<T>(
        &self,
        stream_id: &Identifier,
        id: &Identifier,
        f: impl FnOnce(&topic2::TopicRoot) -> T,
    ) -> T {
        self.with_topics(stream_id, |topics| {
            topics.with_root_by_id(id, |root| f(root))
        })
    }

    pub async fn with_topic_root_by_id_async<T>(
        &self,
        stream_id: &Identifier,
        id: &Identifier,
        f: impl AsyncFnOnce(&topic2::TopicRoot) -> T,
    ) -> T {
        self.with_topics_async(stream_id, async |topics| {
            let id = topics.get_index(id);
            topics
                .with_by_id_async(id, async |(root, _)| f(&root).await)
                .await
        })
        .await
    }

    pub fn with_topic_root_by_id_mut<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl FnOnce(&mut topic2::TopicRoot) -> T,
    ) -> T {
        self.with_topics_mut(stream_id, |topics| {
            topics.with_root_by_id_mut(topic_id, |root| f(root))
        })
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

    pub fn with_partitions_mut<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl FnOnce(&mut Partitions) -> T,
    ) -> T {
        self.with_topics_mut(stream_id, |topics| topics.with_partitions_mut(topic_id, f))
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
        self.root.borrow().len()
    }
}
