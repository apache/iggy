use crate::{
    slab::{
        Keyed,
        consumer_groups::ConsumerGroups,
        helpers,
        partitions::Partitions,
        topics::Topics,
        traits_ext::{
            ComponentsById, ComponentsByIdMapping, ComponentsMapping, DeleteCell,
            EntityComponentSystem, EntityComponentSystemMutCell, InsertCell, InteriorMutability,
            IntoComponents,
        },
    },
    streaming::{
        stats::stats::StreamStats,
        streams::stream2::{self, StreamRef, StreamRefMut},
        topics::{
            consumer_group2::{ConsumerGroupRef, ConsumerGroupRefMut},
            topic2::{self, TopicRef, TopicRefMut},
        },
    },
};
use ahash::AHashMap;
use iggy_common::Identifier;
use slab::Slab;
use std::{cell::RefCell, sync::Arc};

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
    type EntityComponents<'a> = stream2::StreamRef<'a>;

    fn with_components<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityComponents<'a>) -> O,
    {
        f(self.into())
    }

    fn with_components_async<O, F>(&self, f: F) -> impl Future<Output = O>
    where
        F: for<'a> AsyncFnOnce(Self::EntityComponents<'a>) -> O,
    {
        f(self.into())
    }
}

impl EntityComponentSystemMutCell for Streams {
    type EntityComponentsMut<'a> = stream2::StreamRefMut<'a>;

    fn with_components_mut<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityComponentsMut<'a>) -> O,
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

    pub fn with_stream_by_id<T>(
        &self,
        id: &Identifier,
        f: impl FnOnce(ComponentsById<StreamRef>) -> T,
    ) -> T {
        let id = self.get_index(id);
        self.with_components_by_id(id, |stream| f(stream))
    }

    pub fn with_stream_by_id_async<T>(
        &self,
        id: &Identifier,
        f: impl AsyncFnOnce(ComponentsById<StreamRef>) -> T,
    ) -> impl Future<Output = T> {
        let id = self.get_index(id);
        self.with_components_by_id_async(id, async |stream| f(stream).await)
    }

    pub fn with_stream_by_id_mut<T>(
        &self,
        id: &Identifier,
        f: impl FnOnce(ComponentsById<StreamRefMut>) -> T,
    ) -> T {
        let id = self.get_index(id);
        self.with_components_by_id_mut(id, |stream| f(stream))
    }

    pub fn with_topics<T>(&self, stream_id: &Identifier, f: impl FnOnce(&Topics) -> T) -> T {
        self.with_stream_by_id(stream_id, helpers::topics(f))
    }

    pub fn with_topics_async<T>(
        &self,
        stream_id: &Identifier,
        f: impl AsyncFnOnce(&Topics) -> T,
    ) -> impl Future<Output = T> {
        self.with_stream_by_id_async(stream_id, helpers::topics_async(f))
    }

    pub fn with_topics_mut<T>(
        &self,
        stream_id: &Identifier,
        f: impl FnOnce(&mut Topics) -> T,
    ) -> T {
        self.with_stream_by_id_mut(stream_id, helpers::topics_mut(f))
    }

    pub fn with_topic_by_id<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl FnOnce(ComponentsById<TopicRef>) -> T,
    ) -> T {
        self.with_topics(stream_id, |container| {
            container.with_topic_by_id(topic_id, f)
        })
    }

    pub fn with_topic_by_id_async<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl AsyncFnOnce(ComponentsById<TopicRef>) -> T,
    ) -> impl Future<Output = T> {
        self.with_topics_async(stream_id, async |container| {
            container.with_topic_by_id_async(topic_id, f).await
        })
    }

    pub fn with_topic_by_id_mut<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl FnOnce(ComponentsById<TopicRefMut>) -> T,
    ) -> T {
        self.with_topics_mut(stream_id, |container| {
            container.with_topic_by_id_mut(topic_id, f)
        })
    }

    pub fn with_consumer_groups<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl FnOnce(&ConsumerGroups) -> T,
    ) -> T {
        self.with_topics(stream_id, |container| {
            container.with_consumer_groups(topic_id, f)
        })
    }

    pub fn with_consumer_groups_async<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl AsyncFnOnce(&ConsumerGroups) -> T,
    ) -> impl Future<Output = T> {
        self.with_topics_async(stream_id, async |container| {
            container.with_consumer_groups_async(topic_id, f).await
        })
    }

    pub fn with_consumer_group_by_id<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
        f: impl FnOnce(ComponentsById<ConsumerGroupRef>) -> T,
    ) -> T {
        self.with_consumer_groups(stream_id, topic_id, |container| {
            container.with_consumer_group_by_id(group_id, f)
        })
    }

    pub fn with_consumer_group_by_id_mut<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
        f: impl FnOnce(ComponentsById<ConsumerGroupRefMut>) -> T,
    ) -> T {
        self.with_consumer_groups_mut(stream_id, topic_id, |container| {
            container.with_consumer_group_by_id_mut(group_id, f)
        })
    }

    pub fn with_consumer_group_by_id_async<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
        f: impl AsyncFnOnce(ComponentsById<ConsumerGroupRef>) -> T,
    ) -> impl Future<Output = T> {
        self.with_consumer_groups_async(stream_id, topic_id, async move |container| {
            container.with_consumer_group_by_id_async(group_id, f).await
        })
    }

    pub fn with_consumer_groups_mut<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl FnOnce(&mut ConsumerGroups) -> T,
    ) -> T {
        self.with_topics_mut(stream_id, |container| {
            container.with_consumer_groups_mut(topic_id, f)
        })
    }

    pub fn with_partitions<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl FnOnce(&Partitions) -> T,
    ) -> T {
        self.with_topics(stream_id, |container| {
            container.with_partitions(topic_id, f)
        })
    }

    pub fn with_partitions_async<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl AsyncFnOnce(&Partitions) -> T,
    ) -> impl Future<Output = T> {
        self.with_topics_async(stream_id, async |container| {
            container.with_partitions_async(topic_id, f).await
        })
    }

    pub fn with_partitions_mut<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl FnOnce(&mut Partitions) -> T,
    ) -> T {
        self.with_topics_mut(stream_id, |container| {
            container.with_partitions_mut(topic_id, f)
        })
    }

    pub fn len(&self) -> usize {
        self.root.borrow().len()
    }
}
