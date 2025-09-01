use ahash::AHashMap;
use iggy_common::Identifier;
use slab::Slab;
use std::{cell::RefCell, sync::Arc};

use crate::{
    slab::{
        Keyed,
        consumer_groups::ConsumerGroups,
        helpers,
        partitions::Partitions,
        traits_ext::{
            ComponentsById, DeleteCell, EntityComponentSystem, EntityComponentSystemMutCell,
            Insert, InsertCell, InteriorMutability, IntoComponents,
        },
    },
    streaming::{
        partitions::{
            journal::MemoryMessageJournal,
            log::{Log, SegmentedLog},
        },
        stats::stats::TopicStats,
        topics::{
            consumer_group2::{ConsumerGroupRef, ConsumerGroupRefMut},
            topic2::{self, TopicRef, TopicRefMut},
        },
    },
};

const CAPACITY: usize = 1024;
pub type ContainerId = usize;

#[derive(Debug, Clone)]
pub struct Topics {
    index: RefCell<AHashMap<<topic2::TopicRoot as Keyed>::Key, ContainerId>>,
    root: RefCell<Slab<topic2::TopicRoot>>,
    auxilaries: RefCell<Slab<topic2::TopicAuxilary>>,
    stats: RefCell<Slab<Arc<TopicStats>>>,
}

impl InsertCell for Topics {
    type Idx = ContainerId;
    type Item = topic2::Topic;

    fn insert(&self, item: Self::Item) -> Self::Idx {
        let (root, auxilary, stats) = item.into_components();
        let mut root_container = self.root.borrow_mut();
        let mut auxilaries = self.auxilaries.borrow_mut();
        let mut indexes = self.index.borrow_mut();
        let mut stats_container = self.stats.borrow_mut();

        let key = root.key().clone();
        let entity_id = root_container.insert(root);
        let id = stats_container.insert(stats);
        assert_eq!(
            entity_id, id,
            "topic_insert: id mismatch when inserting stats component"
        );
        let id = auxilaries.insert(auxilary);
        assert_eq!(
            entity_id, id,
            "topic_insert: id mismatch when inserting auxilary component"
        );
        let root = root_container.get_mut(entity_id).unwrap();
        root.update_id(entity_id);
        indexes.insert(key, entity_id);
        entity_id
    }
}

impl DeleteCell for Topics {
    type Idx = ContainerId;
    type Item = topic2::Topic;

    fn delete(&self, id: Self::Idx) -> Self::Item {
        // TODO: don't forget to remoev from the index
        todo!()
    }
}

//TODO: those from impls could use a macro aswell.
impl<'a> From<&'a Topics> for topic2::TopicRef<'a> {
    fn from(value: &'a Topics) -> Self {
        let root = value.root.borrow();
        let auxilary = value.auxilaries.borrow();
        let stats = value.stats.borrow();
        topic2::TopicRef::new(root, auxilary, stats)
    }
}

impl<'a> From<&'a Topics> for topic2::TopicRefMut<'a> {
    fn from(value: &'a Topics) -> Self {
        let root = value.root.borrow_mut();
        let auxilary = value.auxilaries.borrow_mut();
        let stats = value.stats.borrow_mut();
        topic2::TopicRefMut::new(root, auxilary, stats)
    }
}

impl Default for Topics {
    fn default() -> Self {
        Self {
            index: RefCell::new(AHashMap::with_capacity(CAPACITY)),
            root: RefCell::new(Slab::with_capacity(CAPACITY)),
            auxilaries: RefCell::new(Slab::with_capacity(CAPACITY)),
            stats: RefCell::new(Slab::with_capacity(CAPACITY)),
        }
    }
}

impl EntityComponentSystem<InteriorMutability> for Topics {
    type Idx = ContainerId;
    type Entity = topic2::Topic;
    type EntityComponents<'a> = topic2::TopicRef<'a>;

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

impl EntityComponentSystemMutCell for Topics {
    type EntityComponentsMut<'a> = topic2::TopicRefMut<'a>;

    fn with_components_mut<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityComponentsMut<'a>) -> O,
    {
        f(self.into())
    }
}

impl Topics {
    pub fn len(&self) -> usize {
        self.root.borrow().len()
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
                *self.index.borrow().get(&key).expect("Topic not found")
            }
        }
    }

    pub fn with_index<T>(
        &self,
        f: impl FnOnce(&AHashMap<<topic2::TopicRoot as Keyed>::Key, usize>) -> T,
    ) -> T {
        let index = self.index.borrow();
        f(&index)
    }

    pub fn with_index_mut<T>(
        &self,
        f: impl FnOnce(&mut AHashMap<<topic2::TopicRoot as Keyed>::Key, usize>) -> T,
    ) -> T {
        let mut index = self.index.borrow_mut();
        f(&mut index)
    }

    pub fn with_topic_by_id<T>(
        &self,
        topic_id: &Identifier,
        f: impl FnOnce(ComponentsById<TopicRef>) -> T,
    ) -> T {
        let id = self.get_index(topic_id);
        self.with_components_by_id(id, |components| f(components))
    }

    pub fn with_topic_by_id_async<T>(
        &self,
        topic_id: &Identifier,
        f: impl AsyncFnOnce(ComponentsById<TopicRef>) -> T,
    ) -> impl Future<Output = T> {
        let id = self.get_index(topic_id);
        self.with_components_by_id_async(id, async |components| f(components).await)
    }

    pub fn with_topic_by_id_mut<T>(
        &self,
        topic_id: &Identifier,
        f: impl FnOnce(ComponentsById<TopicRefMut>) -> T,
    ) -> T {
        let id = self.get_index(topic_id);
        self.with_components_by_id_mut(id, |components| f(components))
    }

    pub fn with_consumer_groups<T>(
        &self,
        topic_id: &Identifier,
        f: impl FnOnce(&ConsumerGroups) -> T,
    ) -> T {
        self.with_topic_by_id(topic_id, helpers::consumer_groups(f))
    }

    pub fn with_consumer_groups_async<T>(
        &self,
        topic_id: &Identifier,
        f: impl AsyncFnOnce(&ConsumerGroups) -> T,
    ) -> impl Future<Output = T> {
        self.with_topic_by_id_async(topic_id, helpers::consumer_groups_async(f))
    }

    pub fn with_consumer_groups_mut<T>(
        &self,
        topic_id: &Identifier,
        f: impl FnOnce(&mut ConsumerGroups) -> T,
    ) -> T {
        self.with_topic_by_id_mut(topic_id, helpers::consumer_groups_mut(f))
    }

    pub fn with_consumer_group_by_id<T>(
        &self,
        topic_id: &Identifier,
        group_id: &Identifier,
        f: impl FnOnce(ComponentsById<ConsumerGroupRef>) -> T,
    ) -> T {
        self.with_consumer_groups(topic_id, |container| {
            container.with_consumer_group_by_id(group_id, f)
        })
    }

    pub fn with_consumer_group_by_id_async<T>(
        &self,
        topic_id: &Identifier,
        group_id: &Identifier,
        f: impl AsyncFnOnce(ComponentsById<ConsumerGroupRef>) -> T,
    ) -> impl Future<Output = T> {
        self.with_consumer_groups_async(topic_id, async |container| {
            container.with_consumer_group_by_id_async(group_id, f).await
        })
    }

    pub fn with_consumer_group_by_id_mut<T>(
        &self,
        topic_id: &Identifier,
        group_id: &Identifier,
        f: impl FnOnce(ComponentsById<ConsumerGroupRefMut>) -> T,
    ) -> T {
        self.with_consumer_groups_mut(topic_id, |container| {
            container.with_consumer_group_by_id_mut(group_id, f)
        })
    }

    pub fn with_partitions<T>(&self, topic_id: &Identifier, f: impl FnOnce(&Partitions) -> T) -> T {
        self.with_topic_by_id(topic_id, helpers::partitions(f))
    }

    pub fn with_partitions_async<T>(
        &self,
        topic_id: &Identifier,
        f: impl AsyncFnOnce(&Partitions) -> T,
    ) -> impl Future<Output = T> {
        self.with_topic_by_id_async(topic_id, helpers::partitions_async(f))
    }

    pub fn with_partitions_mut<T>(
        &self,
        topic_id: &Identifier,
        f: impl FnOnce(&mut Partitions) -> T,
    ) -> T {
        self.with_topic_by_id_mut(topic_id, helpers::partitions_mut(f))
    }
}
