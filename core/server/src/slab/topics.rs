// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
            InsertCell, InteriorMutability, IntoComponents,
        },
    },
    streaming::{
        stats::TopicStats,
        topics::{
            consumer_group::{ConsumerGroupRef, ConsumerGroupRefMut},
            topic::{self, TopicRef, TopicRefMut},
        },
    },
};

const CAPACITY: usize = 1024;
pub type ContainerId = usize;

#[derive(Debug, Clone)]
pub struct Topics {
    index: RefCell<AHashMap<<topic::TopicRoot as Keyed>::Key, ContainerId>>,
    root: RefCell<Slab<topic::TopicRoot>>,
    auxilaries: RefCell<Slab<topic::TopicAuxilary>>,
    stats: RefCell<Slab<Arc<TopicStats>>>,
}

impl InsertCell for Topics {
    type Idx = ContainerId;
    type Item = topic::Topic;

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
    type Item = topic::Topic;

    fn delete(&self, id: Self::Idx) -> Self::Item {
        let mut root_container = self.root.borrow_mut();
        let mut auxilaries = self.auxilaries.borrow_mut();
        let mut indexes = self.index.borrow_mut();
        let mut stats_container = self.stats.borrow_mut();

        let root = root_container.remove(id);
        let auxilary = auxilaries.remove(id);
        let stats = stats_container.remove(id);

        // Remove from index
        let key = root.key();
        indexes.remove(key).unwrap_or_else(|| {
            panic!(
                "topic_delete: key not found with key: {} and id: {}",
                key, id
            )
        });

        topic::Topic::new_with_components(root, auxilary, stats)
    }
}

impl<'a> From<&'a Topics> for topic::TopicRef<'a> {
    fn from(value: &'a Topics) -> Self {
        let root = value.root.borrow();
        let auxilary = value.auxilaries.borrow();
        let stats = value.stats.borrow();
        topic::TopicRef::new(root, auxilary, stats)
    }
}

impl<'a> From<&'a Topics> for topic::TopicRefMut<'a> {
    fn from(value: &'a Topics) -> Self {
        let root = value.root.borrow_mut();
        let auxilary = value.auxilaries.borrow_mut();
        let stats = value.stats.borrow_mut();
        topic::TopicRefMut::new(root, auxilary, stats)
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
    type Entity = topic::Topic;
    type EntityComponents<'a> = topic::TopicRef<'a>;

    fn with_components<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityComponents<'a>) -> O,
    {
        f(self.into())
    }
}

impl EntityComponentSystemMutCell for Topics {
    type EntityComponentsMut<'a> = topic::TopicRefMut<'a>;

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

    pub fn is_empty(&self) -> bool {
        self.root.borrow().is_empty()
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
        f: impl FnOnce(&AHashMap<<topic::TopicRoot as Keyed>::Key, usize>) -> T,
    ) -> T {
        let index = self.index.borrow();
        f(&index)
    }

    pub fn with_index_mut<T>(
        &self,
        f: impl FnOnce(&mut AHashMap<<topic::TopicRoot as Keyed>::Key, usize>) -> T,
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
        self.with_components_by_id(id, f)
    }

    pub fn with_topic_by_id_mut<T>(
        &self,
        topic_id: &Identifier,
        f: impl FnOnce(ComponentsById<TopicRefMut>) -> T,
    ) -> T {
        let id = self.get_index(topic_id);
        self.with_components_by_id_mut(id, f)
    }

    pub fn with_consumer_groups<T>(
        &self,
        topic_id: &Identifier,
        f: impl FnOnce(&ConsumerGroups) -> T,
    ) -> T {
        self.with_topic_by_id(topic_id, helpers::consumer_groups(f))
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

    pub fn with_partitions_mut<T>(
        &self,
        topic_id: &Identifier,
        f: impl FnOnce(&mut Partitions) -> T,
    ) -> T {
        self.with_topic_by_id_mut(topic_id, helpers::partitions_mut(f))
    }
}
