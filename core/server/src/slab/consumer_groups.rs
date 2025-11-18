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

use crate::{
    slab::{
        Keyed, consumer_groups,
        traits_ext::{
            Borrow, ComponentsById, Delete, EntityComponentSystem, EntityComponentSystemMut,
            Insert, IntoComponents,
        },
    },
    streaming::topics::consumer_group::{self, ConsumerGroupRef, ConsumerGroupRefMut},
};
use ahash::AHashMap;
use iggy_common::Identifier;
use slab::Slab;

const CAPACITY: usize = 1024;
pub type ContainerId = usize;

#[derive(Debug, Clone)]
pub struct ConsumerGroups {
    index: AHashMap<<consumer_group::ConsumerGroupRoot as Keyed>::Key, usize>,
    members: Slab<consumer_group::ConsumerGroupMembers>,
    root: Slab<consumer_group::ConsumerGroupRoot>,
}

impl Insert for ConsumerGroups {
    type Idx = consumer_groups::ContainerId;
    type Item = consumer_group::ConsumerGroup;

    fn insert(&mut self, item: Self::Item) -> Self::Idx {
        let (root, members) = item.into_components();
        let key = root.key().clone();

        let entity_id = self.root.insert(root);
        let id = self.members.insert(members);
        assert_eq!(
            entity_id, id,
            "consumer_group: id mismatch when inserting members"
        );
        self.index.insert(key, entity_id);
        let root = self.root.get_mut(entity_id).unwrap();
        root.update_id(entity_id);
        entity_id
    }
}

impl Delete for ConsumerGroups {
    type Idx = consumer_groups::ContainerId;
    type Item = consumer_group::ConsumerGroup;

    fn delete(&mut self, id: Self::Idx) -> Self::Item {
        let root = self.root.remove(id);
        let members = self.members.remove(id);
        self.index
            .remove(root.key())
            .expect("consumer_group_delete: key not found");
        consumer_group::ConsumerGroup::new_with_components(root, members)
    }
}

//TODO: those from impls could use a macro aswell.
impl<'a> From<&'a ConsumerGroups> for consumer_group::ConsumerGroupRef<'a> {
    fn from(value: &'a ConsumerGroups) -> Self {
        consumer_group::ConsumerGroupRef::new(&value.root, &value.members)
    }
}

impl<'a> From<&'a mut ConsumerGroups> for consumer_group::ConsumerGroupRefMut<'a> {
    fn from(value: &'a mut ConsumerGroups) -> Self {
        consumer_group::ConsumerGroupRefMut::new(&mut value.root, &mut value.members)
    }
}

impl EntityComponentSystem<Borrow> for ConsumerGroups {
    type Idx = consumer_groups::ContainerId;
    type Entity = consumer_group::ConsumerGroup;
    type EntityComponents<'a> = consumer_group::ConsumerGroupRef<'a>;

    fn with_components<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityComponents<'a>) -> O,
    {
        f(self.into())
    }
}

impl EntityComponentSystemMut for ConsumerGroups {
    type EntityComponentsMut<'a> = consumer_group::ConsumerGroupRefMut<'a>;

    fn with_components_mut<O, F>(&mut self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityComponentsMut<'a>) -> O,
    {
        f(self.into())
    }
}

impl ConsumerGroups {
    pub fn exists(&self, id: &Identifier) -> bool {
        match id.kind {
            iggy_common::IdKind::Numeric => {
                let id = id.get_u32_value().unwrap() as usize;
                self.root.contains(id)
            }
            iggy_common::IdKind::String => {
                let key = id.get_string_value().unwrap();
                self.index.contains_key(&key)
            }
        }
    }

    pub fn len(&self) -> usize {
        self.root.len()
    }

    pub fn is_empty(&self) -> bool {
        self.root.is_empty()
    }

    pub fn get_index(&self, id: &Identifier) -> usize {
        match id.kind {
            iggy_common::IdKind::Numeric => id.get_u32_value().unwrap() as usize,
            iggy_common::IdKind::String => {
                let key = id.get_string_value().unwrap();
                *self.index.get(&key).expect("Consumer Group not found")
            }
        }
    }

    pub fn with_consumer_group_by_id<T>(
        &self,
        identifier: &Identifier,
        f: impl FnOnce(ComponentsById<ConsumerGroupRef>) -> T,
    ) -> T {
        let id = self.get_index(identifier);
        self.with_components_by_id(id, |components| f(components))
    }

    pub fn with_consumer_group_by_id_mut<T>(
        &mut self,
        identifier: &Identifier,
        f: impl FnOnce(ComponentsById<ConsumerGroupRefMut>) -> T,
    ) -> T {
        let id = self.get_index(identifier);
        self.with_components_by_id_mut(id, |components| f(components))
    }
}

impl Default for ConsumerGroups {
    fn default() -> Self {
        Self {
            index: AHashMap::with_capacity(CAPACITY),
            root: Slab::with_capacity(CAPACITY),
            members: Slab::with_capacity(CAPACITY),
        }
    }
}
