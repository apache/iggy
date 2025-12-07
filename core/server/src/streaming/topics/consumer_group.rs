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

use crate::slab::{
    Keyed, consumer_groups, partitions,
    traits_ext::{EntityMarker, IntoComponents, IntoComponentsById},
};
use arcshift::ArcShift;
use slab::Slab;
use std::sync::atomic::AtomicUsize;

pub const MEMBERS_CAPACITY: usize = 128;

#[derive(Debug, Clone)]
pub struct ConsumerGroupMembers {
    inner: ArcShift<Slab<Member>>,
}

impl ConsumerGroupMembers {
    pub fn new(inner: ArcShift<Slab<Member>>) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> ArcShift<Slab<Member>> {
        self.inner
    }

    pub fn inner(&self) -> &ArcShift<Slab<Member>> {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut ArcShift<Slab<Member>> {
        &mut self.inner
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    root: ConsumerGroupRoot,
    members: ConsumerGroupMembers,
}

#[derive(Default, Debug, Clone)]
pub struct ConsumerGroupRoot {
    id: usize,
    name: String,
    partitions: Vec<partitions::ContainerId>,
}

impl ConsumerGroupRoot {
    pub fn disarray(self) -> (String, Vec<usize>) {
        (self.name, self.partitions)
    }

    pub fn partitions(&self) -> &Vec<partitions::ContainerId> {
        &self.partitions
    }

    pub fn update_id(&mut self, id: usize) {
        self.id = id;
    }

    pub fn assign_partitions(&mut self, partitions: Vec<usize>) {
        self.partitions = partitions;
    }

    pub fn id(&self) -> consumer_groups::ContainerId {
        self.id
    }
}

impl Keyed for ConsumerGroupRoot {
    type Key = String;

    fn key(&self) -> &Self::Key {
        &self.name
    }
}

impl EntityMarker for ConsumerGroup {
    type Idx = consumer_groups::ContainerId;

    fn id(&self) -> Self::Idx {
        self.root.id
    }

    fn update_id(&mut self, id: Self::Idx) {
        self.root.id = id;
    }
}

impl IntoComponents for ConsumerGroup {
    type Components = (ConsumerGroupRoot, ConsumerGroupMembers);

    fn into_components(self) -> Self::Components {
        (self.root, self.members)
    }
}

pub struct ConsumerGroupRef<'a> {
    root: &'a Slab<ConsumerGroupRoot>,
    members: &'a Slab<ConsumerGroupMembers>,
}

impl<'a> ConsumerGroupRef<'a> {
    pub fn new(root: &'a Slab<ConsumerGroupRoot>, members: &'a Slab<ConsumerGroupMembers>) -> Self {
        Self { root, members }
    }
}

impl<'a> IntoComponents for ConsumerGroupRef<'a> {
    type Components = (&'a Slab<ConsumerGroupRoot>, &'a Slab<ConsumerGroupMembers>);

    fn into_components(self) -> Self::Components {
        (self.root, self.members)
    }
}

impl<'a> IntoComponentsById for ConsumerGroupRef<'a> {
    type Idx = consumer_groups::ContainerId;
    type Output = (&'a ConsumerGroupRoot, &'a ConsumerGroupMembers);

    fn into_components_by_id(self, index: Self::Idx) -> Self::Output {
        let root = &self.root[index];
        let members = &self.members[index];
        (root, members)
    }
}

pub struct ConsumerGroupRefMut<'a> {
    root: &'a mut Slab<ConsumerGroupRoot>,
    members: &'a mut Slab<ConsumerGroupMembers>,
}

impl<'a> ConsumerGroupRefMut<'a> {
    pub fn new(
        root: &'a mut Slab<ConsumerGroupRoot>,
        members: &'a mut Slab<ConsumerGroupMembers>,
    ) -> Self {
        Self { root, members }
    }
}

impl<'a> IntoComponents for ConsumerGroupRefMut<'a> {
    type Components = (
        &'a mut Slab<ConsumerGroupRoot>,
        &'a mut Slab<ConsumerGroupMembers>,
    );

    fn into_components(self) -> Self::Components {
        (self.root, self.members)
    }
}

impl<'a> IntoComponentsById for ConsumerGroupRefMut<'a> {
    type Idx = consumer_groups::ContainerId;
    type Output = (&'a mut ConsumerGroupRoot, &'a mut ConsumerGroupMembers);

    fn into_components_by_id(self, index: Self::Idx) -> Self::Output {
        let root = &mut self.root[index];
        let members = &mut self.members[index];
        (root, members)
    }
}

impl ConsumerGroup {
    pub fn new(
        name: String,
        members: ArcShift<Slab<Member>>,
        partitions: Vec<partitions::ContainerId>,
    ) -> Self {
        let root = ConsumerGroupRoot {
            id: 0,
            name,
            partitions,
        };
        let members = ConsumerGroupMembers { inner: members };
        Self { root, members }
    }

    pub fn new_with_components(root: ConsumerGroupRoot, members: ConsumerGroupMembers) -> Self {
        Self { root, members }
    }

    pub fn partitions(&self) -> &Vec<partitions::ContainerId> {
        &self.root.partitions
    }

    pub fn members(&self) -> &ConsumerGroupMembers {
        &self.members
    }
}

#[derive(Debug)]
pub struct Member {
    pub id: usize,
    pub client_id: u32,
    pub partitions: Vec<partitions::ContainerId>,
    pub current_partition_idx: AtomicUsize,
}

impl Clone for Member {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            client_id: self.client_id,
            partitions: self.partitions.clone(),
            current_partition_idx: AtomicUsize::new(0),
        }
    }
}

impl Member {
    pub fn new(client_id: u32) -> Self {
        Member {
            id: 0,
            client_id,
            partitions: Vec::new(),
            current_partition_idx: AtomicUsize::new(0),
        }
    }

    pub fn insert_into(self, container: &mut Slab<Self>) -> usize {
        let idx = container.insert(self);
        let member = &mut container[idx];
        member.id = idx;
        idx
    }
}
