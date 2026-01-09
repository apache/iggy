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

use crate::stm::{ApplyState, StateCommand};
use ahash::AHashMap;
use bytes::Bytes;
use iggy_common::create_consumer_group::CreateConsumerGroup;
use iggy_common::delete_consumer_group::DeleteConsumerGroup;
use iggy_common::{
    BytesSerializable, IggyTimestamp,
    header::{Operation, PrepareHeader},
    message::Message,
};
use slab::Slab;
use std::cell::RefCell;

// ============================================================================
// ConsumerGroupMember - Individual member of a consumer group
// ============================================================================

#[derive(Debug, Clone)]
pub struct ConsumerGroupMember {
    pub id: u32,
    pub joined_at: IggyTimestamp,
}

impl ConsumerGroupMember {
    pub fn new(id: u32, joined_at: IggyTimestamp) -> Self {
        Self { id, joined_at }
    }
}

// ============================================================================
// ConsumerGroup - A group of consumers
// ============================================================================

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub id: usize,
    pub stream_id: usize,
    pub topic_id: usize,
    pub name: String,
    pub created_at: IggyTimestamp,
    pub members: Vec<ConsumerGroupMember>,
}

impl ConsumerGroup {
    pub fn new(stream_id: usize, topic_id: usize, name: String, created_at: IggyTimestamp) -> Self {
        Self {
            id: 0,
            stream_id,
            topic_id,
            name,
            created_at,
            members: Vec::new(),
        }
    }

    pub fn add_member(&mut self, member: ConsumerGroupMember) {
        self.members.push(member);
    }

    pub fn remove_member(&mut self, member_id: u32) -> Option<ConsumerGroupMember> {
        if let Some(pos) = self.members.iter().position(|m| m.id == member_id) {
            Some(self.members.remove(pos))
        } else {
            None
        }
    }

    pub fn members_count(&self) -> usize {
        self.members.len()
    }
}

// ============================================================================
// ConsumerGroups Collection
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct ConsumerGroups {
    // Global index for all consumer groups across all streams/topics
    index: RefCell<AHashMap<(usize, usize, String), usize>>, // (stream_id, topic_id, name) -> id
    items: RefCell<Slab<ConsumerGroup>>,
}

impl ConsumerGroups {
    pub fn new() -> Self {
        Self {
            index: RefCell::new(AHashMap::with_capacity(256)),
            items: RefCell::new(Slab::with_capacity(256)),
        }
    }

    /// Insert a consumer group and return the assigned ID
    pub fn insert(&self, group: ConsumerGroup) -> usize {
        let mut items = self.items.borrow_mut();
        let mut index = self.index.borrow_mut();

        let key = (group.stream_id, group.topic_id, group.name.clone());
        let id = items.insert(group);
        items[id].id = id;
        index.insert(key, id);
        id
    }

    /// Get consumer group by ID
    pub fn get(&self, id: usize) -> Option<ConsumerGroup> {
        self.items.borrow().get(id).cloned()
    }

    /// Get consumer group by stream_id, topic_id, and name
    pub fn get_by_location(
        &self,
        stream_id: usize,
        topic_id: usize,
        name: &str,
    ) -> Option<ConsumerGroup> {
        let index = self.index.borrow();
        let key = (stream_id, topic_id, name.to_string());
        if let Some(&id) = index.get(&key) {
            self.items.borrow().get(id).cloned()
        } else {
            None
        }
    }

    /// Remove consumer group by ID
    pub fn remove(&self, id: usize) -> Option<ConsumerGroup> {
        let mut items = self.items.borrow_mut();
        let mut index = self.index.borrow_mut();

        if !items.contains(id) {
            return None;
        }

        let group = items.remove(id);
        let key = (group.stream_id, group.topic_id, group.name.clone());
        index.remove(&key);
        Some(group)
    }

    /// Get all consumer groups for a specific topic
    pub fn get_by_topic(&self, stream_id: usize, topic_id: usize) -> Vec<ConsumerGroup> {
        self.items
            .borrow()
            .iter()
            .filter_map(|(_, g)| {
                if g.stream_id == stream_id && g.topic_id == topic_id {
                    Some(g.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get number of consumer groups
    pub fn len(&self) -> usize {
        self.items.borrow().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.items.borrow().is_empty()
    }

    /// Get all consumer groups
    pub fn values(&self) -> Vec<ConsumerGroup> {
        self.items
            .borrow()
            .iter()
            .map(|(_, g): (usize, &ConsumerGroup)| g.clone())
            .collect()
    }
}

#[derive(Debug)]
pub enum ConsumerGroupsCommand {
    Create(CreateConsumerGroup),
    Delete(DeleteConsumerGroup),
}

impl StateCommand for ConsumerGroups {
    type Command = ConsumerGroupsCommand;
    type Input = Message<PrepareHeader>;

    fn into_command(input: &Self::Input) -> Option<Self::Command> {
        // TODO: rework this thing, so we don't copy the bytes on each request
        let body = Bytes::copy_from_slice(input.body());
        match input.header().operation {
            Operation::CreateConsumerGroup => Some(ConsumerGroupsCommand::Create(
                CreateConsumerGroup::from_bytes(body.clone()).unwrap(),
            )),
            Operation::DeleteConsumerGroup => Some(ConsumerGroupsCommand::Delete(
                DeleteConsumerGroup::from_bytes(body.clone()).unwrap(),
            )),
            _ => None,
        }
    }
}

impl ApplyState for ConsumerGroups {
    type Output = ();

    fn do_apply(&self, cmd: Self::Command) -> Self::Output {
        match cmd {
            ConsumerGroupsCommand::Create(payload) => {
                todo!("Handle Create consumer group with {:?}", payload)
            }
            ConsumerGroupsCommand::Delete(payload) => {
                todo!("Handle Delete consumer group with {:?}", payload)
            }
        }
    }
}
