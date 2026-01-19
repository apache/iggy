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

use crate::stm::Handler;
use crate::{define_state, impl_absorb};
use ahash::AHashMap;
use iggy_common::create_consumer_group::CreateConsumerGroup;
use iggy_common::delete_consumer_group::DeleteConsumerGroup;
use slab::Slab;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

// ============================================================================
// ConsumerGroupMember Entity
// ============================================================================

#[derive(Debug, Clone)]
pub struct ConsumerGroupMember {
    pub id: usize,
    pub client_id: u32,
    pub partitions: Vec<usize>,
    pub partition_index: Arc<AtomicUsize>,
}

impl ConsumerGroupMember {
    pub fn new(id: usize, client_id: u32) -> Self {
        Self {
            id,
            client_id,
            partitions: Vec::new(),
            partition_index: Arc::new(AtomicUsize::new(0)),
        }
    }
}

// ============================================================================
// ConsumerGroup Entity
// ============================================================================

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub id: usize,
    pub stream_id: usize,
    pub topic_id: usize,
    pub name: Arc<str>,
    pub partitions: Vec<usize>,
    pub members: Slab<ConsumerGroupMember>,
}

impl ConsumerGroup {
    pub fn new(stream_id: usize, topic_id: usize, name: Arc<str>) -> Self {
        Self {
            id: 0,
            stream_id,
            topic_id,
            name,
            partitions: Vec::new(),
            members: Slab::new(),
        }
    }

    /// Rebalance partition assignments among members (round-robin).
    pub fn rebalance_members(&mut self) {
        let partition_count = self.partitions.len();
        let member_count = self.members.len();

        if member_count == 0 || partition_count == 0 {
            return;
        }

        // Clear all member partitions
        let member_ids: Vec<usize> = self.members.iter().map(|(id, _)| id).collect();
        for &member_id in &member_ids {
            if let Some(member) = self.members.get_mut(member_id) {
                member.partitions.clear();
            }
        }

        // Rebuild assignments (round-robin)
        for (i, &partition_id) in self.partitions.iter().enumerate() {
            let member_idx = i % member_count;
            if let Some(&member_id) = member_ids.get(member_idx)
                && let Some(member) = self.members.get_mut(member_id)
            {
                member.partitions.push(partition_id);
            }
        }
    }
}

// ============================================================================
// ConsumerGroups State Machine
// ============================================================================

define_state! {
    ConsumerGroups {
        name_index: AHashMap<Arc<str>, usize>,
        topic_index: AHashMap<(usize, usize), Vec<usize>>,
        items: Slab<ConsumerGroup>,
    },
    [CreateConsumerGroup, DeleteConsumerGroup]
}
impl_absorb!(ConsumerGroupsInner, ConsumerGroupsCommand);

impl Handler for ConsumerGroupsInner {
    fn handle(&mut self, cmd: &ConsumerGroupsCommand) {
        match cmd {
            ConsumerGroupsCommand::CreateConsumerGroup(_payload) => {
                // Actual mutation logic will be implemented later
            }
            ConsumerGroupsCommand::DeleteConsumerGroup(_payload) => {
                // Actual mutation logic will be implemented later
            }
        }
    }
}
