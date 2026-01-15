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
use iggy_common::IggyTimestamp;
use slab::Slab;

// ============================================================================
// ConsumerGroupMember - Individual member of a consumer group
// ============================================================================

#[derive(Debug, Clone, Default)]
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

#[derive(Debug, Clone, Default)]
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
    pub index: AHashMap<(usize, usize, String), usize>,
    pub items: Slab<ConsumerGroup>,
}

impl ConsumerGroups {
    pub fn new() -> Self {
        Self::default()
    }
}
