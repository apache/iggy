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

use crate::metadata::consumer_group_member::ConsumerGroupMemberMeta;
use crate::metadata::{ConsumerGroupId, PartitionId};
use slab::Slab;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ConsumerGroupMeta {
    pub id: ConsumerGroupId,
    pub name: Arc<str>,
    pub partitions: Vec<PartitionId>,
    pub members: Slab<ConsumerGroupMemberMeta>,
}

impl ConsumerGroupMeta {
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
