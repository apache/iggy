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

use iggy_common::IggyTimestamp;
use imbl::HashMap as ImHashMap;

/// Consumer group member stored in the shared snapshot.
/// Partition assignments are computed via round-robin rebalancing.
#[derive(Debug, Clone)]
pub struct ConsumerGroupMember {
    pub client_id: u32,
    pub joined_at: IggyTimestamp,
    pub partitions: Vec<usize>,
}

impl ConsumerGroupMember {
    pub fn new(client_id: u32) -> Self {
        Self {
            client_id,
            joined_at: IggyTimestamp::now(),
            partitions: Vec::new(),
        }
    }

    pub fn with_partitions(client_id: u32, partitions: Vec<usize>) -> Self {
        Self {
            client_id,
            joined_at: IggyTimestamp::now(),
            partitions,
        }
    }
}

/// Consumer group metadata stored in the shared snapshot.
/// Members are managed via RCU pattern for thread-safe reads.
#[derive(Debug, Clone)]
pub struct ConsumerGroupMeta {
    pub id: usize,
    pub name: String,
    pub partitions: Vec<usize>,
    pub members: ImHashMap<u32, ConsumerGroupMember>,
}

impl ConsumerGroupMeta {
    pub fn new(id: usize, name: String, partitions: Vec<usize>) -> Self {
        Self {
            id,
            name,
            partitions,
            members: ImHashMap::new(),
        }
    }

    /// Get the number of members in the group.
    pub fn members_count(&self) -> usize {
        self.members.len()
    }

    /// Check if a client is a member of the group.
    pub fn has_member(&self, client_id: u32) -> bool {
        self.members.contains_key(&client_id)
    }

    /// Get a member by client ID.
    pub fn get_member(&self, client_id: u32) -> Option<&ConsumerGroupMember> {
        self.members.get(&client_id)
    }

    /// Rebalance partitions among members using round-robin distribution.
    /// Returns a new members map with updated partition assignments.
    pub fn rebalance_partitions(&self) -> ImHashMap<u32, ConsumerGroupMember> {
        let member_ids: Vec<u32> = self.members.keys().copied().collect();
        if member_ids.is_empty() {
            return self.members.clone();
        }

        let partitions_per_member = self.partitions.len() / member_ids.len();
        let remainder = self.partitions.len() % member_ids.len();

        let mut new_members = ImHashMap::new();
        let mut partition_idx = 0;

        for (i, &client_id) in member_ids.iter().enumerate() {
            let extra = if i < remainder { 1 } else { 0 };
            let count = partitions_per_member + extra;
            let assigned: Vec<usize> =
                self.partitions[partition_idx..partition_idx + count].to_vec();
            partition_idx += count;

            if let Some(member) = self.members.get(&client_id) {
                new_members.insert(
                    client_id,
                    ConsumerGroupMember {
                        client_id,
                        joined_at: member.joined_at,
                        partitions: assigned,
                    },
                );
            }
        }

        new_members
    }
}
