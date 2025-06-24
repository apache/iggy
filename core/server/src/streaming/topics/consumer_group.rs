/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use ahash::AHashMap;
use iggy_common::IggyError;
use tokio::sync::RwLock;
use tracing::trace;

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub topic_id: u32,
    pub group_id: u32,
    pub name: String,
    pub partitions_count: u32,
    members: AHashMap<u32, ConsumerGroupMember>,
}

#[derive(Debug, Clone)]
pub struct ConsumerGroupMember {
    pub id: u32,
    partitions: AHashMap<u32, u32>,
    current_partition_index: Option<u32>,
    current_partition_id: Option<u32>,
}

impl ConsumerGroup {
    pub fn new(topic_id: u32, group_id: u32, name: &str, partitions_count: u32) -> ConsumerGroup {
        ConsumerGroup {
            topic_id,
            group_id,
            name: name.to_string(),
            partitions_count,
            members: AHashMap::new(),
        }
    }

    pub fn get_members(&self) -> Vec<ConsumerGroupMember> {
        self.members.values().cloned().collect()
    }

    pub async fn reassign_partitions(&mut self, partitions_count: u32) {
        self.partitions_count = partitions_count;
        self.assign_partitions().await;
    }

    pub async fn calculate_partition_id(&self, member_id: u32) -> Result<Option<u32>, IggyError> {
        let member = self.members.get(&member_id);
        if let Some(member) = member {
            return Ok(member.await.calculate_partition_id());
        }
        Err(IggyError::ConsumerGroupMemberNotFound(
            member_id,
            self.group_id,
            self.topic_id,
        ))
    }

    pub async fn get_current_partition_id(&self, member_id: u32) -> Result<Option<u32>, IggyError> {
        let member = self.members.get(&member_id);
        if let Some(member) = member {
            return Ok(member.read().await.current_partition_id);
        }
        Err(IggyError::ConsumerGroupMemberNotFound(
            member_id,
            self.group_id,
            self.topic_id,
        ))
    }

    pub async fn add_member(&mut self, member_id: u32) {
        self.members.insert(
            member_id,
            RwLock::new(ConsumerGroupMember {
                id: member_id,
                partitions: AHashMap::new(),
                current_partition_index: None,
                current_partition_id: None,
            }),
        );
        trace!(
            "Added member with ID: {} to consumer group: {} for topic with ID: {}",
            member_id, self.group_id, self.topic_id
        );
        self.assign_partitions().await;
    }

    pub async fn delete_member(&mut self, member_id: u32) {
        if self.members.remove(&member_id).is_some() {
            trace!(
                "Deleted member with ID: {} in consumer group: {} for topic with ID: {}",
                member_id, self.group_id, self.topic_id
            );
            self.assign_partitions().await;
        }
    }

    async fn assign_partitions(&mut self) {
        let mut members = self.members.values_mut().collect::<Vec<_>>();
        if members.is_empty() {
            return;
        }

        let members_count = members.len() as u32;
        for member in members.iter_mut() {
            let mut member = member.write().await;
            member.current_partition_index = None;
            member.current_partition_id = None;
            member.partitions.clear();
        }

        for partition_index in 0..self.partitions_count {
            let partition_id = partition_index + 1;
            let member_index = partition_index % members_count;
            let member = members.get(member_index as usize).unwrap();
            let mut member = member.write().await;
            let member_partition_index = member.partitions.len() as u32;
            member
                .partitions
                .insert(member_partition_index, partition_id);
            if member.current_partition_id.is_none() {
                member.current_partition_id = Some(partition_id);
                member.current_partition_index = Some(member_partition_index);
            }
            trace!(
                "Assigned partition ID: {} to member with ID: {} for topic with ID: {} in consumer group: {}",
                partition_id, member.id, self.topic_id, self.group_id
            )
        }
    }
}

impl ConsumerGroupMember {
    pub fn get_partitions(&self) -> Vec<u32> {
        self.partitions.values().copied().collect()
    }

    pub fn calculate_partition_id(&mut self) -> Option<u32> {
        let partition_index = self.current_partition_index?;
        let Some(partition_id) = self.partitions.get(&partition_index) else {
            trace!(
                "No partition ID found for index: {} for member with ID: {}.",
                partition_index, self.id
            );
            return None;
        };

        let partition_id = *partition_id;
        self.current_partition_id = Some(partition_id);
        if self.partitions.len() <= (partition_index + 1) as usize {
            self.current_partition_index = Some(0);
        } else {
            self.current_partition_index = Some(partition_index + 1);
        }
        trace!(
            "Calculated partition ID: {} for member with ID: {}",
            partition_id, self.id
        );
        Some(partition_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn should_calculate_partition_id_using_round_robin() {
        let member_id = 123;
        let mut consumer_group = ConsumerGroup {
            topic_id: 1,
            group_id: 1,
            name: "test".to_string(),
            partitions_count: 3,
            members: AHashMap::new(),
        };

        consumer_group.add_member(member_id).await;
        for i in 0..1000 {
            let partition_id = consumer_group
                .calculate_partition_id(member_id)
                .await
                .unwrap()
                .expect("Partition ID not found");
            assert_eq!(partition_id, (i % consumer_group.partitions_count) + 1);
        }
    }

    #[tokio::test]
    async fn should_assign_all_partitions_to_the_only_single_member() {
        let member_id = 123;
        let mut consumer_group = ConsumerGroup {
            topic_id: 1,
            group_id: 1,
            name: "test".to_string(),
            partitions_count: 3,
            members: AHashMap::new(),
        };

        consumer_group.add_member(member_id).await;
        let member = consumer_group.members.get(&member_id).unwrap();
        let member = member.read().await;
        assert_eq!(
            member.partitions.len() as u32,
            consumer_group.partitions_count
        );
        let member_partitions = member.partitions.values().collect::<Vec<_>>();
        for partition_id in 1..=consumer_group.partitions_count {
            assert!(member_partitions.contains(&&partition_id));
        }
    }

    #[tokio::test]
    async fn should_assign_partitions_to_the_multiple_members() {
        let member1_id = 123;
        let member2_id = 456;
        let mut consumer_group = ConsumerGroup {
            topic_id: 1,
            group_id: 1,
            name: "test".to_string(),
            partitions_count: 3,
            members: AHashMap::new(),
        };

        consumer_group.add_member(member1_id).await;
        consumer_group.add_member(member2_id).await;
        let member1 = consumer_group.members.get(&member1_id).unwrap();
        let member2 = consumer_group.members.get(&member2_id).unwrap();
        let member1 = member1.read().await;
        let member2 = member2.read().await;
        assert_eq!(
            member1.partitions.len() + member2.partitions.len(),
            consumer_group.partitions_count as usize
        );
        let member1_partitions = member1.partitions.values().collect::<Vec<_>>();
        let member2_partitions = member2.partitions.values().collect::<Vec<_>>();
        let members_partitions = member1_partitions
            .into_iter()
            .chain(member2_partitions.into_iter())
            .collect::<Vec<_>>();
        assert_eq!(
            members_partitions.len(),
            consumer_group.partitions_count as usize
        );
        for partition_id in 1..=consumer_group.partitions_count {
            assert!(members_partitions.contains(&&partition_id));
        }
    }

    #[tokio::test]
    async fn should_assign_only_single_partition_to_the_only_single_member() {
        let member1_id = 123;
        let member2_id = 456;
        let mut consumer_group = ConsumerGroup {
            topic_id: 1,
            group_id: 1,
            name: "test".to_string(),
            partitions_count: 1,
            members: AHashMap::new(),
        };

        consumer_group.add_member(member1_id).await;
        consumer_group.add_member(member2_id).await;
        let member1 = consumer_group.members.get(&member1_id).unwrap();
        let member2 = consumer_group.members.get(&member2_id).unwrap();
        let member1 = member1.read().await;
        let member2 = member2.read().await;
        if member1.partitions.len() == 1 {
            assert_eq!(member2.partitions.len(), 0);
        } else {
            assert_eq!(member1.partitions.len(), 0);
            assert_eq!(member2.partitions.len(), 1);
        }
    }
}
