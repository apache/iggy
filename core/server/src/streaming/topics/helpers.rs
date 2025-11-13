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

use std::sync::{Arc, atomic::Ordering};

use crate::{
    slab::{
        Keyed,
        consumer_groups::{self, ConsumerGroups},
        topics::{self, Topics},
        traits_ext::{
            ComponentsById, Delete, DeleteCell, EntityComponentSystem, EntityComponentSystemMut,
            EntityMarker, IntoComponents,
        },
    },
    streaming::{
        stats::TopicStats,
        topics::{
            consumer_group::{
                self, ConsumerGroupMembers, ConsumerGroupRef, ConsumerGroupRefMut, Member,
            },
            topic::{Topic, TopicRef, TopicRefMut, TopicRoot},
        },
        utils::hash,
    },
};
use iggy_common::{CompressionAlgorithm, Identifier, IggyExpiry, MaxTopicSize};
use slab::Slab;

pub fn rename_index(
    old_name: &<TopicRoot as Keyed>::Key,
    new_name: String,
) -> impl FnOnce(&Topics) {
    move |topics| {
        topics.with_index_mut(|index| {
            // Rename the key inside of hashmap
            let idx = index.remove(old_name).expect("Rename key: key not found");
            index.insert(new_name, idx);
        })
    }
}

// Topics
pub fn get_stats() -> impl FnOnce(ComponentsById<TopicRef>) -> Arc<TopicStats> {
    |(_, _, stats)| stats.clone()
}

pub fn get_topic_id() -> impl FnOnce(ComponentsById<TopicRef>) -> topics::ContainerId {
    |(root, _, _)| root.id()
}

pub fn get_partition_ids() -> impl FnOnce(ComponentsById<TopicRef>) -> Vec<usize> {
    |(root, ..)| {
        root.partitions().with_components(|components| {
            let (roots, ..) = components.into_components();
            roots.iter().map(|(id, _)| id).collect()
        })
    }
}

pub fn get_message_expiry() -> impl FnOnce(ComponentsById<TopicRef>) -> IggyExpiry {
    |(root, _, _)| root.message_expiry()
}

pub fn get_max_topic_size() -> impl FnOnce(ComponentsById<TopicRef>) -> MaxTopicSize {
    |(root, _, _)| root.max_topic_size()
}

pub fn get_topic_size_info() -> impl FnOnce(ComponentsById<TopicRef>) -> (bool, bool) {
    |(root, _, stats)| {
        let max_size = root.max_topic_size();
        let current_size = stats.size_bytes_inconsistent();
        let is_unlimited = matches!(max_size, MaxTopicSize::Unlimited);
        let is_almost_full = if !is_unlimited {
            let max_bytes = max_size.as_bytes_u64();
            // Consider "almost full" as 90% capacity
            current_size >= (max_bytes * 9 / 10)
        } else {
            false
        };
        (is_unlimited, is_almost_full)
    }
}

pub fn calculate_partition_id_by_messages_key_hash(
    upperbound: usize,
    messages_key: &[u8],
) -> usize {
    let messages_key_hash = hash::calculate_32(messages_key) as usize;
    let partition_id = messages_key_hash % upperbound;
    tracing::trace!(
        "Calculated partition ID: {} for messages key: {:?}, hash: {}",
        partition_id,
        messages_key,
        messages_key_hash
    );
    partition_id
}

pub fn delete_topic(topic_id: &Identifier) -> impl FnOnce(&Topics) -> Topic {
    |container| {
        let id = container.get_index(topic_id);
        let topic = container.delete(id);
        assert_eq!(topic.id(), id, "delete_topic: topic ID mismatch");
        topic
    }
}

pub fn exists(identifier: &Identifier) -> impl FnOnce(&Topics) -> bool {
    |topics| topics.exists(identifier)
}

pub fn cg_exists(identifier: &Identifier) -> impl FnOnce(ComponentsById<TopicRef>) -> bool {
    |(root, ..)| root.consumer_groups().exists(identifier)
}

pub fn update_topic(
    name: String,
    message_expiry: IggyExpiry,
    compression_algorithm: CompressionAlgorithm,
    max_topic_size: MaxTopicSize,
    replication_factor: u8,
) -> impl FnOnce(ComponentsById<TopicRefMut>) -> (String, String) {
    move |(mut root, _, _)| {
        let old_name = root.name().clone();
        root.set_name(name.clone());
        root.set_message_expiry(message_expiry);
        root.set_compression(compression_algorithm);
        root.set_max_topic_size(max_topic_size);
        root.set_replication_factor(replication_factor);
        (old_name, name)
        // TODO: Set message expiry for all partitions and segments.
    }
}

// Consumer Groups
pub fn get_consumer_group_id()
-> impl FnOnce(ComponentsById<ConsumerGroupRef>) -> consumer_groups::ContainerId {
    |(root, ..)| root.id()
}

pub fn delete_consumer_group(
    group_id: &Identifier,
) -> impl FnOnce(&mut ConsumerGroups) -> consumer_group::ConsumerGroup {
    |container| {
        let id = container.get_index(group_id);
        let group = container.delete(id);
        assert_eq!(group.id(), id, "delete_consumer_group: group ID mismatch");
        group
    }
}

pub fn join_consumer_group(client_id: u32) -> impl FnOnce(ComponentsById<ConsumerGroupRefMut>) {
    move |(root, members)| {
        let partitions = root.partitions();
        let id = root.id();
        add_member(id, members, partitions, client_id);
    }
}

pub fn leave_consumer_group(
    client_id: u32,
) -> impl FnOnce(ComponentsById<ConsumerGroupRefMut>) -> Option<usize> {
    move |(root, members)| {
        let partitions = root.partitions();
        let id = root.id();
        delete_member(id, client_id, members, partitions)
    }
}

pub fn rebalance_consumer_group() -> impl FnOnce(ComponentsById<ConsumerGroupRefMut>) {
    move |(root, members)| {
        let partitions = root.partitions();
        let id = root.id();
        members.inner_mut().rcu(|existing_members| {
            let mut new_members = mimic_members(existing_members);
            assign_partitions_to_members(id, &mut new_members, partitions);
            new_members
        });
    }
}

pub fn rebalance_consumer_groups(
    partition_ids: &[usize],
) -> impl FnOnce(ComponentsById<TopicRefMut>) {
    move |(mut root, ..)| {
        root.consumer_groups_mut()
            .with_components_mut(|components| {
                let (all_roots, all_members) = components.into_components();
                for ((_, consumer_group_root), (_, members)) in
                    all_roots.iter().zip(all_members.iter_mut())
                {
                    let id = consumer_group_root.id();
                    members.inner_mut().rcu(|existing_members| {
                        let mut new_members = mimic_members(existing_members);
                        assign_partitions_to_members(id, &mut new_members, partition_ids);
                        new_members
                    });
                }
            });
    }
}

pub fn get_consumer_group_member_id(
    client_id: u32,
) -> impl FnOnce(ComponentsById<ConsumerGroupRef>) -> Option<usize> {
    move |(_, members)| {
        members
            .inner()
            .shared_get()
            .iter()
            .find_map(|(_, member)| (member.client_id == client_id).then_some(member.id))
    }
}

pub fn calculate_partition_id_unchecked(
    member_id: usize,
) -> impl FnOnce(ComponentsById<ConsumerGroupRef>) -> Option<usize> {
    move |(_, members)| {
        let members = members.inner().shared_get();
        let member = &members[member_id];
        if member.partitions.is_empty() {
            return None;
        }

        let partitions_count = member.partitions.len();
        // It's OK to use `Relaxed` ordering, because we have 1-1 mapping between consumer and member.
        // We allow only one consumer to access topic in a given shard
        // therefore there is no contention on the member's current partition index.
        let current = member
            .current_partition_idx
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some((current + 1) % partitions_count)
            })
            .expect("fetch_and_update partition id for consumer group member");
        Some(member.partitions[current])
    }
}

pub fn get_current_partition_id_unchecked(
    member_id: usize,
) -> impl FnOnce(ComponentsById<ConsumerGroupRef>) -> Option<usize> {
    move |(_, members)| {
        let members = members.inner().shared_get();
        let member = &members[member_id];
        if member.partitions.is_empty() {
            return None;
        }

        let partition_idx = member.current_partition_idx.load(Ordering::Relaxed);
        Some(member.partitions[partition_idx])
    }
}

fn add_member(id: usize, members: &mut ConsumerGroupMembers, partitions: &[usize], client_id: u32) {
    members.inner_mut().rcu(move |members| {
        let mut members = mimic_members(members);
        Member::new(client_id).insert_into(&mut members);
        assign_partitions_to_members(id, &mut members, partitions);
        members
    });
}

fn delete_member(
    id: usize,
    client_id: u32,
    members: &mut ConsumerGroupMembers,
    partitions: &[usize],
) -> Option<usize> {
    let member_ids: Vec<usize> = members
        .inner()
        .shared_get()
        .iter()
        .filter_map(|(_, member)| (member.client_id == client_id).then_some(member.id))
        .collect();

    if member_ids.is_empty() {
        return None;
    }

    members.inner_mut().rcu(|members| {
        let mut members = mimic_members(members);
        for member_id in &member_ids {
            members.remove(*member_id);
        }
        members.compact(|entry, _, idx| {
            entry.id = idx;
            true
        });
        assign_partitions_to_members(id, &mut members, partitions);
        members
    });

    Some(member_ids[0])
}

fn assign_partitions_to_members(id: usize, members: &mut Slab<Member>, partitions: &[usize]) {
    members
        .iter_mut()
        .for_each(|(_, member)| member.partitions.clear());
    let count = members.len();
    if count == 0 {
        return;
    }

    for (idx, partition) in partitions.iter().enumerate() {
        let position = idx % count;
        let member = &mut members[position];
        member.partitions.push(*partition);
        tracing::trace!(
            "Assigned partition ID: {} to member with ID: {} in consumer group: {}",
            partition,
            member.id,
            id
        );
    }
}

fn mimic_members(members: &Slab<Member>) -> Slab<Member> {
    let mut container = Slab::with_capacity(members.len());
    for (_, member) in members {
        Member::new(member.client_id).insert_into(&mut container);
    }
    container
}
