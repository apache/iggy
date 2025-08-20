use std::sync::{Arc, atomic::Ordering};

use crate::{
    shard_trace,
    slab::{
        Keyed,
        consumer_groups::{self, ConsumerGroups},
        partitions::{self, Partitions},
        topics::{self, Topics},
        traits_ext::{
            ComponentsById, Delete, DeleteCell, EntityComponentSystem, EntityComponentSystemMut,
            EntityMarker, Insert, IntoComponents,
        },
    },
    streaming::{
        partitions::partition2,
        stats::stats::TopicStats,
        topics::{
            consumer_group2::{
                self, ConsumerGroupMembers, ConsumerGroupRef, ConsumerGroupRefMut,
                MEMBERS_CAPACITY, Member,
            },
            topic2::{Topic, TopicRef, TopicRefMut, TopicRoot},
        },
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
    |(_, stats)| stats.clone()
}

pub fn get_topic_id() -> impl FnOnce(ComponentsById<TopicRef>) -> topics::ContainerId {
    |(root, _)| root.id()
}

pub fn get_message_expiry() -> impl FnOnce(ComponentsById<TopicRef>) -> IggyExpiry {
    |(root, _)| root.message_expiry()
}

pub fn get_max_topic_size() -> impl FnOnce(ComponentsById<TopicRef>) -> MaxTopicSize {
    |(root, _)| root.max_topic_size()
}

pub fn delete_topic(topic_id: &Identifier) -> impl FnOnce(&Topics) -> Topic {
    |container| {
        let id = container.get_index(topic_id);
        let topic = container.delete(id);
        assert_eq!(topic.id(), id, "delete_topic: topic ID mismatch");
        topic
    }
}

pub fn purge_topic_disk() -> impl AsyncFnOnce(ComponentsById<TopicRef>) {
    async |(root, ..)| {}
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
    move |(mut root, _)| {
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
) -> impl FnOnce(&mut ConsumerGroups) -> consumer_group2::ConsumerGroup {
    |container| {
        let id = container.get_index(group_id);
        let group = container.delete(id);
        assert_eq!(group.id(), id, "delete_consumer_group: group ID mismatch");
        group
    }
}

pub fn join_consumer_group(
    shard_id: u16,
    client_id: u32,
) -> impl FnOnce(ComponentsById<ConsumerGroupRefMut>) {
    move |(root, members)| {
        let partitions = root.partitions();
        let id = root.id();
        add_member(shard_id, id, members, partitions, client_id);
    }
}

pub fn leave_consumer_group(
    shard_id: u16,
    client_id: u32,
) -> impl FnOnce(ComponentsById<ConsumerGroupRefMut>) {
    move |(root, members)| {
        let partitions = root.partitions();
        let id = root.id();
        delete_member(shard_id, id, client_id, members, partitions);
    }
}

pub fn get_consumer_group_member_id(
    client_id: u32,
) -> impl FnOnce(ComponentsById<ConsumerGroupRef>) -> usize {
    move |(_, members)| {
        members
            .inner()
            .shared_get()
            .iter()
            .find_map(|(_, member)| (member.client_id == client_id).then_some(member.id))
            .expect("get_member_id: find member in consumer group slab")
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

fn add_member(
    shard_id: u16,
    id: usize,
    members: &mut ConsumerGroupMembers,
    partitions: &Vec<usize>,
    client_id: u32,
) {
    members.inner_mut().rcu(move |members| {
        let mut members = mimic_members(members);
        Member::new(client_id).insert_into(&mut members);
        assign_partitions_to_members(shard_id, id, &mut members, partitions.clone());
        members
    });
}

fn delete_member(
    shard_id: u16,
    id: usize,
    client_id: u32,
    members: &mut ConsumerGroupMembers,
    partitions: &Vec<usize>,
) {
    let member_id = members
        .inner()
        .shared_get()
        .iter()
        .find_map(|(_, member)| (member.client_id == client_id).then_some(member.id))
        .expect("delete_member: find member in consumer group slab");
    members.inner_mut().rcu(|members| {
        let mut members = mimic_members(members);
        members.remove(member_id);
        members.compact(|entry, _, idx| {
            entry.id = idx;
            true
        });
        assign_partitions_to_members(shard_id, id, &mut members, partitions.clone());
        members
    });
}

fn assign_partitions_to_members(
    shard_id: u16,
    id: usize,
    members: &mut Slab<Member>,
    partitions: Vec<usize>,
) {
    members
        .iter_mut()
        .for_each(|(_, member)| member.partitions.clear());
    let count = members.len();
    for (idx, partition) in partitions.iter().enumerate() {
        let position = idx % count;
        let member = &mut members[position];
        member.partitions.push(*partition);
        shard_trace!(
            shard_id,
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

fn reassign_partitions(
    shard_id: u16,
    partitions: Vec<partitions::ContainerId>,
) -> impl FnOnce(ComponentsById<ConsumerGroupRefMut>) {
    move |(root, members)| {
        root.assign_partitions(partitions);
        let partitions = root.partitions();
        let id = root.id();
        reassign_partitions_to_members(shard_id, id, members, partitions);
    }
}

fn reassign_partitions_to_members(
    shard_id: u16,
    id: usize,
    members: &mut ConsumerGroupMembers,
    partitions: &Vec<usize>,
) {
    members.inner_mut().rcu(move |members| {
        let mut members = mimic_members(members);
        assign_partitions_to_members(shard_id, id, &mut members, partitions.clone());
        members
    });
}
