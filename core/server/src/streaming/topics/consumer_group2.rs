use crate::{
    binary::handlers::partitions,
    slab::{IndexedSlab, Keyed, partitions::PARTITIONS_CAPACITY},
};
use ahash::AHashMap;
use arcshift::ArcShift;
use iggy_common::IggyError;
use slab::Slab;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Context,
};
use tracing::trace;

pub const MEMBERS_CAPACITY: usize = 128;

#[derive(Default, Debug)]
pub struct ConsumerGroup {
    id: usize,
    name: String,
    partitions: Vec<usize>,
    members: ArcShift<Slab<Member>>,
}

impl ConsumerGroup {
    pub fn new(name: String, members: ArcShift<Slab<Member>>, partitions: Vec<usize>) -> Self {
        ConsumerGroup {
            id: 0,
            name,
            partitions,
            members,
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn insert_into(self, container: &mut IndexedSlab<Self>) -> usize {
        let idx = container.insert(self);
        let group = &mut container[idx];
        group.id = idx;
        idx
    }

    pub fn with_members<T>(&self, f: impl FnOnce(&Slab<Member>) -> T) -> T {
        f(&self.members.shared_get())
    }

    pub fn reassign_partitions(&mut self, partitions: Vec<usize>) {
        self.partitions = partitions;
        let mut members = self.mimic_members();
        self.assign_partitions_to_members(&mut members);
        self.members.update(members);
    }

    pub fn calculate_partition_id_unchecked(&self, member_id: usize) -> Option<usize> {
        self.with_members(|members| {
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
        })
    }

    pub fn get_current_partition_id_unchecked(&self, member_id: usize) -> Option<usize> {
        self.with_members(|members| {
            let member = &members[member_id];
            if member.partitions.is_empty() {
                return None;
            }

            let partition_idx = member.current_partition_idx.load(Ordering::Relaxed);
            Some(member.partitions[partition_idx])
        })
    }

    pub fn add_member(&mut self, client_id: u32) {
        let mut members = self.mimic_members();
        Member::new(client_id).insert_into(&mut members);
        self.assign_partitions_to_members(&mut members);
        self.members.update(members);
    }

    pub fn delete_member(&mut self, client_id: u32) {
        let member_id = self
            .members
            .shared_get()
            .iter()
            .find_map(|(_, member)| (member.client_id == client_id).then_some(member.id))
            .expect("delete_member: find member in consumer group slab");
        let mut members = self.mimic_members();
        members.remove(member_id);
        members.compact(|entry, _, idx| {
            entry.id = idx;
            true
        });
        self.assign_partitions_to_members(&mut members);
        self.members.update(members);
    }

    fn mimic_members(&self) -> Slab<Member> {
        let mut container = Slab::with_capacity(self.members.shared_get().len());
        self.with_members(|members| {
            for (_, member) in members {
                Member::new(member.client_id).insert_into(&mut container);
            }
        });
        container
    }

    fn assign_partitions_to_members(&self, members: &mut Slab<Member>) {
        members
            .iter_mut()
            .for_each(|(_, member)| member.partitions.clear());
        let count = members.len();
        for (idx, partition) in self.partitions.iter().enumerate() {
            let position = idx % count;
            let member = &mut members[position];
            member.partitions.push(*partition);
            trace!(
                "Assigned partition ID: {} to member with ID: {} in consumer group: {}",
                partition, member.id, self.id
            );
        }
    }
}

#[derive(Debug)]
pub struct Member {
    id: usize,
    client_id: u32,
    partitions: Vec<usize>,
    current_partition_idx: AtomicUsize,
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

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn client_id(&self) -> u32 {
        self.client_id
    }

    pub fn insert_into(self, container: &mut Slab<Self>) -> usize {
        let idx = container.insert(self);
        let member = &mut container[idx];
        member.id = idx;
        idx
    }
}

impl Keyed for ConsumerGroup {
    type Key = String;

    fn key(&self) -> &Self::Key {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arcshift::ArcShift;
    use slab::Slab;
    use std::sync::atomic::Ordering;
    use test_case::test_matrix;

    fn create_test_consumer_group(name: &str, member_count: usize) -> ConsumerGroup {
        let members = bootstrap_members(member_count);
        let partitions = Vec::with_capacity(128);
        ConsumerGroup::new(name.to_string(), members, partitions)
    }

    fn bootstrap_members(members_count: usize) -> ArcShift<Slab<Member>> {
        let mut members = Slab::with_capacity(MEMBERS_CAPACITY);
        for i in 0..members_count {
            Member::new(i as u32 + 1).insert_into(&mut members);
        }
        ArcShift::new(members)
    }

    #[test_matrix([1, 10, 25, 34, 100, 255, 512, 1024, 4096, 8192, 16384])]
    #[test]
    fn test_initial_creation_with_few_members(members_count: usize) {
        let consumer_group = create_test_consumer_group("test_group", members_count);

        assert_eq!(consumer_group.name, "test_group");
        assert_eq!(consumer_group.id, 0); // Initial ID should be 0

        consumer_group.with_members(|members| {
            assert_eq!(members.len(), members_count);

            for (idx, member) in members.iter() {
                assert_eq!(member.id(), idx);
                assert_eq!(member.client_id(), (idx + 1) as u32);
                assert_eq!(member.current_partition_idx.load(Ordering::Relaxed), 0);
                assert!(member.partitions.is_empty());
            }
        });
    }

    #[test_matrix([10, 20, 25, 34, 100, 255, 512, 1024, 4096, 8192, 16384],
        [1, 2, 3, 4 , 5, 6, 7, 8, 9, 12])]
    #[test]
    fn test_consumer_group_partition_assignment(members_count: usize, multiplier: usize) {
        let partitions = (0..(members_count * multiplier)).collect::<Vec<_>>();
        let mut consumer_group = create_test_consumer_group("test_group", members_count);
        consumer_group.reassign_partitions(partitions);

        consumer_group.with_members(|members| {
            assert!(members.len() > 0);
            for (_, member) in members.iter() {
                assert_eq!(member.partitions.len(), multiplier);
                for (idx, partition) in member.partitions.iter().enumerate() {
                    assert_eq!(*partition, (member.id + (members_count * idx)));
                }
            }
        });
    }

    #[test_matrix([10, 20, 26, 30, 60, 100], [10, 25, 32, 45, 96, 100, 128, 256, 321], [1, 2, 3, 4, 5, 6, 7, 8, 9])]
    #[test]
    fn test_consumer_group_join_members(
        mut members_count: usize,
        partitions_count: usize,
        to_join: usize,
    ) {
        let partitions = (0..partitions_count).collect::<Vec<_>>();
        let mut consumer_group = create_test_consumer_group("test_group", members_count);
        consumer_group.reassign_partitions(partitions);

        for client_id in 0..to_join {
            consumer_group.add_member(client_id as u32);
            members_count += 1;
            let base = partitions_count / members_count;
            let remainder = partitions_count % members_count;
            for (idx, member) in consumer_group.members.shared_get().iter() {
                let len = if idx < remainder { base + 1 } else { base };
                assert_eq!(member.partitions.len(), len);
            }
        }

        for client_id in 0..to_join {
            consumer_group.delete_member(client_id as u32);
            members_count -= 1;
            let base = partitions_count / members_count;
            let remainder = partitions_count % members_count;
            for (idx, member) in consumer_group.members.shared_get().iter() {
                let len = if idx < remainder { base + 1 } else { base };
                assert_eq!(member.partitions.len(), len);
            }
        }
    }
}
