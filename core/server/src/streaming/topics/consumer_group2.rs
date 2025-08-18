use crate::slab::{
    Keyed, consumer_groups, partitions,
    traits_ext::{EntityMarker, IntoComponents, IntoComponentsById},
};
use arcshift::ArcShift;
use slab::Slab;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::trace;

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

    pub fn partitions_mut(&mut self) -> &mut Vec<partitions::ContainerId> {
        &mut self.partitions
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

// TODO: Create a macro to impl those ConsumerGroupRef/ConsumerGroupRefMut structs and it's traits.
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

    pub fn reassign_partitions(&mut self, partitions: Vec<partitions::ContainerId>) {
        self.root.partitions = partitions;
        self.members.rcu(|members| {
            let mut members = Self::mimic_members(members);
            let partitions = self.root.partitions.clone();
            Self::assign_partitions_to_members(self.id, &mut members, partitions);
            members
        });
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
            id: self.id.clone(),
            client_id: self.client_id.clone(),
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
    fn test_initial_creation_with_few_members(members_count: usize) {
        let consumer_group = create_test_consumer_group("test_group", members_count);

        assert_eq!(consumer_group.root.name, "test_group");
        assert_eq!(consumer_group.root.id, 0); // Initial ID should be 0

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
