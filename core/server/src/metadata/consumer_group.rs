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

use crate::metadata::consumer_group_member::{
    CompletableRevocation, ConsumerGroupMemberMeta, PendingRevocation,
};
use crate::metadata::partition::PartitionMeta;
use crate::metadata::{ConsumerGroupId, PartitionId};
use crate::streaming::polling_consumer::ConsumerGroupId as CgId;
use iggy_common::IggyTimestamp;
use slab::Slab;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::warn;

#[derive(Clone, Debug)]
pub struct ConsumerGroupMeta {
    pub id: ConsumerGroupId,
    pub name: Arc<str>,
    pub partitions: Vec<PartitionId>,
    pub members: Slab<ConsumerGroupMemberMeta>,
}

impl ConsumerGroupMeta {
    /// Full rebalance: clear all assignments and redistribute round-robin.
    /// Used when a member leaves or partition count changes.
    pub fn rebalance_members(&mut self) {
        let partition_count = self.partitions.len();
        let member_count = self.members.len();

        if member_count == 0 || partition_count == 0 {
            return;
        }

        // Clear all member partitions and pending revocations
        let member_ids: Vec<usize> = self.members.iter().map(|(id, _)| id).collect();
        for &member_id in &member_ids {
            if let Some(member) = self.members.get_mut(member_id) {
                member.partitions.clear();
                member.pending_revocations.clear();
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

    /// Cooperative rebalance: assign unassigned partitions to idle members and
    /// mark excess partitions on over-assigned members as pending revocation.
    pub fn rebalance_cooperative(&mut self) {
        let member_count = self.members.len();
        if member_count == 0 || self.partitions.is_empty() {
            return;
        }

        // Find which partitions are already assigned
        let mut assigned: HashSet<PartitionId> = HashSet::new();
        for (_, member) in self.members.iter() {
            for &pid in &member.partitions {
                assigned.insert(pid);
            }
        }

        // Step 1: Assign unassigned partitions to idle members
        let unassigned: Vec<PartitionId> = self
            .partitions
            .iter()
            .copied()
            .filter(|pid| !assigned.contains(pid))
            .collect();

        if !unassigned.is_empty() {
            let idle_member_ids: Vec<usize> = self
                .members
                .iter()
                .filter(|(_, m)| m.partitions.is_empty())
                .map(|(id, _)| id)
                .collect();

            if !idle_member_ids.is_empty() {
                for (i, partition_id) in unassigned.into_iter().enumerate() {
                    let member_idx = i % idle_member_ids.len();
                    if let Some(member) = self.members.get_mut(idle_member_ids[member_idx]) {
                        member.partitions.push(partition_id);
                    }
                }
            }
        }

        // Step 2: Mark excess partitions as pending revocation
        let partition_count = self.partitions.len();
        let fair_share = partition_count / member_count;
        let remainder = partition_count % member_count;

        // Collect members already targeted by a pending revocation
        let revocation_targets: HashSet<usize> = self
            .members
            .iter()
            .flat_map(|(_, m)| {
                m.pending_revocations
                    .iter()
                    .map(|revocation| revocation.target_slab_id)
            })
            .collect();

        // Collect idle members (no partitions and not already a revocation target)
        let mut idle_slab_ids: Vec<usize> = self
            .members
            .iter()
            .filter(|(id, m)| m.partitions.is_empty() && !revocation_targets.contains(id))
            .map(|(id, _)| id)
            .collect();

        if idle_slab_ids.is_empty() {
            return;
        }

        // Find over-assigned members and mark excess as pending revocation
        let member_ids: Vec<usize> = self.members.iter().map(|(id, _)| id).collect();
        let mut members_with_remainder = remainder;

        for &mid in &member_ids {
            if idle_slab_ids.is_empty() {
                break;
            }
            let effective_count = self
                .members
                .get(mid)
                .map(|m| {
                    let pending: HashSet<PartitionId> = m
                        .pending_revocations
                        .iter()
                        .map(|revocation| revocation.partition_id)
                        .collect();
                    m.partitions.iter().filter(|p| !pending.contains(p)).count()
                })
                .unwrap_or(0);

            // This member's max allowed partitions
            let max_allowed = if members_with_remainder > 0 {
                fair_share + 1
            } else {
                fair_share
            };

            if effective_count <= max_allowed {
                if effective_count > fair_share {
                    members_with_remainder = members_with_remainder.saturating_sub(1);
                }
                continue;
            }

            // Mark excess partitions as pending revocation (from the END of the list)
            let excess_count = effective_count - max_allowed;
            if members_with_remainder > 0 && effective_count > fair_share {
                members_with_remainder = members_with_remainder.saturating_sub(1);
            }

            // Collect partitions eligible for revocation (not already pending)
            let revocable: Vec<PartitionId> = self
                .members
                .get(mid)
                .map(|m| {
                    let pending: HashSet<PartitionId> = m
                        .pending_revocations
                        .iter()
                        .map(|revocation| revocation.partition_id)
                        .collect();
                    m.partitions
                        .iter()
                        .rev()
                        .filter(|p| !pending.contains(p))
                        .copied()
                        .collect()
                })
                .unwrap_or_default();

            for partition_id in revocable.into_iter().take(excess_count) {
                if idle_slab_ids.is_empty() {
                    break;
                }
                let idle_id = idle_slab_ids.remove(0);
                let target_member_id = self
                    .members
                    .get(idle_id)
                    .map(|m| m.id)
                    .unwrap_or(usize::MAX);
                if let Some(member) = self.members.get_mut(mid) {
                    member.pending_revocations.push(PendingRevocation {
                        partition_id,
                        target_slab_id: idle_id,
                        target_member_id,
                        created_at_micros: IggyTimestamp::now().as_micros(),
                    });
                }
            }
        }
    }

    /// Find revocations completable immediately (never polled or already committed).
    pub fn find_completable_revocations(
        &self,
        partitions: &[PartitionMeta],
        cg_id: CgId,
    ) -> Vec<CompletableRevocation> {
        let mut result = Vec::new();
        for (slab_id, member) in self.members.iter() {
            for revocation in &member.pending_revocations {
                let partition = match partitions.get(revocation.partition_id) {
                    Some(p) => p,
                    None => continue,
                };
                let last_polled = {
                    let guard = partition.last_polled_offsets.pin();
                    guard.get(&cg_id).map(|v| v.load(Ordering::Acquire))
                };
                let can_complete = match last_polled {
                    None => true,
                    Some(polled) => {
                        let offsets_guard = partition.consumer_group_offsets.pin();
                        offsets_guard
                            .get(&cg_id)
                            .map(|offset| offset.offset.load(Ordering::Acquire))
                            .is_some_and(|committed| committed >= polled)
                    }
                };
                if can_complete {
                    result.push(CompletableRevocation {
                        slab_id,
                        member_id: member.id,
                        partition_id: revocation.partition_id,
                    });
                }
            }
        }
        result
    }

    /// Complete a pending revocation, moving the partition to the target member.
    pub fn complete_revocation(
        &mut self,
        member_slab_id: usize,
        member_id: usize,
        partition_id: PartitionId,
    ) -> bool {
        let target_info = if let Some(member) = self.members.get_mut(member_slab_id) {
            if member.id != member_id {
                warn!(
                    "Revocation rejected: member ID mismatch (slab={member_slab_id}, expected={member_id}, actual={})",
                    member.id
                );
                return false;
            }
            let pos = member
                .pending_revocations
                .iter()
                .position(|revocation| revocation.partition_id == partition_id);
            if let Some(pos) = pos {
                let removed = member.pending_revocations.remove(pos);
                member.partitions.retain(|&p| p != partition_id);
                Some((removed.target_slab_id, removed.target_member_id))
            } else {
                warn!(
                    "Revocation rejected: no pending revocation for partition={partition_id} on slab={member_slab_id}"
                );
                None
            }
        } else {
            warn!("Revocation rejected: source slab={member_slab_id} not found");
            None
        };

        if let Some((target_slab, expected_target_id)) = target_info {
            if let Some(target_member) = self.members.get_mut(target_slab) {
                if target_member.id != expected_target_id {
                    warn!(
                        "Revocation target slab={target_slab} reused (expected={expected_target_id}, actual={}), full rebalance",
                        target_member.id
                    );
                    self.rebalance_members();
                    return true;
                }
                target_member.partitions.push(partition_id);
                return true;
            }
            warn!("Revocation target slab={target_slab} gone, full rebalance");
            self.rebalance_members();
            return true;
        }

        false
    }
}
