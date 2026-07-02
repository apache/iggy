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

//! Consumer groups, co-located inside the topic node of the Streams STM.
//!
//! Groups belong to a topic, so they live on [`crate::stm::stream::Topic`]:
//! deleting a stream/topic drops its groups for free (the topic's collections
//! are dropped with it), and group operations are applied by the Streams STM.
//!
//! Group ids are **monotonic per topic and never reused** (`next_consumer_group_id`).
//! The consumer-group offset (on the partition plane) is keyed by group id, so a
//! never-reused id guarantees a new group can never inherit a deleted group's
//! offset -- making a metadata->offset purge unnecessary for correctness.

use crate::stm::StateHandler;
use crate::stm::result::{ApplyReply, CreateConsumerGroupResult, DeleteConsumerGroupResult};
use crate::stm::stream::StreamsInner;
use bytes::Bytes;

use bytes::{BufMut, BytesMut};
use iggy_binary_protocol::WireIdentifier;
use iggy_binary_protocol::codec::{
    WireDecode, WireEncode, capped_capacity, read_u32_le, read_u64_le, read_u128_le,
};
use iggy_binary_protocol::requests::consumer_groups::{
    CreateConsumerGroupRequest, DeleteConsumerGroupRequest,
};
use iggy_binary_protocol::responses::consumer_groups::consumer_group_response::ConsumerGroupResponse;
use iggy_binary_protocol::responses::consumer_groups::get_consumer_group::ConsumerGroupDetailsResponse;
use serde::{Deserialize, Serialize};
use slab::Slab;
use std::sync::Arc;

/// A partition being cooperatively handed off from this member to `target`.
///
/// The source keeps polling/committing it until the handoff completes (the
/// consumer drains what it polled, or the revocation times out), so an
/// in-flight batch is never lost mid-rebalance.
#[derive(Debug, Clone)]
pub struct PendingRevocation {
    pub partition_id: usize,
    /// Slab key of the member the partition moves to once completed.
    pub target_member: usize,
    /// Monotonic micros when the revocation was created, for timeout.
    pub created_at: u64,
}

#[derive(Debug, Clone)]
pub struct ConsumerGroupMember {
    pub id: usize,
    pub client_id: u128,
    pub partitions: Vec<usize>,
    /// Partitions marked for cooperative handoff away from this member but not
    /// yet moved. The member still owns them (poll + commit) until completion.
    pub pending_revocations: Vec<PendingRevocation>,
}

impl ConsumerGroupMember {
    #[must_use]
    pub const fn new(id: usize, client_id: u128) -> Self {
        Self {
            id,
            client_id,
            partitions: Vec::new(),
            pending_revocations: Vec::new(),
        }
    }

    /// Partitions this member should poll: owned minus those pending handoff
    /// (the source stops polling a revoked partition so its consumer can drain
    /// + commit, but it still owns it for the commit fence until completion).
    #[must_use]
    pub fn pollable_partitions(&self) -> Vec<usize> {
        self.partitions
            .iter()
            .copied()
            .filter(|&partition_id| self.is_pollable(partition_id))
            .collect()
    }

    /// Whether this member may poll `partition_id`: owned and not pending
    /// handoff. Alloc-free -- the poll fence hits this once per poll under the
    /// metadata read lock, so it avoids the `HashSet` + `Vec` that
    /// `pollable_partitions` builds. `pending_revocations` is tiny (bounded by
    /// the member's partition count), so the linear scan beats a set.
    #[must_use]
    pub fn is_pollable(&self, partition_id: usize) -> bool {
        self.partitions.contains(&partition_id)
            && !self
                .pending_revocations
                .iter()
                .any(|revocation| revocation.partition_id == partition_id)
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    /// Monotonic id, unique per topic and never reused. Doubles as the
    /// consumer-group offset key on the partition plane.
    pub id: u64,
    /// Bumped on every `rebalance_members`. The client caches the generation it
    /// synced at; the coordinator fences stale polls and the heartbeat re-syncs
    /// when it advances.
    pub generation: u64,
    pub name: Arc<str>,
    pub members: Slab<ConsumerGroupMember>,
}

impl ConsumerGroup {
    #[must_use]
    pub const fn new(id: u64, name: Arc<str>) -> Self {
        Self {
            id,
            generation: 0,
            name,
            members: Slab::new(),
        }
    }

    /// Redistribute the topic's current partitions round-robin across members.
    /// Reads the live partition set each call, so it reflects repartitions and
    /// bumps the generation on any membership change.
    ///
    /// Deliberately asymmetric with the join path (`rebalance_cooperative`):
    /// this clears partitions and pending revocations with no cooperative
    /// drain, so a member's in-flight (polled-but-uncommitted) partitions move
    /// immediately and the new owner re-reads that range. That redelivery is
    /// exactly what the join path avoids via pending revocations -- but the
    /// callers here (leave, remove-member on disconnect, repartition) are cases
    /// where the departing member cannot drain anyway, so an eager move is
    /// correct and the redelivery is benign under at-least-once.
    pub fn rebalance_members(&mut self, partition_ids: &[usize]) {
        self.generation += 1;

        let member_count = self.members.len();
        let member_keys: Vec<usize> = self.members.iter().map(|(id, _)| id).collect();
        for &member_id in &member_keys {
            if let Some(member) = self.members.get_mut(member_id) {
                member.partitions.clear();
                member.pending_revocations.clear();
            }
        }

        if member_count == 0 {
            return;
        }

        for (i, &partition_id) in partition_ids.iter().enumerate() {
            let target = i % member_count;
            if let Some(&member_id) = member_keys.get(target)
                && let Some(member) = self.members.get_mut(member_id)
            {
                member.partitions.push(partition_id);
            }
        }
    }

    /// Cooperative rebalance (on member join): assign unassigned partitions to
    /// idle members immediately, and mark each over-assigned member's excess as
    /// pending revocation toward an idle member -- the partition only moves once
    /// the source's consumer drains it (see the reconciler) or it times out, so
    /// no in-flight batch is dropped. `now` is the replicated op timestamp
    /// (micros), kept identical across replicas for deterministic apply.
    #[allow(clippy::too_many_lines)]
    pub fn rebalance_cooperative(
        &mut self,
        partition_ids: &[usize],
        in_flight: &std::collections::HashSet<usize>,
        now: u64,
    ) {
        self.generation += 1;

        let member_count = self.members.len();
        if member_count == 0 || partition_ids.is_empty() {
            return;
        }

        let mut assigned: std::collections::HashSet<usize> = std::collections::HashSet::new();
        for (_, member) in &self.members {
            for &partition_id in &member.partitions {
                assigned.insert(partition_id);
            }
        }

        // Step 1: hand unassigned partitions straight to idle members.
        let unassigned: Vec<usize> = partition_ids
            .iter()
            .copied()
            .filter(|partition_id| !assigned.contains(partition_id))
            .collect();
        if !unassigned.is_empty() {
            let idle: Vec<usize> = self
                .members
                .iter()
                .filter(|(_, member)| member.partitions.is_empty())
                .map(|(slab_id, _)| slab_id)
                .collect();
            if !idle.is_empty() {
                for (i, partition_id) in unassigned.into_iter().enumerate() {
                    let slab_id = idle[i % idle.len()];
                    if let Some(member) = self.members.get_mut(slab_id) {
                        member.partitions.push(partition_id);
                    }
                }
            }
        }

        // Step 2: mark excess on over-assigned members as pending revocation.
        let partition_count = partition_ids.len();
        let fair_share = partition_count / member_count;
        let remainder = partition_count % member_count;

        let revocation_targets: std::collections::HashSet<usize> = self
            .members
            .iter()
            .flat_map(|(_, member)| {
                member
                    .pending_revocations
                    .iter()
                    .map(|revocation| revocation.target_member)
            })
            .collect();
        let idle: Vec<usize> = self
            .members
            .iter()
            .filter(|(slab_id, member)| {
                member.partitions.is_empty() && !revocation_targets.contains(slab_id)
            })
            .map(|(slab_id, _)| slab_id)
            .collect();
        if idle.is_empty() {
            return;
        }

        // Pass 1: collect excess (round-robin into idle members in pass 2 so a
        // single over-assigned member's excess spreads evenly, not all to one).
        let member_keys: Vec<usize> = self.members.iter().map(|(slab_id, _)| slab_id).collect();
        let mut members_with_remainder = remainder;
        let mut all_excess: Vec<(usize, usize)> = Vec::new();
        for &slab_id in &member_keys {
            let Some(member) = self.members.get(slab_id) else {
                continue;
            };
            let pending: std::collections::HashSet<usize> = member
                .pending_revocations
                .iter()
                .map(|revocation| revocation.partition_id)
                .collect();
            let effective: Vec<usize> = member
                .partitions
                .iter()
                .copied()
                .filter(|partition_id| !pending.contains(partition_id))
                .collect();
            let max_allowed = if members_with_remainder > 0 {
                fair_share + 1
            } else {
                fair_share
            };
            if effective.len() <= max_allowed {
                if effective.len() > fair_share {
                    members_with_remainder = members_with_remainder.saturating_sub(1);
                }
                continue;
            }
            let excess_count = effective.len() - max_allowed;
            if members_with_remainder > 0 && effective.len() > fair_share {
                members_with_remainder = members_with_remainder.saturating_sub(1);
            }
            for partition_id in effective.into_iter().rev().take(excess_count) {
                all_excess.push((partition_id, slab_id));
            }
        }

        // Pass 2: distribute the collected excess round-robin over idle members.
        // An in-flight partition (the source polled but hasn't committed it) is
        // pending-revoked so the source can drain it first; a never-polled or
        // already-drained partition has nothing in flight, so it moves now.
        for (i, (partition_id, source_slab)) in all_excess.into_iter().enumerate() {
            let target_slab = idle[i % idle.len()];
            if in_flight.contains(&partition_id) {
                if let Some(source) = self.members.get_mut(source_slab) {
                    source.pending_revocations.push(PendingRevocation {
                        partition_id,
                        target_member: target_slab,
                        created_at: now,
                    });
                }
            } else {
                if let Some(source) = self.members.get_mut(source_slab) {
                    source.partitions.retain(|&p| p != partition_id);
                }
                if let Some(target) = self.members.get_mut(target_slab) {
                    target.partitions.push(partition_id);
                }
            }
        }
    }

    /// Complete a pending revocation: move `partition_id` from the member with
    /// `source_client_id` to its recorded target. Bumps the generation so both
    /// clients re-sync. Returns `false` if no such pending revocation exists
    /// (already completed / stale) or the target member is gone.
    pub fn complete_revocation(&mut self, source_client_id: u128, partition_id: usize) -> bool {
        let Some(source_slab) = self
            .members
            .iter()
            .find(|(_, member)| member.client_id == source_client_id)
            .map(|(slab_id, _)| slab_id)
        else {
            return false;
        };
        let Some(source) = self.members.get_mut(source_slab) else {
            return false;
        };
        let Some(pos) = source
            .pending_revocations
            .iter()
            .position(|revocation| revocation.partition_id == partition_id)
        else {
            return false;
        };
        let revocation = source.pending_revocations.remove(pos);
        source.partitions.retain(|&p| p != partition_id);
        if let Some(target) = self.members.get_mut(revocation.target_member) {
            target.partitions.push(partition_id);
            self.generation += 1;
        } else {
            // Target gone: full rebalance recovers a consistent assignment.
            let mut all: std::collections::HashSet<usize> = std::collections::HashSet::new();
            for (_, member) in &self.members {
                for &p in &member.partitions {
                    all.insert(p);
                }
            }
            all.insert(partition_id);
            let mut partition_ids: Vec<usize> = all.into_iter().collect();
            partition_ids.sort_unstable();
            self.rebalance_members(&partition_ids);
        }
        true
    }

    /// `(source_client_id, partition_id, created_at)` for every pending
    /// revocation in this group, for the reconciler's completion check.
    #[must_use]
    pub fn pending_revocations(&self) -> Vec<(u128, usize, u64)> {
        self.members
            .iter()
            .flat_map(|(_, member)| {
                member.pending_revocations.iter().map(move |revocation| {
                    (
                        member.client_id,
                        revocation.partition_id,
                        revocation.created_at,
                    )
                })
            })
            .collect()
    }
}

/// Replicated `JoinConsumerGroup`, enriched by the primary's home shard.
///
/// The wire request carries only identifiers; the apply needs the client id
/// (which member is joining) and can't read it from the consensus header. The
/// home shard also gathers `in_flight` -- the group's partitions with
/// uncommitted polled data -- so the cooperative rebalance pending-revokes only
/// those and hands off never-polled/drained partitions immediately (the apply
/// has no partition-plane access to classify them itself).
#[derive(Debug, Clone)]
pub struct JoinConsumerGroupRequest {
    pub stream_id: WireIdentifier,
    pub topic_id: WireIdentifier,
    pub group_id: WireIdentifier,
    pub client_id: u128,
    pub in_flight: Vec<u32>,
}

impl WireEncode for JoinConsumerGroupRequest {
    fn encoded_size(&self) -> usize {
        self.stream_id.encoded_size()
            + self.topic_id.encoded_size()
            + self.group_id.encoded_size()
            + 16
            + 4
            + self.in_flight.len() * 4
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.stream_id.encode(buf);
        self.topic_id.encode(buf);
        self.group_id.encode(buf);
        buf.put_u128_le(self.client_id);
        // `encoded_size` + the loop below use the true length, so a silent
        // clamp here would desync the count header from the body. `in_flight`
        // is bounded by the topic's partition count, so this never fires.
        buf.put_u32_le(u32::try_from(self.in_flight.len()).expect("in_flight count fits u32"));
        for partition_id in &self.in_flight {
            buf.put_u32_le(*partition_id);
        }
    }
}

impl WireDecode for JoinConsumerGroupRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), iggy_binary_protocol::WireError> {
        let (stream_id, mut pos) = WireIdentifier::decode(buf)?;
        let (topic_id, n) = WireIdentifier::decode(&buf[pos..])?;
        pos += n;
        let (group_id, n) = WireIdentifier::decode(&buf[pos..])?;
        pos += n;
        let client_id = read_u128_le(buf, pos)?;
        pos += 16;
        let count = read_u32_le(buf, pos)? as usize;
        pos += 4;
        // Cap against the bytes actually left: `count` is read straight off a
        // replicated prepare, so an unbounded `with_capacity` would let a
        // corrupt/bit-rotted count (near u32::MAX) allocate gigabytes and abort
        // every backup that applies it.
        let mut in_flight =
            Vec::with_capacity(capped_capacity(count, buf.len().saturating_sub(pos), 4));
        for _ in 0..count {
            in_flight.push(read_u32_le(buf, pos)?);
            pos += 4;
        }
        Ok((
            Self {
                stream_id,
                topic_id,
                group_id,
                client_id,
                in_flight,
            },
            pos,
        ))
    }
}

/// Replicated `LeaveConsumerGroup`, enriched by the primary with the leaving
/// client's VSR id. The apply removes the member and rebalances.
#[derive(Debug, Clone)]
pub struct LeaveConsumerGroupRequest {
    pub stream_id: WireIdentifier,
    pub topic_id: WireIdentifier,
    pub group_id: WireIdentifier,
    pub client_id: u128,
}

impl WireEncode for LeaveConsumerGroupRequest {
    fn encoded_size(&self) -> usize {
        self.stream_id.encoded_size()
            + self.topic_id.encoded_size()
            + self.group_id.encoded_size()
            + 16
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.stream_id.encode(buf);
        self.topic_id.encode(buf);
        self.group_id.encode(buf);
        buf.put_u128_le(self.client_id);
    }
}

impl WireDecode for LeaveConsumerGroupRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), iggy_binary_protocol::WireError> {
        let (stream_id, mut pos) = WireIdentifier::decode(buf)?;
        let (topic_id, n) = WireIdentifier::decode(&buf[pos..])?;
        pos += n;
        let (group_id, n) = WireIdentifier::decode(&buf[pos..])?;
        pos += n;
        let client_id = read_u128_le(buf, pos)?;
        pos += 16;
        Ok((
            Self {
                stream_id,
                topic_id,
                group_id,
                client_id,
            },
            pos,
        ))
    }
}

impl StateHandler for CreateConsumerGroupRequest {
    type State = StreamsInner;
    #[allow(clippy::cast_possible_truncation)]
    fn apply(
        &self,
        state: &mut StreamsInner,
        _timestamp: iggy_common::IggyTimestamp,
    ) -> ApplyReply {
        let Some(topic) = state.topic_mut(&self.stream_id, &self.topic_id) else {
            return ApplyReply::ok(Bytes::new());
        };
        let name: Arc<str> = Arc::from(self.name.as_str());
        // Per-(stream,topic) name uniqueness. A same-name group already exists:
        // do NOT supersede it -- removing it would drop a live group along with
        // its members and their assignments, and the ejected members would
        // never recover. Leave the existing group untouched, mirroring
        // `CreateStream`/`CreateTopic` on a duplicate name.
        if topic.consumer_group_index.contains_key(&name) {
            return ApplyReply::err(CreateConsumerGroupResult::NameAlreadyExists);
        }
        let id = topic.next_consumer_group_id;
        topic.next_consumer_group_id += 1;
        topic
            .consumer_groups
            .insert(id, ConsumerGroup::new(id, name.clone()));
        topic.consumer_group_index.insert(name, id);

        ApplyReply::ok(
            ConsumerGroupDetailsResponse {
                group: ConsumerGroupResponse {
                    id: id as u32,
                    partitions_count: 0,
                    members_count: 0,
                    name: self.name.clone(),
                },
                members: Vec::new(),
            }
            .to_bytes(),
        )
    }
}

impl StateHandler for DeleteConsumerGroupRequest {
    type State = StreamsInner;
    fn apply(
        &self,
        state: &mut StreamsInner,
        _timestamp: iggy_common::IggyTimestamp,
    ) -> ApplyReply {
        let removed = {
            let Some(topic) = state.topic_mut(&self.stream_id, &self.topic_id) else {
                return ApplyReply::err(DeleteConsumerGroupResult::NotFound);
            };
            if let Some(group_id) = topic.resolve_group_id(&self.group_id)
                && let Some(group) = topic.consumer_groups.remove(&group_id)
            {
                topic.consumer_group_index.remove(&group.name);
                true
            } else {
                false
            }
        };
        if !removed {
            return ApplyReply::err(DeleteConsumerGroupResult::NotFound);
        }
        // Bump the partition-shaping revision so the reconciler's fast-skip
        // doesn't pass over the delete: it reclaims the group's leftover
        // offsets on the topic's surviving partitions.
        state.revision = state.revision.wrapping_add(1);
        // The dropped group may have held pending revocations.
        state.recompute_pending_revocations_count();
        ApplyReply::ok(Bytes::new())
    }
}

impl StateHandler for JoinConsumerGroupRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner, timestamp: iggy_common::IggyTimestamp) -> ApplyReply {
        let Some(topic) = state.topic_mut(&self.stream_id, &self.topic_id) else {
            return ApplyReply::ok(Bytes::new());
        };
        let Some(group_id) = topic.resolve_group_id(&self.group_id) else {
            return ApplyReply::ok(Bytes::new());
        };
        // Snapshot the live partition ids before taking a mutable borrow of the
        // group (both borrow the topic).
        let partition_ids: Vec<usize> = topic.partitions.iter().map(|p| p.id).collect();
        let Some(group) = topic.consumer_groups.get_mut(&group_id) else {
            return ApplyReply::ok(Bytes::new());
        };
        // Idempotent: a re-join from the same client keeps its membership.
        let already = group
            .members
            .iter()
            .any(|(_, m)| m.client_id == self.client_id);
        if already {
            return ApplyReply::ok(Bytes::new());
        }
        let member_key = group
            .members
            .insert(ConsumerGroupMember::new(0, self.client_id));
        group.members[member_key].id = member_key;
        // Cooperative handoff using the home-shard-gathered `in_flight` set:
        // never-polled/drained excess moves to the new member immediately (so a
        // fresh group distributes synchronously at join), while partitions with
        // uncommitted in-flight data are pending-revoked for the reconciler to
        // complete once the source drains them (or on timeout).
        let in_flight: std::collections::HashSet<usize> =
            self.in_flight.iter().map(|&p| p as usize).collect();
        group.rebalance_cooperative(&partition_ids, &in_flight, timestamp.as_micros());
        state.recompute_pending_revocations_count();
        ApplyReply::ok(Bytes::new())
    }
}

impl StateHandler for LeaveConsumerGroupRequest {
    type State = StreamsInner;
    fn apply(
        &self,
        state: &mut StreamsInner,
        _timestamp: iggy_common::IggyTimestamp,
    ) -> ApplyReply {
        let Some(topic) = state.topic_mut(&self.stream_id, &self.topic_id) else {
            return ApplyReply::ok(Bytes::new());
        };
        let Some(group_id) = topic.resolve_group_id(&self.group_id) else {
            return ApplyReply::ok(Bytes::new());
        };
        let partition_ids: Vec<usize> = topic.partitions.iter().map(|p| p.id).collect();
        let Some(group) = topic.consumer_groups.get_mut(&group_id) else {
            return ApplyReply::ok(Bytes::new());
        };
        let member_key = group
            .members
            .iter()
            .find(|(_, m)| m.client_id == self.client_id)
            .map(|(key, _)| key);
        if let Some(key) = member_key {
            group.members.remove(key);
            group.rebalance_members(&partition_ids);
            state.recompute_pending_revocations_count();
        }
        ApplyReply::ok(Bytes::new())
    }
}

/// Server-originated disconnect cleanup.
///
/// Drops a disconnected client from every consumer group it joined. Carries
/// only the VSR client id; applied as a side-effect of the `Logout` commit (the
/// apply can't read the client id from the consensus header).
#[derive(Debug, Clone)]
pub struct RemoveConsumerGroupMemberRequest {
    pub client_id: u128,
}

impl WireEncode for RemoveConsumerGroupMemberRequest {
    fn encoded_size(&self) -> usize {
        16
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u128_le(self.client_id);
    }
}

impl WireDecode for RemoveConsumerGroupMemberRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), iggy_binary_protocol::WireError> {
        let client_id = read_u128_le(buf, 0)?;
        Ok((Self { client_id }, 16))
    }
}

impl StateHandler for RemoveConsumerGroupMemberRequest {
    type State = StreamsInner;
    fn apply(
        &self,
        state: &mut StreamsInner,
        _timestamp: iggy_common::IggyTimestamp,
    ) -> ApplyReply {
        let mut any_removed = false;
        for (_, stream) in &mut state.items {
            for (_, topic) in &mut stream.topics {
                for group in topic.consumer_groups.values_mut() {
                    let member_key = group
                        .members
                        .iter()
                        .find(|(_, m)| m.client_id == self.client_id)
                        .map(|(key, _)| key);
                    // Collect the partition set only when this group actually
                    // holds the member. This runs on every logout inside the
                    // no-await commit critical section on every replica, so the
                    // alloc stays out of the common (no-match) path.
                    if let Some(key) = member_key {
                        let partition_ids: Vec<usize> =
                            topic.partitions.iter().map(|p| p.id).collect();
                        group.members.remove(key);
                        group.rebalance_members(&partition_ids);
                        any_removed = true;
                    }
                }
            }
        }
        // Only perturb the reconciler when a membership actually changed.
        if any_removed {
            state.revision = state.revision.wrapping_add(1);
            state.recompute_pending_revocations_count();
        }
        ApplyReply::ok(Bytes::new())
    }
}

/// Reconciler-originated completion of a pending cooperative revocation.
///
/// Moves `partition_id` from the member `source_client_id` to its recorded
/// target, once the source consumer has drained the partition (committed up to
/// what it polled) or the revocation timed out.
#[derive(Debug, Clone)]
pub struct CompleteConsumerGroupRevocationRequest {
    pub stream_id: WireIdentifier,
    pub topic_id: WireIdentifier,
    pub group_id: u64,
    pub source_client_id: u128,
    pub partition_id: u32,
}

impl WireEncode for CompleteConsumerGroupRevocationRequest {
    fn encoded_size(&self) -> usize {
        self.stream_id.encoded_size() + self.topic_id.encoded_size() + 8 + 16 + 4
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.stream_id.encode(buf);
        self.topic_id.encode(buf);
        buf.put_u64_le(self.group_id);
        buf.put_u128_le(self.source_client_id);
        buf.put_u32_le(self.partition_id);
    }
}

impl WireDecode for CompleteConsumerGroupRevocationRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), iggy_binary_protocol::WireError> {
        let (stream_id, mut pos) = WireIdentifier::decode(buf)?;
        let (topic_id, n) = WireIdentifier::decode(&buf[pos..])?;
        pos += n;
        let group_id = read_u64_le(buf, pos)?;
        pos += 8;
        let source_client_id = read_u128_le(buf, pos)?;
        pos += 16;
        let partition_id = read_u32_le(buf, pos)?;
        pos += 4;
        Ok((
            Self {
                stream_id,
                topic_id,
                group_id,
                source_client_id,
                partition_id,
            },
            pos,
        ))
    }
}

impl StateHandler for CompleteConsumerGroupRevocationRequest {
    type State = StreamsInner;
    fn apply(
        &self,
        state: &mut StreamsInner,
        _timestamp: iggy_common::IggyTimestamp,
    ) -> ApplyReply {
        let Some(topic) = state.topic_mut(&self.stream_id, &self.topic_id) else {
            return ApplyReply::ok(Bytes::new());
        };
        let completed = topic
            .consumer_groups
            .get_mut(&self.group_id)
            .is_some_and(|group| {
                group.complete_revocation(self.source_client_id, self.partition_id as usize)
            });
        if completed {
            state.revision = state.revision.wrapping_add(1);
            state.recompute_pending_revocations_count();
        }
        ApplyReply::ok(Bytes::new())
    }
}

/// Consumer group member snapshot representation for serialization.
///
/// Snapshots are encoded by `rmp_serde::to_vec` as **positional arrays** (no
/// field-name map), so `#[serde(default)]` only fills a missing **trailing**
/// element -- a new field is back/forward-compatible only if appended last. A
/// field inserted mid-struct shifts every later position and corrupts decode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupMemberSnapshot {
    pub id: usize,
    pub client_id: u128,
    pub partitions: Vec<usize>,
    /// `(partition_id, target_member_slab, created_at_micros)` per pending
    /// cooperative revocation. Trailing + `serde(default)` so a snapshot that
    /// predates this field still decodes (the default fills the absent
    /// trailing element).
    #[serde(default)]
    pub pending_revocations: Vec<(usize, usize, u64)>,
}

/// Consumer group snapshot representation for serialization (nested under the
/// topic snapshot). Positional array, same trailing-only rule as
/// [`ConsumerGroupMemberSnapshot`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupSnapshot {
    pub id: u64,
    // No `serde(default)`: `generation` is not the trailing field, so a default
    // could not fill it positionally anyway, and every consumer-group snapshot
    // (new with co-located groups) always writes it.
    pub generation: u64,
    pub name: String,
    pub members: Vec<(usize, ConsumerGroupMemberSnapshot)>,
}

impl ConsumerGroupSnapshot {
    #[must_use]
    pub fn from_group(group: &ConsumerGroup) -> Self {
        let members = group
            .members
            .iter()
            .map(|(member_id, member)| {
                (
                    member_id,
                    ConsumerGroupMemberSnapshot {
                        id: member.id,
                        client_id: member.client_id,
                        partitions: member.partitions.clone(),
                        pending_revocations: member
                            .pending_revocations
                            .iter()
                            .map(|revocation| {
                                (
                                    revocation.partition_id,
                                    revocation.target_member,
                                    revocation.created_at,
                                )
                            })
                            .collect(),
                    },
                )
            })
            .collect();
        Self {
            id: group.id,
            generation: group.generation,
            name: group.name.to_string(),
            members,
        }
    }

    #[must_use]
    pub fn into_group(self) -> ConsumerGroup {
        let members: Slab<ConsumerGroupMember> = self
            .members
            .into_iter()
            .map(|(member_key, member_snap)| {
                (
                    member_key,
                    ConsumerGroupMember {
                        id: member_snap.id,
                        client_id: member_snap.client_id,
                        partitions: member_snap.partitions,
                        pending_revocations: member_snap
                            .pending_revocations
                            .into_iter()
                            .map(
                                |(partition_id, target_member, created_at)| PendingRevocation {
                                    partition_id,
                                    target_member,
                                    created_at,
                                },
                            )
                            .collect(),
                    },
                )
            })
            .collect();
        ConsumerGroup {
            id: self.id,
            generation: self.generation,
            name: Arc::from(self.name.as_str()),
            members,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::WireName;
    use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
    use iggy_binary_protocol::requests::streams::CreateStreamRequest;
    use iggy_binary_protocol::requests::topics::{
        CreateTopicRequest, CreateTopicWithAssignmentsRequest,
    };
    use iggy_common::IggyTimestamp;

    // Groups co-locate in the topic node, so an apply resolves its parent through
    // `topic_mut`. Build a `StreamsInner` holding stream 0 / topic 0 via the same
    // handlers the commit path drives.
    fn streams_with_topic() -> StreamsInner {
        let mut inner = StreamsInner::new();
        let _ = StateHandler::apply(
            &CreateStreamRequest {
                name: WireName::new("stream").unwrap(),
            },
            &mut inner,
            IggyTimestamp::now(),
        );
        let create_topic = CreateTopicWithAssignmentsRequest {
            request: CreateTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                partitions_count: 1,
                compression_algorithm: 0,
                message_expiry: 0,
                max_topic_size: 0,
                replication_factor: 1,
                name: WireName::new("topic").unwrap(),
            },
            partitions: vec![CreatedPartitionAssignment {
                partition_id: 0,
                consensus_group_id: 1,
            }],
        };
        let _ = StateHandler::apply(&create_topic, &mut inner, IggyTimestamp::now());
        inner
    }

    fn create_group(state: &mut StreamsInner, name: &str) -> ApplyReply {
        StateHandler::apply(
            &CreateConsumerGroupRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                name: WireName::new(name).unwrap(),
            },
            state,
            IggyTimestamp::now(),
        )
    }

    #[test]
    fn given_duplicate_name_when_apply_create_consumer_group_should_return_name_already_exists() {
        let mut state = streams_with_topic();
        assert_eq!(create_group(&mut state, "group").code, 0);

        let apply = create_group(&mut state, "group");
        assert_eq!(
            apply.code,
            u32::from(CreateConsumerGroupResult::NameAlreadyExists)
        );
        assert!(apply.body.is_empty());
    }

    #[test]
    fn given_missing_group_when_apply_delete_consumer_group_should_return_not_found() {
        let mut state = streams_with_topic();
        let apply = StateHandler::apply(
            &DeleteConsumerGroupRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                group_id: WireIdentifier::numeric(999),
            },
            &mut state,
            IggyTimestamp::now(),
        );
        assert_eq!(apply.code, u32::from(DeleteConsumerGroupResult::NotFound));
        assert!(apply.body.is_empty());
    }
}
