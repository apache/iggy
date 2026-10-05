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
use crate::stm::id_slab::IdSlab;
use crate::stm::lifecycle::LifecycleAction;
use crate::stm::result::{
    ApplyReply, CreateConsumerGroupResult, DeleteConsumerGroupResult, JoinConsumerGroupResult,
    LeaveConsumerGroupResult,
};
use crate::stm::stream::{Partition, StreamsInner};
use bytes::Bytes;

use bytes::{BufMut, BytesMut};
use iggy_binary_protocol::WireIdentifier;
use iggy_binary_protocol::codec::{WireDecode, WireEncode, read_u32_le, read_u64_le, read_u128_le};
use iggy_binary_protocol::primitives::partition_history::ConsumerGroupOwner;
use iggy_binary_protocol::requests::consumer_groups::{
    CreateConsumerGroupRequest, DeleteConsumerGroupRequest,
};
use iggy_binary_protocol::requests::partitions::InstallConsumerGroupOwnerRequest;
use iggy_binary_protocol::responses::consumer_groups::consumer_group_response::ConsumerGroupResponse;
use iggy_binary_protocol::responses::consumer_groups::get_consumer_group::ConsumerGroupDetailsResponse;
use iggy_common::IggyTimestamp;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// A desired owner remains inactive until its exact partition installation is durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingOwnership {
    pub owner: ConsumerGroupOwner,
    pub metadata_op: u64,
    pub created_at: u64,
    pub skip_drain: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerGroupAssignment {
    pub incarnation: u64,
    pub owner: Option<ConsumerGroupOwner>,
    pub pending: Option<PendingOwnership>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerGroupOwnershipTransition {
    pub stream_id: u32,
    pub topic_id: u32,
    pub partition_id: u32,
    pub source: Option<ConsumerGroupOwner>,
    pub installation: InstallConsumerGroupOwnerRequest,
    pub created_at: u64,
    pub skip_drain: bool,
}

#[derive(Debug, Clone)]
pub struct ConsumerGroupMember {
    pub id: usize,
    pub client_id: u128,
    /// Retained independently of client-table capacity eviction.
    pub session: Option<u64>,
    pub partitions: Vec<usize>,
    /// Derived from the authoritative assignments. Polls stop while offsets drain.
    pub pending_revocations: Vec<usize>,
}

impl ConsumerGroupMember {
    #[must_use]
    pub const fn new(id: usize, client_id: u128) -> Self {
        Self {
            id,
            client_id,
            session: None,
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
        self.partitions.contains(&partition_id) && !self.pending_revocations.contains(&partition_id)
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
    pub members: IdSlab<ConsumerGroupMember>,
    pub assignments: BTreeMap<usize, ConsumerGroupAssignment>,
}

impl ConsumerGroup {
    #[must_use]
    pub const fn new(id: u64, name: Arc<str>) -> Self {
        Self {
            id,
            generation: 0,
            name,
            members: IdSlab::new(),
            assignments: BTreeMap::new(),
        }
    }

    /// Metadata chooses successors; only a durable partition installation activates them.
    pub fn rebalance_members(&mut self, partitions: &[Partition], metadata_op: u64, now: u64) {
        self.generation += 1;
        let members: Vec<(u128, u64)> = self
            .members
            .iter()
            .filter_map(|(_, member)| {
                member
                    .session
                    .filter(|session| *session != 0)
                    .map(|session| (member.client_id, session))
            })
            .collect();
        let partition_ids: std::collections::BTreeSet<usize> =
            partitions.iter().map(|partition| partition.id).collect();
        self.assignments
            .retain(|partition_id, _| partition_ids.contains(partition_id));
        let desired_members = self.balanced_member_indices(partitions, &members);
        for (index, partition) in partitions.iter().enumerate() {
            let incarnation = partition.created_revision;
            let (client_id, session) =
                desired_members[index].map_or((0, 0), |index| members[index]);
            let desired = ConsumerGroupOwner {
                client_id,
                session,
                generation: self.generation,
            };
            let assignment =
                self.assignments
                    .entry(partition.id)
                    .or_insert(ConsumerGroupAssignment {
                        incarnation,
                        owner: None,
                        pending: None,
                    });
            if assignment.incarnation != incarnation {
                assignment.incarnation = incarnation;
                assignment.owner = None;
                assignment.pending = None;
            }
            let same_member = |owner: ConsumerGroupOwner| {
                owner.client_id == client_id && owner.session == session
            };
            if let Some(pending) = assignment.pending {
                if same_member(pending.owner) {
                    continue;
                }
            } else if assignment.owner.is_some_and(same_member)
                || (assignment.owner.is_none() && desired.is_unassigned())
            {
                continue;
            }
            let skip_drain = assignment
                .owner
                .is_none_or(|owner| !members.contains(&(owner.client_id, owner.session)));
            assignment.pending = Some(PendingOwnership {
                owner: desired,
                metadata_op,
                created_at: now,
                skip_drain,
            });
        }
        self.rebuild_member_assignments();
    }

    pub fn complete_revocation(
        &mut self,
        partition_id: usize,
        installation: &InstallConsumerGroupOwnerRequest,
        partition_op: u64,
    ) -> bool {
        if installation.group_id != self.id || partition_op == 0 {
            return false;
        }
        let Some(assignment) = self.assignments.get_mut(&partition_id) else {
            return false;
        };
        let Some(pending) = assignment.pending else {
            return false;
        };
        if assignment.incarnation != installation.incarnation
            || pending.owner != installation.owner
            || pending.metadata_op != installation.metadata_op
        {
            return false;
        }
        let previous = assignment.owner;
        assignment.owner = (!pending.owner.is_unassigned()).then_some(pending.owner);
        assignment.pending = None;
        self.generation += 1;
        for (_, member) in &mut self.members {
            if previous.is_some_and(|owner| {
                owner.client_id == member.client_id && Some(owner.session) == member.session
            }) {
                member
                    .partitions
                    .retain(|assigned| *assigned != partition_id);
                member
                    .pending_revocations
                    .retain(|pending| *pending != partition_id);
            }
            if pending.owner.client_id == member.client_id
                && Some(pending.owner.session) == member.session
            {
                member.partitions.push(partition_id);
                member.partitions.sort_unstable();
            }
        }
        true
    }

    /// Replaces this group's entries in `pending_revocations` with its open transitions.
    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn index_pending_revocations(
        &self,
        stream_id: usize,
        topic_id: usize,
        pending_revocations: &mut BTreeMap<
            (usize, usize, u64, usize),
            ConsumerGroupOwnershipTransition,
        >,
    ) {
        pending_revocations
            .extract_if(
                (stream_id, topic_id, self.id, 0)..=(stream_id, topic_id, self.id, usize::MAX),
                |_, _| true,
            )
            .for_each(drop);
        for (&partition_id, assignment) in &self.assignments {
            let Some(pending) = assignment.pending else {
                continue;
            };
            pending_revocations.insert(
                (stream_id, topic_id, self.id, partition_id),
                ConsumerGroupOwnershipTransition {
                    stream_id: stream_id as u32,
                    topic_id: topic_id as u32,
                    partition_id: partition_id as u32,
                    source: assignment.owner,
                    installation: InstallConsumerGroupOwnerRequest {
                        incarnation: assignment.incarnation,
                        group_id: self.id,
                        owner: pending.owner,
                        metadata_op: pending.metadata_op,
                    },
                    created_at: pending.created_at,
                    skip_drain: pending.skip_drain,
                },
            );
        }
    }

    fn balanced_member_indices(
        &self,
        partitions: &[Partition],
        members: &[(u128, u64)],
    ) -> Vec<Option<usize>> {
        let mut loads = vec![0usize; members.len()];
        let mut desired_members: Vec<Option<usize>> = partitions
            .iter()
            .map(|partition| {
                let assignment = self.assignments.get(&partition.id)?;
                if assignment.incarnation != partition.created_revision {
                    return None;
                }
                let owner = assignment
                    .pending
                    .map(|pending| pending.owner)
                    .or(assignment.owner)?;
                let index = members
                    .iter()
                    .position(|member| *member == (owner.client_id, owner.session))?;
                loads[index] += 1;
                Some(index)
            })
            .collect();
        if !members.is_empty() {
            let fair_share = partitions.len() / members.len();
            let mut capacities = vec![fair_share; members.len()];
            let mut ranked_members: Vec<usize> = (0..members.len()).collect();
            ranked_members.sort_by_key(|index| std::cmp::Reverse(loads[*index]));
            for index in ranked_members
                .into_iter()
                .take(partitions.len() % members.len())
            {
                capacities[index] += 1;
            }
            for desired in desired_members.iter_mut().rev() {
                if let Some(index) = *desired
                    && loads[index] > capacities[index]
                {
                    loads[index] -= 1;
                    *desired = None;
                }
            }
            for desired in &mut desired_members {
                if desired.is_none()
                    && let Some(index) = loads
                        .iter()
                        .enumerate()
                        .filter(|(index, load)| **load < capacities[*index])
                        .min_by_key(|(_, load)| **load)
                        .map(|(index, _)| index)
                {
                    loads[index] += 1;
                    *desired = Some(index);
                }
            }
        }
        desired_members
    }

    fn rebuild_member_assignments(&mut self) {
        for (_, member) in &mut self.members {
            member.partitions.clear();
            member.pending_revocations.clear();
            for (&partition_id, assignment) in &self.assignments {
                if assignment.owner.is_some_and(|owner| {
                    owner.client_id == member.client_id && Some(owner.session) == member.session
                }) {
                    member.partitions.push(partition_id);
                    if assignment.pending.is_some() {
                        member.pending_revocations.push(partition_id);
                    }
                }
            }
        }
    }
}

/// Replicated join enriched with the authenticated member identity.
/// Drain decisions belong to the partition's installed owner.
#[derive(Debug, Clone)]
pub struct JoinConsumerGroupRequest {
    pub stream_id: WireIdentifier,
    pub topic_id: WireIdentifier,
    pub group_id: WireIdentifier,
    pub client_id: u128,
    pub session: u64,
}

impl WireEncode for JoinConsumerGroupRequest {
    fn encoded_size(&self) -> usize {
        self.stream_id.encoded_size()
            + self.topic_id.encoded_size()
            + self.group_id.encoded_size()
            + size_of::<u128>()
            + size_of::<u64>()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.stream_id.encode(buf);
        self.topic_id.encode(buf);
        self.group_id.encode(buf);
        buf.put_u128_le(self.client_id);
        buf.put_u64_le(self.session);
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
        pos += size_of::<u128>();
        let session = read_u64_le(buf, pos)?;
        pos += size_of::<u64>();
        if client_id == 0 || session == 0 || pos != buf.len() {
            return Err(iggy_binary_protocol::WireError::Validation(
                "invalid consumer group member identity or trailing bytes".into(),
            ));
        }
        Ok((
            Self {
                stream_id,
                topic_id,
                group_id,
                client_id,
                session,
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
        // A missing parent is a committed rejection, mirroring `CreateTopic`
        // on a missing stream. Resolve level by level so the error names the
        // level that missed.
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return ApplyReply::err(CreateConsumerGroupResult::StreamNotFound);
        };
        let Some(topic_id) = state.resolve_topic_id(stream_id, &self.topic_id) else {
            return ApplyReply::err(CreateConsumerGroupResult::TopicNotFound);
        };
        if state.lifecycle_blocks_consumer(stream_id, topic_id, None) {
            return ApplyReply::err(iggy_common::IggyError::LifecycleBusy.as_code());
        }

        let Some(topic) = state
            .items
            .get_mut(stream_id)
            .and_then(|stream| stream.topics.get_mut(topic_id))
        else {
            return ApplyReply::err(CreateConsumerGroupResult::TopicNotFound);
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
        let partitions_count = topic.partitions.len() as u32;

        ApplyReply::ok(
            ConsumerGroupDetailsResponse {
                group: ConsumerGroupResponse {
                    id: id as u32,
                    partitions_count,
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
        // Same level-by-level resolution as Join/Leave, mirroring the legacy
        // `resolve_consumer_group` ladder instead of collapsing a missing
        // stream or topic into the group-not-found code.
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return ApplyReply::err(DeleteConsumerGroupResult::StreamNotFound);
        };
        let Some(topic_id) = state.resolve_topic_id(stream_id, &self.topic_id) else {
            return ApplyReply::err(DeleteConsumerGroupResult::TopicNotFound);
        };
        let Some(topic) = state
            .items
            .get_mut(stream_id)
            .and_then(|stream| stream.topics.get_mut(topic_id))
        else {
            return ApplyReply::err(DeleteConsumerGroupResult::TopicNotFound);
        };
        let Some(group_id) = topic.resolve_group_id(&self.group_id) else {
            return ApplyReply::err(DeleteConsumerGroupResult::ConsumerGroupNotFound);
        };
        let Ok(wire_group_id) = u32::try_from(group_id) else {
            return ApplyReply::err(iggy_common::IggyError::InvalidCommand.as_code());
        };
        if let Some(reply) = state.begin_lifecycle(
            stream_id,
            Some(topic_id),
            LifecycleAction::DeleteConsumerGroup {
                group_id: wire_group_id,
            },
        ) {
            return reply;
        }
        let Some(topic) = state
            .items
            .get_mut(stream_id)
            .and_then(|stream| stream.topics.get_mut(topic_id))
        else {
            return ApplyReply::err(DeleteConsumerGroupResult::TopicNotFound);
        };
        let Some(group) = topic.consumer_groups.remove(&group_id) else {
            return ApplyReply::err(DeleteConsumerGroupResult::ConsumerGroupNotFound);
        };
        topic.consumer_group_index.remove(&group.name);
        // Bump the partition-shaping revision so the reconciler's fast-skip
        // doesn't pass over the delete: it reclaims the group's leftover
        // offsets on the topic's surviving partitions.
        state.revision = state.revision.wrapping_add(1);
        // The dropped group may have held pending revocations.
        state.recompute_consumer_group_metadata();
        ApplyReply::ok(Bytes::new())
    }
}

impl StateHandler for JoinConsumerGroupRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner, timestamp: iggy_common::IggyTimestamp) -> ApplyReply {
        // Resolve level by level so the committed rejection names the level that
        // missed, mirroring the legacy `resolve_consumer_group` ladder instead of
        // the old silent-OK no-op.
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return ApplyReply::err(JoinConsumerGroupResult::StreamNotFound);
        };
        let Some(topic_id) = state.resolve_topic_id(stream_id, &self.topic_id) else {
            return ApplyReply::err(JoinConsumerGroupResult::TopicNotFound);
        };

        let group_id = state
            .items
            .get(stream_id)
            .and_then(|stream| stream.topics.get(topic_id))
            .and_then(|topic| topic.resolve_group_id(&self.group_id));
        if state.lifecycle_blocks_consumer(stream_id, topic_id, group_id) {
            return ApplyReply::err(iggy_common::IggyError::LifecycleBusy.as_code());
        }

        let Some(topic) = state
            .items
            .get_mut(stream_id)
            .and_then(|stream| stream.topics.get_mut(topic_id))
        else {
            return ApplyReply::err(JoinConsumerGroupResult::TopicNotFound);
        };
        let Some(group_id) = topic.resolve_group_id(&self.group_id) else {
            return ApplyReply::err(JoinConsumerGroupResult::ConsumerGroupNotFound);
        };
        let Some(group) = topic.consumer_groups.get_mut(&group_id) else {
            return ApplyReply::err(JoinConsumerGroupResult::ConsumerGroupNotFound);
        };
        // Idempotent: a re-join from the same client keeps its membership.
        let already = group
            .members
            .iter_mut()
            .find(|(_, member)| member.client_id == self.client_id);
        if let Some((_, member)) = already {
            let session = Some(self.session);
            if member.session != session {
                member.session = session;
                group.rebalance_members(
                    &topic.partitions,
                    state.apply_context.metadata_op,
                    timestamp.as_micros(),
                );
                state.recompute_consumer_group_metadata();
            }
            return ApplyReply::ok(Bytes::new());
        }
        let member_key = group
            .members
            .insert(ConsumerGroupMember::new(0, self.client_id));
        group.members[member_key].id = member_key;
        group.members[member_key].session = Some(self.session);
        group.rebalance_members(
            &topic.partitions,
            state.apply_context.metadata_op,
            timestamp.as_micros(),
        );
        state.recompute_consumer_group_metadata();
        ApplyReply::ok(Bytes::new())
    }
}

impl StateHandler for LeaveConsumerGroupRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner, timestamp: iggy_common::IggyTimestamp) -> ApplyReply {
        // Same level-by-level resolution as Join, surfacing a loud rejection
        // instead of the old silent-OK no-op when a level is gone.
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return ApplyReply::err(LeaveConsumerGroupResult::StreamNotFound);
        };
        let Some(topic_id) = state.resolve_topic_id(stream_id, &self.topic_id) else {
            return ApplyReply::err(LeaveConsumerGroupResult::TopicNotFound);
        };

        let group_id = state
            .items
            .get(stream_id)
            .and_then(|stream| stream.topics.get(topic_id))
            .and_then(|topic| topic.resolve_group_id(&self.group_id));
        if state.lifecycle_blocks_consumer(stream_id, topic_id, group_id) {
            return ApplyReply::err(iggy_common::IggyError::LifecycleBusy.as_code());
        }

        let Some(topic) = state
            .items
            .get_mut(stream_id)
            .and_then(|stream| stream.topics.get_mut(topic_id))
        else {
            return ApplyReply::err(LeaveConsumerGroupResult::TopicNotFound);
        };
        let Some(group_id) = topic.resolve_group_id(&self.group_id) else {
            return ApplyReply::err(LeaveConsumerGroupResult::ConsumerGroupNotFound);
        };
        let Some(group) = topic.consumer_groups.get_mut(&group_id) else {
            return ApplyReply::err(LeaveConsumerGroupResult::ConsumerGroupNotFound);
        };
        let member_key = group
            .members
            .iter()
            .find(|(_, m)| m.client_id == self.client_id)
            .map(|(key, _)| key);
        let Some(key) = member_key else {
            // Group exists but this client never joined it: mirror legacy's
            // ConsumerGroupMemberNotFound instead of a silent no-op. The
            // server-internal RemoveConsumerGroupMember (disconnect cleanup)
            // stays idempotent -- only this client-facing Leave is loud.
            return ApplyReply::err(LeaveConsumerGroupResult::ConsumerGroupMemberNotFound);
        };
        group.members.remove(key);
        group.rebalance_members(
            &topic.partitions,
            state.apply_context.metadata_op,
            timestamp.as_micros(),
        );
        state.recompute_consumer_group_metadata();
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
    fn apply(&self, state: &mut StreamsInner, timestamp: iggy_common::IggyTimestamp) -> ApplyReply {
        let mut any_removed = false;
        for (_, stream) in &mut state.items {
            for (_, topic) in &mut stream.topics {
                for group in topic.consumer_groups.values_mut() {
                    let member_key = group
                        .members
                        .iter()
                        .find(|(_, m)| m.client_id == self.client_id)
                        .map(|(key, _)| key);
                    if let Some(key) = member_key {
                        group.members.remove(key);
                        group.rebalance_members(
                            &topic.partitions,
                            state.apply_context.metadata_op,
                            timestamp.as_micros(),
                        );
                        any_removed = true;
                    }
                }
            }
        }
        // Only perturb the reconciler when a membership actually changed.
        if any_removed {
            state.revision = state.revision.wrapping_add(1);
            state.recompute_consumer_group_metadata();
        }
        ApplyReply::ok(Bytes::new())
    }
}

/// Durable partition evidence for one exact pending ownership transition.
#[derive(Debug, Clone)]
pub struct CompleteConsumerGroupRevocationRequest {
    pub stream_id: WireIdentifier,
    pub topic_id: WireIdentifier,
    pub partition_id: u32,
    pub installation: InstallConsumerGroupOwnerRequest,
    pub partition_op: u64,
}

impl WireEncode for CompleteConsumerGroupRevocationRequest {
    fn encoded_size(&self) -> usize {
        self.stream_id.encoded_size()
            + self.topic_id.encoded_size()
            + size_of::<u32>()
            + self.installation.encoded_size()
            + size_of::<u64>()
    }
    fn encode(&self, buf: &mut BytesMut) {
        self.stream_id.encode(buf);
        self.topic_id.encode(buf);
        buf.put_u32_le(self.partition_id);
        self.installation.encode(buf);
        buf.put_u64_le(self.partition_op);
    }
}

impl WireDecode for CompleteConsumerGroupRevocationRequest {
    fn decode_from(buf: &[u8]) -> Result<Self, iggy_binary_protocol::WireError> {
        let (request, consumed) = Self::decode(buf)?;
        if consumed != buf.len() || request.partition_op == 0 {
            return Err(iggy_binary_protocol::WireError::Validation(
                "invalid ownership completion length or partition operation".into(),
            ));
        }
        Ok(request)
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize), iggy_binary_protocol::error::WireError> {
        let (stream_id, mut pos) = WireIdentifier::decode(buf)?;
        let (topic_id, consumed) = WireIdentifier::decode(&buf[pos..])?;
        pos += consumed;
        let partition_id = read_u32_le(buf, pos)?;
        pos += size_of::<u32>();
        let (installation, consumed) = InstallConsumerGroupOwnerRequest::decode(&buf[pos..])?;
        pos += consumed;
        let partition_op = read_u64_le(buf, pos)?;
        pos += size_of::<u64>();
        Ok((
            Self {
                stream_id,
                topic_id,
                partition_id,
                installation,
                partition_op,
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
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return ApplyReply::ok(Bytes::new());
        };
        let Some(topic_id) = state.resolve_topic_id(stream_id, &self.topic_id) else {
            return ApplyReply::ok(Bytes::new());
        };
        let Some(topic) = state
            .items
            .get_mut(stream_id)
            .and_then(|stream| stream.topics.get_mut(topic_id))
        else {
            return ApplyReply::ok(Bytes::new());
        };
        let completed = topic
            .consumer_groups
            .get_mut(&self.installation.group_id)
            .is_some_and(|group| {
                group.complete_revocation(
                    self.partition_id as usize,
                    &self.installation,
                    self.partition_op,
                )
            });
        if completed {
            state.revision = state.revision.wrapping_add(1);
            state.pending_revocations.remove(&(
                stream_id,
                topic_id,
                self.installation.group_id,
                self.partition_id as usize,
            ));
        }
        ApplyReply::ok(Bytes::new())
    }
}

/// A committed Register refreshes all memberships of that identity.
#[derive(Debug, Clone)]
pub struct RefreshConsumerGroupSessionRequest {
    pub client_id: u128,
    pub session: u64,
}

impl StateHandler for RefreshConsumerGroupSessionRequest {
    type State = StreamsInner;

    fn apply(&self, state: &mut StreamsInner, timestamp: IggyTimestamp) -> ApplyReply {
        if let Some(memberships) = state.consumer_group_members.get(&self.client_id) {
            for &(stream_id, topic_id, group_id, member_id) in memberships {
                let Some(topic) = state
                    .items
                    .get_mut(stream_id)
                    .and_then(|stream| stream.topics.get_mut(topic_id))
                else {
                    continue;
                };
                let Some(group) = topic.consumer_groups.get_mut(&group_id) else {
                    continue;
                };
                let Some(member) = group.members.get_mut(member_id) else {
                    continue;
                };
                if member.session != Some(self.session) {
                    member.session = Some(self.session);
                    group.rebalance_members(
                        &topic.partitions,
                        state.apply_context.metadata_op,
                        timestamp.as_micros(),
                    );
                    group.index_pending_revocations(
                        stream_id,
                        topic_id,
                        &mut state.pending_revocations,
                    );
                }
            }
        }
        ApplyReply::ok(Bytes::new())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupMemberSnapshot {
    pub id: usize,
    pub client_id: u128,
    pub session: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupSnapshot {
    pub id: u64,
    pub generation: u64,
    pub name: String,
    pub members: Vec<(usize, ConsumerGroupMemberSnapshot)>,
    pub assignments: BTreeMap<usize, ConsumerGroupAssignment>,
}

impl ConsumerGroupSnapshot {
    #[must_use]
    pub fn from_group(group: &ConsumerGroup) -> Self {
        Self {
            id: group.id,
            generation: group.generation,
            name: group.name.to_string(),
            members: group
                .members
                .iter()
                .map(|(member_id, member)| {
                    (
                        member_id,
                        ConsumerGroupMemberSnapshot {
                            id: member.id,
                            client_id: member.client_id,
                            session: member.session,
                        },
                    )
                })
                .collect(),
            assignments: group.assignments.clone(),
        }
    }

    #[must_use]
    pub fn into_group(self) -> ConsumerGroup {
        let mut group = ConsumerGroup {
            id: self.id,
            generation: self.generation,
            name: Arc::from(self.name),
            members: self
                .members
                .into_iter()
                .map(|(key, member)| {
                    (
                        key,
                        ConsumerGroupMember {
                            id: member.id,
                            client_id: member.client_id,
                            session: member.session,
                            partitions: Vec::new(),
                            pending_revocations: Vec::new(),
                        },
                    )
                })
                .collect(),
            assignments: self.assignments,
        };
        group.rebuild_member_assignments();
        group
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stm::snapshot::Snapshotable;
    use crate::stm::stream::Streams;
    use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
    use iggy_binary_protocol::requests::streams::{CreateStreamRequest, DeleteStreamRequest};
    use iggy_binary_protocol::requests::topics::{
        CreateTopicRequest, CreateTopicWithAssignmentsRequest, DeleteTopicRequest,
    };
    use iggy_binary_protocol::{WireName, WireOptions};
    use iggy_common::IggyTimestamp;

    #[test]
    fn given_balanced_owners_when_fourth_member_joins_should_move_only_three_partitions() {
        let mut group = ConsumerGroup::new(0, Arc::from("group"));
        let partitions: Vec<_> = (0..12)
            .map(|id| Partition::new(id, id as u64 + 1, IggyTimestamp::from(1), 1, 0))
            .collect();
        let complete = |group: &mut ConsumerGroup| {
            let pending: Vec<_> = group
                .assignments
                .iter()
                .filter_map(|(&id, assignment)| {
                    let pending = assignment.pending?;
                    Some((
                        id,
                        InstallConsumerGroupOwnerRequest {
                            incarnation: assignment.incarnation,
                            group_id: group.id,
                            owner: pending.owner,
                            metadata_op: pending.metadata_op,
                        },
                    ))
                })
                .collect();
            for (id, installation) in pending {
                assert!(group.complete_revocation(id, &installation, 1));
            }
        };
        for id in 0..3 {
            let mut member = ConsumerGroupMember::new(id, id as u128 + 1);
            member.session = Some(id as u64 + 1);
            group.members.insert(member);
            group.rebalance_members(&partitions, id as u64 + 10, 100);
            complete(&mut group);
        }
        assert!(
            group
                .members
                .iter()
                .all(|(_, member)| member.partitions.len() == 4)
        );
        let before = group.assignments.clone();
        let mut member = ConsumerGroupMember::new(3, 4);
        member.session = Some(4);
        group.members.insert(member);
        group.rebalance_members(&partitions, 20, 200);
        assert_eq!(
            group
                .assignments
                .values()
                .filter(|assignment| assignment.pending.is_some())
                .count(),
            3
        );
        for (&id, assignment) in &group.assignments {
            assert_eq!(assignment.owner, before[&id].owner);
        }
        complete(&mut group);
        assert!(
            group
                .members
                .iter()
                .all(|(_, member)| member.partitions.len() == 3)
        );
        let before = group.assignments.clone();
        group.members.remove(3);
        group.rebalance_members(&partitions, 30, 300);
        assert_eq!(
            group
                .assignments
                .values()
                .filter(|assignment| assignment.pending.is_some())
                .count(),
            3
        );
        for (&id, assignment) in &group.assignments {
            if assignment.pending.is_none() {
                assert_eq!(assignment.owner, before[&id].owner);
            }
        }
        complete(&mut group);
        assert!(
            group
                .members
                .iter()
                .all(|(_, member)| member.partitions.len() == 4)
        );
    }

    #[test]
    fn given_durable_owner_when_reassigned_should_fence_superseded_installations() {
        let mut group = ConsumerGroup::new(0, Arc::from("group"));
        let mut first = ConsumerGroupMember::new(0, 7);
        first.session = Some(11);
        group.members.insert(first);
        let mut partitions: Vec<_> = (0..2)
            .map(|id| Partition::new(id, id as u64 + 1, IggyTimestamp::from(1), 1, 0))
            .collect();
        group.rebalance_members(&partitions, 12, 100);
        assert_eq!(group.members[0].partitions, [] as [usize; 0]);
        for partition_id in 0..2 {
            let assignment = &group.assignments[&partition_id];
            let pending = assignment.pending.unwrap();
            assert!(pending.skip_drain);
            let installation = InstallConsumerGroupOwnerRequest {
                incarnation: assignment.incarnation,
                group_id: group.id,
                owner: pending.owner,
                metadata_op: pending.metadata_op,
            };
            assert!(group.complete_revocation(partition_id, &installation, 1));
        }
        let unchanged = group.assignments[&0].owner;
        let old_owner = group.assignments[&1].owner;
        let mut second = ConsumerGroupMember::new(1, 8);
        second.session = Some(13);
        group.members.insert(second);
        group.rebalance_members(&partitions, 14, 200);
        assert_eq!(group.assignments[&0].owner, unchanged);
        assert!(group.assignments[&0].pending.is_none());
        assert_eq!(group.assignments[&1].owner, old_owner);
        assert_eq!(group.members[0].pollable_partitions(), [0]);
        assert_eq!(group.members[1].partitions, [] as [usize; 0]);
        let assignment = &group.assignments[&1];
        let pending = assignment.pending.unwrap();
        assert!(!pending.skip_drain);
        let superseded = InstallConsumerGroupOwnerRequest {
            incarnation: assignment.incarnation,
            group_id: group.id,
            owner: pending.owner,
            metadata_op: pending.metadata_op,
        };
        group.rebalance_members(&partitions, 15, 300);
        assert_eq!(group.assignments[&1].pending, Some(pending));
        group.members.remove(1);
        group.rebalance_members(&partitions, 16, 400);
        assert!(!group.complete_revocation(1, &superseded, 2));
        let replacement = group.assignments[&1].pending.unwrap();
        assert_eq!(replacement.owner.client_id, old_owner.unwrap().client_id);
        assert!(replacement.owner.generation > superseded.owner.generation);
        let mut installation = InstallConsumerGroupOwnerRequest {
            incarnation: superseded.incarnation,
            group_id: group.id,
            owner: replacement.owner,
            metadata_op: replacement.metadata_op,
        };
        installation.metadata_op -= 1;
        assert!(!group.complete_revocation(1, &installation, 3));
        installation.metadata_op += 1;
        assert!(group.complete_revocation(1, &installation, 3));
        assert!(!group.complete_revocation(1, &superseded, 2));
        assert_eq!(group.members[0].pollable_partitions(), [0, 1]);

        partitions[1].created_revision += 1;
        group.rebalance_members(&partitions, 17, 500);
        assert_eq!(group.assignments[&0].owner, unchanged);
        assert!(group.assignments[&1].owner.is_none());
        assert!(group.assignments[&1].pending.unwrap().skip_drain);
        assert!(!group.complete_revocation(1, &installation, 3));
        group.members.remove(0);
        group.rebalance_members(&partitions, 18, 600);
        for (&partition_id, assignment) in &group.assignments.clone() {
            let pending = assignment.pending.unwrap();
            assert!(pending.owner.is_unassigned());
            assert!(pending.skip_drain);
            let installation = InstallConsumerGroupOwnerRequest {
                incarnation: assignment.incarnation,
                group_id: group.id,
                owner: pending.owner,
                metadata_op: pending.metadata_op,
            };
            assert!(group.complete_revocation(partition_id, &installation, 4));
            assert!(group.assignments[&partition_id].owner.is_none());
        }
    }

    #[test]
    fn given_session_refresh_when_rebalancing_should_index_revocations_like_full_rebuild() {
        const CLIENT: u128 = 7;
        const OTHER: u128 = 8;
        const SESSION: u64 = 11;
        let mut state = streams_with_topic();
        assert_eq!(create_group(&mut state, "first").code, 0);
        assert_eq!(create_group(&mut state, "second").code, 0);
        assert_eq!(join(&mut state, 0, 0, 0, CLIENT).code, 0);
        assert_eq!(join(&mut state, 0, 0, 1, OTHER).code, 0);
        state.apply_context.metadata_op += 1;
        let refresh = RefreshConsumerGroupSessionRequest {
            client_id: CLIENT,
            session: SESSION,
        };
        assert_eq!(
            StateHandler::apply(&refresh, &mut state, IggyTimestamp::now()).code,
            0
        );
        let refreshed = state.pending_revocations.clone();
        assert_eq!(refreshed.len(), 2);
        assert_eq!(refreshed[&(0, 0, 0, 0)].installation.owner.session, SESSION);
        assert_eq!(refreshed[&(0, 0, 1, 0)].installation.owner.client_id, OTHER);
        state.recompute_consumer_group_metadata();
        assert_eq!(state.pending_revocations, refreshed);
    }

    #[test]
    fn session_refresh_index_tracks_membership_removal_and_rejoin() {
        const CLIENT: u128 = 7;
        const OTHER: u128 = 8;
        let mut state = streams_with_topic();
        assert_eq!(create_group(&mut state, "first").code, 0);
        assert_eq!(create_group(&mut state, "second").code, 0);
        assert_eq!(join(&mut state, 0, 0, 0, CLIENT).code, 0);
        assert_eq!(join(&mut state, 0, 0, 1, CLIENT).code, 0);
        assert_eq!(join(&mut state, 0, 0, 0, CLIENT).code, 0);
        assert_eq!(join(&mut state, 0, 0, 0, OTHER).code, 0);
        assert_eq!(state.consumer_group_members[&CLIENT].len(), 2);
        assert_eq!(leave(&mut state, 0, 0, 0, CLIENT).code, 0);
        assert_eq!(state.consumer_group_members[&CLIENT].len(), 1);
        assert_eq!(
            crate::stm::lifecycle::apply_with_lifecycle_completion(
                &DeleteConsumerGroupRequest {
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                    group_id: WireIdentifier::numeric(1),
                },
                &mut state,
                IggyTimestamp::now(),
            )
            .code,
            0
        );
        assert!(!state.consumer_group_members.contains_key(&CLIENT));
        assert_eq!(
            crate::stm::lifecycle::apply_with_lifecycle_completion(
                &RemoveConsumerGroupMemberRequest { client_id: OTHER },
                &mut state,
                IggyTimestamp::now(),
            )
            .code,
            0
        );
        assert!(state.consumer_group_members.is_empty());
        assert_eq!(join(&mut state, 0, 0, 0, CLIENT).code, 0);
        let streams = Streams::from(state);
        streams.refresh_consumer_group_session(
            CLIENT,
            11,
            11,
            iggy_common::IggyTimestamp::default(),
        );
        assert_eq!(streams.consumer_group_session(CLIENT), Some(11));
        assert_eq!(streams.consumer_group_session(OTHER), None);
    }

    #[test]
    fn deleting_a_parent_removes_its_session_refresh_index() {
        const CLIENT: u128 = 7;
        for delete_stream in [false, true] {
            let mut state = streams_with_topic();
            assert_eq!(create_group(&mut state, "group").code, 0);
            assert_eq!(join(&mut state, 0, 0, 0, CLIENT).code, 0);
            let reply = if delete_stream {
                crate::stm::lifecycle::apply_with_lifecycle_completion(
                    &DeleteStreamRequest {
                        stream_id: WireIdentifier::numeric(0),
                    },
                    &mut state,
                    IggyTimestamp::now(),
                )
            } else {
                crate::stm::lifecycle::apply_with_lifecycle_completion(
                    &DeleteTopicRequest {
                        stream_id: WireIdentifier::numeric(0),
                        topic_id: WireIdentifier::numeric(0),
                    },
                    &mut state,
                    IggyTimestamp::now(),
                )
            };
            assert_eq!(reply.code, 0);
            assert!(state.consumer_group_members.is_empty());
        }
    }

    #[test]
    fn session_refresh_preserves_other_clients_after_boot_and_in_place_restore() {
        const CLIENT: u128 = 7;
        const OTHER: u128 = 8;
        let mut state = streams_with_topic();
        assert_eq!(create_group(&mut state, "first").code, 0);
        assert_eq!(create_group(&mut state, "second").code, 0);
        assert_eq!(join(&mut state, 0, 0, 0, CLIENT).code, 0);
        assert_eq!(join(&mut state, 0, 0, 1, CLIENT).code, 0);
        assert_eq!(join(&mut state, 0, 0, 0, OTHER).code, 0);
        let original = Streams::from(state);
        let boot = Streams::from_snapshot(original.to_snapshot()).unwrap();
        let mut restored = StreamsInner::new();
        restored.restore_in_place(original.to_snapshot());
        for streams in [boot, Streams::from(restored)] {
            for epoch in [11, 12, 13] {
                streams.refresh_consumer_group_session(
                    CLIENT,
                    epoch,
                    epoch,
                    iggy_common::IggyTimestamp::default(),
                );
                streams.refresh_consumer_group_session(
                    u128::MAX,
                    epoch,
                    epoch,
                    iggy_common::IggyTimestamp::default(),
                );
                assert!(streams.read(|inner| {
                    let topic = &inner.items[0].topics[0];
                    for group in topic.consumer_groups.values() {
                        for (_, member) in &group.members {
                            assert_eq!(
                                member.session,
                                Some(if member.client_id == CLIENT { epoch } else { 1 })
                            );
                        }
                    }
                    !inner.consumer_group_members.contains_key(&u128::MAX)
                }));
            }
        }
    }

    #[test]
    fn given_pending_ownership_when_restoring_snapshot_should_preserve_inactive_successor() {
        const CLIENT: u128 = 7;
        const SESSION: u64 = 11;
        let mut group = ConsumerGroup::new(0, Arc::from("group"));
        let mut member = ConsumerGroupMember::new(0, CLIENT);
        member.session = Some(SESSION);
        group.members.insert(member);
        let partition = Partition::new(2, 1, IggyTimestamp::from(1), 1, 0);
        group.rebalance_members(&[partition], 12, 1);
        let encoded = rmp_serde::to_vec(&ConsumerGroupSnapshot::from_group(&group)).unwrap();
        let mut restored = rmp_serde::from_slice::<ConsumerGroupSnapshot>(&encoded)
            .unwrap()
            .into_group();
        assert_eq!(restored.members[0].session, Some(SESSION));
        assert_eq!(restored.members[0].partitions, [] as [usize; 0]);
        assert_eq!(restored.assignments, group.assignments);
        let assignment = &restored.assignments[&2];
        let pending = assignment.pending.unwrap();
        let installation = InstallConsumerGroupOwnerRequest {
            incarnation: assignment.incarnation,
            group_id: restored.id,
            owner: pending.owner,
            metadata_op: pending.metadata_op,
        };
        assert!(!restored.complete_revocation(2, &installation, 0));
        assert_eq!(restored.members[0].partitions, [] as [usize; 0]);
        assert!(restored.complete_revocation(2, &installation, 21));
        assert_eq!(restored.members[0].partitions, [2]);
        let encoded = rmp_serde::to_vec(&ConsumerGroupSnapshot::from_group(&restored)).unwrap();
        let restored = rmp_serde::from_slice::<ConsumerGroupSnapshot>(&encoded)
            .unwrap()
            .into_group();
        assert_eq!(restored.members[0].partitions, [2]);
        assert!(restored.assignments[&2].pending.is_none());

        let legacy = rmp_serde::to_vec(&(0usize, CLIENT, vec![2usize])).unwrap();
        assert!(rmp_serde::from_slice::<ConsumerGroupMemberSnapshot>(&legacy).is_err());
    }

    #[test]
    fn replicated_join_requires_exact_live_session_identity() {
        let join = JoinConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            group_id: WireIdentifier::numeric(0),
            client_id: 7,
            session: 11,
        };
        let encoded = join.to_bytes();
        let decoded = JoinConsumerGroupRequest::decode_from(&encoded).unwrap();
        assert_eq!(decoded.client_id, join.client_id);
        assert_eq!(decoded.session, join.session);
        for length in 0..encoded.len() {
            assert!(JoinConsumerGroupRequest::decode_from(&encoded[..length]).is_err());
        }
        let mut invalid = encoded.to_vec();
        invalid.push(0);
        assert!(JoinConsumerGroupRequest::decode_from(&invalid).is_err());
        invalid = encoded.to_vec();
        let end = invalid.len();
        invalid[end - size_of::<u64>()..].fill(0);
        assert!(JoinConsumerGroupRequest::decode_from(&invalid).is_err());
    }

    fn streams_with_topic() -> StreamsInner {
        let mut inner = StreamsInner::new();
        let _ = StateHandler::apply(
            &CreateStreamRequest {
                name: WireName::new("stream").unwrap(),
                options: WireOptions::empty(),
            },
            &mut inner,
            IggyTimestamp::now(),
        );
        let create_topic = CreateTopicWithAssignmentsRequest {
            created_view: 0,
            request: CreateTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                partitions_count: 1,
                name: WireName::new("topic").unwrap(),
                options: WireOptions::empty(),
            },
            derived_options: WireOptions::empty(),
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

    fn join(
        state: &mut StreamsInner,
        stream: u32,
        topic: u32,
        group: u32,
        client_id: u128,
    ) -> ApplyReply {
        state.apply_context.metadata_op += 1;
        StateHandler::apply(
            &JoinConsumerGroupRequest {
                stream_id: WireIdentifier::numeric(stream),
                topic_id: WireIdentifier::numeric(topic),
                group_id: WireIdentifier::numeric(group),
                client_id,

                session: 1,
            },
            state,
            IggyTimestamp::now(),
        )
    }

    fn leave(
        state: &mut StreamsInner,
        stream: u32,
        topic: u32,
        group: u32,
        client_id: u128,
    ) -> ApplyReply {
        StateHandler::apply(
            &LeaveConsumerGroupRequest {
                stream_id: WireIdentifier::numeric(stream),
                topic_id: WireIdentifier::numeric(topic),
                group_id: WireIdentifier::numeric(group),
                client_id,
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
    fn given_missing_parent_when_apply_create_consumer_group_should_return_not_found() {
        let mut state = streams_with_topic();

        let missing_stream = StateHandler::apply(
            &CreateConsumerGroupRequest {
                stream_id: WireIdentifier::numeric(999),
                topic_id: WireIdentifier::numeric(0),
                name: WireName::new("group").unwrap(),
            },
            &mut state,
            IggyTimestamp::now(),
        );
        assert_eq!(
            missing_stream.code,
            u32::from(CreateConsumerGroupResult::StreamNotFound)
        );
        assert!(missing_stream.body.is_empty());

        let missing_topic = StateHandler::apply(
            &CreateConsumerGroupRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(999),
                name: WireName::new("group").unwrap(),
            },
            &mut state,
            IggyTimestamp::now(),
        );
        assert_eq!(
            missing_topic.code,
            u32::from(CreateConsumerGroupResult::TopicNotFound)
        );
        assert!(missing_topic.body.is_empty());
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
        assert_eq!(
            apply.code,
            u32::from(DeleteConsumerGroupResult::ConsumerGroupNotFound)
        );
        assert!(apply.body.is_empty());
    }

    #[test]
    fn given_missing_levels_when_apply_delete_consumer_group_should_mirror_resolution_ladder() {
        let mut state = streams_with_topic();
        let missing_stream = StateHandler::apply(
            &DeleteConsumerGroupRequest {
                stream_id: WireIdentifier::numeric(999),
                topic_id: WireIdentifier::numeric(0),
                group_id: WireIdentifier::numeric(0),
            },
            &mut state,
            IggyTimestamp::now(),
        );
        assert_eq!(
            missing_stream.code,
            u32::from(DeleteConsumerGroupResult::StreamNotFound)
        );

        let missing_topic = StateHandler::apply(
            &DeleteConsumerGroupRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(999),
                group_id: WireIdentifier::numeric(0),
            },
            &mut state,
            IggyTimestamp::now(),
        );
        assert_eq!(
            missing_topic.code,
            u32::from(DeleteConsumerGroupResult::TopicNotFound)
        );
    }

    #[test]
    fn given_existing_group_when_apply_join_consumer_group_should_succeed() {
        let mut state = streams_with_topic();
        assert_eq!(create_group(&mut state, "group").code, 0);

        // Group ids are 0-based, so the group just created resolves as id 0.
        let apply = join(&mut state, 0, 0, 0, 1);
        assert_eq!(apply.code, 0);
        assert!(apply.body.is_empty());

        // A re-join from the same client is an idempotent success.
        assert_eq!(join(&mut state, 0, 0, 0, 1).code, 0);
    }

    #[test]
    fn given_missing_stream_when_apply_join_consumer_group_should_return_stream_not_found() {
        let mut state = streams_with_topic();
        let apply = join(&mut state, 999, 0, 0, 1);
        assert_eq!(
            apply.code,
            u32::from(JoinConsumerGroupResult::StreamNotFound)
        );
        assert!(apply.body.is_empty());
    }

    #[test]
    fn given_missing_topic_when_apply_join_consumer_group_should_return_topic_not_found() {
        let mut state = streams_with_topic();
        let apply = join(&mut state, 0, 999, 0, 1);
        assert_eq!(
            apply.code,
            u32::from(JoinConsumerGroupResult::TopicNotFound)
        );
        assert!(apply.body.is_empty());
    }

    #[test]
    fn given_missing_group_when_apply_join_consumer_group_should_return_consumer_group_not_found() {
        let mut state = streams_with_topic();
        let apply = join(&mut state, 0, 0, 999, 1);
        assert_eq!(
            apply.code,
            u32::from(JoinConsumerGroupResult::ConsumerGroupNotFound)
        );
        assert!(apply.body.is_empty());
    }

    #[test]
    fn given_joined_member_when_apply_leave_consumer_group_should_succeed() {
        let mut state = streams_with_topic();
        assert_eq!(create_group(&mut state, "group").code, 0);
        assert_eq!(join(&mut state, 0, 0, 0, 1).code, 0);

        let apply = leave(&mut state, 0, 0, 0, 1);
        assert_eq!(apply.code, 0);
        assert!(apply.body.is_empty());
    }

    #[test]
    fn given_absent_member_when_apply_leave_consumer_group_should_return_member_not_found() {
        let mut state = streams_with_topic();
        assert_eq!(create_group(&mut state, "group").code, 0);
        assert_eq!(join(&mut state, 0, 0, 0, 1).code, 0);

        // Client 2 is not a member of the group, which does exist.
        let apply = leave(&mut state, 0, 0, 0, 2);
        assert_eq!(
            apply.code,
            u32::from(LeaveConsumerGroupResult::ConsumerGroupMemberNotFound)
        );
        assert!(apply.body.is_empty());
    }

    #[test]
    fn given_missing_stream_when_apply_leave_consumer_group_should_return_stream_not_found() {
        let mut state = streams_with_topic();
        let apply = leave(&mut state, 999, 0, 0, 1);
        assert_eq!(
            apply.code,
            u32::from(LeaveConsumerGroupResult::StreamNotFound)
        );
        assert!(apply.body.is_empty());
    }

    #[test]
    fn given_missing_topic_when_apply_leave_consumer_group_should_return_topic_not_found() {
        let mut state = streams_with_topic();
        let apply = leave(&mut state, 0, 999, 0, 1);
        assert_eq!(
            apply.code,
            u32::from(LeaveConsumerGroupResult::TopicNotFound)
        );
        assert!(apply.body.is_empty());
    }

    #[test]
    fn given_missing_group_when_apply_leave_consumer_group_should_return_consumer_group_not_found()
    {
        let mut state = streams_with_topic();
        let apply = leave(&mut state, 0, 0, 999, 1);
        assert_eq!(
            apply.code,
            u32::from(LeaveConsumerGroupResult::ConsumerGroupNotFound)
        );
        assert!(apply.body.is_empty());
    }
}
