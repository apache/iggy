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

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};

use ahash::AHashMap;
use bytes::{BufMut, Bytes, BytesMut};
use iggy_binary_protocol::codec::{read_u32_le, read_u64_le};
use iggy_binary_protocol::primitives::partition_history::ConsumerGroupOwner;
use iggy_binary_protocol::requests::consumer_groups::DeleteConsumerGroupRequest;
use iggy_binary_protocol::requests::partitions::{
    DeletePartitionsRequest, InstallConsumerGroupOwnerRequest, TransitionPartitionHistoryRequest,
};
use iggy_binary_protocol::requests::streams::DeleteStreamRequest;
use iggy_binary_protocol::requests::topics::DeleteTopicRequest;
use iggy_binary_protocol::{Operation, WireDecode, WireEncode, WireError, WireIdentifier};
use iggy_common::{IggyError, IggyTimestamp};
use serde::{Deserialize, Serialize};

use crate::stm::result::ApplyReply;
use crate::stm::snapshot::SnapshotError;
use crate::stm::stream::{
    PartitionSnapshot, Streams, StreamsInner, StreamsSnapshot, TopicSnapshot,
};
use crate::stm::{ApplyContext, StateHandler};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum LifecycleAction {
    DeleteStream,
    DeleteTopic,
    DeletePartitions { count: u32 },
    DeleteConsumerGroup { group_id: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LifecycleFence {
    History(TransitionPartitionHistoryRequest),
    Owner(InstallConsumerGroupOwnerRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecyclePartition {
    pub topic_id: u32,
    pub partition_id: u32,
    pub fence: LifecycleFence,
    pub partition_op: Option<u64>,
}

/// Retained until every affected partition durably installs its exact fence.
///
/// A partition whose only replica failed recovery retires its incarnation instead
/// (see [`RETIRED_PARTITION_OP`]). The original request identity stays stable
/// through retry, failover and restore.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleIntent {
    pub context: ApplyContext,
    pub stream_id: u32,
    pub topic_id: Option<u32>,
    pub action: LifecycleAction,
    pub partitions: Vec<LifecyclePartition>,
    #[serde(skip)]
    partition_index: BTreeMap<(u32, u32), usize>,
    #[serde(skip)]
    remaining_partitions: usize,
}

impl LifecycleIntent {
    pub(crate) fn rebuild_partition_index(&mut self) {
        self.partition_index = self
            .partitions
            .iter()
            .enumerate()
            .map(|(index, partition)| ((partition.topic_id, partition.partition_id), index))
            .collect();
        self.remaining_partitions = self
            .partitions
            .iter()
            .filter(|partition| partition.partition_op.is_none())
            .count();
    }
}

/// The `partition_op` of a target whose only replica failed recovery. No log
/// can install its fence, so a durable retirement fence that keeps the
/// incarnation offline completes it instead.
pub const RETIRED_PARTITION_OP: u64 = u64::MAX;

#[derive(Debug, Clone, Copy)]
pub struct CompleteLifecycleRequest {
    pub metadata_op: u64,
    pub stream_id: u32,
    pub topic_id: u32,
    pub partition_id: u32,
    pub partition_op: u64,
}

impl WireEncode for CompleteLifecycleRequest {
    fn encoded_size(&self) -> usize {
        2 * size_of::<u64>() + 3 * size_of::<u32>()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.metadata_op);
        buf.put_u32_le(self.stream_id);
        buf.put_u32_le(self.topic_id);
        buf.put_u32_le(self.partition_id);
        buf.put_u64_le(self.partition_op);
    }
}

impl WireDecode for CompleteLifecycleRequest {
    fn decode_from(buf: &[u8]) -> Result<Self, WireError> {
        let (request, consumed) = Self::decode(buf)?;
        if consumed != buf.len() {
            return Err(WireError::Validation(
                "trailing lifecycle completion bytes".into(),
            ));
        }
        Ok(request)
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let metadata_op = read_u64_le(buf, 0)?;
        let stream_id = read_u32_le(buf, 8)?;
        let topic_id = read_u32_le(buf, 12)?;
        let partition_id = read_u32_le(buf, 16)?;
        let partition_op = read_u64_le(buf, 20)?;
        if metadata_op == 0 || partition_op == 0 {
            return Err(WireError::Validation(
                "missing lifecycle completion identity".into(),
            ));
        }
        let request = Self {
            metadata_op,
            stream_id,
            topic_id,
            partition_id,
            partition_op,
        };
        let size = request.encoded_size();
        Ok((request, size))
    }
}

impl Streams {
    pub fn lifecycle_pending(&self, metadata_op: u64) -> bool {
        self.read(|state| state.lifecycle_intents.contains_key(&metadata_op))
    }

    pub fn has_pending_lifecycles(&self) -> bool {
        self.read(|state| !state.lifecycle_intents.is_empty())
    }

    pub fn pending_lifecycles(&self) -> Vec<LifecycleIntent> {
        self.read(|state| state.lifecycle_intents.values().cloned().collect())
    }

    pub fn lifecycle_blocks(&self, stream_id: usize, topic_id: Option<usize>) -> bool {
        self.read(|state| state.lifecycle_blocks(stream_id, topic_id))
    }

    pub fn lifecycle_blocks_consumer(
        &self,
        stream_id: usize,
        topic_id: usize,
        group_id: Option<u64>,
    ) -> bool {
        self.read(|state| state.lifecycle_blocks_consumer(stream_id, topic_id, group_id))
    }

    pub fn request_lifecycle_blocked(&self, operation: Operation, body: &[u8]) -> bool {
        let topic_scoped = match operation {
            Operation::UpdateStream | Operation::DeleteStream | Operation::CreateTopic => false,
            Operation::UpdateTopic
            | Operation::DeleteTopic
            | Operation::CreatePartitions
            | Operation::DeletePartitions
            | Operation::CreateConsumerGroup
            | Operation::DeleteConsumerGroup
            | Operation::JoinConsumerGroup
            | Operation::LeaveConsumerGroup => true,
            _ => return false,
        };
        let Ok((stream, consumed)) = WireIdentifier::decode(body) else {
            return false;
        };
        self.read(|state| {
            let Some(stream_id) = state.resolve_stream_id(&stream) else {
                return false;
            };
            if operation == Operation::CreateTopic {
                return state.lifecycle_blocks_topic_creation(stream_id);
            }
            if !topic_scoped {
                return state.lifecycle_blocks(stream_id, None);
            }
            let Ok((topic, topic_bytes)) = WireIdentifier::decode(&body[consumed..]) else {
                return false;
            };
            let Some(topic_id) = state.resolve_topic_id(stream_id, &topic) else {
                return false;
            };
            if operation == Operation::CreateConsumerGroup {
                return state.lifecycle_blocks_consumer(stream_id, topic_id, None);
            }
            if matches!(
                operation,
                Operation::JoinConsumerGroup | Operation::LeaveConsumerGroup
            ) {
                let Ok((group, _)) = WireIdentifier::decode(&body[consumed + topic_bytes..]) else {
                    return false;
                };
                let group_id = state
                    .items
                    .get(stream_id)
                    .and_then(|stream| stream.topics.get(topic_id))
                    .and_then(|topic| topic.resolve_group_id(&group));
                return state.lifecycle_blocks_consumer(stream_id, topic_id, group_id);
            }
            state.lifecycle_blocks(stream_id, Some(topic_id))
        })
    }
}

impl StreamsInner {
    pub(crate) fn lifecycle_blocks(&self, stream_id: usize, topic_id: Option<usize>) -> bool {
        self.lifecycle_blockers(stream_id, topic_id)
            .next()
            .is_some()
    }

    /// A topic-scoped intent leaves the rest of its stream writable, so only a
    /// stream-wide intent blocks a new topic. Admission and apply share this rule.
    pub(crate) fn lifecycle_blocks_topic_creation(&self, stream_id: usize) -> bool {
        self.lifecycle_blockers(stream_id, None)
            .any(|intent| intent.topic_id.is_none())
    }

    pub(crate) fn lifecycle_blocks_consumer(
        &self,
        stream_id: usize,
        topic_id: usize,
        group_id: Option<u64>,
    ) -> bool {
        self.lifecycle_blockers(stream_id, Some(topic_id))
            .any(|intent| {
                !matches!(intent.action, LifecycleAction::DeleteConsumerGroup { group_id: deleted }
                if Some(u64::from(deleted)) != group_id)
            })
    }

    fn lifecycle_blockers(
        &self,
        stream_id: usize,
        topic_id: Option<usize>,
    ) -> impl Iterator<Item = &LifecycleIntent> {
        self.lifecycle_intents.values().filter(move |intent| {
            !self.finalizing_lifecycle
                && intent.stream_id as usize == stream_id
                && (intent.topic_id.is_none()
                    || topic_id.is_none()
                    || intent.topic_id.map(|id| id as usize) == topic_id)
        })
    }

    /// Called after command validation, before its destructive metadata effect.
    /// Empty scopes finish in this same apply; all other scopes retain their rows.
    #[allow(clippy::too_many_lines)]
    pub(crate) fn begin_lifecycle(
        &mut self,
        stream_id: usize,
        topic_id: Option<usize>,
        action: LifecycleAction,
    ) -> Option<ApplyReply> {
        if self.finalizing_lifecycle {
            return None;
        }
        if self.lifecycle_blocks(stream_id, topic_id) {
            return Some(ApplyReply::err(IggyError::LifecycleBusy.as_code()));
        }
        let stream = self.items.get(stream_id)?;
        let Ok(wire_stream_id) = u32::try_from(stream_id) else {
            return Some(ApplyReply::err(IggyError::InvalidCommand.as_code()));
        };
        let Ok(wire_topic_id) = topic_id.map(u32::try_from).transpose() else {
            return Some(ApplyReply::err(IggyError::InvalidCommand.as_code()));
        };
        let mut partitions = Vec::new();
        for (id, topic) in &stream.topics {
            if topic_id.is_some_and(|wanted| wanted != id) {
                continue;
            }
            let first = match action {
                LifecycleAction::DeletePartitions { count } => {
                    topic.partitions.len().checked_sub(count as usize)?
                }
                _ => 0,
            };
            for partition in &topic.partitions[first..] {
                let incarnation = partition.created_revision;
                let fence = if let LifecycleAction::DeleteConsumerGroup { group_id } = action {
                    let group = topic.consumer_groups.get(&u64::from(group_id))?;
                    let Some(generation) = group.generation.checked_add(1) else {
                        return Some(ApplyReply::err(IggyError::InvalidCommand.as_code()));
                    };
                    LifecycleFence::Owner(InstallConsumerGroupOwnerRequest {
                        incarnation,
                        group_id: u64::from(group_id),
                        owner: ConsumerGroupOwner {
                            client_id: 0,
                            session: 0,
                            generation,
                        },
                        metadata_op: self.apply_context.metadata_op,
                    })
                } else {
                    LifecycleFence::History(TransitionPartitionHistoryRequest {
                        incarnation,
                        metadata_op: self.apply_context.metadata_op,
                    })
                };
                let (Ok(topic_id), Ok(partition_id)) =
                    (u32::try_from(id), u32::try_from(partition.id))
                else {
                    return Some(ApplyReply::err(IggyError::InvalidCommand.as_code()));
                };
                partitions.push(LifecyclePartition {
                    topic_id,
                    partition_id,
                    fence,
                    partition_op: None,
                });
            }
        }
        if partitions.is_empty() {
            return None;
        }
        if self.apply_context.metadata_op == 0 {
            return Some(ApplyReply::err(IggyError::InvalidCommand.as_code()));
        }
        let mut intent = LifecycleIntent {
            context: self.apply_context,
            stream_id: wire_stream_id,
            topic_id: wire_topic_id,
            action,
            partitions,
            partition_index: BTreeMap::new(),
            remaining_partitions: 0,
        };
        intent.rebuild_partition_index();
        self.lifecycle_intents
            .insert(self.apply_context.metadata_op, intent);
        self.revision = self.revision.wrapping_add(1);
        Some(ApplyReply::ok(Bytes::new()))
    }
}

impl StateHandler for CompleteLifecycleRequest {
    type State = StreamsInner;

    fn apply(&self, state: &mut StreamsInner, timestamp: IggyTimestamp) -> ApplyReply {
        let Entry::Occupied(mut entry) = state.lifecycle_intents.entry(self.metadata_op) else {
            return ApplyReply::ok(Bytes::new());
        };
        let intent = entry.get_mut();
        if intent.stream_id != self.stream_id {
            return ApplyReply::err(IggyError::InvalidCommand.as_code());
        }
        let Some(&index) = intent
            .partition_index
            .get(&(self.topic_id, self.partition_id))
        else {
            return ApplyReply::err(IggyError::InvalidCommand.as_code());
        };
        let Some(partition) = intent.partitions.get_mut(index) else {
            return ApplyReply::err(IggyError::InvalidCommand.as_code());
        };
        if partition
            .partition_op
            .is_some_and(|op| op != self.partition_op)
        {
            return ApplyReply::err(IggyError::InvalidCommand.as_code());
        }
        let previous_completion = partition.partition_op;
        partition.partition_op = Some(self.partition_op);
        if previous_completion.is_none() {
            intent.remaining_partitions -= 1;
        }
        if intent.remaining_partitions != 0 {
            return ApplyReply::ok(Bytes::new());
        }
        let mut intent = entry.remove();
        let stream_id = WireIdentifier::numeric(intent.stream_id);
        let topic_id = WireIdentifier::numeric(intent.topic_id.unwrap_or_default());
        let context = state.apply_context;
        state.apply_context = intent.context;
        state.finalizing_lifecycle = true;
        let reply = match intent.action {
            LifecycleAction::DeleteStream => {
                DeleteStreamRequest { stream_id }.apply(state, timestamp)
            }
            LifecycleAction::DeleteTopic => DeleteTopicRequest {
                stream_id,
                topic_id,
            }
            .apply(state, timestamp),
            LifecycleAction::DeletePartitions { count } => DeletePartitionsRequest {
                stream_id,
                topic_id,
                partitions_count: count,
            }
            .apply(state, timestamp),
            LifecycleAction::DeleteConsumerGroup { group_id } => DeleteConsumerGroupRequest {
                stream_id,
                topic_id,
                group_id: WireIdentifier::numeric(group_id),
            }
            .apply(state, timestamp),
        };
        state.finalizing_lifecycle = false;
        state.apply_context = context;
        if reply.code != 0 {
            // Keep the final report pending so reconciliation can retry finalization.
            if let Some(partition) = intent.partitions.get_mut(index) {
                partition.partition_op = previous_completion;
                if previous_completion.is_none() {
                    intent.remaining_partitions += 1;
                }
            }
            state.lifecycle_intents.insert(self.metadata_op, intent);
        }
        reply
    }
}

/// Owner rows and lifecycle targets look partitions up by id. A scan per row
/// made recovery quadratic in partitions per topic. The first duplicate wins,
/// as it did for the scan.
fn partition_index(topic: &TopicSnapshot) -> AHashMap<usize, &PartitionSnapshot> {
    let mut index = AHashMap::with_capacity(topic.partitions.len());
    for partition in &topic.partitions {
        index.entry(partition.id).or_insert(partition);
    }
    index
}

impl StreamsSnapshot {
    #[allow(clippy::too_many_lines)]
    pub(crate) fn validate_ownership_history(&self, metadata_op: u64) -> Result<(), SnapshotError> {
        for (_, stream) in &self.items {
            for (_, topic) in &stream.topics {
                let partitions = partition_index(topic);
                for (group_id, group) in &topic.consumer_groups {
                    if group.id != *group_id || *group_id >= topic.next_consumer_group_id {
                        return Err(SnapshotError::InvalidState(
                            "consumer group allocation is inconsistent",
                        ));
                    }
                    let covers_all_partitions = group.assignments.len() == topic.partitions.len();
                    if (!group.members.is_empty() || !group.assignments.is_empty())
                        && !covers_all_partitions
                    {
                        return Err(SnapshotError::InvalidState(
                            "incomplete consumer ownership coverage",
                        ));
                    }
                    let mut members = BTreeSet::new();
                    for (_, member) in &group.members {
                        if member.client_id == 0
                            || member.session.is_none_or(|session| session == 0)
                            || !members.insert((member.client_id, member.session))
                        {
                            return Err(SnapshotError::InvalidState(
                                "invalid consumer member identity",
                            ));
                        }
                    }
                    for (&partition_id, assignment) in &group.assignments {
                        let partition =
                            partitions
                                .get(&partition_id)
                                .ok_or(SnapshotError::InvalidState(
                                    "owner refers to an absent partition",
                                ))?;
                        if assignment.incarnation != partition.created_revision {
                            return Err(SnapshotError::InvalidState(
                                "owner belongs to another history",
                            ));
                        }
                        if assignment.owner.is_some_and(|owner| {
                            owner.is_unassigned()
                                || owner.client_id == 0
                                || owner.session == 0
                                || owner.generation == 0
                                || owner.generation > group.generation
                                || (assignment.pending.is_none()
                                    && !members.contains(&(owner.client_id, Some(owner.session))))
                        }) {
                            return Err(SnapshotError::InvalidState(
                                "invalid installed owner identity",
                            ));
                        }
                        if let Some(pending) = assignment.pending
                            && ((pending.owner.client_id == 0) != (pending.owner.session == 0)
                                || pending.owner.generation == 0
                                || pending.owner.generation > group.generation
                                || pending.metadata_op == 0
                                || pending.metadata_op > metadata_op
                                || (!pending.owner.is_unassigned()
                                    && !members.contains(&(
                                        pending.owner.client_id,
                                        Some(pending.owner.session),
                                    )))
                                || assignment.owner.is_some_and(|owner| {
                                    pending.owner.generation <= owner.generation
                                }))
                        {
                            return Err(SnapshotError::InvalidState(
                                "invalid pending owner installation",
                            ));
                        }
                    }
                }
            }
        }
        let mut scopes = BTreeSet::new();
        for (&op, intent) in &self.lifecycle_intents {
            if op == 0
                || op != intent.context.metadata_op
                || op > metadata_op
                || intent.partitions.is_empty()
                || intent
                    .partitions
                    .iter()
                    .all(|target| target.partition_op.is_some())
            {
                return Err(SnapshotError::InvalidState(
                    "invalid lifecycle identity or completion",
                ));
            }
            let stream_wide = matches!(intent.action, LifecycleAction::DeleteStream);
            if stream_wide != intent.topic_id.is_none() {
                return Err(SnapshotError::InvalidState(
                    "lifecycle scope does not match its action",
                ));
            }
            let stream = self
                .items
                .iter()
                .find(|(id, _)| *id == intent.stream_id as usize)
                .map(|(_, stream)| stream)
                .ok_or(SnapshotError::InvalidState("lifecycle stream is absent"))?;
            let mut expected = BTreeSet::new();
            for (topic_id, topic) in &stream.topics {
                if intent.topic_id.is_some_and(|id| id as usize != *topic_id) {
                    continue;
                }
                if !scopes.insert((intent.stream_id, *topic_id)) {
                    return Err(SnapshotError::InvalidState("overlapping lifecycle intents"));
                }
                let first = if let LifecycleAction::DeletePartitions { count } = intent.action {
                    if count == 0 {
                        return Err(SnapshotError::InvalidState("empty partition deletion"));
                    }
                    topic.partitions.len().checked_sub(count as usize).ok_or(
                        SnapshotError::InvalidState("partition deletion exceeds its topic"),
                    )?
                } else {
                    0
                };
                for partition in &topic.partitions[first..] {
                    expected.insert((*topic_id, partition.id));
                }
            }
            let mut topics = AHashMap::with_capacity(stream.topics.len());
            for (topic_id, topic) in &stream.topics {
                topics
                    .entry(*topic_id)
                    .or_insert_with(|| (topic, partition_index(topic)));
            }
            for target in &intent.partitions {
                let (topic, partitions) = topics
                    .get(&(target.topic_id as usize))
                    .ok_or(SnapshotError::InvalidState("lifecycle topic is absent"))?;
                let partition = partitions
                    .get(&(target.partition_id as usize))
                    .ok_or(SnapshotError::InvalidState("lifecycle partition is absent"))?;
                if !expected.remove(&(target.topic_id as usize, target.partition_id as usize))
                    || target.partition_op == Some(0)
                {
                    return Err(SnapshotError::InvalidState(
                        "duplicate or unexpected lifecycle partition",
                    ));
                }
                match (intent.action, target.fence) {
                    (
                        LifecycleAction::DeleteConsumerGroup { group_id },
                        LifecycleFence::Owner(installation),
                    ) => {
                        let group = topic
                            .consumer_groups
                            .iter()
                            .find(|(id, _)| *id == u64::from(group_id))
                            .map(|(_, group)| group)
                            .ok_or(SnapshotError::InvalidState(
                                "deleted consumer group is absent",
                            ))?;
                        if installation.incarnation != partition.created_revision
                            || installation.metadata_op != op
                            || installation.group_id != u64::from(group_id)
                            || !installation.owner.is_unassigned()
                            || installation.owner.generation == 0
                            || installation.owner.generation > group.generation.saturating_add(1)
                        {
                            return Err(SnapshotError::InvalidState(
                                "incorrect consumer group deletion fence",
                            ));
                        }
                    }
                    (LifecycleAction::DeleteConsumerGroup { .. }, _)
                    | (_, LifecycleFence::Owner(_)) => {
                        return Err(SnapshotError::InvalidState(
                            "incorrect lifecycle fence kind",
                        ));
                    }
                    (_, LifecycleFence::History(transition)) => {
                        if transition.incarnation != partition.created_revision
                            || transition.metadata_op != op
                        {
                            return Err(SnapshotError::InvalidState(
                                "incorrect partition history fence",
                            ));
                        }
                    }
                }
            }
            if !expected.is_empty() {
                return Err(SnapshotError::InvalidState(
                    "lifecycle omits affected partitions",
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn apply_with_lifecycle_completion<R: StateHandler<State = StreamsInner>>(
    request: &R,
    state: &mut StreamsInner,
    timestamp: IggyTimestamp,
) -> ApplyReply {
    state.apply_context.metadata_op += 1;
    let op = state.apply_context.metadata_op;
    let reply = request.apply(state, timestamp);
    if reply.code != 0 {
        return reply;
    }
    let Some(intent) = state.lifecycle_intents.get(&op).cloned() else {
        return reply;
    };
    for (index, partition) in intent.partitions.iter().enumerate() {
        state.apply_context.metadata_op += 1;
        let result = CompleteLifecycleRequest {
            metadata_op: op,
            stream_id: intent.stream_id,
            topic_id: partition.topic_id,
            partition_id: partition.partition_id,
            partition_op: u64::try_from(index).unwrap() + 1,
        }
        .apply(state, timestamp);
        assert_eq!(
            result.code, 0,
            "fixture must complete each matching partition fence"
        );
    }
    assert!(!state.lifecycle_intents.contains_key(&op));
    reply
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stm::State;
    use crate::stm::consumer_group::{
        CompleteConsumerGroupRevocationRequest, ConsumerGroup, ConsumerGroupMember,
        ConsumerGroupSnapshot,
    };
    use crate::stm::snapshot::Snapshotable;
    use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
    use iggy_binary_protocol::requests::consumer_groups::JoinConsumerGroupRequest;
    use iggy_binary_protocol::requests::topics::{
        CreateTopicRequest, CreateTopicWithAssignmentsRequest,
    };
    use iggy_binary_protocol::{Operation, PrepareHeader, WireName, WireOptions};
    use iggy_common::Either;
    use server_common::Message;
    use server_common::sharding::IggyNamespace;
    use std::sync::Arc;

    const INTENT_OP: u64 = 50;

    fn streams() -> Streams {
        let streams: Streams = StreamsInner::new().into();
        streams.seed_namespace(IggyNamespace::new(0, 0, 1), 2, 0);
        streams
    }

    fn context() -> ApplyContext {
        ApplyContext {
            metadata_op: INTENT_OP,
            client_id: 17,
            session: 5,
            request: 8,
        }
    }

    fn apply(
        streams: &Streams,
        operation: Operation,
        request: &impl WireEncode,
        context: ApplyContext,
    ) -> ApplyReply {
        let body = request.to_bytes();
        let size = iggy_binary_protocol::HEADER_SIZE + body.len();
        let mut prepare = Message::<PrepareHeader>::new(size).transmute_header(
            |_, header: &mut PrepareHeader| {
                header.command = iggy_binary_protocol::Command::Prepare;
                header.operation = operation;
                header.op = context.metadata_op;
                header.client = context.client_id;
                header.session = context.session;
                header.request = context.request;
                header.timestamp = 100;
                header.size = u32::try_from(size).unwrap();
            },
        );
        prepare.as_mut_slice()[iggy_binary_protocol::HEADER_SIZE..].copy_from_slice(&body);
        match State::apply(streams, prepare).unwrap() {
            Either::Left(reply) => reply,
            Either::Right(_) => panic!("lifecycle operation was not dispatched"),
        }
    }

    fn complete(streams: &Streams, partition_id: u32, partition_op: u64) {
        let reply = apply(
            streams,
            Operation::CompleteLifecycle,
            &CompleteLifecycleRequest {
                metadata_op: INTENT_OP,
                stream_id: 0,
                topic_id: 0,
                partition_id,
                partition_op,
            },
            ApplyContext {
                metadata_op: INTENT_OP + 1 + u64::from(partition_id),
                ..ApplyContext::default()
            },
        );
        assert_eq!(reply.code, 0);
    }

    #[test]
    fn given_invalid_lifecycle_snapshot_when_restoring_should_reject_without_mutating_state() {
        let streams = streams();
        assert_eq!(
            apply(
                &streams,
                Operation::DeleteTopic,
                &DeleteTopicRequest {
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                },
                context()
            )
            .code,
            0
        );
        let valid = streams.to_snapshot();
        #[allow(clippy::type_complexity)]
        let mutations: &[(&str, fn(&mut LifecycleIntent))] = &[
            ("missing partition", |intent| {
                intent.partitions.pop();
            }),
            ("duplicate partition", |intent| {
                intent.partitions.push(intent.partitions[0].clone());
            }),
            ("wrong scope", |intent| {
                intent.topic_id = Some(7);
            }),
            ("zero completion", |intent| {
                intent.partitions[0].partition_op = Some(0);
            }),
            ("false completion", |intent| {
                for partition in &mut intent.partitions {
                    partition.partition_op = Some(1);
                }
            }),
            ("wrong history", |intent| {
                if let LifecycleFence::History(transition) = &mut intent.partitions[0].fence {
                    transition.incarnation += 1;
                }
            }),
        ];
        for (label, mutate) in mutations {
            let mut invalid = valid.clone();
            mutate(invalid.lifecycle_intents.get_mut(&INTENT_OP).unwrap());
            let mut snapshot = crate::stm::snapshot::MetadataSnapshot::new(INTENT_OP);
            snapshot.streams = Some(invalid);
            let encoded = snapshot.encode().unwrap();
            assert!(
                crate::stm::snapshot::MetadataSnapshot::decode(&encoded).is_err(),
                "{label}"
            );
            assert!(
                crate::stm::snapshot::RestoreSnapshotInPlace::restore_snapshot_in_place(
                    &streams, &snapshot
                )
                .is_err(),
                "{label}"
            );
            assert_eq!(
                rmp_serde::to_vec(&streams.to_snapshot()).unwrap(),
                rmp_serde::to_vec(&valid).unwrap(),
                "{label}"
            );
        }
    }

    #[test]
    fn given_invalid_owner_snapshot_when_restoring_should_reject_missing_or_unrelated_authority() {
        let streams = streams();
        let partitions = streams.read(|inner| inner.items[0].topics[0].partitions.clone());
        let mut group = ConsumerGroup::new(0, Arc::from("group"));
        let mut member = ConsumerGroupMember::new(0, 17);
        member.session = Some(5);
        group.members.insert(member);
        group.rebalance_members(&partitions, INTENT_OP, 100);
        let assignment = &group.assignments[&0];
        let pending = assignment.pending.unwrap();
        assert!(group.complete_revocation(
            0,
            &InstallConsumerGroupOwnerRequest {
                incarnation: assignment.incarnation,
                group_id: 0,
                owner: pending.owner,
                metadata_op: pending.metadata_op,
            },
            12
        ));
        let mut valid = streams.to_snapshot();
        let topic = &mut valid.items[0].1.topics[0].1;
        topic.next_consumer_group_id = 1;
        topic
            .consumer_groups
            .push((0, ConsumerGroupSnapshot::from_group(&group)));
        assert!(valid.validate_ownership_history(INTENT_OP).is_ok());
        for mutation in 0..4 {
            let mut invalid = valid.clone();
            let group = &mut invalid.items[0].1.topics[0].1.consumer_groups[0].1;
            match mutation {
                0 => {
                    group.assignments.remove(&1);
                }
                1 => {
                    group
                        .assignments
                        .get_mut(&0)
                        .unwrap()
                        .owner
                        .as_mut()
                        .unwrap()
                        .session += 1;
                }
                2 => {
                    group
                        .assignments
                        .get_mut(&1)
                        .unwrap()
                        .pending
                        .as_mut()
                        .unwrap()
                        .owner
                        .client_id += 1;
                }
                _ => {
                    group.members[0].1.session = None;
                }
            }
            let mut snapshot = crate::stm::snapshot::MetadataSnapshot::new(INTENT_OP);
            snapshot.streams = Some(invalid);
            assert!(
                crate::stm::snapshot::MetadataSnapshot::decode(&snapshot.encode().unwrap())
                    .is_err()
            );
        }
    }

    #[test]
    fn given_partial_delete_when_restored_should_retain_identity_and_wait_for_every_partition() {
        let streams = streams();
        let namespaces = [IggyNamespace::new(0, 0, 0), IggyNamespace::new(0, 0, 1)];
        let reply = apply(
            &streams,
            Operation::DeleteTopic,
            &DeleteTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
            },
            context(),
        );
        assert_eq!(reply.code, 0);
        assert!(streams.lifecycle_pending(INTENT_OP));
        complete(&streams, 0, 12);
        for namespace in namespaces {
            assert!(streams.created_revision_for_namespace(namespace).is_some());
        }

        let snapshot = streams.to_snapshot();
        let encoded = rmp_serde::to_vec_named(&snapshot).unwrap();
        let restored = Streams::from_snapshot(rmp_serde::from_slice(&encoded).unwrap()).unwrap();
        let pending = restored.pending_lifecycles();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].context.client_id, 17);
        assert_eq!(pending[0].context.session, 5);
        assert_eq!(pending[0].context.request, 8);
        assert_eq!(pending[0].partitions[0].partition_op, Some(12));
        assert_eq!(pending[0].remaining_partitions, 1);
        complete(&restored, 0, 12);
        assert!(restored.lifecycle_pending(INTENT_OP));
        assert_eq!(restored.pending_lifecycles()[0].remaining_partitions, 1);
        for namespace in namespaces {
            assert!(restored.created_revision_for_namespace(namespace).is_some());
        }
        complete(&restored, 1, 23);
        assert!(!restored.lifecycle_pending(INTENT_OP));
        for namespace in namespaces {
            assert_eq!(restored.created_revision_for_namespace(namespace), None);
        }
    }

    #[test]
    fn given_restored_ownership_transitions_when_completed_should_retain_other_targets() {
        let streams = streams();
        let partitions = streams.read(|inner| inner.items[0].topics[0].partitions.clone());
        let mut snapshot = streams.to_snapshot();
        let topic = &mut snapshot.items[0].1.topics[0].1;
        for group_id in 0..2 {
            let mut group = ConsumerGroup::new(group_id, Arc::from(format!("group-{group_id}")));
            let mut member = ConsumerGroupMember::new(0, 17);
            member.session = Some(5);
            group.members.insert(member);
            group.rebalance_members(&partitions, INTENT_OP - 1, 100);
            topic
                .consumer_groups
                .push((group_id, ConsumerGroupSnapshot::from_group(&group)));
        }
        topic.next_consumer_group_id = 2;
        let encoded = rmp_serde::to_vec_named(&snapshot).unwrap();
        let restored = Streams::from_snapshot(rmp_serde::from_slice(&encoded).unwrap()).unwrap();
        let pending = restored.consumer_group_pending_revocations();
        assert_eq!(pending.len(), 4);
        let target = pending[0];
        let completion = CompleteConsumerGroupRevocationRequest {
            stream_id: WireIdentifier::numeric(target.stream_id),
            topic_id: WireIdentifier::numeric(target.topic_id),
            partition_id: target.partition_id,
            installation: target.installation,
            partition_op: 12,
        };
        assert_eq!(
            apply(
                &restored,
                Operation::CompleteConsumerGroupRevocation,
                &completion,
                context()
            )
            .code,
            0
        );
        let remaining = restored.consumer_group_pending_revocations();
        assert_eq!(remaining.len(), 3);
        assert!(
            !remaining
                .iter()
                .any(|transition| transition.partition_id == target.partition_id
                    && transition.installation.group_id == target.installation.group_id)
        );
        assert_eq!(
            apply(
                &restored,
                Operation::CompleteConsumerGroupRevocation,
                &completion,
                context()
            )
            .code,
            0
        );
        assert_eq!(restored.consumer_group_pending_revocations().len(), 3);
        let encoded = rmp_serde::to_vec_named(&restored.to_snapshot()).unwrap();
        let restored = Streams::from_snapshot(rmp_serde::from_slice(&encoded).unwrap()).unwrap();
        assert_eq!(restored.consumer_group_pending_revocations().len(), 3);
    }

    #[test]
    fn given_pending_group_deletion_when_other_consumers_arrive_should_isolate_the_group() {
        let streams = streams();
        let partitions = streams.read(|inner| inner.items[0].topics[0].partitions.clone());
        let mut snapshot = streams.to_snapshot();
        let topic = &mut snapshot.items[0].1.topics[0].1;
        for group_id in 0..2 {
            let mut group = ConsumerGroup::new(group_id, Arc::from(format!("group-{group_id}")));
            group.rebalance_members(&partitions, INTENT_OP - 1, 100);
            topic
                .consumer_groups
                .push((group_id, ConsumerGroupSnapshot::from_group(&group)));
        }
        topic.next_consumer_group_id = 2;
        let streams = Streams::from_snapshot(snapshot).unwrap();
        assert_eq!(
            apply(
                &streams,
                Operation::DeleteConsumerGroup,
                &DeleteConsumerGroupRequest {
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                    group_id: WireIdentifier::numeric(0),
                },
                context()
            )
            .code,
            0
        );
        assert!(streams.lifecycle_pending(INTENT_OP));
        assert!(streams.lifecycle_blocks_consumer(0, 0, Some(0)));
        assert!(!streams.lifecycle_blocks_consumer(0, 0, Some(1)));
        assert!(!streams.lifecycle_blocks_consumer(0, 0, None));
        for group_id in 0..2 {
            let request = JoinConsumerGroupRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                group_id: WireIdentifier::numeric(group_id),
            };
            assert_eq!(
                streams
                    .request_lifecycle_blocked(Operation::JoinConsumerGroup, &request.to_bytes()),
                group_id == 0
            );
        }
        assert!(!streams.request_lifecycle_blocked(Operation::SendMessages, &[]));
        assert!(
            streams.request_lifecycle_blocked(
                Operation::DeleteTopic,
                &DeleteTopicRequest {
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                }
                .to_bytes()
            )
        );
        complete(&streams, 0, 12);
        assert!(streams.lifecycle_blocks_consumer(0, 0, Some(0)));
        complete(&streams, 1, 23);
        assert!(!streams.lifecycle_blocks_consumer(0, 0, Some(0)));
        assert!(!streams.lifecycle_pending(INTENT_OP));
        assert_eq!(
            streams.read(|inner| inner.items[0].topics[0]
                .consumer_groups
                .keys()
                .copied()
                .collect::<Vec<_>>()),
            vec![1]
        );
    }

    #[test]
    fn given_pending_delete_when_conflicting_delete_arrives_should_retain_retirement_coverage() {
        let streams = streams();
        let namespace = IggyNamespace::new(0, 0, 0);
        let incarnation = streams.created_revision_for_namespace(namespace).unwrap();
        let reply = apply(
            &streams,
            Operation::DeleteTopic,
            &DeleteTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
            },
            context(),
        );
        assert_eq!(reply.code, 0);
        complete(&streams, 0, 12);
        assert_eq!(
            streams.created_revision_for_namespace(namespace),
            Some(incarnation)
        );
        let conflict = apply(
            &streams,
            Operation::DeletePartitions,
            &DeletePartitionsRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                partitions_count: 1,
            },
            ApplyContext {
                metadata_op: 60,
                ..context()
            },
        );
        assert_eq!(conflict.code, IggyError::LifecycleBusy.as_code());
        assert_eq!(streams.pending_lifecycles().len(), 1);
        complete(&streams, 1, 23);
        assert_eq!(streams.created_revision_for_namespace(namespace), None);
        streams.seed_namespace(namespace, 3, 0);
        assert!(streams.created_revision_for_namespace(namespace).unwrap() > incarnation);
    }

    #[test]
    fn given_pending_intent_when_topic_is_created_should_admit_and_apply_alike() {
        let create_topic = CreateTopicWithAssignmentsRequest {
            created_view: 0,
            request: CreateTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                partitions_count: 1,
                name: WireName::new("other").unwrap(),
                options: WireOptions::empty(),
            },
            derived_options: WireOptions::empty(),
            partitions: vec![CreatedPartitionAssignment {
                partition_id: 0,
                consensus_group_id: 1,
            }],
        };
        for stream_wide in [false, true] {
            let streams = streams();
            let intent = if stream_wide {
                apply(
                    &streams,
                    Operation::DeleteStream,
                    &DeleteStreamRequest {
                        stream_id: WireIdentifier::numeric(0),
                    },
                    context(),
                )
            } else {
                apply(
                    &streams,
                    Operation::DeleteTopic,
                    &DeleteTopicRequest {
                        stream_id: WireIdentifier::numeric(0),
                        topic_id: WireIdentifier::numeric(0),
                    },
                    context(),
                )
            };
            assert_eq!(intent.code, 0);
            assert!(streams.lifecycle_pending(INTENT_OP));
            assert_eq!(
                streams.request_lifecycle_blocked(
                    Operation::CreateTopic,
                    &create_topic.request.to_bytes()
                ),
                stream_wide,
                "admission, stream_wide={stream_wide}"
            );
            let created = apply(
                &streams,
                Operation::CreateTopicWithAssignments,
                &create_topic,
                ApplyContext {
                    metadata_op: INTENT_OP + 10,
                    ..context()
                },
            );
            let expected = if stream_wide {
                IggyError::LifecycleBusy.as_code()
            } else {
                0
            };
            assert_eq!(created.code, expected, "apply, stream_wide={stream_wide}");
        }
    }
}
