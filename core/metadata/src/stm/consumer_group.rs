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
use crate::stm::stream::StreamsInner;
use bytes::Bytes;

use bytes::{BufMut, BytesMut};
use iggy_binary_protocol::WireIdentifier;
use iggy_binary_protocol::codec::{WireDecode, WireEncode};
use iggy_binary_protocol::requests::consumer_groups::{
    CreateConsumerGroupRequest, DeleteConsumerGroupRequest,
};
use iggy_binary_protocol::responses::consumer_groups::consumer_group_response::ConsumerGroupResponse;
use iggy_binary_protocol::responses::consumer_groups::get_consumer_group::ConsumerGroupDetailsResponse;
use serde::{Deserialize, Serialize};
use slab::Slab;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ConsumerGroupMember {
    pub id: usize,
    pub client_id: u128,
    pub partitions: Vec<usize>,
}

impl ConsumerGroupMember {
    #[must_use]
    pub const fn new(id: usize, client_id: u128) -> Self {
        Self {
            id,
            client_id,
            partitions: Vec::new(),
        }
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
    pub fn rebalance_members(&mut self, partition_ids: &[usize]) {
        self.generation += 1;

        let member_count = self.members.len();
        let member_keys: Vec<usize> = self.members.iter().map(|(id, _)| id).collect();
        for &member_id in &member_keys {
            if let Some(member) = self.members.get_mut(member_id) {
                member.partitions.clear();
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
}

/// Replicated `JoinConsumerGroup`, enriched by the primary with the joining
/// client's VSR id.
///
/// The wire request carries only identifiers; the apply needs the client id
/// (which member is joining) and can't read it from the consensus header. The
/// topic's partition set is read directly from the co-located topic, so no
/// partition-count enrichment is needed.
#[derive(Debug, Clone)]
pub struct JoinConsumerGroupRequest {
    pub stream_id: WireIdentifier,
    pub topic_id: WireIdentifier,
    pub group_id: WireIdentifier,
    pub client_id: u128,
}

impl WireEncode for JoinConsumerGroupRequest {
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

impl WireDecode for JoinConsumerGroupRequest {
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

fn read_u128_le(buf: &[u8], pos: usize) -> Result<u128, iggy_binary_protocol::WireError> {
    let slice =
        buf.get(pos..pos + 16)
            .ok_or_else(|| iggy_binary_protocol::WireError::UnexpectedEof {
                offset: pos,
                need: 16,
                have: buf.len().saturating_sub(pos),
            })?;
    Ok(u128::from_le_bytes(slice.try_into().expect("16 bytes")))
}

impl StateHandler for CreateConsumerGroupRequest {
    type State = StreamsInner;
    #[allow(clippy::cast_possible_truncation)]
    fn apply(&self, state: &mut StreamsInner, _timestamp: iggy_common::IggyTimestamp) -> Bytes {
        let Some(topic) = state.topic_mut(&self.stream_id, &self.topic_id) else {
            return Bytes::new();
        };
        let name: Arc<str> = Arc::from(self.name.as_str());
        // Per-(stream,topic) name uniqueness: supersede an existing same-name
        // group in this topic (idempotent create across a delete+recreate).
        if let Some(stale_id) = topic.consumer_group_index.get(&name).copied() {
            topic.consumer_groups.remove(&stale_id);
        }
        let id = topic.next_consumer_group_id;
        topic.next_consumer_group_id += 1;
        topic
            .consumer_groups
            .insert(id, ConsumerGroup::new(id, name.clone()));
        topic.consumer_group_index.insert(name, id);

        ConsumerGroupDetailsResponse {
            group: ConsumerGroupResponse {
                id: id as u32,
                partitions_count: 0,
                members_count: 0,
                name: self.name.clone(),
            },
            members: Vec::new(),
        }
        .to_bytes()
    }
}

impl StateHandler for DeleteConsumerGroupRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner, _timestamp: iggy_common::IggyTimestamp) -> Bytes {
        let removed = {
            let Some(topic) = state.topic_mut(&self.stream_id, &self.topic_id) else {
                return Bytes::new();
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
        // Bump the partition-shaping revision so the reconciler's fast-skip
        // doesn't pass over the delete: it reclaims the group's leftover
        // offsets on the topic's surviving partitions.
        if removed {
            state.revision = state.revision.wrapping_add(1);
        }
        Bytes::new()
    }
}

impl StateHandler for JoinConsumerGroupRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner, _timestamp: iggy_common::IggyTimestamp) -> Bytes {
        let Some(topic) = state.topic_mut(&self.stream_id, &self.topic_id) else {
            return Bytes::new();
        };
        let Some(group_id) = topic.resolve_group_id(&self.group_id) else {
            return Bytes::new();
        };
        // Snapshot the live partition ids before taking a mutable borrow of the
        // group (both borrow the topic).
        let partition_ids: Vec<usize> = topic.partitions.iter().map(|p| p.id).collect();
        let Some(group) = topic.consumer_groups.get_mut(&group_id) else {
            return Bytes::new();
        };
        // Idempotent: a re-join from the same client keeps its membership.
        let already = group
            .members
            .iter()
            .any(|(_, m)| m.client_id == self.client_id);
        if !already {
            let member_key = group
                .members
                .insert(ConsumerGroupMember::new(0, self.client_id));
            group.members[member_key].id = member_key;
        }
        group.rebalance_members(&partition_ids);
        Bytes::new()
    }
}

impl StateHandler for LeaveConsumerGroupRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner, _timestamp: iggy_common::IggyTimestamp) -> Bytes {
        let Some(topic) = state.topic_mut(&self.stream_id, &self.topic_id) else {
            return Bytes::new();
        };
        let Some(group_id) = topic.resolve_group_id(&self.group_id) else {
            return Bytes::new();
        };
        let partition_ids: Vec<usize> = topic.partitions.iter().map(|p| p.id).collect();
        let Some(group) = topic.consumer_groups.get_mut(&group_id) else {
            return Bytes::new();
        };
        let member_key = group
            .members
            .iter()
            .find(|(_, m)| m.client_id == self.client_id)
            .map(|(key, _)| key);
        if let Some(key) = member_key {
            group.members.remove(key);
            group.rebalance_members(&partition_ids);
        }
        Bytes::new()
    }
}

/// Consumer group member snapshot representation for serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupMemberSnapshot {
    pub id: usize,
    pub client_id: u128,
    pub partitions: Vec<usize>,
}

/// Consumer group snapshot representation for serialization (nested under the
/// topic snapshot).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupSnapshot {
    pub id: u64,
    #[serde(default)]
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
