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

use crate::stm::StateHandler;
use crate::stm::snapshot::Snapshotable;
use crate::{collect_handlers, define_state, impl_fill_restore};
use bytes::Bytes;

use ahash::AHashMap;
use bytes::{BufMut, BytesMut};
use iggy_binary_protocol::codec::{WireDecode, WireEncode, read_u32_le};
use iggy_binary_protocol::requests::consumer_groups::{
    CreateConsumerGroupRequest, DeleteConsumerGroupRequest,
};
use iggy_binary_protocol::responses::consumer_groups::consumer_group_response::ConsumerGroupResponse;
use iggy_binary_protocol::responses::consumer_groups::get_consumer_group::{
    ConsumerGroupDetailsResponse, ConsumerGroupMemberResponse,
};
use iggy_binary_protocol::{WireIdentifier, WireName};
use serde::{Deserialize, Serialize};
use slab::Slab;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone)]
pub struct ConsumerGroupMember {
    pub id: usize,
    pub client_id: u128,
    pub partitions: Vec<usize>,
    pub partition_index: Arc<AtomicUsize>,
}

impl ConsumerGroupMember {
    #[must_use]
    pub fn new(id: usize, client_id: u128) -> Self {
        Self {
            id,
            client_id,
            partitions: Vec::new(),
            partition_index: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub id: usize,
    /// Numeric stream/topic id the group belongs to. Resolved from the
    /// (possibly named) identifiers on the first Join and used to report
    /// membership via `get_me` without re-resolving names.
    pub stream_id: u32,
    pub topic_id: u32,
    pub name: Arc<str>,
    pub partitions: Vec<usize>,
    pub members: Slab<ConsumerGroupMember>,
}

impl ConsumerGroup {
    #[must_use]
    pub const fn new(name: Arc<str>) -> Self {
        Self {
            id: 0,
            stream_id: 0,
            topic_id: 0,
            name,
            partitions: Vec::new(),
            members: Slab::new(),
        }
    }

    pub fn rebalance_members(&mut self) {
        let partition_count = self.partitions.len();
        let member_count = self.members.len();

        if member_count == 0 || partition_count == 0 {
            return;
        }

        let member_keys: Vec<usize> = self.members.iter().map(|(id, _)| id).collect();
        for &member_id in &member_keys {
            if let Some(member) = self.members.get_mut(member_id) {
                member.partitions.clear();
            }
        }

        for (i, &partition_id) in self.partitions.iter().enumerate() {
            let target = i % member_count;
            if let Some(&member_id) = member_keys.get(target)
                && let Some(member) = self.members.get_mut(member_id)
            {
                member.partitions.push(partition_id);
            }
        }
    }
}

define_state! {
    ConsumerGroups {
        name_index: AHashMap<Arc<str>, usize>,
        topic_index: AHashMap<(usize, usize), Vec<usize>>,
        topic_name_index: AHashMap<(Arc<str>, Arc<str>), Vec<usize>>,
        items: Slab<ConsumerGroup>,
    }
}

collect_handlers! {
    ConsumerGroups {
        CreateConsumerGroup,
        DeleteConsumerGroup,
        JoinConsumerGroup,
        LeaveConsumerGroup,
    }
}

/// Replicated `JoinConsumerGroup`, enriched by the primary.
///
/// Carries the joining client's VSR id and the topic's partition count (the
/// wire request carries neither, and the apply cannot read the Streams STM).
/// The apply seeds the group's partition list on first join, adds the member,
/// and round-robin rebalances.
#[derive(Debug, Clone)]
pub struct JoinConsumerGroupRequest {
    pub stream_id: WireIdentifier,
    pub topic_id: WireIdentifier,
    pub group_id: WireIdentifier,
    pub client_id: u128,
    pub partitions_count: u32,
    /// Numeric stream/topic ids resolved by the primary, stored on the group
    /// so membership reads (`get_me`) need not re-resolve named identifiers.
    pub resolved_stream_id: u32,
    pub resolved_topic_id: u32,
}

impl WireEncode for JoinConsumerGroupRequest {
    fn encoded_size(&self) -> usize {
        self.stream_id.encoded_size()
            + self.topic_id.encoded_size()
            + self.group_id.encoded_size()
            + 16
            + 4
            + 4
            + 4
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.stream_id.encode(buf);
        self.topic_id.encode(buf);
        self.group_id.encode(buf);
        buf.put_u128_le(self.client_id);
        buf.put_u32_le(self.partitions_count);
        buf.put_u32_le(self.resolved_stream_id);
        buf.put_u32_le(self.resolved_topic_id);
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
        let partitions_count = read_u32_le(buf, pos)?;
        pos += 4;
        let resolved_stream_id = read_u32_le(buf, pos)?;
        pos += 4;
        let resolved_topic_id = read_u32_le(buf, pos)?;
        pos += 4;
        Ok((
            Self {
                stream_id,
                topic_id,
                group_id,
                client_id,
                partitions_count,
                resolved_stream_id,
                resolved_topic_id,
            },
            pos,
        ))
    }
}

/// Replicated `LeaveConsumerGroup`, enriched by the primary with the leaving
/// client's VSR id. The apply removes the member and round-robin rebalances.
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

impl StateHandler for JoinConsumerGroupRequest {
    type State = ConsumerGroupsInner;
    fn apply(
        &self,
        state: &mut ConsumerGroupsInner,
        _timestamp: iggy_common::IggyTimestamp,
    ) -> Bytes {
        let Some(id) = state.resolve_consumer_group_id_by_identifiers(
            &self.stream_id,
            &self.topic_id,
            &self.group_id,
        ) else {
            return Bytes::new();
        };
        let partitions_count = self.partitions_count as usize;
        let Some(group) = state.items.get_mut(id) else {
            return Bytes::new();
        };
        // Seed the partition list on first join (the group is created with an
        // empty list; the topic's partition count only reaches here via the
        // enriched op).
        if group.partitions.is_empty() && partitions_count > 0 {
            group.partitions = (0..partitions_count).collect();
        }
        // Record the numeric stream/topic the group lives under (the create op
        // may have used named identifiers, leaving these unresolved).
        group.stream_id = self.resolved_stream_id;
        group.topic_id = self.resolved_topic_id;
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
        group.rebalance_members();
        Bytes::new()
    }
}

impl StateHandler for LeaveConsumerGroupRequest {
    type State = ConsumerGroupsInner;
    fn apply(
        &self,
        state: &mut ConsumerGroupsInner,
        _timestamp: iggy_common::IggyTimestamp,
    ) -> Bytes {
        let Some(id) = state.resolve_consumer_group_id_by_identifiers(
            &self.stream_id,
            &self.topic_id,
            &self.group_id,
        ) else {
            return Bytes::new();
        };
        let Some(group) = state.items.get_mut(id) else {
            return Bytes::new();
        };
        let member_key = group
            .members
            .iter()
            .find(|(_, m)| m.client_id == self.client_id)
            .map(|(key, _)| key);
        if let Some(key) = member_key {
            group.members.remove(key);
            group.rebalance_members();
        }
        Bytes::new()
    }
}

impl ConsumerGroupsInner {
    fn resolve_group_in_list(
        &self,
        groups_in_topic: &[usize],
        group_id: &WireIdentifier,
    ) -> Option<usize> {
        match group_id {
            WireIdentifier::Numeric(id) => {
                let g_id = *id as usize;
                groups_in_topic.contains(&g_id).then_some(g_id)
            }
            WireIdentifier::String(name) => groups_in_topic
                .iter()
                .find(|&&id| {
                    self.items
                        .get(id)
                        .is_some_and(|g| g.name.as_ref() == name.as_str())
                })
                .copied(),
        }
    }

    fn resolve_consumer_group_id_by_identifiers(
        &self,
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
        group_id: &WireIdentifier,
    ) -> Option<usize> {
        if let (WireIdentifier::Numeric(s), WireIdentifier::Numeric(t)) = (stream_id, topic_id) {
            let groups_in_topic = self.topic_index.get(&(*s as usize, *t as usize))?;
            return self.resolve_group_in_list(groups_in_topic, group_id);
        }

        if let (WireIdentifier::String(s), WireIdentifier::String(t)) = (stream_id, topic_id) {
            let key = (Arc::from(s.as_str()), Arc::from(t.as_str()));
            let groups_in_topic = self.topic_name_index.get(&key)?;
            return self.resolve_group_in_list(groups_in_topic, group_id);
        }

        None
    }
}

impl ConsumerGroups {
    #[must_use]
    pub fn read<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&ConsumerGroupsInner) -> R,
    {
        self.inner.read(f)
    }

    /// Build the `ConsumerGroupDetailsResponse` for a group, including each
    /// member's current round-robin partition assignment. `None` when the
    /// group does not exist.
    #[must_use]
    // The `WireName` fallback only fires on an invalid group name, which the
    // create path already rejected, so the `expect` is unreachable.
    #[allow(clippy::cast_possible_truncation, clippy::missing_panics_doc)]
    pub fn details(
        &self,
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
        group_id: &WireIdentifier,
    ) -> Option<ConsumerGroupDetailsResponse> {
        self.inner.read(|inner| {
            let id =
                inner.resolve_consumer_group_id_by_identifiers(stream_id, topic_id, group_id)?;
            let group = inner.items.get(id)?;
            let members = group
                .members
                .iter()
                .map(|(_, member)| ConsumerGroupMemberResponse {
                    id: member.id as u32,
                    partitions_count: member.partitions.len() as u32,
                    partitions: member.partitions.iter().map(|&p| p as u32).collect(),
                })
                .collect();
            Some(ConsumerGroupDetailsResponse {
                group: ConsumerGroupResponse {
                    id: group.id as u32,
                    partitions_count: group.partitions.len() as u32,
                    members_count: group.members.len() as u32,
                    name: WireName::new(group.name.as_ref())
                        .unwrap_or_else(|_| WireName::new("unknown").expect("valid")),
                },
                members,
            })
        })
    }

    /// `(stream_id, topic_id, group_id)` of every group the client belongs to,
    /// for `get_me` / `get_clients` membership reporting.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn memberships(&self, client_id: u128) -> Vec<(u32, u32, u32)> {
        self.inner.read(|inner| {
            inner
                .items
                .iter()
                .filter(|(_, group)| group.members.iter().any(|(_, m)| m.client_id == client_id))
                .map(|(_, group)| (group.stream_id, group.topic_id, group.id as u32))
                .collect()
        })
    }

    /// Resolve a consumer-group poll to the next partition the polling client's
    /// member should read, advancing the member's round-robin cursor. Returns
    /// `(group_id, partition_id, member_id)` -- `group_id` is the per-partition
    /// consumer-group offset key, `member_id` the slab key. `None` if the group
    /// or member is unknown, or the member holds no partitions.
    ///
    /// The cursor is a non-replicated, per-node read-path counter (it mirrors
    /// the legacy server), so it advances through the shared atomic under a
    /// read guard rather than a consensus apply.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn resolve_member_next_partition(
        &self,
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
        group_id: &WireIdentifier,
        client_id: u128,
    ) -> Option<(u32, u32, usize)> {
        self.inner.read(|inner| {
            let id =
                inner.resolve_consumer_group_id_by_identifiers(stream_id, topic_id, group_id)?;
            let group = inner.items.get(id)?;
            let (member_id, member) =
                group.members.iter().find(|(_, m)| m.client_id == client_id)?;
            let count = member.partitions.len();
            if count == 0 {
                return None;
            }
            let current = member
                .partition_index
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| Some((c + 1) % count))
                .unwrap_or(0);
            let partition_id = member.partitions[current % count];
            Some((group.id as u32, partition_id as u32, member_id))
        })
    }
}

impl StateHandler for CreateConsumerGroupRequest {
    type State = ConsumerGroupsInner;
    #[allow(clippy::cast_possible_truncation)]
    fn apply(
        &self,
        state: &mut ConsumerGroupsInner,
        _timestamp: iggy_common::IggyTimestamp,
    ) -> Bytes {
        let name: Arc<str> = Arc::from(self.name.as_str());
        if state.name_index.contains_key(&name) {
            return Bytes::new();
        }

        let group = ConsumerGroup::new(name.clone());
        let id = state.items.insert(group);
        state.items[id].id = id;

        state.name_index.insert(name, id);

        if let (WireIdentifier::Numeric(s), WireIdentifier::Numeric(t)) =
            (&self.stream_id, &self.topic_id)
        {
            state
                .topic_index
                .entry((*s as usize, *t as usize))
                .or_default()
                .push(id);
        }

        if let (WireIdentifier::String(s), WireIdentifier::String(t)) =
            (&self.stream_id, &self.topic_id)
        {
            let key = (Arc::from(s.as_str()), Arc::from(t.as_str()));
            state.topic_name_index.entry(key).or_default().push(id);
        }

        // Reply body: the SDK `create_consumer_group` decodes a
        // `ConsumerGroupDetailsResponse`. A freshly created group has no
        // assigned partitions and no joined members yet.
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
    type State = ConsumerGroupsInner;
    fn apply(
        &self,
        state: &mut ConsumerGroupsInner,
        _timestamp: iggy_common::IggyTimestamp,
    ) -> Bytes {
        let Some(id) = state.resolve_consumer_group_id_by_identifiers(
            &self.stream_id,
            &self.topic_id,
            &self.group_id,
        ) else {
            return Bytes::new();
        };

        let group = state.items.remove(id);
        state.name_index.remove(&group.name);

        if let (WireIdentifier::Numeric(s), WireIdentifier::Numeric(t)) =
            (&self.stream_id, &self.topic_id)
            && let Some(vec) = state.topic_index.get_mut(&(*s as usize, *t as usize))
        {
            vec.retain(|&x| x != id);
        }

        if let (WireIdentifier::String(s), WireIdentifier::String(t)) =
            (&self.stream_id, &self.topic_id)
        {
            let key = (Arc::from(s.as_str()), Arc::from(t.as_str()));
            if let Some(vec) = state.topic_name_index.get_mut(&key) {
                vec.retain(|&x| x != id);
            }
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
    pub partition_index: usize,
}

/// Consumer group snapshot representation for serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupSnapshot {
    pub id: usize,
    #[serde(default)]
    pub stream_id: u32,
    #[serde(default)]
    pub topic_id: u32,
    pub name: String,
    pub partitions: Vec<usize>,
    pub members: Vec<(usize, ConsumerGroupMemberSnapshot)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupsSnapshot {
    pub items: Vec<(usize, ConsumerGroupSnapshot)>,
    pub topic_index: Vec<((usize, usize), Vec<usize>)>,
    pub topic_name_index: Vec<((String, String), Vec<usize>)>,
}

impl Snapshotable for ConsumerGroups {
    type Snapshot = ConsumerGroupsSnapshot;

    fn to_snapshot(&self) -> Self::Snapshot {
        self.inner.read(|inner| {
            let items: Vec<(usize, ConsumerGroupSnapshot)> = inner
                .items
                .iter()
                .map(|(group_id, group)| {
                    let members: Vec<(usize, ConsumerGroupMemberSnapshot)> = group
                        .members
                        .iter()
                        .map(|(member_id, member)| {
                            (
                                member_id,
                                ConsumerGroupMemberSnapshot {
                                    id: member.id,
                                    client_id: member.client_id,
                                    partitions: member.partitions.clone(),
                                    partition_index: member.partition_index.load(Ordering::Relaxed),
                                },
                            )
                        })
                        .collect();

                    (
                        group_id,
                        ConsumerGroupSnapshot {
                            id: group.id,
                            stream_id: group.stream_id,
                            topic_id: group.topic_id,
                            name: group.name.to_string(),
                            partitions: group.partitions.clone(),
                            members,
                        },
                    )
                })
                .collect();

            let topic_index: Vec<((usize, usize), Vec<usize>)> = inner
                .topic_index
                .iter()
                .map(|(&k, v)| (k, v.clone()))
                .collect();

            let topic_name_index: Vec<((String, String), Vec<usize>)> = inner
                .topic_name_index
                .iter()
                .map(|((s, t), v)| ((s.to_string(), t.to_string()), v.clone()))
                .collect();

            ConsumerGroupsSnapshot {
                items,
                topic_index,
                topic_name_index,
            }
        })
    }

    fn from_snapshot(
        snapshot: Self::Snapshot,
    ) -> Result<Self, crate::stm::snapshot::SnapshotError> {
        let mut name_index: AHashMap<Arc<str>, usize> = AHashMap::new();
        let mut group_entries: Vec<(usize, ConsumerGroup)> = Vec::new();

        for (slab_key, group_snap) in snapshot.items {
            let members: Slab<ConsumerGroupMember> = group_snap
                .members
                .into_iter()
                .map(|(member_key, member_snap)| {
                    let member = ConsumerGroupMember {
                        id: member_snap.id,
                        client_id: member_snap.client_id,
                        partitions: member_snap.partitions,
                        partition_index: Arc::new(AtomicUsize::new(member_snap.partition_index)),
                    };
                    (member_key, member)
                })
                .collect();

            let group_name: Arc<str> = Arc::from(group_snap.name.as_str());
            let group = ConsumerGroup {
                id: group_snap.id,
                stream_id: group_snap.stream_id,
                topic_id: group_snap.topic_id,
                name: group_name.clone(),
                partitions: group_snap.partitions,
                members,
            };

            name_index.insert(group_name, slab_key);
            group_entries.push((slab_key, group));
        }

        let items = group_entries.into_iter().collect();

        let topic_index: AHashMap<(usize, usize), Vec<usize>> =
            snapshot.topic_index.into_iter().collect();

        let topic_name_index: AHashMap<(Arc<str>, Arc<str>), Vec<usize>> = snapshot
            .topic_name_index
            .into_iter()
            .map(|((s, t), v)| ((Arc::from(s.as_str()), Arc::from(t.as_str())), v))
            .collect();

        let inner = ConsumerGroupsInner {
            name_index,
            topic_index,
            topic_name_index,
            items,
            last_result: None,
        };
        Ok(inner.into())
    }
}

impl_fill_restore!(ConsumerGroups, consumer_groups);
