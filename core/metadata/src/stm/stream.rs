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
use crate::stm::consumer_group::{
    CompleteConsumerGroupRevocationRequest, ConsumerGroup, ConsumerGroupSnapshot,
    JoinConsumerGroupRequest, LeaveConsumerGroupRequest, RemoveConsumerGroupMemberRequest,
};
use crate::stm::result::{
    ApplyReply, CreatePartitionsResult, CreateStreamResult, CreateTopicResult,
    DeletePartitionsResult, DeleteStreamResult, DeleteTopicResult, PurgeStreamResult,
    PurgeTopicResult, UpdateStreamResult, UpdateTopicResult,
};
use crate::stm::snapshot::Snapshotable;
use crate::{collect_handlers, define_state, impl_fill_restore};
use ahash::AHashMap;
use bytes::{BufMut, Bytes, BytesMut};
use iggy_binary_protocol::codec::{WireDecode, WireEncode};
use iggy_binary_protocol::requests::consumer_groups::{
    CreateConsumerGroupRequest, DeleteConsumerGroupRequest,
};
use iggy_binary_protocol::requests::partitions::{
    CreatePartitionsWithAssignmentsRequest, DeletePartitionsRequest,
};
use iggy_binary_protocol::requests::streams::{
    CreateStreamRequest, DeleteStreamRequest, PurgeStreamRequest, UpdateStreamRequest,
};
use iggy_binary_protocol::requests::topics::{
    CreateTopicRequest, CreateTopicWithAssignmentsRequest, DeleteTopicRequest, PurgeTopicRequest,
    UpdateTopicRequest,
};
use iggy_binary_protocol::responses::consumer_groups::consumer_group_response::ConsumerGroupResponse;
use iggy_binary_protocol::responses::consumer_groups::get_consumer_group::{
    ConsumerGroupDetailsResponse, ConsumerGroupMemberResponse,
};
use iggy_binary_protocol::responses::streams::StreamResponse;
use iggy_binary_protocol::responses::streams::get_stream::{GetStreamResponse, TopicHeader};
use iggy_binary_protocol::responses::topics::get_topic::PartitionResponse;
use iggy_binary_protocol::{WireIdentifier, WireName};
use iggy_common::{
    CompressionAlgorithm, IggyExpiry, IggyTimestamp, MaxTopicSize, StreamStats, TopicStats,
};
use serde::{Deserialize, Serialize};
use server_common::sharding::IggyNamespace;
use slab::Slab;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Partition snapshot representation for serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSnapshot {
    pub id: usize,
    pub consensus_group_id: u64,
    pub created_at: IggyTimestamp,
    /// `#[serde(default)]` so snapshots predating this field restore
    /// with revision 0 instead of failing to decode.
    #[serde(default)]
    pub created_revision: u64,
    /// `#[serde(default)]` so pre-watermark snapshots restore at 0.
    #[serde(default)]
    pub deleted_up_to_offset: u64,
    /// `#[serde(default)]` so pre-purge snapshots restore at 0.
    #[serde(default)]
    pub purge_generation: u64,
}

#[derive(Debug, Clone)]
pub struct Partition {
    pub id: usize,
    pub consensus_group_id: u64,
    pub created_at: IggyTimestamp,
    /// `StreamsInner::revision` at creation. Reconciler compares it to the
    /// epoch it stored at materialisation; a mismatch means a delete+recreate
    /// reused the slab key, so the local partition is stale and must be torn
    /// down before rebuild.
    pub created_revision: u64,
    /// Replicated delete watermark: the reconciler on every replica removes
    /// sealed segments with `end_offset` below this. Advanced monotonically by
    /// `TruncatePartition` (the resolved form of a client `DeleteSegments`).
    /// `0` means nothing has been trimmed.
    pub deleted_up_to_offset: u64,
    /// Replicated purge counter: `PurgeTopic` increments it for every partition
    /// in the topic. The reconciler on every replica resets a partition to a
    /// single empty segment at offset 0 (clearing consumer offsets) when this
    /// exceeds the generation it last applied locally. Monotonic so a redundant
    /// reconcile pass does not re-wipe a partition already at this generation.
    pub purge_generation: u64,
}

impl Partition {
    #[must_use]
    pub const fn new(
        id: usize,
        consensus_group_id: u64,
        created_at: IggyTimestamp,
        created_revision: u64,
    ) -> Self {
        Self {
            id,
            consensus_group_id,
            created_at,
            created_revision,
            deleted_up_to_offset: 0,
            purge_generation: 0,
        }
    }
}

/// Stats snapshot representation for serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsSnapshot {
    pub size_bytes: u64,
    pub messages_count: u64,
    pub segments_count: u32,
}

/// Topic snapshot representation for serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicSnapshot {
    pub id: usize,
    pub name: String,
    pub created_at: IggyTimestamp,
    pub replication_factor: u8,
    pub message_expiry: IggyExpiry,
    pub compression_algorithm: CompressionAlgorithm,
    pub max_topic_size: MaxTopicSize,
    pub stats: StatsSnapshot,
    pub partitions: Vec<PartitionSnapshot>,
    // `round_robin_counter` is intentionally NOT snapshotted. It is a local
    // load-balancing hint advanced on the `Balanced`-send read path (outside
    // the replicated apply), so each replica's value drifts independently;
    // persisting it would make the snapshot diverge per replica. Restored to 0.
    #[serde(default)]
    pub consumer_groups: Vec<(u64, ConsumerGroupSnapshot)>,
    #[serde(default)]
    pub next_consumer_group_id: u64,
}

#[derive(Debug, Clone)]
pub struct Topic {
    pub id: usize,
    pub name: Arc<str>,
    pub created_at: IggyTimestamp,
    pub replication_factor: u8,
    pub message_expiry: IggyExpiry,
    pub compression_algorithm: CompressionAlgorithm,
    pub max_topic_size: MaxTopicSize,

    pub stats: Arc<TopicStats>,
    pub partitions: Vec<Partition>,
    pub round_robin_counter: Arc<AtomicUsize>,

    /// Consumer groups belonging to this topic, keyed by monotonic group id.
    /// Co-located so a stream/topic delete drops them automatically.
    pub consumer_groups: AHashMap<u64, ConsumerGroup>,
    /// Group name -> id, for per-topic name uniqueness + name resolution.
    pub consumer_group_index: AHashMap<Arc<str>, u64>,
    /// Monotonic group-id counter; never reused so the partition-plane offset
    /// key (keyed by group id) can't be inherited by a recreated group.
    ///
    /// Ceiling: the partition-plane offset key is `u32`, so a group id must stay
    /// within `u32::MAX` (the wire rewrite in `server-ng` truncates to u32 and
    /// `expect`s this). ~4 billion group creates on a single topic is
    /// unreachable in practice, but the cap is real -- past it the wire id would
    /// wrap and could collide with a live group's offset key.
    pub next_consumer_group_id: u64,
}

impl Default for Topic {
    fn default() -> Self {
        Self {
            id: 0,
            name: Arc::from(""),
            created_at: IggyTimestamp::default(),
            replication_factor: 1,
            message_expiry: IggyExpiry::default(),
            compression_algorithm: CompressionAlgorithm::default(),
            max_topic_size: MaxTopicSize::default(),
            stats: Arc::new(TopicStats::default()),
            partitions: Vec::new(),
            round_robin_counter: Arc::new(AtomicUsize::new(0)),
            consumer_groups: AHashMap::default(),
            consumer_group_index: AHashMap::default(),
            next_consumer_group_id: 1,
        }
    }
}

impl Topic {
    pub fn new(
        name: Arc<str>,
        created_at: IggyTimestamp,
        replication_factor: u8,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        stream_stats: Arc<StreamStats>,
    ) -> Self {
        Self {
            id: 0,
            name,
            created_at,
            replication_factor,
            message_expiry,
            compression_algorithm,
            max_topic_size,
            stats: Arc::new(TopicStats::new(stream_stats)),
            partitions: Vec::new(),
            round_robin_counter: Arc::new(AtomicUsize::new(0)),
            consumer_groups: AHashMap::default(),
            consumer_group_index: AHashMap::default(),
            next_consumer_group_id: 1,
        }
    }

    /// Re-run round-robin assignment for every consumer group under this topic
    /// against the current partition set. Called after a partition-count change
    /// (`CreatePartitions`/`DeletePartitions`) so groups pick up added
    /// partitions and drop removed ones; each `rebalance_members` bumps the
    /// group generation so stale clients re-sync.
    pub fn rebalance_consumer_groups(&mut self) {
        if self.consumer_groups.is_empty() {
            return;
        }
        let partition_ids: Vec<usize> = self.partitions.iter().map(|p| p.id).collect();
        for group in self.consumer_groups.values_mut() {
            group.rebalance_members(&partition_ids);
        }
    }

    /// Resolve a consumer-group identifier to its monotonic id within this
    /// topic. Numeric resolves directly; string via the name index.
    #[must_use]
    pub fn resolve_group_id(&self, group_id: &WireIdentifier) -> Option<u64> {
        match group_id {
            WireIdentifier::Numeric(id) => {
                let id = u64::from(*id);
                self.consumer_groups.contains_key(&id).then_some(id)
            }
            WireIdentifier::String(name) => self.consumer_group_index.get(name.as_str()).copied(),
        }
    }
}

/// Stream snapshot representation for serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamSnapshot {
    pub id: usize,
    pub name: String,
    pub created_at: IggyTimestamp,
    pub stats: StatsSnapshot,
    pub topics: Vec<(usize, TopicSnapshot)>,
}

#[derive(Debug)]
pub struct Stream {
    pub id: usize,
    pub name: Arc<str>,
    pub created_at: IggyTimestamp,

    pub stats: Arc<StreamStats>,
    pub topics: Slab<Topic>,
    pub topic_index: AHashMap<Arc<str>, usize>,
}

impl Default for Stream {
    fn default() -> Self {
        Self {
            id: 0,
            name: Arc::from(""),
            created_at: IggyTimestamp::default(),
            stats: Arc::new(StreamStats::default()),
            topics: Slab::new(),
            topic_index: AHashMap::default(),
        }
    }
}

impl Clone for Stream {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            name: self.name.clone(),
            created_at: self.created_at,
            stats: self.stats.clone(),
            topics: self.topics.clone(),
            topic_index: self.topic_index.clone(),
        }
    }
}

impl Stream {
    #[must_use]
    pub fn new(name: Arc<str>, created_at: IggyTimestamp) -> Self {
        Self {
            id: 0,
            name,
            created_at,
            stats: Arc::new(StreamStats::default()),
            topics: Slab::new(),
            topic_index: AHashMap::default(),
        }
    }

    #[must_use]
    pub fn with_stats(name: Arc<str>, created_at: IggyTimestamp, stats: Arc<StreamStats>) -> Self {
        Self {
            id: 0,
            name,
            created_at,
            stats,
            topics: Slab::new(),
            topic_index: AHashMap::default(),
        }
    }
}

define_state! {
    Streams {
        index: AHashMap<Arc<str>, usize>,
        items: Slab<Stream>,
        // Monotonic counter bumped on every partition-shaping commit
        // (create/delete topic, create/delete partitions, delete stream).
        // Reconciler uses it for a fast-skip when nothing changed and stamps
        // it onto each new Partition::created_revision. Deterministic across
        // replicas: same ops, same order.
        revision: u64,
        // Total pending cooperative revocations across all groups, recomputed
        // once per commit by `post_apply`. The consensus tick reads it O(1)
        // every 10ms instead of walking every stream/topic/group/member to
        // decide whether to wake the reconciler. Deterministic (same ops, same
        // recompute on every replica).
        pending_revocations_count: u64,
    }
}

/// Server-originated request that advances a partition's delete watermark.
///
/// `up_to_offset` is resolved on the owning shard from a client
/// `DeleteSegments` count, then replicated through metadata so every replica
/// applies the same monotonic watermark (see [`Partition::deleted_up_to_offset`]).
#[derive(Debug, Clone)]
pub struct TruncatePartitionRequest {
    pub stream_id: WireIdentifier,
    pub topic_id: WireIdentifier,
    pub partition_id: u32,
    pub up_to_offset: u64,
}

impl WireEncode for TruncatePartitionRequest {
    fn encoded_size(&self) -> usize {
        self.stream_id.encoded_size() + self.topic_id.encoded_size() + 4 + 8
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.stream_id.encode(buf);
        self.topic_id.encode(buf);
        buf.put_u32_le(self.partition_id);
        buf.put_u64_le(self.up_to_offset);
    }
}

impl WireDecode for TruncatePartitionRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), iggy_binary_protocol::WireError> {
        let (stream_id, mut pos) = WireIdentifier::decode(buf)?;
        let (topic_id, n) = WireIdentifier::decode(&buf[pos..])?;
        pos += n;
        let partition_slice = buf.get(pos..pos + 4).ok_or_else(|| {
            iggy_binary_protocol::WireError::UnexpectedEof {
                offset: pos,
                need: 4,
                have: buf.len().saturating_sub(pos),
            }
        })?;
        let partition_id = u32::from_le_bytes(partition_slice.try_into().expect("4 bytes"));
        pos += 4;
        let offset_slice = buf.get(pos..pos + 8).ok_or_else(|| {
            iggy_binary_protocol::WireError::UnexpectedEof {
                offset: pos,
                need: 8,
                have: buf.len().saturating_sub(pos),
            }
        })?;
        let up_to_offset = u64::from_le_bytes(offset_slice.try_into().expect("8 bytes"));
        pos += 8;
        Ok((
            Self {
                stream_id,
                topic_id,
                partition_id,
                up_to_offset,
            },
            pos,
        ))
    }
}

impl StateHandler for TruncatePartitionRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner, _timestamp: IggyTimestamp) -> ApplyReply {
        // Internal op (no client result enum): a missing parent / partition is
        // an idempotent no-op, like the other reconciler-fed internal ops.
        {
            let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
                return ApplyReply::ok(Bytes::new());
            };
            let Some(topic_id) = state.resolve_topic_id(stream_id, &self.topic_id) else {
                return ApplyReply::ok(Bytes::new());
            };
            let Some(stream) = state.items.get_mut(stream_id) else {
                return ApplyReply::ok(Bytes::new());
            };
            let Some(topic) = stream.topics.get_mut(topic_id) else {
                return ApplyReply::ok(Bytes::new());
            };
            let Some(partition) = topic
                .partitions
                .iter_mut()
                .find(|partition| partition.id == self.partition_id as usize)
            else {
                return ApplyReply::ok(Bytes::new());
            };
            // Monotonic: a stale or duplicate replay never rewinds the watermark.
            if self.up_to_offset > partition.deleted_up_to_offset {
                partition.deleted_up_to_offset = self.up_to_offset;
            }
        }
        // Bump on every applied truncate (partition resolved), even when the
        // watermark did not advance. A client `DeleteSegments` re-resolving to
        // an already-committed offset must still re-drive the reconciler so
        // segments the consumer barrier has since released are removed -- legacy
        // `delete_segments` re-evaluates the barrier on every call. The
        // watermark itself stays monotonic (set above); only the
        // reconcile-trigger fires unconditionally.
        state.revision = state.revision.wrapping_add(1);
        ApplyReply::ok(Bytes::new())
    }
}

collect_handlers! {
    Streams {
        CreateStream,
        UpdateStream,
        DeleteStream,
        PurgeStream,
        CreateTopicWithAssignments,
        UpdateTopic,
        DeleteTopic,
        PurgeTopic,
        CreatePartitionsWithAssignments,
        DeletePartitions,
        // Consumer groups are co-located under the topic, so the Streams STM
        // applies these too. `Join`/`Leave` use the enriched request types from
        // `crate::stm::consumer_group` (imported above) which carry the VSR
        // client id.
        CreateConsumerGroup,
        DeleteConsumerGroup,
        JoinConsumerGroup,
        LeaveConsumerGroup,
        RemoveConsumerGroupMember,
        CompleteConsumerGroupRevocation,
        TruncatePartition,
    }
}

impl StreamsInner {
    /// Recompute `pending_revocations_count` so the consensus tick's
    /// `has_pending_revocations` read (and the reconciler's fast-skip) is O(1)
    /// instead of walking every group each 10ms. Called only by the apply
    /// handlers that can change pending revocations (join, leave, remove,
    /// complete, and group-dropping deletes), so non-consumer-group commits pay
    /// nothing. Recompute (not a delta) keeps the count drift-proof.
    pub(crate) fn recompute_pending_revocations_count(&mut self) {
        let mut count: u64 = 0;
        for (_, stream) in &self.items {
            for (_, topic) in &stream.topics {
                for group in topic.consumer_groups.values() {
                    for (_, member) in &group.members {
                        count += member.pending_revocations.len() as u64;
                    }
                }
            }
        }
        self.pending_revocations_count = count;
    }

    fn resolve_stream_id(&self, identifier: &WireIdentifier) -> Option<usize> {
        match identifier {
            WireIdentifier::Numeric(id) => {
                let id = *id as usize;
                if self.items.contains(id) {
                    Some(id)
                } else {
                    None
                }
            }
            WireIdentifier::String(name) => self.index.get(name.as_str()).copied(),
        }
    }

    fn resolve_topic_id(&self, stream_id: usize, identifier: &WireIdentifier) -> Option<usize> {
        let stream = self.items.get(stream_id)?;
        match identifier {
            WireIdentifier::Numeric(id) => {
                let id = *id as usize;
                if stream.topics.contains(id) {
                    Some(id)
                } else {
                    None
                }
            }
            WireIdentifier::String(name) => stream.topic_index.get(name.as_str()).copied(),
        }
    }

    /// Mutable topic resolved from (stream, topic) identifiers -- the
    /// consumer-group `StateHandler`s in [`crate::stm::consumer_group`] operate
    /// through this.
    pub(crate) fn topic_mut(
        &mut self,
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
    ) -> Option<&mut Topic> {
        let stream_id = self.resolve_stream_id(stream_id)?;
        let topic_id = self.resolve_topic_id(stream_id, topic_id)?;
        self.items.get_mut(stream_id)?.topics.get_mut(topic_id)
    }
}

impl Streams {
    #[must_use]
    pub fn read<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&StreamsInner) -> R,
    {
        self.inner.read(f)
    }

    /// Committed delete watermark for a partition (the offset below which
    /// sealed segments are removed), or `0` if the partition is unknown or
    /// never trimmed. The per-shard reconciler reads this to enforce a
    /// committed `TruncatePartition` against its local segments.
    #[must_use]
    pub fn partition_delete_watermark(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
    ) -> u64 {
        self.inner.read(|inner| {
            inner
                .items
                .get(stream_id)
                .and_then(|stream| stream.topics.get(topic_id))
                .and_then(|topic| topic.partitions.iter().find(|p| p.id == partition_id))
                .map_or(0, |partition| partition.deleted_up_to_offset)
        })
    }

    /// Committed purge generation for a partition. The reconciler resets the
    /// local partition (single empty segment at offset 0, cleared consumer
    /// offsets) whenever this exceeds the generation it last applied. `0` means
    /// never purged. Mirrors [`Self::partition_delete_watermark`].
    #[must_use]
    pub fn partition_purge_generation(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
    ) -> u64 {
        self.inner.read(|inner| {
            inner
                .items
                .get(stream_id)
                .and_then(|stream| stream.topics.get(topic_id))
                .and_then(|topic| topic.partitions.iter().find(|p| p.id == partition_id))
                .map_or(0, |partition| partition.purge_generation)
        })
    }

    /// Retention policy for a topic: `(message_expiry, max_topic_size,
    /// partition_count)`, or `None` if the stream or topic is unknown. The
    /// per-shard segment cleaner reads this off-pump to decide local segment
    /// deletion; `partition_count` lets it derive a per-partition size budget.
    #[must_use]
    pub fn topic_retention_config(
        &self,
        stream_id: usize,
        topic_id: usize,
    ) -> Option<(IggyExpiry, MaxTopicSize, usize)> {
        self.inner.read(|inner| {
            let topic = inner.items.get(stream_id)?.topics.get(topic_id)?;
            Some((
                topic.message_expiry,
                topic.max_topic_size,
                topic.partitions.len(),
            ))
        })
    }

    /// Build the `ConsumerGroupDetailsResponse` for a group (members + their
    /// round-robin partition assignment). `partitions_count` is the topic's
    /// total partition count. `None` if the stream/topic/group is unknown.
    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::missing_panics_doc)]
    pub fn consumer_group_details(
        &self,
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
        group_id: &WireIdentifier,
    ) -> Option<ConsumerGroupDetailsResponse> {
        self.inner.read(|inner| {
            let stream_id = inner.resolve_stream_id(stream_id)?;
            let topic_id = inner.resolve_topic_id(stream_id, topic_id)?;
            let topic = inner.items.get(stream_id)?.topics.get(topic_id)?;
            let group = topic
                .consumer_groups
                .get(&topic.resolve_group_id(group_id)?)?;
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
                    partitions_count: topic.partitions.len() as u32,
                    members_count: group.members.len() as u32,
                    // The name was validated at create, so the fallback is
                    // unreachable.
                    name: WireName::new(group.name.as_ref())
                        .unwrap_or_else(|_| WireName::new("unknown").expect("valid")),
                },
                members,
            })
        })
    }

    /// All consumer groups of a topic (for `GetConsumerGroups`). `None` if the
    /// stream/topic is unknown.
    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::missing_panics_doc)]
    pub fn consumer_group_list(
        &self,
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
    ) -> Option<Vec<ConsumerGroupResponse>> {
        self.inner.read(|inner| {
            let stream_id = inner.resolve_stream_id(stream_id)?;
            let topic_id = inner.resolve_topic_id(stream_id, topic_id)?;
            let topic = inner.items.get(stream_id)?.topics.get(topic_id)?;
            let partitions_count = topic.partitions.len() as u32;
            Some(
                topic
                    .consumer_groups
                    .values()
                    .map(|group| ConsumerGroupResponse {
                        id: group.id as u32,
                        partitions_count,
                        members_count: group.members.len() as u32,
                        name: WireName::new(group.name.as_ref())
                            .unwrap_or_else(|_| WireName::new("unknown").expect("valid")),
                    })
                    .collect(),
            )
        })
    }

    /// The requesting member's `(generation, partitions)` -- served by the
    /// `SyncConsumerGroup` endpoint for client-side partition selection.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn consumer_group_member_assignment(
        &self,
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
        group_id: &WireIdentifier,
        client_id: u128,
    ) -> Option<(u64, Vec<u32>)> {
        self.inner.read(|inner| {
            let stream_id = inner.resolve_stream_id(stream_id)?;
            let topic_id = inner.resolve_topic_id(stream_id, topic_id)?;
            let topic = inner.items.get(stream_id)?.topics.get(topic_id)?;
            let group = topic
                .consumer_groups
                .get(&topic.resolve_group_id(group_id)?)?;
            let (_, member) = group
                .members
                .iter()
                .find(|(_, m)| m.client_id == client_id)?;
            // The client polls only its non-revoked partitions; a partition
            // pending handoff stays owned (commit fence) but is no longer polled
            // so its consumer can drain + commit it, completing the revocation.
            let partitions = member
                .pollable_partitions()
                .iter()
                .map(|&p| p as u32)
                .collect();
            Some((group.generation, partitions))
        })
    }

    /// Whether any consumer group has a pending cooperative revocation. O(1):
    /// reads the `pending_revocations_count` that `post_apply` maintains per
    /// commit. The consensus tick polls this every 10ms to wake the reconciler
    /// promptly when a source drains a revoked partition, so it must not walk.
    #[must_use]
    pub fn has_pending_revocations(&self) -> bool {
        self.inner.read(|inner| inner.pending_revocations_count > 0)
    }

    /// The topic's current partition ids, for the join-time in-flight gather
    /// (the home shard reads each partition's poll/commit state to classify the
    /// cooperative handoff). `None` if the stream/topic does not resolve.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn topic_partition_ids(
        &self,
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
    ) -> Option<Vec<u32>> {
        self.inner.read(|inner| {
            let stream_id = inner.resolve_stream_id(stream_id)?;
            let topic_id = inner.resolve_topic_id(stream_id, topic_id)?;
            let topic = inner.items.get(stream_id)?.topics.get(topic_id)?;
            Some(topic.partitions.iter().map(|p| p.id as u32).collect())
        })
    }

    /// Partitions currently owned by some live member of the group (union over
    /// members, pending-revoked included since the source still owns them until
    /// completion). The join-time in-flight gather uses this to tell a genuine
    /// in-flight hold (a live member polled past its commit) from a stale
    /// `last_polled` left by a since-removed member: only an owned partition can
    /// be in flight, so an unowned one with uncommitted data is the dead-member
    /// residue of a reconnect and must be reassigned, not protected.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn consumer_group_assigned_partitions(
        &self,
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
        group_id: &WireIdentifier,
    ) -> Option<std::collections::HashSet<u32>> {
        self.inner.read(|inner| {
            let stream_id = inner.resolve_stream_id(stream_id)?;
            let topic_id = inner.resolve_topic_id(stream_id, topic_id)?;
            let topic = inner.items.get(stream_id)?.topics.get(topic_id)?;
            let group = topic
                .consumer_groups
                .get(&topic.resolve_group_id(group_id)?)?;
            Some(
                group
                    .members
                    .iter()
                    .flat_map(|(_, member)| member.partitions.iter().map(|&p| p as u32))
                    .collect(),
            )
        })
    }

    /// Every pending cooperative revocation across all groups, as
    /// `(stream_id, topic_id, group_id, source_client_id, partition_id,
    /// created_at_micros)`. The reconciler reads this each pass to decide which
    /// revocations to complete (source drained, or timed out).
    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::type_complexity)]
    pub fn consumer_group_pending_revocations(&self) -> Vec<(u32, u32, u64, u128, u32, u64)> {
        self.inner.read(|inner| {
            let mut out = Vec::new();
            for (stream_id, stream) in &inner.items {
                for (topic_id, topic) in &stream.topics {
                    for group in topic.consumer_groups.values() {
                        for (source_client_id, partition_id, created_at) in
                            group.pending_revocations()
                        {
                            out.push((
                                stream_id as u32,
                                topic_id as u32,
                                group.id,
                                source_client_id,
                                partition_id as u32,
                                created_at,
                            ));
                        }
                    }
                }
            }
            out
        })
    }

    /// The group's id (the consumer-group offset key) if `client_id` currently
    /// owns `partition_id` in it -- the poll/commit fence. `None` for a stale
    /// client whose partition was reassigned, prompting a re-sync.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn consumer_group_fence(
        &self,
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
        group_id: &WireIdentifier,
        client_id: u128,
        partition_id: u32,
        require_pollable: bool,
    ) -> Option<u64> {
        self.inner.read(|inner| {
            let stream_id = inner.resolve_stream_id(stream_id)?;
            let topic_id = inner.resolve_topic_id(stream_id, topic_id)?;
            let topic = inner.items.get(stream_id)?.topics.get(topic_id)?;
            let group = topic
                .consumer_groups
                .get(&topic.resolve_group_id(group_id)?)?;
            let (_, member) = group
                .members
                .iter()
                .find(|(_, m)| m.client_id == client_id)?;
            // Poll fence (`require_pollable`) rejects a pending-revoked partition
            // so the source stops polling it (re-sync drops it from its set);
            // commit fence keeps the full owned set so the source can still
            // commit it and drain the handoff.
            let owns = if require_pollable {
                member.is_pollable(partition_id as usize)
            } else {
                member.partitions.iter().any(|&p| p as u32 == partition_id)
            };
            owns.then_some(group.id)
        })
    }

    /// The group's monotonic id (the consumer-group offset key) regardless of
    /// membership. `None` if the stream/topic/group no longer resolves, so a
    /// consumer-offset read of a deleted group reports "no offset" and a write
    /// rewrite can substitute the numeric id the partition plane keys under.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn resolve_consumer_group_id(
        &self,
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
        group_id: &WireIdentifier,
    ) -> Option<u64> {
        self.inner.read(|inner| {
            let stream_id = inner.resolve_stream_id(stream_id)?;
            let topic_id = inner.resolve_topic_id(stream_id, topic_id)?;
            let topic = inner.items.get(stream_id)?.topics.get(topic_id)?;
            topic.resolve_group_id(group_id)
        })
    }

    /// `(stream_id, topic_id, group_id)` of every group the client belongs to,
    /// for `get_me` membership reporting.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn consumer_group_memberships(&self, client_id: u128) -> Vec<(u32, u32, u32)> {
        self.inner.read(|inner| {
            let mut out = Vec::new();
            for (stream_id, stream) in &inner.items {
                for (topic_id, topic) in &stream.topics {
                    for group in topic.consumer_groups.values() {
                        if group.members.iter().any(|(_, m)| m.client_id == client_id) {
                            out.push((stream_id as u32, topic_id as u32, group.id as u32));
                        }
                    }
                }
            }
            out
        })
    }

    /// Drop a disconnected client from every consumer group it joined and
    /// rebalance. Applied through the left-right writer as a deterministic
    /// side-effect of the `Logout` commit on each replica (not a separate
    /// replicated op). A no-op on the reader-mode peers, where commits aren't
    /// applied.
    pub fn remove_consumer_group_member(&self, client_id: u128, timestamp: IggyTimestamp) {
        let cmd = StreamsCommand::RemoveConsumerGroupMember(
            RemoveConsumerGroupMemberRequest { client_id },
            timestamp,
        );
        if let Err(error) = self.inner.try_apply(cmd) {
            tracing::error!(
                client_id,
                %error,
                "remove_consumer_group_member dispatched to reader-only Streams STM"
            );
        }
    }

    /// Total consumer-group count across all topics (for stats).
    #[must_use]
    pub fn consumer_group_count(&self) -> usize {
        self.inner.read(|inner| {
            inner
                .items
                .iter()
                .flat_map(|(_, stream)| stream.topics.iter())
                .map(|(_, topic)| topic.consumer_groups.len())
                .sum()
        })
    }

    #[must_use]
    pub fn partition_count_context(
        &self,
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
    ) -> Option<((usize, usize), u32)> {
        self.inner.read(|inner| {
            let stream_id = inner.resolve_stream_id(stream_id)?;
            let topic_id = inner.resolve_topic_id(stream_id, topic_id)?;
            let stream = inner.items.get(stream_id)?;
            let topic = stream.topics.get(topic_id)?;
            let next_partition_id = topic
                .partitions
                .iter()
                .map(|partition| partition.id)
                .max()
                .and_then(|partition_id| partition_id.checked_add(1))
                .and_then(|partition_id| u32::try_from(partition_id).ok())
                .unwrap_or(0);
            Some(((stream_id, topic_id), next_partition_id))
        })
    }

    #[must_use]
    pub fn current_partition_count(
        &self,
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
    ) -> Option<u32> {
        self.partition_count_context(stream_id, topic_id)
            .map(|(_, next_partition_id)| next_partition_id)
    }

    /// Pick the next partition for a `Balanced` send, advancing the topic's
    /// round-robin counter. `None` if the topic has no partitions.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn next_balanced_partition(
        &self,
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
    ) -> Option<u32> {
        self.inner.read(|inner| {
            let stream_id = inner.resolve_stream_id(stream_id)?;
            let topic_id = inner.resolve_topic_id(stream_id, topic_id)?;
            let topic = inner.items.get(stream_id)?.topics.get(topic_id)?;
            let count = topic.partitions.len();
            if count == 0 {
                return None;
            }
            let current = topic
                .round_robin_counter
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                    Some((c + 1) % count)
                })
                .unwrap_or(0);
            Some(topic.partitions[current % count].id as u32)
        })
    }

    /// Pick the partition for a `MessagesKey` send by hashing the key modulo
    /// the partition count. `None` if the topic has no partitions.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn partition_by_messages_key(
        &self,
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
        key: &[u8],
    ) -> Option<u32> {
        self.inner.read(|inner| {
            let stream_id = inner.resolve_stream_id(stream_id)?;
            let topic_id = inner.resolve_topic_id(stream_id, topic_id)?;
            let topic = inner.items.get(stream_id)?.topics.get(topic_id)?;
            let count = topic.partitions.len();
            if count == 0 {
                return None;
            }
            let index = iggy_common::calculate_32(key) as usize % count;
            Some(topic.partitions[index % count].id as u32)
        })
    }

    #[must_use]
    pub fn namespace_from_partition(
        &self,
        stream_id: &WireIdentifier,
        topic_id: &WireIdentifier,
        partition_id: u32,
    ) -> Option<IggyNamespace> {
        self.inner.read(|inner| {
            let stream_id = inner.resolve_stream_id(stream_id)?;
            let topic_id = inner.resolve_topic_id(stream_id, topic_id)?;
            let stream = inner.items.get(stream_id)?;
            let topic = stream.topics.get(topic_id)?;
            let partition_id = usize::try_from(partition_id).ok()?;
            topic
                .partitions
                .iter()
                .any(|partition| partition.id == partition_id)
                .then(|| IggyNamespace::new(stream_id, topic_id, partition_id))
        })
    }

    #[must_use]
    pub fn highest_partition_consensus_group_id(&self) -> u64 {
        self.inner.read(|inner| {
            inner
                .items
                .iter()
                .flat_map(|(_, stream)| stream.topics.iter())
                .flat_map(|(_, topic)| topic.partitions.iter())
                .map(|partition| partition.consensus_group_id)
                .max()
                .unwrap_or(0)
        })
    }
}

impl StateHandler for CreateStreamRequest {
    type State = StreamsInner;
    #[allow(clippy::cast_possible_truncation)]
    fn apply(&self, state: &mut StreamsInner, timestamp: IggyTimestamp) -> ApplyReply {
        let name_arc: Arc<str> = Arc::from(self.name.as_str());
        if state.index.contains_key(&name_arc) {
            return ApplyReply::err(CreateStreamResult::NameAlreadyExists);
        }

        let stream = Stream {
            id: 0,
            name: name_arc.clone(),
            created_at: timestamp,
            stats: Arc::new(StreamStats::default()),
            topics: Slab::new(),
            topic_index: AHashMap::default(),
        };

        let id = state.items.insert(stream);
        if let Some(stream) = state.items.get_mut(id) {
            stream.id = id;
        }
        state.index.insert(name_arc, id);

        // Reply body: a freshly created stream has no topics. The SDK
        // `create_stream` decodes a `GetStreamResponse`. Serialization is
        // local to this state machine (it owns the committed shape).
        ApplyReply::ok(
            GetStreamResponse {
                stream: StreamResponse {
                    id: id as u32,
                    created_at: timestamp.as_micros(),
                    topics_count: 0,
                    size_bytes: 0,
                    messages_count: 0,
                    name: self.name.clone(),
                },
                topics: Vec::new(),
            }
            .to_bytes(),
        )
    }
}

impl StateHandler for UpdateStreamRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner, _timestamp: IggyTimestamp) -> ApplyReply {
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return ApplyReply::err(UpdateStreamResult::StreamNotFound);
        };
        let Some(stream) = state.items.get_mut(stream_id) else {
            return ApplyReply::err(UpdateStreamResult::StreamNotFound);
        };

        let new_name_arc: Arc<str> = Arc::from(self.name.as_str());
        if let Some(&existing_id) = state.index.get(&new_name_arc)
            && existing_id != stream_id
        {
            return ApplyReply::err(UpdateStreamResult::NameAlreadyExists);
        }

        state.index.remove(&stream.name);
        stream.name = new_name_arc.clone();
        state.index.insert(new_name_arc, stream_id);
        ApplyReply::ok(Bytes::new())
    }
}

impl StateHandler for DeleteStreamRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner, _timestamp: IggyTimestamp) -> ApplyReply {
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return ApplyReply::err(DeleteStreamResult::StreamNotFound);
        };

        let Some(stream) = state.items.get(stream_id) else {
            return ApplyReply::err(DeleteStreamResult::StreamNotFound);
        };
        let name = stream.name.clone();
        state.items.remove(stream_id);
        state.index.remove(&name);
        state.revision = state.revision.wrapping_add(1);
        // The dropped stream may have held groups with pending revocations.
        state.recompute_pending_revocations_count();
        ApplyReply::ok(Bytes::new())
    }
}

impl StateHandler for PurgeStreamRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner, _timestamp: IggyTimestamp) -> ApplyReply {
        // Message data lives in the partition plane, not metadata. A purge leaves
        // the metadata shape (streams, topics, partition ids) intact, so apply
        // only validates the parent and acks: no mutation, no `revision` bump
        // (the reconciler keys off partition shape, which a purge preserves). The
        // partition-journal segment drop is not yet wired off this committed op,
        // so a committed purge is a metadata-plane no-op for now.
        // TODO: partition-plane purge off the committed op.
        if state.resolve_stream_id(&self.stream_id).is_none() {
            return ApplyReply::err(PurgeStreamResult::StreamNotFound);
        }
        ApplyReply::ok(Bytes::new())
    }
}

impl StateHandler for CreateTopicWithAssignmentsRequest {
    type State = StreamsInner;
    #[allow(clippy::cast_possible_truncation)]
    fn apply(&self, state: &mut StreamsInner, timestamp: IggyTimestamp) -> ApplyReply {
        let Some(stream_id) = state.resolve_stream_id(&self.request.stream_id) else {
            return ApplyReply::err(CreateTopicResult::StreamNotFound);
        };

        let name_arc: Arc<str> = Arc::from(self.request.name.as_str());
        // Validate under a short immutable borrow that ends before the
        // revision bump below takes `&mut state`.
        {
            let Some(stream) = state.items.get(stream_id) else {
                return ApplyReply::err(CreateTopicResult::StreamNotFound);
            };
            if stream.topic_index.contains_key(&name_arc) {
                return ApplyReply::err(CreateTopicResult::NameAlreadyExists);
            }
        }

        // Past validation: this commit adds partitions, so bump the
        // monotonic revision and stamp every new partition with it.
        let new_revision = state.revision.wrapping_add(1);
        state.revision = new_revision;

        let Some(stream) = state.items.get_mut(stream_id) else {
            return ApplyReply::err(CreateTopicResult::StreamNotFound);
        };

        let replication_factor = if self.request.replication_factor == 0 {
            1
        } else {
            self.request.replication_factor
        };

        let topic = Topic {
            id: 0,
            name: name_arc.clone(),
            created_at: timestamp,
            replication_factor,
            message_expiry: IggyExpiry::from(self.request.message_expiry),
            compression_algorithm: CompressionAlgorithm::from_code(
                self.request.compression_algorithm,
            )
            .unwrap_or_default(),
            max_topic_size: MaxTopicSize::from(self.request.max_topic_size),
            stats: Arc::new(TopicStats::new(stream.stats.clone())),
            partitions: Vec::new(),
            round_robin_counter: Arc::new(AtomicUsize::new(0)),
            consumer_groups: AHashMap::default(),
            consumer_group_index: AHashMap::default(),
            next_consumer_group_id: 1,
        };

        let topic_id = stream.topics.insert(topic);
        if let Some(topic) = stream.topics.get_mut(topic_id) {
            topic.id = topic_id;

            for partition in &self.partitions {
                let partition = Partition {
                    id: partition.partition_id as usize,
                    consensus_group_id: partition.consensus_group_id,
                    created_at: timestamp,
                    created_revision: new_revision,
                    deleted_up_to_offset: 0,
                    purge_generation: 0,
                };
                topic.partitions.push(partition);
            }
        }

        stream.topic_index.insert(name_arc, topic_id);

        let Some(topic) = stream.topics.get(topic_id) else {
            return ApplyReply::err(CreateTopicResult::StreamNotFound);
        };
        ApplyReply::ok(encode_create_topic_reply(&self.request, topic_id, topic))
    }
}

/// Encode the `CreateTopic` reply as `[TopicHeader][PartitionResponse]*`,
/// the `GetTopicResponse` shape the SDK already decodes, so the create
/// reply deserializes without a schema break. Returns empty bytes on a
/// `u32` overflow (same contract as a validation rejection) rather than
/// saturating to `u32::MAX`.
fn encode_create_topic_reply(
    request: &CreateTopicRequest,
    topic_id: usize,
    topic: &Topic,
) -> Bytes {
    let Ok(topic_id_u32) = u32::try_from(topic_id) else {
        return Bytes::new();
    };
    let Ok(partitions_count_u32) = u32::try_from(topic.partitions.len()) else {
        return Bytes::new();
    };
    let header = TopicHeader {
        id: topic_id_u32,
        created_at: topic.created_at.into(),
        partitions_count: partitions_count_u32,
        message_expiry: request.message_expiry,
        compression_algorithm: request.compression_algorithm,
        max_topic_size: request.max_topic_size,
        replication_factor: topic.replication_factor,
        size_bytes: 0,
        messages_count: 0,
        name: request.name.clone(),
    };
    let Ok(partitions_resp) = topic
        .partitions
        .iter()
        .map(|p| {
            u32::try_from(p.id).map(|id| PartitionResponse {
                id,
                created_at: p.created_at.into(),
                segments_count: 0,
                current_offset: 0,
                size_bytes: 0,
                messages_count: 0,
            })
        })
        .collect::<Result<Vec<PartitionResponse>, _>>()
    else {
        return Bytes::new();
    };

    let mut buf = BytesMut::with_capacity(
        header.encoded_size()
            + partitions_resp
                .iter()
                .map(WireEncode::encoded_size)
                .sum::<usize>(),
    );
    header.encode(&mut buf);
    for partition in &partitions_resp {
        partition.encode(&mut buf);
    }
    buf.freeze()
}

impl StateHandler for UpdateTopicRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner, _timestamp: IggyTimestamp) -> ApplyReply {
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return ApplyReply::err(UpdateTopicResult::StreamNotFound);
        };
        let Some(topic_id) = state.resolve_topic_id(stream_id, &self.topic_id) else {
            return ApplyReply::err(UpdateTopicResult::TopicNotFound);
        };

        let Some(stream) = state.items.get_mut(stream_id) else {
            return ApplyReply::err(UpdateTopicResult::StreamNotFound);
        };
        let Some(topic) = stream.topics.get_mut(topic_id) else {
            return ApplyReply::err(UpdateTopicResult::TopicNotFound);
        };

        let new_name_arc: Arc<str> = Arc::from(self.name.as_str());
        if let Some(&existing_id) = stream.topic_index.get(&new_name_arc)
            && existing_id != topic_id
        {
            return ApplyReply::err(UpdateTopicResult::NameAlreadyExists);
        }

        stream.topic_index.remove(&topic.name);
        topic.name = new_name_arc.clone();
        topic.compression_algorithm =
            CompressionAlgorithm::from_code(self.compression_algorithm).unwrap_or_default();
        topic.message_expiry = IggyExpiry::from(self.message_expiry);
        topic.max_topic_size = MaxTopicSize::from(self.max_topic_size);
        if self.replication_factor != 0 {
            topic.replication_factor = self.replication_factor;
        }
        stream.topic_index.insert(new_name_arc, topic_id);
        ApplyReply::ok(Bytes::new())
    }
}

impl StateHandler for DeleteTopicRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner, _timestamp: IggyTimestamp) -> ApplyReply {
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return ApplyReply::err(DeleteTopicResult::StreamNotFound);
        };
        let Some(topic_id) = state.resolve_topic_id(stream_id, &self.topic_id) else {
            return ApplyReply::err(DeleteTopicResult::TopicNotFound);
        };
        let Some(stream) = state.items.get_mut(stream_id) else {
            return ApplyReply::err(DeleteTopicResult::StreamNotFound);
        };

        let Some(topic) = stream.topics.get(topic_id) else {
            return ApplyReply::err(DeleteTopicResult::TopicNotFound);
        };
        let name = topic.name.clone();
        stream.topics.remove(topic_id);
        stream.topic_index.remove(&name);
        state.revision = state.revision.wrapping_add(1);
        // The dropped topic may have held groups with pending revocations.
        state.recompute_pending_revocations_count();
        ApplyReply::ok(Bytes::new())
    }
}

impl StateHandler for PurgeTopicRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner, _timestamp: IggyTimestamp) -> ApplyReply {
        // Purge keeps the topic, its partitions, and consumer-group membership;
        // it wipes message data and consumer offsets per partition. The on-disk
        // reset happens on every replica's reconciler -- here we only advance
        // each partition's monotonic purge generation, which the reconciler
        // observes (committed generation > locally applied) and turns into a
        // single empty segment at offset 0 plus cleared offsets.
        let advanced = {
            let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
                return ApplyReply::err(PurgeTopicResult::StreamNotFound);
            };
            let Some(topic_id) = state.resolve_topic_id(stream_id, &self.topic_id) else {
                return ApplyReply::err(PurgeTopicResult::TopicNotFound);
            };
            let Some(stream) = state.items.get_mut(stream_id) else {
                return ApplyReply::err(PurgeTopicResult::StreamNotFound);
            };
            let Some(topic) = stream.topics.get_mut(topic_id) else {
                return ApplyReply::err(PurgeTopicResult::TopicNotFound);
            };
            for partition in &mut topic.partitions {
                partition.purge_generation = partition.purge_generation.wrapping_add(1);
            }
            !topic.partitions.is_empty()
        };
        if advanced {
            state.revision = state.revision.wrapping_add(1);
        }
        ApplyReply::ok(Bytes::new())
    }
}

impl StateHandler for CreatePartitionsWithAssignmentsRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner, timestamp: IggyTimestamp) -> ApplyReply {
        let Some(stream_id) = state.resolve_stream_id(&self.request.stream_id) else {
            return ApplyReply::err(CreatePartitionsResult::StreamNotFound);
        };
        let Some(topic_id) = state.resolve_topic_id(stream_id, &self.request.topic_id) else {
            return ApplyReply::err(CreatePartitionsResult::TopicNotFound);
        };

        // Resolve absolute partition ids under a borrow that ends before
        // the revision bump. Validate every id transition before mutating
        // topic.partitions; mid-batch overflow + retry would otherwise
        // re-base over a partial set and mint duplicate ids.
        let resolved: Vec<usize> = {
            let Some(stream) = state.items.get_mut(stream_id) else {
                return ApplyReply::err(CreatePartitionsResult::StreamNotFound);
            };
            let Some(topic) = stream.topics.get_mut(topic_id) else {
                return ApplyReply::err(CreatePartitionsResult::TopicNotFound);
            };

            let base_partition_id = topic
                .partitions
                .iter()
                .map(|partition| partition.id)
                .max()
                .and_then(|partition_id| partition_id.checked_add(1))
                .unwrap_or(0);
            let Ok(base_partition_id) = u32::try_from(base_partition_id) else {
                return ApplyReply::err(CreatePartitionsResult::InvalidPartitionsCount);
            };

            let mut resolved: Vec<usize> = Vec::with_capacity(self.partitions.len());
            for partition in &self.partitions {
                let Some(resolved_id_u32) = partition.partition_id.checked_add(base_partition_id)
                else {
                    return ApplyReply::err(CreatePartitionsResult::InvalidPartitionsCount);
                };
                let Ok(resolved_id_usize) = usize::try_from(resolved_id_u32) else {
                    return ApplyReply::err(CreatePartitionsResult::InvalidPartitionsCount);
                };
                resolved.push(resolved_id_usize);
            }
            resolved
        };

        let new_revision = state.revision.wrapping_add(1);
        state.revision = new_revision;

        let Some(stream) = state.items.get_mut(stream_id) else {
            return ApplyReply::err(CreatePartitionsResult::StreamNotFound);
        };
        let Some(topic) = stream.topics.get_mut(topic_id) else {
            return ApplyReply::err(CreatePartitionsResult::TopicNotFound);
        };
        for (resolved_id_usize, partition) in resolved.into_iter().zip(self.partitions.iter()) {
            topic.partitions.push(Partition {
                id: resolved_id_usize,
                consensus_group_id: partition.consensus_group_id,
                created_at: timestamp,
                created_revision: new_revision,
                deleted_up_to_offset: 0,
                purge_generation: 0,
            });
        }
        // Added partitions are unassigned until the groups rebalance.
        topic.rebalance_consumer_groups();

        // Matches legacy CreatePartitions wire contract: empty-ok body on
        // success. SDK discards the reply payload (resolved ids are derivable
        // from the request's base + count).
        ApplyReply::ok(Bytes::new())
    }
}

impl StateHandler for DeletePartitionsRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner, _timestamp: IggyTimestamp) -> ApplyReply {
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return ApplyReply::err(DeletePartitionsResult::StreamNotFound);
        };
        let Some(topic_id) = state.resolve_topic_id(stream_id, &self.topic_id) else {
            return ApplyReply::err(DeletePartitionsResult::TopicNotFound);
        };

        let Some(stream) = state.items.get_mut(stream_id) else {
            return ApplyReply::err(DeletePartitionsResult::StreamNotFound);
        };
        let Some(topic) = stream.topics.get_mut(topic_id) else {
            return ApplyReply::err(DeletePartitionsResult::TopicNotFound);
        };

        let count_to_delete = self.partitions_count as usize;
        let did_delete = count_to_delete > 0 && count_to_delete <= topic.partitions.len();
        if did_delete {
            topic
                .partitions
                .truncate(topic.partitions.len() - count_to_delete);
            // Members assigned the removed partitions must give them up.
            topic.rebalance_consumer_groups();
        }
        if did_delete {
            state.revision = state.revision.wrapping_add(1);
        }
        ApplyReply::ok(Bytes::new())
    }
}

/// Snapshot representation for the Streams state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamsSnapshot {
    pub items: Vec<(usize, StreamSnapshot)>,
    /// `#[serde(default)]` so older snapshots restore at revision 0.
    #[serde(default)]
    pub revision: u64,
}

impl Snapshotable for Streams {
    type Snapshot = StreamsSnapshot;

    fn to_snapshot(&self) -> Self::Snapshot {
        self.inner.read(|inner| {
            let items: Vec<(usize, StreamSnapshot)> = inner
                .items
                .iter()
                .map(|(stream_id, stream)| {
                    let (size_bytes, messages_count, segments_count) =
                        stream.stats.load_for_snapshot();
                    let topics: Vec<(usize, TopicSnapshot)> = stream
                        .topics
                        .iter()
                        .map(|(topic_id, topic)| {
                            let (t_size, t_msgs, t_segs) = topic.stats.load_for_snapshot();
                            (
                                topic_id,
                                TopicSnapshot {
                                    id: topic.id,
                                    name: topic.name.to_string(),
                                    created_at: topic.created_at,
                                    replication_factor: topic.replication_factor,
                                    message_expiry: topic.message_expiry,
                                    compression_algorithm: topic.compression_algorithm,
                                    max_topic_size: topic.max_topic_size,
                                    stats: StatsSnapshot {
                                        size_bytes: t_size,
                                        messages_count: t_msgs,
                                        segments_count: t_segs,
                                    },
                                    partitions: topic
                                        .partitions
                                        .iter()
                                        .map(|p| PartitionSnapshot {
                                            id: p.id,
                                            consensus_group_id: p.consensus_group_id,
                                            created_at: p.created_at,
                                            created_revision: p.created_revision,
                                            deleted_up_to_offset: p.deleted_up_to_offset,
                                            purge_generation: p.purge_generation,
                                        })
                                        .collect(),
                                    consumer_groups: topic
                                        .consumer_groups
                                        .iter()
                                        .map(|(&id, group)| {
                                            (id, ConsumerGroupSnapshot::from_group(group))
                                        })
                                        .collect(),
                                    next_consumer_group_id: topic.next_consumer_group_id,
                                },
                            )
                        })
                        .collect();
                    (
                        stream_id,
                        StreamSnapshot {
                            id: stream.id,
                            name: stream.name.to_string(),
                            created_at: stream.created_at,
                            stats: StatsSnapshot {
                                size_bytes,
                                messages_count,
                                segments_count,
                            },
                            topics,
                        },
                    )
                })
                .collect();
            StreamsSnapshot {
                items,
                revision: inner.revision,
            }
        })
    }

    fn from_snapshot(
        snapshot: Self::Snapshot,
    ) -> Result<Self, crate::stm::snapshot::SnapshotError> {
        let mut index: AHashMap<Arc<str>, usize> = AHashMap::new();
        let mut stream_entries: Vec<(usize, Stream)> = Vec::new();

        for (slab_key, stream_snap) in snapshot.items {
            let stream_stats = Arc::new(StreamStats::default());
            stream_stats.store_from_snapshot(
                stream_snap.stats.size_bytes,
                stream_snap.stats.messages_count,
                stream_snap.stats.segments_count,
            );

            let mut topic_index: AHashMap<Arc<str>, usize> = AHashMap::new();
            let mut topic_entries: Vec<(usize, Topic)> = Vec::new();

            for (topic_slab_key, topic_snap) in stream_snap.topics {
                let topic_stats = Arc::new(TopicStats::new(stream_stats.clone()));
                topic_stats.store_from_snapshot(
                    topic_snap.stats.size_bytes,
                    topic_snap.stats.messages_count,
                    topic_snap.stats.segments_count,
                );
                let topic_name: Arc<str> = Arc::from(topic_snap.name.as_str());
                let topic = Topic {
                    id: topic_snap.id,
                    name: topic_name.clone(),
                    created_at: topic_snap.created_at,
                    replication_factor: topic_snap.replication_factor,
                    message_expiry: topic_snap.message_expiry,
                    compression_algorithm: topic_snap.compression_algorithm,
                    max_topic_size: topic_snap.max_topic_size,
                    stats: topic_stats,
                    partitions: topic_snap
                        .partitions
                        .into_iter()
                        .map(|p| Partition {
                            id: p.id,
                            consensus_group_id: p.consensus_group_id,
                            created_at: p.created_at,
                            created_revision: p.created_revision,
                            deleted_up_to_offset: p.deleted_up_to_offset,
                            purge_generation: p.purge_generation,
                        })
                        .collect(),
                    // Not snapshotted (see `TopicSnapshot`): start fresh.
                    round_robin_counter: Arc::new(AtomicUsize::new(0)),
                    consumer_group_index: topic_snap
                        .consumer_groups
                        .iter()
                        .map(|(_, group_snap)| (Arc::from(group_snap.name.as_str()), group_snap.id))
                        .collect(),
                    next_consumer_group_id: topic_snap
                        .next_consumer_group_id
                        .max(
                            topic_snap
                                .consumer_groups
                                .iter()
                                .map(|(id, _)| id + 1)
                                .max()
                                .unwrap_or(1),
                        )
                        .max(1),
                    consumer_groups: topic_snap
                        .consumer_groups
                        .into_iter()
                        .map(|(id, group_snap)| (id, group_snap.into_group()))
                        .collect(),
                };
                topic_index.insert(topic_name, topic_slab_key);
                topic_entries.push((topic_slab_key, topic));
            }

            let topics: Slab<Topic> = topic_entries.into_iter().collect();

            let stream_name: Arc<str> = Arc::from(stream_snap.name.as_str());
            let stream = Stream {
                id: stream_snap.id,
                name: stream_name.clone(),
                created_at: stream_snap.created_at,
                stats: stream_stats,
                topics,
                topic_index,
            };

            index.insert(stream_name, slab_key);
            stream_entries.push((slab_key, stream));
        }

        let items: Slab<Stream> = stream_entries.into_iter().collect();
        let mut inner = StreamsInner {
            index,
            items,
            revision: snapshot.revision,
            // Recomputed from the restored groups just below.
            pending_revocations_count: 0,
            last_result: None,
        };
        inner.recompute_pending_revocations_count();
        Ok(inner.into())
    }
}

impl_fill_restore!(Streams, streams);

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::WireName;
    use iggy_binary_protocol::codec::WireDecode;
    use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
    use iggy_binary_protocol::requests::partitions::{
        CreatePartitionsRequest as WireCreatePartitionsRequest,
        CreatePartitionsWithAssignmentsRequest,
    };
    use iggy_binary_protocol::requests::topics::{
        CreateTopicRequest as WireCreateTopicRequest, CreateTopicWithAssignmentsRequest,
    };
    use iggy_binary_protocol::responses::topics::get_topic::GetTopicResponse;

    #[test]
    fn truncate_partition_request_round_trips() {
        let request = TruncatePartitionRequest {
            stream_id: WireIdentifier::numeric(7),
            topic_id: WireIdentifier::numeric(3),
            partition_id: 5,
            up_to_offset: 1234,
        };
        let bytes = request.to_bytes();
        let (decoded, consumed) = TruncatePartitionRequest::decode(&bytes).expect("decode");
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.stream_id, request.stream_id);
        assert_eq!(decoded.topic_id, request.topic_id);
        assert_eq!(decoded.partition_id, request.partition_id);
        assert_eq!(decoded.up_to_offset, request.up_to_offset);
    }

    fn create_stream(inner: &mut StreamsInner, name: &str) {
        let request = CreateStreamRequest {
            name: WireName::new(name).unwrap(),
        };
        let _ = StateHandler::apply(&request, inner, IggyTimestamp::now());
    }

    fn make_topic_request(
        stream_id: u32,
        partitions_count: u32,
        name: &str,
    ) -> WireCreateTopicRequest {
        WireCreateTopicRequest {
            stream_id: WireIdentifier::numeric(stream_id),
            partitions_count,
            compression_algorithm: 0,
            message_expiry: 0,
            max_topic_size: 0,
            replication_factor: 1,
            name: WireName::new(name).unwrap(),
        }
    }

    #[test]
    fn current_partition_count_scans_existing_topic_state() {
        let mut inner = StreamsInner::new();
        create_stream(&mut inner, "stream");
        let create_topic = CreateTopicWithAssignmentsRequest {
            request: make_topic_request(0, 2, "topic"),
            partitions: vec![
                CreatedPartitionAssignment {
                    partition_id: 0,
                    consensus_group_id: 1,
                },
                CreatedPartitionAssignment {
                    partition_id: 1,
                    consensus_group_id: 2,
                },
            ],
        };
        let _ = StateHandler::apply(&create_topic, &mut inner, IggyTimestamp::now());
        let streams: Streams = inner.into();

        assert_eq!(
            streams
                .current_partition_count(&WireIdentifier::numeric(0), &WireIdentifier::numeric(0)),
            Some(2)
        );
    }

    #[test]
    fn applying_enriched_create_commands_stores_consensus_group_ids() {
        let mut inner = StreamsInner::new();
        create_stream(&mut inner, "stream");
        let create_topic = CreateTopicWithAssignmentsRequest {
            request: make_topic_request(0, 2, "topic"),
            partitions: vec![
                CreatedPartitionAssignment {
                    partition_id: 0,
                    consensus_group_id: 10,
                },
                CreatedPartitionAssignment {
                    partition_id: 1,
                    consensus_group_id: 11,
                },
            ],
        };
        let _ = StateHandler::apply(&create_topic, &mut inner, IggyTimestamp::now());

        let create_partitions = CreatePartitionsWithAssignmentsRequest {
            request: WireCreatePartitionsRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                partitions_count: 2,
            },
            partitions: vec![
                CreatedPartitionAssignment {
                    partition_id: 0,
                    consensus_group_id: 12,
                },
                CreatedPartitionAssignment {
                    partition_id: 1,
                    consensus_group_id: 13,
                },
            ],
        };
        let _ = StateHandler::apply(&create_partitions, &mut inner, IggyTimestamp::now());

        assert_eq!(inner.items[0].topics[0].partitions.len(), 4);
        assert_eq!(inner.items[0].topics[0].partitions[2].id, 2);
        assert_eq!(inner.items[0].topics[0].partitions[3].id, 3);
        assert_eq!(
            inner.items[0].topics[0].partitions[0].consensus_group_id,
            10
        );
        assert_eq!(
            inner.items[0].topics[0].partitions[3].consensus_group_id,
            13
        );
    }

    #[test]
    fn create_topic_apply_returns_get_topic_response_compatible_bytes() {
        // STM apply must emit `[TopicHeader][PartitionResponse]*` so existing
        // SDK decoders (`decode_response::<GetTopicResponse>`) parse the reply
        // without a wire-schema break.
        let mut inner = StreamsInner::new();
        create_stream(&mut inner, "stream");
        let create_topic = CreateTopicWithAssignmentsRequest {
            request: make_topic_request(0, 2, "topic"),
            partitions: vec![
                CreatedPartitionAssignment {
                    partition_id: 0,
                    consensus_group_id: 100,
                },
                CreatedPartitionAssignment {
                    partition_id: 1,
                    consensus_group_id: 101,
                },
            ],
        };

        let apply = StateHandler::apply(&create_topic, &mut inner, IggyTimestamp::now());
        assert_eq!(apply.code, 0);
        let (reply, consumed) = GetTopicResponse::decode(&apply.body).expect("reply decodes");
        assert_eq!(consumed, apply.body.len());
        assert_eq!(reply.topic.id, 0);
        assert_eq!(reply.topic.partitions_count, 2);
        assert_eq!(reply.topic.name.as_str(), "topic");
        assert_eq!(reply.partitions.len(), 2);
        assert_eq!(reply.partitions[0].id, 0);
        assert_eq!(reply.partitions[1].id, 1);
    }

    #[test]
    fn create_partitions_apply_resolves_ids_and_returns_empty_reply() {
        // STM resolves request-relative ids against the topic's current
        // partition count; the wire reply is empty (matches legacy
        // empty-ok response; SDK ignores the body).
        let mut inner = StreamsInner::new();
        create_stream(&mut inner, "stream");
        let create_topic = CreateTopicWithAssignmentsRequest {
            request: make_topic_request(0, 2, "topic"),
            partitions: vec![
                CreatedPartitionAssignment {
                    partition_id: 0,
                    consensus_group_id: 50,
                },
                CreatedPartitionAssignment {
                    partition_id: 1,
                    consensus_group_id: 51,
                },
            ],
        };
        let _ = StateHandler::apply(&create_topic, &mut inner, IggyTimestamp::now());

        let create_partitions = CreatePartitionsWithAssignmentsRequest {
            request: WireCreatePartitionsRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                partitions_count: 2,
            },
            // request-relative offsets 0..=1; base is 2 (next after the
            // two topic-creation partitions), so resolved ids are 2 and 3.
            partitions: vec![
                CreatedPartitionAssignment {
                    partition_id: 0,
                    consensus_group_id: 60,
                },
                CreatedPartitionAssignment {
                    partition_id: 1,
                    consensus_group_id: 61,
                },
            ],
        };

        let apply = StateHandler::apply(&create_partitions, &mut inner, IggyTimestamp::now());
        assert_eq!(apply.code, 0);
        assert!(apply.body.is_empty());

        let partitions = &inner.items[0].topics[0].partitions;
        assert_eq!(partitions.len(), 4);
        assert_eq!(partitions[2].id, 2);
        assert_eq!(partitions[2].consensus_group_id, 60);
        assert_eq!(partitions[3].id, 3);
        assert_eq!(partitions[3].consensus_group_id, 61);
    }

    #[test]
    fn given_missing_topic_when_apply_create_partitions_should_return_topic_not_found() {
        let mut inner = StreamsInner::new();
        create_stream(&mut inner, "stream");
        // Topic missing => validation failure path
        let create_partitions = CreatePartitionsWithAssignmentsRequest {
            request: WireCreatePartitionsRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(99),
                partitions_count: 1,
            },
            partitions: vec![CreatedPartitionAssignment {
                partition_id: 0,
                consensus_group_id: 1,
            }],
        };
        let apply = StateHandler::apply(&create_partitions, &mut inner, IggyTimestamp::now());
        assert_eq!(apply.code, u32::from(CreatePartitionsResult::TopicNotFound));
        assert!(apply.body.is_empty());
    }

    #[test]
    fn given_live_stream_when_apply_purge_stream_should_return_ok_with_empty_body() {
        let mut inner = StreamsInner::new();
        create_stream(&mut inner, "stream");
        let request = PurgeStreamRequest {
            stream_id: WireIdentifier::numeric(0),
        };
        let apply = StateHandler::apply(&request, &mut inner, IggyTimestamp::now());
        assert_eq!(apply.code, 0);
        assert!(apply.body.is_empty());
        // Purge leaves the metadata shape intact: stream still present.
        assert_eq!(inner.items.len(), 1);
    }

    #[test]
    fn given_missing_topic_when_apply_purge_topic_should_return_topic_not_found() {
        let mut inner = StreamsInner::new();
        create_stream(&mut inner, "stream");
        let request = PurgeTopicRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(99),
        };
        let apply = StateHandler::apply(&request, &mut inner, IggyTimestamp::now());
        assert_eq!(apply.code, u32::from(PurgeTopicResult::TopicNotFound));
    }

    // Drives the real `State::apply` path (parse -> dispatch -> left/right ->
    // read-back) so both `absorb_first` and `absorb_second` run, and pins that
    // they agree: a duplicate create returns the conflict code AND leaves
    // exactly one stream.
    #[test]
    fn given_duplicate_create_when_applied_through_state_should_converge_both_buffers() {
        use crate::stm::State;
        use iggy_common::Either;

        let streams = Streams::default();
        let Either::Left(first) = streams
            .apply(make_create_stream_prepare("dup", 1))
            .expect("first apply ok")
        else {
            panic!("CreateStream must be handled by the Streams state");
        };
        assert_eq!(first.code, 0);

        let Either::Left(second) = streams
            .apply(make_create_stream_prepare("dup", 2))
            .expect("second apply ok")
        else {
            panic!("CreateStream must be handled by the Streams state");
        };
        assert_eq!(
            second.code,
            u32::from(CreateStreamResult::NameAlreadyExists)
        );

        let count = streams.read(|inner| inner.items.len());
        assert_eq!(count, 1, "duplicate must not insert a second stream");
    }

    fn make_create_stream_prepare(
        name: &str,
        op: u64,
    ) -> server_common::Message<iggy_binary_protocol::PrepareHeader> {
        use iggy_binary_protocol::{Command2, Operation, PrepareHeader};
        use server_common::Message;
        use server_common::iobuf::Owned;
        use std::mem::size_of;

        let body = CreateStreamRequest {
            name: WireName::new(name).unwrap(),
        }
        .to_bytes();
        let header_size = size_of::<PrepareHeader>();
        let total = header_size + body.len();
        let mut buffer = Owned::<4096>::zeroed(total);
        {
            let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(
                &mut buffer.as_mut_slice()[..header_size],
            );
            header.command = Command2::Prepare;
            header.operation = Operation::CreateStream;
            header.op = op;
            header.size = u32::try_from(total).unwrap();
        }
        buffer.as_mut_slice()[header_size..].copy_from_slice(&body);
        Message::try_from(buffer).unwrap()
    }
}
