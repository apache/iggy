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
    PurgeTopicResult, TruncatePartitionResult, UpdateStreamResult, UpdateTopicResult,
};
use crate::stm::snapshot::Snapshotable;
use crate::{collect_handlers, define_state, impl_fill_restore};
use ahash::{AHashMap, AHashSet};
use bytes::{BufMut, Bytes, BytesMut};
use iggy_binary_protocol::codec::{WireDecode, WireEncode};
// Only `seed_namespace` (below, sim/test-gated) uses these at module scope;
// keep the imports under the same gate so a production build does not see them
// as unused. The test module re-imports them independently.
#[cfg(any(test, feature = "simulator"))]
use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
use iggy_binary_protocol::requests::consumer_groups::{
    CreateConsumerGroupRequest, DeleteConsumerGroupRequest,
};
#[cfg(any(test, feature = "simulator"))]
use iggy_binary_protocol::requests::partitions::CreatePartitionsRequest;
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
    CompressionAlgorithm, IggyExpiry, IggyTimestamp, MaxTopicSize, PartitionStats, StreamStats,
    TopicStats,
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
    /// `0` means nothing has been trimmed. Monotone only WITHIN one offset
    /// space: a purge restarts offsets at 0 and clears this back to 0, or the
    /// stale watermark would keep re-staging trims over post-purge segments.
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
    /// within `u32::MAX` (the wire rewrite in `server-ng` clamps past-ceiling
    /// ids to `u32::MAX` rather than panic). ~4 billion group creates on a
    /// single topic is unreachable in practice, but the cap is real -- past it
    /// clamped wire ids all collide on `u32::MAX`, including with a live
    /// group's offset key.
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
            next_consumer_group_id: 0,
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
            next_consumer_group_id: 0,
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

/// Cross-buffer shared aggregate stats.
///
/// The metadata STM is left-right double-buffered; `create_stream` /
/// `create_topic` / snapshot restore run on BOTH buffers (Absorb
/// re-dispatches each op), which would mint a distinct `Arc<StreamStats>` /
/// `Arc<TopicStats>` per buffer. Aggregate message/size counters are
/// `AtomicU64`s the partition plane increments DIRECTLY (outside the consensus
/// op-log), so per-buffer Arcs silently drop those increments on a buffer swap
/// (a read lands on the other, un-incremented buffer -- hence the
/// `messages_count_inconsistent` naming). This registry hands both buffers the
/// SAME `Arc` (get-or-create by id), so a direct increment is visible on every
/// read.
///
/// Shared across buffers and reader shards via `Arc` (a `StreamsInner` clone
/// shares it). Only shard 0's writer mutates the maps, under the
/// single-threaded Absorb; the `Mutex` is for `Sync` (uncontended), not
/// concurrency. Ids are deterministic across replicas (same op order), so both
/// buffers resolve the same key.
#[derive(Debug, Default)]
pub struct StatsRegistry {
    streams: std::sync::Mutex<AHashMap<usize, Arc<StreamStats>>>,
    topics: std::sync::Mutex<AHashMap<(usize, usize), Arc<TopicStats>>>,
    partitions: std::sync::Mutex<AHashMap<(usize, usize, usize), PartitionEntry>>,
}

/// Shared partition counters plus the purge generation they were last reset for.
#[derive(Debug)]
struct PartitionEntry {
    stats: Arc<PartitionStats>,
    /// Highest [`Partition::purge_generation`] this entry's counters were reset
    /// for, the registry's mirror of the partition plane's
    /// `applied_purge_generation` gate.
    ///
    /// Load-bearing: an apply runs on BOTH left-right buffers and the second run
    /// is deferred to the next metadata publish, which can be long after the
    /// purge acked. Counters are shared side state (one `Arc` across buffers),
    /// so an ungated second reset would wipe messages sent since the purge.
    purged_generation: u64,
}

impl StatsRegistry {
    fn stream(&self, id: usize) -> Arc<StreamStats> {
        self.streams
            .lock()
            .expect("stats registry mutex poisoned")
            .entry(id)
            .or_insert_with(|| Arc::new(StreamStats::default()))
            .clone()
    }

    fn topic(
        &self,
        stream_id: usize,
        topic_id: usize,
        parent: Arc<StreamStats>,
    ) -> Arc<TopicStats> {
        self.topics
            .lock()
            .expect("stats registry mutex poisoned")
            .entry((stream_id, topic_id))
            .or_insert_with(|| Arc::new(TopicStats::new(parent)))
            .clone()
    }

    /// Get-or-create the shared per-partition stats. The owning shard calls
    /// this when it materializes the partition; any shard's `get_topic` reply
    /// reads the same `Arc`, so partition-plane counters are visible
    /// cross-shard without a gather.
    ///
    /// # Panics
    /// If the registry mutex is poisoned.
    pub fn partition(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        parent: Arc<TopicStats>,
    ) -> Arc<PartitionStats> {
        self.partitions
            .lock()
            .expect("stats registry mutex poisoned")
            .entry((stream_id, topic_id, partition_id))
            .or_insert_with(|| PartitionEntry {
                stats: Arc::new(PartitionStats::new(parent)),
                purged_generation: 0,
            })
            .stats
            .clone()
    }

    /// Read-only lookup for reply builders: `None` until the owning shard
    /// materializes the partition.
    ///
    /// # Panics
    /// If the registry mutex is poisoned.
    pub fn partition_get(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
    ) -> Option<Arc<PartitionStats>> {
        self.partitions
            .lock()
            .expect("stats registry mutex poisoned")
            .get(&(stream_id, topic_id, partition_id))
            .map(|entry| entry.stats.clone())
    }

    /// Reset the counters of every partition a purge just advanced, so a client
    /// that reads right after the ack sees the purge instead of pre-purge
    /// totals. The on-disk reset stays async (the reconciler resets each
    /// partition on every replica once it observes the committed generation);
    /// this only moves the counters to the shape that reset converges on.
    ///
    /// Reset, never decrement: `zero_out_all` swaps in 0 and rolls each parent
    /// back by exactly what it swapped out, so a replayed purge entry over an
    /// already-zeroed registry cannot underflow a parent total. The generation
    /// gate on top makes the replay a no-op outright.
    ///
    /// The entry is created when missing so the gate is recorded even for a
    /// partition this node has not materialized yet. A fresh entry holds no
    /// segment, and `ensure_initial_segment` counts the one it plants, hence
    /// the segment is restored only for a partition that already had storage --
    /// inventing one here would double-count against that later bump.
    // The guard spans a read-modify-write of one entry (check the gate, stamp
    // it, take the `Arc`), so it cannot collapse into the single chained
    // expression the drop-tightening lint asks for.
    #[allow(clippy::significant_drop_tightening)]
    fn reset_purged_partitions(
        &self,
        stream_id: usize,
        topic_id: usize,
        parent: &Arc<TopicStats>,
        partitions: &[Partition],
    ) {
        for partition in partitions {
            // Guard dropped before the counters move: `zero_out_all` cascades a
            // rollback into the parent topic and stream totals, which the
            // registry map has no part in.
            let stats = {
                let mut entries = self
                    .partitions
                    .lock()
                    .expect("stats registry mutex poisoned");
                let entry = entries
                    .entry((stream_id, topic_id, partition.id))
                    .or_insert_with(|| PartitionEntry {
                        stats: Arc::new(PartitionStats::new(Arc::clone(parent))),
                        purged_generation: 0,
                    });
                if entry.purged_generation >= partition.purge_generation {
                    continue;
                }
                entry.purged_generation = partition.purge_generation;
                entry.stats.clone()
            };
            let had_storage = stats.segments_count_inconsistent() > 0;
            stats.zero_out_all();
            if had_storage {
                stats.increment_segments_count(1);
            }
        }
    }

    fn remove_stream(&self, id: usize) {
        self.streams
            .lock()
            .expect("stats registry mutex poisoned")
            .remove(&id);
        self.topics
            .lock()
            .expect("stats registry mutex poisoned")
            .retain(|(stream_id, _), _| *stream_id != id);
        self.partitions
            .lock()
            .expect("stats registry mutex poisoned")
            .retain(|(stream_id, _, _), _| *stream_id != id);
    }

    fn remove_topic(&self, stream_id: usize, topic_id: usize) {
        self.topics
            .lock()
            .expect("stats registry mutex poisoned")
            .remove(&(stream_id, topic_id));
        self.partitions
            .lock()
            .expect("stats registry mutex poisoned")
            .retain(|(sid, tid, _), _| !(*sid == stream_id && *tid == topic_id));
    }

    fn remove_partitions_from(&self, stream_id: usize, topic_id: usize, first_removed: usize) {
        self.partitions
            .lock()
            .expect("stats registry mutex poisoned")
            .retain(|(sid, tid, pid), _| {
                !(*sid == stream_id && *tid == topic_id && *pid >= first_removed)
            });
    }

    /// Drop every entry the snapshot does not describe, keeping the rest.
    ///
    /// Used by the in-place restore (state transfer), which replaces the whole
    /// stream tree but must not replace the registry: partition counters live
    /// only here (never snapshotted), so survivors have to keep their `Arc`s
    /// or every already-materialized partition reads (0,0,0,0) forever. Slab
    /// keys are recycled, so anything the snapshot dropped has to go with it.
    ///
    /// # Panics
    /// If the registry mutex is poisoned.
    fn retain_from_snapshot(&self, snapshot: &StreamsSnapshot) {
        let mut live_streams: AHashSet<usize> = AHashSet::new();
        let mut live_topics: AHashSet<(usize, usize)> = AHashSet::new();
        let mut live_partitions: AHashSet<(usize, usize, usize)> = AHashSet::new();
        for (stream_key, stream) in &snapshot.items {
            live_streams.insert(*stream_key);
            for (topic_key, topic) in &stream.topics {
                live_topics.insert((*stream_key, *topic_key));
                for partition in &topic.partitions {
                    live_partitions.insert((*stream_key, *topic_key, partition.id));
                }
            }
        }
        self.streams
            .lock()
            .expect("stats registry mutex poisoned")
            .retain(|id, _| live_streams.contains(id));
        self.topics
            .lock()
            .expect("stats registry mutex poisoned")
            .retain(|key, _| live_topics.contains(key));
        self.partitions
            .lock()
            .expect("stats registry mutex poisoned")
            .retain(|key, _| live_partitions.contains(key));
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
        // Shared aggregate stats, one `Arc` per stream/topic across both
        // left-right buffers (see `StatsRegistry`). Not snapshotted -- rebuilt
        // as streams/topics restore.
        stats_registry: Arc<StatsRegistry>,
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
        // The committed form of a client `DeleteSegments`: an unresolvable
        // target commits as a rejection, so the outcome is recorded against
        // the client's request id (its retry dedups) while surfacing the
        // typed error an empty ack would swallow.
        {
            let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
                return ApplyReply::err(TruncatePartitionResult::StreamNotFound);
            };
            let Some(topic_id) = state.resolve_topic_id(stream_id, &self.topic_id) else {
                return ApplyReply::err(TruncatePartitionResult::TopicNotFound);
            };
            let Some(stream) = state.items.get_mut(stream_id) else {
                return ApplyReply::err(TruncatePartitionResult::StreamNotFound);
            };
            let Some(topic) = stream.topics.get_mut(topic_id) else {
                return ApplyReply::err(TruncatePartitionResult::TopicNotFound);
            };
            let Some(partition) = topic
                .partitions
                .iter_mut()
                .find(|partition| partition.id == self.partition_id as usize)
            else {
                return ApplyReply::err(TruncatePartitionResult::PartitionNotFound);
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

    pub(crate) fn resolve_stream_id(&self, identifier: &WireIdentifier) -> Option<usize> {
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

    pub(crate) fn resolve_topic_id(
        &self,
        stream_id: usize,
        identifier: &WireIdentifier,
    ) -> Option<usize> {
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

    /// Committed [`Partition::created_revision`] for the exact partition the
    /// `namespace` tuple denotes, or `None` if any level of the tuple is not
    /// committed.
    ///
    /// Resolves by slab index rather than by name, so a delete + recreate that
    /// recycled the freed keys reports the NEW incarnation's revision under the
    /// byte-identical namespace. That difference is the only thing separating
    /// the two incarnations, so callers can use it to tell a materialised
    /// partition apart from the committed one it is impersonating.
    /// This sits on the per-request incarnation fence
    /// (`IggyShard::serves_committed_incarnation`) and on the park stamp, so it
    /// runs once per partition request rather than once per reconciler pass. A
    /// plain scan of `partitions` would therefore cost ~N element visits per
    /// request on an N-partition topic. Partitions are pushed in dense id order by
    /// `CreateTopicWithAssignments` / `CreatePartitionsWithAssignments`, so the
    /// direct index hits in one step; the scan stays as the fallback because
    /// nothing in the type enforces that density.
    #[must_use]
    pub fn created_revision_for_namespace(&self, namespace: IggyNamespace) -> Option<u64> {
        self.inner.read(|inner| {
            let stream = inner.items.get(namespace.stream_id())?;
            let topic = stream.topics.get(namespace.topic_id())?;
            let partition_id = namespace.partition_id();
            if let Some(partition) = topic.partitions.get(partition_id)
                && partition.id == partition_id
            {
                return Some(partition.created_revision);
            }
            topic
                .partitions
                .iter()
                .find(|partition| partition.id == partition_id)
                .map(|partition| partition.created_revision)
        })
    }

    /// Create stream slabs `0..=stream_slab`, skipping those already present.
    #[cfg(any(test, feature = "simulator"))]
    fn seed_stream_slabs(&self, stream_slab: usize) {
        for slab in 0..=stream_slab {
            if self.read(|inner| inner.items.contains(slab)) {
                continue;
            }
            self.inner
                .try_apply(StreamsCommand::CreateStream(
                    CreateStreamRequest {
                        name: WireName::new(format!("sim-stream-{slab}"))
                            .expect("sim stream name is valid"),
                    },
                    IggyTimestamp::from(1),
                ))
                .expect("sim stream seed applies on the metadata writer");
        }
    }

    /// Create topic slabs `0..=topic_slab` under `stream_slab`, skipping those
    /// already present. Only the last slab receives `target_partitions`.
    #[cfg(any(test, feature = "simulator"))]
    fn seed_topic_slabs(
        &self,
        stream_slab: usize,
        topic_slab: usize,
        target_partitions: &[CreatedPartitionAssignment],
    ) {
        let stream_wire =
            WireIdentifier::numeric(u32::try_from(stream_slab).expect("sim stream slab fits u32"));
        for slab in 0..=topic_slab {
            let present = self.read(|inner| {
                inner
                    .items
                    .get(stream_slab)
                    .is_some_and(|stream| stream.topics.contains(slab))
            });
            if present {
                continue;
            }
            let partitions = if slab == topic_slab {
                target_partitions.to_vec()
            } else {
                Vec::new()
            };
            self.inner
                .try_apply(StreamsCommand::CreateTopicWithAssignments(
                    CreateTopicWithAssignmentsRequest {
                        request: CreateTopicRequest {
                            stream_id: stream_wire.clone(),
                            partitions_count: u32::try_from(partitions.len())
                                .expect("sim partition count fits u32"),
                            compression_algorithm: 0,
                            message_expiry: 0,
                            max_topic_size: 0,
                            replication_factor: 1,
                            name: WireName::new(format!("sim-topic-{stream_slab}-{slab}"))
                                .expect("sim topic name is valid"),
                        },
                        partitions,
                    },
                    IggyTimestamp::from(1),
                ))
                .expect("sim topic seed applies on the metadata writer");
        }
    }

    /// Seed the stream / topic / partition that `namespace` denotes straight
    /// into the STM so a simulator or test namespace resolves without driving
    /// the metadata consensus + reconciler chain (which the simulator does not
    /// wire).
    ///
    /// Slab keys are handed out in creation order, so landing on stream slab
    /// `s` / topic slab `t` means creating every slab below it too; those
    /// fillers carry no partitions and are inert. Each level is skipped when
    /// already present, so seeding sibling partitions of one topic adds only
    /// the missing partition. Mirrors [`Users::ensure_root_user`]: a seed
    /// helper that bypasses consensus, never a production runtime path.
    ///
    /// # Panics
    /// Panics if the apply is attempted on a reader handle rather than the
    /// writer (never for the simulator's writer-backed STM), or if a slab id
    /// exceeds the `u32` wire identifier space.
    #[cfg(any(test, feature = "simulator"))]
    pub fn seed_namespace(&self, namespace: IggyNamespace, consensus_group_id: u64) {
        let stream_slab = namespace.stream_id();
        let topic_slab = namespace.topic_id();
        let partition_id =
            u32::try_from(namespace.partition_id()).expect("sim partition id fits u32");
        let stream_wire = || {
            WireIdentifier::numeric(u32::try_from(stream_slab).expect("sim stream slab fits u32"))
        };
        let topic_wire =
            || WireIdentifier::numeric(u32::try_from(topic_slab).expect("sim topic slab fits u32"));
        // Filler partitions created to keep the id range dense get the group id
        // their own namespace would carry, so the addressed one keeps the
        // caller's value.
        let sibling_group_id = |id: u32| {
            if id == partition_id {
                consensus_group_id
            } else {
                IggyNamespace::new(stream_slab, topic_slab, id as usize).inner()
            }
        };

        // Only the addressed topic carries partitions; the fillers below it
        // exist purely to advance the slab counter. Ids are absolute on the
        // topic-create command, but the additive path below can only append
        // above the current maximum, so seed the whole dense range up front and
        // leave no gap for a later sibling to fall into.
        let target_partitions: Vec<_> = (0..=partition_id)
            .map(|id| CreatedPartitionAssignment {
                partition_id: id,
                consensus_group_id: sibling_group_id(id),
            })
            .collect();
        self.seed_stream_slabs(stream_slab);
        self.seed_topic_slabs(stream_slab, topic_slab, &target_partitions);

        if self.created_revision_for_namespace(namespace).is_some() {
            return;
        }
        // Sibling partition of a topic seeded by an earlier call. Ids on this
        // command are relative to `max(existing) + 1`, so append the whole run
        // up to the addressed id rather than the single id itself.
        let base = self.read(|inner| {
            inner
                .items
                .get(stream_slab)
                .and_then(|stream| stream.topics.get(topic_slab))
                .and_then(|topic| topic.partitions.iter().map(|partition| partition.id).max())
                .map_or(0, |highest| highest + 1)
        });
        let base = u32::try_from(base).expect("sim partition base fits u32");
        let partitions = (base..=partition_id)
            .map(|id| CreatedPartitionAssignment {
                partition_id: id - base,
                consensus_group_id: sibling_group_id(id),
            })
            .collect::<Vec<_>>();
        self.inner
            .try_apply(StreamsCommand::CreatePartitionsWithAssignments(
                CreatePartitionsWithAssignmentsRequest {
                    request: CreatePartitionsRequest {
                        stream_id: stream_wire(),
                        topic_id: topic_wire(),
                        partitions_count: u32::try_from(partitions.len())
                            .expect("sim partition count fits u32"),
                    },
                    partitions,
                },
                IggyTimestamp::from(1),
            ))
            .expect("sim partition seed applies on the metadata writer");
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

        // Share one `Arc<StreamStats>` across both left-right buffers via the
        // registry (see `StatsRegistry`). The id the next insert will use is
        // deterministic, so both buffers resolve the same registry key.
        let id = state.items.vacant_key();
        let stream_stats = state.stats_registry.stream(id);
        let stream = Stream {
            id,
            name: name_arc.clone(),
            created_at: timestamp,
            stats: stream_stats,
            topics: Slab::new(),
            topic_index: AHashMap::default(),
        };
        let inserted = state.items.insert(stream);
        debug_assert_eq!(inserted, id, "vacant_key must match the insert slot");
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
        // Evict registry entries so a reused slab id starts with fresh stats.
        state.stats_registry.remove_stream(stream_id);
        state.revision = state.revision.wrapping_add(1);
        // The dropped stream may have held groups with pending revocations.
        state.recompute_pending_revocations_count();
        ApplyReply::ok(Bytes::new())
    }
}

impl StateHandler for PurgeStreamRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner, _timestamp: IggyTimestamp) -> ApplyReply {
        // Stream purge = topic purge over every topic in the stream: advance
        // each partition's monotonic purge generation, clear the delete
        // watermark, and reset the partition counters; every replica's
        // reconciler observes the committed generation and resets the partition
        // to a single empty segment at offset 0 with cleared offsets (see
        // `PurgeTopicRequest`). Metadata shape stays intact.
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return ApplyReply::err(PurgeStreamResult::StreamNotFound);
        };
        let Some(stream) = state.items.get_mut(stream_id) else {
            return ApplyReply::err(PurgeStreamResult::StreamNotFound);
        };
        let mut advanced = false;
        for (topic_id, topic) in &mut stream.topics {
            for partition in &mut topic.partitions {
                partition.purge_generation = partition.purge_generation.wrapping_add(1);
                partition.deleted_up_to_offset = 0;
                advanced = true;
            }
            state.stats_registry.reset_purged_partitions(
                stream_id,
                topic_id,
                &topic.stats,
                &topic.partitions,
            );
        }
        if advanced {
            state.revision = state.revision.wrapping_add(1);
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

        // Share one `Arc<TopicStats>` across both left-right buffers via the
        // registry, parented to the stream's shared `Arc<StreamStats>`. The id
        // the next insert will use is deterministic across buffers. Fetched
        // under an immutable borrow that ends before the `&mut stream` below,
        // so the registry access (a sibling field) does not alias.
        let (topic_id, parent_stats) = {
            let Some(stream) = state.items.get(stream_id) else {
                return ApplyReply::err(CreateTopicResult::StreamNotFound);
            };
            (stream.topics.vacant_key(), stream.stats.clone())
        };
        let topic_stats = state
            .stats_registry
            .topic(stream_id, topic_id, parent_stats);

        let Some(stream) = state.items.get_mut(stream_id) else {
            return ApplyReply::err(CreateTopicResult::StreamNotFound);
        };

        let replication_factor = if self.request.replication_factor == 0 {
            1
        } else {
            self.request.replication_factor
        };

        let topic = Topic {
            id: topic_id,
            name: name_arc.clone(),
            created_at: timestamp,
            replication_factor,
            message_expiry: IggyExpiry::from(self.request.message_expiry),
            compression_algorithm: CompressionAlgorithm::from_code(
                self.request.compression_algorithm,
            )
            .unwrap_or_default(),
            max_topic_size: MaxTopicSize::from(self.request.max_topic_size),
            stats: topic_stats,
            partitions: Vec::new(),
            round_robin_counter: Arc::new(AtomicUsize::new(0)),
            consumer_groups: AHashMap::default(),
            consumer_group_index: AHashMap::default(),
            next_consumer_group_id: 0,
        };

        let inserted = stream.topics.insert(topic);
        debug_assert_eq!(inserted, topic_id, "vacant_key must match the insert slot");
        if let Some(topic) = stream.topics.get_mut(inserted) {
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
        // Evict registry entry so a reused slab id starts with fresh stats.
        state.stats_registry.remove_topic(stream_id, topic_id);
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
        //
        // The delete watermark is replicated state describing the PRE-purge
        // offset space, so it is cleared in the same apply: the purge restarts
        // offsets at 0 and drops the consumer-offset barrier that bounded the
        // trim, and the reconciler re-stages any nonzero watermark on every
        // pass -- a surviving one would delete post-purge segments.
        //
        // The shared partition counters are reset here too: they are read back
        // by `get_topic` / `get_stream` on any node that applied this commit,
        // and leaving them until the reconciler runs makes a purge ack followed
        // by a read report pre-purge totals.
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
            partition.deleted_up_to_offset = 0;
        }
        let advanced = !topic.partitions.is_empty();
        state.stats_registry.reset_purged_partitions(
            stream_id,
            topic_id,
            &topic.stats,
            &topic.partitions,
        );
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
        if count_to_delete > topic.partitions.len() {
            return ApplyReply::err(DeletePartitionsResult::InvalidPartitionsCount);
        }
        // Zero count is rejected pre-consensus; a replayed legacy entry still
        // applies as the historical ok no-op.
        if count_to_delete > 0 {
            let retained = topic.partitions.len() - count_to_delete;
            topic.partitions.truncate(retained);
            // Members assigned the removed partitions must give them up.
            topic.rebalance_consumer_groups();
            // Evict registry entries so re-created partition ids start with
            // fresh stats.
            state
                .stats_registry
                .remove_partitions_from(stream_id, topic_id, retained);
            state.revision = state.revision.wrapping_add(1);
        }
        ApplyReply::ok(Bytes::new())
    }
}

/// Snapshot representation for the Streams state machine.
///
/// Serialized-form invariant (see [`crate::stm::snapshot::MetadataSnapshot`]):
/// `items` and the nested `topics` / `consumer_groups` / `partitions` stay ordered
/// `Vec`s even though the runtime holds them in `AHashMap`s and a `Slab`. Swapping
/// any back to an unordered map reorders on a decode and re-encode, breaking the
/// checkpoint checksum cross-check recovery relies on.
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
        // Boot: no live registry exists yet, so mint one. Safe because
        // `new_from_empty` clones this single inner onto the other left-right
        // buffer rather than building a second one.
        Ok(StreamsInner::inner_from_snapshot(snapshot, Arc::new(StatsRegistry::default())).into())
    }
}

impl StreamsInner {
    /// Rebuild from a snapshot section IN PLACE, keeping the live stats
    /// registry.
    ///
    /// The restore command is absorbed on BOTH left-right buffers, so minting
    /// a registry here would hand the two buffers different `Arc`s and split
    /// every direct partition-plane counter increment by publish parity --
    /// exactly what [`StatsRegistry`] exists to prevent. Carrying the registry
    /// across also preserves the `Arc<PartitionStats>` the data plane
    /// registered at bootstrap and reconcile, which nothing in a snapshot can
    /// reconstruct (partition counters are not snapshotted).
    pub(crate) fn restore_in_place(&mut self, snapshot: StreamsSnapshot) {
        let registry = Arc::clone(&self.stats_registry);
        // Slab keys are recycled, so an entry left over from a stream the
        // snapshot does not have would hand its counters to whatever lands in
        // that slot next.
        registry.retain_from_snapshot(&snapshot);
        *self = Self::inner_from_snapshot(snapshot, registry);
    }

    /// Build a complete `StreamsInner` from a snapshot section against
    /// `stats_registry`. Shared by wrapper construction
    /// ([`Snapshotable::from_snapshot`]) and the in-place restore command
    /// (state transfer), which absorbs it on both left-right buffers.
    pub(crate) fn inner_from_snapshot(
        snapshot: StreamsSnapshot,
        stats_registry: Arc<StatsRegistry>,
    ) -> Self {
        let mut index: AHashMap<Arc<str>, usize> = AHashMap::new();
        let mut stream_entries: Vec<(usize, Stream)> = Vec::new();

        for (slab_key, stream_snap) in snapshot.items {
            let stream_stats = stats_registry.stream(slab_key);
            stream_stats.store_from_snapshot(
                stream_snap.stats.size_bytes,
                stream_snap.stats.messages_count,
                stream_snap.stats.segments_count,
            );

            let mut topic_index: AHashMap<Arc<str>, usize> = AHashMap::new();
            let mut topic_entries: Vec<(usize, Topic)> = Vec::new();

            for (topic_slab_key, topic_snap) in stream_snap.topics {
                let topic_stats =
                    stats_registry.topic(slab_key, topic_slab_key, stream_stats.clone());
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
                    next_consumer_group_id: topic_snap.next_consumer_group_id.max(
                        topic_snap
                            .consumer_groups
                            .iter()
                            .map(|(id, _)| id + 1)
                            .max()
                            .unwrap_or(0),
                    ),
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
        let mut inner = Self {
            index,
            items,
            revision: snapshot.revision,
            // Recomputed from the restored groups just below.
            pending_revocations_count: 0,
            last_result: None,
            stats_registry,
        };
        inner.recompute_pending_revocations_count();
        inner
    }
}

impl_fill_restore!(Streams, streams);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stm::snapshot::MetadataSnapshot;
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

    /// Regression guard for the [`StreamsSnapshot`] serialized-form invariant: a
    /// populated snapshot must re-encode byte-identically after a decode, or the
    /// checkpoint checksum cross-check (`recovery::verify_checkpoint_pairing`) would
    /// diverge and refuse boot on a healthy node. Populates the three map-derived
    /// `Vec`s (`items`, `topics`, `consumer_groups`) with two entries each, since one
    /// entry cannot reorder and only >= 2 makes a regression observable.
    #[test]
    fn populated_streams_snapshot_reencode_is_byte_stable() {
        let mut inner = StreamsInner::new();
        for name in ["alpha", "beta"] {
            create_stream(&mut inner, name);
        }
        // Streams are assigned ids 0, 1 in creation order; two topics per stream,
        // two consumer groups per topic.
        for stream_id in 0..2u32 {
            for topic_name in ["logs", "events"] {
                let create_topic = CreateTopicWithAssignmentsRequest {
                    request: make_topic_request(stream_id, 2, topic_name),
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
            }
        }
        for stream_id in 0..2u32 {
            for topic_id in 0..2u32 {
                for group_name in ["cg-a", "cg-b"] {
                    let request = CreateConsumerGroupRequest {
                        stream_id: WireIdentifier::numeric(stream_id),
                        topic_id: WireIdentifier::numeric(topic_id),
                        name: WireName::new(group_name).unwrap(),
                    };
                    let _ = StateHandler::apply(&request, &mut inner, IggyTimestamp::now());
                }
            }
        }
        let streams: Streams = inner.into();

        let mut snapshot = MetadataSnapshot::new(7);
        snapshot.streams = Some(streams.to_snapshot());

        // The tree really is populated, else a byte-stable empty snapshot would pass
        // vacuously.
        let streams_snapshot = snapshot.streams.as_ref().unwrap();
        assert_eq!(streams_snapshot.items.len(), 2, "two streams");
        let (_, first_stream) = &streams_snapshot.items[0];
        assert_eq!(
            first_stream.topics.len(),
            2,
            "two topics in the first stream"
        );
        let (_, first_topic) = &first_stream.topics[0];
        assert_eq!(
            first_topic.consumer_groups.len(),
            2,
            "two consumer groups in the first topic"
        );

        let encoded = snapshot.encode().unwrap();
        let reencoded = MetadataSnapshot::decode(&encoded)
            .unwrap()
            .encode()
            .unwrap();
        assert_eq!(
            encoded, reencoded,
            "a populated snapshot must re-encode byte-identically after a decode; an \
             unordered collection would reorder and break the checkpoint checksum \
             cross-check, refusing boot on a healthy node"
        );
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

    /// Over-count deletes were acked ok as a silent no-op; they must commit the
    /// legacy `InvalidPartitionsCount` rejection. Zero stays an ok no-op at the
    /// apply (rejected pre-consensus; a replayed entry keeps its historical ack).
    #[test]
    fn given_delete_partitions_counts_when_applied_should_reject_over_count() {
        let cases: &[(u32, u32, u32, usize)] = &[
            // (partitions in topic, count to delete, expected code, remaining)
            (
                3,
                4,
                u32::from(DeletePartitionsResult::InvalidPartitionsCount),
                3,
            ),
            (
                0,
                1,
                u32::from(DeletePartitionsResult::InvalidPartitionsCount),
                0,
            ),
            (3, 0, 0, 3),
            (3, 3, 0, 0),
            (3, 2, 0, 1),
        ];
        for &(partitions_count, count_to_delete, expected_code, expected_remaining) in cases {
            let mut inner = StreamsInner::new();
            create_stream(&mut inner, "stream");
            let create_topic = CreateTopicWithAssignmentsRequest {
                request: make_topic_request(0, partitions_count, "topic"),
                partitions: (0..partitions_count)
                    .map(|partition_id| CreatedPartitionAssignment {
                        partition_id,
                        consensus_group_id: 1,
                    })
                    .collect(),
            };
            let _ = StateHandler::apply(&create_topic, &mut inner, IggyTimestamp::now());

            let delete = DeletePartitionsRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                partitions_count: count_to_delete,
            };
            let apply = StateHandler::apply(&delete, &mut inner, IggyTimestamp::now());

            assert_eq!(
                apply.code, expected_code,
                "deleting {count_to_delete} of {partitions_count} partitions"
            );
            assert!(apply.body.is_empty());
            assert_eq!(
                inner.items[0].topics[0].partitions.len(),
                expected_remaining,
                "deleting {count_to_delete} of {partitions_count} partitions"
            );
        }
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

    /// A purge restarts the offset space at 0, so a watermark from the old one
    /// must not survive: the reconciler re-stages every nonzero watermark on
    /// each pass, and the consumer-offset barrier that bounded the trim is
    /// cleared by the purge too, so a stale watermark deletes post-purge
    /// segments.
    #[test]
    fn given_truncated_partition_when_apply_purge_should_clear_delete_watermark() {
        let mut inner = StreamsInner::new();
        create_stream(&mut inner, "stream");
        let create_topic = CreateTopicWithAssignmentsRequest {
            request: make_topic_request(0, 1, "topic"),
            partitions: vec![CreatedPartitionAssignment {
                partition_id: 0,
                consensus_group_id: 1,
            }],
        };
        let _ = StateHandler::apply(&create_topic, &mut inner, IggyTimestamp::now());

        let truncate = TruncatePartitionRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partition_id: 0,
            up_to_offset: 500,
        };
        let apply = StateHandler::apply(&truncate, &mut inner, IggyTimestamp::now());
        assert_eq!(apply.code, 0);
        assert_eq!(
            inner.items[0].topics[0].partitions[0].deleted_up_to_offset,
            500
        );

        let purge = PurgeTopicRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
        };
        let apply = StateHandler::apply(&purge, &mut inner, IggyTimestamp::now());
        assert_eq!(apply.code, 0);
        assert_eq!(
            inner.items[0].topics[0].partitions[0].deleted_up_to_offset, 0,
            "the purge must clear the pre-purge delete watermark"
        );
        assert_eq!(
            inner.items[0].topics[0].partitions[0].purge_generation, 1,
            "the purge generation still advances"
        );

        // Same for the stream-wide purge, which walks every topic.
        let _ = StateHandler::apply(&truncate, &mut inner, IggyTimestamp::now());
        assert_eq!(
            inner.items[0].topics[0].partitions[0].deleted_up_to_offset,
            500
        );
        let purge_stream = PurgeStreamRequest {
            stream_id: WireIdentifier::numeric(0),
        };
        let _ = StateHandler::apply(&purge_stream, &mut inner, IggyTimestamp::now());
        assert_eq!(
            inner.items[0].topics[0].partitions[0].deleted_up_to_offset, 0,
            "a stream purge clears the watermark on every partition it walks"
        );
    }

    /// A purge acks on commit while the on-disk reset waits for the reconciler,
    /// so the counters `get_topic` / `get_stream` read must move in the apply or
    /// a read right after the ack reports pre-purge totals.
    #[test]
    fn given_counted_partition_when_apply_purge_topic_should_zero_the_scope() {
        let mut inner = inner_with_registered_partition();
        let stats = inner.stats_registry.partition_get(0, 0, 0).expect("stats");
        stats.increment_segments_count(1);
        stats.increment_messages_count(7);
        stats.increment_size_bytes(512);
        stats.set_current_offset(6);
        assert_eq!(
            inner.items[0].topics[0].stats.messages_count_inconsistent(),
            7,
            "partition counters must roll up before the purge, or the test proves nothing"
        );

        let purge = PurgeTopicRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
        };
        let apply = StateHandler::apply(&purge, &mut inner, IggyTimestamp::now());
        assert_eq!(apply.code, 0);

        assert_eq!(stats.messages_count_inconsistent(), 0);
        assert_eq!(stats.size_bytes_inconsistent(), 0);
        assert_eq!(stats.current_offset(), 0);
        assert_eq!(
            stats.segments_count_inconsistent(),
            1,
            "a purged partition keeps the one empty segment the reset lands on"
        );
        let topic_stats = &inner.items[0].topics[0].stats;
        assert_eq!(topic_stats.messages_count_inconsistent(), 0);
        assert_eq!(topic_stats.size_bytes_inconsistent(), 0);
        let stream_stats = &inner.items[0].stats;
        assert_eq!(stream_stats.messages_count_inconsistent(), 0);
        assert_eq!(stream_stats.size_bytes_inconsistent(), 0);
    }

    /// A stream purge walks every topic, so every topic's partitions must reset,
    /// not just the first one.
    #[test]
    fn given_counted_partitions_when_apply_purge_stream_should_zero_every_topic() {
        let mut inner = inner_with_registered_partition();
        let create_topic = CreateTopicWithAssignmentsRequest {
            request: make_topic_request(0, 1, "metrics"),
            partitions: vec![CreatedPartitionAssignment {
                partition_id: 0,
                consensus_group_id: 2,
            }],
        };
        let _ = StateHandler::apply(&create_topic, &mut inner, IggyTimestamp::now());
        let second_topic_stats = inner.items[0].topics[1].stats.clone();
        inner.stats_registry.partition(0, 1, 0, second_topic_stats);

        let counters: Vec<Arc<PartitionStats>> = (0..2)
            .map(|topic_id| {
                let stats = inner
                    .stats_registry
                    .partition_get(0, topic_id, 0)
                    .expect("stats");
                stats.increment_segments_count(1);
                stats.increment_messages_count(9);
                stats.increment_size_bytes(64);
                stats
            })
            .collect();
        assert_eq!(inner.items[0].stats.messages_count_inconsistent(), 18);

        let purge = PurgeStreamRequest {
            stream_id: WireIdentifier::numeric(0),
        };
        let apply = StateHandler::apply(&purge, &mut inner, IggyTimestamp::now());
        assert_eq!(apply.code, 0);

        for stats in &counters {
            assert_eq!(stats.messages_count_inconsistent(), 0);
            assert_eq!(stats.size_bytes_inconsistent(), 0);
            assert_eq!(stats.segments_count_inconsistent(), 1);
        }
        assert_eq!(inner.items[0].stats.messages_count_inconsistent(), 0);
        assert_eq!(inner.items[0].stats.size_bytes_inconsistent(), 0);
    }

    /// The left-right buffers absorb every op twice and the second absorb is
    /// deferred to the next metadata publish, which can land long after the
    /// purge acked. Counters are shared side state, so the deferred replay must
    /// leave post-purge traffic alone -- and must not decrement a parent total
    /// it already rolled back.
    #[test]
    fn given_purged_buffer_when_other_buffer_replays_purge_should_keep_new_counters() {
        let mut first = inner_with_registered_partition();
        let mut second = first.clone();
        let stats = first.stats_registry.partition_get(0, 0, 0).expect("stats");
        stats.increment_segments_count(1);
        stats.increment_messages_count(10);
        stats.increment_size_bytes(320);

        let purge = PurgeTopicRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
        };
        let _ = StateHandler::apply(&purge, &mut first, IggyTimestamp::now());
        assert_eq!(stats.messages_count_inconsistent(), 0);

        // Sent after the ack, before the deferred absorb on the other buffer.
        stats.increment_messages_count(4);
        stats.increment_size_bytes(128);

        let _ = StateHandler::apply(&purge, &mut second, IggyTimestamp::now());
        assert_eq!(
            second.items[0].topics[0].partitions[0].purge_generation, 1,
            "the replay computes the same generation, so the gate is what stops it"
        );
        assert_eq!(
            stats.messages_count_inconsistent(),
            4,
            "the deferred replay must not wipe post-purge counters"
        );
        assert_eq!(stats.size_bytes_inconsistent(), 128);
        let topic_stats = first.items[0].topics[0].stats.clone();
        assert_eq!(
            topic_stats.messages_count_inconsistent(),
            4,
            "a second rollback of the same total would underflow the parent"
        );
        assert_eq!(topic_stats.size_bytes_inconsistent(), 128);

        // A genuinely new purge still resets: the gate is per generation.
        let _ = StateHandler::apply(&purge, &mut first, IggyTimestamp::now());
        assert_eq!(stats.messages_count_inconsistent(), 0);
        assert_eq!(topic_stats.messages_count_inconsistent(), 0);
    }

    /// Boot replays the metadata WAL before any partition materializes, so the
    /// purge has no counters to reset -- but it must still record the gate, or
    /// the deferred second absorb wipes whatever the partition loaded since.
    #[test]
    fn given_unmaterialized_partition_when_apply_purge_should_gate_the_replay() {
        let mut inner = StreamsInner::new();
        create_stream(&mut inner, "alpha");
        let create_topic = CreateTopicWithAssignmentsRequest {
            request: make_topic_request(0, 1, "logs"),
            partitions: vec![CreatedPartitionAssignment {
                partition_id: 0,
                consensus_group_id: 1,
            }],
        };
        let _ = StateHandler::apply(&create_topic, &mut inner, IggyTimestamp::now());
        let mut replay = inner.clone();

        let purge = PurgeTopicRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
        };
        let _ = StateHandler::apply(&purge, &mut inner, IggyTimestamp::now());

        // The data plane materializes the partition afterwards and counts what
        // it plants; the purge must not have invented a segment for it.
        let topic_stats = inner.items[0].topics[0].stats.clone();
        let stats = inner.stats_registry.partition(0, 0, 0, topic_stats);
        assert_eq!(stats.segments_count_inconsistent(), 0);
        stats.increment_segments_count(1);
        stats.increment_messages_count(5);

        let _ = StateHandler::apply(&purge, &mut replay, IggyTimestamp::now());
        assert_eq!(
            stats.messages_count_inconsistent(),
            5,
            "the gate recorded at apply must survive into the partition's entry"
        );
        assert_eq!(stats.segments_count_inconsistent(), 1);
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

    /// One stream, one topic, one partition, materialized in the registry the
    /// way the data plane does at bootstrap.
    fn inner_with_registered_partition() -> StreamsInner {
        let mut inner = StreamsInner::new();
        create_stream(&mut inner, "alpha");
        let create_topic = CreateTopicWithAssignmentsRequest {
            request: make_topic_request(0, 1, "logs"),
            partitions: vec![CreatedPartitionAssignment {
                partition_id: 0,
                consensus_group_id: 1,
            }],
        };
        let _ = StateHandler::apply(&create_topic, &mut inner, IggyTimestamp::now());
        let topic_stats = inner.items[0].topics[0].stats.clone();
        inner.stats_registry.partition(0, 0, 0, topic_stats);
        inner
    }

    // The restore command is absorbed on BOTH left-right buffers. Minting a
    // registry per call would hand the two buffers different `Arc`s, so a
    // direct partition-plane increment would land on one buffer and vanish on
    // the next publish -- the `messages_count_inconsistent` failure the
    // registry exists to prevent.
    #[test]
    fn in_place_restore_keeps_one_registry_across_both_buffers() {
        let mut first = inner_with_registered_partition();
        let snapshot = Streams::from(first.clone()).to_snapshot();
        let mut second = first.clone();

        first.restore_in_place(snapshot.clone());
        second.restore_in_place(snapshot);

        assert!(
            Arc::ptr_eq(&first.stats_registry, &second.stats_registry),
            "both buffers must keep the one shared registry"
        );
        let from_first = first
            .stats_registry
            .partition_get(0, 0, 0)
            .expect("survivor keeps its partition stats");
        let from_second = second
            .stats_registry
            .partition_get(0, 0, 0)
            .expect("survivor keeps its partition stats");
        assert!(Arc::ptr_eq(&from_first, &from_second));
    }

    // Partition counters live only in the registry (never snapshotted), so an
    // install that dropped them would leave every already-materialized
    // partition reading zeroes with no way to recover them.
    #[test]
    fn in_place_restore_keeps_survivor_partition_stats() {
        let mut inner = inner_with_registered_partition();
        let stats = inner.stats_registry.partition_get(0, 0, 0).expect("stats");
        stats.increment_messages_count(42);
        let snapshot = Streams::from(inner.clone()).to_snapshot();

        inner.restore_in_place(snapshot);

        let after = inner
            .stats_registry
            .partition_get(0, 0, 0)
            .expect("partition survived the restore, so its stats must too");
        assert!(Arc::ptr_eq(&stats, &after));
        assert_eq!(after.messages_count_inconsistent(), 42);
    }

    // Slab keys are recycled: an entry left behind by a stream the snapshot
    // does not carry would hand its counters to whatever lands in that slot.
    #[test]
    fn in_place_restore_prunes_entries_the_snapshot_dropped() {
        let mut inner = inner_with_registered_partition();
        // A second stream that the snapshot below will not contain.
        let empty = Streams::from(StreamsInner::new()).to_snapshot();
        assert!(inner.stats_registry.partition_get(0, 0, 0).is_some());

        inner.restore_in_place(empty);

        assert!(
            inner.stats_registry.partition_get(0, 0, 0).is_none(),
            "a partition the snapshot dropped must not keep its registry entry"
        );
    }
}
