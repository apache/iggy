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

use crate::stats::{StreamStats, TopicStats};
use crate::stm::StateHandler;
use crate::stm::snapshot::Snapshotable;
use crate::{collect_handlers, define_state, impl_fill_restore};
use ahash::AHashMap;
use iggy_common::create_partitions::CreatePartitions;
use iggy_common::create_stream::CreateStream;
use iggy_common::create_topic::CreateTopic;
use iggy_common::delete_partitions::DeletePartitions;
use iggy_common::delete_stream::DeleteStream;
use iggy_common::delete_topic::DeleteTopic;
use iggy_common::purge_stream::PurgeStream;
use iggy_common::purge_topic::PurgeTopic;
use iggy_common::update_stream::UpdateStream;
use iggy_common::update_topic::UpdateTopic;
use iggy_common::{CompressionAlgorithm, IggyExpiry, IggyTimestamp, MaxTopicSize};
use serde::{Deserialize, Serialize};
use slab::Slab;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Partition snapshot representation for serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSnapshot {
    pub id: usize,
    pub created_at: IggyTimestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(into = "PartitionSnapshot", from = "PartitionSnapshot")]
pub struct Partition {
    pub id: usize,
    pub created_at: IggyTimestamp,
}

impl Partition {
    pub fn new(id: usize, created_at: IggyTimestamp) -> Self {
        Self { id, created_at }
    }
}

impl From<Partition> for PartitionSnapshot {
    fn from(p: Partition) -> Self {
        Self {
            id: p.id,
            created_at: p.created_at,
        }
    }
}

impl From<PartitionSnapshot> for Partition {
    fn from(p: PartitionSnapshot) -> Self {
        Self {
            id: p.id,
            created_at: p.created_at,
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
    pub round_robin_counter: usize,
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
        }
    }
}

impl From<&Topic> for TopicSnapshot {
    fn from(topic: &Topic) -> Self {
        let (size_bytes, messages_count, segments_count) = topic.stats.load_for_snapshot();
        Self {
            id: topic.id,
            name: topic.name.to_string(),
            created_at: topic.created_at,
            replication_factor: topic.replication_factor,
            message_expiry: topic.message_expiry,
            compression_algorithm: topic.compression_algorithm,
            max_topic_size: topic.max_topic_size,
            stats: StatsSnapshot {
                size_bytes,
                messages_count,
                segments_count,
            },
            partitions: topic
                .partitions
                .iter()
                .map(|p| PartitionSnapshot::from(p.clone()))
                .collect(),
            round_robin_counter: topic.round_robin_counter.load(Ordering::Relaxed),
        }
    }
}

impl TopicSnapshot {
    /// Convert to Topic with the given parent stream stats.
    pub fn into_topic(self, stream_stats: Arc<StreamStats>) -> Topic {
        let topic_stats = Arc::new(TopicStats::new(stream_stats));
        topic_stats.store_from_snapshot(
            self.stats.size_bytes,
            self.stats.messages_count,
            self.stats.segments_count,
        );
        Topic {
            id: self.id,
            name: Arc::from(self.name.as_str()),
            created_at: self.created_at,
            replication_factor: self.replication_factor,
            message_expiry: self.message_expiry,
            compression_algorithm: self.compression_algorithm,
            max_topic_size: self.max_topic_size,
            stats: topic_stats,
            partitions: self.partitions.into_iter().map(Partition::from).collect(),
            round_robin_counter: Arc::new(AtomicUsize::new(self.round_robin_counter)),
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

impl From<&Stream> for StreamSnapshot {
    fn from(stream: &Stream) -> Self {
        let (size_bytes, messages_count, segments_count) = stream.stats.load_for_snapshot();
        Self {
            id: stream.id,
            name: stream.name.to_string(),
            created_at: stream.created_at,
            stats: StatsSnapshot {
                size_bytes,
                messages_count,
                segments_count,
            },
            topics: stream
                .topics
                .iter()
                .map(|(id, topic)| (id, TopicSnapshot::from(topic)))
                .collect(),
        }
    }
}

impl StreamSnapshot {
    /// Convert to Stream. Returns the stream and a Vec of (expected_id, actual_id) for validation.
    pub fn into_stream(self) -> Result<Stream, crate::stm::snapshot::SnapshotError> {
        use crate::stm::snapshot::SnapshotError;

        let stream_stats = Arc::new(StreamStats::default());
        stream_stats.store_from_snapshot(
            self.stats.size_bytes,
            self.stats.messages_count,
            self.stats.segments_count,
        );

        let mut topics: Slab<Topic> = Slab::new();
        let mut topic_index: AHashMap<Arc<str>, usize> = AHashMap::new();

        for (expected_id, topic_snap) in self.topics {
            let topic = topic_snap.into_topic(stream_stats.clone());
            let topic_name = topic.name.clone();
            let actual_id = topics.insert(topic);
            if actual_id != expected_id {
                return Err(SnapshotError::SlabIdMismatch {
                    section: "streams.topics",
                    expected: expected_id,
                    actual: actual_id,
                });
            }
            topic_index.insert(topic_name, actual_id);
        }

        Ok(Stream {
            id: self.id,
            name: Arc::from(self.name.as_str()),
            created_at: self.created_at,
            stats: stream_stats,
            topics,
            topic_index,
        })
    }
}

define_state! {
    Streams {
        index: AHashMap<Arc<str>, usize>,
        items: Slab<Stream>,
    }
}

collect_handlers! {
    Streams {
        CreateStream,
        UpdateStream,
        DeleteStream,
        PurgeStream,
        CreateTopic,
        UpdateTopic,
        DeleteTopic,
        PurgeTopic,
        CreatePartitions,
        DeletePartitions,
    }
}

impl StreamsInner {
    fn resolve_stream_id(&self, identifier: &iggy_common::Identifier) -> Option<usize> {
        use iggy_common::IdKind;
        match identifier.kind {
            IdKind::Numeric => {
                let id = identifier.get_u32_value().ok()? as usize;
                if self.items.contains(id) {
                    Some(id)
                } else {
                    None
                }
            }
            IdKind::String => {
                let name = identifier.get_string_value().ok()?;
                self.index.get(name.as_str()).copied()
            }
        }
    }

    fn resolve_topic_id(
        &self,
        stream_id: usize,
        identifier: &iggy_common::Identifier,
    ) -> Option<usize> {
        use iggy_common::IdKind;
        let stream = self.items.get(stream_id)?;

        match identifier.kind {
            IdKind::Numeric => {
                let id = identifier.get_u32_value().ok()? as usize;
                if stream.topics.contains(id) {
                    Some(id)
                } else {
                    None
                }
            }
            IdKind::String => {
                let name = identifier.get_string_value().ok()?;
                stream.topic_index.get(name.as_str()).copied()
            }
        }
    }
}

impl StateHandler for CreateStream {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner) {
        let name_arc: Arc<str> = Arc::from(self.name.as_str());
        if state.index.contains_key(&name_arc) {
            return;
        }

        let stream = Stream {
            id: 0,
            name: name_arc.clone(),
            created_at: iggy_common::IggyTimestamp::now(),
            stats: Arc::new(StreamStats::default()),
            topics: Slab::new(),
            topic_index: AHashMap::default(),
        };

        let id = state.items.insert(stream);
        if let Some(stream) = state.items.get_mut(id) {
            stream.id = id;
        }
        state.index.insert(name_arc, id);
    }
}

impl StateHandler for UpdateStream {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner) {
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return;
        };
        let Some(stream) = state.items.get_mut(stream_id) else {
            return;
        };

        let new_name_arc: Arc<str> = Arc::from(self.name.as_str());
        if let Some(&existing_id) = state.index.get(&new_name_arc)
            && existing_id != stream_id
        {
            return;
        }

        state.index.remove(&stream.name);
        stream.name = new_name_arc.clone();
        state.index.insert(new_name_arc, stream_id);
    }
}

impl StateHandler for DeleteStream {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner) {
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return;
        };

        if let Some(stream) = state.items.get(stream_id) {
            let name = stream.name.clone();

            state.items.remove(stream_id);
            state.index.remove(&name);
        }
    }
}

impl StateHandler for PurgeStream {
    type State = StreamsInner;
    fn apply(&self, _state: &mut StreamsInner) {
        // TODO
        todo!();
    }
}

impl StateHandler for CreateTopic {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner) {
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return;
        };
        let Some(stream) = state.items.get_mut(stream_id) else {
            return;
        };

        let name_arc: Arc<str> = Arc::from(self.name.as_str());
        if stream.topic_index.contains_key(&name_arc) {
            return;
        }

        let topic = Topic {
            id: 0,
            name: name_arc.clone(),
            created_at: iggy_common::IggyTimestamp::now(),
            replication_factor: self.replication_factor.unwrap_or(1),
            message_expiry: self.message_expiry,
            compression_algorithm: self.compression_algorithm,
            max_topic_size: self.max_topic_size,
            stats: Arc::new(TopicStats::new(stream.stats.clone())),
            partitions: Vec::new(),
            round_robin_counter: Arc::new(AtomicUsize::new(0)),
        };

        let topic_id = stream.topics.insert(topic);
        if let Some(topic) = stream.topics.get_mut(topic_id) {
            topic.id = topic_id;

            for partition_id in 0..self.partitions_count as usize {
                let partition = Partition {
                    id: partition_id,
                    created_at: iggy_common::IggyTimestamp::now(),
                };
                topic.partitions.push(partition);
            }
        }

        stream.topic_index.insert(name_arc, topic_id);
    }
}

impl StateHandler for UpdateTopic {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner) {
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return;
        };
        let Some(topic_id) = state.resolve_topic_id(stream_id, &self.topic_id) else {
            return;
        };

        let Some(stream) = state.items.get_mut(stream_id) else {
            return;
        };
        let Some(topic) = stream.topics.get_mut(topic_id) else {
            return;
        };

        let new_name_arc: Arc<str> = Arc::from(self.name.as_str());
        if let Some(&existing_id) = stream.topic_index.get(&new_name_arc)
            && existing_id != topic_id
        {
            return;
        }

        stream.topic_index.remove(&topic.name);
        topic.name = new_name_arc.clone();
        topic.compression_algorithm = self.compression_algorithm;
        topic.message_expiry = self.message_expiry;
        topic.max_topic_size = self.max_topic_size;
        if let Some(rf) = self.replication_factor {
            topic.replication_factor = rf;
        }
        stream.topic_index.insert(new_name_arc, topic_id);
    }
}

impl StateHandler for DeleteTopic {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner) {
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return;
        };
        let Some(topic_id) = state.resolve_topic_id(stream_id, &self.topic_id) else {
            return;
        };
        let Some(stream) = state.items.get_mut(stream_id) else {
            return;
        };

        if let Some(topic) = stream.topics.get(topic_id) {
            let name = topic.name.clone();
            stream.topics.remove(topic_id);
            stream.topic_index.remove(&name);
        }
    }
}

impl StateHandler for PurgeTopic {
    type State = StreamsInner;
    fn apply(&self, _state: &mut StreamsInner) {
        // TODO
        todo!();
    }
}

impl StateHandler for CreatePartitions {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner) {
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return;
        };
        let Some(topic_id) = state.resolve_topic_id(stream_id, &self.topic_id) else {
            return;
        };

        let Some(stream) = state.items.get_mut(stream_id) else {
            return;
        };
        let Some(topic) = stream.topics.get_mut(topic_id) else {
            return;
        };

        let current_partition_count = topic.partitions.len();
        for i in 0..self.partitions_count as usize {
            let partition_id = current_partition_count + i;
            let partition = Partition {
                id: partition_id,
                created_at: iggy_common::IggyTimestamp::now(),
            };
            topic.partitions.push(partition);
        }
    }
}

impl StateHandler for DeletePartitions {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner) {
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return;
        };
        let Some(topic_id) = state.resolve_topic_id(stream_id, &self.topic_id) else {
            return;
        };

        let Some(stream) = state.items.get_mut(stream_id) else {
            return;
        };
        let Some(topic) = stream.topics.get_mut(topic_id) else {
            return;
        };

        let count_to_delete = self.partitions_count as usize;
        if count_to_delete > 0 && count_to_delete <= topic.partitions.len() {
            topic
                .partitions
                .truncate(topic.partitions.len() - count_to_delete);
        }
    }
}

/// Snapshot representation for the Streams state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamsSnapshot {
    pub items: Vec<(usize, StreamSnapshot)>,
}

impl Snapshotable for Streams {
    type Snapshot = StreamsSnapshot;

    fn to_snapshot(&self) -> Self::Snapshot {
        self.snapshot_read(|inner| {
            let items: Vec<(usize, StreamSnapshot)> = inner
                .items
                .iter()
                .map(|(stream_id, stream)| (stream_id, StreamSnapshot::from(stream)))
                .collect();
            StreamsSnapshot { items }
        })
    }

    fn from_snapshot(
        snapshot: Self::Snapshot,
    ) -> Result<Self, crate::stm::snapshot::SnapshotError> {
        use crate::stm::snapshot::SnapshotError;

        let mut items: Slab<Stream> = Slab::new();
        let mut index: AHashMap<Arc<str>, usize> = AHashMap::new();

        for (expected_id, stream_snap) in snapshot.items {
            let stream = stream_snap.into_stream()?;
            let stream_name = stream.name.clone();

            let actual_id = items.insert(stream);
            if actual_id != expected_id {
                return Err(SnapshotError::SlabIdMismatch {
                    section: "streams",
                    expected: expected_id,
                    actual: actual_id,
                });
            }
            index.insert(stream_name, actual_id);
        }

        let inner = StreamsInner { index, items };
        Ok(inner.into())
    }
}

impl_fill_restore!(Streams, streams);
