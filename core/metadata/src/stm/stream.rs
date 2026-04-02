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
use bytes::Bytes;
use iggy_binary_protocol::WireIdentifier;
use iggy_binary_protocol::requests::partitions::{
    CreatePartitionsRequest, DeletePartitionsRequest,
};
use iggy_binary_protocol::requests::streams::{
    CreateStreamRequest, DeleteStreamRequest, PurgeStreamRequest, UpdateStreamRequest,
};
use iggy_binary_protocol::requests::topics::{
    CreateTopicRequest, DeleteTopicRequest, PurgeTopicRequest, UpdateTopicRequest,
};
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

#[derive(Debug, Clone)]
pub struct Partition {
    pub id: usize,
    pub created_at: IggyTimestamp,
}

impl Partition {
    #[must_use]
    pub const fn new(id: usize, created_at: IggyTimestamp) -> Self {
        Self { id, created_at }
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
}

// TODO(hubcio): Serialize proper reply (e.g. assigned stream ID) instead of empty Bytes.
impl StateHandler for CreateStreamRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner) -> Bytes {
        let name_arc: Arc<str> = Arc::from(self.name.as_str());
        if state.index.contains_key(&name_arc) {
            return Bytes::new();
        }

        let stream = Stream {
            id: 0,
            name: name_arc.clone(),
            created_at: IggyTimestamp::now(),
            stats: Arc::new(StreamStats::default()),
            topics: Slab::new(),
            topic_index: AHashMap::default(),
        };

        let id = state.items.insert(stream);
        if let Some(stream) = state.items.get_mut(id) {
            stream.id = id;
        }
        state.index.insert(name_arc, id);
        Bytes::new()
    }
}

impl StateHandler for UpdateStreamRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner) -> Bytes {
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return Bytes::new();
        };
        let Some(stream) = state.items.get_mut(stream_id) else {
            return Bytes::new();
        };

        let new_name_arc: Arc<str> = Arc::from(self.name.as_str());
        if let Some(&existing_id) = state.index.get(&new_name_arc)
            && existing_id != stream_id
        {
            return Bytes::new();
        }

        state.index.remove(&stream.name);
        stream.name = new_name_arc.clone();
        state.index.insert(new_name_arc, stream_id);
        Bytes::new()
    }
}

impl StateHandler for DeleteStreamRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner) -> Bytes {
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return Bytes::new();
        };

        if let Some(stream) = state.items.get(stream_id) {
            let name = stream.name.clone();

            state.items.remove(stream_id);
            state.index.remove(&name);
        }
        Bytes::new()
    }
}

impl StateHandler for PurgeStreamRequest {
    type State = StreamsInner;
    fn apply(&self, _state: &mut StreamsInner) -> Bytes {
        // TODO
        todo!();
    }
}

// TODO(hubcio): Serialize proper reply (e.g. assigned topic ID) instead of empty Bytes.
impl StateHandler for CreateTopicRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner) -> Bytes {
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return Bytes::new();
        };
        let Some(stream) = state.items.get_mut(stream_id) else {
            return Bytes::new();
        };

        let name_arc: Arc<str> = Arc::from(self.name.as_str());
        if stream.topic_index.contains_key(&name_arc) {
            return Bytes::new();
        }

        let replication_factor = if self.replication_factor == 0 {
            1
        } else {
            self.replication_factor
        };

        let topic = Topic {
            id: 0,
            name: name_arc.clone(),
            created_at: IggyTimestamp::now(),
            replication_factor,
            message_expiry: IggyExpiry::from(self.message_expiry),
            compression_algorithm: CompressionAlgorithm::from_code(self.compression_algorithm)
                .unwrap_or_default(),
            max_topic_size: MaxTopicSize::from(self.max_topic_size),
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
                    created_at: IggyTimestamp::now(),
                };
                topic.partitions.push(partition);
            }
        }

        stream.topic_index.insert(name_arc, topic_id);
        Bytes::new()
    }
}

impl StateHandler for UpdateTopicRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner) -> Bytes {
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return Bytes::new();
        };
        let Some(topic_id) = state.resolve_topic_id(stream_id, &self.topic_id) else {
            return Bytes::new();
        };

        let Some(stream) = state.items.get_mut(stream_id) else {
            return Bytes::new();
        };
        let Some(topic) = stream.topics.get_mut(topic_id) else {
            return Bytes::new();
        };

        let new_name_arc: Arc<str> = Arc::from(self.name.as_str());
        if let Some(&existing_id) = stream.topic_index.get(&new_name_arc)
            && existing_id != topic_id
        {
            return Bytes::new();
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
        Bytes::new()
    }
}

impl StateHandler for DeleteTopicRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner) -> Bytes {
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return Bytes::new();
        };
        let Some(topic_id) = state.resolve_topic_id(stream_id, &self.topic_id) else {
            return Bytes::new();
        };
        let Some(stream) = state.items.get_mut(stream_id) else {
            return Bytes::new();
        };

        if let Some(topic) = stream.topics.get(topic_id) {
            let name = topic.name.clone();
            stream.topics.remove(topic_id);
            stream.topic_index.remove(&name);
        }
        Bytes::new()
    }
}

impl StateHandler for PurgeTopicRequest {
    type State = StreamsInner;
    fn apply(&self, _state: &mut StreamsInner) -> Bytes {
        // TODO
        todo!();
    }
}

// TODO(hubcio): Serialize proper reply (e.g. assigned partition IDs) instead of empty Bytes.
impl StateHandler for CreatePartitionsRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner) -> Bytes {
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return Bytes::new();
        };
        let Some(topic_id) = state.resolve_topic_id(stream_id, &self.topic_id) else {
            return Bytes::new();
        };

        let Some(stream) = state.items.get_mut(stream_id) else {
            return Bytes::new();
        };
        let Some(topic) = stream.topics.get_mut(topic_id) else {
            return Bytes::new();
        };

        let current_partition_count = topic.partitions.len();
        for i in 0..self.partitions_count as usize {
            let partition_id = current_partition_count + i;
            let partition = Partition {
                id: partition_id,
                created_at: IggyTimestamp::now(),
            };
            topic.partitions.push(partition);
        }
        Bytes::new()
    }
}

impl StateHandler for DeletePartitionsRequest {
    type State = StreamsInner;
    fn apply(&self, state: &mut StreamsInner) -> Bytes {
        let Some(stream_id) = state.resolve_stream_id(&self.stream_id) else {
            return Bytes::new();
        };
        let Some(topic_id) = state.resolve_topic_id(stream_id, &self.topic_id) else {
            return Bytes::new();
        };

        let Some(stream) = state.items.get_mut(stream_id) else {
            return Bytes::new();
        };
        let Some(topic) = stream.topics.get_mut(topic_id) else {
            return Bytes::new();
        };

        let count_to_delete = self.partitions_count as usize;
        if count_to_delete > 0 && count_to_delete <= topic.partitions.len() {
            topic
                .partitions
                .truncate(topic.partitions.len() - count_to_delete);
        }
        Bytes::new()
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
                                            created_at: p.created_at,
                                        })
                                        .collect(),
                                    round_robin_counter: topic
                                        .round_robin_counter
                                        .load(Ordering::Relaxed),
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
            StreamsSnapshot { items }
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
                            created_at: p.created_at,
                        })
                        .collect(),
                    round_robin_counter: Arc::new(AtomicUsize::new(topic_snap.round_robin_counter)),
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
        let inner = StreamsInner {
            index,
            items,
            last_result: None,
        };
        Ok(inner.into())
    }
}

impl_fill_restore!(Streams, streams);
