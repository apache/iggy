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
use crate::{collect_handlers, define_state};
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
use slab::Slab;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

#[derive(Debug, Clone)]
pub struct Partition {
    pub id: usize,
    pub created_at: IggyTimestamp,
}

impl Partition {
    pub fn new(id: usize, created_at: IggyTimestamp) -> Self {
        Self { id, created_at }
    }
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
