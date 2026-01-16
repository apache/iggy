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
use crate::stm::{Handler, LeftRight};
use crate::{define_state, impl_absorb};
use ahash::AHashMap;
use iggy_common::create_stream::CreateStream;
use iggy_common::delete_stream::DeleteStream;
use iggy_common::purge_stream::PurgeStream;
use iggy_common::update_stream::UpdateStream;
use iggy_common::{CompressionAlgorithm, IggyExpiry, IggyTimestamp, MaxTopicSize};
use left_right::Absorb;
use slab::Slab;
use std::sync::Arc;

// ============================================================================
// Partition Entity
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct Partition {
    pub id: usize,
}

impl Partition {
    pub fn new(id: usize) -> Self {
        Self { id }
    }
}

// ============================================================================
// Partitions Collection
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct Partitions {
    pub items: Slab<Partition>,
}

impl Partitions {
    pub fn new() -> Self {
        Self::default()
    }
}

// ============================================================================
// ConsumerGroup (local to Topic, not a state machine)
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct ConsumerGroup {
    pub id: usize,
    pub name: String,
    pub created_at: IggyTimestamp,
}

impl ConsumerGroup {
    pub fn new(name: String, created_at: IggyTimestamp) -> Self {
        Self {
            id: 0,
            name,
            created_at,
        }
    }
}

// ============================================================================
// ConsumerGroups (local to Topic, simple collection - no left_right)
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct ConsumerGroups {
    index: AHashMap<String, usize>,
    items: Slab<ConsumerGroup>,
}

impl ConsumerGroups {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, _group: ConsumerGroup) -> usize {
        0
    }

    pub fn get(&self, _id: usize) -> Option<ConsumerGroup> {
        None
    }

    pub fn get_by_name(&self, _name: &str) -> Option<ConsumerGroup> {
        None
    }

    pub fn remove(&self, _id: usize) -> Option<ConsumerGroup> {
        None
    }

    pub fn len(&self) -> usize {
        0
    }

    pub fn is_empty(&self) -> bool {
        true
    }
}

// ============================================================================
// Topic Entity
// ============================================================================

#[derive(Debug, Clone)]
pub struct Topic {
    pub id: usize,
    pub name: String,
    pub created_at: IggyTimestamp,
    pub replication_factor: u8,
    pub message_expiry: IggyExpiry,
    pub compression_algorithm: CompressionAlgorithm,
    pub max_topic_size: MaxTopicSize,

    pub stats: Arc<TopicStats>,
    pub partitions: Partitions,
    pub consumer_groups: ConsumerGroups,
}

impl Default for Topic {
    fn default() -> Self {
        Self {
            id: 0,
            name: String::new(),
            created_at: IggyTimestamp::default(),
            replication_factor: 1,
            message_expiry: IggyExpiry::default(),
            compression_algorithm: CompressionAlgorithm::default(),
            max_topic_size: MaxTopicSize::default(),
            stats: Arc::new(TopicStats::default()),
            partitions: Partitions::new(),
            consumer_groups: ConsumerGroups::new(),
        }
    }
}

impl Topic {
    pub fn new(
        name: String,
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
            partitions: Partitions::new(),
            consumer_groups: ConsumerGroups::new(),
        }
    }
}

// ============================================================================
// Topics Collection
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct Topics {
    pub index: AHashMap<String, usize>,
    pub items: Slab<Topic>,
}

impl Topics {
    pub fn new() -> Self {
        Self::default()
    }
}

// ============================================================================
// Stream Entity
// ============================================================================

#[derive(Debug)]
pub struct Stream {
    pub id: usize,
    pub name: String,
    pub created_at: IggyTimestamp,

    pub stats: Arc<StreamStats>,
    pub topics: Topics,
}

impl Default for Stream {
    fn default() -> Self {
        Self {
            id: 0,
            name: String::new(),
            created_at: IggyTimestamp::default(),
            stats: Arc::new(StreamStats::default()),
            topics: Topics::new(),
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
        }
    }
}

impl Stream {
    pub fn new(name: String, created_at: IggyTimestamp) -> Self {
        Self {
            id: 0,
            name,
            created_at,
            stats: Arc::new(StreamStats::default()),
            topics: Topics::new(),
        }
    }
}

fn foo() {
    let streams_inner = StreamsInner {
        index: AHashMap::new(),
        items: Slab::new(),
    };

    let streams: LeftRight<StreamsInner, StreamsCommand> = streams_inner.into();
    let streams_2: Streams<LeftRight<StreamsInner, StreamsCommand>> = streams.into();
}

define_state! {
    Streams,
    StreamsInner {
        index: AHashMap<String, usize>,
        items: Slab<Stream>,
    },
    StreamsCommand,
    [CreateStream, UpdateStream, DeleteStream, PurgeStream]
}
impl_absorb!(StreamsInner, StreamsCommand);

impl Handler for StreamsInner {
    fn handle(&mut self, cmd: &StreamsCommand) {
        match cmd {
            StreamsCommand::CreateStream(_payload) => {
                // Actual mutation logic will be implemented later
            }
            StreamsCommand::UpdateStream(_payload) => {
                // Actual mutation logic will be implemented later
            }
            StreamsCommand::DeleteStream(_payload) => {
                // Actual mutation logic will be implemented later
            }
            StreamsCommand::PurgeStream(_payload) => {
                // Actual mutation logic will be implemented later
            }
        }
    }
}
