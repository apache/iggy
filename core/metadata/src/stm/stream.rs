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
use crate::stm::Handler;
use crate::{define_state, impl_absorb};
use ahash::AHashMap;
use iggy_common::create_stream::CreateStream;
use iggy_common::delete_stream::DeleteStream;
use iggy_common::purge_stream::PurgeStream;
use iggy_common::update_stream::UpdateStream;
use iggy_common::{CompressionAlgorithm, IggyExpiry, IggyTimestamp, MaxTopicSize};
use slab::Slab;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

// ============================================================================
// Partition Entity
// ============================================================================

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

// ============================================================================
// Topic Entity
// ============================================================================

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

// ============================================================================
// Stream Entity
// ============================================================================

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

// ============================================================================
// Streams State Machine
// ============================================================================

define_state! {
    Streams {
        index: AHashMap<Arc<str>, usize>,
        items: Slab<Stream>,
    },
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
