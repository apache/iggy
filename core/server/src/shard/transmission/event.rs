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

use iggy_common::{Identifier, IggyTimestamp, TransportProtocol};
use std::net::SocketAddr;
use strum::Display;

/// Minimal partition info for event broadcasting (no slab dependency)
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub id: usize,
    pub created_at: IggyTimestamp,
}

/// Events that require broadcasting between shards.
///
/// Note: Metadata events (CreatedStream, DeletedStream, CreatedTopic, DeletedTopic,
/// UpdatedStream, UpdatedTopic, CreatedConsumerGroup, DeletedConsumerGroup) are NOT
/// broadcast because SharedMetadata is already visible to all shards via LeftRight.
/// Only events that require per-shard local actions are broadcast.
#[derive(Debug, Clone, Display)]
#[strum(serialize_all = "PascalCase")]
pub enum ShardEvent {
    /// Flush unsaved buffer to disk for a specific partition
    FlushUnsavedBuffer {
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: usize,
        fsync: bool,
    },
    /// Purge all messages, consumer groups and consumer group offsets from a topic
    PurgedTopic {
        stream_id: Identifier,
        topic_id: Identifier,
    },
    /// Purges all topics in a stream
    PurgedStream { stream_id: Identifier },
    /// New partitions created (requires per-shard log initialization)
    CreatedPartitions {
        stream_id: Identifier,
        topic_id: Identifier,
        partitions: Vec<PartitionInfo>,
    },
    /// Partitions deleted (requires per-shard log cleanup)
    DeletedPartitions {
        stream_id: Identifier,
        topic_id: Identifier,
        partitions_count: u32,
        partition_ids: Vec<usize>,
    },
    /// Transport address bound (for config file writing)
    AddressBound {
        protocol: TransportProtocol,
        address: SocketAddr,
    },
}
