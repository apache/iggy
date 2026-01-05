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

use crate::metadata::consumer_group::ConsumerGroupMeta;
use crate::metadata::partition::PartitionMeta;
use crate::metadata::{ConsumerGroupId, SLAB_SEGMENT_SIZE, TopicId};
use crate::streaming::stats::TopicStats;
use iggy_common::collections::SegmentedSlab;
use iggy_common::{CompressionAlgorithm, IggyExpiry, IggyTimestamp, MaxTopicSize};
use imbl::HashMap as ImHashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

/// Topic metadata stored in the shared snapshot.
#[derive(Clone, Debug)]
pub struct TopicMeta {
    pub id: TopicId,
    pub name: Arc<str>,
    pub created_at: IggyTimestamp,
    pub message_expiry: IggyExpiry,
    pub compression_algorithm: CompressionAlgorithm,
    pub max_topic_size: MaxTopicSize,
    pub replication_factor: u8,
    pub partitions_count: u32,
    pub stats: Arc<TopicStats>,
    pub partitions: SegmentedSlab<PartitionMeta, SLAB_SEGMENT_SIZE>,
    pub consumer_groups: SegmentedSlab<ConsumerGroupMeta, SLAB_SEGMENT_SIZE>,
    pub consumer_group_index: ImHashMap<Arc<str>, ConsumerGroupId>,
    pub partition_counter: Arc<AtomicUsize>,
}
