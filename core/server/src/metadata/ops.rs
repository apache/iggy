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

use crate::metadata::inner::InnerMetadata;
use crate::metadata::{
    ConsumerGroupId, ConsumerGroupMeta, PartitionId, PartitionMeta, StreamId, StreamMeta, TopicId,
    TopicMeta, UserId, UserMeta,
};
use crate::streaming::partitions::consumer_group_offsets::ConsumerGroupOffsets;
use crate::streaming::partitions::consumer_offsets::ConsumerOffsets;
use iggy_common::{CompressionAlgorithm, IggyExpiry, MaxTopicSize, PersonalAccessToken};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

#[derive(Clone)]
pub enum MetadataOp {
    Initialize(Box<InnerMetadata>),

    AddStream {
        meta: StreamMeta,
        assigned_id: Arc<AtomicUsize>,
    },
    UpdateStream {
        id: StreamId,
        new_name: Arc<str>,
    },
    DeleteStream {
        id: StreamId,
    },
    AddTopic {
        stream_id: StreamId,
        meta: TopicMeta,
        assigned_id: Arc<AtomicUsize>,
    },
    UpdateTopic {
        stream_id: StreamId,
        topic_id: TopicId,
        new_name: Arc<str>,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: u8,
    },
    DeleteTopic {
        stream_id: StreamId,
        topic_id: TopicId,
    },
    AddPartitions {
        stream_id: StreamId,
        topic_id: TopicId,
        partitions: Vec<PartitionMeta>,
        revision_id: u64,
    },
    DeletePartitions {
        stream_id: StreamId,
        topic_id: TopicId,
        count: u32,
    },
    SetPartitionOffsets {
        stream_id: StreamId,
        topic_id: TopicId,
        partition_id: PartitionId,
        consumer_offsets: Arc<ConsumerOffsets>,
        consumer_group_offsets: Arc<ConsumerGroupOffsets>,
    },
    AddUser {
        meta: UserMeta,
        assigned_id: Arc<AtomicUsize>,
    },
    UpdateUserMeta {
        id: UserId,
        meta: UserMeta,
    },
    DeleteUser {
        id: UserId,
    },

    AddPersonalAccessToken {
        user_id: UserId,
        pat: PersonalAccessToken,
    },
    DeletePersonalAccessToken {
        user_id: UserId,
        token_hash: Arc<str>,
    },
    AddConsumerGroup {
        stream_id: StreamId,
        topic_id: TopicId,
        meta: ConsumerGroupMeta,
        assigned_id: Arc<AtomicUsize>,
    },
    DeleteConsumerGroup {
        stream_id: StreamId,
        topic_id: TopicId,
        group_id: ConsumerGroupId,
    },
    JoinConsumerGroup {
        stream_id: StreamId,
        topic_id: TopicId,
        group_id: ConsumerGroupId,
        client_id: u32,
        member_id: Arc<AtomicUsize>,
    },
    LeaveConsumerGroup {
        stream_id: StreamId,
        topic_id: TopicId,
        group_id: ConsumerGroupId,
        client_id: u32,
        removed_member_id: Arc<AtomicUsize>,
    },
    RebalanceConsumerGroupsForTopic {
        stream_id: StreamId,
        topic_id: TopicId,
        partitions_count: u32,
    },
}
