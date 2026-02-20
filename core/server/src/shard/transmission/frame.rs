/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
use crate::{
    shard::transmission::message::ShardMessage,
    streaming::{segments::IggyMessagesBatchSet, users::user::User},
};
use async_channel::Sender;
use iggy_common::{
    CompressionAlgorithm, IggyError, IggyExpiry, IggyPollMetadata, IggyTimestamp, MaxTopicSize,
    PersonalAccessToken, Stats,
};
use std::sync::Arc;

/// Data needed to construct a stream creation response.
#[derive(Debug)]
pub struct StreamResponseData {
    pub id: u32,
    pub name: Arc<str>,
    pub created_at: IggyTimestamp,
}

/// Data needed to construct a topic creation response.
#[derive(Debug)]
pub struct TopicResponseData {
    pub id: u32,
    pub name: Arc<str>,
    pub created_at: IggyTimestamp,
    pub partitions: Vec<super::event::PartitionInfo>,
    pub message_expiry: IggyExpiry,
    pub compression_algorithm: CompressionAlgorithm,
    pub max_topic_size: MaxTopicSize,
    pub replication_factor: u8,
}

/// Data needed to construct a consumer group creation response.
#[derive(Debug)]
pub struct ConsumerGroupResponseData {
    pub id: u32,
    pub name: Arc<str>,
    pub partitions_count: u32,
}

// TODO: make nice types in common module so that each command has respective *Response struct, i.e. CreateStream -> CreateStreamResponse
#[derive(Debug)]
pub enum ShardResponse {
    PollMessages((IggyPollMetadata, IggyMessagesBatchSet)),
    SendMessages,
    FlushUnsavedBuffer {
        flushed_count: u32,
    },
    DeleteSegments {
        deleted_segments: u64,
        deleted_messages: u64,
    },
    CleanTopicMessages {
        deleted_segments: u64,
        deleted_messages: u64,
    },
    Event,
    CreateStreamResponse(StreamResponseData),
    DeleteStreamResponse(usize),
    CreateTopicResponse(TopicResponseData),
    UpdateTopicResponse,
    DeleteTopicResponse(usize),
    CreateUserResponse(User),
    DeleteUserResponse(User),
    GetStatsResponse(Stats),
    CreatePartitionsResponse(Vec<usize>),
    DeletePartitionsResponse(Vec<usize>),
    UpdateStreamResponse,
    SocketTransferResponse,
    UpdatePermissionsResponse,
    ChangePasswordResponse,
    UpdateUserResponse(User),
    CreateConsumerGroupResponse(ConsumerGroupResponseData),
    JoinConsumerGroupResponse,
    LeaveConsumerGroupResponse,
    DeleteConsumerGroupResponse,
    CreatePersonalAccessTokenResponse(PersonalAccessToken, String),
    DeletePersonalAccessTokenResponse,
    LeaveConsumerGroupMetadataOnlyResponse,
    CompletePartitionRevocationResponse,
    PurgeStreamResponse,
    PurgeTopicResponse,
    ErrorResponse(IggyError),
}

#[derive(Debug)]
pub struct ShardFrame {
    pub message: ShardMessage,
    pub response_sender: Option<Sender<ShardResponse>>,
}

impl ShardFrame {
    pub fn new(message: ShardMessage, response_sender: Option<Sender<ShardResponse>>) -> Self {
        Self {
            message,
            response_sender,
        }
    }
}
