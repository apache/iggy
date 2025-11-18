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
    shard::{
        system::messages::PollingArgs,
        transmission::{event::ShardEvent, frame::ShardResponse},
    },
    slab::partitions,
    streaming::{polling_consumer::PollingConsumer, segments::IggyMessagesBatchMut},
};
use iggy_common::{
    CompressionAlgorithm, Identifier, IggyExpiry, MaxTopicSize, Permissions, UserStatus,
};

#[allow(clippy::large_enum_variant)]
pub enum ShardSendRequestResult {
    // TODO: In the future we can add other variants, for example backpressure from the destination shard,
    Recoil(ShardMessage),
    Response(ShardResponse),
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum ShardMessage {
    Request(ShardRequest),
    Event(ShardEvent),
}

#[derive(Debug)]
pub struct ShardRequest {
    pub stream_id: Identifier,
    pub topic_id: Identifier,
    pub partition_id: usize,
    pub payload: ShardRequestPayload,
}

impl ShardRequest {
    pub fn new(
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: partitions::ContainerId,
        payload: ShardRequestPayload,
    ) -> Self {
        Self {
            stream_id,
            topic_id,
            partition_id,
            payload,
        }
    }
}

#[derive(Debug)]
pub enum ShardRequestPayload {
    SendMessages {
        batch: IggyMessagesBatchMut,
    },
    PollMessages {
        consumer: PollingConsumer,
        args: PollingArgs,
    },
    FlushUnsavedBuffer {
        fsync: bool,
    },
    CreateStream {
        user_id: u32,
        name: String,
    },
    DeleteStream {
        user_id: u32,
        stream_id: Identifier,
    },
    CreateTopic {
        user_id: u32,
        stream_id: Identifier,
        name: String,
        partitions_count: u32,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    },
    UpdateTopic {
        user_id: u32,
        stream_id: Identifier,
        topic_id: Identifier,
        name: String,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    },
    DeleteTopic {
        user_id: u32,
        stream_id: Identifier,
        topic_id: Identifier,
    },
    CreateUser {
        user_id: u32,
        username: String,
        password: String,
        status: UserStatus,
        permissions: Option<Permissions>,
    },
    DeleteUser {
        session_user_id: u32,
        user_id: Identifier,
    },
    GetStats {
        user_id: u32,
    },
    DeleteSegments {
        segments_count: u32,
    },
}

impl From<ShardRequest> for ShardMessage {
    fn from(request: ShardRequest) -> Self {
        ShardMessage::Request(request)
    }
}

impl From<ShardEvent> for ShardMessage {
    fn from(event: ShardEvent) -> Self {
        ShardMessage::Event(event)
    }
}
