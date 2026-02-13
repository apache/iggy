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
    shard::{system::messages::PollingArgs, transmission::event::ShardEvent},
    streaming::{polling_consumer::PollingConsumer, segments::IggyMessagesBatchMut},
};
use iggy_common::{
    change_password::ChangePassword, create_consumer_group::CreateConsumerGroup,
    create_partitions::CreatePartitions, create_personal_access_token::CreatePersonalAccessToken,
    create_stream::CreateStream, create_topic::CreateTopic, create_user::CreateUser,
    delete_consumer_group::DeleteConsumerGroup, delete_partitions::DeletePartitions,
    delete_personal_access_token::DeletePersonalAccessToken, delete_stream::DeleteStream,
    delete_topic::DeleteTopic, delete_user::DeleteUser, join_consumer_group::JoinConsumerGroup,
    leave_consumer_group::LeaveConsumerGroup, purge_stream::PurgeStream, purge_topic::PurgeTopic,
    sharding::IggyNamespace, update_permissions::UpdatePermissions, update_stream::UpdateStream,
    update_topic::UpdateTopic, update_user::UpdateUser,
};

use std::{net::SocketAddr, os::fd::OwnedFd};

/// Resolved stream ID. Contains only the numeric ID - `Identifier` stays at handler boundary.
#[derive(Debug, Clone, Copy)]
pub struct ResolvedStream(pub usize);

impl ResolvedStream {
    pub fn id(self) -> usize {
        self.0
    }
}

/// Resolved topic with parent stream context.
#[derive(Debug, Clone, Copy)]
pub struct ResolvedTopic {
    pub stream_id: usize,
    pub topic_id: usize,
}

/// Resolved partition with full context.
#[derive(Debug, Clone, Copy)]
pub struct ResolvedPartition {
    pub stream_id: usize,
    pub topic_id: usize,
    pub partition_id: usize,
}

/// Resolved consumer group with full context.
#[derive(Debug, Clone, Copy)]
pub struct ResolvedConsumerGroup {
    pub stream_id: usize,
    pub topic_id: usize,
    pub group_id: usize,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum ShardMessage {
    Request(ShardRequest),
    Event(ShardEvent),
}

/// Routing envelope determining which shard handles the request.
#[derive(Debug)]
pub struct ShardRequest {
    /// None = shard 0 (control-plane), Some = partition owner (data-plane)
    pub routing: Option<IggyNamespace>,
    pub payload: ShardRequestPayload,
}

impl ShardRequest {
    /// Control-plane operations always route to shard 0
    pub fn control_plane(payload: ShardRequestPayload) -> Self {
        Self {
            routing: None,
            payload,
        }
    }

    /// Data-plane operations route by partition namespace
    pub fn data_plane(namespace: IggyNamespace, payload: ShardRequestPayload) -> Self {
        Self {
            routing: Some(namespace),
            payload,
        }
    }
}

#[derive(Debug)]
pub enum ShardRequestPayload {
    // Data-plane operations: namespace provided via ShardRequest
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
    DeleteSegments {
        segments_count: u32,
    },
    CleanTopicMessages {
        stream_id: usize,
        topic_id: usize,
        partition_ids: Vec<usize>,
    },
    SocketTransfer {
        fd: OwnedFd,
        from_shard: u16,
        client_id: u32,
        user_id: u32,
        address: SocketAddr,
        initial_data: IggyMessagesBatchMut,
    },

    // Control-plane: stream operations
    CreateStreamRequest {
        user_id: u32,
        command: CreateStream,
    },
    UpdateStreamRequest {
        user_id: u32,
        command: UpdateStream,
    },
    DeleteStreamRequest {
        user_id: u32,
        command: DeleteStream,
    },
    PurgeStreamRequest {
        user_id: u32,
        command: PurgeStream,
    },

    // Control-plane: topic operations
    CreateTopicRequest {
        user_id: u32,
        command: CreateTopic,
    },
    UpdateTopicRequest {
        user_id: u32,
        command: UpdateTopic,
    },
    DeleteTopicRequest {
        user_id: u32,
        command: DeleteTopic,
    },
    PurgeTopicRequest {
        user_id: u32,
        command: PurgeTopic,
    },

    // Control-plane: partition operations
    CreatePartitionsRequest {
        user_id: u32,
        command: CreatePartitions,
    },
    DeletePartitionsRequest {
        user_id: u32,
        command: DeletePartitions,
    },

    // Control-plane: user operations
    CreateUserRequest {
        user_id: u32,
        command: CreateUser,
    },
    UpdateUserRequest {
        user_id: u32,
        command: UpdateUser,
    },
    DeleteUserRequest {
        user_id: u32,
        command: DeleteUser,
    },
    UpdatePermissionsRequest {
        user_id: u32,
        command: UpdatePermissions,
    },
    ChangePasswordRequest {
        user_id: u32,
        command: ChangePassword,
    },

    // Control-plane: consumer group operations
    CreateConsumerGroupRequest {
        user_id: u32,
        command: CreateConsumerGroup,
    },
    DeleteConsumerGroupRequest {
        user_id: u32,
        command: DeleteConsumerGroup,
    },
    JoinConsumerGroupRequest {
        user_id: u32,
        client_id: u32,
        command: JoinConsumerGroup,
    },
    LeaveConsumerGroupRequest {
        user_id: u32,
        client_id: u32,
        command: LeaveConsumerGroup,
    },
    LeaveConsumerGroupMetadataOnly {
        stream_id: usize,
        topic_id: usize,
        group_id: usize,
        client_id: u32,
    },
    CompletePartitionRevocation {
        stream_id: usize,
        topic_id: usize,
        group_id: usize,
        member_slab_id: usize,
        member_id: usize,
        partition_id: usize,
        timed_out: bool,
    },

    // Control-plane: PAT operations
    CreatePersonalAccessTokenRequest {
        user_id: u32,
        command: CreatePersonalAccessToken,
    },
    DeletePersonalAccessTokenRequest {
        user_id: u32,
        command: DeletePersonalAccessToken,
    },

    // Control-plane: stats
    GetStats {
        user_id: u32,
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
