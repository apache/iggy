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

use std::collections::HashMap;

use iggy::prelude;
use rmcp::schemars::{self, JsonSchema};
use serde::Deserialize;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetStream {
    #[schemars(description = "stream identifier (name or number)")]
    pub stream_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreateStream {
    #[schemars(description = "stream name (required, must be unique)")]
    pub name: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct UpdateStream {
    #[schemars(description = "stream identifier (name or number)")]
    pub stream_id: String,
    #[schemars(description = "stream name (required, must be unique)")]
    pub name: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DeleteStream {
    #[schemars(description = "stream identifier (name or number)")]
    pub stream_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct PurgeStream {
    #[schemars(description = "stream identifier (name or number)")]
    pub stream_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetTopics {
    #[schemars(description = "stream identifier (name or number)")]
    pub stream_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetTopic {
    #[schemars(description = "stream identifier (name or number)")]
    pub stream_id: String,

    #[schemars(description = "topic identifier (name or number)")]
    pub topic_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreateTopic {
    #[schemars(description = "stream identifier (name or number)")]
    pub stream_id: String,

    #[schemars(description = "name (required, must be unique)")]
    pub name: String,

    #[schemars(description = "partitions count (required, must be greater than 0)")]
    pub partitions_count: u32,

    #[schemars(description = "compression algorithm (optional, can be one of 'none', 'gzip')")]
    pub compression_algorithm: Option<String>,

    #[schemars(description = "replication factor (optional, must be greater than 0)")]
    pub replication_factor: Option<u8>,

    #[schemars(description = "message expiry (optional)")]
    pub message_expiry: Option<String>,

    #[schemars(description = "maximum size (optional)")]
    pub max_size: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct UpdateTopic {
    #[schemars(description = "stream identifier (name or number)")]
    pub stream_id: String,

    #[schemars(description = "topic identifier (name or number)")]
    pub topic_id: String,

    #[schemars(description = "name (required, must be unique)")]
    pub name: String,

    #[schemars(description = "compression algorithm (optional, can be one of 'none', 'gzip')")]
    pub compression_algorithm: Option<String>,

    #[schemars(description = "replication factor (optional, must be greater than 0)")]
    pub replication_factor: Option<u8>,

    #[schemars(description = "message expiry (optional)")]
    pub message_expiry: Option<String>,

    #[schemars(description = "maximum size (optional)")]
    pub max_size: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DeleteTopic {
    #[schemars(description = "stream identifier (name or number)")]
    pub stream_id: String,

    #[schemars(description = "topic identifier (name or number)")]
    pub topic_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct PurgeTopic {
    #[schemars(description = "stream identifier (name or number)")]
    pub stream_id: String,

    #[schemars(description = "topic identifier (name or number)")]
    pub topic_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreatePartitions {
    #[schemars(description = "stream identifier (name or number)")]
    pub stream_id: String,

    #[schemars(description = "topic identifier (name or number)")]
    pub topic_id: String,

    #[schemars(description = "partitions count (required, must be greater than 0)")]
    pub partitions_count: u32,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DeletePartitions {
    #[schemars(description = "stream identifier (name or number)")]
    pub stream_id: String,

    #[schemars(description = "topic identifier (name or number)")]
    pub topic_id: String,

    #[schemars(description = "partitions count (required, must be greater than 0)")]
    pub partitions_count: u32,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DeleteSegments {
    #[schemars(description = "stream identifier (name or number)")]
    pub stream_id: String,

    #[schemars(description = "topic identifier (name or number)")]
    pub topic_id: String,

    #[schemars(description = "partition identifier (number)")]
    pub partition_id: u32,

    #[schemars(description = "segments count (required, must be greater than 0)")]
    pub segments_count: u32,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct PollMessages {
    #[schemars(description = "stream identifier (name or number)")]
    pub stream_id: String,

    #[schemars(description = "topic identifier (name or number)")]
    pub topic_id: String,

    #[schemars(description = "partition identifier (optional, number)")]
    pub partition_id: Option<u32>,

    #[schemars(description = "strategy (optional, string)")]
    pub strategy: Option<String>,

    #[schemars(description = "offset to start from (optional)")]
    pub offset: Option<u64>,

    #[schemars(description = "timestamp to start from (optional, microseconds from Unix Epoch)")]
    pub timestamp: Option<u64>,

    #[schemars(description = "count (optional, must be greater than 0)")]
    pub count: Option<u32>,

    #[schemars(description = "auto commit (optional, boolean)")]
    pub auto_commit: Option<bool>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SendMessages {
    #[schemars(description = "stream identifier (name or number)")]
    pub stream_id: String,

    #[schemars(description = "topic identifier (name or number)")]
    pub topic_id: String,

    #[schemars(description = "partition identifier (optional, number)")]
    pub partition_id: Option<u32>,

    #[schemars(description = "partitioning (optional, string)")]
    pub partitioning: Option<String>,

    #[schemars(description = "messages key (optional, string)")]
    pub messages_key: Option<String>,

    #[schemars(description = "messages collection")]
    pub messages: Vec<Message>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct Message {
    #[schemars(description = "message identifier (optional, number)")]
    pub id: Option<u128>,

    #[schemars(description = "message payload, base64 encoded string")]
    pub payload: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetClient {
    #[schemars(description = "client identifier (number)")]
    pub client_id: u32,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct Snapshot {
    #[schemars(description = "compression (optional, string)")]
    pub compression: Option<String>,

    #[schemars(description = "types")]
    pub types: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetConsumerGroup {
    #[schemars(description = "stream identifier (name or number)")]
    pub stream_id: String,

    #[schemars(description = "topic identifier (name or number)")]
    pub topic_id: String,

    #[schemars(description = "consumer group identifier (name or number)")]
    pub group_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetConsumerGroups {
    #[schemars(description = "stream identifier (name or number)")]
    pub stream_id: String,

    #[schemars(description = "topic identifier (name or number)")]
    pub topic_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreateConsumerGroup {
    #[schemars(description = "stream identifier (name or number)")]
    pub stream_id: String,

    #[schemars(description = "topic identifier (name or number)")]
    pub topic_id: String,

    #[schemars(description = "consumer group name (required, must be unique)")]
    pub name: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DeleteConsumerGroup {
    #[schemars(description = "stream identifier (name or number)")]
    pub stream_id: String,

    #[schemars(description = "topic identifier (name or number)")]
    pub topic_id: String,

    #[schemars(description = "consumer group identifier (name or number)")]
    pub group_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetConsumerOffset {
    #[schemars(description = "stream identifier (name or number)")]
    pub stream_id: String,

    #[schemars(description = "topic identifier (name or number)")]
    pub topic_id: String,

    #[schemars(description = "partition identifier (optional, number)")]
    pub partition_id: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct StoreConsumerOffset {
    #[schemars(description = "stream identifier (name or number)")]
    pub stream_id: String,

    #[schemars(description = "topic identifier (name or number)")]
    pub topic_id: String,

    #[schemars(description = "partition identifier (optional, number)")]
    pub partition_id: Option<u32>,

    #[schemars(description = "offset (required, number)")]
    pub offset: u64,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DeleteConsumerOffset {
    #[schemars(description = "stream identifier (name or number)")]
    pub stream_id: String,

    #[schemars(description = "topic identifier (name or number)")]
    pub topic_id: String,

    #[schemars(description = "partition identifier (optional, number)")]
    pub partition_id: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreatePersonalAccessToken {
    #[schemars(description = "personal access token name")]
    pub name: String,

    #[schemars(description = "personal access token expiry(optional)")]
    pub expiry: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DeletePersonalAccessToken {
    #[schemars(description = "personal access token name")]
    pub name: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetUser {
    #[schemars(description = "user identifier (name or number)")]
    pub user_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreateUser {
    #[schemars(description = "username")]
    pub username: String,

    #[schemars(description = "password")]
    pub password: String,

    #[schemars(description = "active (optional)")]
    pub active: Option<bool>,

    #[schemars(description = "permissions (optional)")]
    pub permissions: Option<Permissions>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct UpdateUser {
    #[schemars(description = "user identifier (name or number)")]
    pub user_id: String,

    #[schemars(description = "username (optional)")]
    pub username: Option<String>,

    #[schemars(description = "active (optional)")]
    pub active: Option<bool>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct UpdatePermissions {
    #[schemars(description = "user identifier (name or number)")]
    pub user_id: String,

    #[schemars(description = "permissions (optional)")]
    pub permissions: Option<Permissions>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DeleteUser {
    #[schemars(description = "user identifier (name or number)")]
    pub user_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ChangePassword {
    #[schemars(description = "user identifier (name or number)")]
    pub user_id: String,

    #[schemars(description = "current password")]
    pub current_password: String,

    #[schemars(description = "new password")]
    pub new_password: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct Permissions {
    #[schemars(description = "global permissions (optional)")]
    pub global: Option<GlobalPermissions>,

    #[schemars(description = "stream permissions (optional)")]
    pub streams: Option<HashMap<usize, StreamPermissions>>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GlobalPermissions {
    #[schemars(description = "manage servers (optional)")]
    pub manage_servers: Option<bool>,

    #[schemars(description = "read servers (optional)")]
    pub read_servers: Option<bool>,

    #[schemars(description = "manage users (optional)")]
    pub manage_users: Option<bool>,

    #[schemars(description = "read users (optional)")]
    pub read_users: Option<bool>,

    #[schemars(description = "manage streams (optional)")]
    pub manage_streams: Option<bool>,

    #[schemars(description = "read streams (optional)")]
    pub read_streams: Option<bool>,

    #[schemars(description = "manage topics (optional)")]
    pub manage_topics: Option<bool>,

    #[schemars(description = "read topics (optional)")]
    pub read_topics: Option<bool>,

    #[schemars(description = "poll messages (optional)")]
    pub poll_messages: Option<bool>,

    #[schemars(description = "send messages (optional)")]
    pub send_messages: Option<bool>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct StreamPermissions {
    #[schemars(description = "manage stream (optional)")]
    pub manage_stream: Option<bool>,

    #[schemars(description = "read stream (optional)")]
    pub read_stream: Option<bool>,

    #[schemars(description = "manage topics (optional)")]
    pub manage_topics: Option<bool>,

    #[schemars(description = "read topics (optional)")]
    pub read_topics: Option<bool>,

    #[schemars(description = "poll messages (optional)")]
    pub poll_messages: Option<bool>,

    #[schemars(description = "send messages (optional)")]
    pub send_messages: Option<bool>,

    #[schemars(description = "topics permissions (optional)")]
    pub topics: Option<HashMap<usize, TopicPermissions>>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct TopicPermissions {
    #[schemars(description = "manage topic (optional)")]
    pub manage_topic: Option<bool>,

    #[schemars(description = "read topic (optional)")]
    pub read_topic: Option<bool>,

    #[schemars(description = "poll messages (optional)")]
    pub poll_messages: Option<bool>,

    #[schemars(description = "send messages (optional)")]
    pub send_messages: Option<bool>,
}

impl From<Permissions> for prelude::Permissions {
    fn from(permissions: Permissions) -> Self {
        prelude::Permissions {
            global: permissions
                .global
                .map(|global| global.into())
                .unwrap_or_default(),
            streams: permissions.streams.map(|streams| {
                streams
                    .into_iter()
                    .map(|(id, stream)| (id, stream.into()))
                    .collect()
            }),
        }
    }
}

impl From<GlobalPermissions> for prelude::GlobalPermissions {
    fn from(permissions: GlobalPermissions) -> Self {
        prelude::GlobalPermissions {
            manage_servers: permissions.manage_servers.unwrap_or_default(),
            read_servers: permissions.read_servers.unwrap_or_default(),
            manage_users: permissions.manage_users.unwrap_or_default(),
            read_users: permissions.read_users.unwrap_or_default(),
            manage_streams: permissions.manage_streams.unwrap_or_default(),
            read_streams: permissions.read_streams.unwrap_or_default(),
            manage_topics: permissions.manage_topics.unwrap_or_default(),
            read_topics: permissions.read_topics.unwrap_or_default(),
            poll_messages: permissions.poll_messages.unwrap_or_default(),
            send_messages: permissions.send_messages.unwrap_or_default(),
        }
    }
}

impl From<StreamPermissions> for prelude::StreamPermissions {
    fn from(permissions: StreamPermissions) -> Self {
        prelude::StreamPermissions {
            manage_stream: permissions.manage_stream.unwrap_or_default(),
            read_stream: permissions.read_stream.unwrap_or_default(),
            manage_topics: permissions.manage_topics.unwrap_or_default(),
            read_topics: permissions.read_topics.unwrap_or_default(),
            poll_messages: permissions.poll_messages.unwrap_or_default(),
            send_messages: permissions.send_messages.unwrap_or_default(),
            topics: permissions.topics.map(|topics| {
                topics
                    .into_iter()
                    .map(|(id, topic)| (id, topic.into()))
                    .collect()
            }),
        }
    }
}

impl From<TopicPermissions> for prelude::TopicPermissions {
    fn from(permissions: TopicPermissions) -> Self {
        prelude::TopicPermissions {
            manage_topic: permissions.manage_topic.unwrap_or_default(),
            read_topic: permissions.read_topic.unwrap_or_default(),
            poll_messages: permissions.poll_messages.unwrap_or_default(),
            send_messages: permissions.send_messages.unwrap_or_default(),
        }
    }
}
