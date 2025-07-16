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
    #[schemars(description = "stream identifier (numeric, optional)")]
    pub stream_id: Option<u32>,
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

    #[schemars(description = "topic identifier (numeric, optional)")]
    pub topic_id: Option<u32>,

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
