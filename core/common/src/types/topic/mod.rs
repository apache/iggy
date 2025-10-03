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

use crate::CompressionAlgorithm;
use crate::Partition;
use crate::utils::byte_size::IggyByteSize;
use crate::utils::expiry::IggyExpiry;
use crate::utils::timestamp::IggyTimestamp;
use crate::utils::topic_size::MaxTopicSize;
use serde::{Deserialize, Serialize};

/// `Topic` represents the medium level of logical separation of data as it's a part of the stream.
/// It consists of the following fields:
/// - `id`: the unique identifier (numeric) of the topic.
/// - `created_at`: the timestamp when the topic was created.
/// - `name`: the unique name of the topic.
/// - `size`: the total size of the topic in bytes.
/// - `message_expiry`: the expiry of the messages in the topic.
/// - `max_topic_size`: the maximum size of the topic.
/// - `replication_factor`: replication factor for the topic.
/// - `messages_count`: the total number of messages in the topic.
/// - `partitions_count`: the total number of partitions in the topic.
#[derive(Debug, Serialize, Deserialize)]
pub struct Topic {
    /// The unique identifier (numeric) of the topic.
    pub id: u32,
    /// The timestamp when the topic was created.
    pub created_at: IggyTimestamp,
    /// The unique name of the topic.
    pub name: String,
    /// The total size of the topic in bytes.
    pub size: IggyByteSize,
    /// The expiry of the messages in the topic.
    pub message_expiry: IggyExpiry,
    /// Compression algorithm for the topic.
    pub compression_algorithm: CompressionAlgorithm,
    /// The optional maximum size of the topic.
    /// Can't be lower than segment size in the config.
    pub max_topic_size: MaxTopicSize,
    /// Replication factor for the topic.
    pub replication_factor: u8,
    /// The total number of messages in the topic.
    pub messages_count: u64,
    /// The total number of partitions in the topic.
    pub partitions_count: u32,
}

/// `TopicDetails` represents the detailed information about the topic.
/// It consists of the following fields:
/// - `id`: the unique identifier (numeric) of the topic.
/// - `created_at`: the timestamp when the topic was created.
/// - `name`: the unique name of the topic.
/// - `size`: the total size of the topic.
/// - `message_expiry`: the expiry of the messages in the topic.
/// - `max_topic_size`: the maximum size of the topic.
/// - `replication_factor`: replication factor for the topic.
/// - `messages_count`: the total number of messages in the topic.
/// - `partitions_count`: the total number of partitions in the topic.
/// - `partitions`: the collection of partitions in the topic.
#[derive(Debug, Serialize, Deserialize)]
pub struct TopicDetails {
    /// The unique identifier (numeric) of the topic.
    pub id: u32,
    /// The timestamp when the topic was created.
    pub created_at: IggyTimestamp,
    /// The unique name of the topic.
    pub name: String,
    /// The total size of the topic.
    pub size: IggyByteSize,
    /// The expiry of the messages in the topic.
    pub message_expiry: IggyExpiry,
    /// Compression algorithm for the topic.
    pub compression_algorithm: CompressionAlgorithm,
    /// The optional maximum size of the topic.
    /// Can't be lower than segment size in the config.
    pub max_topic_size: MaxTopicSize,
    /// Replication factor for the topic.
    pub replication_factor: u8,
    /// The total number of messages in the topic.
    pub messages_count: u64,
    /// The total number of partitions in the topic.
    pub partitions_count: usize,
    /// The collection of partitions in the topic.
    pub partitions: Vec<Partition>,
}
