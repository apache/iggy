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

use super::{MAX_NAME_LENGTH, MAX_PARTITIONS_COUNT};
use crate::CompressionAlgorithm;
use crate::Identifier;
use crate::Validatable;
use crate::error::IggyError;
use crate::utils::expiry::IggyExpiry;
use crate::utils::topic_size::MaxTopicSize;
use serde::{Deserialize, Serialize};

/// `CreateTopic` command is used to create a new topic in a stream.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `partitions_count` - number of partitions in the topic, max value is 1000.
/// - `message_expiry` - message expiry, if `NeverExpire` then messages will never expire.
/// - `max_topic_size` - maximum size of the topic, if `Unlimited` then topic size is unlimited.
///   Can't be lower than segment size in the config.
/// - `replication_factor` - replication factor for the topic.
/// - `name` - unique topic name, max length is 255 characters.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct CreateTopic {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Number of partitions in the topic, max value is 1000.
    pub partitions_count: u32,
    /// Compression algorithm for the topic.
    pub compression_algorithm: CompressionAlgorithm,
    /// Message expiry, if `NeverExpire` then messages will never expire.
    pub message_expiry: IggyExpiry,
    /// Max topic size, if `Unlimited` then topic size is unlimited.
    /// Can't be lower than segment size in the config.
    pub max_topic_size: MaxTopicSize,
    /// Replication factor for the topic.
    pub replication_factor: Option<u8>,
    /// Unique topic name, max length is 255 characters.
    pub name: String,
}

impl Default for CreateTopic {
    fn default() -> Self {
        CreateTopic {
            stream_id: Identifier::default(),
            partitions_count: 1,
            compression_algorithm: CompressionAlgorithm::None,
            message_expiry: IggyExpiry::NeverExpire,
            max_topic_size: MaxTopicSize::ServerDefault,
            replication_factor: None,
            name: "topic".to_string(),
        }
    }
}

impl Validatable<IggyError> for CreateTopic {
    fn validate(&self) -> Result<(), IggyError> {
        if self.name.is_empty() || self.name.len() > MAX_NAME_LENGTH {
            return Err(IggyError::InvalidTopicName);
        }

        if !(0..=MAX_PARTITIONS_COUNT).contains(&self.partitions_count) {
            return Err(IggyError::TooManyPartitions);
        }

        if let Some(replication_factor) = self.replication_factor
            && replication_factor == 0
        {
            return Err(IggyError::InvalidReplicationFactor);
        }

        Ok(())
    }
}
