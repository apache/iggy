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

use super::{MAX_NAME_LENGTH, MAX_PARTITIONS_COUNT};
use crate::Identifier;
use crate::OptionInputMap;
use crate::Validatable;
use crate::error::IggyError;
use serde::{Deserialize, Serialize};

/// `CreateTopic` command is used to create a new topic in a stream.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `partitions_count` - number of partitions in the topic, max value is 1000.
/// - `name` - unique topic name, max length is 255 characters.
/// - `options` - topic options keyed by option name, including
///   `compression_algorithm`, `message_expiry` and `max_topic_size`.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct CreateTopic {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Number of partitions in the topic, max value is 1000.
    pub partitions_count: u32,
    /// Unique topic name, max length is 255 characters.
    pub name: String,
    /// Topic options keyed by option name: a JSON string in the config-file
    /// form (`"7 days"`, `"1 GiB"`), a JSON integer, or a boolean. Unknown keys
    /// are rejected.
    #[serde(default, skip_serializing_if = "OptionInputMap::is_empty")]
    pub options: OptionInputMap,
}

impl Default for CreateTopic {
    fn default() -> Self {
        CreateTopic {
            stream_id: Identifier::default(),
            partitions_count: 1,
            name: "topic".to_string(),
            options: OptionInputMap::default(),
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

        Ok(())
    }
}
