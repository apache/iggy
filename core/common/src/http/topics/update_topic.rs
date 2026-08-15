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

use super::MAX_NAME_LENGTH;
use crate::CompressionAlgorithm;
use crate::Identifier;
use crate::Validatable;
use crate::error::IggyError;
use crate::utils::expiry::IggyExpiry;
use crate::utils::topic_size::MaxTopicSize;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// `UpdateTopic` command is used to update a topic in a stream.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `topic_id` - unique topic ID (numeric or name).
/// - `name` - unique topic name, max length is 255 characters.
/// - `compression_algorithm`, `message_expiry`, `max_topic_size` - omit a field
///   to leave the topic's current value alone. Named here for REST ergonomics;
///   the server folds them into the same option keys the binary protocol uses,
///   so there is still one source per setting.
/// - `options` - additional option keys as strings; only keys the update path
///   accepts are allowed.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct UpdateTopic {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Unique topic ID (numeric or name).
    #[serde(skip)]
    pub topic_id: Identifier,
    /// Compression algorithm; omit to leave the current one alone.
    #[serde(default)]
    pub compression_algorithm: Option<CompressionAlgorithm>,
    /// Message expiry; omit to leave the current one alone.
    #[serde(default)]
    pub message_expiry: Option<IggyExpiry>,
    /// Max topic size; omit to leave the current one alone.
    #[serde(default)]
    pub max_topic_size: Option<MaxTopicSize>,
    /// Unique topic name, max length is 255 characters.
    pub name: String,
    /// Additional topic options as string key-values. Restricted to the keys
    /// an update may change; anything else is rejected.
    #[serde(default)]
    pub options: BTreeMap<String, String>,
}

impl Default for UpdateTopic {
    fn default() -> Self {
        UpdateTopic {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            compression_algorithm: None,
            message_expiry: None,
            max_topic_size: None,
            name: "topic".to_string(),
            options: BTreeMap::new(),
        }
    }
}

impl Validatable<IggyError> for UpdateTopic {
    fn validate(&self) -> Result<(), IggyError> {
        if self.name.is_empty() || self.name.len() > MAX_NAME_LENGTH {
            return Err(IggyError::InvalidTopicName);
        }

        Ok(())
    }
}
