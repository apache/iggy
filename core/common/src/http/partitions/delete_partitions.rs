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

use super::MAX_PARTITIONS_COUNT;
use crate::Identifier;
use crate::Validatable;
use crate::error::IggyError;
use serde::{Deserialize, Serialize};

/// `DeletePartitions` command is used to delete partitions from a topic.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `topic_id` - unique topic ID (numeric or name).
/// - `partitions_count` - number of partitions in the topic to delete, max value is 1000.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct DeletePartitions {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Unique topic ID (numeric or name).
    #[serde(skip)]
    pub topic_id: Identifier,
    /// Number of partitions in the topic to delete, max value is 1000.
    pub partitions_count: u32,
}

impl Default for DeletePartitions {
    fn default() -> Self {
        DeletePartitions {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partitions_count: 1,
        }
    }
}

impl Validatable<IggyError> for DeletePartitions {
    fn validate(&self) -> Result<(), IggyError> {
        if !(1..=MAX_PARTITIONS_COUNT).contains(&self.partitions_count) {
            return Err(IggyError::TooManyPartitions);
        }

        Ok(())
    }
}
