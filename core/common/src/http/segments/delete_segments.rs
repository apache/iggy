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
use crate::Identifier;
use crate::Validatable;
use crate::error::IggyError;
use serde::{Deserialize, Serialize};

/// `DeleteSegments` command is used to delete segments from a partition.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `topic_id` - unique topic ID (numeric or name).
/// - `partition_id` - unique partition ID (numeric or name).
/// - `segments_count` - number of segments in the partition to delete.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct DeleteSegments {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Unique topic ID (numeric or name).
    #[serde(skip)]
    pub topic_id: Identifier,
    /// Unique partition ID (numeric or name).
    #[serde(skip)]
    pub partition_id: u32,
    /// Number of segments in the partition to delete, max value is 1000.
    pub segments_count: u32,
}

impl Default for DeleteSegments {
    fn default() -> Self {
        DeleteSegments {
            segments_count: 1,
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partition_id: u32::default(),
        }
    }
}

impl Validatable<IggyError> for DeleteSegments {
    fn validate(&self) -> Result<(), IggyError> {
        Ok(())
    }
}
