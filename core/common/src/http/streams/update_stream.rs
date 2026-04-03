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

use super::MAX_NAME_LENGTH;
use crate::Identifier;
use crate::Validatable;
use crate::error::IggyError;
use serde::{Deserialize, Serialize};

/// `UpdateStream` command is used to update an existing stream.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `name` - unique stream name (string), max length is 255 characters.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct UpdateStream {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Unique stream name (string), max length is 255 characters.
    pub name: String,
}

impl Default for UpdateStream {
    fn default() -> Self {
        UpdateStream {
            stream_id: Identifier::default(),
            name: "stream".to_string(),
        }
    }
}

impl Validatable<IggyError> for UpdateStream {
    fn validate(&self) -> Result<(), IggyError> {
        if self.name.is_empty() || self.name.len() > MAX_NAME_LENGTH {
            return Err(IggyError::InvalidStreamName);
        }

        Ok(())
    }
}
