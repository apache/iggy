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
use crate::OptionInputMap;
use crate::Validatable;
use crate::error::IggyError;
use serde::{Deserialize, Serialize};

/// `CreateStream` command is used to create a new stream.
/// It has additional payload:
/// - `name` - unique stream name (string), max length is 255 characters.
/// - `options` - stream options, such as the stream's own `encryption_key` as
///   canonical standard base64.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct CreateStream {
    /// Unique stream name (string), max length is 255 characters.
    pub name: String,
    /// Stream options keyed by option name. Unknown keys are rejected.
    #[serde(default, skip_serializing_if = "OptionInputMap::is_empty")]
    pub options: OptionInputMap,
}

impl Default for CreateStream {
    fn default() -> Self {
        CreateStream {
            name: "stream".to_string(),
            options: OptionInputMap::default(),
        }
    }
}

impl Validatable<IggyError> for CreateStream {
    fn validate(&self) -> Result<(), IggyError> {
        if self.name.is_empty() || self.name.len() > MAX_NAME_LENGTH {
            return Err(IggyError::InvalidStreamName);
        }

        Ok(())
    }
}
