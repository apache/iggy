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

/// `DeleteStream` command is used to delete an existing stream.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Clone)]
pub struct DeleteStream {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
}

impl Validatable<IggyError> for DeleteStream {
    fn validate(&self) -> Result<(), IggyError> {
        Ok(())
    }
}
