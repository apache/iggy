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

use crate::Validatable;
use crate::defaults::*;
use crate::error::IggyError;
use serde::{Deserialize, Serialize};

/// `DeletePersonalAccessToken` command is used to delete a personal access token for the authenticated user.
/// It has additional payload:
/// - `name` - unique name of the token, must be between 3 and 30 characters long.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeletePersonalAccessToken {
    /// Unique name of the token, must be between 3 and 30 characters long.
    pub name: String,
}

impl Default for DeletePersonalAccessToken {
    fn default() -> Self {
        DeletePersonalAccessToken {
            name: "token".to_string(),
        }
    }
}

impl Validatable<IggyError> for DeletePersonalAccessToken {
    fn validate(&self) -> Result<(), IggyError> {
        if self.name.is_empty()
            || self.name.len() > MAX_PERSONAL_ACCESS_TOKEN_NAME_LENGTH
            || self.name.len() < MIN_PERSONAL_ACCESS_TOKEN_NAME_LENGTH
        {
            return Err(IggyError::InvalidPersonalAccessTokenName);
        }

        Ok(())
    }
}
