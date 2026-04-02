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

use super::defaults::*;
use crate::Identifier;
use crate::UserStatus;
use crate::Validatable;
use crate::error::IggyError;
use serde::{Deserialize, Serialize};

/// `UpdateUser` command is used to update a user's username and status.
/// It has additional payload:
/// - `user_id` - unique user ID (numeric or name).
/// - `username` - new username (optional), if provided, must be between 3 and 50 characters long.
/// - `status` - new status (optional)
#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Clone)]
pub struct UpdateUser {
    #[serde(skip)]
    pub user_id: Identifier,
    pub username: Option<String>,
    pub status: Option<UserStatus>,
}

impl Validatable<IggyError> for UpdateUser {
    fn validate(&self) -> Result<(), IggyError> {
        if self.username.is_none() {
            return Ok(());
        }

        let username = self.username.as_ref().unwrap();
        if username.is_empty()
            || username.len() > MAX_USERNAME_LENGTH
            || username.len() < MIN_USERNAME_LENGTH
        {
            return Err(IggyError::InvalidUsername);
        }

        Ok(())
    }
}
