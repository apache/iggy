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
use crate::Permissions;
use crate::UserStatus;
use crate::Validatable;
use crate::error::IggyError;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};

/// `CreateUser` command is used to create a new user.
/// It has additional payload:
/// - `username` - unique name of the user, must be between 3 and 50 characters long.
/// - `password` - password of the user, must be between 3 and 100 characters long.
/// - `status` - status of the user, can be either `active` or `inactive`.
/// - `permissions` - optional permissions of the user. If not provided, user will have no permissions.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CreateUser {
    /// Unique name of the user, must be between 3 and 50 characters long.
    pub username: String,
    /// Password of the user, must be between 3 and 100 characters long.
    #[serde(serialize_with = "crate::utils::serde_secret::serialize_secret")]
    pub password: SecretString,
    /// Status of the user, can be either `active` or `inactive`.
    pub status: UserStatus,
    /// Optional permissions of the user. If not provided, user will have no permissions.
    pub permissions: Option<Permissions>,
}

impl Default for CreateUser {
    fn default() -> Self {
        CreateUser {
            username: "user".to_string(),
            password: SecretString::from("secret"),
            status: UserStatus::Active,
            permissions: None,
        }
    }
}

impl Validatable<IggyError> for CreateUser {
    fn validate(&self) -> Result<(), IggyError> {
        if self.username.is_empty()
            || self.username.len() > MAX_USERNAME_LENGTH
            || self.username.len() < MIN_USERNAME_LENGTH
        {
            return Err(IggyError::InvalidUsername);
        }

        let password = self.password.expose_secret();
        if password.is_empty()
            || password.len() > MAX_PASSWORD_LENGTH
            || password.len() < MIN_PASSWORD_LENGTH
        {
            return Err(IggyError::InvalidPassword);
        }

        Ok(())
    }
}
