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
use crate::Validatable;
use crate::error::IggyError;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};

/// `ChangePassword` command is used to change a user's password.
/// It has additional payload:
/// - `user_id` - unique user ID (numeric or name).
/// - `current_password` - current password, must be between 3 and 100 characters long.
/// - `new_password` - new password, must be between 3 and 100 characters long.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChangePassword {
    /// Unique user ID (numeric or name).
    #[serde(skip)]
    pub user_id: Identifier,
    /// Current password, must be between 3 and 100 characters long.
    #[serde(serialize_with = "crate::utils::serde_secret::serialize_secret")]
    pub current_password: SecretString,
    /// New password, must be between 3 and 100 characters long.
    #[serde(serialize_with = "crate::utils::serde_secret::serialize_secret")]
    pub new_password: SecretString,
}

impl Default for ChangePassword {
    fn default() -> Self {
        ChangePassword {
            user_id: Identifier::default(),
            current_password: SecretString::from("secret"),
            new_password: SecretString::from("topsecret"),
        }
    }
}

impl Validatable<IggyError> for ChangePassword {
    fn validate(&self) -> Result<(), IggyError> {
        let current_password = self.current_password.expose_secret();
        if current_password.is_empty()
            || current_password.len() > MAX_PASSWORD_LENGTH
            || current_password.len() < MIN_PASSWORD_LENGTH
        {
            return Err(IggyError::InvalidPassword);
        }

        let new_password = self.new_password.expose_secret();
        if new_password.is_empty()
            || new_password.len() > MAX_PASSWORD_LENGTH
            || new_password.len() < MIN_PASSWORD_LENGTH
        {
            return Err(IggyError::InvalidPassword);
        }

        Ok(())
    }
}
