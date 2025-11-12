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
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::utils::crypto;
use dashmap::DashMap;
use iggy_common::IggyTimestamp;
use iggy_common::UserStatus;
use iggy_common::defaults::*;
use iggy_common::{Permissions, UserId};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct User {
    pub id: UserId,
    pub status: UserStatus,
    pub username: String,
    pub password: String,
    pub created_at: IggyTimestamp,
    pub permissions: Option<Permissions>,
    pub personal_access_tokens: DashMap<Arc<String>, PersonalAccessToken>,
}

impl Default for User {
    fn default() -> Self {
        Self {
            id: 0,
            status: UserStatus::Active,
            username: "user".to_string(),
            password: "secret".to_string(),
            created_at: IggyTimestamp::now(),
            permissions: None,
            personal_access_tokens: DashMap::new(),
        }
    }
}

impl User {
    pub fn empty(id: UserId) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }

    pub fn new(
        id: u32,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Self {
        Self::with_password(
            id,
            username,
            crypto::hash_password(password),
            status,
            permissions,
        )
    }

    pub fn with_password(
        id: u32,
        username: &str,
        password: String,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Self {
        Self {
            id,
            username: username.into(),
            password,
            created_at: IggyTimestamp::now(),
            status,
            permissions,
            personal_access_tokens: DashMap::new(),
        }
    }

    pub fn root(username: &str, password: &str) -> Self {
        Self::new(
            DEFAULT_ROOT_USER_ID,
            username,
            password,
            UserStatus::Active,
            Some(Permissions::root()),
        )
    }

    pub fn is_root(&self) -> bool {
        self.id == DEFAULT_ROOT_USER_ID
    }

    pub fn is_active(&self) -> bool {
        self.status == UserStatus::Active
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_root_user_data_and_credentials_should_be_valid() {
        let user = User::root(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD);
        assert_eq!(user.id, DEFAULT_ROOT_USER_ID);
        assert_eq!(user.username, DEFAULT_ROOT_USERNAME);
        assert_ne!(user.password, DEFAULT_ROOT_PASSWORD);
        assert!(crypto::verify_password(
            DEFAULT_ROOT_PASSWORD,
            &user.password
        ));
        assert_eq!(user.status, UserStatus::Active);
        assert!(user.created_at.as_micros() > 0);
    }

    #[test]
    fn should_be_created_given_specific_status() {
        let status = UserStatus::Inactive;
        let user = User::new(1, "test", "test", status, None);
        assert_eq!(user.status, status);
    }
}
