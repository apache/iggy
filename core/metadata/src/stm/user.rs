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

use crate::permissioner::Permissioner;
use crate::stm::Handler;
use crate::{define_state, impl_absorb};
use ahash::AHashMap;
use iggy_common::change_password::ChangePassword;
use iggy_common::create_user::CreateUser;
use iggy_common::delete_user::DeleteUser;
use iggy_common::update_permissions::UpdatePermissions;
use iggy_common::update_user::UpdateUser;
use iggy_common::{IggyTimestamp, Permissions, PersonalAccessToken, UserId, UserStatus};
use slab::Slab;
use std::sync::Arc;

// ============================================================================
// User Entity
// ============================================================================

#[derive(Debug, Clone)]
pub struct User {
    pub id: UserId,
    pub username: Arc<str>,
    pub password_hash: Arc<str>,
    pub status: UserStatus,
    pub created_at: IggyTimestamp,
    pub permissions: Option<Arc<Permissions>>,
}

impl Default for User {
    fn default() -> Self {
        Self {
            id: 0,
            username: Arc::from(""),
            password_hash: Arc::from(""),
            status: UserStatus::default(),
            created_at: IggyTimestamp::default(),
            permissions: None,
        }
    }
}

impl User {
    pub fn new(
        username: Arc<str>,
        password_hash: Arc<str>,
        status: UserStatus,
        created_at: IggyTimestamp,
        permissions: Option<Arc<Permissions>>,
    ) -> Self {
        Self {
            id: 0,
            username,
            password_hash,
            status,
            created_at,
            permissions,
        }
    }
}

// ============================================================================
// Users State Machine
// ============================================================================

define_state! {
    Users {
        index: AHashMap<Arc<str>, UserId>,
        items: Slab<User>,
        personal_access_tokens: AHashMap<UserId, AHashMap<Arc<str>, PersonalAccessToken>>,
        permissioner: Permissioner,
    },
    [CreateUser, UpdateUser, DeleteUser, ChangePassword, UpdatePermissions]
}
impl_absorb!(UsersInner, UsersCommand);

impl Handler for UsersInner {
    fn handle(&mut self, cmd: &UsersCommand) {
        match cmd {
            UsersCommand::CreateUser(_payload) => {
                // Actual mutation logic will be implemented later
            }
            UsersCommand::UpdateUser(_payload) => {
                // Actual mutation logic will be implemented later
            }
            UsersCommand::DeleteUser(_payload) => {
                // Actual mutation logic will be implemented later
            }
            UsersCommand::ChangePassword(_payload) => {
                // Actual mutation logic will be implemented later
            }
            UsersCommand::UpdatePermissions(_payload) => {
                // Actual mutation logic will be implemented later
            }
        }
    }
}
