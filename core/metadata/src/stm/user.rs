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

use crate::define_state;
use crate::permissioner::Permissioner;
use crate::stm::Handle;
use ahash::AHashMap;
use iggy_common::change_password::ChangePassword;
use iggy_common::create_user::CreateUser;
use iggy_common::delete_user::DeleteUser;
use iggy_common::update_permissions::UpdatePermissions;
use iggy_common::update_user::UpdateUser;
use iggy_common::{IggyTimestamp, Permissions, PersonalAccessToken, UserId, UserStatus};
use slab::Slab;

// ============================================================================
// User Entity
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct User {
    pub id: UserId,
    pub username: String,
    pub password: String,
    pub status: UserStatus,
    pub created_at: IggyTimestamp,
    pub permissions: Option<Permissions>,
    pub personal_access_tokens: AHashMap<String, PersonalAccessToken>,
}

impl User {
    pub fn new(
        username: String,
        password: String,
        status: UserStatus,
        created_at: IggyTimestamp,
        permissions: Option<Permissions>,
    ) -> Self {
        Self {
            id: 0,
            username,
            password,
            status,
            created_at,
            permissions,
            personal_access_tokens: AHashMap::new(),
        }
    }
}

// ============================================================================
// Users State Machine
// ============================================================================

define_state! {
    Users,
    UsersInner {
        index: AHashMap<String, usize>,
        items: Slab<User>,
        permissioner: Permissioner,
    },
    UsersCommand,
    [CreateUser, UpdateUser, DeleteUser, ChangePassword, UpdatePermissions]
}

impl Handle for UsersInner {
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
