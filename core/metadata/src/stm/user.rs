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
use iggy_common::create_personal_access_token::CreatePersonalAccessToken;
use iggy_common::create_user::CreateUser;
use iggy_common::delete_personal_access_token::DeletePersonalAccessToken;
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

define_state! {
    Users {
        index: AHashMap<Arc<str>, UserId>,
        items: Slab<User>,
        personal_access_tokens: AHashMap<UserId, AHashMap<Arc<str>, PersonalAccessToken>>,
        permissioner: Permissioner,
    },
    [
        CreateUser,
        UpdateUser,
        DeleteUser,
        ChangePassword,
        UpdatePermissions,
        CreatePersonalAccessToken,
        DeletePersonalAccessToken
    ]
}
impl_absorb!(UsersInner, UsersCommand);

impl UsersInner {
    fn resolve_user_id(&self, identifier: &iggy_common::Identifier) -> Option<usize> {
        use iggy_common::IdKind;
        match identifier.kind {
            IdKind::Numeric => {
                let id = identifier.get_u32_value().ok()? as usize;
                if self.items.contains(id) {
                    Some(id)
                } else {
                    None
                }
            }
            IdKind::String => {
                let username = identifier.get_string_value().ok()?;
                self.index.get(username.as_str()).map(|&id| id as usize)
            }
        }
    }
}

impl Handler for UsersInner {
    fn handle(&mut self, cmd: &UsersCommand) {
        match cmd {
            UsersCommand::CreateUser(payload) => {
                let username_arc: Arc<str> = Arc::from(payload.username.as_str());
                if self.index.contains_key(&username_arc) {
                    return;
                }

                let user = User {
                    id: 0,
                    username: username_arc.clone(),
                    password_hash: Arc::from(payload.password.as_str()),
                    status: payload.status,
                    created_at: iggy_common::IggyTimestamp::now(),
                    permissions: payload.permissions.as_ref().map(|p| Arc::new(p.clone())),
                };

                let id = self.items.insert(user);
                if let Some(user) = self.items.get_mut(id) {
                    user.id = id as UserId;
                }

                self.index.insert(username_arc, id as UserId);
                self.personal_access_tokens
                    .insert(id as UserId, AHashMap::default());
            }
            UsersCommand::UpdateUser(payload) => {
                let Some(user_id) = self.resolve_user_id(&payload.user_id) else {
                    return;
                };

                let Some(user) = self.items.get_mut(user_id) else {
                    return;
                };

                if let Some(new_username) = &payload.username {
                    let new_username_arc: Arc<str> = Arc::from(new_username.as_str());
                    if let Some(&existing_id) = self.index.get(&new_username_arc)
                        && existing_id != user_id as UserId
                    {
                        return;
                    }

                    self.index.remove(&user.username);
                    user.username = new_username_arc.clone();
                    self.index.insert(new_username_arc, user_id as UserId);
                }

                if let Some(new_status) = payload.status {
                    user.status = new_status;
                }
            }
            UsersCommand::DeleteUser(payload) => {
                let Some(user_id) = self.resolve_user_id(&payload.user_id) else {
                    return;
                };

                if let Some(user) = self.items.get(user_id) {
                    let username = user.username.clone();
                    self.items.remove(user_id);
                    self.index.remove(&username);
                    self.personal_access_tokens.remove(&(user_id as UserId));
                }
            }
            UsersCommand::ChangePassword(payload) => {
                let Some(user_id) = self.resolve_user_id(&payload.user_id) else {
                    return;
                };

                if let Some(user) = self.items.get_mut(user_id) {
                    user.password_hash = Arc::from(payload.new_password.as_str());
                }
            }
            UsersCommand::UpdatePermissions(payload) => {
                let Some(user_id) = self.resolve_user_id(&payload.user_id) else {
                    return;
                };

                if let Some(user) = self.items.get_mut(user_id) {
                    user.permissions = payload.permissions.as_ref().map(|p| Arc::new(p.clone()));
                }
            }
            UsersCommand::CreatePersonalAccessToken(payload) => {
                // TODO: Stub untill protocol gets adjusted.
                let user_id = 0;
                let user_tokens = self.personal_access_tokens.entry(user_id).or_default();
                let name_arc: Arc<str> = Arc::from(payload.name.as_str());
                if user_tokens.contains_key(&name_arc) {
                    return;
                }

                let expiry_at =
                    PersonalAccessToken::calculate_expiry_at(IggyTimestamp::now(), payload.expiry);
                if let Some(expiry_at) = expiry_at
                    && expiry_at.as_micros() <= IggyTimestamp::now().as_micros()
                {
                    return;
                }

                let (pat, _) = PersonalAccessToken::new(
                    user_id,
                    payload.name.as_ref(),
                    IggyTimestamp::now(),
                    payload.expiry,
                );
                user_tokens.insert(name_arc, pat);
            }
            UsersCommand::DeletePersonalAccessToken(payload) => {
                // TODO: Stub untill protocol gets adjusted.
                let user_id = 0;

                if let Some(user_tokens) = self.personal_access_tokens.get_mut(&user_id) {
                    let name_arc: Arc<str> = Arc::from(payload.name.as_str());
                    user_tokens.remove(&name_arc);
                }
            }
        }
    }
}
