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
use crate::stm::StateHandler;
use crate::stm::snapshot::Snapshotable;
use crate::{collect_handlers, define_state, impl_fill_restore};
use ahash::AHashMap;
use iggy_common::change_password::ChangePassword;
use iggy_common::create_personal_access_token::CreatePersonalAccessToken;
use iggy_common::create_user::CreateUser;
use iggy_common::delete_personal_access_token::DeletePersonalAccessToken;
use iggy_common::delete_user::DeleteUser;
use iggy_common::update_permissions::UpdatePermissions;
use iggy_common::update_user::UpdateUser;
use iggy_common::{
    GlobalPermissions, IggyTimestamp, Permissions, PersonalAccessToken, StreamPermissions, UserId,
    UserStatus,
};
use serde::{Deserialize, Serialize};
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
    }
}

collect_handlers! {
    Users {
        CreateUser,
        UpdateUser,
        DeleteUser,
        ChangePassword,
        UpdatePermissions,
        CreatePersonalAccessToken,
        DeletePersonalAccessToken,
    }
}

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

impl StateHandler for CreateUser {
    type State = UsersInner;
    fn apply(&self, state: &mut UsersInner) {
        let username_arc: Arc<str> = Arc::from(self.username.as_str());
        if state.index.contains_key(&username_arc) {
            return;
        }

        let user = User {
            id: 0,
            username: username_arc.clone(),
            password_hash: Arc::from(self.password.as_str()),
            status: self.status,
            created_at: iggy_common::IggyTimestamp::now(),
            permissions: self.permissions.as_ref().map(|p| Arc::new(p.clone())),
        };

        let id = state.items.insert(user);
        if let Some(user) = state.items.get_mut(id) {
            user.id = id as UserId;
        }

        state.index.insert(username_arc, id as UserId);
        state
            .personal_access_tokens
            .insert(id as UserId, AHashMap::default());
    }
}

impl StateHandler for UpdateUser {
    type State = UsersInner;
    fn apply(&self, state: &mut UsersInner) {
        let Some(user_id) = state.resolve_user_id(&self.user_id) else {
            return;
        };

        let Some(user) = state.items.get_mut(user_id) else {
            return;
        };

        if let Some(new_username) = &self.username {
            let new_username_arc: Arc<str> = Arc::from(new_username.as_str());
            if let Some(&existing_id) = state.index.get(&new_username_arc)
                && existing_id != user_id as UserId
            {
                return;
            }

            state.index.remove(&user.username);
            user.username = new_username_arc.clone();
            state.index.insert(new_username_arc, user_id as UserId);
        }

        if let Some(new_status) = self.status {
            user.status = new_status;
        }
    }
}

impl StateHandler for DeleteUser {
    type State = UsersInner;
    fn apply(&self, state: &mut UsersInner) {
        let Some(user_id) = state.resolve_user_id(&self.user_id) else {
            return;
        };

        if let Some(user) = state.items.get(user_id) {
            let username = user.username.clone();
            state.items.remove(user_id);
            state.index.remove(&username);
            state.personal_access_tokens.remove(&(user_id as UserId));
        }
    }
}

impl StateHandler for ChangePassword {
    type State = UsersInner;
    fn apply(&self, state: &mut UsersInner) {
        let Some(user_id) = state.resolve_user_id(&self.user_id) else {
            return;
        };

        if let Some(user) = state.items.get_mut(user_id) {
            user.password_hash = Arc::from(self.new_password.as_str());
        }
    }
}

impl StateHandler for UpdatePermissions {
    type State = UsersInner;
    fn apply(&self, state: &mut UsersInner) {
        let Some(user_id) = state.resolve_user_id(&self.user_id) else {
            return;
        };

        if let Some(user) = state.items.get_mut(user_id) {
            user.permissions = self.permissions.as_ref().map(|p| Arc::new(p.clone()));
        }
    }
}

impl StateHandler for CreatePersonalAccessToken {
    type State = UsersInner;
    fn apply(&self, state: &mut UsersInner) {
        // TODO: Stub until protocol gets adjusted.
        let user_id = 0;
        let user_tokens = state.personal_access_tokens.entry(user_id).or_default();
        let name_arc: Arc<str> = Arc::from(self.name.as_str());
        if user_tokens.contains_key(&name_arc) {
            return;
        }

        let expiry_at = PersonalAccessToken::calculate_expiry_at(IggyTimestamp::now(), self.expiry);
        if let Some(expiry_at) = expiry_at
            && expiry_at.as_micros() <= IggyTimestamp::now().as_micros()
        {
            return;
        }

        let (pat, _) = PersonalAccessToken::new(
            user_id,
            self.name.as_ref(),
            IggyTimestamp::now(),
            self.expiry,
        );
        user_tokens.insert(name_arc, pat);
    }
}

impl StateHandler for DeletePersonalAccessToken {
    type State = UsersInner;
    fn apply(&self, state: &mut UsersInner) {
        // TODO: Stub until protocol gets adjusted.
        let user_id = 0;

        if let Some(user_tokens) = state.personal_access_tokens.get_mut(&user_id) {
            let name_arc: Arc<str> = Arc::from(self.name.as_str());
            user_tokens.remove(&name_arc);
        }
    }
}

/// User snapshot representation for serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserSnapshot {
    pub id: UserId,
    pub username: String,
    pub password_hash: String,
    pub status: UserStatus,
    pub created_at: IggyTimestamp,
    pub permissions: Option<Permissions>,
}

/// Personal access token snapshot representation for serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersonalAccessTokenSnapshot {
    pub user_id: UserId,
    pub name: String,
    pub token: String,
    pub expiry_at: Option<IggyTimestamp>,
}

/// Permissioner snapshot representation for serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionerSnapshot {
    pub users_permissions: Vec<(UserId, GlobalPermissions)>,
    pub users_streams_permissions: Vec<((UserId, usize), StreamPermissions)>,
    pub users_that_can_poll_messages_from_all_streams: Vec<UserId>,
    pub users_that_can_send_messages_to_all_streams: Vec<UserId>,
    pub users_that_can_poll_messages_from_specific_streams: Vec<(UserId, usize)>,
    pub users_that_can_send_messages_to_specific_streams: Vec<(UserId, usize)>,
}

/// Snapshot representation for the Users state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsersSnapshot {
    pub items: Vec<(usize, UserSnapshot)>,
    pub personal_access_tokens: Vec<(UserId, Vec<(String, PersonalAccessTokenSnapshot)>)>,
    pub permissioner: PermissionerSnapshot,
}

impl Snapshotable for Users {
    type Snapshot = UsersSnapshot;

    fn to_snapshot(&self) -> Self::Snapshot {
        self.inner.read(|inner| {
            let items: Vec<(usize, UserSnapshot)> = inner
                .items
                .iter()
                .map(|(user_id, user)| {
                    (
                        user_id,
                        UserSnapshot {
                            id: user.id,
                            username: user.username.to_string(),
                            password_hash: user.password_hash.to_string(),
                            status: user.status,
                            created_at: user.created_at,
                            permissions: user.permissions.as_ref().map(|p| (**p).clone()),
                        },
                    )
                })
                .collect();

            let personal_access_tokens: Vec<(UserId, Vec<(String, PersonalAccessTokenSnapshot)>)> =
                inner
                    .personal_access_tokens
                    .iter()
                    .map(|(&user_id, tokens)| {
                        let token_list: Vec<(String, PersonalAccessTokenSnapshot)> = tokens
                            .iter()
                            .map(|(name, pat)| {
                                (
                                    name.to_string(),
                                    PersonalAccessTokenSnapshot {
                                        user_id: pat.user_id,
                                        name: pat.name.to_string(),
                                        token: pat.token.to_string(),
                                        expiry_at: pat.expiry_at,
                                    },
                                )
                            })
                            .collect();
                        (user_id, token_list)
                    })
                    .collect();

            let permissioner = PermissionerSnapshot {
                users_permissions: inner
                    .permissioner
                    .users_permissions
                    .iter()
                    .map(|(&k, v)| (k, v.clone()))
                    .collect(),
                users_streams_permissions: inner
                    .permissioner
                    .users_streams_permissions
                    .iter()
                    .map(|(&k, v)| (k, v.clone()))
                    .collect(),
                users_that_can_poll_messages_from_all_streams: inner
                    .permissioner
                    .users_that_can_poll_messages_from_all_streams
                    .iter()
                    .copied()
                    .collect(),
                users_that_can_send_messages_to_all_streams: inner
                    .permissioner
                    .users_that_can_send_messages_to_all_streams
                    .iter()
                    .copied()
                    .collect(),
                users_that_can_poll_messages_from_specific_streams: inner
                    .permissioner
                    .users_that_can_poll_messages_from_specific_streams
                    .iter()
                    .copied()
                    .collect(),
                users_that_can_send_messages_to_specific_streams: inner
                    .permissioner
                    .users_that_can_send_messages_to_specific_streams
                    .iter()
                    .copied()
                    .collect(),
            };

            UsersSnapshot {
                items,
                personal_access_tokens,
                permissioner,
            }
        })
    }

    fn from_snapshot(
        snapshot: Self::Snapshot,
    ) -> Result<Self, crate::stm::snapshot::SnapshotError> {
        let mut index: AHashMap<Arc<str>, UserId> = AHashMap::new();
        let mut user_entries: Vec<(usize, User)> = Vec::new();

        for (slab_key, user_snap) in snapshot.items {
            let username: Arc<str> = Arc::from(user_snap.username.as_str());
            let user = User {
                id: user_snap.id,
                username: username.clone(),
                password_hash: Arc::from(user_snap.password_hash.as_str()),
                status: user_snap.status,
                created_at: user_snap.created_at,
                permissions: user_snap.permissions.map(Arc::new),
            };

            index.insert(username, slab_key as UserId);
            user_entries.push((slab_key, user));
        }

        let items: Slab<User> = user_entries.into_iter().collect();

        let mut personal_access_tokens: AHashMap<UserId, AHashMap<Arc<str>, PersonalAccessToken>> =
            AHashMap::new();
        for (user_id, tokens) in snapshot.personal_access_tokens {
            let mut token_map: AHashMap<Arc<str>, PersonalAccessToken> = AHashMap::new();
            for (name, pat_snap) in tokens {
                let pat = PersonalAccessToken::raw(
                    pat_snap.user_id,
                    &pat_snap.name,
                    &pat_snap.token,
                    pat_snap.expiry_at,
                );
                token_map.insert(Arc::from(name.as_str()), pat);
            }
            personal_access_tokens.insert(user_id, token_map);
        }

        let permissioner = Permissioner {
            users_permissions: snapshot
                .permissioner
                .users_permissions
                .into_iter()
                .collect(),
            users_streams_permissions: snapshot
                .permissioner
                .users_streams_permissions
                .into_iter()
                .collect(),
            users_that_can_poll_messages_from_all_streams: snapshot
                .permissioner
                .users_that_can_poll_messages_from_all_streams
                .into_iter()
                .collect(),
            users_that_can_send_messages_to_all_streams: snapshot
                .permissioner
                .users_that_can_send_messages_to_all_streams
                .into_iter()
                .collect(),
            users_that_can_poll_messages_from_specific_streams: snapshot
                .permissioner
                .users_that_can_poll_messages_from_specific_streams
                .into_iter()
                .collect(),
            users_that_can_send_messages_to_specific_streams: snapshot
                .permissioner
                .users_that_can_send_messages_to_specific_streams
                .into_iter()
                .collect(),
        };

        let inner = UsersInner {
            index,
            items,
            personal_access_tokens,
            permissioner,
        };
        Ok(inner.into())
    }
}

impl_fill_restore!(Users, users);
