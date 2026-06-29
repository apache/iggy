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
use crate::stm::result::{
    ApplyReply, ChangePasswordResult, CreatePersonalAccessTokenResult, CreateUserResult,
    DeletePersonalAccessTokenResult, DeleteUserResult, UpdatePermissionsResult, UpdateUserResult,
};
use crate::stm::snapshot::Snapshotable;
use crate::{collect_handlers, define_state, impl_fill_restore};
use ahash::AHashMap;
use bytes::Bytes;
use bytes::{BufMut, BytesMut};
use iggy_binary_protocol::codec::{WireDecode, WireEncode, read_u32_le, read_u64_le};
use iggy_binary_protocol::primitives::permissions::{WireGlobalPermissions, WirePermissions};
use iggy_binary_protocol::requests::users::{
    ChangePasswordRequest, CreateUserRequest, DeleteUserRequest, UpdatePermissionsRequest,
    UpdateUserRequest,
};
use iggy_binary_protocol::responses::users::get_user::UserDetailsResponse;
use iggy_binary_protocol::responses::users::user_response::UserResponse;
use iggy_binary_protocol::{WireIdentifier, WireName};
use iggy_common::{
    GlobalPermissions, IggyExpiry, IggyTimestamp, Permissions, PersonalAccessToken,
    StreamPermissions, UserId, UserStatus,
};
use serde::{Deserialize, Serialize};
use slab::Slab;
use std::collections::BTreeSet;
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
    #[must_use]
    pub const fn new(
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
        // SAFETY: deterministic-apply invariant. `AHashMap` iteration order
        // differs across replicas (random seed), so this map MUST only be
        // touched via single-key `.get` / `.insert` / `.remove`. Never
        // iterate. Reach for `BTreeMap` the first time iteration is needed.
        personal_access_token_index: AHashMap<Arc<str>, (UserId, Arc<str>)>,
        // Expiry-ordered index of expiring PATs: `(expiry_micros, user_id,
        // name)`. Never-expiring tokens are absent. Unlike the sibling
        // `AHashMap` index above, a `BTreeSet` is safe to iterate (its order
        // is deterministic across replicas), which lets the PAT cleaner find
        // expired tokens in O(log n) per tick instead of scanning every token.
        personal_access_token_expiry_index: BTreeSet<(u64, UserId, Arc<str>)>,
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
    fn resolve_user_id(&self, identifier: &WireIdentifier) -> Option<usize> {
        match identifier {
            WireIdentifier::Numeric(id) => {
                let id = *id as usize;
                if self.items.contains(id) {
                    Some(id)
                } else {
                    None
                }
            }
            WireIdentifier::String(name) => self.index.get(name.as_str()).map(|&id| id as usize),
        }
    }

    /// Collect `(user_id, name)` for every expired personal access token.
    ///
    /// Walks the expiry-ordered `personal_access_token_expiry_index`,
    /// stopping at the first not-yet-expired entry, so a tick with nothing
    /// due costs O(log n), not a full O(users x tokens) scan. The `BTreeSet`
    /// is sorted, so iteration is deterministic across replicas (the sibling
    /// `personal_access_token_index` `AHashMap` is not); the leader-only
    /// caller replicates each delete regardless.
    #[must_use]
    pub fn expired_personal_access_tokens(&self, now: IggyTimestamp) -> Vec<(UserId, Arc<str>)> {
        let now_micros = now.as_micros();
        self.personal_access_token_expiry_index
            .iter()
            .take_while(|(expiry_micros, _, _)| *expiry_micros <= now_micros)
            .map(|(_, user_id, name)| (*user_id, Arc::clone(name)))
            .collect()
    }
}

impl Users {
    #[must_use]
    pub fn read<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&UsersInner) -> R,
    {
        self.inner.read(f)
    }

    /// Ensures a root user exists in an empty user set.
    ///
    /// # Panics
    ///
    /// Panics if `username` is not a valid wire-format username.
    pub fn ensure_root_user(&self, username: &str, password_hash: &str) {
        if self.read(|users| !users.items.is_empty()) {
            return;
        }

        // Boot-only invariant: server-ng calls this before listeners and
        // consensus traffic start, on shard 0 initialization. The read/apply
        // split cannot race another user creation in that phase.
        let username = WireName::new(username).expect("root username must be valid");
        // Bootstrap path is intentionally unreplicated (every replica calls
        // this locally on shard 0). A fresh `IggyTimestamp::now()` per replica
        // would make `root.created_at` differ across the cluster and break
        // any future snapshot/state-hash equality check. Stamp a fixed
        // non-zero sentinel instead: deterministic across replicas (every
        // replica reads the same value) while remaining a valid `> 0`
        // creation timestamp for clients that assert one.
        self.inner
            .try_apply(UsersCommand::CreateUser(
                CreateUserRequest {
                    username,
                    password: password_hash.to_string(),
                    status: UserStatus::Active.as_code(),
                    permissions: Some(WirePermissions {
                        global: WireGlobalPermissions {
                            manage_servers: true,
                            read_servers: true,
                            manage_users: true,
                            read_users: true,
                            manage_streams: true,
                            read_streams: true,
                            manage_topics: true,
                            read_topics: true,
                            poll_messages: true,
                            send_messages: true,
                        },
                        streams: Vec::new(),
                    }),
                },
                IggyTimestamp::from(1),
            ))
            .expect("root user bootstrap must run on the metadata writer");
    }
}

/// Replicated create-PAT command enriched with the authenticated user id
/// and a primary-minted `token_hash`.
///
/// `token_hash` is the hash of the raw token, generated once by the primary
/// in `maybe_rewrite_pat_request` and shipped in the prepare body. Apply
/// stores it directly via `PersonalAccessToken::raw`; calling
/// `PersonalAccessToken::new` inside apply would `ring::rand` per-replica
/// and produce divergent state (different hash → divergent
/// `personal_access_token_index`).
///
/// Fixed-width 64-byte hex hash on the wire (SHA-256 of the raw token).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePersonalAccessTokenRequest {
    pub user_id: UserId,
    pub name: WireName,
    pub expiry: u64,
    pub token_hash: [u8; PAT_TOKEN_HASH_BYTES],
}

pub const PAT_TOKEN_HASH_BYTES: usize = 64;

impl WireEncode for CreatePersonalAccessTokenRequest {
    fn encoded_size(&self) -> usize {
        4 + self.name.encoded_size() + 8 + PAT_TOKEN_HASH_BYTES
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.user_id);
        self.name.encode(buf);
        buf.put_u64_le(self.expiry);
        buf.put_slice(&self.token_hash);
    }
}

impl WireDecode for CreatePersonalAccessTokenRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), iggy_binary_protocol::WireError> {
        let user_id = read_u32_le(buf, 0)?;
        let (name, mut pos) = WireName::decode(&buf[4..])?;
        pos += 4;
        let expiry = read_u64_le(buf, pos)?;
        pos += 8;
        if buf.len() < pos + PAT_TOKEN_HASH_BYTES {
            return Err(iggy_binary_protocol::WireError::UnexpectedEof {
                offset: pos,
                need: PAT_TOKEN_HASH_BYTES,
                have: buf.len() - pos,
            });
        }
        let mut token_hash = [0u8; PAT_TOKEN_HASH_BYTES];
        token_hash.copy_from_slice(&buf[pos..pos + PAT_TOKEN_HASH_BYTES]);
        pos += PAT_TOKEN_HASH_BYTES;
        Ok((
            Self {
                user_id,
                name,
                expiry,
                token_hash,
            },
            pos,
        ))
    }
}

/// Replicated delete-PAT command enriched with the authenticated user id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletePersonalAccessTokenRequest {
    pub user_id: UserId,
    pub name: WireName,
    /// `true` for a cleaner-originated delete: apply removes the token only if
    /// the stored token is still expired as of the prepare timestamp, so a
    /// token deleted and recreated under the same name with a fresh expiry
    /// between the cleaner's snapshot read and this commit survives. `false`
    /// for an unconditional client delete by name.
    pub only_if_expired: bool,
}

impl WireEncode for DeletePersonalAccessTokenRequest {
    fn encoded_size(&self) -> usize {
        4 + self.name.encoded_size() + 1
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.user_id);
        self.name.encode(buf);
        buf.put_u8(u8::from(self.only_if_expired));
    }
}

impl WireDecode for DeletePersonalAccessTokenRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), iggy_binary_protocol::WireError> {
        let user_id = read_u32_le(buf, 0)?;
        let (name, consumed) = WireName::decode(&buf[4..])?;
        let pos = 4 + consumed;
        let Some(&flag) = buf.get(pos) else {
            return Err(iggy_binary_protocol::WireError::UnexpectedEof {
                offset: pos,
                need: 1,
                have: 0,
            });
        };
        Ok((
            Self {
                user_id,
                name,
                only_if_expired: flag != 0,
            },
            pos + 1,
        ))
    }
}

impl StateHandler for CreateUserRequest {
    type State = UsersInner;
    #[allow(clippy::cast_possible_truncation)]
    fn apply(&self, state: &mut UsersInner, timestamp: IggyTimestamp) -> ApplyReply {
        let username_arc: Arc<str> = Arc::from(self.username.as_str());
        if state.index.contains_key(&username_arc) {
            return ApplyReply::err(CreateUserResult::UserAlreadyExists);
        }

        let status = UserStatus::from_code(self.status).unwrap_or_default();
        let permissions = self
            .permissions
            .as_ref()
            .map(|p| Arc::new(Permissions::from(p.clone())));

        let user = User {
            id: 0,
            username: username_arc.clone(),
            password_hash: Arc::from(self.password.as_str()),
            status,
            created_at: timestamp,
            permissions,
        };

        let id = state.items.insert(user);
        if let Some(user) = state.items.get_mut(id) {
            user.id = id as UserId;
        }

        state.index.insert(username_arc, id as UserId);
        state
            .personal_access_tokens
            .insert(id as UserId, AHashMap::default());

        // Reply body: the SDK `create_user` decodes a `UserDetailsResponse`.
        // Serialization local to this state machine.
        ApplyReply::ok(
            UserDetailsResponse {
                user: UserResponse {
                    id: id as u32,
                    created_at: timestamp.as_micros(),
                    status: self.status,
                    username: self.username.clone(),
                },
                permissions: self.permissions.clone(),
            }
            .to_bytes(),
        )
    }
}

impl StateHandler for UpdateUserRequest {
    type State = UsersInner;
    #[allow(clippy::cast_possible_truncation)]
    fn apply(&self, state: &mut UsersInner, _timestamp: IggyTimestamp) -> ApplyReply {
        let Some(user_id) = state.resolve_user_id(&self.user_id) else {
            return ApplyReply::err(UpdateUserResult::UserNotFound);
        };

        let Some(user) = state.items.get_mut(user_id) else {
            return ApplyReply::err(UpdateUserResult::UserNotFound);
        };

        if let Some(new_username) = &self.username {
            let new_username_arc: Arc<str> = Arc::from(new_username.as_str());
            if let Some(&existing_id) = state.index.get(&new_username_arc)
                && existing_id != user_id as UserId
            {
                return ApplyReply::err(UpdateUserResult::UsernameAlreadyExists);
            }

            state.index.remove(&user.username);
            user.username = new_username_arc.clone();
            state.index.insert(new_username_arc, user_id as UserId);
        }

        if let Some(status_code) = self.status
            && let Ok(new_status) = UserStatus::from_code(status_code)
        {
            user.status = new_status;
        }
        ApplyReply::ok(Bytes::new())
    }
}

impl StateHandler for DeleteUserRequest {
    type State = UsersInner;
    #[allow(clippy::cast_possible_truncation)]
    fn apply(&self, state: &mut UsersInner, _timestamp: IggyTimestamp) -> ApplyReply {
        let Some(user_id) = state.resolve_user_id(&self.user_id) else {
            return ApplyReply::err(DeleteUserResult::UserNotFound);
        };

        if let Some(user) = state.items.get(user_id) {
            let username = user.username.clone();
            state.items.remove(user_id);
            state.index.remove(&username);
            if let Some(tokens) = state.personal_access_tokens.remove(&(user_id as UserId)) {
                for pat in tokens.values() {
                    state.personal_access_token_index.remove(&pat.token);
                    if let Some(expiry_at) = pat.expiry_at {
                        state.personal_access_token_expiry_index.remove(&(
                            expiry_at.as_micros(),
                            user_id as UserId,
                            Arc::clone(&pat.name),
                        ));
                    }
                }
            }
        }
        ApplyReply::ok(Bytes::new())
    }
}

impl StateHandler for ChangePasswordRequest {
    type State = UsersInner;
    fn apply(&self, state: &mut UsersInner, _timestamp: IggyTimestamp) -> ApplyReply {
        let Some(user_id) = state.resolve_user_id(&self.user_id) else {
            return ApplyReply::err(ChangePasswordResult::UserNotFound);
        };

        if let Some(user) = state.items.get_mut(user_id) {
            user.password_hash = Arc::from(self.new_password.as_str());
        }
        ApplyReply::ok(Bytes::new())
    }
}

impl StateHandler for UpdatePermissionsRequest {
    type State = UsersInner;
    fn apply(&self, state: &mut UsersInner, _timestamp: IggyTimestamp) -> ApplyReply {
        let Some(user_id) = state.resolve_user_id(&self.user_id) else {
            return ApplyReply::err(UpdatePermissionsResult::UserNotFound);
        };

        if let Some(user) = state.items.get_mut(user_id) {
            user.permissions = self
                .permissions
                .as_ref()
                .map(|p| Arc::new(Permissions::from(p.clone())));
        }
        ApplyReply::ok(Bytes::new())
    }
}

// TODO(hubcio): Serialize proper reply (e.g. generated raw token from the
// primary-side mint) instead of empty Bytes. The raw token is currently
// generated only at the request-rewrite step on the primary and dropped;
// surfacing it back to the client needs a side-channel out of
// `maybe_rewrite_pat_request`.
impl StateHandler for CreatePersonalAccessTokenRequest {
    type State = UsersInner;
    fn apply(&self, state: &mut UsersInner, timestamp: IggyTimestamp) -> ApplyReply {
        let expiry = IggyExpiry::from(self.expiry);
        let user_tokens = state
            .personal_access_tokens
            .entry(self.user_id)
            .or_default();
        if user_tokens.contains_key(self.name.as_str()) {
            return ApplyReply::err(CreatePersonalAccessTokenResult::AlreadyExists);
        }

        let expiry_at = PersonalAccessToken::calculate_expiry_at(timestamp, expiry);
        if let Some(expiry_at) = expiry_at
            && expiry_at.as_micros() <= timestamp.as_micros()
        {
            return ApplyReply::err(CreatePersonalAccessTokenResult::InvalidExpiry);
        }

        // Replicas store the primary's hash directly via `raw`. Calling
        // `PersonalAccessToken::new` here would re-roll `ring::rand` per
        // replica and diverge `personal_access_token_index` -- a
        // deterministic-apply violation.
        let token_hash_str = std::str::from_utf8(&self.token_hash)
            .expect("token_hash is hex-ASCII generated by the primary");
        let pat =
            PersonalAccessToken::raw(self.user_id, self.name.as_ref(), token_hash_str, expiry_at);
        // `raw` already minted one `Arc<str>` for the name; share that single
        // allocation across the map key and both indices rather than
        // re-allocating the name per consumer.
        let name: Arc<str> = Arc::clone(&pat.name);
        let token_hash = Arc::clone(&pat.token);
        user_tokens.insert(Arc::clone(&name), pat);
        if let Some(expiry_at) = expiry_at {
            state.personal_access_token_expiry_index.insert((
                expiry_at.as_micros(),
                self.user_id,
                Arc::clone(&name),
            ));
        }
        state
            .personal_access_token_index
            .insert(token_hash, (self.user_id, name));
        ApplyReply::ok(Bytes::new())
    }
}

impl StateHandler for DeletePersonalAccessTokenRequest {
    type State = UsersInner;
    fn apply(&self, state: &mut UsersInner, timestamp: IggyTimestamp) -> ApplyReply {
        let Some(user_tokens) = state.personal_access_tokens.get_mut(&self.user_id) else {
            return ApplyReply::err(DeletePersonalAccessTokenResult::NotFound);
        };
        let name_arc: Arc<str> = Arc::from(self.name.as_str());
        if self.only_if_expired {
            // Cleaner-originated. Re-check the *currently stored* token
            // against this prepare's replicated timestamp and skip unless
            // it is still expired. A token deleted and recreated under the
            // same name with a fresh expiry between the cleaner's snapshot
            // and this commit is ordered before this delete, so apply sees
            // the new expiry and preserves it. A never-expiring recreate
            // (`expiry_at == None`) is likewise preserved. A gated delete of
            // an already-gone token is a deterministic no-op success, not a
            // rejection, so the cleaner stays idempotent under replay.
            let still_expired = user_tokens
                .get(&name_arc)
                .and_then(|pat| pat.expiry_at)
                .is_some_and(|expiry_at| expiry_at.as_micros() <= timestamp.as_micros());
            if !still_expired {
                return ApplyReply::ok(Bytes::new());
            }
        }
        match user_tokens.remove(&name_arc) {
            Some(pat) => {
                state.personal_access_token_index.remove(&pat.token);
                if let Some(expiry_at) = pat.expiry_at {
                    state.personal_access_token_expiry_index.remove(&(
                        expiry_at.as_micros(),
                        self.user_id,
                        name_arc,
                    ));
                }
                ApplyReply::ok(Bytes::new())
            }
            None => ApplyReply::err(DeletePersonalAccessTokenResult::NotFound),
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

    #[allow(clippy::cast_possible_truncation)]
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
        let mut personal_access_token_index: AHashMap<Arc<str>, (UserId, Arc<str>)> =
            AHashMap::new();
        let mut personal_access_token_expiry_index: BTreeSet<(u64, UserId, Arc<str>)> =
            BTreeSet::new();
        for (user_id, tokens) in snapshot.personal_access_tokens {
            let mut token_map: AHashMap<Arc<str>, PersonalAccessToken> = AHashMap::new();
            for (name, pat_snap) in tokens {
                let name: Arc<str> = Arc::from(name.as_str());
                let pat = PersonalAccessToken::raw(
                    pat_snap.user_id,
                    &pat_snap.name,
                    &pat_snap.token,
                    pat_snap.expiry_at,
                );
                if let Some(expiry_at) = pat.expiry_at {
                    personal_access_token_expiry_index.insert((
                        expiry_at.as_micros(),
                        user_id,
                        Arc::clone(&name),
                    ));
                }
                personal_access_token_index
                    .insert(Arc::clone(&pat.token), (user_id, Arc::clone(&name)));
                token_map.insert(name, pat);
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
            personal_access_token_index,
            personal_access_token_expiry_index,
            permissioner,
            last_result: None,
        };
        Ok(inner.into())
    }
}

impl_fill_restore!(Users, users);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_pat_request_roundtrip_preserves_user_id() {
        let request = CreatePersonalAccessTokenRequest {
            user_id: 7,
            name: WireName::new("api-token").unwrap(),
            expiry: 3600,
            token_hash: [b'a'; PAT_TOKEN_HASH_BYTES],
        };

        let bytes = request.to_bytes();
        let (decoded, consumed) = CreatePersonalAccessTokenRequest::decode(&bytes).unwrap();

        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, request);
    }

    #[test]
    fn delete_pat_request_roundtrip_preserves_user_id() {
        let request = DeletePersonalAccessTokenRequest {
            user_id: 11,
            name: WireName::new("staging").unwrap(),
            only_if_expired: true,
        };

        let bytes = request.to_bytes();
        let (decoded, consumed) = DeletePersonalAccessTokenRequest::decode(&bytes).unwrap();

        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, request);
    }

    #[test]
    fn pat_apply_uses_authenticated_user_id() {
        let mut users = UsersInner::new();
        let create = CreatePersonalAccessTokenRequest {
            user_id: 5,
            name: WireName::new("deploy").unwrap(),
            expiry: 0,
            token_hash: [b'a'; PAT_TOKEN_HASH_BYTES],
        };
        create.apply(&mut users, IggyTimestamp::now());

        assert!(users.personal_access_tokens.contains_key(&5));
        assert!(!users.personal_access_tokens.contains_key(&0));
        let token = users.personal_access_tokens[&5]
            .get("deploy")
            .expect("token should be stored under the authenticated user");
        assert_eq!(token.user_id, 5);

        let delete = DeletePersonalAccessTokenRequest {
            user_id: 5,
            name: WireName::new("deploy").unwrap(),
            only_if_expired: false,
        };
        delete.apply(&mut users, IggyTimestamp::now());

        assert!(users.personal_access_tokens[&5].is_empty());
        assert!(users.personal_access_token_index.is_empty());
    }

    #[test]
    fn delete_keeps_expiry_index_in_sync() {
        let mut users = UsersInner::new();
        // Created at the epoch with a 1-unit expiry: a `Some(expiry_at)`, so
        // it lands in the expiry index (a never-expiring token would not).
        CreatePersonalAccessTokenRequest {
            user_id: 7,
            name: WireName::new("ci").unwrap(),
            expiry: 1,
            token_hash: [b'a'; PAT_TOKEN_HASH_BYTES],
        }
        .apply(&mut users, IggyTimestamp::zero());
        assert_eq!(users.personal_access_token_expiry_index.len(), 1);

        DeletePersonalAccessTokenRequest {
            user_id: 7,
            name: WireName::new("ci").unwrap(),
            only_if_expired: false,
        }
        .apply(&mut users, IggyTimestamp::zero());
        assert!(users.personal_access_token_expiry_index.is_empty());
    }

    #[test]
    fn expiry_gated_delete_skips_token_until_expired() {
        let mut users = UsersInner::new();
        // Created at the epoch with a 1-unit expiry, so it lands in the expiry
        // index with a `Some(expiry_at)` just after the epoch.
        CreatePersonalAccessTokenRequest {
            user_id: 3,
            name: WireName::new("foo").unwrap(),
            expiry: 1,
            token_hash: [b'a'; PAT_TOKEN_HASH_BYTES],
        }
        .apply(&mut users, IggyTimestamp::zero());

        // Gated delete stamped at the epoch: the token's expiry is still in the
        // future relative to this prepare, so the cleaner-style delete is a
        // no-op. Models a fresh recreate racing the cleaner.
        DeletePersonalAccessTokenRequest {
            user_id: 3,
            name: WireName::new("foo").unwrap(),
            only_if_expired: true,
        }
        .apply(&mut users, IggyTimestamp::zero());
        assert!(users.personal_access_tokens[&3].contains_key("foo"));
        assert_eq!(users.personal_access_token_expiry_index.len(), 1);

        // Gated delete stamped now (>> the epoch+1 expiry): genuinely expired,
        // so it is removed and the indices stay in sync.
        DeletePersonalAccessTokenRequest {
            user_id: 3,
            name: WireName::new("foo").unwrap(),
            only_if_expired: true,
        }
        .apply(&mut users, IggyTimestamp::now());
        assert!(users.personal_access_tokens[&3].is_empty());
        assert!(users.personal_access_token_expiry_index.is_empty());
        assert!(users.personal_access_token_index.is_empty());
    }

    #[test]
    fn expired_personal_access_tokens_collects_only_expired() {
        let mut users = UsersInner::new();
        // Created at the epoch with a 1-unit expiry: expired relative to now.
        for (user_id, name, fill) in [(5u32, "expired", b'a'), (9u32, "stale", b'b')] {
            CreatePersonalAccessTokenRequest {
                user_id,
                name: WireName::new(name).unwrap(),
                expiry: 1,
                token_hash: [fill; PAT_TOKEN_HASH_BYTES],
            }
            .apply(&mut users, IggyTimestamp::zero());
        }
        // expiry 0 -> ServerDefault -> no expiry_at, so never collected as expired.
        CreatePersonalAccessTokenRequest {
            user_id: 5,
            name: WireName::new("forever").unwrap(),
            expiry: 0,
            token_hash: [b'c'; PAT_TOKEN_HASH_BYTES],
        }
        .apply(&mut users, IggyTimestamp::zero());

        let mut expired = users.expired_personal_access_tokens(IggyTimestamp::now());
        expired.sort();
        assert_eq!(
            expired,
            vec![(5, Arc::from("expired")), (9, Arc::from("stale"))]
        );
    }

    fn create_user(users: &mut UsersInner, username: &str) {
        let request = CreateUserRequest {
            username: WireName::new(username).unwrap(),
            password: "hash".to_owned(),
            status: 1,
            permissions: None,
        };
        let apply = StateHandler::apply(&request, users, IggyTimestamp::now());
        assert_eq!(apply.code, 0);
    }

    #[test]
    fn given_duplicate_username_when_apply_create_user_should_return_user_already_exists() {
        let mut users = UsersInner::new();
        create_user(&mut users, "alice");
        let request = CreateUserRequest {
            username: WireName::new("alice").unwrap(),
            password: "hash".to_owned(),
            status: 1,
            permissions: None,
        };
        let apply = StateHandler::apply(&request, &mut users, IggyTimestamp::now());
        assert_eq!(apply.code, u32::from(CreateUserResult::UserAlreadyExists));
        assert!(apply.body.is_empty());
    }

    #[test]
    fn given_missing_user_when_apply_delete_user_should_return_user_not_found() {
        let mut users = UsersInner::new();
        let request = DeleteUserRequest {
            user_id: WireIdentifier::numeric(999),
        };
        let apply = StateHandler::apply(&request, &mut users, IggyTimestamp::now());
        assert_eq!(apply.code, u32::from(DeleteUserResult::UserNotFound));
    }
}
