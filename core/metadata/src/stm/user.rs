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
use crate::stm::id_slab::IdSlab;
use crate::stm::result::{
    ApplyReply, ChangePasswordResult, CreatePersonalAccessTokenResult, CreateUserResult,
    DeletePersonalAccessTokenResult, DeleteUserResult, DeletedResource, UpdatePermissionsResult,
    UpdateUserResult,
};
use crate::stm::snapshot::Snapshotable;
use crate::stm::stream::{PatchError, patched_options, public_reply_options};
use crate::stm::{ApplyContext, StateHandler};
use crate::{collect_handlers, define_state, impl_fill_restore};
use ahash::AHashMap;
use bytes::Bytes;
use bytes::{BufMut, BytesMut};
#[cfg(any(test, feature = "simulator"))]
use iggy_binary_protocol::WireOptions;
use iggy_binary_protocol::codec::{WireDecode, WireEncode, read_u32_le, read_u64_le};
#[cfg(any(test, feature = "simulator"))]
use iggy_binary_protocol::primitives::permissions::{WireGlobalPermissions, WirePermissions};
use iggy_binary_protocol::requests::users::{
    ChangePasswordRequest, CreateUserRequest, DeleteUserRequest, UpdatePermissionsRequest,
    UpdateUserResolvedRequest,
};
use iggy_binary_protocol::responses::users::get_user::UserDetailsResponse;
use iggy_binary_protocol::responses::users::user_response::UserResponse;
use iggy_binary_protocol::{WireIdentifier, WireName};
use iggy_common::defaults::{DEFAULT_ROOT_USER_ID, MAX_USERNAME_LENGTH, MIN_USERNAME_LENGTH};
use iggy_common::wire_conversions::resource_options_from_wire;
use iggy_common::{
    GlobalPermissions, IggyError, IggyExpiry, IggyTimestamp, OptionsScope, Permissions,
    PersonalAccessToken, ResourceOptions, StreamPermissions, UserId, UserStatus,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;

/// Fixed timestamp for root fixtures in tests and the simulator.
#[cfg(any(test, feature = "simulator"))]
pub const ROOT_USER_CREATED_AT_MICROS: u64 = 1;

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
    pub options: ResourceOptions,
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
            options: ResourceOptions::new(),
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
            options: ResourceOptions::new(),
        }
    }
}

define_state! {
    Users {
        pub(crate) index: AHashMap<Arc<str>, UserId>,
        pub items: IdSlab<User>,
        pub personal_access_tokens: AHashMap<UserId, AHashMap<Arc<str>, PersonalAccessToken>>,
        // SAFETY: deterministic-apply invariant. `AHashMap` iteration order
        // differs across replicas (random seed), so this map MUST only be
        // touched via single-key `.get` / `.insert` / `.remove`. Never
        // iterate. Reach for `BTreeMap` the first time iteration is needed.
        pub personal_access_token_index: AHashMap<Arc<str>, (UserId, Arc<str>)>,
        // Expiry-ordered index of expiring PATs: `(expiry_micros, user_id,
        // name)`. Never-expiring tokens are absent. Unlike the sibling
        // `AHashMap` index above, a `BTreeSet` is safe to iterate (its order
        // is deterministic across replicas), which lets the PAT cleaner find
        // expired tokens in O(log n) per tick instead of scanning every token.
        pub(crate) personal_access_token_expiry_index: BTreeSet<(u64, UserId, Arc<str>)>,
        pub(crate) permissioner: Permissioner,
    }
}

collect_handlers! {
    Users {
        CreateUser,
        UpdateUserResolved,
        DeleteUser,
        ChangePassword,
        UpdatePermissions,
        CreatePersonalAccessToken,
        DeletePersonalAccessToken,
    }
    internal { RemoveGrants }
}

impl UsersInner {
    #[must_use]
    pub fn is_active_account(&self, user_id: u32, created_at: u64) -> bool {
        self.items.get(user_id as usize).is_some_and(|user| {
            user.status == UserStatus::Active && user.created_at.as_micros() == created_at
        })
    }

    /// Run a grant check in the same snapshot as other user checks.
    ///
    /// # Errors
    /// Propagates the rule's `IggyError` on denial.
    pub fn authorize(
        &self,
        rule: impl FnOnce(&Permissioner) -> Result<(), IggyError>,
    ) -> Result<(), IggyError> {
        rule(&self.permissioner)
    }

    /// Resolve a wire user identifier to its committed slab id, `None` when
    /// the user does not exist.
    #[must_use]
    pub fn resolve_user_id(&self, identifier: &WireIdentifier) -> Option<usize> {
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

    /// Committed user record named `username`, `None` when no such user
    /// exists.
    #[must_use]
    pub fn user_by_name(&self, username: &str) -> Option<&User> {
        self.index
            .get(username)
            .and_then(|&id| self.items.get(id as usize))
    }

    /// Id and stored password hash of the user named by `identifier`, `None`
    /// when the user does not resolve.
    #[must_use]
    pub fn resolve_password_hash(&self, identifier: &WireIdentifier) -> Option<(UserId, Arc<str>)> {
        self.resolve_user_id(identifier)
            .and_then(|user_id| self.items.get(user_id))
            .map(|user| (user.id, Arc::clone(&user.password_hash)))
    }

    /// Clone the committed permissions of the user at slab `id`, if any.
    ///
    /// The permissioner is a denormalized index of `User.permissions`; both
    /// apply paths feed it from here, reading the just-stored user so the two
    /// never drift.
    #[must_use]
    fn permissions_of(&self, id: usize) -> Option<Permissions> {
        self.items
            .get(id)
            .and_then(|user| user.permissions.as_deref().cloned())
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

    /// Collect `(name, expiry_at)` for every live personal access token of
    /// one user, sorted by name.
    ///
    /// Read-only accessor for the non-replicated list path (the caller lists
    /// its own tokens). The backing per-user map is an `AHashMap`, whose
    /// iteration order is seeded per process, so the collected list is
    /// sorted before it is returned. Never lift this iteration into an apply
    /// handler: apply must stay deterministic across replicas (see the
    /// invariant on `personal_access_token_index`).
    #[must_use]
    pub fn personal_access_tokens_of(
        &self,
        user_id: UserId,
    ) -> Vec<(Arc<str>, Option<IggyTimestamp>)> {
        let mut tokens: Vec<_> = self
            .personal_access_tokens
            .get(&user_id)
            .map(|user_tokens| {
                user_tokens
                    .values()
                    .map(|pat| (Arc::clone(&pat.name), pat.expiry_at))
                    .collect()
            })
            .unwrap_or_default();
        tokens.sort_by(|(left, _), (right, _)| left.cmp(right));
        tokens
    }

    /// Count of live personal access tokens for one user.
    ///
    /// Single-map read for the primary's create cap; the sibling
    /// `personal_access_tokens_of` allocates and sorts, too heavy for a
    /// per-request check.
    #[must_use]
    pub fn pat_count_of(&self, user_id: UserId) -> usize {
        self.personal_access_tokens
            .get(&user_id)
            .map_or(0, |user_tokens| user_tokens.len())
    }

    /// The checks `CreateUserRequest::apply` makes before it mutates state, and
    /// the decoded options when they pass. `reject_dangling_grant` runs them
    /// before it checks the grants, so their refusal wins.
    pub(crate) fn validate_create_user(
        &self,
        request: &CreateUserRequest,
    ) -> Result<ResourceOptions, CreateUserResult> {
        // `WireName` permits 1..=255 bytes, so a raw binary client bypasses the
        // length bound the edges (HTTP DTO, SDK, login) enforce. Re-check in the
        // replicated apply -- the choke point every transport shares -- so state
        // never holds a username no ingress considers valid. Rejected before any
        // mutation, committing as a deterministic no-op.
        if !(MIN_USERNAME_LENGTH..=MAX_USERNAME_LENGTH).contains(&request.username.as_str().len()) {
            return Err(CreateUserResult::InvalidUsername);
        }
        if self.index.contains_key(request.username.as_str()) {
            return Err(CreateUserResult::UserAlreadyExists);
        }
        resource_options_from_wire(&request.options, true)
            .map_err(|_| CreateUserResult::InvalidOptionValue)
    }

    /// Slab id of the user whose grants `UpdatePermissionsRequest::apply`
    /// replaces, or the refusal of that apply. `reject_dangling_grant` runs this
    /// before it checks the grants, so the refusal wins.
    pub(crate) fn permissions_target(
        &self,
        identifier: &WireIdentifier,
    ) -> Result<usize, UpdatePermissionsResult> {
        let user_id = self
            .resolve_user_id(identifier)
            .ok_or(UpdatePermissionsResult::UserNotFound)?;
        // Root (slab id 0) permissions are immutable, matching legacy
        // `CannotChangePermissions`. Rejected before any mutation, so the op is a
        // committed no-op: root keeps its full grants and the permissioner index
        // is untouched.
        if user_id == DEFAULT_ROOT_USER_ID as usize {
            return Err(UpdatePermissionsResult::CannotChangePermissions);
        }
        Ok(user_id)
    }

    /// Drop the token map of `user_id` with the index and expiry entries of
    /// every token in it.
    fn remove_personal_access_tokens(&mut self, user_id: UserId) {
        let Some(tokens) = self.personal_access_tokens.remove(&user_id) else {
            return;
        };
        // Removals commute, so the per-replica iteration order of `tokens`
        // cannot make replicas diverge.
        for pat in tokens.values() {
            self.personal_access_token_index.remove(&pat.token);
            if let Some(expiry_at) = pat.expiry_at {
                self.personal_access_token_expiry_index.remove(&(
                    expiry_at.as_micros(),
                    user_id,
                    Arc::clone(&pat.name),
                ));
            }
        }
    }
}

impl Users {
    pub fn is_active_account(&self, user_id: u32, created_at: u64) -> bool {
        self.read(|users| users.is_active_account(user_id, created_at))
    }

    #[must_use]
    pub fn read<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&UsersInner) -> R,
    {
        self.inner.read(f)
    }

    /// Run a permissioner rule against the committed user permissions and
    /// return its decision. The dispatch-time authorization surface for the
    /// checks the in-apply RBAC gate cannot cover: non-replicated reads and
    /// partition-plane ops, both decided on the connection's own shard rather
    /// than in a replicated apply. The permissioner stays private; callers pass
    /// the rule closure.
    ///
    /// # Errors
    /// Propagates the rule's `IggyError` (always `Unauthorized`) on denial.
    pub fn authorize(
        &self,
        rule: impl FnOnce(&Permissioner) -> Result<(), IggyError>,
    ) -> Result<(), IggyError> {
        self.read(|inner| inner.authorize(rule))
    }

    /// Drop every grant on a stream or topic that a committed delete removed.
    /// Applied through the writer as a deterministic side effect of that delete
    /// on each replica (not a separate replicated op), so WAL replay repeats it.
    pub(crate) fn remove_grants(
        &self,
        deleted: DeletedResource,
        timestamp: IggyTimestamp,
        context: ApplyContext,
    ) {
        let cmd = UsersCommand::RemoveGrants(RemoveGrantsRequest { deleted }, timestamp, context);
        if let Err(error) = self.inner.try_apply(cmd) {
            tracing::error!(?deleted, %error, "remove_grants dispatched to reader-only Users STM");
        }
    }

    /// Insert a root fixture into an empty user set for tests and the simulator.
    ///
    /// # Panics
    ///
    /// Panics if `username` is not a valid wire-format username or its length is
    /// outside `MIN_USERNAME_LENGTH..=MAX_USERNAME_LENGTH`.
    #[cfg(any(test, feature = "simulator"))]
    pub fn ensure_root_user(&self, username: &str, password_hash: &str) {
        if self.read(|users| !users.items.is_empty()) {
            return;
        }

        let length = username.len();
        assert!(
            (MIN_USERNAME_LENGTH..=MAX_USERNAME_LENGTH).contains(&length),
            "root fixture username length {length} outside {MIN_USERNAME_LENGTH}..={MAX_USERNAME_LENGTH}"
        );

        // Fixture setup runs before simulated traffic, so the read/apply split
        // cannot race another user creation.
        let username = WireName::new(username).expect("root username must be valid");
        // Fixtures bypass replication and need a fixed non-zero timestamp
        // so snapshots remain deterministic across simulated replicas.
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
                    options: WireOptions::empty(),
                },
                IggyTimestamp::from(ROOT_USER_CREATED_AT_MICROS),
                ApplyContext::default(),
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
        let options = match state.validate_create_user(self) {
            Ok(options) => options,
            Err(result) => return ApplyReply::err(result),
        };
        // The reply is cached for retries, so it is projected before caching.
        let reply_options = public_reply_options(OptionsScope::User, &options);

        let username_arc: Arc<str> = Arc::from(self.username.as_str());
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
            options,
        };

        let id = state.items.insert(user);
        if let Some(user) = state.items.get_mut(id) {
            user.id = id as UserId;
        }

        state.index.insert(username_arc, id as UserId);
        // A reused ID must not inherit token-index entries from its previous holder.
        state.remove_personal_access_tokens(id as UserId);
        state
            .personal_access_tokens
            .insert(id as UserId, AHashMap::default());

        let user_permissions = state.permissions_of(id);
        state
            .permissioner
            .init_permissions_for_user(id as UserId, user_permissions);

        // Reply body: the SDK `create_user` decodes a `UserDetailsResponse`.
        // Serialization local to this state machine.
        ApplyReply::ok(
            UserDetailsResponse {
                user: UserResponse {
                    id: id as u32,
                    created_at: timestamp.as_micros(),
                    status: self.status,
                    username: self.username.clone(),
                    options: reply_options.0,
                    derived_options: reply_options.1,
                },
                permissions: self.permissions.clone(),
            }
            .to_bytes(),
        )
    }
}

impl StateHandler for UpdateUserResolvedRequest {
    type State = UsersInner;
    #[allow(clippy::cast_possible_truncation)]
    fn apply(&self, state: &mut UsersInner, _timestamp: IggyTimestamp) -> ApplyReply {
        let Some(user_id) = state.resolve_user_id(&self.user_id) else {
            return ApplyReply::err(UpdateUserResult::UserNotFound);
        };
        let Some(user) = state.items.get(user_id) else {
            return ApplyReply::err(UpdateUserResult::UserNotFound);
        };

        if user_id as u32 == DEFAULT_ROOT_USER_ID
            && (self.username.is_some() || self.status.is_some())
            && (state.apply_context.user_id != DEFAULT_ROOT_USER_ID
                || self.status == Some(UserStatus::Inactive.as_code()))
        {
            return ApplyReply::err(UpdateUserResult::CannotModifyRootUser);
        }

        // Every check runs before any mutation, so a refused patch leaves the
        // user exactly as it was: no half-applied rename or status change.
        let new_username: Option<Arc<str>> =
            self.username.as_ref().map(|name| Arc::from(name.as_str()));
        if let Some(new_username) = &new_username {
            // Same bound as CreateUser apply: a rename must not smuggle in a
            // username the edges reject.
            if !(MIN_USERNAME_LENGTH..=MAX_USERNAME_LENGTH).contains(&new_username.len()) {
                return ApplyReply::err(UpdateUserResult::InvalidUsername);
            }
            if let Some(&existing_id) = state.index.get(new_username)
                && existing_id != user_id as UserId
            {
                return ApplyReply::err(UpdateUserResult::UsernameAlreadyExists);
            }
        }
        let Ok(new_status) = self.status.map(UserStatus::from_code).transpose() else {
            return ApplyReply::err(UpdateUserResult::InvalidUserStatus);
        };
        let options =
            match patched_options(&user.options, &self.explicit_options, &self.derived_options) {
                Ok(options) => options,
                Err(PatchError::InvalidValue) => {
                    return ApplyReply::err(UpdateUserResult::InvalidOptionValue);
                }
                Err(PatchError::TooLarge) => {
                    return ApplyReply::err(UpdateUserResult::OptionsBlockTooLarge);
                }
            };

        let Some(user) = state.items.get_mut(user_id) else {
            return ApplyReply::err(UpdateUserResult::UserNotFound);
        };
        if let Some(new_username) = new_username {
            state.index.remove(&user.username);
            user.username = new_username.clone();
            state.index.insert(new_username, user_id as UserId);
        }
        if let Some(new_status) = new_status {
            user.status = new_status;
        }
        user.options = options;
        ApplyReply {
            revoked_user: (user.status != UserStatus::Active).then_some(user_id as UserId),
            ..ApplyReply::ok(Bytes::new())
        }
    }
}

impl StateHandler for DeleteUserRequest {
    type State = UsersInner;
    #[allow(clippy::cast_possible_truncation)]
    fn apply(&self, state: &mut UsersInner, _timestamp: IggyTimestamp) -> ApplyReply {
        let Some(user_id) = state.resolve_user_id(&self.user_id) else {
            return ApplyReply::err(DeleteUserResult::UserNotFound);
        };

        // Root (slab id 0) is undeletable, matching legacy `CannotDeleteUser`.
        // The rejection is deterministic, so every replica keeps slab id 0 as
        // the root user and the in-apply RBAC gate's `user_id == 0` root
        // short-circuit can never be inherited by a recreated user (the slab
        // reuses freed keys, so a deleted root's slot would be reclaimed).
        if user_id == DEFAULT_ROOT_USER_ID as usize {
            return ApplyReply::err(DeleteUserResult::CannotDeleteUser);
        }

        if let Some(user) = state.items.get(user_id) {
            let username = user.username.clone();
            state.items.remove(user_id);
            state.index.remove(&username);
            state.remove_personal_access_tokens(user_id as UserId);
            state
                .permissioner
                .delete_permissions_for_user(user_id as UserId);
        }
        ApplyReply {
            revoked_user: Some(user_id as UserId),
            ..ApplyReply::ok(Bytes::new())
        }
    }
}

impl StateHandler for ChangePasswordRequest {
    type State = UsersInner;
    fn apply(&self, state: &mut UsersInner, _timestamp: IggyTimestamp) -> ApplyReply {
        let Some(user_id) = state.resolve_user_id(&self.user_id) else {
            return ApplyReply::err(ChangePasswordResult::UserNotFound);
        };

        // An empty `new_password` is the primary's signal that the caller's
        // current password did not match or that the target did not resolve
        // there (see the server `verify_and_rewrite_change_password`). The
        // accept path always replicates a non-empty Argon2 hash, so this is
        // unambiguous. Rejecting here (rather than denying pre-consensus)
        // commits the op as a no-op, recording the request id in the
        // ClientTable so a retry of it dedups.
        if self.new_password.is_empty() {
            return ApplyReply::err(ChangePasswordResult::InvalidCredentials);
        }

        if let Some(user) = state.items.get_mut(user_id) {
            user.password_hash = Arc::from(self.new_password.as_str());
        }
        ApplyReply::ok(Bytes::new())
    }
}

impl StateHandler for UpdatePermissionsRequest {
    type State = UsersInner;
    #[allow(clippy::cast_possible_truncation)]
    fn apply(&self, state: &mut UsersInner, _timestamp: IggyTimestamp) -> ApplyReply {
        let user_id = match state.permissions_target(&self.user_id) {
            Ok(user_id) => user_id,
            Err(result) => return ApplyReply::err(result),
        };

        if let Some(user) = state.items.get_mut(user_id) {
            user.permissions = self
                .permissions
                .as_ref()
                .map(|p| Arc::new(Permissions::from(p.clone())));
        }
        let user_permissions = state.permissions_of(user_id);
        state
            .permissioner
            .update_permissions_for_user(user_id as UserId, user_permissions);
        ApplyReply::ok(Bytes::new())
    }
}

/// Server-originated request that drops every grant on a deleted resource.
///
/// Never decoded from the wire: `gated_apply` applies it right after the
/// delete, so every replica and WAL replay remove the same grants.
#[derive(Debug, Clone)]
pub struct RemoveGrantsRequest {
    pub deleted: DeletedResource,
}

impl StateHandler for RemoveGrantsRequest {
    type State = UsersInner;
    fn apply(&self, state: &mut UsersInner, _timestamp: IggyTimestamp) -> ApplyReply {
        for (_, user) in &mut state.items {
            let Some(pruned) = user
                .permissions
                .as_deref()
                .and_then(|permissions| without_grant(permissions, self.deleted))
            else {
                continue;
            };
            let permissioner = &mut state.permissioner;
            match self.deleted {
                DeletedResource::Stream(stream_id) => {
                    let key = (user.id, stream_id);
                    permissioner.users_streams_permissions.remove(&key);
                    permissioner
                        .users_that_can_poll_messages_from_specific_streams
                        .remove(&key);
                    permissioner
                        .users_that_can_send_messages_to_specific_streams
                        .remove(&key);
                }
                DeletedResource::Topic {
                    stream_id,
                    topic_id,
                } => {
                    if let Some(stream) = permissioner
                        .users_streams_permissions
                        .get_mut(&(user.id, stream_id))
                        && let Some(topics) = &mut stream.topics
                    {
                        topics.remove(&topic_id);
                        if topics.is_empty() {
                            stream.topics = None;
                        }
                    }
                }
            }
            user.permissions = Some(Arc::new(pruned));
        }
        ApplyReply::ok(Bytes::new())
    }
}

/// `permissions` without its grant on `deleted`, `None` when it holds none. An
/// emptied map becomes `None`, the shape a wire decode of an empty list gives.
fn without_grant(permissions: &Permissions, deleted: DeletedResource) -> Option<Permissions> {
    let (DeletedResource::Stream(stream_id) | DeletedResource::Topic { stream_id, .. }) = deleted;
    let stream = permissions.streams.as_ref()?.get(&stream_id)?;
    // Most users hold no grant on the deleted resource: check before cloning.
    if let DeletedResource::Topic { topic_id, .. } = deleted
        && !stream.topics.as_ref()?.contains_key(&topic_id)
    {
        return None;
    }
    let mut pruned = permissions.clone();
    let streams = pruned.streams.as_mut()?;
    match deleted {
        DeletedResource::Stream(_) => {
            streams.remove(&stream_id);
        }
        DeletedResource::Topic { topic_id, .. } => {
            let stream = streams.get_mut(&stream_id)?;
            let topics = stream.topics.as_mut()?;
            topics.remove(&topic_id)?;
            if topics.is_empty() {
                stream.topics = None;
            }
        }
    }
    if streams.is_empty() {
        pruned.streams = None;
    }
    Some(pruned)
}

/// The permissioner is a denormalized index of `User.permissions`, so it is
/// built from the users alone and cannot drift from them. Slab iteration is
/// ascending by key, so the build is deterministic.
#[allow(clippy::cast_possible_truncation)]
fn permissioner_of(items: &IdSlab<User>) -> Permissioner {
    let mut permissioner = Permissioner::new();
    for (user_id, user) in items {
        permissioner
            .init_permissions_for_user(user_id as UserId, user.permissions.as_deref().cloned());
    }
    permissioner
}

/// The success reply here is deliberately empty: the raw token the caller needs
/// is the one thing this apply must never see.
///
/// The primary mints the raw token and its hash at ingress (server
/// `pat::rewrite_pat_request_for_user`) and replicates only the hash. Minting
/// inside this apply would call `ring::rand` on every replica and diverge the
/// token index, and replicating the raw token would persist a live credential in
/// every WAL and snapshot. So the raw token leaves the primary by a side channel
/// (`maybe_rewrite_pat_request` returns it alongside the rewritten request) and
/// the home shard splices it into this op's reply as a typed
/// `RawPersonalAccessTokenResponse` (server `reply_frame::build_raw_pat_reply`).
///
/// One consequence rides on that: the secret exists only on the wire of the
/// original reply, so a replayed request with a re-minted hash cannot be served
/// from the client-table cache. `impls::metadata::unreplayable_secret_refusal`
/// refuses it instead.
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

        let Ok(expiry_at) = PersonalAccessToken::calculate_expiry_at(timestamp, expiry) else {
            return ApplyReply::err(CreatePersonalAccessTokenResult::InvalidExpiry);
        };
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
    #[serde(default)]
    pub options: ResourceOptions,
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
///
/// Serialized-form invariant (see [`crate::stm::snapshot::MetadataSnapshot`]):
/// `items`, `personal_access_tokens`, and the permissioner's maps stay ordered
/// (`Vec` / `BTreeMap`) even though the runtime holds them in `AHashMap`s. Swapping
/// any to an unordered map breaks the checkpoint checksum cross-check.
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
                            options: user.options.clone(),
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

            // The permissioner is a denormalized index of `User.permissions`.
            // Its `AHashMap`/`AHashSet` fields iterate in a per-replica-random
            // order, so serializing them would make the snapshot bytes differ
            // across replicas. Persist empty vecs (kept only for the positional
            // msgpack layout) and rebuild the index from the users on restore.
            let permissioner = PermissionerSnapshot {
                users_permissions: Vec::new(),
                users_streams_permissions: Vec::new(),
                users_that_can_poll_messages_from_all_streams: Vec::new(),
                users_that_can_send_messages_to_all_streams: Vec::new(),
                users_that_can_poll_messages_from_specific_streams: Vec::new(),
                users_that_can_send_messages_to_specific_streams: Vec::new(),
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
        Ok(UsersInner::inner_from_snapshot(snapshot).into())
    }
}

impl UsersInner {
    /// Rebuild from a snapshot section IN PLACE (state transfer), absorbed on
    /// both left-right buffers.
    ///
    /// Nothing here is shared across buffers the way `StreamsInner`'s stats
    /// registry is, so a wholesale replace is correct.
    pub(crate) fn restore_in_place(&mut self, snapshot: UsersSnapshot) {
        *self = Self::inner_from_snapshot(snapshot);
    }

    /// Build a complete `UsersInner` from a snapshot section. Shared by
    /// wrapper construction ([`Snapshotable::from_snapshot`]) and the
    /// in-place restore command (state transfer), which absorbs it on both
    /// left-right buffers.
    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn inner_from_snapshot(snapshot: UsersSnapshot) -> Self {
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
                options: user_snap.options,
            };

            index.insert(username, slab_key as UserId);
            user_entries.push((slab_key, user));
        }

        let items: IdSlab<User> = user_entries.into_iter().collect();

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

        // Rebuild the permissioner from the restored users rather than the
        // snapshot vecs, which are intentionally empty (see `to_snapshot`).
        let permissioner = permissioner_of(&items);

        Self {
            index,
            items,
            personal_access_tokens,
            personal_access_token_index,
            personal_access_token_expiry_index,
            permissioner,
            last_result: None,
            apply_context: ApplyContext::default(),
        }
    }
}

impl_fill_restore!(Users, users);

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::primitives::permissions::{
        WireStreamPermissions, WireTopicPermissions,
    };
    use iggy_common::wire_conversions::resource_options_to_wire;
    use iggy_common::{
        HeaderKey, HeaderValue, IggyError, OptionKey, OptionValue, OptionsProvenance,
    };

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
    fn pat_count_of_tracks_create_and_delete() {
        let mut users = UsersInner::new();
        assert_eq!(users.pat_count_of(9), 0);

        for (name, hash_byte) in [("first", b'a'), ("second", b'b')] {
            CreatePersonalAccessTokenRequest {
                user_id: 9,
                name: WireName::new(name).unwrap(),
                expiry: 0,
                token_hash: [hash_byte; PAT_TOKEN_HASH_BYTES],
            }
            .apply(&mut users, IggyTimestamp::now());
        }
        assert_eq!(users.pat_count_of(9), 2);
        assert_eq!(users.pat_count_of(1), 0, "count is scoped per user");

        DeletePersonalAccessTokenRequest {
            user_id: 9,
            name: WireName::new("first").unwrap(),
            only_if_expired: false,
        }
        .apply(&mut users, IggyTimestamp::now());
        assert_eq!(users.pat_count_of(9), 1);
    }

    #[test]
    fn given_expiry_past_the_end_of_the_clock_when_creating_token_should_refuse_it() {
        let mut users = UsersInner::new();
        let now = IggyTimestamp::now();
        for expiry in [u64::MAX - now.as_micros() + 1, 0xFFFF_FFFF_0000_0000] {
            let reply = CreatePersonalAccessTokenRequest {
                user_id: 4,
                name: WireName::new("far").unwrap(),
                expiry,
                token_hash: [b'a'; PAT_TOKEN_HASH_BYTES],
            }
            .apply(&mut users, now);

            assert_eq!(
                reply.code,
                u32::from(CreatePersonalAccessTokenResult::InvalidExpiry)
            );
        }
        assert_eq!(users.pat_count_of(4), 0);
        assert!(users.personal_access_token_index.is_empty());
        assert!(users.personal_access_token_expiry_index.is_empty());
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

    #[test]
    fn personal_access_tokens_of_lists_only_callers_sorted_by_name() {
        let mut users = UsersInner::new();
        // Insertion order deliberately not name order; a token for user 9
        // proves the listing is caller-scoped.
        for (user_id, name, expiry, fill) in [
            (5u32, "zeta", 0u64, b'a'),
            (5u32, "alpha", 1, b'b'),
            (9u32, "other", 0, b'c'),
        ] {
            CreatePersonalAccessTokenRequest {
                user_id,
                name: WireName::new(name).unwrap(),
                expiry,
                token_hash: [fill; PAT_TOKEN_HASH_BYTES],
            }
            .apply(&mut users, IggyTimestamp::zero());
        }

        let tokens = users.personal_access_tokens_of(5);
        let names: Vec<&str> = tokens.iter().map(|(name, _)| name.as_ref()).collect();
        assert_eq!(names, ["alpha", "zeta"], "own tokens only, sorted by name");
        assert!(
            tokens[0].1.is_some(),
            "expiring token must carry its expiry_at"
        );
        assert!(
            tokens[1].1.is_none(),
            "never-expiring token must carry no expiry_at"
        );
        assert!(
            users.personal_access_tokens_of(42).is_empty(),
            "a user without tokens lists empty"
        );
    }

    /// User ids come from `items.insert`, and the free list is not part of
    /// `UsersSnapshot`, so the next `CreateUser` id depended on local delete
    /// order rather than committed state. A restored replica would hand a
    /// different id to the same log entry, forking the users table and every
    /// permission keyed off it.
    #[test]
    fn given_descending_deletes_when_round_tripping_a_snapshot_should_keep_the_next_user_id() {
        let mut users = UsersInner::new();
        for username in ["alpha", "bravo", "charlie", "delta"] {
            create_user(&mut users, username);
        }
        // Key 0 is the protected root user, so holes go above it. The highest
        // key stays occupied, or the holes are trailing and a rebuild agrees.
        let keys: Vec<usize> = users.items.iter().map(|(key, _)| key).collect();
        let last = keys.len() - 1;
        for user_id in [keys[last - 1], keys[last - 2]] {
            let request = DeleteUserRequest {
                user_id: WireIdentifier::numeric(u32::try_from(user_id).unwrap()),
            };
            assert_eq!(
                StateHandler::apply(&request, &mut users, IggyTimestamp::now()).code,
                0,
                "user {user_id} must delete"
            );
        }
        let before = users.items.vacant_key();

        let snapshot = Users::from(users.clone()).to_snapshot();
        let restored = UsersInner::inner_from_snapshot(snapshot);

        assert_eq!(
            restored.items.vacant_key(),
            before,
            "the id the next CreateUser is assigned must not depend on whether \
             a snapshot was restored in between"
        );
    }

    fn create_user(users: &mut UsersInner, username: &str) {
        let request = CreateUserRequest {
            username: WireName::new(username).unwrap(),
            password: "hash".to_owned(),
            status: 1,
            permissions: None,
            options: WireOptions::empty(),
        };
        let apply = StateHandler::apply(&request, users, IggyTimestamp::now());
        assert_eq!(apply.code, 0);
    }

    #[test]
    fn given_unknown_critical_key_when_apply_create_user_should_store_it_without_echoing_options() {
        let mut options = ResourceOptions::new();
        options.insert(
            HeaderKey::from(OptionKey::CriticalKeys),
            OptionValue::explicit(HeaderValue::try_from("a_future_key").unwrap()),
        );
        options.insert(
            HeaderKey::try_from("a_future_key").unwrap(),
            OptionValue::explicit(HeaderValue::from(7u64)),
        );
        let request = CreateUserRequest {
            username: WireName::new("alice").unwrap(),
            password: "hash".to_owned(),
            status: 1,
            permissions: None,
            options: resource_options_to_wire(&options, OptionsProvenance::All).unwrap(),
        };
        let mut users = UsersInner::new();

        let apply = StateHandler::apply(&request, &mut users, IggyTimestamp::now());

        assert_eq!(apply.code, 0);
        assert_eq!(users.items.get(0).unwrap().options, options);
        let (reply, _) = UserDetailsResponse::decode(&apply.body).unwrap();
        assert_eq!(
            (reply.user.options, reply.user.derived_options),
            (WireOptions::empty(), WireOptions::empty())
        );
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
            options: WireOptions::empty(),
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

    #[test]
    fn given_root_user_when_apply_delete_user_should_reject_and_keep_root() {
        // Replicated root creation takes the first slab slot, id 0.
        // Deleting it must be rejected in-apply, or
        // a later `CreateUser` would reclaim slot 0 and inherit the RBAC gate's
        // `user_id == 0` root short-circuit.
        let mut users = UsersInner::new();
        let root_id = create_user_with(&mut users, "iggy", None);
        assert_eq!(root_id, DEFAULT_ROOT_USER_ID, "root takes slab id 0");

        let delete_root = DeleteUserRequest {
            user_id: WireIdentifier::numeric(root_id),
        };
        let reply = StateHandler::apply(&delete_root, &mut users, IggyTimestamp::now());
        assert_eq!(reply.code, u32::from(DeleteUserResult::CannotDeleteUser));
        assert!(
            users.items.contains(root_id as usize),
            "root must survive a delete attempt"
        );

        // A non-root user (slab id 1) stays deletable: the guard is scoped to
        // root, so there is no over-block regression.
        let alice_id = create_user_with(&mut users, "alice", None);
        assert_ne!(alice_id, DEFAULT_ROOT_USER_ID);
        let delete_alice = DeleteUserRequest {
            user_id: WireIdentifier::numeric(alice_id),
        };
        let reply = StateHandler::apply(&delete_alice, &mut users, IggyTimestamp::now());
        assert_eq!(reply.code, 0, "non-root deletion still succeeds");
        assert!(!users.items.contains(alice_id as usize));
    }

    #[test]
    fn given_root_user_when_apply_update_permissions_should_reject_and_keep_grants() {
        // Replicated root creation occupies slab id 0. Its
        // permissions must be immutable in-apply, or a delegated manage_users
        // admin could tamper with root's grants.
        let mut users = UsersInner::new();
        let mut root_global = empty_global();
        root_global.manage_servers = true;
        root_global.poll_messages = true;
        let root_id = create_user_with(
            &mut users,
            "iggy",
            Some(WirePermissions {
                global: root_global,
                streams: Vec::new(),
            }),
        );
        assert_eq!(root_id, DEFAULT_ROOT_USER_ID, "root takes slab id 0");
        assert!(users.permissioner.poll_messages(root_id, 1, 1).is_ok());

        // Attempt to strip root's grants: rejected, nothing mutates.
        let strip = UpdatePermissionsRequest {
            user_id: WireIdentifier::numeric(root_id),
            permissions: None,
        };
        let reply = StateHandler::apply(&strip, &mut users, IggyTimestamp::now());
        assert_eq!(
            reply.code,
            u32::from(UpdatePermissionsResult::CannotChangePermissions)
        );
        assert!(
            users
                .items
                .get(root_id as usize)
                .and_then(|user| user.permissions.as_ref())
                .is_some(),
            "root permissions must survive"
        );
        assert!(
            users.permissioner.poll_messages(root_id, 1, 1).is_ok(),
            "root gate access unchanged"
        );

        // A non-root user's permissions can still be updated: no over-block.
        let alice_id = create_user_with(&mut users, "alice", None);
        assert_ne!(alice_id, DEFAULT_ROOT_USER_ID);
        let mut alice_global = empty_global();
        alice_global.poll_messages = true;
        let grant = UpdatePermissionsRequest {
            user_id: WireIdentifier::numeric(alice_id),
            permissions: Some(WirePermissions {
                global: alice_global,
                streams: Vec::new(),
            }),
        };
        let reply = StateHandler::apply(&grant, &mut users, IggyTimestamp::now());
        assert_eq!(reply.code, 0, "non-root permission update still succeeds");
        assert!(users.permissioner.poll_messages(alice_id, 1, 1).is_ok());
    }

    fn empty_global() -> WireGlobalPermissions {
        WireGlobalPermissions {
            manage_servers: false,
            read_servers: false,
            manage_users: false,
            read_users: false,
            manage_streams: false,
            read_streams: false,
            manage_topics: false,
            read_topics: false,
            poll_messages: false,
            send_messages: false,
        }
    }

    fn create_user_with(
        users: &mut UsersInner,
        username: &str,
        permissions: Option<WirePermissions>,
    ) -> UserId {
        let request = CreateUserRequest {
            username: WireName::new(username).unwrap(),
            password: "hash".to_owned(),
            status: 1,
            permissions,
            options: WireOptions::empty(),
        };
        let reply = StateHandler::apply(&request, users, IggyTimestamp::now());
        assert_eq!(reply.code, 0);
        *users.index.get(username).expect("user created")
    }

    #[test]
    fn given_global_poll_grant_when_apply_create_user_should_allow_poll_from_any_stream() {
        let mut users = UsersInner::new();
        let mut global = empty_global();
        global.poll_messages = true;
        let uid = create_user_with(
            &mut users,
            "poller",
            Some(WirePermissions {
                global,
                streams: Vec::new(),
            }),
        );

        // The global poll bit denormalizes into the can-poll-all set, so the
        // user may poll any stream/topic without a stream-specific grant.
        assert!(users.permissioner.poll_messages(uid, 7, 3).is_ok());
        assert!(
            users
                .permissioner
                .users_that_can_poll_messages_from_all_streams
                .contains(&uid)
        );
    }

    #[test]
    fn given_poll_grant_when_apply_update_permissions_removes_it_should_return_unauthorized() {
        let mut users = UsersInner::new();
        // Occupy slab id 0 with the (immutable) root user so the poller under
        // test is a non-root user; root permission changes are rejected in-apply.
        create_user_with(&mut users, "iggy", None);
        let mut global = empty_global();
        global.poll_messages = true;
        let uid = create_user_with(
            &mut users,
            "poller",
            Some(WirePermissions {
                global,
                streams: Vec::new(),
            }),
        );
        assert!(users.permissioner.poll_messages(uid, 1, 1).is_ok());

        let update = UpdatePermissionsRequest {
            user_id: WireIdentifier::numeric(uid),
            permissions: None,
        };
        let reply = StateHandler::apply(&update, &mut users, IggyTimestamp::now());
        assert_eq!(reply.code, 0);

        assert!(matches!(
            users.permissioner.poll_messages(uid, 1, 1),
            Err(IggyError::Unauthorized)
        ));
        assert!(
            !users
                .permissioner
                .users_that_can_poll_messages_from_all_streams
                .contains(&uid)
        );
    }

    #[test]
    fn given_permissioned_user_when_apply_delete_user_should_clear_all_indexes() {
        let mut users = UsersInner::new();
        // Occupy slab id 0 with the (undeletable) root user so the victim under
        // test is a non-root user; root deletion is rejected in-apply.
        create_user_with(&mut users, "iggy", None);
        let mut global = empty_global();
        global.poll_messages = true;
        global.send_messages = true;
        let stream = WireStreamPermissions {
            stream_id: 4,
            manage_stream: false,
            read_stream: false,
            manage_topics: false,
            read_topics: false,
            poll_messages: true,
            send_messages: true,
            topics: Vec::new(),
        };
        let uid = create_user_with(
            &mut users,
            "victim",
            Some(WirePermissions {
                global,
                streams: vec![stream],
            }),
        );
        assert!(!users.permissioner.users_permissions.is_empty());

        let delete = DeleteUserRequest {
            user_id: WireIdentifier::numeric(uid),
        };
        let reply = StateHandler::apply(&delete, &mut users, IggyTimestamp::now());
        assert_eq!(reply.code, 0);

        let permissioner = &users.permissioner;
        assert!(permissioner.users_permissions.is_empty());
        assert!(permissioner.users_streams_permissions.is_empty());
        assert!(
            permissioner
                .users_that_can_poll_messages_from_all_streams
                .is_empty()
        );
        assert!(
            permissioner
                .users_that_can_send_messages_to_all_streams
                .is_empty()
        );
        assert!(
            permissioner
                .users_that_can_poll_messages_from_specific_streams
                .is_empty()
        );
        assert!(
            permissioner
                .users_that_can_send_messages_to_specific_streams
                .is_empty()
        );
    }

    #[test]
    fn given_tokens_left_at_a_freed_id_when_create_user_reuses_it_should_drop_their_index_entries()
    {
        let mut users = UsersInner::new();
        create_user_with(&mut users, "iggy", None);
        let alice = create_user_with(&mut users, "alice", None);
        let delete = DeleteUserRequest {
            user_id: WireIdentifier::numeric(alice),
        };
        let reply = StateHandler::apply(&delete, &mut users, IggyTimestamp::now());
        assert_eq!(reply.code, 0);

        // A token create admitted before the delete applied commits after it.
        let reply = CreatePersonalAccessTokenRequest {
            user_id: alice,
            name: WireName::new("ci").unwrap(),
            expiry: 1,
            token_hash: [b'a'; PAT_TOKEN_HASH_BYTES],
        }
        .apply(&mut users, IggyTimestamp::zero());
        assert_eq!(reply.code, 0);
        assert_eq!(users.personal_access_token_index.len(), 1);
        assert_eq!(users.personal_access_token_expiry_index.len(), 1);

        let bob = create_user_with(&mut users, "bob", None);
        assert_eq!(bob, alice, "bob must take the freed id");
        assert!(users.personal_access_token_index.is_empty());
        assert!(users.personal_access_token_expiry_index.is_empty());
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_shared_grants_when_removing_resource_should_preserve_other_permissions() {
        const DELETED_STREAM: usize = 4;
        const KEPT_STREAM: usize = 5;
        const TOPIC: usize = 2;

        for deleted in [
            DeletedResource::Stream(DELETED_STREAM),
            DeletedResource::Topic {
                stream_id: DELETED_STREAM,
                topic_id: TOPIC,
            },
        ] {
            let mut inner = UsersInner::new();
            create_user_with(&mut inner, "iggy", None);
            let permissions = WirePermissions {
                global: empty_global(),
                streams: [DELETED_STREAM, KEPT_STREAM]
                    .into_iter()
                    .map(|stream_id| WireStreamPermissions {
                        stream_id: u32::try_from(stream_id).unwrap(),
                        manage_stream: false,
                        read_stream: stream_id == KEPT_STREAM,
                        manage_topics: false,
                        read_topics: false,
                        poll_messages: true,
                        send_messages: true,
                        topics: vec![WireTopicPermissions {
                            topic_id: u32::try_from(TOPIC).unwrap(),
                            manage_topic: false,
                            read_topic: true,
                            poll_messages: false,
                            send_messages: false,
                        }],
                    })
                    .collect(),
            };
            let alice = create_user_with(&mut inner, "alice", Some(permissions.clone()));
            let bob = create_user_with(&mut inner, "bob", Some(permissions));
            let mut global = empty_global();
            global.poll_messages = true;
            global.send_messages = true;
            let global_user = create_user_with(
                &mut inner,
                "global",
                Some(WirePermissions {
                    global,
                    streams: Vec::new(),
                }),
            );
            let stream_only = create_user_with(
                &mut inner,
                "stream-only",
                Some(WirePermissions {
                    global: empty_global(),
                    streams: vec![WireStreamPermissions {
                        stream_id: u32::try_from(DELETED_STREAM).unwrap(),
                        manage_stream: false,
                        read_stream: false,
                        manage_topics: false,
                        read_topics: false,
                        poll_messages: true,
                        send_messages: false,
                        topics: Vec::new(),
                    }],
                }),
            );
            let stream_only_record = |inner: &UsersInner| {
                inner
                    .items
                    .get(stream_only as usize)
                    .and_then(|user| user.permissions.clone())
                    .unwrap()
            };
            let stream_only_before = stream_only_record(&inner);
            let keeps_stream = matches!(deleted, DeletedResource::Topic { .. });

            let reply = RemoveGrantsRequest { deleted }.apply(&mut inner, IggyTimestamp::now());
            assert_eq!(reply.code, 0);
            assert_eq!(
                Arc::ptr_eq(&stream_only_before, &stream_only_record(&inner)),
                keeps_stream,
                "a stream delete must replace the stream-only record, a topic delete must keep it"
            );
            let users: Users = inner.into();
            let restored = Users::from_snapshot(users.to_snapshot()).unwrap();
            for users in [&users, &restored] {
                let () = users.read(|inner| {
                    let permissioner = &inner.permissioner;
                    assert_eq!(
                        permissioner
                            .poll_messages(stream_only, DELETED_STREAM, TOPIC)
                            .is_ok(),
                        keeps_stream
                    );
                    for user_id in [alice, bob] {
                        assert!(
                            permissioner
                                .get_topic(user_id, DELETED_STREAM, TOPIC)
                                .is_err()
                        );
                        assert_eq!(
                            permissioner
                                .poll_messages(user_id, DELETED_STREAM, TOPIC)
                                .is_ok(),
                            keeps_stream
                        );
                        assert_eq!(
                            permissioner
                                .append_messages(user_id, DELETED_STREAM, TOPIC)
                                .is_ok(),
                            keeps_stream
                        );
                        assert!(permissioner.get_stream(user_id, KEPT_STREAM).is_ok());
                        assert!(permissioner.get_topic(user_id, KEPT_STREAM, TOPIC).is_ok());
                        assert!(
                            permissioner
                                .poll_messages(user_id, KEPT_STREAM, TOPIC)
                                .is_ok()
                        );
                        assert!(
                            permissioner
                                .append_messages(user_id, KEPT_STREAM, TOPIC)
                                .is_ok()
                        );
                    }
                    assert!(
                        permissioner
                            .poll_messages(global_user, DELETED_STREAM, TOPIC)
                            .is_ok()
                    );
                    assert!(
                        permissioner
                            .append_messages(global_user, DELETED_STREAM, TOPIC)
                            .is_ok()
                    );
                });
            }
        }
    }

    #[test]
    fn given_populated_permissions_when_snapshot_round_trip_should_rebuild_index_and_persist_empty_vecs()
     {
        let mut inner = UsersInner::new();
        let mut global = empty_global();
        global.poll_messages = true;
        global.send_messages = true;
        let stream = WireStreamPermissions {
            stream_id: 2,
            manage_stream: false,
            read_stream: false,
            manage_topics: false,
            read_topics: false,
            poll_messages: true,
            send_messages: true,
            topics: Vec::new(),
        };
        let uid = create_user_with(
            &mut inner,
            "alice",
            Some(WirePermissions {
                global,
                streams: vec![stream],
            }),
        );
        let pre_poll = inner.permissioner.poll_messages(uid, 9, 9).is_ok();
        let pre_send = inner.permissioner.append_messages(uid, 2, 0).is_ok();

        let users: Users = inner.into();
        let snapshot = users.to_snapshot();

        // The derived index is not serialized: every permissioner vec is empty.
        let snap = &snapshot.permissioner;
        assert_eq!(snap.users_permissions, []);
        assert_eq!(snap.users_streams_permissions, []);
        assert_eq!(
            snap.users_that_can_poll_messages_from_all_streams,
            [] as [u32; 0]
        );
        assert_eq!(
            snap.users_that_can_send_messages_to_all_streams,
            [] as [u32; 0]
        );
        assert_eq!(snap.users_that_can_poll_messages_from_specific_streams, []);
        assert_eq!(snap.users_that_can_send_messages_to_specific_streams, []);

        let restored = Users::from_snapshot(snapshot).expect("snapshot restore");
        let (poll_ok, send_ok, in_poll_all, in_send_specific) = restored.read(|inner| {
            let permissioner = &inner.permissioner;
            (
                permissioner.poll_messages(uid, 9, 9).is_ok(),
                permissioner.append_messages(uid, 2, 0).is_ok(),
                permissioner
                    .users_that_can_poll_messages_from_all_streams
                    .contains(&uid),
                permissioner
                    .users_that_can_send_messages_to_specific_streams
                    .contains(&(uid, 2)),
            )
        });

        // Rule results survive the round-trip...
        assert_eq!(poll_ok, pre_poll);
        assert_eq!(send_ok, pre_send);
        // ...because the index was rebuilt from the restored user: the global
        // poll bit repopulated the can-poll-all set and the stream send bit
        // repopulated the specific set.
        assert!(in_poll_all);
        assert!(in_send_specific);
    }

    #[test]
    fn given_root_user_when_ensure_root_after_bootstrap_should_have_root_grants() {
        let users = Users::default();
        users.ensure_root_user("iggy", "hash");
        // Root carries `Permissions::root()`: full grants across every rule.
        let (can_create_stream, can_get_stats, can_poll) = users.read(|inner| {
            let root = *inner.index.get("iggy").expect("root user created");
            (
                inner.permissioner.create_stream(root).is_ok(),
                inner.permissioner.get_stats(root).is_ok(),
                inner.permissioner.poll_messages(root, 1, 1).is_ok(),
            )
        });
        assert!(can_create_stream);
        assert!(can_get_stats);
        assert!(can_poll);
    }

    #[test]
    fn given_out_of_bound_username_when_apply_create_user_should_reject_without_insert() {
        let mut users = UsersInner::new();

        // `WireName` accepts 1..=255 bytes, so a raw binary client can submit a
        // username shorter than the edge-enforced minimum. Apply must reject it.
        for username in [
            "a".repeat(MIN_USERNAME_LENGTH - 1),
            "a".repeat(MAX_USERNAME_LENGTH + 1),
        ] {
            let request = CreateUserRequest {
                username: WireName::new(&username).unwrap(),
                password: "hash".to_owned(),
                status: 1,
                permissions: None,
                options: WireOptions::empty(),
            };
            let reply = StateHandler::apply(&request, &mut users, IggyTimestamp::now());
            assert_eq!(reply.code, u32::from(CreateUserResult::InvalidUsername));
            assert!(reply.body.is_empty());
        }
        assert!(users.items.is_empty(), "no user inserted on reject");
        assert!(users.index.is_empty(), "index untouched on reject");

        // The minimum-length boundary is accepted.
        let username = "a".repeat(MIN_USERNAME_LENGTH);
        let request = CreateUserRequest {
            username: WireName::new(&username).unwrap(),
            password: "hash".to_owned(),
            status: 1,
            permissions: None,
            options: WireOptions::empty(),
        };
        let reply = StateHandler::apply(&request, &mut users, IggyTimestamp::now());
        assert_eq!(reply.code, 0);
        assert!(users.index.contains_key(username.as_str()));
    }

    #[test]
    fn given_too_short_rename_when_apply_update_user_should_reject_and_keep_username() {
        let mut users = UsersInner::new();
        create_user_with(&mut users, "iggy", None);
        let alice_id = create_user_with(&mut users, "alice", None);

        let short = "a".repeat(MIN_USERNAME_LENGTH - 1);
        let rename = UpdateUserResolvedRequest {
            user_id: WireIdentifier::numeric(alice_id),
            username: Some(WireName::new(&short).unwrap()),
            status: None,
            explicit_options: WireOptions::empty(),
            derived_options: WireOptions::empty(),
        };
        let reply = StateHandler::apply(&rename, &mut users, IggyTimestamp::now());
        assert_eq!(reply.code, u32::from(UpdateUserResult::InvalidUsername));

        assert_eq!(
            users
                .items
                .get(alice_id as usize)
                .map(|u| u.username.as_ref()),
            Some("alice"),
            "username must survive a rejected rename"
        );
        assert_eq!(users.index.get("alice"), Some(&alice_id));
        assert!(!users.index.contains_key(short.as_str()));
    }

    #[test]
    fn given_user_status_patch_when_applying_should_revoke_inactive_sessions() {
        let mut users = UsersInner::new();
        create_user_with(&mut users, "iggy", None);
        let alice_id = create_user_with(&mut users, "alice", None);
        let patch =
            |username: Option<&str>, status: Option<UserStatus>| UpdateUserResolvedRequest {
                user_id: WireIdentifier::numeric(alice_id),
                username: username.map(|name| WireName::new(name).unwrap()),
                status: status.map(|status| status.as_code()),
                explicit_options: WireOptions::empty(),
                derived_options: WireOptions::empty(),
            };

        let rename = StateHandler::apply(
            &patch(Some("alicia"), None),
            &mut users,
            IggyTimestamp::now(),
        );
        assert_eq!(rename.code, 0);
        assert_eq!(rename.revoked_user, None);
        assert_eq!(users.index.get("alicia"), Some(&alice_id));
        assert!(!users.index.contains_key("alice"));

        let deactivate = StateHandler::apply(
            &patch(None, Some(UserStatus::Inactive)),
            &mut users,
            IggyTimestamp::now(),
        );
        assert_eq!(deactivate.code, 0);
        assert_eq!(deactivate.revoked_user, Some(alice_id));
        assert_eq!(
            users.items.get(alice_id as usize).map(|user| user.status),
            Some(UserStatus::Inactive)
        );

        let taken =
            StateHandler::apply(&patch(Some("iggy"), None), &mut users, IggyTimestamp::now());
        assert_eq!(
            taken.code,
            u32::from(UpdateUserResult::UsernameAlreadyExists)
        );
        assert_eq!(taken.revoked_user, None);

        let invalid_status = UpdateUserResolvedRequest {
            status: Some(9),
            ..patch(None, None)
        };
        assert_eq!(
            StateHandler::apply(&invalid_status, &mut users, IggyTimestamp::now()).code,
            u32::from(UpdateUserResult::InvalidUserStatus)
        );
    }

    #[test]
    fn given_root_patch_when_applying_should_enforce_root_identity_and_active_status() {
        let mut users = UsersInner::new();
        create_user_with(&mut users, "iggy", None);
        let manager_id = create_user_with(&mut users, "manager", None);
        let patch =
            |username: Option<&str>, status: Option<UserStatus>| UpdateUserResolvedRequest {
                user_id: WireIdentifier::numeric(DEFAULT_ROOT_USER_ID),
                username: username.map(|name| WireName::new(name).unwrap()),
                status: status.map(|value| value.as_code()),
                explicit_options: WireOptions::empty(),
                derived_options: WireOptions::empty(),
            };

        users.apply_context.user_id = manager_id;
        for request in [
            patch(Some("root-renamed"), None),
            patch(None, Some(UserStatus::Inactive)),
        ] {
            let reply = StateHandler::apply(&request, &mut users, IggyTimestamp::zero());
            assert_eq!(reply.code, IggyError::CannotModifyRootUser.as_code());
            assert_eq!(
                users.items.get(0).map(|user| user.username.as_ref()),
                Some("iggy")
            );
            assert_eq!(
                users.items.get(0).map(|user| user.status),
                Some(UserStatus::Active)
            );
        }

        users.apply_context.user_id = DEFAULT_ROOT_USER_ID;
        let renamed = StateHandler::apply(
            &patch(Some("root-renamed"), None),
            &mut users,
            IggyTimestamp::zero(),
        );
        assert_eq!(renamed.code, 0);
        assert_eq!(
            users.items.get(0).map(|user| user.username.as_ref()),
            Some("root-renamed")
        );

        let deactivated = StateHandler::apply(
            &patch(None, Some(UserStatus::Inactive)),
            &mut users,
            IggyTimestamp::zero(),
        );
        assert_eq!(deactivated.code, IggyError::CannotModifyRootUser.as_code());
        assert_eq!(
            users.items.get(0).map(|user| user.status),
            Some(UserStatus::Active)
        );
    }
}
