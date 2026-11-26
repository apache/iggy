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

//! User password-hashing request rewriting.
//!
//! Passwords arrive raw on the wire. The metadata STM stores
//! `CreateUserRequest::password` and the `password_hash` of its replicated
//! password change verbatim as the credential hash, and login verifies against it with
//! Argon2, so the stored value must already be a PHC hash. Hashing inside
//! the replicated `apply` would call `OsRng` for the Argon2 salt on every
//! replica and diverge state (and a raw password would never parse,
//! panicking `verify_password`). So the primary hashes once here and
//! replicates the hash, mirroring the PAT mint in [`crate::pat`].
//!
//! `ChangePassword` also verifies the caller-supplied `current_password`
//! against the target's stored hash before the op replicates (the legacy
//! server does this for self- and admin-initiated changes alike), pins the
//! target to the id of the user it verified with that user's access revision,
//! and drops the current password, so no plaintext credential ever rides the
//! replicated prepare/WAL. A mismatch or an unresolved target is committed as a
//! rejecting no-op (signalled by an empty hash) rather than denied
//! pre-consensus, so the caller's request sequence stays contiguous. See
//! `verify_and_rewrite_change_password`.

use crate::shell::{ShellBus, ShellShard};
use crate::wire::{request_body, rewrite_request_body};
use bytes::Bytes;
use consensus::MetadataHandle;
use iggy_binary_protocol::codec::{WireDecode, WireEncode};
use iggy_binary_protocol::requests::users::{
    ChangePasswordRequest as WireChangePasswordRequest, CreateUserRequest,
};
use iggy_binary_protocol::{Operation, PrepareHeader, RoutedRequestHeader, WireIdentifier};
use iggy_common::{IggyError, UserId};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::user::ChangePasswordRequest;
use server_common::{Message, crypto};
use std::rc::Rc;

/// Rewrite a raw wire password request before replication.
///
/// `CreateUser` hashes the new password. `ChangePassword` strips the current
/// password unconditionally (so no plaintext enters consensus) and verifies it
/// against the target's stored hash. A mismatch or an unresolved target is
/// signalled to the replicated apply as a committed rejection (see
/// [`verify_and_rewrite_change_password`]). Every other operation passes
/// through unchanged. Returns [`IggyError::InvalidCommand`] only on an
/// undecodable password body.
pub fn maybe_rewrite_user_password_request<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    request: Message<RoutedRequestHeader>,
    keep_session: bool,
) -> Result<Message<RoutedRequestHeader>, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let operation = request.header().operation;
    let body = request_body(&request);
    let rewritten = match operation {
        Operation::CreateUser => {
            let mut wire =
                CreateUserRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
            wire.password = crypto::hash_password(&wire.password);
            wire.to_bytes()
        }
        Operation::ChangePassword => {
            let wire = WireChangePasswordRequest::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            let target = shard
                .plane
                .metadata()
                .mux_stm
                .users()
                .read(|users| users.resolve_password_hash(&wire.user_id));
            verify_and_rewrite_change_password(
                wire,
                keep_session,
                target
                    .as_ref()
                    .map(|(user_id, stored_hash, access_revision)| {
                        (*user_id, stored_hash.as_ref(), *access_revision)
                    }),
            )
        }
        _ => return Ok(request),
    };

    rewrite_request_body(&request, &rewritten)
}

/// Drop the current password and encode the replicated change.
///
/// `target` is the id, stored hash and access revision of the user
/// `wire.user_id` resolves to on this node, `None` when it does not resolve.
/// The supplied `current_password` is verified against that hash, and the
/// change is pinned to that id and revision. Apply resolves the identifier
/// again, so without the pin a name rebound before apply (root renamed to the
/// verified name) would point the change at a user whose password was never
/// checked. Without the revision a concurrent change verified against the same
/// hash would replace a password that changed after its verify.
///
/// A mismatch or an unresolved target does NOT deny pre-consensus: that would
/// consume the client's request id without recording it in the replicated
/// `ClientTable`, so a retry of that id would re-execute instead of deduping.
/// Instead the hash is left empty, which signals the committed apply to
/// reject (a committed no-op that advances the watermark). Apply answers
/// `UserNotFound` when the target is still missing and `InvalidCredentials`
/// otherwise, also for a target created or renamed into place before apply.
///
/// Either way the current password is dropped and an accepted new one hashed,
/// so no plaintext credential ever enters consensus.
fn verify_and_rewrite_change_password(
    wire: WireChangePasswordRequest,
    keep_session: bool,
    target: Option<(UserId, &str, u64)>,
) -> Bytes {
    let (user_id, access_revision, credentials_ok) = match target {
        Some((user_id, stored_hash, access_revision)) => (
            WireIdentifier::numeric(user_id),
            access_revision,
            crypto::verify_password(&wire.current_password, stored_hash),
        ),
        None => (wire.user_id, 0, false),
    };
    ChangePasswordRequest {
        user_id,
        access_revision,
        keep_session,
        password_hash: if credentials_ok {
            crypto::hash_password(&wire.new_password)
        } else {
            // Empty hash is a reserved sentinel the committed apply reads as
            // InvalidCredentials. Unambiguous only because `hash_password` is
            // total and its Argon2 PHC output is never empty, so no real hash
            // collides with it.
            String::new()
        },
    }
    .to_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::test_support::{
        FIRST_BOOT, SpyBus, TestShard, prepare_message, request_message, test_shard,
    };
    use iggy_binary_protocol::requests::users::UpdateUserResolvedRequest;
    use iggy_binary_protocol::{WireGlobalPermissions, WireName, WireOptions, WirePermissions};
    use iggy_common::UserStatus;
    use iggy_common::defaults::DEFAULT_ROOT_USER_ID;
    use metadata::stm::authz::GatedApply;
    use metadata::stm::result::{ApplyReply, ChangePasswordResult};
    use std::cell::Cell;
    use std::mem::size_of;
    use std::sync::Arc;

    const CURRENT_PASSWORD: &str = "current-password";
    const NEW_PASSWORD: &str = "brand-new-password";
    const ROOT_USERNAME: &str = "iggy";
    const ROOT_PASSWORD: &str = "root-password";
    const ADMIN_USERNAME: &str = "mallory";
    const ADMIN_PASSWORD: &str = "mallory-password";
    const UNUSED_USERNAME: &str = "unused-name";
    const VSR_CLIENT_ID: u128 = 1;
    const VERIFIED_REVISION: u64 = 42;

    thread_local! {
        /// Root holds op 1, as in a real cluster.
        static NEXT_OP: Cell<u64> = const { Cell::new(2) };
    }

    #[test]
    fn given_wrong_current_password_when_rewrite_should_signal_reject() {
        let stored_hash = crypto::hash_password("some-other-password");

        let body = verify_and_rewrite_change_password(
            change_password_request(),
            true,
            Some((7, &stored_hash, VERIFIED_REVISION)),
        );

        let decoded =
            ChangePasswordRequest::decode_from(&body).expect("re-encoded body round-trips");
        // An empty hash is the reject signal the committed apply turns into
        // InvalidCredentials, never a raw or hashed plaintext.
        assert_eq!(
            decoded,
            ChangePasswordRequest {
                user_id: WireIdentifier::numeric(7),
                access_revision: VERIFIED_REVISION,
                keep_session: true,
                password_hash: String::new(),
            }
        );
    }

    #[test]
    fn given_correct_current_password_when_rewrite_should_pin_revision_and_hash() {
        let stored_hash = crypto::hash_password(CURRENT_PASSWORD);

        let body = verify_and_rewrite_change_password(
            change_password_request(),
            true,
            Some((7, &stored_hash, VERIFIED_REVISION)),
        );

        let decoded =
            ChangePasswordRequest::decode_from(&body).expect("re-encoded body round-trips");
        assert_eq!(decoded.user_id, WireIdentifier::numeric(7));
        assert_eq!(decoded.access_revision, VERIFIED_REVISION);
        assert_ne!(
            decoded.password_hash, NEW_PASSWORD,
            "new password must be hashed, not replicated raw"
        );
        assert!(
            crypto::verify_password(NEW_PASSWORD, &decoded.password_hash),
            "hashed new password must verify against the raw input"
        );
    }

    #[test]
    fn given_unresolved_target_when_rewrite_should_signal_reject() {
        let body = verify_and_rewrite_change_password(change_password_request(), true, None);

        let decoded =
            ChangePasswordRequest::decode_from(&body).expect("re-encoded body round-trips");
        assert_eq!(
            decoded,
            ChangePasswordRequest {
                user_id: WireIdentifier::numeric(7),
                access_revision: 0,
                keep_session: true,
                password_hash: String::new(),
            }
        );
    }

    #[test]
    fn given_change_verified_before_another_commits_when_committed_should_keep_newer_password() {
        let shard = credentials_shard();
        let admin = create_admin(&shard);
        let stale = edge_rewritten_change_password(&shard, ADMIN_USERNAME, ADMIN_PASSWORD);
        let newer = edge_rewritten_change_password(&shard, ADMIN_USERNAME, ADMIN_PASSWORD);

        let first = commit(&shard, admin, Operation::ChangePassword, &newer);
        let second = commit(&shard, admin, Operation::ChangePassword, &stale);

        assert_eq!(first.code, 0, "the first verified change commits");
        assert_eq!(
            second.code,
            u32::from(ChangePasswordResult::InvalidCredentials),
            "a change verified against the replaced password must be refused"
        );
        assert_password(&shard, admin, NEW_PASSWORD);
    }

    #[test]
    fn given_root_renamed_to_target_after_rewrite_when_committed_should_keep_root_password() {
        let shard = credentials_shard();
        let admin = create_admin(&shard);
        let change = edge_rewritten_change_password(&shard, UNUSED_USERNAME, CURRENT_PASSWORD);

        let rename = commit(
            &shard,
            DEFAULT_ROOT_USER_ID,
            Operation::UpdateUserResolved,
            &rename_body(DEFAULT_ROOT_USER_ID, UNUSED_USERNAME),
        );
        assert_eq!(rename.code, 0, "root may rename itself");
        let reply = commit(&shard, admin, Operation::ChangePassword, &change);

        assert_eq!(
            reply.code,
            u32::from(ChangePasswordResult::InvalidCredentials)
        );
        assert_password(&shard, DEFAULT_ROOT_USER_ID, ROOT_PASSWORD);
    }

    #[test]
    fn given_names_swapped_after_verify_when_committed_should_change_only_verified_user() {
        let shard = credentials_shard();
        let admin = create_admin(&shard);
        let change = edge_rewritten_change_password(&shard, ADMIN_USERNAME, ADMIN_PASSWORD);

        for (user_id, username) in [
            (admin, "mallory-old"),
            (DEFAULT_ROOT_USER_ID, ADMIN_USERNAME),
        ] {
            let rename = commit(
                &shard,
                user_id,
                Operation::UpdateUserResolved,
                &rename_body(user_id, username),
            );
            assert_eq!(rename.code, 0, "a user may rename itself to {username}");
        }
        let reply = commit(&shard, admin, Operation::ChangePassword, &change);

        assert_eq!(reply.code, 0, "the verified user changes its own password");
        assert_password(&shard, DEFAULT_ROOT_USER_ID, ROOT_PASSWORD);
        assert_password(&shard, admin, NEW_PASSWORD);
    }

    fn change_password_request() -> WireChangePasswordRequest {
        WireChangePasswordRequest {
            user_id: WireIdentifier::numeric(7),
            current_password: CURRENT_PASSWORD.to_owned(),
            new_password: NEW_PASSWORD.to_owned(),
        }
    }

    fn credentials_shard() -> Rc<TestShard> {
        let shard = Rc::new(test_shard(&SpyBus::default(), 0, 1, FIRST_BOOT));
        shard
            .plane
            .metadata()
            .mux_stm
            .users()
            .ensure_root_user(ROOT_USERNAME, &crypto::hash_password(ROOT_PASSWORD));
        shard
    }

    /// Creates a user holding `manage_users` alone and returns its id.
    fn create_admin(shard: &Rc<TestShard>) -> u32 {
        let body = CreateUserRequest {
            username: WireName::new(ADMIN_USERNAME).expect("valid username"),
            password: crypto::hash_password(ADMIN_PASSWORD),
            status: UserStatus::Active.as_code(),
            permissions: Some(WirePermissions {
                global: WireGlobalPermissions {
                    manage_servers: false,
                    read_servers: false,
                    manage_users: true,
                    read_users: false,
                    manage_streams: false,
                    read_streams: false,
                    manage_topics: false,
                    read_topics: false,
                    poll_messages: false,
                    send_messages: false,
                },
                streams: Vec::new(),
            }),
            options: WireOptions::empty(),
        }
        .to_bytes();
        let created = commit(shard, DEFAULT_ROOT_USER_ID, Operation::CreateUser, &body);
        assert_eq!(created.code, 0, "root creates the admin");
        shard
            .plane
            .metadata()
            .mux_stm
            .users()
            .read(|users| users.user_by_name(ADMIN_USERNAME).map(|user| user.id))
            .expect("admin exists")
    }

    /// The body the edge replicates for a `ChangePassword` it rewrites against
    /// the state it has applied so far.
    fn edge_rewritten_change_password(
        shard: &Rc<TestShard>,
        target: &str,
        current_password: &str,
    ) -> Bytes {
        let body = WireChangePasswordRequest {
            user_id: WireIdentifier::named(target).expect("valid target name"),
            current_password: current_password.to_owned(),
            new_password: NEW_PASSWORD.to_owned(),
        }
        .to_bytes();
        let request = request_message(Operation::ChangePassword, VSR_CLIENT_ID, 1, 1, &body);
        let rewritten = maybe_rewrite_user_password_request(shard, request, true)
            .expect("well-formed body rewrites");
        Bytes::copy_from_slice(request_body(&rewritten))
    }

    fn rename_body(user_id: u32, username: &str) -> Bytes {
        UpdateUserResolvedRequest {
            user_id: WireIdentifier::numeric(user_id),
            username: Some(WireName::new(username).expect("valid username")),
            status: None,
            explicit_options: WireOptions::empty(),
            derived_options: WireOptions::empty(),
        }
        .to_bytes()
    }

    /// Commits `body` as `caller` through the in-apply RBAC gate, at the op
    /// after the previous commit of this test.
    fn commit(shard: &Rc<TestShard>, caller: u32, operation: Operation, body: &[u8]) -> ApplyReply {
        let mut prepare = prepare_message(operation, VSR_CLIENT_ID, 1, body);
        let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(
            &mut prepare.as_mut_slice()[..size_of::<PrepareHeader>()],
        );
        header.user_id = caller;
        header.op = NEXT_OP.with(|next| next.replace(next.get() + 1));
        shard
            .plane
            .metadata()
            .mux_stm
            .gated_update(consensus::seal_prepare_checksum(prepare))
            .expect("committed body applies")
    }

    fn assert_password(shard: &Rc<TestShard>, user_id: u32, password: &str) {
        let stored_hash = shard
            .plane
            .metadata()
            .mux_stm
            .users()
            .read(|users| {
                users
                    .items
                    .get(user_id as usize)
                    .map(|user| Arc::clone(&user.password_hash))
            })
            .expect("user exists");
        assert!(
            crypto::verify_password(password, &stored_hash),
            "user {user_id} must hold the expected password"
        );
    }
}
