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
//! `CreateUserRequest::password` / `ChangePasswordRequest::new_password`
//! verbatim as the credential hash, and login verifies against it with
//! Argon2, so the stored value must already be a PHC hash. Hashing inside
//! the replicated `apply` would call `OsRng` for the Argon2 salt on every
//! replica and diverge state (and a raw password would never parse,
//! panicking `verify_password`). So the primary hashes once here and
//! replicates the hash, mirroring the PAT mint in [`crate::pat`].
//!
//! `ChangePassword` also verifies the caller-supplied `current_password`
//! against the target's stored hash before the op replicates (the legacy
//! server does this for self- and admin-initiated changes alike), and it
//! always empties that field, so no plaintext credential ever rides the
//! replicated prepare/WAL. A mismatch is committed as a rejecting no-op
//! (signalled by an empty new password) rather than denied pre-consensus, so
//! the caller's request sequence stays contiguous; see
//! [`verify_and_rewrite_change_password`].

use crate::bootstrap::{ShellBus, ShellShard};
use crate::wire::{request_body, rewrite_request_body};
use bytes::Bytes;
use consensus::MetadataHandle;
use iggy_binary_protocol::codec::{WireDecode, WireEncode};
use iggy_binary_protocol::requests::users::{ChangePasswordRequest, CreateUserRequest};
use iggy_binary_protocol::{Operation, PrepareHeader, RequestHeader};
use iggy_common::IggyError;
use journal::{Journal, JournalHandle};
use metadata::impls::metadata::StreamsFrontend;
use server_common::{Message, crypto};
use std::rc::Rc;

/// Rewrite a raw wire password request before replication.
///
/// `CreateUser` hashes the new password. `ChangePassword` strips the current
/// password unconditionally (so no plaintext enters consensus) and verifies it
/// against the target's stored hash when the target resolves; a mismatch is
/// signalled to the replicated apply as a committed `InvalidCredentials`
/// rejection (see [`verify_and_rewrite_change_password`]). Every other operation
/// passes through unchanged. Returns [`IggyError::InvalidCommand`] only on an
/// undecodable password body.
pub(crate) fn maybe_rewrite_user_password_request<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    request: Message<RequestHeader>,
) -> Result<Message<RequestHeader>, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
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
            let mut wire =
                ChangePasswordRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
            let stored_hash = shard
                .plane
                .metadata()
                .mux_stm
                .users()
                .read(|users| users.password_hash_of(&wire.user_id));
            verify_and_rewrite_change_password(&mut wire, stored_hash.as_deref())
        }
        _ => return Ok(request),
    };

    rewrite_request_body(&request, &rewritten)
}

/// Strip the current password, then re-encode the body for replication.
///
/// When the target resolves (`stored_hash` is `Some`) the supplied
/// `current_password` is verified against it first. A mismatch does NOT deny
/// pre-consensus: that would consume the client's request id without advancing
/// the replicated `ClientTable`, gapping the sequence so the next replicated op
/// is dropped (`RequestGap`) and the caller stalls. Instead the new password is
/// emptied, which signals the committed apply to reject with
/// `InvalidCredentials` (a committed no-op that keeps the sequence contiguous).
///
/// Either way the current password is stripped and the accepted new one hashed,
/// so no plaintext credential ever enters consensus. An unresolved target keeps
/// its hashed new password and the replicated apply rejects it with
/// `UserNotFound`. ACCEPTED RESIDUAL: if the target is created in the narrow
/// window between this rewrite and the apply, the current-password check is
/// skipped for that one op; RBAC still gates cross-user changes. Concurrent
/// same-user `ChangePassword` ops each verify against the hash read here, so
/// the last to commit wins over a now-stale predecessor; there is no
/// compare-and-swap, which is benign and inherent to distributed submission.
fn verify_and_rewrite_change_password(
    wire: &mut ChangePasswordRequest,
    stored_hash: Option<&str>,
) -> Bytes {
    let credentials_ok = match stored_hash {
        Some(stored_hash) => crypto::verify_password(&wire.current_password, stored_hash),
        None => true,
    };
    wire.current_password = String::new();
    wire.new_password = if credentials_ok {
        crypto::hash_password(&wire.new_password)
    } else {
        // Empty hash is a reserved sentinel the committed apply reads as
        // InvalidCredentials. Unambiguous only because `hash_password` is total
        // and its Argon2 PHC output is never empty, so no real hash collides
        // with it.
        String::new()
    };
    wire.to_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::WireIdentifier;

    const CURRENT_PASSWORD: &str = "current-password";
    const NEW_PASSWORD: &str = "brand-new-password";

    fn change_password_request() -> ChangePasswordRequest {
        ChangePasswordRequest {
            user_id: WireIdentifier::numeric(7),
            current_password: CURRENT_PASSWORD.to_owned(),
            new_password: NEW_PASSWORD.to_owned(),
        }
    }

    #[test]
    fn given_wrong_current_password_when_rewrite_should_signal_reject() {
        let stored_hash = crypto::hash_password("some-other-password");
        let mut wire = change_password_request();

        let body = verify_and_rewrite_change_password(&mut wire, Some(&stored_hash));

        let decoded =
            ChangePasswordRequest::decode_from(&body).expect("re-encoded body round-trips");
        assert!(
            decoded.current_password.is_empty(),
            "current password must be stripped before replication"
        );
        // An empty new password is the reject signal the committed apply turns
        // into InvalidCredentials, never a raw or hashed plaintext.
        assert!(
            decoded.new_password.is_empty(),
            "a wrong current password must signal reject via an empty new password"
        );
    }

    #[test]
    fn given_correct_current_password_when_rewrite_should_strip_and_hash() {
        let stored_hash = crypto::hash_password(CURRENT_PASSWORD);
        let mut wire = change_password_request();

        let body = verify_and_rewrite_change_password(&mut wire, Some(&stored_hash));

        let decoded =
            ChangePasswordRequest::decode_from(&body).expect("re-encoded body round-trips");
        assert!(
            decoded.current_password.is_empty(),
            "current password must be stripped before replication"
        );
        assert_ne!(
            decoded.new_password, NEW_PASSWORD,
            "new password must be hashed, not replicated raw"
        );
        assert!(
            crypto::verify_password(NEW_PASSWORD, &decoded.new_password),
            "hashed new password must verify against the raw input"
        );
    }

    #[test]
    fn given_unresolved_target_when_rewrite_should_hash_and_strip_without_verify() {
        let mut wire = change_password_request();

        // No stored hash: nothing to verify against, but the plaintext must
        // still never reach consensus, so the body is rewritten regardless.
        let body = verify_and_rewrite_change_password(&mut wire, None);

        let decoded =
            ChangePasswordRequest::decode_from(&body).expect("re-encoded body round-trips");
        assert!(
            decoded.current_password.is_empty(),
            "current password must be stripped even for an unresolved target"
        );
        assert!(
            crypto::verify_password(NEW_PASSWORD, &decoded.new_password),
            "new password must still be hashed for an unresolved target"
        );
    }
}
