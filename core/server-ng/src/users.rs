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
//! always empties that field and hashes the new password, so no plaintext
//! credential ever rides the replicated prepare/WAL.

use crate::bootstrap::ServerNgShard;
use crate::wire::{request_body, rewrite_request_body};
use bytes::Bytes;
use consensus::MetadataHandle;
use iggy_binary_protocol::codec::{WireDecode, WireEncode};
use iggy_binary_protocol::requests::users::{ChangePasswordRequest, CreateUserRequest};
use iggy_binary_protocol::{Operation, RequestHeader};
use iggy_common::IggyError;
use metadata::impls::metadata::StreamsFrontend;
use server_common::{Message, crypto};
use std::rc::Rc;

/// Rewrite a raw wire password request before replication.
///
/// `CreateUser` hashes the new password. `ChangePassword` hashes the new
/// password and strips the current one unconditionally (so neither plaintext
/// enters consensus), and additionally verifies the supplied `current_password`
/// against the target's stored hash when the target resolves. Every other
/// operation passes through unchanged. Returns [`IggyError::InvalidCredentials`]
/// when the current password does not match, matching the legacy server.
pub(crate) fn maybe_rewrite_user_password_request(
    shard: &Rc<ServerNgShard>,
    request: Message<RequestHeader>,
) -> Result<Message<RequestHeader>, IggyError> {
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
            verify_and_rewrite_change_password(&mut wire, stored_hash.as_deref())?
        }
        _ => return Ok(request),
    };

    rewrite_request_body(&request, &rewritten)
}

/// Hash the new password and strip the current one, always, then return the
/// re-encoded body. When the target resolves (`stored_hash` is `Some`) the
/// supplied `current_password` is verified against it first, returning
/// [`IggyError::InvalidCredentials`] on mismatch (matching the legacy server).
///
/// The hash + strip are unconditional so no plaintext credential ever enters
/// consensus, even for an unresolved target (which the replicated apply rejects
/// with `UserNotFound`). ACCEPTED RESIDUAL: if the target is created in the
/// narrow window between this rewrite and the apply, the current-password check
/// is skipped for that one op; RBAC still gates cross-user changes.
fn verify_and_rewrite_change_password(
    wire: &mut ChangePasswordRequest,
    stored_hash: Option<&str>,
) -> Result<Bytes, IggyError> {
    if let Some(stored_hash) = stored_hash
        && !crypto::verify_password(&wire.current_password, stored_hash)
    {
        return Err(IggyError::InvalidCredentials);
    }
    wire.current_password = String::new();
    wire.new_password = crypto::hash_password(&wire.new_password);
    Ok(wire.to_bytes())
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
    fn given_wrong_current_password_when_rewrite_should_reject() {
        let stored_hash = crypto::hash_password("some-other-password");
        let mut wire = change_password_request();

        let result = verify_and_rewrite_change_password(&mut wire, Some(&stored_hash));

        assert!(matches!(result, Err(IggyError::InvalidCredentials)));
    }

    #[test]
    fn given_correct_current_password_when_rewrite_should_strip_and_hash() {
        let stored_hash = crypto::hash_password(CURRENT_PASSWORD);
        let mut wire = change_password_request();

        let body = verify_and_rewrite_change_password(&mut wire, Some(&stored_hash))
            .expect("verification succeeds and re-encodes a body");

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
        let body = verify_and_rewrite_change_password(&mut wire, None)
            .expect("unresolved target is not an error");

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
