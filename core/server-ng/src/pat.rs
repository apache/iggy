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

//! Personal-access-token request rewriting.
//!
//! The primary mints the raw token plus its hash here, replaces the wire
//! body with the hash-carrying replicated request, and ships only the
//! hash through consensus.

use crate::session_manager::SessionManager;
use crate::wire::{request_body, rewrite_request_body};
use iggy_binary_protocol::requests::personal_access_tokens::{
    CreatePersonalAccessTokenRequest as WireCreatePersonalAccessTokenRequest,
    DeletePersonalAccessTokenRequest as WireDeletePersonalAccessTokenRequest,
};
use iggy_binary_protocol::{Operation, RequestHeader, WireDecode, WireEncode};
use iggy_common::IggyError;
use metadata::stm::user::{
    CreatePersonalAccessTokenRequest as ReplicatedCreatePersonalAccessTokenRequest,
    DeletePersonalAccessTokenRequest as ReplicatedDeletePersonalAccessTokenRequest,
};
use server_common::Message;
use std::cell::RefCell;
use std::rc::Rc;

pub(crate) fn maybe_rewrite_pat_request(
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    max_tokens_per_user: u32,
    pat_count_of: impl FnOnce(u32) -> usize,
    request: Message<RequestHeader>,
) -> Result<(Message<RequestHeader>, Option<String>), IggyError> {
    let user_id = match request.header().operation {
        Operation::CreatePersonalAccessToken | Operation::DeletePersonalAccessToken => sessions
            .borrow()
            .get_user_id(transport_client_id)
            .ok_or(IggyError::Unauthenticated)?,
        _ => return Ok((request, None)),
    };
    rewrite_pat_request_for_user(user_id, max_tokens_per_user, pat_count_of, request)
}

/// PAT rewrite for a caller that already holds the authenticated `user_id`.
///
/// The HTTP listener authenticates against its own session table rather than
/// the transport `SessionManager`, so it resolves the id itself and calls this
/// directly. `CreatePersonalAccessToken` enforces `max_tokens_per_user`
/// against `pat_count_of` (the caller's committed-STM read, invoked lazily
/// and only for creates), then mints the raw token + hash and hands the raw
/// token back; `DeletePersonalAccessToken` is rewritten to the replicated
/// form scoped to `user_id` (`only_if_expired` false); every other operation
/// passes through unchanged with `None` (the HTTP write core routes all of
/// its ops here, so the non-PAT arm is a no-op, not `unreachable`).
pub(crate) fn rewrite_pat_request_for_user(
    user_id: u32,
    max_tokens_per_user: u32,
    pat_count_of: impl FnOnce(u32) -> usize,
    request: Message<RequestHeader>,
) -> Result<(Message<RequestHeader>, Option<String>), IggyError> {
    let body = request_body(&request);
    let mut raw_token = None;
    let rewritten = match request.header().operation {
        Operation::CreatePersonalAccessToken => {
            let wire = WireCreatePersonalAccessTokenRequest::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            // The cap is config-derived, so it must be enforced here at
            // ingress: a config-dependent branch inside the replicated apply
            // would diverge replicas configured differently. Denying before
            // consensus also runs it ahead of the in-apply duplicate-name
            // grading, so an at-limit duplicate name reports the limit,
            // matching the legacy server's ordering. Reads the local
            // committed count only: concurrent in-flight creates can briefly
            // overshoot, the same soft cap legacy has.
            if pat_count_of(user_id) >= max_tokens_per_user as usize {
                return Err(IggyError::PersonalAccessTokensLimitReached(
                    user_id,
                    max_tokens_per_user,
                ));
            }
            // Primary mints the raw token + hash here and ships the hash
            // through consensus. Replicas decode the hash directly. Doing
            // this inside `CreatePersonalAccessTokenRequest::apply` would
            // call `ring::rand` per-replica and diverge state. The raw token
            // is returned to this client only (see `handle_client_request`).
            let (raw, token_hash) = mint_pat_raw_and_hash();
            raw_token = Some(raw);
            ReplicatedCreatePersonalAccessTokenRequest {
                user_id,
                name: wire.name,
                expiry: wire.expiry,
                token_hash,
            }
            .to_bytes()
        }
        Operation::DeletePersonalAccessToken => {
            let wire = WireDeletePersonalAccessTokenRequest::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            ReplicatedDeletePersonalAccessTokenRequest {
                user_id,
                name: wire.name,
                // Client-initiated: delete by name unconditionally. Only the
                // background cleaner sets the expiry gate.
                only_if_expired: false,
            }
            .to_bytes()
        }
        _ => return Ok((request, None)),
    };

    Ok((rewrite_request_body(&request, &rewritten)?, raw_token))
}

/// Mints a fresh PAT and returns the raw token plus its hex-encoded SHA-256
/// hash (64 bytes ASCII). Only the hash is replicated; the raw token is
/// returned to the minting client by the home shard (it cannot be reproduced
/// by the deterministic `apply` running on every replica).
fn mint_pat_raw_and_hash() -> (String, [u8; 64]) {
    let (raw, hash) = iggy_common::PersonalAccessToken::mint_raw_and_hash();
    let bytes = hash.as_bytes();
    // The hash is blake3 hex -- always exactly 64 ASCII chars. `copy_from_slice`
    // pins that invariant (panics on a length mismatch) rather than silently
    // shipping a wrong-length hash.
    let mut out = [0u8; 64];
    out.copy_from_slice(bytes);
    (raw, out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::WireName;
    use iggy_common::IggyTimestamp;
    use metadata::stm::StateHandler;
    use metadata::stm::user::{PAT_TOKEN_HASH_BYTES, UsersInner};

    const USER_ID: u32 = 7;
    const MAX_TOKENS: u32 = 3;
    const AT_LIMIT_TOKENS: [(&str, u8); 3] = [("one", b'a'), ("two", b'b'), ("three", b'c')];

    fn create_pat_request(name: &str) -> Message<RequestHeader> {
        let header_len = std::mem::size_of::<RequestHeader>();
        let mut template = Message::<RequestHeader>::new(header_len);
        let header = bytemuck::checked::try_from_bytes_mut::<RequestHeader>(
            &mut template.as_mut_slice()[..header_len],
        )
        .expect("zeroed bytes are a valid request header");
        header.operation = Operation::CreatePersonalAccessToken;
        let body = WireCreatePersonalAccessTokenRequest {
            name: WireName::new(name).expect("test token name fits the wire bound"),
            expiry: 0,
        }
        .to_bytes();
        rewrite_request_body(&template, &body).expect("test body fits a request message")
    }

    fn users_with_tokens(tokens: &[(&str, u8)]) -> UsersInner {
        let mut users = UsersInner::new();
        for (name, hash_byte) in tokens {
            ReplicatedCreatePersonalAccessTokenRequest {
                user_id: USER_ID,
                name: WireName::new(*name).expect("test token name fits the wire bound"),
                expiry: 0,
                token_hash: [*hash_byte; PAT_TOKEN_HASH_BYTES],
            }
            .apply(&mut users, IggyTimestamp::now());
        }
        users
    }

    #[test]
    fn given_count_below_limit_when_rewrite_should_mint_and_stamp_user() {
        let (rewritten, raw_token) = rewrite_pat_request_for_user(
            USER_ID,
            MAX_TOKENS,
            |_| (MAX_TOKENS - 1) as usize,
            create_pat_request("fresh"),
        )
        .expect("below-limit create passes the gate");

        assert!(
            raw_token.is_some(),
            "create must hand back the one-time raw token"
        );
        let replicated =
            ReplicatedCreatePersonalAccessTokenRequest::decode_from(request_body(&rewritten))
                .expect("replicated body round-trips");
        assert_eq!(replicated.user_id, USER_ID);
    }

    #[test]
    fn given_count_at_limit_when_rewrite_should_deny_with_limit_error() {
        let error = rewrite_pat_request_for_user(
            USER_ID,
            MAX_TOKENS,
            |_| MAX_TOKENS as usize,
            create_pat_request("one-too-many"),
        )
        .expect_err("at-limit create must be denied");

        assert!(
            matches!(
                error,
                IggyError::PersonalAccessTokensLimitReached(USER_ID, MAX_TOKENS)
            ),
            "expected PersonalAccessTokensLimitReached({USER_ID}, {MAX_TOKENS}), got {error:?}"
        );
    }

    #[test]
    fn given_at_limit_duplicate_name_when_rewrite_should_report_limit_not_duplicate() {
        let users = users_with_tokens(&AT_LIMIT_TOKENS);

        // "one" already exists, but the ingress gate must fire before the
        // in-apply duplicate grading ever could (legacy ordering).
        let error = rewrite_pat_request_for_user(
            USER_ID,
            MAX_TOKENS,
            |user_id| users.pat_count_of(user_id),
            create_pat_request("one"),
        )
        .expect_err("at-limit create must be denied even for a duplicate name");

        assert!(
            matches!(error, IggyError::PersonalAccessTokensLimitReached(..)),
            "expected the limit error before duplicate grading, got {error:?}"
        );
    }

    #[test]
    fn given_deletion_freed_a_slot_when_rewrite_should_mint_again() {
        let mut users = users_with_tokens(&AT_LIMIT_TOKENS);
        ReplicatedDeletePersonalAccessTokenRequest {
            user_id: USER_ID,
            name: WireName::new("one").expect("test token name fits the wire bound"),
            only_if_expired: false,
        }
        .apply(&mut users, IggyTimestamp::now());

        let result = rewrite_pat_request_for_user(
            USER_ID,
            MAX_TOKENS,
            |user_id| users.pat_count_of(user_id),
            create_pat_request("replacement"),
        );

        assert!(
            result.is_ok(),
            "a freed slot must admit the next create, got {:?}",
            result.err()
        );
    }

    #[test]
    fn given_delete_op_when_at_limit_should_pass_the_gate() {
        let header_len = std::mem::size_of::<RequestHeader>();
        let mut template = Message::<RequestHeader>::new(header_len);
        let header = bytemuck::checked::try_from_bytes_mut::<RequestHeader>(
            &mut template.as_mut_slice()[..header_len],
        )
        .expect("zeroed bytes are a valid request header");
        header.operation = Operation::DeletePersonalAccessToken;
        let body = WireDeletePersonalAccessTokenRequest {
            name: WireName::new("one").expect("test token name fits the wire bound"),
        }
        .to_bytes();
        let request =
            rewrite_request_body(&template, &body).expect("test body fits a request message");

        // The cap gates creates only; a delete is how an at-limit user frees a
        // slot, so gating it too would lock the account at the cap forever.
        let (_, raw_token) =
            rewrite_pat_request_for_user(USER_ID, MAX_TOKENS, |_| MAX_TOKENS as usize, request)
                .expect("delete must pass the create cap untouched");
        assert!(raw_token.is_none(), "delete mints no token");
    }
}
