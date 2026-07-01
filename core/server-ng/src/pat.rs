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
    request: Message<RequestHeader>,
) -> Result<(Message<RequestHeader>, Option<String>), IggyError> {
    let user_id = match request.header().operation {
        Operation::CreatePersonalAccessToken | Operation::DeletePersonalAccessToken => sessions
            .borrow()
            .get_user_id(transport_client_id)
            .ok_or(IggyError::Unauthenticated)?,
        _ => return Ok((request, None)),
    };
    rewrite_pat_request_for_user(user_id, request)
}

/// PAT rewrite for a caller that already holds the authenticated `user_id`.
///
/// The HTTP listener authenticates against its own session table rather than
/// the transport `SessionManager`, so it resolves the id itself and calls this
/// directly. Mints the raw token + hash for `CreatePersonalAccessToken` and
/// hands the raw token back; every other operation passes through unchanged
/// with `None` (the HTTP write core routes all of its ops here, so the non-PAT
/// arm is a no-op, not `unreachable`).
pub(crate) fn rewrite_pat_request_for_user(
    user_id: u32,
    request: Message<RequestHeader>,
) -> Result<(Message<RequestHeader>, Option<String>), IggyError> {
    let body = request_body(&request);
    let mut raw_token = None;
    let rewritten = match request.header().operation {
        Operation::CreatePersonalAccessToken => {
            let wire = WireCreatePersonalAccessTokenRequest::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
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
