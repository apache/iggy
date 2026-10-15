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

//! Bearer-credential extractor for protected shard-0 HTTP routes.

use std::net::IpAddr;
use std::rc::Rc;

use axum::extract::{ConnectInfo, FromRequestParts};
use axum::http::HeaderMap;
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use iggy_common::{IggyError, IggyTimestamp, PersonalAccessToken};
use send_wrapper::SendWrapper;
use tracing::debug;

use super::HttpState;
use super::error::AuthError;
use super::jwt::TokenSubject;
use super::session::HttpSession;
use crate::dispatch::session_ops::verify_pat_credentials_with_expiry;
use crate::http::ClientAddr;

/// Bearer scheme prefix in the `Authorization` header.
const BEARER: &str = "Bearer ";

/// Key-space prefixes: a JWT `jti` and a PAT hash live in disjoint namespaces
/// so the two credential kinds can never collide on the same table key.
const JWT_KEY_PREFIX: &str = "jwt:";
const PAT_KEY_PREFIX: &str = "pat:";

/// Resolved caller identity for a protected write route: the shared VSR
/// session established once per presenting credential (it carries the
/// authenticated user id).
pub struct Authenticated {
    /// Per-credential VSR session. The write path reads its client id and
    /// session number and serializes writes through its gate.
    ///
    /// Wrapped because axum requires every handler argument to be `Send` and
    /// `Rc` is not; this mirrors the `HttpState` bridge. Sound for the same
    /// reason: the extractor mints it on shard 0's compio thread and every
    /// handler runs on that same thread, so the wrapper is never crossed.
    pub session: SendWrapper<Rc<HttpSession>>,
}

impl FromRequestParts<HttpState> for Authenticated {
    type Rejection = AuthError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &HttpState,
    ) -> Result<Self, Self::Rejection> {
        let bearer = bearer_token(&parts.headers)?;

        // Both `resolve_credential` (its JWT verify may await a `!Send` JWKS
        // fetch through cyper) and `resolve_session` (`Rc`-based, `!Send`) must
        // run, yet axum requires this extractor future to be `Send`.
        // `SendWrapper` bridges each: sound because compio pins the future to
        // shard 0's thread - the only thread the JWKS client and session table
        // ever run on (mirrors legacy `HttpSafeShard`). Neither future holds a
        // `RefCell` borrow or `DashMap` guard across the `.await` (each such
        // critical section is synchronous), so a cooperatively-scheduled sibling
        // task on this thread never observes a borrowed session table.
        let (jwt_id, user_id, user_created_at, expiry) =
            SendWrapper::new(resolve_credential(state, bearer)).await?;
        // External issuers choose their own jti, and their configured username
        // can resolve to another account later. Scope the session to this account.
        let key = jwt_id.map_or_else(
            || {
                format!(
                    "{PAT_KEY_PREFIX}{}",
                    PersonalAccessToken::hash_token(bearer)
                )
            },
            |jwt_id| {
                format!(
                    "{JWT_KEY_PREFIX}{user_id}:{}:{jwt_id}",
                    user_created_at.as_micros()
                )
            },
        );
        let session =
            SendWrapper::new(state.resolve_session(key, user_id, user_created_at, expiry)).await?;
        Ok(Self {
            session: SendWrapper::new(session),
        })
    }
}

/// Read-only caller identity: the account's id and creation timestamp, plus the
/// request path/query used for follower redirects. The timestamp prevents user
/// ID reuse from changing the caller across a wait.
///
/// Unlike [`Authenticated`], it verifies the bearer WITHOUT minting or
/// Registering a VSR session. Reads are served from the local metadata STM and
/// never enter consensus, so a session (and the `Register` commit round-trip it
/// costs) would be dead weight. Same 401 rejection surface as [`Authenticated`]
/// (missing/invalid/expired credential), and the verify runs through the same
/// [`resolve_credential`] chokepoint, so a JWT and a PAT are honored identically.
pub struct Identity {
    pub user_id: u32,
    pub user_created_at: IggyTimestamp,
    /// Original request path + query (e.g. `/streams?consistency=linearizable`),
    /// captured so a linearizable read that reaches a follower can build the
    /// `Location` for its 307 redirect to the primary. Empty only when the URI
    /// carries neither, which a routed read never is.
    pub path_and_query: String,
    /// Transport-level peer IP, from the [`ClientAddr`] connect info both
    /// serve paths install. Used only to pick the advertised address a client
    /// is told about (cluster metadata, the 307 redirect `Location`) - never
    /// for authorization. `None` (defensive; the extension is always present
    /// today) degrades to the catch-all advertised address.
    pub client_ip: Option<IpAddr>,
}

impl FromRequestParts<HttpState> for Identity {
    type Rejection = AuthError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &HttpState,
    ) -> Result<Self, Self::Rejection> {
        let bearer = bearer_token(&parts.headers)?;

        // Verify only. The JWT id and expiry `resolve_credential` also
        // returns feed the write path's session table; a read discards them -
        // its read-your-writes floor is keyed by user id, not by credential
        // (see `MetadataWatermarks`).
        // The verify is `!Send` (a trusted-issuer JWT may await a JWKS fetch),
        // so bridge it with `SendWrapper` - sound only because compio pins this
        // future to shard 0's single thread, the only thread the JWKS client
        // ever runs on (mirrors legacy `HttpSafeShard`). It holds no `RefCell`
        // borrow or `DashMap` guard across the `.await`, so a sibling task
        // scheduled on this thread meanwhile never observes a borrowed cell.
        let (_jwt_id, user_id, user_created_at, _expiry) =
            SendWrapper::new(resolve_credential(state, bearer)).await?;
        let path_and_query = parts
            .uri
            .path_and_query()
            .map(|value| value.as_str().to_owned())
            .unwrap_or_default();
        let client_ip = parts
            .extensions
            .get::<ConnectInfo<ClientAddr>>()
            .map(|ConnectInfo(address)| address.addr.ip());
        if client_ip.is_none() {
            debug!(
                path = parts.uri.path(),
                "request carries no ConnectInfo extension; advertised-address resolution degrades to the catch-all"
            );
        }
        Ok(Self {
            user_id,
            user_created_at,
            path_and_query,
            client_ip,
        })
    }
}

/// Extract the raw bearer token from `Authorization: Bearer <token>`, or reject
/// as `AccessTokenMissing` (the 401 both extractors share). The `?` at each call
/// site converts the `IggyError` into the extractor's `AuthError`. Takes the
/// header map (not `Parts`) so the forward middleware, which holds a full
/// `Request`, runs the identical extraction.
pub(in crate::http) fn bearer_token(headers: &HeaderMap) -> Result<&str, IggyError> {
    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix(BEARER))
        .ok_or(IggyError::AccessTokenMissing)
}

/// Map a bearer credential to its `(optional JWT id, user id, user created_at,
/// expiry secs)`. Only `Authenticated` builds a session key from these facts.
///
/// A JWT is the primary credential, keyed by its account and `jti`. A raw PAT
/// presented as the bearer is the documented fallback, keyed by the same
/// 256-bit BLAKE3 digest (`PersonalAccessToken::hash_token`) Iggy already
/// indexes PATs by, so the key is stable and collision-free while the raw
/// secret never enters the table. The JWT verify is `async` and `!Send` (a
/// trusted-issuer token may fetch the issuer's JWKS on a cache miss), so both
/// call sites drive it inside a `SendWrapper`; the PAT check is local and
/// synchronous. Also the forward middleware's verify-only gate: a follower
/// proves the bearer before relaying a request, without minting a session.
///
/// The `created_at` names the account behind the credential, because a deleted
/// user's id goes to the next user created. A self-issued JWT must name the
/// account that holds its id now, and a trusted-issuer JWT resolves the
/// username its issuer config names.
pub(in crate::http) async fn resolve_credential(
    state: &HttpState,
    bearer: &str,
) -> Result<(Option<String>, u32, IggyTimestamp, u64), AuthError> {
    if let Ok(token) = state.jwt.decode(bearer).await {
        let (user_id, user_created_at) = match token.subject {
            TokenSubject::Account {
                user_id,
                created_at,
            } => (
                user_id,
                state.ensure_active_user(user_id, Some(created_at))?,
            ),
            TokenSubject::TrustedIssuer(issuer) => state.trusted_issuer_user(&issuer)?,
        };
        return Ok((Some(token.jti), user_id, user_created_at, token.exp));
    }

    let (user_id, user_created_at, expiry) =
        verify_pat_credentials_with_expiry(&state.shard, bearer)
            .map_err(|_| IggyError::Unauthenticated)?;
    Ok((None, user_id, user_created_at, expiry))
}
