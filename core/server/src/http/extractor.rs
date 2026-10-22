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
use super::state::HttpInner;
use crate::dispatch::session_ops::verify_pat_credentials_with_expiry;
use crate::http::ClientAddr;

/// Bearer scheme prefix in the `Authorization` header.
const BEARER: &str = "Bearer ";

/// Key-space prefixes: a JWT `jti` and a PAT hash live in disjoint namespaces
/// so the two credential kinds can never collide on the same table key.
const JWT_KEY_PREFIX: &str = "jwt:";
const PAT_KEY_PREFIX: &str = "pat:";

/// What [`resolve_credential`] proved about a bearer: the JWT id when it is a
/// JWT, the account behind it and its expiry in seconds. The partition route
/// layer stores it in the request extensions for the extractor behind it.
#[derive(Clone)]
pub(in crate::http) struct VerifiedBearer {
    pub jwt_id: Option<String>,
    pub user_id: u32,
    pub user_created_at: IggyTimestamp,
    pub expiry: u64,
}

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
        let credential = Credential::from_request_parts(parts, state).await?;
        // `resolve_session` is `Rc`-based and `!Send`, yet axum requires this
        // extractor future to be `Send`. `SendWrapper` bridges it: sound because
        // compio pins the future to shard 0's thread, the only thread the
        // session table ever runs on (mirrors legacy `HttpSafeShard`). It holds
        // no `RefCell` borrow across the `.await` (each such critical section is
        // synchronous), so a cooperatively-scheduled sibling task on this thread
        // never observes a borrowed session table.
        let session = SendWrapper::new(credential.session(state)).await?;
        Ok(Self {
            session: SendWrapper::new(session),
        })
    }
}

/// A verified bearer with what its VSR session is keyed and leased by, but no
/// session yet. A partition write resolves the session only where it is
/// served ([`Self::session`]): a relayed write runs under the serving node's
/// own session, and a `Register` on the relaying node would be a metadata
/// commit spent on nothing.
pub struct Credential {
    key: String,
    pub user_id: u32,
    user_created_at: IggyTimestamp,
    expiry: u64,
}

impl Credential {
    /// Whether this node holds a live session for the credential, so serving
    /// here Registers none.
    pub(in crate::http) fn has_session(&self, state: &HttpInner) -> bool {
        state.has_live_session(&self.key)
    }

    /// The credential's VSR session, minted and Registered on first use.
    pub(in crate::http) async fn session(
        &self,
        state: &HttpInner,
    ) -> Result<Rc<HttpSession>, AuthError> {
        state
            .resolve_session(&self.key, self.user_id, self.user_created_at, self.expiry)
            .await
    }
}

impl FromRequestParts<HttpState> for Credential {
    type Rejection = AuthError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &HttpState,
    ) -> Result<Self, Self::Rejection> {
        let bearer = bearer_token(&parts.headers)?;
        let verified = verified_bearer(parts, state, bearer).await?;
        Ok(Self {
            key: credential_key(
                bearer,
                verified.jwt_id.as_deref(),
                verified.user_id,
                verified.user_created_at,
            ),
            user_id: verified.user_id,
            user_created_at: verified.user_created_at,
            expiry: verified.expiry,
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
        let verified = verified_bearer(parts, state, bearer).await?;
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
            user_id: verified.user_id,
            user_created_at: verified.user_created_at,
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

/// The facts of `bearer`: the ones a route layer stored when it verified the
/// same bearer, so a request is verified once, or a fresh verify.
async fn verified_bearer(
    parts: &Parts,
    state: &HttpState,
    bearer: &str,
) -> Result<VerifiedBearer, AuthError> {
    if let Some(verified) = parts.extensions.get::<VerifiedBearer>() {
        return Ok(verified.clone());
    }
    // The verify is `!Send` (a trusted-issuer JWT may await a JWKS fetch), so
    // bridge it with `SendWrapper` - sound only because compio pins this
    // future to shard 0's single thread, the only thread the JWKS client ever
    // runs on (mirrors legacy `HttpSafeShard`). It holds no `RefCell` borrow
    // or `DashMap` guard across the `.await`, so a sibling task scheduled on
    // this thread meanwhile never observes a borrowed cell.
    SendWrapper::new(resolve_credential(state, bearer)).await
}

/// Map a bearer credential to its [`VerifiedBearer`] facts. Only
/// `Authenticated` builds a session key from them.
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
) -> Result<VerifiedBearer, AuthError> {
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
        return Ok(VerifiedBearer {
            jwt_id: Some(token.jti),
            user_id,
            user_created_at,
            expiry: token.exp,
        });
    }

    let (user_id, user_created_at, expiry) =
        verify_pat_credentials_with_expiry(&state.shard, bearer)
            .map_err(|_| IggyError::Unauthenticated)?;
    Ok(VerifiedBearer {
        jwt_id: None,
        user_id,
        user_created_at,
        expiry,
    })
}

/// Table key of a credential [`resolve_credential`] resolved: the account and
/// `jti` of a JWT, or the PAT's hash, so the raw secret never becomes a key.
pub(in crate::http) fn credential_key(
    bearer: &str,
    jwt_id: Option<&str>,
    user_id: u32,
    user_created_at: IggyTimestamp,
) -> String {
    // External issuers choose their own jti, and their configured username
    // can resolve to another account later. Scope the key to this account.
    jwt_id.map_or_else(
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
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::reads::tests::read_state;
    use axum::http::Request;

    const LAYER_USER_ID: u32 = 7;

    /// The partition route layer verifies the bearer before its handler runs,
    /// so the extractor behind it reuses that proof. A bearer that would not
    /// verify shows the extractor did not verify it again.
    #[compio::test]
    async fn given_a_bearer_the_route_layer_verified_when_extracting_should_not_verify_it_again() {
        let state = read_state();
        let verified = VerifiedBearer {
            jwt_id: Some("layer-jti".to_owned()),
            user_id: LAYER_USER_ID,
            user_created_at: IggyTimestamp::from(1),
            expiry: u64::MAX,
        };

        let mut parts = unverifiable_bearer_parts();
        parts.extensions.insert(verified.clone());
        let credential = Credential::from_request_parts(&mut parts, &state)
            .await
            .unwrap_or_else(|_| panic!("a verified bearer must not be verified again"));
        assert_eq!(credential.user_id, LAYER_USER_ID);
        assert_eq!(
            credential.key,
            credential_key(
                "not-a-token",
                verified.jwt_id.as_deref(),
                LAYER_USER_ID,
                verified.user_created_at
            )
        );
        let identity = Identity::from_request_parts(&mut parts, &state)
            .await
            .unwrap_or_else(|_| panic!("a verified bearer must not be verified again"));
        assert_eq!(identity.user_id, LAYER_USER_ID);

        let mut unverified = unverifiable_bearer_parts();
        assert!(
            Credential::from_request_parts(&mut unverified, &state)
                .await
                .is_err(),
            "without a layer the extractor still verifies the bearer"
        );
    }

    fn unverifiable_bearer_parts() -> Parts {
        Request::builder()
            .uri("/streams/1/topics/1/messages")
            .header(AUTHORIZATION, "Bearer not-a-token")
            .body(())
            .expect("request builds")
            .into_parts()
            .0
    }
}
