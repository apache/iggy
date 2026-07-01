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

use std::rc::Rc;

use axum::extract::FromRequestParts;
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use iggy_common::{IggyError, PersonalAccessToken};
use send_wrapper::SendWrapper;

use super::{AuthError, HttpSession, HttpState};
use crate::auth::verify_pat_credentials_with_expiry;

/// Bearer scheme prefix in the `Authorization` header.
const BEARER: &str = "Bearer ";

/// Key-space prefixes: a JWT `jti` and a PAT hash live in disjoint namespaces
/// so the two credential kinds can never collide on the same table key.
const JWT_KEY_PREFIX: &str = "jwt:";
const PAT_KEY_PREFIX: &str = "pat:";

/// Resolved caller identity for a protected route: the numeric user id plus the
/// shared VSR session established once per presenting credential.
pub struct Authenticated {
    pub user_id: u32,
    /// Per-credential VSR session. The write path (2c) reads its client id and
    /// session number and serializes writes through its gate; `get_me` and
    /// other read-only probes ignore it.
    ///
    /// Wrapped because axum requires every handler argument to be `Send` and
    /// `Rc` is not; this mirrors the `HttpState` bridge. Sound for the same
    /// reason: the extractor mints it on shard 0's compio thread and every
    /// handler runs on that same thread, so the wrapper is never crossed.
    #[expect(
        dead_code,
        reason = "read by the write path (2c) via client id, session, and gate"
    )]
    pub session: SendWrapper<Rc<HttpSession>>,
}

impl FromRequestParts<HttpState> for Authenticated {
    type Rejection = AuthError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &HttpState,
    ) -> Result<Self, Self::Rejection> {
        let bearer = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix(BEARER))
            .ok_or(IggyError::AccessTokenMissing)?;

        let (key, user_id, expiry) = resolve_credential(state, bearer)?;

        // `resolve_session` is `Rc`-based and `!Send`, yet axum requires this
        // extractor future to be `Send`. `SendWrapper` bridges the gap: sound
        // because compio pins the future to shard 0's thread - the only thread
        // that ever touches the session table (mirrors legacy `HttpSafeShard`).
        let session = SendWrapper::new(state.resolve_session(key, user_id, expiry)).await?;
        Ok(Self {
            user_id,
            session: SendWrapper::new(session),
        })
    }
}

/// Map a bearer credential to its `(session key, user id, expiry secs)`.
///
/// A JWT is the primary credential, keyed by its `jti`. A raw PAT presented as
/// the bearer is the documented fallback, keyed by its SHA-256 hash - the same
/// digest Iggy already indexes PATs by, so the key is stable and collision-free
/// while the raw secret never enters the table. Both checks are local and
/// synchronous, so no borrow is held across an await here.
fn resolve_credential(state: &HttpState, bearer: &str) -> Result<(String, u32, u64), AuthError> {
    if let Ok(claims) = state.jwt.decode(bearer)
        && let Ok(user_id) = claims.sub.parse::<u32>()
    {
        return Ok((
            format!("{JWT_KEY_PREFIX}{}", claims.jti),
            user_id,
            claims.exp,
        ));
    }

    let (user_id, expiry) = verify_pat_credentials_with_expiry(&state.shard, bearer)
        .map_err(|_| IggyError::Unauthenticated)?;
    let key = format!(
        "{PAT_KEY_PREFIX}{}",
        PersonalAccessToken::hash_token(bearer)
    );
    Ok((key, user_id, expiry))
}
