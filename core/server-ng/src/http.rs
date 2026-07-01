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

//! Shard-0 HTTP/REST listener: state bridge, router, and the auth-issuance
//! half (login + bearer extraction). Management and data-plane routes land
//! later; this module owns only health, login, and the authenticated probe.

mod extractor;
mod jwt;

use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use configs::http::HttpJwtConfig;
use iggy_common::login_user::LoginUser;
use iggy_common::login_with_personal_access_token::LoginWithPersonalAccessToken;
use iggy_common::{IdentityInfo, IggyError, IggyTimestamp, TokenInfo};
use message_bus::client_listener;
use secrecy::ExposeSecret;
use send_wrapper::SendWrapper;
use server::http::error::{CustomError, ErrorResponse};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::auth::{verify_login_credentials, verify_pat_credentials};
use crate::bootstrap::ServerNgShard;
use crate::dispatch::submit_register_on_owner;
use crate::http::extractor::Authenticated;
use crate::http::jwt::JwtManager;
use crate::login_register::LoginRegisterError;
use crate::server_error::ServerNgError;

/// `GET /ping` response body, matching the legacy HTTP server's health probe.
const PONG: &str = "pong";

/// Hard cap on live per-credential sessions. A leak-guard, not a tuning knob:
/// reaching it means this many distinct live tokens are in flight at once
/// (pathological at P1 volume). New sessions past the cap are refused with a
/// transient 503 rather than evicting a live one; the client retries. Expired
/// entries are dropped first, so the cap only bites on live oversubscription.
const MAX_HTTP_SESSIONS: usize = 100_000;

/// First per-session request id the write path (2c) will hand out. VSR request
/// numbers are 1-based and strictly increasing within a session.
const FIRST_REQUEST_ID: u64 = 1;

/// One VSR session established for a single login credential (a JWT `jti` or a
/// PAT). Shared via `Rc` by every concurrent request bearing that credential,
/// so the session granularity is per-login.
struct HttpSession {
    /// Shard-0 client id minted for this credential; its top 16 bits are 0, so
    /// it shares the shard-0 id space with TCP virtual clients without
    /// colliding. Consumed by the write path (2c).
    #[expect(
        dead_code,
        reason = "read by the write path (2c) to fill RequestHeader.client"
    )]
    client_id: u128,
    /// Cluster session number returned by the VSR `Register` commit. Consumed
    /// by the write path (2c).
    #[expect(
        dead_code,
        reason = "read by the write path (2c) to fill RequestHeader.session"
    )]
    session: u64,
    /// User the credential authenticated as. Consumed by the write path (2c).
    #[expect(dead_code, reason = "read by the write path (2c) for authorization")]
    user_id: u32,
    /// Credential expiry in unix seconds (`u64::MAX` = never). Drives lazy
    /// eviction of stale table entries.
    expiry: u64,
    /// Serializes the write path (2c): the guarded value is the NEXT request
    /// id. A `tokio::sync::Mutex` because 2c holds it across the submit `.await`
    /// so each session's request numbers stay monotonic and gap-free.
    #[expect(
        dead_code,
        reason = "locked by the write path (2c) to serialize per-session writes"
    )]
    gate: Mutex<u64>,
}

/// Rejection for protected routes.
///
/// Two failure classes get two statuses: a missing, invalid, or expired
/// credential is the caller's fault (401, rendered exactly like the legacy
/// server so SDK error bodies match), while a VSR `Register` that cannot commit
/// right now is a transient server condition (503) and must never masquerade as
/// an auth failure.
pub enum AuthError {
    Unauthenticated(IggyError),
    SessionUnavailable,
}

impl From<IggyError> for AuthError {
    fn from(error: IggyError) -> Self {
        Self::Unauthenticated(error)
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        match self {
            // Reuse the legacy `IggyError -> CustomError` rendering so 401
            // bodies stay byte-identical to what the SDKs are tested against.
            Self::Unauthenticated(error) => CustomError::from(error).into_response(),
            // Register could not commit (no caught-up primary, pipeline full, or
            // a view-change cancel). Transient server condition -> 503, retryable.
            Self::SessionUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse::from_error(
                    IggyError::CannotEstablishConnection,
                )),
            )
                .into_response(),
        }
    }
}

/// Shared shard-0 HTTP state.
///
/// Groups the shard handle, the JWT issuer/verifier, and the per-credential VSR
/// session table so every handler and the [`Authenticated`] extractor reach
/// them through one axum `State`.
struct HttpInner {
    shard: Rc<ServerNgShard>,
    jwt: JwtManager,
    /// Per-credential VSR sessions keyed by JWT `jti` / PAT hash. `RefCell` is
    /// sound here - shard 0 is single-threaded and the `SendWrapper` state
    /// bridge tolerates the `!Sync` interior - but the guard must never be held
    /// across an `.await` (see [`HttpInner::resolve_session`]).
    sessions: RefCell<HashMap<String, Rc<HttpSession>>>,
}

impl HttpInner {
    /// Resolve the VSR session for `key`, minting and Registering one on first
    /// use. Every later request bearing the same credential reuses it.
    ///
    /// Borrow discipline: the `RefCell` table guard is taken, read, and dropped
    /// WITHOUT crossing the `.await`. Holding it across the Register suspend
    /// would panic the moment a sibling shard-0 task borrowed the table while
    /// this one is parked (single-threaded `RefCell` + cooperative scheduling).
    async fn resolve_session(
        &self,
        key: String,
        user_id: u32,
        expiry: u64,
    ) -> Result<Rc<HttpSession>, AuthError> {
        let now = IggyTimestamp::now().to_secs();
        if let Some(session) = self.live_session(&key, now) {
            return Ok(session);
        }

        // Miss: mint + Register with no borrow held (an async VSR commit).
        let fresh = self.register_session(user_id, expiry).await?;

        let mut table = self.sessions.borrow_mut();
        // A concurrent first request for the same key may have Registered and
        // inserted while we were parked. Last-writer-wins: reuse the installed
        // entry and drop `fresh`, whose client id is then orphaned on the peers
        // until they evict it - an accepted rare cost at P1 volume.
        if let Some(session) = live_entry(&table, &key, now) {
            return Ok(session);
        }
        table.retain(|_, session| session.expiry > now);
        if table.len() >= MAX_HTTP_SESSIONS {
            // Still full after dropping expired entries: too many genuinely live
            // sessions. Refuse rather than evict a live one; the client retries.
            return Err(AuthError::SessionUnavailable);
        }
        table.insert(key, Rc::clone(&fresh));
        Ok(fresh)
    }

    /// Clone the live (non-expired) entry for `key`, if present. Confines the
    /// shared `RefCell` borrow to this call so it can never span an `.await`.
    fn live_session(&self, key: &str, now_secs: u64) -> Option<Rc<HttpSession>> {
        live_entry(&self.sessions.borrow(), key, now_secs)
    }

    /// Mint a shard-0 client id and run the VSR `Register` for a fresh session.
    /// Holds no table borrow; the caller inserts the result.
    async fn register_session(
        &self,
        user_id: u32,
        expiry: u64,
    ) -> Result<Rc<HttpSession>, AuthError> {
        let coordinator = self
            .shard
            .coordinator()
            .ok_or(AuthError::SessionUnavailable)?;
        // Reuse the TCP accept path's minter: it draws from the same shard-0
        // `client_seq`, so an HTTP session id can never collide with a TCP
        // virtual client's and the shard-0 tag (top 16 bits == 0) is preserved.
        let client_id = coordinator.mint_shard_zero_client_id();
        // The minter seeds at 1, so 0 is only reachable after a 2^112 wrap.
        // Guard anyway: `submit_register_in_process` asserts `client_id != 0`,
        // and an assert on this request path would be a panic.
        if client_id == 0 {
            return Err(AuthError::SessionUnavailable);
        }
        // Shared Register entry point; on shard 0 (always, for HTTP) it runs
        // `submit_register_in_process` directly on the metadata owner.
        let session = submit_register_on_owner(&self.shard, client_id)
            .await
            .map_err(|error| {
                warn!(?error, "server-ng HTTP: VSR Register submit failed");
                AuthError::SessionUnavailable
            })?;
        Ok(Rc::new(HttpSession {
            client_id,
            session,
            user_id,
            expiry,
            gate: Mutex::new(FIRST_REQUEST_ID),
        }))
    }
}

/// Borrow-and-clone a live table entry, or `None` if missing or expired. Shared
/// by the fast path and the post-Register re-check so neither leaks a guard.
fn live_entry(
    table: &HashMap<String, Rc<HttpSession>>,
    key: &str,
    now_secs: u64,
) -> Option<Rc<HttpSession>> {
    table
        .get(key)
        .filter(|session| session.expiry > now_secs)
        .map(Rc::clone)
}

/// Axum router state: shard-0's [`HttpInner`] behind an `Rc`, `!Send` yet
/// bridged into axum's `Send + Sync` requirement by `SendWrapper`. Sound
/// because the listener and every handler run on shard 0's compio thread - the
/// same thread that builds this state. Never touch it off that thread.
type HttpState = SendWrapper<Rc<HttpInner>>;

/// Bind the shard-0 HTTP listener and spawn the `cyper-axum` serve loop as a
/// background task on shard 0's compio runtime.
///
/// The caller gates this to shard 0 and to `http.enabled`; the listener stops
/// when the bus shutdown token fires.
///
/// # Errors
///
/// Returns [`ServerNgError`] if the JWT manager cannot be built from
/// `jwt_config` or the listener cannot bind to `addr`.
pub async fn start(
    shard: &Rc<ServerNgShard>,
    addr: SocketAddr,
    jwt_config: &HttpJwtConfig,
) -> Result<(), ServerNgError> {
    let jwt = JwtManager::build(jwt_config)?;
    let (listener, bound_addr) = client_listener::tcp::bind(addr).await?;
    info!(address = %bound_addr, "server-ng HTTP listener started");

    let state: HttpState = SendWrapper::new(Rc::new(HttpInner {
        shard: Rc::clone(shard),
        jwt,
        sessions: RefCell::new(HashMap::new()),
    }));
    let router = router(state);

    let shutdown = shard.bus.token();
    let handle = compio::runtime::spawn(async move {
        if let Err(error) = cyper_axum::serve(listener, router)
            .with_graceful_shutdown(async move { shutdown.wait().await })
            .await
        {
            error!(%error, "server-ng HTTP listener terminated with error");
        }
    });
    shard.bus.track_background(handle);

    Ok(())
}

/// Assemble the shard-0 router: unauthenticated health + login routes, plus
/// one authenticated probe that exercises the [`Authenticated`] extractor.
fn router(state: HttpState) -> Router {
    Router::new()
        .route("/ping", get(ping))
        .route("/users/login", post(login_user))
        .route(
            "/personal-access-tokens/login",
            post(login_with_personal_access_token),
        )
        .route("/users/me", get(get_me))
        .with_state(state)
}

/// Extracting the state here proves at compile time that the `!Send` state
/// bridges into axum's `Send + Sync` router state on shard 0's compio thread.
/// Ping needs no state, so it is discarded.
async fn ping(State(_state): State<HttpState>) -> &'static str {
    PONG
}

async fn login_user(
    State(state): State<HttpState>,
    Json(command): Json<LoginUser>,
) -> Result<Json<IdentityInfo>, CustomError> {
    let user_id = verify_login_credentials(
        &state.shard,
        &command.username,
        command.password.expose_secret(),
    )
    .map_err(|error| login_error_to_iggy(&error))?;
    issue_identity(&state, user_id)
}

async fn login_with_personal_access_token(
    State(state): State<HttpState>,
    Json(command): Json<LoginWithPersonalAccessToken>,
) -> Result<Json<IdentityInfo>, CustomError> {
    let user_id = verify_pat_credentials(&state.shard, command.token.expose_secret())
        .map_err(|error| login_error_to_iggy(&error))?;
    issue_identity(&state, user_id)
}

/// Interim authenticated probe: echoes the caller's resolved `user_id` so the
/// [`Authenticated`] extractor is covered end to end. Iggy exposes no "current
/// identity" route to reuse. This path also shadows a future
/// `GET /users/{user_id}` and must be reconciled when that route is added.
async fn get_me(identity: Authenticated) -> Json<IdentityInfo> {
    Json(IdentityInfo {
        user_id: identity.user_id,
        access_token: None,
    })
}

/// Issue a fresh access token for `user_id` and wrap it in the exact
/// `IdentityInfo` shape the SDKs pin: numeric `user_id` plus an `access_token`
/// carrying the token string and its unix-seconds expiry.
fn issue_identity(inner: &HttpInner, user_id: u32) -> Result<Json<IdentityInfo>, CustomError> {
    let generated = inner.jwt.generate(user_id)?;
    Ok(Json(IdentityInfo {
        user_id: generated.user_id,
        access_token: Some(TokenInfo {
            token: generated.access_token,
            expiry: generated.access_token_expiry,
        }),
    }))
}

/// Grade a credential-verification failure onto the `IggyError` the legacy
/// HTTP error map already maps to a status + body, so `CustomError` renders
/// what the SDKs are tested against. `verify_*` only yield the first three
/// variants; the tail is unreachable but kept terminal (401) for the
/// `#[non_exhaustive]` enum.
const fn login_error_to_iggy(error: &LoginRegisterError) -> IggyError {
    match error {
        LoginRegisterError::InvalidCredentials => IggyError::InvalidCredentials,
        LoginRegisterError::InvalidToken => IggyError::InvalidPersonalAccessToken,
        LoginRegisterError::UserInactive => IggyError::UserInactive,
        _ => IggyError::Unauthenticated,
    }
}
