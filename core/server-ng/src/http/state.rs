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

//! Shared shard-0 HTTP state: the [`HttpInner`] bridge (shard handle, JWT
//! issuer, per-credential VSR session table, cluster roster) plus the axum
//! `State` newtype and the per-response view-header stamp.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use axum::http::{HeaderName, HeaderValue};
use axum::response::Response;
use configs::server_ng::NgSystemConfig;
use consensus::{MetadataHandle, VsrConsensus};
use futures::channel::oneshot;
use iggy_common::{ClusterMetadata, IggyTimestamp};
use message_bus::InstanceToken;
use send_wrapper::SendWrapper;
use tokio::sync::Mutex;
use tracing::warn;

use crate::bootstrap::ServerNgShard;
use crate::cluster_meta::ClusterRoster;
use crate::dispatch::submit_register_on_owner;
use crate::http::error::{AuthError, ReadError, primary_redirect_location};
use crate::http::jwt::JwtManager;
use crate::http::session::{
    BarrierEntry, FIRST_REQUEST_ID, HttpSession, MAX_HTTP_SESSIONS, RegistrationBarrier,
    forget_if_same, live_entry, sweep_expired,
};

/// Response header carrying the current VSR view number. Stamped by
/// `insert_view_header` on success and redirect responses only (never on
/// errors, and the router suppresses it on `/ping`) while this node has live
/// consensus.
const VIEW_HEADER: HeaderName = HeaderName::from_static("x-iggy-view");

/// Axum router state: shard-0's [`HttpInner`] behind an `Rc`, `!Send` yet
/// bridged into axum's `Send + Sync` requirement by `SendWrapper`. Sound
/// because the listener and every handler run on shard 0's compio thread - the
/// same thread that builds this state. Never touch it off that thread.
pub(in crate::http) type HttpState = SendWrapper<Rc<HttpInner>>;

/// Shared shard-0 HTTP state.
///
/// Groups the shard handle, the JWT issuer/verifier, and the per-credential VSR
/// session table so every handler and the [`Authenticated`] extractor reach
/// them through one axum `State`.
pub(in crate::http) struct HttpInner {
    pub(in crate::http) shard: Rc<ServerNgShard>,
    pub(in crate::http) jwt: JwtManager,
    /// Read-only server config for the snapshot collector (log directory +
    /// runtime config paths); the shard does not expose config on the read
    /// path.
    pub(in crate::http) system_config: Arc<NgSystemConfig>,
    /// Per-credential VSR sessions keyed by JWT `jti` / PAT hash. `RefCell` is
    /// sound here - shard 0 is single-threaded and the `SendWrapper` state
    /// bridge tolerates the `!Sync` interior - but the guard must never be held
    /// across an `.await` (see [`HttpInner::resolve_session`]).
    pub(in crate::http) sessions: RefCell<HashMap<String, Rc<HttpSession>>>,
    /// Per-key registration barrier: prevents a thundering herd of first
    /// requests for one credential from each running its own `Register`.
    pub(in crate::http) registrations: RegistrationBarrier,
    pub(in crate::http) roster: ClusterRoster,
    /// Awaited partition writes currently in flight across all sessions, gated
    /// by [`MAX_IN_FLIGHT_WRITES_GLOBAL`]. Only [`InFlightWriteGuard`] touches
    /// it, so every admission is paired with exactly one release.
    pub(in crate::http) in_flight_writes: Cell<u32>,
}

impl HttpInner {
    /// True when this shard-0 node is the current VSR metadata primary, i.e.
    /// `primary_index(current_view) == own_replica_id` - the check consensus
    /// `is_primary` already encapsulates over the live view and this replica's
    /// id. Absent consensus (never on shard 0 under VSR, only a no-replica
    /// build) is treated as not-primary so a linearizable read fails closed
    /// rather than serving possibly-stale local state as authoritative.
    pub(in crate::http) fn is_metadata_primary(&self) -> bool {
        self.shard
            .plane
            .metadata()
            .consensus
            .as_ref()
            .is_some_and(VsrConsensus::is_primary)
    }

    /// Grade a linearizable read that reached a follower: redirect (307) to the
    /// current VSR primary's HTTP address when it resolves from the roster, else
    /// fail closed to the 503. The target is the roster node whose `replica_id`
    /// equals `primary_index(view)`; an absent consensus, an unmatched id, or a
    /// port-less node all fall back to [`ReadError::NotPrimary`].
    pub(in crate::http) fn not_primary_read_error(&self, path_and_query: &str) -> ReadError {
        let location = self
            .shard
            .plane
            .metadata()
            .consensus
            .as_ref()
            .and_then(|consensus| {
                let primary_index = consensus.primary_index(consensus.view());
                primary_redirect_location(&self.roster, primary_index, path_and_query)
            });
        location.map_or(ReadError::NotPrimary, ReadError::RedirectToPrimary)
    }

    /// Resolve the VSR session for `key`, minting and Registering one on first
    /// use. Every later request bearing the same credential reuses it.
    ///
    /// Borrow discipline: the `RefCell` table guard is taken, read, and dropped
    /// WITHOUT crossing the `.await`. Holding it across the Register suspend
    /// would panic the moment a sibling shard-0 task borrowed the table while
    /// this one is parked (single-threaded `RefCell` + cooperative scheduling).
    pub(in crate::http) async fn resolve_session(
        &self,
        key: String,
        user_id: u32,
        expiry: u64,
    ) -> Result<Rc<HttpSession>, AuthError> {
        loop {
            let now = IggyTimestamp::now().to_secs();
            if let Some(session) = self.live_session(&key, now) {
                return Ok(session);
            }

            // Miss. Serialize registration per credential so a herd of
            // concurrent first-requests runs one `Register`, not N.
            match self.registrations.enter(&key) {
                BarrierEntry::Wait(waiter) => {
                    // Another first-request is registering this credential.
                    // Park until it finishes (its guard wakes us on drop),
                    // then loop to re-check the table for what it installed.
                    let _ = waiter.await;
                }
                BarrierEntry::Lead(_guard) => {
                    // Sole registrant for this key: mint + Register with no
                    // borrow held (an async VSR commit). The guard wakes any
                    // waiters when this scope ends, cancellation drop included.
                    let fresh = self.register_session(key.clone(), user_id, expiry).await?;
                    // Resample after the await: the pre-await stamp is stale for
                    // the expiry sweep and cap check below.
                    let now = IggyTimestamp::now().to_secs();
                    let (admitted, torn) = {
                        let mut table = self.sessions.borrow_mut();
                        let torn = sweep_expired(&mut table, now);
                        if table.len() >= MAX_HTTP_SESSIONS {
                            // Still full after dropping expired entries: too many
                            // genuinely live sessions. Refuse rather than evict a
                            // live one (its `fresh` client id is orphaned on the
                            // peers until they evict it - a rare at-cap cost).
                            (None, torn)
                        } else {
                            table.insert(key.clone(), Rc::clone(&fresh));
                            (Some(fresh), torn)
                        }
                    };
                    self.teardown_reply_targets(torn);
                    return admitted.ok_or(AuthError::SessionUnavailable);
                }
            }
        }
    }

    /// Clone the live (non-expired) entry for `key`, if present. Confines the
    /// shared `RefCell` borrow to this call so it can never span an `.await`.
    fn live_session(&self, key: &str, now_secs: u64) -> Option<Rc<HttpSession>> {
        live_entry(&self.sessions.borrow(), key, now_secs)
    }

    /// Mint a shard-0 client id and run the VSR `Register` for a fresh session.
    /// Holds no table borrow; the caller inserts the result under `key`.
    async fn register_session(
        &self,
        key: String,
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
        //
        // Detached so a client disconnect cannot cancel the Register
        // mid-flight: the in-process submit drives shared consensus machinery
        // (pipeline push, WAL append, the `on_ack` commit loop), and hyper
        // drops this handler future the moment the HTTP peer disconnects.
        // A canceled submit used to strand consensus state mid-await; now the
        // detached task always drives it to completion and a disconnect only
        // drops the receiver half (same discipline as `submit_committed`).
        let (result_slot, committed) = oneshot::channel();
        let shard = Rc::clone(&self.shard);
        compio::runtime::spawn(async move {
            let result = submit_register_on_owner(&shard, client_id, user_id).await;
            // A failed send means the handler died mid-await; the Register
            // itself has already committed, which is what matters.
            let _ = result_slot.send(result);
        })
        .detach();
        let session = committed
            .await
            .map_err(|_| AuthError::SessionUnavailable)?
            .map_err(|error| {
                warn!(?error, "server-ng HTTP: VSR Register submit failed");
                AuthError::SessionUnavailable
            })?;
        Ok(Rc::new(HttpSession {
            key,
            client_id,
            session,
            user_id,
            expiry,
            gate: Mutex::new(FIRST_REQUEST_ID),
            data_request: Cell::new(FIRST_REQUEST_ID),
            registry_token: Cell::new(None),
            in_flight_writes: Cell::new(0),
        }))
    }

    /// Drop the session table entry for `session`, but only if it is still the
    /// current occupant of its key (pointer-fenced, so a later re-registration
    /// under the same key is never purged). Also tears down its in-process
    /// reply target. Called when a control write comes back evicted: the VSR
    /// slot is gone, so leaving the entry would 401-loop every retry on the
    /// same credential until the token expires; removing it makes the next
    /// request re-register cleanly through the barrier.
    pub(in crate::http) fn forget_session(&self, session: &Rc<HttpSession>) {
        let torn = forget_if_same(&mut self.sessions.borrow_mut(), session);
        self.teardown_reply_targets(torn.into_iter().collect());
    }

    /// Tear down the in-process reply targets of swept/forgotten sessions,
    /// token-fenced so a stale teardown can never remove a later occupant's
    /// registry entry. Runs outside the `sessions` borrow.
    fn teardown_reply_targets(&self, torn: Vec<(u128, InstanceToken)>) {
        for (client_id, token) in torn {
            self.shard
                .bus
                .clients()
                .remove_if_token_matches(client_id, token);
        }
    }

    /// Build the live [`ClusterMetadata`] for `GET /cluster/metadata` through the
    /// shared [`ClusterRoster`] assembly. The leader marking comes from this
    /// shard's consensus view; the HTTP listener is shard-0-only, so consensus is
    /// always present and every roster read carries real leader/follower roles.
    pub(in crate::http) fn build_cluster_metadata(&self) -> ClusterMetadata {
        let primary_index = self
            .shard
            .plane
            .metadata()
            .consensus
            .as_ref()
            .map(|consensus| consensus.primary_index(consensus.view()));
        self.roster.cluster_metadata(primary_index)
    }
}

/// Set the [`VIEW_HEADER`] to the current VSR view on a successful or redirect
/// `response`. Omits the header on error responses and when this node has no
/// live consensus: a missing header is unambiguous, whereas a fabricated view
/// number would mislead.
pub(in crate::http) fn insert_view_header(state: &HttpInner, mut response: Response) -> Response {
    // The view is cluster-internal; error responses (notably pre-auth 401s)
    // must not leak it. Success and the 307 primary-redirect still carry it.
    if !(response.status().is_success() || response.status().is_redirection()) {
        return response;
    }
    if let Some(consensus) = state.shard.plane.metadata().consensus.as_ref() {
        response
            .headers_mut()
            .insert(VIEW_HEADER, HeaderValue::from(consensus.view()));
    }
    response
}
