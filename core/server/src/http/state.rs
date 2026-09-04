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
use std::collections::{BTreeSet, HashMap};
use std::net::IpAddr;
use std::rc::Rc;
use std::sync::Arc;

use axum::http::{HeaderName, HeaderValue};
use axum::response::Response;
use configs::external_auth::ExternalAuthConfig;
use configs::server::ServerSystemConfig;
use consensus::{MetadataHandle, VsrConsensus};
use futures::channel::oneshot;
use iggy_common::{ClusterMetadata, IggyTimestamp, Permissions};
use message_bus::InstanceToken;
use metadata::MetadataSubmitError;
use send_wrapper::SendWrapper;
use tokio::sync::Mutex;
use tracing::warn;

use crate::cluster_meta::ClusterRoster;
use crate::dispatch::session_ops::submit_register_on_owner;
use crate::http::error::{AuthError, ReadError, primary_redirect_location};

use crate::http::jwt::JwtManager;
use crate::http::metrics::HttpMetrics;
use crate::http::session::{
    BarrierEntry, FIRST_REQUEST_ID, FRESH_ENTRY_WATERMARK, HttpSession, RegistrationBarrier,
    forget_if_same, live_entry, sweep_expired,
};
use crate::shell::ServerShard;

/// Response header carrying the current VSR view number. Stamped by
/// `insert_view_header` on success and redirect responses only (never on
/// errors, and the router suppresses it on `/ping`) while this node has live
/// consensus. Fill-if-absent: a response relayed from the primary already
/// carries the SERVING node's view, which must win over the relaying
/// follower's possibly-stale one.
pub(in crate::http) const VIEW_HEADER: HeaderName = HeaderName::from_static("iggy-view");

/// Axum router state: shard-0's [`HttpInner`] behind an `Rc`, `!Send` yet
/// bridged into axum's `Send + Sync` requirement by `SendWrapper`. Sound
/// because the listener and every handler run on shard 0's compio thread - the
/// same thread that builds this state. Never touch it off that thread.
pub(in crate::http) type HttpState = SendWrapper<Rc<HttpInner>>;

/// Per-node forwarding context hung off `HttpInner`: the outbound client
/// (pinned-cert TLS when the listener serves HTTPS), the scheme it dials, the
/// request-body buffer bound, and the in-flight budget. Built by
/// `http::forward::build_forward_state`; lives here so the state hub never
/// imports the forwarding middleware.
pub(in crate::http) struct ForwardState {
    /// False when no cluster-wide bearer key material exists (no configured
    /// JWT secret, no cluster PSK): a forwarded bearer would 401 on the
    /// primary, so the middleware passes through and followers answer with
    /// the transient 503 instead.
    pub(in crate::http) active: bool,
    pub(in crate::http) client: cyper::Client,
    /// Also read by the 307 redirect builder: the primary is assumed to serve
    /// the same scheme as this node (uniform cluster HTTP config).
    pub(in crate::http) scheme: &'static str,
    pub(in crate::http) body_limit: usize,
    pub(in crate::http) in_flight: Cell<u32>,
}

/// Shared shard-0 HTTP state.
///
/// Groups the shard handle, the JWT issuer/verifier, and the per-credential VSR
/// session table so every handler and the [`Authenticated`] extractor reach
/// them through one axum `State`.
pub(in crate::http) struct HttpInner {
    pub(in crate::http) shard: Rc<ServerShard>,
    pub(in crate::http) jwt: JwtManager,
    /// Read-only server config for the snapshot collector (log directory +
    /// runtime config paths); the shard does not expose config on the read
    /// path.
    pub(in crate::http) system_config: Arc<ServerSystemConfig>,
    /// Per-credential VSR sessions keyed by JWT `jti` / PAT hash. `RefCell` is
    /// sound here - shard 0 is single-threaded and the `SendWrapper` state
    /// bridge tolerates the `!Sync` interior - but the guard must never be held
    /// across an `.await` (see [`HttpInner::resolve_session`]).
    pub(in crate::http) sessions: RefCell<HashMap<String, Rc<HttpSession>>>,
    /// Per-key registration barrier: prevents a thundering herd of first
    /// requests for one credential from each running its own `Register`.
    pub(in crate::http) registrations: RegistrationBarrier,
    pub(in crate::http) roster: Rc<ClusterRoster>,
    /// Cap on live per-credential sessions: half the configured `[metadata]
    /// clients_table_max`, so HTTP sessions cannot crowd the TCP/QUIC/WS virtual
    /// clients out of the shared VSR client table. Read by `resolve_session`
    /// when admitting a fresh session.
    pub(in crate::http) max_http_sessions: usize,
    /// Configured `[personal_access_token] max_tokens_per_user`, enforced
    /// pre-consensus by the PAT rewrite inside the write submit (the cap is
    /// config-derived, so it must never branch inside the replicated apply).
    pub(in crate::http) max_tokens_per_user: u32,
    /// Awaited partition writes currently in flight across all sessions, gated
    /// by [`MAX_IN_FLIGHT_WRITES_GLOBAL`]. Only [`InFlightWriteGuard`] touches
    /// it, so every admission is paired with exactly one release.
    pub(in crate::http) in_flight_writes: Cell<u32>,
    /// Follower-to-primary forwarding context: outbound client, scheme, body
    /// bound, and its own in-flight budget (see `http::forward`).
    pub(in crate::http) forward: ForwardState,
    /// Legacy-parity metric registry served by the scrape route; the router's
    /// counting layer holds a clone of its request counter.
    pub(in crate::http) metrics: HttpMetrics,
    /// External authentication callout config. Shared across all handlers.
    pub(in crate::http) external_auth: Arc<ExternalAuthConfig>,
    /// Session-scoped permissions for synthetic user IDs (external auth
    /// inline-grant sessions). Keyed by synthetic `user_id`. Cleaned up on
    /// session expiry sweep.
    pub(in crate::http) synthetic_permissions: RefCell<HashMap<u32, Permissions>>,
    /// Shared counter for minting synthetic user IDs. All transports draw
    /// from the same sequence so no two can mint the same ID.
    pub(in crate::http) synthetic_counter: crate::external_auth::SyntheticUserIdCounter,
    /// Freed synthetic user IDs available for reuse.
    pub(in crate::http) free_synthetic_ids: RefCell<BTreeSet<u32>>,
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
    /// port-less node all fall back to [`ReadError::NotPrimary`]. `client_ip`
    /// picks the primary's advertised address from its per-client-network
    /// selectors, so the redirected client lands on the address for its own
    /// network.
    pub(in crate::http) fn not_primary_read_error(
        &self,
        path_and_query: &str,
        client_ip: Option<IpAddr>,
    ) -> ReadError {
        let location = self
            .shard
            .plane
            .metadata()
            .consensus
            .as_ref()
            .and_then(|consensus| {
                let primary_index = consensus.primary_index(consensus.view());
                primary_redirect_location(
                    &self.roster,
                    primary_index,
                    self.forward.scheme,
                    path_and_query,
                    client_ip,
                )
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
                    let (admitted, torn, expired_user_ids) = {
                        let mut table = self.sessions.borrow_mut();
                        let (torn, expired_user_ids) = sweep_expired(&mut table, now);
                        if table.len() >= self.max_http_sessions {
                            (None, torn, expired_user_ids)
                        } else {
                            table.insert(key.clone(), Rc::clone(&fresh));
                            (Some(fresh), torn, expired_user_ids)
                        }
                    };
                    self.teardown_reply_targets(torn);
                    self.sweep_synthetic_permissions(&expired_user_ids);
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

    /// Mint a shard-0 client id and run the VSR `Register` for a fresh session,
    /// retrying on a fresh id if the minted one turns out to be taken.
    ///
    /// The minter is a per-process counter reseeded from the client table at
    /// boot, so a fresh mint normally lands on a free id. Two situations break
    /// that, and neither is predictable from here: a promoted primary mints
    /// from a counter with no relationship to the ids its predecessor
    /// committed, and in a cluster every node counts independently. Landing on
    /// an occupied entry is therefore reactive to detect and cheap to fix --
    /// mint again. Bounded, because a run of collisions means the counter is
    /// wrong rather than unlucky, and looping would hide that.
    ///
    /// The two collision signals are asymmetric. A different owner is refused
    /// terminally by the register ownership gate. The SAME user is not refused
    /// at all -- it rebinds, silently inheriting a watermark written by another
    /// of that user's sessions, which would make this session's first writes
    /// read as duplicates and answer them from the other session's cache. A
    /// non-zero watermark on what should be a brand-new session is exactly that
    /// tell.
    async fn register_session(
        &self,
        key: String,
        user_id: u32,
        expiry: u64,
    ) -> Result<Rc<HttpSession>, AuthError> {
        /// Enough to ride out a promotion-era counter overlap; beyond this the
        /// minter is misconfigured and the 503 is the honest answer.
        const MINT_ATTEMPTS: u8 = 3;

        for attempt in 1..=MINT_ATTEMPTS {
            match self
                .register_session_once(key.clone(), user_id, expiry)
                .await
            {
                Ok(session) => return Ok(session),
                Err(AuthError::SessionIdOwnedByAnotherUser | AuthError::SessionIdTaken)
                    if attempt < MINT_ATTEMPTS =>
                {
                    warn!(
                        attempt,
                        "server HTTP: minted client id was already registered; re-minting"
                    );
                }
                Err(error) => return Err(error),
            }
        }
        Err(AuthError::SessionUnavailable)
    }

    /// One mint-and-Register attempt. `SessionIdTaken` means the id was live
    /// under this same user, so the caller should mint a different one.
    async fn register_session_once(
        &self,
        key: String,
        user_id: u32,
        expiry: u64,
    ) -> Result<Rc<HttpSession>, AuthError> {
        let coordinator = self
            .shard
            .coordinator()
            .ok_or(AuthError::SessionUnavailable)?;
        // Refold the client table into the minter if this is the first mint of
        // the current view. Cheap and skipped within a view, and it is what
        // stops a PROMOTED primary from minting against ids its predecessor
        // committed from an unrelated counter -- the table is replicated, the
        // counter is per process. Boot does the same call (`bootstrap`); this
        // one covers every later view.
        {
            let metadata = self.shard.plane.metadata();
            if let Some(consensus) = metadata.consensus.as_ref() {
                coordinator.seed_client_sequence(
                    consensus.view(),
                    metadata.client_table.borrow().client_ids(),
                );
            }
        }
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
        let bound = committed
            .await
            .map_err(|_| AuthError::SessionUnavailable)?
            .map_err(|error| {
                warn!(?error, "server HTTP: VSR Register submit failed");
                register_submit_auth_error(&error)
            })?;
        // A fresh mint must land on a fresh entry, so a watermark it did not
        // write means the id was already registered to this same user (see
        // `register_session`). Rebinding onto it would inherit that session's
        // dedup history; hand the id back instead and let the caller re-mint.
        if bound.watermark != FRESH_ENTRY_WATERMARK {
            warn!(
                client_id,
                user_id,
                watermark = bound.watermark,
                "server HTTP: minted client id already had a committed session for this user"
            );
            return Err(AuthError::SessionIdTaken);
        }
        Ok(Rc::new(HttpSession {
            key,
            client_id,
            session: bound.epoch,
            user_id,
            expiry,
            gate: Mutex::new(FIRST_REQUEST_ID),
            data_gate: Mutex::new(FIRST_REQUEST_ID),
            registry_token: Cell::new(None),
            in_flight_writes: Cell::new(0),
        }))
    }

    /// Mint a synthetic user ID for an external auth inline-grant session.
    /// Reuses a previously freed ID when available, otherwise draws from the
    /// shared counter. Returns `None` when the synthetic ID space is exhausted.
    pub(in crate::http) fn mint_synthetic_user_id(&self) -> Option<u32> {
        if let Some(id) = self.free_synthetic_ids.borrow_mut().pop_last() {
            return Some(id);
        }
        self.synthetic_counter.mint()
    }

    /// Return a synthetic user ID to the free list without removing a session.
    pub(in crate::http) fn reclaim_synthetic_user_id(&self, id: u32) {
        self.free_synthetic_ids.borrow_mut().insert(id);
    }

    /// Look up session-scoped permissions for a synthetic user ID. Returns
    /// `None` for non-synthetic users or when no permissions are stored.
    pub(in crate::http) fn get_synthetic_permissions(&self, user_id: u32) -> Option<Permissions> {
        self.synthetic_permissions.borrow().get(&user_id).cloned()
    }

    /// Remove `synthetic_permissions` entries for the given expired user IDs.
    /// Only cleans up IDs explicitly passed in (from swept expired sessions)
    /// so freshly-minted IDs whose JWT has not been presented yet are never
    /// removed prematurely.
    pub(in crate::http) fn sweep_synthetic_permissions(&self, expired_user_ids: &[u32]) {
        let mut perms = self.synthetic_permissions.borrow_mut();
        let mut free = self.free_synthetic_ids.borrow_mut();
        for &uid in expired_user_ids {
            if perms.remove(&uid).is_some() {
                free.insert(uid);
            }
        }
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
        if torn.is_some()
            && self
                .synthetic_permissions
                .borrow_mut()
                .remove(&session.user_id)
                .is_some()
        {
            self.free_synthetic_ids.borrow_mut().insert(session.user_id);
        }
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
    /// `client_ip` picks each node's advertised address from its
    /// per-client-network selectors.
    pub(in crate::http) fn build_cluster_metadata(
        &self,
        client_ip: Option<IpAddr>,
    ) -> ClusterMetadata {
        let primary_index = self
            .shard
            .plane
            .metadata()
            .consensus
            .as_ref()
            .map(|consensus| consensus.primary_index(consensus.view()));
        self.roster.cluster_metadata(primary_index, client_ip)
    }
}

const fn register_submit_auth_error(error: &MetadataSubmitError) -> AuthError {
    match error {
        // These outcomes prove the Register never entered a pipeline, so a
        // forwarding peer may safely retry against a re-resolved primary.
        MetadataSubmitError::NotPrimary
        | MetadataSubmitError::NotCaughtUp
        | MetadataSubmitError::PipelineFull
        | MetadataSubmitError::PrimaryUnreachable => AuthError::SessionNotAccepted,
        MetadataSubmitError::ClientIdOwnedByAnotherUser => AuthError::SessionIdOwnedByAnotherUser,
        // The proposal may still commit. Unknown future outcomes fail closed
        // into the same client-only retry class.
        MetadataSubmitError::InProgress
        | MetadataSubmitError::Canceled
        | MetadataSubmitError::ForwardTimedOut
        | _ => AuthError::SessionUnavailable,
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
        // Fill-if-absent: a relayed response already carries the serving
        // primary's view, which must not be overwritten with this follower's.
        response
            .headers_mut()
            .entry(VIEW_HEADER)
            .or_insert(HeaderValue::from(consensus.view()));
    }
    response
}

#[cfg(test)]
mod tests {
    use super::register_submit_auth_error;
    use crate::http::error::AuthError;
    use metadata::MetadataSubmitError;

    #[test]
    fn register_submit_errors_preserve_known_and_unknown_outcomes() {
        for error in [
            MetadataSubmitError::NotPrimary,
            MetadataSubmitError::NotCaughtUp,
            MetadataSubmitError::PipelineFull,
            MetadataSubmitError::PrimaryUnreachable,
        ] {
            assert!(matches!(
                register_submit_auth_error(&error),
                AuthError::SessionNotAccepted
            ));
        }
        assert!(matches!(
            register_submit_auth_error(&MetadataSubmitError::ClientIdOwnedByAnotherUser),
            AuthError::SessionIdOwnedByAnotherUser
        ));
        for error in [
            MetadataSubmitError::InProgress,
            MetadataSubmitError::Canceled,
            MetadataSubmitError::ForwardTimedOut,
        ] {
            assert!(matches!(
                register_submit_auth_error(&error),
                AuthError::SessionUnavailable
            ));
        }
    }
}
