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

//! One link per replica of the roster, and the sweep that signs in.
//!
//! `connect()` sweeps the known endpoints. The first link that answers
//! becomes the bootstrap link, and the session signs in there. A read of the
//! roster then gives one slot per replica, keyed by its replica id. Each slot
//! runs one task that dials its replica, binds the session there and redials
//! after a loss. A request never dials: it takes a ready link or fails with
//! `NotConnected`.

use super::connect::{
    ConnectCoordinator, accepts_state, is_same_address, is_same_spelling, node_address,
    transport_port,
};
use super::{
    Dialer, Link, OPERATION_TIMEOUT, backoff, event_channel, exchange, lost_link,
    pause_before_retry,
};
use crate::SDK_VERSION;
use crate::session_owner::{SessionEpoch, SessionOwner};
use async_broadcast::{Receiver, Sender};
use futures::{FutureExt, future::join_all};
use iggy_binary_protocol::codec::{WireDecode, WireEncode};
use iggy_binary_protocol::codes::{BIND_SESSION_CODE, GET_CLUSTER_METADATA_CODE, PING_CODE};
use iggy_binary_protocol::requests::system::{
    BindSessionRequest, GetClusterMetadataRequest, PingRequest,
};
use iggy_binary_protocol::responses::system::ClusterMetadataResponse;
use iggy_binary_protocol::responses::users::LoginRegisterResponse;
use iggy_common::{
    ClientState, ClusterMetadata, ClusterNodeRole, Credentials, DiagnosticEvent, IggyError,
    QuicClientReconnectionConfig, TcpClientReconnectionConfig, WebSocketClientReconnectionConfig,
    decode_session_binding, rust_sdk_version_info, sign_in_request,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::AbortHandle;
use tokio::time::{Instant, sleep, timeout_at};
use tracing::{info, warn};

/// Dials that run at once, for one client.
const MAX_CONCURRENT_DIALS: usize = 4;

/// A replica holds a bind for at most two metadata read budgets, one for its
/// frontier and one for the forward to shard 0: 6 s at the default
/// `commit_broadcast_interval`. A bind without a reply by this deadline means
/// a dead link, so the client drops it and dials again.
const BIND_TIMEOUT: Duration = Duration::from_secs(10);

/// The longest pause before a slot or a sweep tries again.
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(10);

#[cfg(test)]
const TEST_JITTER_DRAW: f64 = 0.5;

/// The reconnection configuration, the same for every transport.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReconnectPolicy {
    /// Without it, a lost link stays down, and a sweep makes one pass.
    pub(super) enabled: bool,
    /// The attempts of a slot after its first, and the passes of a sweep
    /// after its first. `None` means no limit. A keepalive restarts a slot
    /// that used them up.
    pub(super) max_retries: Option<u32>,
    /// The base of the backoff.
    pub(super) interval: Duration,
    /// A link that stayed up this long resets its backoff after a loss.
    pub(super) reestablish_after: Duration,
}

impl ReconnectPolicy {
    fn allows(&self, retry: u32) -> bool {
        self.enabled && self.max_retries.is_none_or(|max| retry <= max)
    }

    fn pause(&self, retry: u32) -> Duration {
        #[cfg(not(test))]
        let draw = rand::random();
        #[cfg(test)]
        let draw = TEST_JITTER_DRAW;
        backoff(retry, self.interval, RECONNECT_BACKOFF_MAX, draw)
    }
}

macro_rules! reconnect_policy_from {
    ($($config:ty),+) => {$(
        impl From<&$config> for ReconnectPolicy {
            fn from(config: &$config) -> Self {
                Self {
                    enabled: config.enabled,
                    max_retries: config.max_retries,
                    interval: config.interval.get_duration(),
                    reestablish_after: config.reestablish_after.get_duration(),
                }
            }
        }
    )+};
}

reconnect_policy_from!(
    TcpClientReconnectionConfig,
    QuicClientReconnectionConfig,
    WebSocketClientReconnectionConfig
);

/// Where a request goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Target {
    /// The metadata primary of the last roster, else the bootstrap link.
    MetadataPrimary,
    Replica(u8),
}

/// The links of one client. Dropping it aborts every task and drops every
/// link, without a runtime.
#[derive(Debug)]
pub(crate) struct ReplicaLinks {
    shared: Arc<LinksShared>,
}

/// The part that the tasks hold. No task holds [`ReplicaLinks`], so its
/// `Drop` always runs.
#[derive(Debug)]
struct LinksShared {
    dialer: Dialer,
    configured_endpoint: String,
    policy: ReconnectPolicy,
    session: Arc<SessionOwner>,
    /// Never held across an await. The epoch of the session is read before
    /// this lock, never under it.
    table: Mutex<Table>,
    dial_permits: Semaphore,
    coordinator: ConnectCoordinator,
    events: Sender<DiagnosticEvent>,
    /// Never read. Each subscriber is a clone, so it replays the backlog.
    subscribers: Receiver<DiagnosticEvent>,
}

#[derive(Debug)]
struct Table {
    state: ClientState,
    shutdown_complete: bool,
    /// One slot per replica of the last roster. A replica id is a `u8`, so
    /// the table holds at most 256 slots.
    slots: BTreeMap<u8, Slot>,
    bootstrap: Option<Bootstrap>,
    metadata_primary: Option<u8>,
    /// The endpoints of the last roster. A sweep dials them after the
    /// configured endpoint.
    roster: Vec<String>,
}

#[derive(Debug)]
struct Slot {
    endpoint: String,
    link: Option<ReadyLink>,
    /// `None` after the task stopped. Only the task of this handle changes
    /// the slot.
    task: Option<AbortHandle>,
}

/// The link that the sweep signed in on, until a slot takes it over or the
/// metadata primary slot is ready.
#[derive(Debug)]
struct Bootstrap {
    endpoint: String,
    link: ReadyLink,
    /// `None` while the sweep still uses the link.
    watcher: Option<AbortHandle>,
}

#[derive(Debug)]
struct ReadyLink {
    link: Arc<Link>,
    /// The fence of the session that the link carries, `None` before a
    /// sign-in.
    bound_fence: Option<u64>,
}

/// How one endpoint of a sweep ended.
enum Attempt {
    /// Another endpoint can still work.
    EndpointFailed,
    /// No retry can fix it, but the endpoints behind this one can still work.
    ConfigFault(IggyError),
    /// The sign-in lost its link. Another endpoint can still work.
    SignInLost(IggyError),
    /// The sweep ends with this error.
    Failed(IggyError),
}

#[derive(Debug)]
struct SignInFailure {
    error: IggyError,
    link_lost: bool,
}

impl From<IggyError> for SignInFailure {
    fn from(error: IggyError) -> Self {
        Self {
            link_lost: lost_link(&error),
            error,
        }
    }
}

impl ReplicaLinks {
    pub(crate) fn new(
        dialer: Dialer,
        configured_endpoint: String,
        policy: ReconnectPolicy,
        session: Arc<SessionOwner>,
    ) -> Self {
        let (events, subscribers) = event_channel();
        let table = Table {
            state: ClientState::Disconnected,
            shutdown_complete: false,
            slots: BTreeMap::new(),
            bootstrap: None,
            metadata_primary: None,
            roster: Vec::new(),
        };
        Self {
            shared: Arc::new(LinksShared {
                dialer,
                configured_endpoint,
                policy,
                session,
                table: Mutex::new(table),
                dial_permits: Semaphore::new(MAX_CONCURRENT_DIALS),
                coordinator: ConnectCoordinator::new(),
                events,
                subscribers,
            }),
        }
    }

    /// Sweeps the known endpoints until one of them signs in. Concurrent
    /// callers share one result. On a connected client, every slot without a
    /// link restarts with its initial reconnect jitter.
    pub(crate) async fn connect(&self) -> Result<(), IggyError> {
        let shared = &self.shared;
        let disconnected = shared.table().state == ClientState::Disconnected;
        if disconnected {
            shared.coordinator.wait_until_inactive().await;
        }
        shared
            .coordinator
            .run(|abandoned| shared.sweep_in(shared.session.epoch().lineage, abandoned, true))
            .await
    }

    /// An explicit attempt supersedes an automatic sweep, then reuses its connection machinery.
    pub(crate) async fn prepare_sign_in(&self) -> Result<(), IggyError> {
        let shared = &self.shared;
        let lineage = shared.session.operation_lineage().lineage();
        loop {
            if shared.session.epoch().lineage != lineage {
                return Err(IggyError::Unauthenticated);
            }
            let mut owner = false;
            let result = shared
                .coordinator
                .run(|abandoned| {
                    owner = true;
                    shared.sweep_in(lineage, abandoned, false)
                })
                .await;
            if owner {
                return result;
            }
        }
    }

    pub(crate) fn disconnect(&self) {
        self.shared.disconnect();
    }

    /// Ends every link the way its protocol expects. It is final.
    pub(crate) async fn shutdown(&self) {
        let links = {
            let mut table = self.shared.table();
            if table.shutdown_complete {
                return;
            }
            table.state = ClientState::Shutdown;
            table.clear()
        };
        join_all(links.into_iter().map(Link::shutdown)).await;
        self.shared.dialer.shutdown().await;
        let mut table = self.shared.table();
        if !table.shutdown_complete {
            table.shutdown_complete = true;
            drop(table);
            self.shared.publish(DiagnosticEvent::Shutdown);
        }
    }

    /// Pings every link that serves the session, all at once. A failed ping
    /// retires the link so its owner can rebind even when TCP remains open.
    ///
    /// Before the pings, while reconnection is on and the session is bound,
    /// or a link reset it and still owes the sign-in, it runs
    /// [`Self::connect`] when a slot used up its retries or the client is
    /// disconnected. That restarts every slot without a link with a fresh
    /// budget, or sweeps to sign in again, which nothing else does, because
    /// a request never dials. An explicit disconnect or a logout ends the
    /// session and owes nothing, so a keepalive never undoes it.
    pub(crate) async fn keepalive(&self) -> Result<bool, IggyError> {
        let stopped = {
            let epoch = self.shared.session.epoch();
            let table = self.shared.table();
            self.shared.keepalive_restarts(epoch)
                && (table.state == ClientState::Disconnected
                    || table.slots.values().any(|slot| slot.task.is_none()))
        };
        if stopped && let Err(error) = self.connect().await {
            warn!("Failed to restart the stopped links: {error}");
        }
        let links: Vec<Arc<Link>> = {
            let epoch = self.shared.session.epoch();
            let table = self.shared.table();
            if table.state == ClientState::Shutdown {
                return Err(IggyError::ClientShutdown);
            }
            table
                .links()
                .filter(|ready| serves(ready, epoch))
                .map(|ready| Arc::clone(&ready.link))
                .collect()
        };
        let deadline = Instant::now() + OPERATION_TIMEOUT;
        let pings = links.iter().map(|link| {
            exchange(
                &self.shared.session,
                link,
                PING_CODE,
                PingRequest.to_bytes(),
                deadline,
            )
        });
        let mut succeeded = false;
        for (link, result) in links.iter().zip(join_all(pings).await) {
            match result {
                Ok(_) => succeeded = true,
                Err(error) => {
                    link.close();
                    warn!("A keepalive ping failed: {error}");
                }
            }
        }
        Ok(succeeded)
    }

    /// Whether the client is up or comes back without the caller. A
    /// disconnected client comes back only when a keepalive restarts it, and
    /// a shutdown is final.
    pub(crate) fn can_recover(&self) -> bool {
        let epoch = self.shared.session.epoch();
        match self.shared.table().state {
            ClientState::Shutdown => false,
            ClientState::Disconnected => self.shared.keepalive_restarts(epoch),
            _ => true,
        }
    }

    /// The link for `target`. A request never dials, so a target without a
    /// ready link fails with `NotConnected`.
    pub(crate) fn link(&self, target: Target) -> Result<Arc<Link>, IggyError> {
        let epoch = self.shared.session.epoch();
        let table = self.shared.table();
        let slot = |id: u8| table.slots.get(&id).and_then(|slot| slot.link.as_ref());
        let ready = match target {
            Target::Replica(id) => slot(id),
            Target::MetadataPrimary => table.primary_link(epoch),
        };
        ready
            .filter(|ready| serves(ready, epoch))
            .map(|ready| Arc::clone(&ready.link))
            .ok_or(IggyError::NotConnected)
    }

    /// The replicas with a link that serves the session, in id order.
    pub(crate) fn ready_replicas(&self) -> Vec<u8> {
        let epoch = self.shared.session.epoch();
        self.shared
            .table()
            .slots
            .iter()
            .filter(|(_, slot)| slot.link.as_ref().is_some_and(|ready| serves(ready, epoch)))
            .map(|(id, _)| *id)
            .collect()
    }

    pub(crate) fn metadata_primary(&self) -> Option<u8> {
        self.shared.table().metadata_primary
    }

    pub(crate) fn session(&self) -> &Arc<SessionOwner> {
        &self.shared.session
    }

    /// Records that `link` carries the session of `fence`. A sign-in calls it
    /// before it binds the session, so the slots keep this link when they
    /// close the links of the old session.
    pub(crate) fn mark_bound(&self, link: &Arc<Link>, fence: u64) {
        self.shared.mark_bound(link, fence);
    }

    /// Reads the roster on `link` and rebuilds the slots from it.
    pub(crate) async fn refresh_roster(&self, link: &Arc<Link>) -> Result<(), IggyError> {
        timeout_at(
            iggy_common::binary_operation_deadline(),
            self.shared.refresh_roster(link),
        )
        .await
        .map_err(|_| IggyError::TransientNotAccepted)?
    }

    pub(crate) fn subscribe(&self) -> Receiver<DiagnosticEvent> {
        self.shared.subscribers.clone()
    }

    pub(crate) fn publish(&self, event: DiagnosticEvent) {
        let table = self.shared.table();
        if event == DiagnosticEvent::SignedIn && table.state != ClientState::Authenticated {
            return;
        }
        self.shared.publish(event);
    }

    pub(crate) fn state(&self) -> ClientState {
        self.shared.table().state
    }

    /// Late sign-in replies cannot undo an explicit disconnect or shutdown.
    pub(crate) fn set_state(&self, state: ClientState) {
        let mut table = self.shared.table();
        if accepts_state(table.state, state, true) {
            table.state = state;
        }
    }

    /// The endpoint of the metadata primary, else of the bootstrap link, else
    /// the configured one.
    pub(crate) fn connection_address(&self) -> String {
        let table = self.shared.table();
        table
            .metadata_primary
            .and_then(|id| table.slots.get(&id))
            .map(|slot| &slot.endpoint)
            .or(table
                .bootstrap
                .as_ref()
                .map(|bootstrap| &bootstrap.endpoint))
            .unwrap_or(&self.shared.configured_endpoint)
            .clone()
    }
}

impl Drop for ReplicaLinks {
    fn drop(&mut self) {
        // An abort needs no runtime, and each dropped link stops its own
        // tasks.
        drop(self.shared.table().clear());
    }
}

impl LinksShared {
    fn table(&self) -> MutexGuard<'_, Table> {
        self.table.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The channel runs in overflow mode, so a publish never waits. A first
    /// install and a last close publish under the table lock, so their events
    /// keep the order of the table changes.
    fn publish(&self, event: DiagnosticEvent) {
        let _ = self.events.try_broadcast(event);
    }

    /// Whether a keepalive restarts the links that stopped, see
    /// [`ReplicaLinks::keepalive`].
    fn keepalive_restarts(&self, epoch: SessionEpoch) -> bool {
        self.policy.enabled && (epoch.bound || epoch.sign_in_owed)
    }

    fn disconnect(&self) {
        let (links, previous) = {
            let mut table = self.table();
            if table.state == ClientState::Shutdown {
                return;
            }
            let links = table.clear();
            (
                links,
                std::mem::replace(&mut table.state, ClientState::Disconnected),
            )
        };
        links.iter().for_each(|link| link.close());
        if previous != ClientState::Disconnected {
            self.publish(DiagnosticEvent::Disconnected);
        }
    }

    async fn sweep_in(
        self: &Arc<Self>,
        lineage: u64,
        abandoned: bool,
        automatic_sign_in: bool,
    ) -> Result<(), IggyError> {
        let mut epoch = self.session.subscribe_epoch();
        tokio::select! {
            biased;
            _ = epoch.wait_for(|epoch| epoch.lineage != lineage) => {
                self.disconnect();
                Err(IggyError::Unauthenticated)
            }
            result = self.sweep(abandoned, automatic_sign_in) => result,
        }
    }

    async fn sweep(
        self: &Arc<Self>,
        abandoned: bool,
        automatic_sign_in: bool,
    ) -> Result<(), IggyError> {
        if abandoned {
            // The last sweep lost its caller halfway.
            self.disconnect();
        }
        let (candidates, stopped) = {
            let epoch = self.session.epoch();
            let mut table = self.table();
            match table.state {
                ClientState::Shutdown => return Err(IggyError::ClientShutdown),
                ClientState::Connected
                | ClientState::Authenticating
                | ClientState::Authenticated => {
                    if epoch.bound
                        || (!epoch.sign_in_owed
                            && table
                                .primary_link(epoch)
                                .is_some_and(|ready| ready.link.closed().now_or_never().is_none()))
                    {
                        self.restart_down_slots(&mut table);
                        return Ok(());
                    }
                    // Unbound slot tasks wait for a session, so only a
                    // bootstrap sweep can carry the next sign-in. A live
                    // primary link serves a sign-in by the caller, but an
                    // owed sign-in has no caller.
                }
                ClientState::Disconnected | ClientState::Connecting => {}
            }
            table.state = ClientState::Connecting;
            (self.candidates(&table.roster), table.clear())
        };
        stopped.iter().for_each(|link| link.close());

        let mut pass = 0;
        loop {
            let mut config_fault = None;
            let mut sign_in_failure = None;
            for endpoint in &candidates {
                match self.try_endpoint(endpoint, automatic_sign_in).await {
                    Ok(()) => return Ok(()),
                    Err(Attempt::EndpointFailed) => {}
                    Err(Attempt::ConfigFault(error)) => config_fault = Some(error),
                    Err(Attempt::SignInLost(error)) => sign_in_failure = Some(error),
                    Err(Attempt::Failed(error)) => return Err(error),
                }
            }
            pass += 1;
            let failure = match config_fault {
                Some(error) => Some(error),
                None if !self.policy.allows(pass) => {
                    Some(sign_in_failure.unwrap_or(IggyError::CannotEstablishConnection))
                }
                None => None,
            };
            if let Some(error) = failure {
                self.fail_sweep();
                return Err(error);
            }
            sweeping(self.table().state)?;
            let pause = self.policy.pause(pass);
            info!("No endpoint signed in. Pass {pass} starts in {pause:?}.");
            sleep(pause).await;
        }
    }

    /// The configured endpoint, then the endpoints of the last roster, each
    /// spelling once. Spellings need no name lookup, which can stall a sweep.
    fn candidates(&self, roster: &[String]) -> Vec<String> {
        let mut candidates = vec![self.configured_endpoint.clone()];
        for endpoint in roster {
            if !candidates
                .iter()
                .any(|known| is_same_spelling(known, endpoint))
            {
                candidates.push(endpoint.clone());
            }
        }
        candidates
    }

    async fn try_endpoint(
        self: &Arc<Self>,
        endpoint: &str,
        automatic_sign_in: bool,
    ) -> Result<(), Attempt> {
        sweeping(self.table().state).map_err(Attempt::Failed)?;
        info!("Connecting to {endpoint}...");
        let dialed = match self.dial_permits.acquire().await {
            Ok(_permit) => self.dialer.dial(endpoint).await,
            Err(_) => Err(IggyError::CannotEstablishConnection),
        };
        let link = match dialed {
            Ok(link) => Arc::new(link),
            Err(IggyError::CannotEstablishConnection) => return Err(Attempt::EndpointFailed),
            Err(error) => return Err(Attempt::ConfigFault(error)),
        };
        if let Err(error) = self.install_bootstrap(endpoint, &link) {
            link.close();
            return Err(Attempt::Failed(error));
        }
        self.publish(DiagnosticEvent::Connected);
        info!("Connected to {endpoint}.");

        if let Err(failure) = self.resume_or_sign_in(&link, automatic_sign_in).await {
            if failure.link_lost {
                warn!(
                    "The sign-in on {endpoint} did not complete: {}",
                    failure.error
                );
                self.lose_bootstrap(&link);
                return Err(Attempt::SignInLost(failure.error));
            }
            // The link stands, and only the session is missing. Every
            // endpoint gives the same answer to the same credentials.
            self.watch_bootstrap(&link);
            return Err(Attempt::Failed(failure.error));
        }
        if self.session.epoch().bound
            && let Err(error) = self.refresh_roster(&link).await
        {
            warn!("Failed to read the roster from {endpoint}: {error}");
            self.lose_bootstrap(&link);
            if lost_link(&error)
                || matches!(
                    error,
                    IggyError::TransientNotCommitted
                        | IggyError::TransientNotAccepted
                        | IggyError::NotPrimary
                        | IggyError::StaleClient
                )
            {
                return Err(Attempt::SignInLost(error));
            }
            self.fail_sweep();
            return Err(Attempt::Failed(error));
        }
        self.watch_bootstrap(&link);
        Ok(())
    }

    fn install_bootstrap(&self, endpoint: &str, link: &Arc<Link>) -> Result<(), IggyError> {
        let mut table = self.table();
        sweeping(table.state)?;
        table.bootstrap = Some(Bootstrap {
            endpoint: endpoint.to_owned(),
            link: ReadyLink {
                link: Arc::clone(link),
                bound_fence: None,
            },
            watcher: None,
        });
        table.state = ClientState::Connected;
        Ok(())
    }

    /// Drops a bootstrap link that its sign-in lost. The sweep goes on.
    fn lose_bootstrap(&self, link: &Arc<Link>) {
        link.close();
        let mut table = self.table();
        if !table.is_bootstrap(link) {
            return;
        }
        table.bootstrap = None;
        table.state = ClientState::Connecting;
        drop(table);
        self.publish(DiagnosticEvent::Disconnected);
    }

    fn fail_sweep(&self) {
        let mut table = self.table();
        if table.state == ClientState::Connecting {
            table.state = ClientState::Disconnected;
            drop(table);
            self.publish(DiagnosticEvent::Disconnected);
        }
    }

    /// Watches a bootstrap link that the sweep is done with, until it closes.
    fn watch_bootstrap(self: &Arc<Self>, link: &Arc<Link>) {
        let mut table = self.table();
        let Some(bootstrap) = table
            .bootstrap
            .as_mut()
            .filter(|bootstrap| Arc::ptr_eq(&bootstrap.link.link, link))
        else {
            return;
        };
        let shared = Arc::clone(self);
        let link = Arc::clone(link);
        let watcher = tokio::spawn(async move {
            shared.hold(&link).await;
            shared.bootstrap_down(&link);
        });
        bootstrap.watcher = Some(watcher.abort_handle());
    }

    fn bootstrap_down(&self, link: &Arc<Link>) {
        let mut table = self.table();
        if !table.is_bootstrap(link) {
            return;
        }
        table.bootstrap = None;
        let last = table.ready_count() == 0;
        table.settle();
        if last {
            self.publish(DiagnosticEvent::Disconnected);
        }
    }

    /// Resumes a bound session on the bootstrap link, else signs in there.
    /// Without `automatic_sign_in`, the caller signs in next, so the session
    /// that replaces a lost one owes no sign-in.
    async fn resume_or_sign_in(
        &self,
        link: &Arc<Link>,
        automatic_sign_in: bool,
    ) -> Result<(), SignInFailure> {
        let epoch = self.session.epoch();
        if epoch.bound {
            match self.bind_session(link).await {
                Ok(()) => {
                    self.mark_bound(link, epoch.fence);
                    self.set_state_on(link, ClientState::Authenticated);
                    self.publish(DiagnosticEvent::SignedIn);
                    info!("Resumed the session.");
                    return Ok(());
                }
                Err(error) if session_lost(&error) => {
                    info!("The session ended ({error}). Signing in again.");
                    if automatic_sign_in {
                        self.session.reset_if_fence(epoch.fence, true)?;
                    } else {
                        self.session.reset_for_sign_in(epoch.lineage)?;
                    }
                }
                // A replica that cannot bind inside its read budget is behind
                // the session, and another one can be further ahead.
                Err(error) => {
                    return Err(SignInFailure {
                        link_lost: lost_link(&error)
                            || matches!(error, IggyError::TransientNotAccepted),
                        error,
                    });
                }
            }
        }
        if automatic_sign_in {
            self.sign_in(link).await
        } else {
            Ok(())
        }
    }

    /// Signs in on `link` with the known credentials, if any. A failure that
    /// leaves the link up leaves the client connected without a session. A
    /// rejected credential is forgotten, because it stays rejected, and each
    /// replay costs the server a password hash. The calls that wait for the
    /// session fail with `Unauthenticated`, so the log keeps the rejection.
    async fn sign_in(&self, link: &Arc<Link>) -> Result<(), SignInFailure> {
        let started = self.session.epoch();
        loop {
            let epoch = self.session.epoch();
            if epoch.fence != started.fence || epoch.lineage != started.lineage {
                return Err(IggyError::Unauthenticated.into());
            }
            let Some(credentials) = self.session.sign_in_credentials() else {
                info!("No credentials to sign in with.");
                return Ok(());
            };
            self.set_state_on(link, ClientState::Authenticating);
            let error = match self.register(link, &credentials).await {
                Ok(user_id) => {
                    self.finish_sign_in(link, started.fence, user_id)?;
                    info!("Signed in.");
                    return Ok(());
                }
                Err(error) => error,
            };
            let failure = SignInFailure::from(error);
            if !failure.link_lost {
                self.set_state_on(link, ClientState::Connected);
            }
            if rejects_credentials(&failure.error) {
                if self.session.refuse_sign_in(started, &credentials) {
                    continue;
                }
                warn!(
                    "The server rejected the attempted sign-in ({}).",
                    failure.error
                );
            }
            return Err(failure);
        }
    }

    fn finish_sign_in(
        &self,
        link: &Arc<Link>,
        fence: u64,
        user_id: u32,
    ) -> Result<(), SignInFailure> {
        let remembered = self.session.remember_recovered_unless(
            || {
                let current_fence = self.session.fence();
                let mut table = self.table();
                if current_fence != fence || table.find(link).is_none() {
                    return true;
                }
                table.state = ClientState::Authenticated;
                self.publish(DiagnosticEvent::SignedIn);
                false
            },
            user_id,
        );
        if remembered {
            Ok(())
        } else {
            Err(IggyError::Disconnected.into())
        }
    }

    /// Registers a new session on `link` and binds it. A Register that the
    /// server did not commit or did not admit goes again with the same client
    /// id, so it registers one session.
    async fn register(
        &self,
        link: &Arc<Link>,
        credentials: &Credentials,
    ) -> Result<u32, IggyError> {
        let fence = self.session.fence();
        let (code, body) = sign_in_request(
            credentials,
            rust_sdk_version_info(SDK_VERSION)?,
            self.session.bind_secret()?,
        )?;
        let deadline = Instant::now() + OPERATION_TIMEOUT;
        let mut retry = 0_u32;
        let reply = loop {
            let error = match exchange(&self.session, link, code, body.clone(), deadline).await {
                Ok(reply) => break reply,
                Err(error) => error,
            };
            if !matches!(
                error,
                IggyError::TransientNotCommitted | IggyError::TransientNotAccepted
            ) || !pause_before_retry(retry, deadline, rand::random()).await
            {
                return Err(error);
            }
            retry = retry.saturating_add(1);
        };
        let registered =
            LoginRegisterResponse::decode_from(&reply).map_err(|_| IggyError::InvalidFormat)?;
        self.mark_bound(link, fence);
        self.session.bind_at(fence, registered.session)?;
        Ok(registered.user_id)
    }

    /// Binds the bound session on `link`. The reply must name the same
    /// session.
    async fn bind_session(&self, link: &Link) -> Result<(), IggyError> {
        let identity = self.session.identity()?;
        let request = BindSessionRequest {
            version_info: rust_sdk_version_info(SDK_VERSION)?,
            identity,
            bind_secret: self.session.bind_secret()?,
        };
        let reply = exchange(
            &self.session,
            link,
            BIND_SESSION_CODE,
            request.to_bytes(),
            Instant::now() + BIND_TIMEOUT,
        )
        .await?;
        decode_session_binding(&identity, &reply).map(drop)
    }

    async fn refresh_roster(self: &Arc<Self>, link: &Arc<Link>) -> Result<(), IggyError> {
        let reply = exchange(
            &self.session,
            link,
            GET_CLUSTER_METADATA_CODE,
            GetClusterMetadataRequest.to_bytes(),
            Instant::now() + OPERATION_TIMEOUT,
        )
        .await?;
        let roster =
            ClusterMetadataResponse::decode_from(&reply).map_err(|_| IggyError::InvalidFormat)?;
        self.apply_roster(&ClusterMetadata::try_from(roster)?, link)
            .await
    }

    /// Rebuilds the slots from the roster that `via` carried. A removed
    /// replica or a moved endpoint retires its link, so the requests that
    /// hold it finish first. The slot of a one-node roster dials the
    /// configured endpoint.
    async fn apply_roster(
        self: &Arc<Self>,
        roster: &ClusterMetadata,
        via: &Arc<Link>,
    ) -> Result<(), IggyError> {
        let protocol = self.dialer.protocol();
        let mut ids = BTreeSet::new();
        let mut wanted = BTreeMap::new();
        for node in &roster.nodes {
            if !ids.insert(node.replica_id) {
                warn!(
                    "The roster names replica {} twice, so the links stay as they are.",
                    node.replica_id
                );
                return Err(IggyError::InvalidFormat);
            }
            // A node without a port for this transport gets no slot, so the
            // client never dials `ip:0`.
            let port = transport_port(node, protocol);
            if port != 0 {
                wanted.insert(node.replica_id, node_address(node, port));
            }
        }
        // A single node advertises its own view of its address, such as a
        // bind address or a container name. Only the configured endpoint is
        // known to reach it.
        if wanted.len() == 1
            && let Some(endpoint) = wanted.values_mut().next()
        {
            endpoint.clone_from(&self.configured_endpoint);
        }
        let primary = roster
            .nodes
            .iter()
            .find(|node| node.role == ClusterNodeRole::Leader)
            .map(|node| node.replica_id)
            .filter(|id| wanted.contains_key(id));
        let adopt = self.bootstrap_replica(&wanted).await;

        let mut retired = Vec::new();
        let mut table = self.table();
        if table.find(via).is_none() {
            // A disconnect ran during the read.
            return Err(IggyError::Disconnected);
        }
        table.slots.retain(|id, slot| {
            let kept = wanted.get(id) == Some(&slot.endpoint);
            if !kept {
                if let Some(task) = slot.task.take() {
                    task.abort();
                }
                retired.extend(slot.link.take());
            }
            kept
        });
        for (id, endpoint) in &wanted {
            if table.slots.contains_key(id) {
                continue;
            }
            let adopted = match &adopt {
                Some((link, replica)) if replica == id && table.is_bootstrap(link) => {
                    table.retire_bootstrap()
                }
                _ => None,
            };
            let task = self.spawn_slot(
                *id,
                endpoint.clone(),
                adopted.as_ref().map(|ready| Arc::clone(&ready.link)),
            );
            table.slots.insert(
                *id,
                Slot {
                    endpoint: endpoint.clone(),
                    link: adopted,
                    task: Some(task),
                },
            );
        }
        table.metadata_primary = primary;
        table.roster = wanted.into_values().collect();
        if primary.is_some_and(|id| table.slots.get(&id).is_some_and(|slot| slot.link.is_some())) {
            retired.extend(table.retire_bootstrap());
        }
        drop(table);
        drop(retired);
        Ok(())
    }

    /// The replica that the bootstrap link reaches. With one replica in the
    /// roster, that replica, because no other one serves this transport.
    async fn bootstrap_replica(&self, wanted: &BTreeMap<u8, String>) -> Option<(Arc<Link>, u8)> {
        let (link, endpoint) = {
            let table = self.table();
            let bootstrap = table.bootstrap.as_ref()?;
            (Arc::clone(&bootstrap.link.link), bootstrap.endpoint.clone())
        };
        if wanted.len() == 1 {
            return wanted.keys().next().map(|id| (link, *id));
        }
        for (id, replica) in wanted {
            if is_same_address(&endpoint, replica).await {
                return Some((link, *id));
            }
        }
        None
    }

    fn spawn_slot(
        self: &Arc<Self>,
        id: u8,
        endpoint: String,
        adopted: Option<Arc<Link>>,
    ) -> AbortHandle {
        tokio::spawn(Arc::clone(self).run_slot(id, endpoint, adopted)).abort_handle()
    }

    /// Restarts each slot without a link using its initial reconnect jitter.
    fn restart_down_slots(self: &Arc<Self>, table: &mut Table) {
        for (id, slot) in table
            .slots
            .iter_mut()
            .filter(|(_, slot)| slot.link.is_none())
        {
            if let Some(task) = slot.task.take() {
                task.abort();
            }
            slot.task = Some(self.spawn_slot(*id, slot.endpoint.clone(), None));
        }
    }

    /// Every background dial waits its jitter, including the first attempt
    /// after a stable link resets the backoff.
    async fn run_slot(self: Arc<Self>, id: u8, endpoint: String, mut adopted: Option<Arc<Link>>) {
        let mut signs_in_at = None;
        let mut retry = 0_u32;
        loop {
            let link = match adopted.take() {
                Some(link) => link,
                None => {
                    if retry > 0 && !self.policy.allows(retry) {
                        break;
                    }
                    sleep(self.policy.pause(retry)).await;
                    match self.open(id, &endpoint, &mut signs_in_at).await {
                        Some(link) => link,
                        None => {
                            retry = retry.saturating_add(1);
                            continue;
                        }
                    }
                }
            };
            let up_since = Instant::now();
            self.hold(&link).await;
            self.slot_down(id, &link);
            if !self.policy.enabled {
                break;
            }
            retry = if up_since.elapsed() >= self.policy.reestablish_after {
                0
            } else {
                retry.saturating_add(1)
            };
        }
        self.slot_stopped(id);
    }

    /// Dials replica `id` and binds the session there. If the bind finds the
    /// session gone and this task resets it, this task signs in on its new
    /// link, and it owes that sign-in until one succeeds.
    async fn open(
        &self,
        id: u8,
        endpoint: &str,
        signs_in_at: &mut Option<u64>,
    ) -> Option<Arc<Link>> {
        let owed = *signs_in_at;
        // An extra link has no use without a session.
        let mut epochs = self.session.subscribe_epoch();
        epochs
            .wait_for(|epoch| epoch.bound || Some(epoch.fence) == owed)
            .await
            .ok()?;
        let _permit = self.dial_permits.acquire().await.ok()?;
        let link = match self.dialer.dial(endpoint).await {
            Ok(link) => Arc::new(link),
            Err(error) => {
                warn!("Failed to connect to replica {id} at {endpoint}: {error}");
                return None;
            }
        };
        let epoch = self.session.epoch();
        if epoch.bound {
            let error = match self.bind_session(&link).await {
                Ok(()) => {
                    *signs_in_at = None;
                    return self.install(id, link, Some(epoch.fence));
                }
                Err(error) => error,
            };
            let reset = if session_lost(&error) {
                self.session
                    .reset_if_fence(epoch.fence, true)
                    .ok()
                    .flatten()
            } else {
                None
            };
            let Some(fence) = reset else {
                warn!("Failed to bind the session on replica {id} at {endpoint}: {error}");
                link.close();
                return None;
            };
            warn!("The session ended ({error}). Replica {id} signs in again.");
            *signs_in_at = Some(fence);
        } else if *signs_in_at != Some(epoch.fence) {
            link.close();
            return None;
        }
        let link = self.install(id, link, None)?;
        match self.sign_in(&link).await {
            Err(failure) if failure.link_lost => link.close(),
            _ => *signs_in_at = None,
        }
        Some(link)
    }

    /// Makes `link` the link of slot `id`, while this task still runs the
    /// slot and no reset replaced the session that the link carries.
    fn install(&self, id: u8, link: Arc<Link>, bound_fence: Option<u64>) -> Option<Arc<Link>> {
        let current = bound_fence.is_none_or(|fence| fence == self.session.fence());
        let mut table = self.table();
        let first = table.ready_count() == 0;
        let Some(slot) = table.own_slot(id).filter(|_| current) else {
            drop(table);
            link.close();
            return None;
        };
        slot.link = Some(ReadyLink {
            link: Arc::clone(&link),
            bound_fence,
        });
        let retired = if table.metadata_primary == Some(id) {
            table.retire_bootstrap()
        } else {
            None
        };
        if first {
            self.publish(DiagnosticEvent::Connected);
            // After every link closed, a link that binds the session again
            // resumes it, as a sweep does.
            if bound_fence.is_some() && table.state == ClientState::Authenticated {
                self.publish(DiagnosticEvent::SignedIn);
            }
        }
        drop(table);
        drop(retired);
        info!("The link to replica {id} is ready.");
        Some(link)
    }

    fn slot_down(&self, id: u8, link: &Arc<Link>) {
        let mut table = self.table();
        let Some(slot) = table.own_slot(id) else {
            return;
        };
        if !slot
            .link
            .as_ref()
            .is_some_and(|ready| Arc::ptr_eq(&ready.link, link))
        {
            return;
        }
        slot.link = None;
        if table.ready_count() == 0 {
            self.publish(DiagnosticEvent::Disconnected);
        }
        drop(table);
        warn!("The link to replica {id} closed.");
    }

    fn slot_stopped(&self, id: u8) {
        let mut table = self.table();
        let Some(slot) = table.own_slot(id) else {
            return;
        };
        slot.task = None;
        // Subscribers read the state on each event, and the Disconnected of
        // the last close came while a slot still tried again. Only this one
        // shows that the client stopped.
        if table.settle() {
            self.publish(DiagnosticEvent::Disconnected);
        }
        drop(table);
        warn!("Replica {id} stays down until a keepalive or a connect restarts it.");
    }

    /// Waits until `link` closes. When a sign-in binds a new session, a link
    /// that carries an older one closes here: the server keeps that link
    /// bound to the old session.
    async fn hold(&self, link: &Arc<Link>) {
        let mut epochs = self.session.subscribe_epoch();
        loop {
            let epoch = *epochs.borrow_and_update();
            if epoch.bound && self.bound_fence(link) != Some(epoch.fence) {
                link.close();
                return;
            }
            tokio::select! {
                () = link.closed() => return,
                changed = epochs.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
            }
        }
    }

    fn bound_fence(&self, link: &Arc<Link>) -> Option<u64> {
        self.table().find(link).and_then(|ready| ready.bound_fence)
    }

    fn mark_bound(&self, link: &Arc<Link>, fence: u64) {
        if let Some(ready) = self.table().find_mut(link) {
            ready.bound_fence = Some(fence);
        }
    }

    /// Sets the state for a sign-in on `link`, unless a disconnect or a
    /// roster change took the link away.
    fn set_state_on(&self, link: &Arc<Link>, state: ClientState) {
        let signing_in_again =
            state == ClientState::Authenticating && self.session.epoch().sign_in_owed;
        let mut table = self.table();
        if table.find(link).is_some() {
            table.state = state;
            if signing_in_again {
                self.publish(DiagnosticEvent::SignedOut);
            }
        }
    }
}

impl Table {
    fn primary_link(&self, epoch: SessionEpoch) -> Option<&ReadyLink> {
        self.metadata_primary
            .and_then(|id| self.slots.get(&id))
            .and_then(|slot| slot.link.as_ref())
            .filter(|ready| serves(ready, epoch))
            .or(self.bootstrap.as_ref().map(|bootstrap| &bootstrap.link))
    }

    fn links(&self) -> impl Iterator<Item = &ReadyLink> {
        self.slots
            .values()
            .filter_map(|slot| slot.link.as_ref())
            .chain(self.bootstrap.iter().map(|bootstrap| &bootstrap.link))
    }

    fn find(&self, link: &Arc<Link>) -> Option<&ReadyLink> {
        self.links().find(|ready| Arc::ptr_eq(&ready.link, link))
    }

    fn find_mut(&mut self, link: &Arc<Link>) -> Option<&mut ReadyLink> {
        self.slots
            .values_mut()
            .filter_map(|slot| slot.link.as_mut())
            .chain(
                self.bootstrap
                    .iter_mut()
                    .map(|bootstrap| &mut bootstrap.link),
            )
            .find(|ready| Arc::ptr_eq(&ready.link, link))
    }

    fn ready_count(&self) -> usize {
        self.links().count()
    }

    fn is_bootstrap(&self, link: &Arc<Link>) -> bool {
        self.bootstrap
            .as_ref()
            .is_some_and(|bootstrap| Arc::ptr_eq(&bootstrap.link.link, link))
    }

    fn retire_bootstrap(&mut self) -> Option<ReadyLink> {
        let bootstrap = self.bootstrap.take()?;
        if let Some(watcher) = bootstrap.watcher {
            watcher.abort();
        }
        Some(bootstrap.link)
    }

    /// Aborts every task and empties the table, but keeps the state and the
    /// roster. Returns the links, for the caller to close.
    fn clear(&mut self) -> Vec<Arc<Link>> {
        self.metadata_primary = None;
        let slots = std::mem::take(&mut self.slots)
            .into_values()
            .filter_map(|slot| {
                if let Some(task) = slot.task {
                    task.abort();
                }
                slot.link
            });
        slots
            .chain(self.retire_bootstrap())
            .map(|ready| ready.link)
            .collect()
    }

    /// The slot `id`, while the current task runs it.
    fn own_slot(&mut self, id: u8) -> Option<&mut Slot> {
        let current = tokio::task::try_id()?;
        self.slots
            .get_mut(&id)
            .filter(|slot| slot.task.as_ref().is_some_and(|task| task.id() == current))
    }

    /// Without a link and without a slot task that tries again, only a
    /// keepalive or a connect brings the client back, so it counts as
    /// disconnected. Returns whether this changed the state.
    fn settle(&mut self) -> bool {
        let retrying = self.slots.values().any(|slot| slot.task.is_some());
        let stopped = self.ready_count() == 0
            && !retrying
            && matches!(
                self.state,
                ClientState::Connected | ClientState::Authenticating | ClientState::Authenticated
            );
        if stopped {
            self.state = ClientState::Disconnected;
        }
        stopped
    }
}

/// While a session is bound, only a link that carries it serves requests.
/// Without one, every link does, for example for the next sign-in.
fn serves(ready: &ReadyLink, epoch: SessionEpoch) -> bool {
    !epoch.bound || ready.bound_fence == Some(epoch.fence)
}

/// Whether a sweep can go on in `state`. A disconnect or a shutdown ends it.
fn sweeping(state: ClientState) -> Result<(), IggyError> {
    match state {
        ClientState::Connecting => Ok(()),
        ClientState::Shutdown => Err(IggyError::ClientShutdown),
        _ => Err(IggyError::Disconnected),
    }
}

/// Whether a bind failed because the server no longer knows the session.
/// `StaleClient` is left out because no server path answers a BindSession with it.
fn session_lost(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::Unauthenticated | IggyError::TransientNotCommitted
    )
}

/// Whether the server rejected the credentials of a sign-in for good, so a
/// replay gets the same answer.
fn rejects_credentials(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::InvalidCredentials
            | IggyError::InvalidUsername
            | IggyError::InvalidPassword
            | IggyError::InvalidPersonalAccessToken
            | IggyError::PersonalAccessTokenExpired(..)
            | IggyError::UserInactive
            | IggyError::Unauthenticated
            | IggyError::InvalidFormat
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::links::dial::DIAL_TIMEOUT;
    use crate::links::fake_cluster::{
        Answer, FakeCluster, INTERVAL, PORT, REESTABLISH_AFTER, TEST_BUDGET, UNANSWERED_CODE,
        connected, endpoint, eventually, ip, links, node, policy,
    };
    use bytes::Bytes;
    use iggy_binary_protocol::codes::LOGIN_REGISTER_CODE;
    use std::sync::atomic::Ordering;

    /// A load balancer in front of the cluster, outside its roster.
    const BALANCER: &str = "10.0.9.9:8090";

    fn slot_endpoints(links: &ReplicaLinks) -> Vec<(u8, String)> {
        links
            .shared
            .table()
            .slots
            .iter()
            .map(|(id, slot)| (*id, slot.endpoint.clone()))
            .collect()
    }

    fn slot_task_runs(links: &ReplicaLinks, id: u8) -> bool {
        links.shared.table().slots[&id].task.is_some()
    }

    #[tokio::test(start_paused = true)]
    async fn given_cluster_metadata_when_building_slots_should_key_by_replica_id_and_skip_port_zero()
     {
        let cluster = FakeCluster::new(0);
        *cluster.nodes.lock().unwrap() = vec![
            node(2, &ip(2), PORT, false),
            node(0, &ip(0), PORT, true),
            node(1, &ip(1), 0, false),
        ];
        let links = links(&cluster, &endpoint(0), policy());

        links.connect().await.unwrap();
        eventually("both slots are ready", || links.ready_replicas() == [0, 2]).await;

        assert_eq!(slot_endpoints(&links), [(0, endpoint(0)), (2, endpoint(2))]);
        assert!(cluster.dials_to(&format!("{}:0", ip(1))).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_duplicate_replica_id_when_building_slots_should_keep_the_previous_table() {
        let cluster = FakeCluster::new(2);
        let links = connected(&cluster, 2).await;
        cluster
            .nodes
            .lock()
            .unwrap()
            .push(node(1, "10.0.0.9", PORT, false));
        let link = links.link(Target::MetadataPrimary).unwrap();

        let refreshed = links.refresh_roster(&link).await;

        assert_eq!(refreshed.unwrap_err(), IggyError::InvalidFormat);
        assert_eq!(slot_endpoints(&links), [(0, endpoint(0)), (1, endpoint(1))]);
        assert_eq!(links.ready_replicas(), [0, 1]);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_refused_initial_roster_when_connecting_should_install_managed_slots() {
        let cluster = FakeCluster::new(1);
        cluster.script(
            &endpoint(0),
            GET_CLUSTER_METADATA_CODE,
            [Answer::Refuse(IggyError::TransientNotAccepted)],
        );
        let links = links(&cluster, &endpoint(0), policy());

        links.connect().await.unwrap();

        assert_eq!(links.ready_replicas(), [0]);
        assert!(slot_task_runs(&links, 0));
        assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_stale_client_reply_to_the_initial_roster_when_connecting_should_sign_in_again()
    {
        let cluster = FakeCluster::new(1);
        cluster.script(
            &endpoint(0),
            GET_CLUSTER_METADATA_CODE,
            [Answer::Refuse(IggyError::StaleClient)],
        );
        let links = links(&cluster, &endpoint(0), policy());

        links.connect().await.unwrap();

        assert_eq!(links.ready_replicas(), [0]);
        assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_single_node_roster_when_its_address_differs_should_adopt_the_bootstrap_link() {
        let cluster = FakeCluster::new(0);
        *cluster.nodes.lock().unwrap() = vec![node(0, "0.0.0.0", PORT, true)];
        let links = links(&cluster, "127.0.0.1:8090", policy());

        links.connect().await.unwrap();
        sleep(RECONNECT_BACKOFF_MAX).await;

        assert_eq!(links.ready_replicas(), [0]);
        assert!(links.shared.table().bootstrap.is_none());
        assert_eq!(cluster.dials.lock().unwrap().len(), 1, "no dial to 0.0.0.0");
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_single_node_roster_when_its_link_drops_should_redial_the_configured_endpoint()
    {
        // A port map in front of the node, which advertises an address that
        // the client cannot reach.
        const MAPPED: &str = "127.0.0.1:18090";
        let cluster = FakeCluster::new(1);
        cluster.down.lock().unwrap().insert(endpoint(0));
        let links = links(&cluster, MAPPED, policy());
        links.connect().await.unwrap();

        cluster.drop_connections(MAPPED);
        eventually("the slot redials the configured endpoint", || {
            cluster.dials_to(MAPPED).len() == 2 && links.ready_replicas() == [0]
        })
        .await;
        let link = links.link(Target::Replica(0)).unwrap();
        links.refresh_roster(&link).await.unwrap();

        assert!(cluster.dials_to(&endpoint(0)).is_empty());
        assert_eq!(slot_endpoints(&links), [(0, MAPPED.to_owned())]);
        assert!(Arc::ptr_eq(&links.link(Target::Replica(0)).unwrap(), &link));
        assert_eq!(links.connection_address(), MAPPED);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_bootstrap_endpoint_outside_the_roster_when_the_primary_slot_is_ready_should_close_the_bootstrap_link()
     {
        let cluster = FakeCluster::new(2);
        let links = links(&cluster, BALANCER, policy());

        links.connect().await.unwrap();
        eventually("the primary slot is ready", || {
            links.ready_replicas().contains(&0)
        })
        .await;

        assert!(links.shared.table().bootstrap.is_none());
        eventually("the balancer connection closes", || {
            cluster.open_connections(BALANCER) == 0
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn given_extra_links_when_bound_should_send_bind_session_and_never_register() {
        let cluster = FakeCluster::new(3);

        let _links = connected(&cluster, 3).await;

        assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 1);
        assert_eq!(cluster.codes(&endpoint(0))[0], LOGIN_REGISTER_CODE);
        for id in 1..3 {
            assert_eq!(cluster.codes(&endpoint(id)), [BIND_SESSION_CODE]);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_replica_that_never_answers_a_bind_when_its_deadline_passes_should_redial_and_bind()
     {
        let cluster = FakeCluster::new(2);
        cluster.script(&endpoint(1), BIND_SESSION_CODE, [Answer::Silence]);

        let links = connected(&cluster, 2).await;

        let dials = cluster.dials_to(&endpoint(1));
        let pause = backoff(1, INTERVAL, RECONNECT_BACKOFF_MAX, TEST_JITTER_DRAW);
        assert_eq!(dials.len(), 2);
        assert_eq!(dials[1], dials[0] + BIND_TIMEOUT + pause);
        assert_eq!(
            cluster.codes(&endpoint(1)),
            [BIND_SESSION_CODE, BIND_SESSION_CODE]
        );
        assert_eq!(links.state(), ClientState::Authenticated);
    }

    #[tokio::test(start_paused = true)]
    async fn given_six_silent_replicas_when_dialing_should_keep_four_dials_and_cut_each_at_two_seconds()
     {
        let cluster = FakeCluster::new(7);
        cluster.silent.lock().unwrap().extend((1..7).map(endpoint));
        let links = links(&cluster, &endpoint(0), policy());

        links.connect().await.unwrap();
        sleep(DIAL_TIMEOUT * 3).await;

        let cut_dials = cluster.cut_dials.lock().unwrap().clone();
        assert_eq!(cluster.max_open_dials.load(Ordering::SeqCst), 4);
        assert!(cut_dials.len() >= 6, "every silent replica was dialed");
        assert!(cut_dials.iter().all(|lasted| *lasted == DIAL_TIMEOUT));
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_stable_link_when_it_drops_should_jitter_its_first_redial() {
        let cluster = FakeCluster::new(2);
        let links = connected(&cluster, 2).await;
        sleep(REESTABLISH_AFTER).await;

        let dropped_at = Instant::now();
        cluster.drop_connections(&endpoint(1));
        eventually("replica 1 is ready again", || {
            cluster.dials_to(&endpoint(1)).len() == 2 && links.ready_replicas().len() == 2
        })
        .await;

        let delay = backoff(0, INTERVAL, RECONNECT_BACKOFF_MAX, TEST_JITTER_DRAW);
        assert_eq!(cluster.dials_to(&endpoint(1))[1], dropped_at + delay);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_new_client_when_connecting_should_dial_the_bootstrap_without_backoff() {
        let cluster = FakeCluster::new(1);
        let links = links(&cluster, &endpoint(0), policy());
        let started = Instant::now();

        links.connect().await.unwrap();

        assert_eq!(cluster.dials_to(&endpoint(0)), [started]);
    }

    #[tokio::test(start_paused = true)]
    async fn given_reconnection_disabled_when_a_link_drops_should_not_redial() {
        let cluster = FakeCluster::new(2);
        let disabled = ReconnectPolicy {
            enabled: false,
            ..policy()
        };
        let links = links(&cluster, &endpoint(0), disabled);
        links.connect().await.unwrap();
        eventually("both slots are ready", || links.ready_replicas().len() == 2).await;
        // A stable link resets its retry count, but still honors the switch.
        sleep(REESTABLISH_AFTER).await;

        cluster.drop_connections(&endpoint(1));
        sleep(RECONNECT_BACKOFF_MAX * 2).await;
        assert!(links.keepalive().await.unwrap());

        assert_eq!(cluster.dials_to(&endpoint(1)).len(), 1);
        assert_eq!(links.ready_replicas(), [0]);
        assert!(!slot_task_runs(&links, 1));
    }

    #[tokio::test(start_paused = true)]
    async fn given_max_retries_when_a_slot_keeps_failing_should_stop_after_that_many_attempts() {
        const MAX_RETRIES: u32 = 3;
        let cluster = FakeCluster::new(2);
        cluster.down.lock().unwrap().insert(endpoint(1));
        let limited = ReconnectPolicy {
            max_retries: Some(MAX_RETRIES),
            ..policy()
        };
        let links = links(&cluster, &endpoint(0), limited);

        links.connect().await.unwrap();
        eventually("slot 1 stops", || !slot_task_runs(&links, 1)).await;

        assert_eq!(
            cluster.dials_to(&endpoint(1)).len(),
            1 + MAX_RETRIES as usize
        );
        assert_eq!(links.state(), ClientState::Authenticated);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_slot_out_of_retries_when_keepalive_ticks_should_restart_it_with_a_fresh_budget()
     {
        const MAX_RETRIES: u32 = 1;
        let cluster = FakeCluster::new(2);
        cluster.down.lock().unwrap().insert(endpoint(1));
        let limited = ReconnectPolicy {
            max_retries: Some(MAX_RETRIES),
            ..policy()
        };
        let links = links(&cluster, &endpoint(0), limited);
        links.connect().await.unwrap();
        eventually("slot 1 stops", || !slot_task_runs(&links, 1)).await;

        assert!(links.keepalive().await.unwrap());
        eventually("slot 1 stops again", || !slot_task_runs(&links, 1)).await;
        assert_eq!(
            cluster.dials_to(&endpoint(1)).len(),
            2 * (1 + MAX_RETRIES as usize)
        );

        cluster.down.lock().unwrap().clear();
        assert!(links.keepalive().await.unwrap());
        eventually("slot 1 is ready", || links.ready_replicas() == [0, 1]).await;
        assert_eq!(links.state(), ClientState::Authenticated);
    }

    #[tokio::test(start_paused = true)]
    async fn given_every_slot_out_of_retries_when_keepalive_ticks_should_resume_the_session() {
        let cluster = FakeCluster::new(1);
        let limited = ReconnectPolicy {
            max_retries: Some(1),
            ..policy()
        };
        let links = links(&cluster, &endpoint(0), limited);
        links.connect().await.unwrap();
        cluster.down.lock().unwrap().insert(endpoint(0));
        cluster.drop_connections(&endpoint(0));
        eventually("the client is disconnected", || {
            links.state() == ClientState::Disconnected
        })
        .await;

        assert!(!links.keepalive().await.unwrap());
        assert_eq!(links.state(), ClientState::Disconnected);
        cluster.down.lock().unwrap().clear();
        assert!(links.keepalive().await.unwrap());

        assert_eq!(links.state(), ClientState::Authenticated);
        assert_eq!(links.ready_replicas(), [0]);
        assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_slot_that_owes_the_sign_in_when_it_runs_out_of_retries_should_sign_in_on_the_next_keepalive()
     {
        let cluster = FakeCluster::new(1);
        let limited = ReconnectPolicy {
            max_retries: Some(1),
            ..policy()
        };
        let links = links(&cluster, &endpoint(0), limited);
        links.connect().await.unwrap();
        // The bind finds the session gone, so the slot resets it, and the
        // sign-in that the slot owes loses its link.
        *cluster.session.lock().unwrap() = None;
        cluster
            .hang_up_on_register
            .lock()
            .unwrap()
            .insert(endpoint(0));
        cluster.drop_connections(&endpoint(0));
        eventually("the client is disconnected", || {
            links.state() == ClientState::Disconnected
        })
        .await;
        assert!(!links.shared.session.epoch().bound);

        cluster.hang_up_on_register.lock().unwrap().clear();
        assert!(links.keepalive().await.unwrap());

        assert_eq!(links.state(), ClientState::Authenticated);
        assert!(links.shared.session.epoch().bound);
        assert_eq!(links.ready_replicas(), [0]);
        assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_live_primary_link_when_the_slot_that_owes_the_sign_in_stops_should_sign_in_on_the_next_keepalive()
     {
        let cluster = FakeCluster::new(2);
        let limited = ReconnectPolicy {
            max_retries: Some(1),
            ..policy()
        };
        let links = links(&cluster, &endpoint(0), limited);
        links.connect().await.unwrap();
        eventually("both slots are ready", || links.ready_replicas() == [0, 1]).await;
        *cluster.session.lock().unwrap() = None;
        cluster
            .hang_up_on_register
            .lock()
            .unwrap()
            .insert(endpoint(1));
        cluster.drop_connections(&endpoint(1));
        eventually("slot 1 stops", || !slot_task_runs(&links, 1)).await;
        assert!(!links.shared.session.epoch().bound);

        cluster.hang_up_on_register.lock().unwrap().clear();
        assert!(links.keepalive().await.unwrap());

        eventually("both slots serve a new session", || {
            links.shared.session.epoch().bound && links.ready_replicas() == [0, 1]
        })
        .await;
        assert_eq!(links.state(), ClientState::Authenticated);
        assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_rejected_sign_in_after_the_session_was_lost_when_keepalive_ticks_should_not_sign_in_again()
     {
        let cluster = FakeCluster::new(1);
        let limited = ReconnectPolicy {
            max_retries: Some(1),
            ..policy()
        };
        let links = links(&cluster, &endpoint(0), limited);
        links.connect().await.unwrap();
        cluster.down.lock().unwrap().insert(endpoint(0));
        cluster.drop_connections(&endpoint(0));
        eventually("the client is disconnected", || {
            links.state() == ClientState::Disconnected
        })
        .await;
        *cluster.session.lock().unwrap() = None;
        *cluster.reject_credentials.lock().unwrap() = true;
        cluster.down.lock().unwrap().clear();
        links.keepalive().await.unwrap();
        assert_eq!(links.state(), ClientState::Connected);
        cluster.drop_connections(&endpoint(0));
        eventually("the client is disconnected", || {
            links.state() == ClientState::Disconnected
        })
        .await;

        let pinged = links.keepalive().await.unwrap();

        assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 2);
        assert!(!pinged);
        assert_eq!(links.state(), ClientState::Disconnected);
    }

    #[tokio::test(start_paused = true)]
    async fn given_ready_links_when_keepalive_ticks_should_ping_each_one() {
        let cluster = FakeCluster::new(3);
        let links = connected(&cluster, 3).await;

        assert!(links.keepalive().await.unwrap());

        for id in 0..3 {
            let pings = cluster
                .codes(&endpoint(id))
                .into_iter()
                .filter(|code| *code == PING_CODE)
                .count();
            assert_eq!(pings, 1, "replica {id}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_silent_links_when_keepalive_expires_should_rebind_the_same_session() {
        let cluster = FakeCluster::new(2);
        let links = connected(&cluster, 2).await;
        let fence = links.shared.session.fence();
        for id in 0..2 {
            cluster.script(&endpoint(id), PING_CODE, [Answer::Silence]);
        }

        assert!(!links.keepalive().await.unwrap());
        eventually("both silent links rebind", || {
            (0..2).all(|id| cluster.dials_to(&endpoint(id)).len() == 2)
                && links.ready_replicas().len() == 2
        })
        .await;
        assert_eq!(links.shared.session.fence(), fence);
        assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 1);
        assert!(links.keepalive().await.unwrap());

        // As `Client::disconnect` does, so the keepalive does not reconnect.
        links.disconnect();
        links.shared.session.reset().unwrap();
        assert!(!links.keepalive().await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn given_one_of_three_links_lost_when_others_are_ready_should_publish_no_event() {
        let cluster = FakeCluster::new(3);
        let links = connected(&cluster, 3).await;
        let mut events = links.subscribe();
        while events.try_recv().is_ok() {}

        cluster.drop_connections(&endpoint(2));
        eventually("replica 2 is ready again", || {
            cluster.dials_to(&endpoint(2)).len() == 2 && links.ready_replicas().len() == 3
        })
        .await;

        assert!(events.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn given_every_link_lost_when_a_slot_binds_the_session_again_should_publish_signed_in() {
        let cluster = FakeCluster::new(3);
        let links = connected(&cluster, 3).await;
        let fence = links.shared.session.fence();
        let mut events = links.subscribe();
        while events.try_recv().is_ok() {}

        // Refused dials keep every slot down until the last link closed.
        cluster.down.lock().unwrap().extend((0..3).map(endpoint));
        for id in 0..3 {
            cluster.drop_connections(&endpoint(id));
        }
        eventually("every link is down", || links.ready_replicas().is_empty()).await;
        assert_eq!(links.state(), ClientState::Authenticated);
        cluster.down.lock().unwrap().clear();
        eventually("every slot is ready again", || {
            links.ready_replicas().len() == 3
        })
        .await;

        let published: Vec<_> = std::iter::from_fn(|| events.try_recv().ok()).collect();
        assert_eq!(
            published,
            [
                DiagnosticEvent::Disconnected,
                DiagnosticEvent::Connected,
                DiagnosticEvent::SignedIn
            ]
        );
        assert_eq!(links.shared.session.fence(), fence);
        assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_session_reset_when_links_are_ready_should_close_all_but_the_sign_in_link() {
        let cluster = FakeCluster::new(3);
        let links = connected(&cluster, 3).await;
        let sign_in_link = links.link(Target::Replica(1)).unwrap();
        let old_links = [0, 2].map(|id| links.link(Target::Replica(id)).unwrap());

        links.shared.session.reset().unwrap();
        links.shared.sign_in(&sign_in_link).await.unwrap();
        eventually("every slot serves the new session", || {
            links.ready_replicas().len() == 3
        })
        .await;

        assert!(Arc::ptr_eq(
            &links.link(Target::Replica(1)).unwrap(),
            &sign_in_link
        ));
        for (id, old_link) in [0, 2].into_iter().zip(old_links) {
            let link = links.link(Target::Replica(id)).unwrap();
            assert!(!Arc::ptr_eq(&link, &old_link), "replica {id}");
        }
        assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn given_disconnect_when_slot_tasks_run_should_abort_them_and_fail_pending_requests() {
        let cluster = FakeCluster::new(3);
        cluster.down.lock().unwrap().insert(endpoint(2));
        let links = links(&cluster, &endpoint(0), policy());
        links.connect().await.unwrap();
        eventually("replica 1 is ready", || links.ready_replicas() == [0, 1]).await;
        let link = links.link(Target::Replica(1)).unwrap();
        let pending = exchange(
            &links.shared.session,
            &link,
            UNANSWERED_CODE,
            Bytes::new(),
            Instant::now() + TEST_BUDGET,
        );

        let (result, ()) = tokio::join!(pending, async {
            eventually("the request reaches replica 1", || {
                cluster.codes(&endpoint(1)).contains(&UNANSWERED_CODE)
            })
            .await;
            links.disconnect();
        });
        let dials = cluster.dials.lock().unwrap().len();
        sleep(RECONNECT_BACKOFF_MAX * 2).await;

        assert_eq!(result.unwrap_err(), IggyError::Disconnected);
        assert_eq!(cluster.dials.lock().unwrap().len(), dials);
        assert_eq!(links.state(), ClientState::Disconnected);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_completed_disconnect_when_automatic_sign_in_finishes_should_keep_credentials_forgotten()
     {
        let cluster = FakeCluster::new(1);
        let session = Arc::new(SessionOwner::new(iggy_common::AutoLogin::Disabled));
        let credentials = Credentials::UsernamePassword("iggy".to_owned(), "iggy".into());
        session.remember(credentials.clone(), 1);
        let links = ReplicaLinks::new(
            cluster.dialer(),
            endpoint(0),
            policy(),
            Arc::clone(&session),
        );
        links.connect().await.unwrap();
        let link = links.link(Target::MetadataPrimary).unwrap();
        let fence = session.fence();
        let mut events = links.subscribe();
        while events.try_recv().is_ok() {}
        links.disconnect();
        session.forget();
        session.reset().unwrap();

        assert!(links.shared.finish_sign_in(&link, fence, 1).is_err());

        assert!(session.sign_in_credentials().is_none());
        assert_eq!(events.try_recv().unwrap(), DiagnosticEvent::Disconnected);
        assert!(events.try_recv().is_err());
        links.connect().await.unwrap();
        assert_eq!(links.state(), ClientState::Connected);
        assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_interrupted_dial_when_connecting_after_disconnect_should_start_a_new_dial() {
        let cluster = FakeCluster::new(1);
        cluster.silent.lock().unwrap().insert(endpoint(0));
        let links = links(&cluster, &endpoint(0), policy());
        let mut first = Box::pin(links.connect());
        assert!(futures_util::poll!(&mut first).is_pending());
        links.disconnect();
        cluster.silent.lock().unwrap().clear();

        let (interrupted, reconnected) = tokio::join!(first, links.connect());

        assert!(interrupted.is_err());
        assert_eq!(reconnected, Ok(()));
        assert_eq!(links.state(), ClientState::Authenticated);
    }

    #[tokio::test(start_paused = true)]
    async fn given_disconnect_when_late_sign_in_steps_run_should_keep_the_client_disconnected() {
        let cluster = FakeCluster::new(2);
        let links = connected(&cluster, 2).await;
        let mut events = links.subscribe();
        while events.try_recv().is_ok() {}
        links.disconnect();
        links.set_state(ClientState::Authenticated);
        links.publish(DiagnosticEvent::SignedIn);
        assert_eq!(links.state(), ClientState::Disconnected);
        assert_eq!(events.try_recv().unwrap(), DiagnosticEvent::Disconnected);
        assert!(events.try_recv().is_err());
        links.connect().await.unwrap();
        assert_eq!(links.state(), ClientState::Authenticated);
        links.shutdown().await;
        links.set_state(ClientState::Connected);
        assert_eq!(links.state(), ClientState::Shutdown);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_reconnect_sweep_when_shutdown_should_stop_before_the_next_dial() {
        let cluster = FakeCluster::new(1);
        cluster.down.lock().unwrap().insert(endpoint(0));
        let links = links(&cluster, &endpoint(0), policy());

        let (result, ()) = tokio::join!(links.connect(), async {
            eventually("the first dial fails", || {
                cluster.dials_to(&endpoint(0)).len() == 1
            })
            .await;
            links.shutdown().await;
            cluster.down.lock().unwrap().clear();
        });

        assert_eq!(result.unwrap_err(), IggyError::ClientShutdown);
        assert_eq!(links.state(), ClientState::Shutdown);
        assert_eq!(cluster.dials_to(&endpoint(0)).len(), 1);
        assert!(links.ready_replicas().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn given_shutdown_during_the_last_dial_when_it_fails_should_skip_retry_backoff() {
        let cluster = FakeCluster::new(1);
        cluster.silent.lock().unwrap().insert(endpoint(0));
        let links = links(
            &cluster,
            &endpoint(0),
            ReconnectPolicy {
                interval: RECONNECT_BACKOFF_MAX,
                ..policy()
            },
        );
        let started = Instant::now();

        let (result, ()) = tokio::join!(links.connect(), async {
            eventually("the dial starts", || {
                !cluster.dials_to(&endpoint(0)).is_empty()
            })
            .await;
            links.shutdown().await;
        });

        assert_eq!(result.unwrap_err(), IggyError::ClientShutdown);
        assert_eq!(started.elapsed(), DIAL_TIMEOUT);
    }

    #[tokio::test(start_paused = true)]
    async fn given_connection_info_when_slots_change_should_name_the_metadata_primary() {
        let cluster = FakeCluster::new(2);
        let links = links(&cluster, BALANCER, policy());
        assert_eq!(links.connection_address(), BALANCER);

        links.connect().await.unwrap();
        eventually("both slots are ready", || links.ready_replicas().len() == 2).await;
        assert_eq!(links.connection_address(), endpoint(0));

        {
            let mut nodes = cluster.nodes.lock().unwrap();
            nodes[0].role = ClusterNodeRole::Follower as u8;
            nodes[1].role = ClusterNodeRole::Leader as u8;
        }
        let link = links.link(Target::Replica(1)).unwrap();
        links.refresh_roster(&link).await.unwrap();

        assert_eq!(links.connection_address(), endpoint(1));
        assert_eq!(links.metadata_primary(), Some(1));
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_lost_session_when_the_slots_bind_again_should_register_one_new_session() {
        let cluster = FakeCluster::new(3);
        let links = connected(&cluster, 3).await;
        let fence = links.shared.session.fence();
        // Each slot waits its initial jitter, then its bind finds the session gone.
        sleep(REESTABLISH_AFTER).await;
        let mut events = links.subscribe();
        while events.try_recv().is_ok() {}

        *cluster.session.lock().unwrap() = None;
        for id in 0..3 {
            cluster.drop_connections(&endpoint(id));
        }
        eventually("every slot serves a new session", || {
            links.shared.session.fence() > fence && links.ready_replicas().len() == 3
        })
        .await;

        assert_eq!(links.shared.session.fence(), fence + 1);
        assert_eq!(cluster.count(LOGIN_REGISTER_CODE), 2);
        assert_eq!(links.state(), ClientState::Authenticated);
        let published: Vec<_> = std::iter::from_fn(|| events.try_recv().ok()).collect();
        assert_eq!(
            published,
            [
                DiagnosticEvent::Disconnected,
                DiagnosticEvent::Connected,
                DiagnosticEvent::SignedOut,
                DiagnosticEvent::SignedIn
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_no_endpoint_when_connecting_should_make_one_pass_more_than_max_retries() {
        const MAX_RETRIES: u32 = 2;
        let cluster = FakeCluster::new(1);
        cluster.down.lock().unwrap().insert(endpoint(0));
        let limited = ReconnectPolicy {
            max_retries: Some(MAX_RETRIES),
            ..policy()
        };
        let links = links(&cluster, &endpoint(0), limited);

        let connected = links.connect().await;

        assert_eq!(connected.unwrap_err(), IggyError::CannotEstablishConnection);
        assert_eq!(
            cluster.dials_to(&endpoint(0)).len(),
            1 + MAX_RETRIES as usize
        );
        assert_eq!(links.state(), ClientState::Disconnected);
    }

    #[tokio::test(start_paused = true)]
    async fn given_reconnection_disabled_when_no_endpoint_answers_should_make_one_pass() {
        let cluster = FakeCluster::new(1);
        cluster.down.lock().unwrap().insert(endpoint(0));
        let disabled = ReconnectPolicy {
            enabled: false,
            ..policy()
        };
        let links = links(&cluster, &endpoint(0), disabled);
        links.shared.table().roster = vec![endpoint(0), BALANCER.to_owned()];
        cluster.down.lock().unwrap().insert(BALANCER.to_owned());

        let connected = links.connect().await;

        assert_eq!(connected.unwrap_err(), IggyError::CannotEstablishConnection);
        assert_eq!(cluster.dials_to(&endpoint(0)).len(), 1);
        assert_eq!(cluster.dials_to(BALANCER).len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_sign_in_that_loses_its_link_when_sweeping_should_sign_in_on_the_next_endpoint()
    {
        let cluster = FakeCluster::new(2);
        cluster
            .hang_up_on_register
            .lock()
            .unwrap()
            .insert(endpoint(0));
        let links = links(&cluster, &endpoint(0), policy());
        links.shared.table().roster = vec![endpoint(0), endpoint(1)];

        links.connect().await.unwrap();

        assert_eq!(links.state(), ClientState::Authenticated);
        assert_eq!(cluster.codes(&endpoint(1))[0], LOGIN_REGISTER_CODE);
    }

    #[tokio::test(start_paused = true)]
    async fn given_rejected_credentials_when_sweeping_should_stop_and_keep_the_link() {
        let cluster = FakeCluster::new(2);
        *cluster.reject_credentials.lock().unwrap() = true;
        let links = links(&cluster, &endpoint(0), policy());
        links.shared.table().roster = vec![endpoint(0), endpoint(1)];

        let connected = links.connect().await;

        assert_eq!(connected.unwrap_err(), IggyError::InvalidCredentials);
        assert_eq!(links.state(), ClientState::Connected);
        assert!(links.link(Target::MetadataPrimary).is_ok());
        assert!(cluster.dials_to(&endpoint(1)).is_empty());
    }
}
