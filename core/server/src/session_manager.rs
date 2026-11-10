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

//! Transport-to-consensus session bridge for server.
//!
//! Maps ephemeral transport connections to durable consensus sessions.
//! Connections bind to a registered session after credential verification.
//!
//! The [`SessionManager`] is the server-side counterpart of the SDK's
//! session lifecycle. It does **not** own the `ClientTable`. That lives
//! in the consensus layer. This module tracks the binding between a
//! transport connection and the consensus-level `(client_id, session)` pair.

use crate::cluster_meta::ClusterRoster;
use ahash::AHashMap;
use consensus::client_table::SessionAttachment;
use futures::future::{AbortHandle, AbortRegistration};
use iggy_binary_protocol::ConsumerSession;
use iggy_common::IggyError;
use message_bus::installer::conn_info::ClientTransportKind;
use shard::config::DeferredPollConfig;
use shard::{CommitGate, ConnectedClientInfo};
use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::{Duration, Instant};

/// What the request funnel resolves from one `connections` lookup per frame.
///
/// The bound consensus session and its attachment, the acting user, the
/// transport peer address and the read-your-writes floor. `Default`
/// (everything absent, no address, floor `0`) stands for a connection neither
/// this map nor the bus knows.
#[derive(Debug, Clone, Default)]
pub struct ConnectionContext {
    /// `(client_id, session)` once register committed, `None` before.
    pub bound: Option<(u128, u64)>,
    /// The attachment of the bound session, `None` before register committed.
    /// The session can end while a read waits, so a read checks it again
    /// after every await.
    pub attachment: Option<SessionAttachment>,
    /// Acting user from `login`, `None` while still `Connected`.
    pub user_id: Option<u32>,
    /// Peer address recorded by [`SessionManager::ensure_connection`]; the
    /// non-replicated reads pick the advertised address from it, and
    /// `None` degrades to the catch-all address.
    pub address: Option<SocketAddr>,
    /// Read-your-writes floor: the metadata commit this connection's own
    /// writes have reached, which its reads must not be served below.
    ///
    /// A connection this map does not know reads as `0` -- it was promised
    /// nothing, so its reads wait for nothing.
    pub metadata_watermark: u64,
}

/// A binding is local to a connection; several connections can share a session.
#[derive(Debug, Clone)]
pub enum ConnectionState {
    /// Connection established, not yet authenticated.
    Connected,
    /// Register committed through consensus. Connection is bound to a
    /// `(client_id, session)` pair. Requests on this connection use
    /// these values to populate `RoutedRequestHeader.client` and
    /// `RoutedRequestHeader.session`.
    Bound {
        user_id: u32,
        client_id: u128,
        session: u64,
    },
}

/// SDK identity reported in the login-register version prefix.
#[derive(Debug, Clone)]
pub struct ClientSdkInfo {
    pub sdk_name: String,
    pub sdk_version: String,
    /// Packed protocol version, see `iggy_binary_protocol::ProtocolVersion`.
    pub protocol_version: u32,
}

/// Per-connection metadata tracked by the session manager.
#[derive(Debug, Clone)]
pub struct Connection {
    pub address: SocketAddr,
    pub transport: ClientTransportKind,
    pub state: ConnectionState,
    /// Last time the client proved liveness (a `ping`). The heartbeat
    /// verifier evicts connections stale past the configured threshold.
    pub last_heartbeat: Instant,
    /// Recorded at login; `None` until the connection authenticates.
    pub sdk: Option<ClientSdkInfo>,
    /// Highest metadata op this connection has been told committed.
    ///
    /// Seeded from the bound session (the register's own commit op, which
    /// floors everything the client committed before it re-homed) and raised by
    /// every committed reply relayed on this connection. The read gate holds a
    /// local read until the node's applied frontier covers it, so a client
    /// cannot be served state older than a write it already saw acked.
    ///
    /// Per-connection rather than per-client: the number only has to cover what
    /// THIS socket was told, and a client that reconnects re-seeds from the
    /// session it binds.
    pub metadata_watermark: u64,
    consumer_session: Option<(u128, SessionAttachment)>,
    deferred_requests: AHashMap<u64, DeferredRequestEntry>,
}

/// Bridges transport connections to consensus sessions.
///
/// NOT thread-safe: each shard owns one `SessionManager` on its
/// single-threaded compio runtime, the same way the rest of server
/// is structured. All mutators take `&mut self`; the type carries no
/// internal locking.
///
/// ## Invariants
///
/// - A `connection_id` appears in at most one of `connections`.
/// - Every bound connection holds an attachment invalidated by session end.
/// - Disconnecting one connection leaves the shared session and its peers live.
pub struct SessionManager {
    connections: AHashMap<u128, Connection>,
    next_deferred_request: u64,
    /// The fetch reads that this shard runs per session and owner shard.
    fetch_reads: AHashMap<(u128, u16), FetchReads>,
    /// This shard's copy of the configured cluster roster, served by the
    /// `GetClusterMetadata` read. Lives here because it is the
    /// per-shard context already threaded to the non-replicated read path;
    /// installed once at bootstrap, disabled until then.
    cluster_roster: Rc<ClusterRoster>,
}

impl SessionManager {
    #[must_use]
    pub fn new() -> Self {
        Self {
            connections: AHashMap::new(),
            next_deferred_request: 0,
            fetch_reads: AHashMap::new(),
            cluster_roster: Rc::new(ClusterRoster::disabled()),
        }
    }

    /// Install this shard's configured cluster roster (once, at bootstrap).
    pub fn set_cluster_roster(&mut self, roster: Rc<ClusterRoster>) {
        self.cluster_roster = roster;
    }

    /// The configured cluster roster for the `GetClusterMetadata` read.
    #[must_use]
    pub fn cluster_roster(&self) -> Rc<ClusterRoster> {
        Rc::clone(&self.cluster_roster)
    }

    pub fn ensure_connection(
        &mut self,
        connection_id: u128,
        address: SocketAddr,
        transport: ClientTransportKind,
        now: Instant,
    ) {
        self.connections
            .entry(connection_id)
            .or_insert_with(|| Connection {
                address,
                transport,
                state: ConnectionState::Connected,
                last_heartbeat: now,
                sdk: None,
                metadata_watermark: 0,
                consumer_session: None,
                deferred_requests: AHashMap::new(),
            });
    }

    /// The request funnel's per-frame view of a connection: stamp the
    /// liveness clock and read back everything the dispatch arms resolve
    /// from it, in ONE map lookup.
    ///
    /// `None` means the connection is not registered yet, which happens
    /// only on a transport's first frame; the caller installs it from the
    /// bus metadata ([`Self::ensure_connection`]) and asks again.
    pub(crate) fn touch_connection(
        &mut self,
        connection_id: u128,
        include_attachment: bool,
        now: Instant,
    ) -> Option<ConnectionContext> {
        let conn = self.connections.get_mut(&connection_id)?;
        if conn
            .consumer_session
            .as_ref()
            .is_some_and(|(_, attachment)| !attachment.is_valid())
        {
            conn.state = ConnectionState::Connected;
            conn.consumer_session = None;
            conn.metadata_watermark = 0;
        }
        conn.last_heartbeat = now;
        let (bound, user_id) = match conn.state {
            ConnectionState::Bound {
                user_id,
                client_id,
                session,
            } => (Some((client_id, session)), Some(user_id)),
            ConnectionState::Connected => (None, None),
        };
        Some(ConnectionContext {
            bound,
            attachment: conn
                .consumer_session
                .as_ref()
                .filter(|_| include_attachment)
                .map(|(_, attachment)| attachment.clone()),
            user_id,
            address: Some(conn.address),
            metadata_watermark: conn.metadata_watermark,
        })
    }

    /// Connection ids whose last heartbeat is older than `max_age` -- the
    /// stale set the heartbeat verifier evicts. Only `Bound`
    /// connections are considered (a freshly-`Connected` socket mid-handshake
    /// is left alone until it authenticates).
    #[must_use]
    pub fn collect_stale(&self, max_age: Duration, now: Instant) -> Vec<u128> {
        self.connections
            .iter()
            .filter(|(_, conn)| !matches!(conn.state, ConnectionState::Connected))
            .filter(|(_, conn)| {
                conn.deferred_requests
                    .values()
                    .all(|entry| now >= entry.deadline)
            })
            .filter(|(_, conn)| now.duration_since(conn.last_heartbeat) > max_age)
            .map(|(&id, _)| id)
            .collect()
    }

    /// The consensus client id a connection is bound to, if any. The heartbeat
    /// verifier reads it to look up consumer-group membership before deciding
    /// whether an eviction would actually release anything.
    #[must_use]
    pub fn bound_client_id(&self, connection_id: u128) -> Option<u128> {
        match self.connections.get(&connection_id)?.state {
            ConnectionState::Bound { client_id, .. } => Some(client_id),
            ConnectionState::Connected => None,
        }
    }

    /// Record (or refresh on re-login) the SDK identity for a connection.
    /// No state-machine constraint: the gate already validated the version
    /// and a missing connection just drops the record.
    pub fn record_sdk_info(&mut self, connection_id: u128, info: ClientSdkInfo) {
        if let Some(conn) = self.connections.get_mut(&connection_id) {
            conn.sdk = Some(info);
        }
    }

    /// Remove a transport connection without ending its durable session.
    pub fn remove_connection(&mut self, connection_id: u128) -> Option<(u128, u64)> {
        if let Some(conn) = self.connections.remove(&connection_id) {
            for entry in conn.deferred_requests.into_values() {
                if let Some(gate) = &entry.commit_gate {
                    let _ = gate.cancel();
                }
                entry.abort.abort();
            }
            if let ConnectionState::Bound {
                client_id, session, ..
            } = conn.state
            {
                return Some((client_id, session));
            }
        }
        None
    }

    /// # Errors
    /// Rejects a missing connection or an exhausted request identity or watchdog.
    pub fn begin_deferred_request(
        sessions: &Rc<RefCell<Self>>,
        connection_id: u128,
        (wire_request_id, commit_gate): (u64, Option<CommitGate>),
        watchdog: Duration,
        now: Instant,
    ) -> Result<(DeferredRequestGuard, AbortRegistration), IggyError> {
        let deadline = now.checked_add(watchdog).ok_or(IggyError::InvalidCommand)?;
        let (abort, registration) = AbortHandle::new_pair();
        let mut manager = sessions.borrow_mut();
        let request_id = manager
            .next_deferred_request
            .checked_add(1)
            .ok_or(IggyError::TransientNotAccepted)?;
        manager.next_deferred_request = request_id;
        let connection = manager
            .connections
            .get_mut(&connection_id)
            .ok_or(IggyError::StaleClient)?;
        connection.deferred_requests.insert(
            request_id,
            DeferredRequestEntry {
                wire_request_id,
                deadline,
                abort,
                commit_gate,
            },
        );
        Ok((
            DeferredRequestGuard {
                sessions: Rc::clone(sessions),
                connection_id,
                request_id,
            },
            registration,
        ))
    }

    /// Cancels only requests admitted on this connection with the given wire id.
    /// A committing request stops only while none of its reads committed, and
    /// otherwise answers as usual.
    pub fn cancel_deferred_request(
        &self,
        connection_id: u128,
        client: u128,
        session: u64,
        wire_request_id: u64,
    ) -> bool {
        let Some(connection) = self.connections.get(&connection_id) else {
            return false;
        };
        if !matches!(connection.state, ConnectionState::Bound { client_id, session: bound_session, .. } if client_id == client && bound_session == session)
            || connection
                .consumer_session
                .as_ref()
                .is_none_or(|(_, attachment)| !attachment.is_valid())
        {
            return false;
        }
        for entry in connection.deferred_requests.values() {
            if entry.wire_request_id == wire_request_id
                && entry.commit_gate.as_ref().is_none_or(CommitGate::cancel)
            {
                entry.abort.abort();
            }
        }
        true
    }

    /// Reserves one fetch read of the session `client_id` on the owner shard
    /// `owner`, which can add `cost` bytes to a reply that this shard holds
    /// until it is written. Refused when the reads that this shard holds there
    /// for the session would pass the per-session quota or the byte budget of
    /// `limits`. The owner keeps its own disk read budget and also counts the
    /// parked waits and the reads that other shards send for the session, so
    /// it can still refuse a reserved read.
    #[must_use]
    pub fn reserve_fetch_read(
        sessions: &Rc<RefCell<Self>>,
        (client_id, owner): (u128, u16),
        cost: usize,
        limits: &DeferredPollConfig,
    ) -> Option<FetchReadGuard> {
        let key = (client_id, owner);
        let mut manager = sessions.borrow_mut();
        let running = manager.fetch_reads.get(&key).copied().unwrap_or_default();
        let bytes = running.bytes.checked_add(cost)?;
        if running.reads >= limits.max_pending_per_session || bytes > limits.max_inflight_bytes {
            return None;
        }
        manager.fetch_reads.insert(
            key,
            FetchReads {
                reads: running.reads + 1,
                bytes,
            },
        );
        Some(FetchReadGuard {
            sessions: Rc::clone(sessions),
            key,
            cost,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn bind_authenticated_connection(
        &mut self,
        connection_id: u128,
        client_id: u128,
        session: u64,
        user_id: u32,
        attachment: SessionAttachment,
        metadata_watermark: u64,
        now: Instant,
    ) -> Result<(), IggyError> {
        if !attachment.is_valid() {
            return Err(IggyError::Unauthenticated);
        }
        let connection = self
            .connections
            .get_mut(&connection_id)
            .ok_or(IggyError::Unauthenticated)?;
        if let ConnectionState::Bound {
            user_id: bound_user,
            client_id: bound_client,
            session: bound_session,
        } = connection.state
            && (bound_user != user_id || bound_client != client_id || bound_session != session)
        {
            return Err(IggyError::AlreadyAuthenticated);
        }
        connection.state = ConnectionState::Bound {
            user_id,
            client_id,
            session,
        };
        connection.consumer_session = Some((client_id, attachment));
        connection.metadata_watermark = connection
            .metadata_watermark
            .max(metadata_watermark)
            .max(session);
        connection.last_heartbeat = now;
        Ok(())
    }

    /// Raise this connection's metadata watermark to `commit`. Monotone, so a
    /// late or out-of-order reply cannot lower it; no-op for an unknown
    /// connection.
    ///
    /// Only committed replies belong here. A pre-consensus rejection stamps the
    /// primary's `commit_max`, which is an op this connection was never
    /// promised and, on a backup-homed connection, one it would then wait for.
    pub fn record_metadata_watermark(&mut self, connection_id: u128, commit: u64) {
        if let Some(conn) = self.connections.get_mut(&connection_id) {
            conn.metadata_watermark = conn.metadata_watermark.max(commit);
        }
    }

    /// Resolve the attached group identity without extending its lifetime.
    ///
    /// # Errors
    /// Returns `Unauthenticated` without an authenticated attachment, or
    /// `StaleClient` when the parent epoch has ended.
    pub fn consumer_session(
        &self,
        connection_id: u128,
    ) -> Result<(u128, SessionAttachment), IggyError> {
        self.attached_consumer_session(connection_id)?
            .ok_or(IggyError::Unauthenticated)
    }

    /// # Errors
    /// Returns `Unauthenticated` for an unbound connection and `StaleClient`
    /// when its attached parent session has ended.
    pub fn attached_consumer_session(
        &self,
        connection_id: u128,
    ) -> Result<Option<(u128, SessionAttachment)>, IggyError> {
        let connection = self
            .connections
            .get(&connection_id)
            .ok_or(IggyError::Unauthenticated)?;
        if !matches!(connection.state, ConnectionState::Bound { .. }) {
            return Err(IggyError::Unauthenticated);
        }
        let (client_id, attachment) = connection
            .consumer_session
            .as_ref()
            .ok_or(IggyError::Unauthenticated)?;
        if !attachment.is_valid() {
            return Err(IggyError::StaleClient);
        }
        Ok(Some((*client_id, attachment.clone())))
    }

    /// Look up the consensus session for a connection.
    ///
    /// Returns `(client_id, session)` if the connection is `Bound`, `None`
    /// otherwise. The request funnel reads it through the crate-private
    /// `touch_connection` instead, which resolves it in the same lookup as
    /// the heartbeat; this is for the session-op paths that only need the
    /// binding.
    #[must_use]
    pub fn get_session(&self, connection_id: u128) -> Option<(u128, u64)> {
        let conn = self.connections.get(&connection_id)?;
        if conn
            .consumer_session
            .as_ref()
            .is_some_and(|(_, attachment)| !attachment.is_valid())
        {
            return None;
        }
        match conn.state {
            ConnectionState::Bound {
                client_id, session, ..
            } => Some((client_id, session)),
            ConnectionState::Connected => None,
        }
    }

    /// Look up the authenticated user id for a connection.
    #[must_use]
    pub fn get_user_id(&self, connection_id: u128) -> Option<u32> {
        let conn = self.connections.get(&connection_id)?;
        if conn
            .consumer_session
            .as_ref()
            .is_some_and(|(_, attachment)| !attachment.is_valid())
        {
            return None;
        }
        match conn.state {
            ConnectionState::Bound { user_id, .. } => Some(user_id),
            ConnectionState::Connected => None,
        }
    }

    /// Flatten one connection into a [`ConnectedClientInfo`] for `get_me`.
    ///
    /// This is the single per-shard source for the client-info reads:
    /// `user_id`, `transport`, and `address` all come from the local
    /// `SessionManager`, so the caller no longer consults the message
    /// bus's `client_meta`.
    #[must_use]
    pub fn client_record(&self, connection_id: u128) -> Option<ConnectedClientInfo> {
        let conn = self.connections.get(&connection_id)?;
        Some(record_from(connection_id, conn))
    }

    /// Iterate every locally-homed connected client as a
    /// [`ConnectedClientInfo`]. The per-shard half of the `get_clients`
    /// scatter-gather.
    pub fn iter_clients(&self) -> impl Iterator<Item = ConnectedClientInfo> + '_ {
        self.connections
            .iter()
            .map(|(&id, conn)| record_from(id, conn))
    }

    /// The number of locally-homed connected clients, which
    /// [`Self::iter_clients`] would yield, without building their records.
    #[must_use]
    pub fn client_count(&self) -> usize {
        self.connections.len()
    }

    pub fn iter_consumer_sessions(
        &self,
        timeout: std::time::Duration,
        now: Instant,
    ) -> impl Iterator<Item = ConsumerSession> + '_ {
        self.connections.values().filter_map(move |connection| {
            if now.saturating_duration_since(connection.last_heartbeat) >= timeout
                || connection
                    .consumer_session
                    .as_ref()
                    .is_none_or(|(_, attachment)| !attachment.is_valid())
            {
                return None;
            }
            if let ConnectionState::Bound {
                client_id, session, ..
            } = connection.state
            {
                Some(ConsumerSession { client_id, session })
            } else {
                None
            }
        })
    }
}

pub struct DeferredRequestGuard {
    sessions: Rc<RefCell<SessionManager>>,
    connection_id: u128,
    request_id: u64,
}

impl DeferredRequestGuard {
    pub fn written(&self, now: Instant) {
        if let Some(connection) = self
            .sessions
            .borrow_mut()
            .connections
            .get_mut(&self.connection_id)
            && connection
                .deferred_requests
                .get(&self.request_id)
                .is_some_and(|entry| now < entry.deadline)
        {
            connection.last_heartbeat = now;
        }
    }
}

impl Drop for DeferredRequestGuard {
    fn drop(&mut self) {
        if let Some(connection) = self
            .sessions
            .borrow_mut()
            .connections
            .get_mut(&self.connection_id)
        {
            connection.deferred_requests.remove(&self.request_id);
        }
    }
}

/// One deferred request admitted on a connection.
#[derive(Debug, Clone)]
struct DeferredRequestEntry {
    wire_request_id: u64,
    deadline: Instant,
    abort: AbortHandle,
    /// Set for a request that commits as it reads.
    commit_gate: Option<CommitGate>,
}

#[derive(Debug, Default, Clone, Copy)]
struct FetchReads {
    reads: usize,
    bytes: usize,
}

/// One read that [`SessionManager::reserve_fetch_read`] reserved, released on
/// drop.
pub struct FetchReadGuard {
    sessions: Rc<RefCell<SessionManager>>,
    key: (u128, u16),
    cost: usize,
}

impl FetchReadGuard {
    /// Charges the bytes that the read returned instead of what it could
    /// return, for as long as its reply is held.
    pub fn settle(&mut self, bytes: usize) {
        let mut manager = self.sessions.borrow_mut();
        let Some(running) = manager.fetch_reads.get_mut(&self.key) else {
            return;
        };
        running.bytes = running
            .bytes
            .saturating_sub(self.cost)
            .saturating_add(bytes);
        self.cost = bytes;
    }
}

impl Drop for FetchReadGuard {
    fn drop(&mut self) {
        let mut manager = self.sessions.borrow_mut();
        let Some(running) = manager.fetch_reads.get_mut(&self.key) else {
            return;
        };
        running.reads -= 1;
        running.bytes = running.bytes.saturating_sub(self.cost);
        if running.reads == 0 {
            manager.fetch_reads.remove(&self.key);
        }
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Flatten a connection + its id into a [`ConnectedClientInfo`].
fn record_from(connection_id: u128, conn: &Connection) -> ConnectedClientInfo {
    let (user_id, vsr_client_id) = match conn.state {
        ConnectionState::Bound {
            user_id, client_id, ..
        } => (Some(user_id), Some(client_id)),
        ConnectionState::Connected => (None, None),
    };
    ConnectedClientInfo {
        client_id: connection_id,
        vsr_client_id,
        user_id,
        transport: conn.transport,
        address: conn.address,
        sdk_name: conn.sdk.as_ref().map(|sdk| sdk.sdk_name.clone()),
        sdk_version: conn.sdk.as_ref().map(|sdk| sdk.sdk_version.clone()),
        protocol_version: conn.sdk.as_ref().map(|sdk| sdk.protocol_version),
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;

    use super::*;
    use crate::reply_frame::build_empty_reply;
    use consensus::ClientTable;
    use iggy_binary_protocol::{Command, Operation, RoutedRequestHeader};
    use std::net::{IpAddr, Ipv4Addr};

    const CLIENT: u128 = 7;
    const USER: u32 = 1;
    const EPOCH: u64 = 11;
    const TIMEOUT: Duration = Duration::from_secs(30);

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    fn registry(client: u128, user: u32, epoch: u64) -> ClientTable {
        let mut table = ClientTable::new(2);
        let header = RoutedRequestHeader {
            command: Command::Request,
            operation: Operation::Register,
            client,
            size: u32::try_from(size_of::<RoutedRequestHeader>()).unwrap(),
            ..Default::default()
        };
        table
            .commit_register(
                client,
                user,
                [0x5a; 32],
                build_empty_reply(&header, client, epoch, epoch),
            )
            .unwrap();
        table
    }

    fn bind(sessions: &mut SessionManager, table: &mut ClientTable, connection: u128) {
        sessions.ensure_connection(
            connection,
            addr(5000),
            ClientTransportKind::Tcp,
            std::time::Instant::now(),
        );
        let attachment = table.attach_session(CLIENT, EPOCH, USER).unwrap();
        sessions
            .bind_authenticated_connection(
                connection,
                CLIENT,
                EPOCH,
                USER,
                attachment,
                EPOCH,
                std::time::Instant::now(),
            )
            .unwrap();
    }

    #[test]
    fn shared_bindings_survive_disconnect_and_end_together_on_logout() {
        let mut table = registry(CLIENT, USER, EPOCH);
        let mut sessions = SessionManager::new();
        for connection in [1, 2, 3] {
            bind(&mut sessions, &mut table, connection);
        }
        assert_eq!(sessions.remove_connection(1), Some((CLIENT, EPOCH)));
        assert_eq!(table.get_epoch(CLIENT), Some(EPOCH));
        assert_eq!(sessions.get_session(2), Some((CLIENT, EPOCH)));
        assert!(table.end_session(CLIENT, USER, EPOCH, EPOCH + 1));
        for connection in [2, 3] {
            assert_eq!(sessions.get_session(connection), None);
            assert_eq!(sessions.get_user_id(connection), None);
            assert!(matches!(
                sessions.consumer_session(connection),
                Err(IggyError::StaleClient)
            ));
        }
        assert_eq!(
            sessions
                .iter_consumer_sessions(TIMEOUT, std::time::Instant::now())
                .count(),
            0
        );
    }

    #[test]
    fn record_sdk_info_exposed_via_iter_clients() {
        let mut mgr = SessionManager::new();
        mgr.ensure_connection(
            1,
            addr(5000),
            ClientTransportKind::Tcp,
            std::time::Instant::now(),
        );

        // Pre-login: no SDK identity.
        let info = mgr.iter_clients().next().unwrap();
        assert!(info.sdk_name.is_none());

        mgr.record_sdk_info(
            1,
            ClientSdkInfo {
                sdk_name: "rust-sdk".to_string(),
                sdk_version: "1.0.0".to_string(),
                protocol_version: 42,
            },
        );
        let info = mgr.iter_clients().next().unwrap();
        assert_eq!(info.sdk_name.as_deref(), Some("rust-sdk"));
        assert_eq!(info.sdk_version.as_deref(), Some("1.0.0"));
        assert_eq!(info.protocol_version, Some(42));

        // Re-login overwrites (client may reconnect after an upgrade).
        mgr.record_sdk_info(
            1,
            ClientSdkInfo {
                sdk_name: "rust-sdk".to_string(),
                sdk_version: "2.0.0".to_string(),
                protocol_version: 43,
            },
        );
        let info = mgr.iter_clients().next().unwrap();
        assert_eq!(info.sdk_version.as_deref(), Some("2.0.0"));

        // Unknown connection: record is dropped, no panic.
        mgr.record_sdk_info(
            999,
            ClientSdkInfo {
                sdk_name: "go-sdk".to_string(),
                sdk_version: "0.1.0".to_string(),
                protocol_version: 1,
            },
        );
        assert_eq!(mgr.iter_clients().count(), 1);
    }

    #[test]
    fn activity_reports_include_only_recent_valid_bindings() {
        let mut table = registry(CLIENT, USER, EPOCH);
        let mut sessions = SessionManager::new();
        sessions.ensure_connection(
            9,
            addr(5000),
            ClientTransportKind::Tcp,
            std::time::Instant::now(),
        );
        assert_eq!(
            sessions
                .iter_consumer_sessions(TIMEOUT, std::time::Instant::now())
                .count(),
            0
        );
        for connection in [1, 2] {
            bind(&mut sessions, &mut table, connection);
        }
        sessions.connections.get_mut(&1).unwrap().last_heartbeat =
            Instant::now().checked_sub(TIMEOUT).unwrap();
        assert_eq!(
            sessions
                .iter_consumer_sessions(TIMEOUT, std::time::Instant::now())
                .collect::<Vec<_>>(),
            [ConsumerSession {
                client_id: CLIENT,
                session: EPOCH
            }]
        );
        sessions
            .touch_connection(1, false, std::time::Instant::now())
            .unwrap();
        assert_eq!(
            sessions
                .iter_consumer_sessions(TIMEOUT, std::time::Instant::now())
                .count(),
            2
        );
        sessions.remove_connection(2);
        assert_eq!(
            sessions
                .iter_consumer_sessions(TIMEOUT, std::time::Instant::now())
                .count(),
            1
        );
        table.end_session(CLIENT, USER, EPOCH, EPOCH + 1);
        assert_eq!(
            sessions
                .iter_consumer_sessions(TIMEOUT, std::time::Instant::now())
                .count(),
            0
        );
    }

    #[test]
    fn repeated_binding_is_idempotent_and_cannot_replace_a_live_binding() {
        let mut table = registry(CLIENT, USER, EPOCH);
        let mut sessions = SessionManager::new();
        bind(&mut sessions, &mut table, 1);
        let attachment = table.attach_session(CLIENT, EPOCH, USER).unwrap();
        sessions
            .bind_authenticated_connection(
                1,
                CLIENT,
                EPOCH,
                USER,
                attachment.clone(),
                EPOCH + 5,
                std::time::Instant::now(),
            )
            .unwrap();
        assert!(matches!(
            sessions.bind_authenticated_connection(
                1,
                CLIENT + 1,
                EPOCH,
                USER,
                attachment,
                EPOCH,
                std::time::Instant::now()
            ),
            Err(IggyError::AlreadyAuthenticated)
        ));
        assert_eq!(sessions.get_session(1), Some((CLIENT, EPOCH)));
        assert_eq!(
            sessions
                .touch_connection(1, false, Instant::now())
                .map(|context| context.metadata_watermark),
            Some(EPOCH + 5)
        );
    }

    #[test]
    fn missing_connection_or_ended_attachment_cannot_bind() {
        let mut table = registry(CLIENT, USER, EPOCH);
        let mut sessions = SessionManager::new();
        let attachment = table.attach_session(CLIENT, EPOCH, USER).unwrap();
        assert!(matches!(
            sessions.bind_authenticated_connection(
                1,
                CLIENT,
                EPOCH,
                USER,
                attachment.clone(),
                EPOCH,
                std::time::Instant::now()
            ),
            Err(IggyError::Unauthenticated)
        ));
        sessions.ensure_connection(
            1,
            addr(5000),
            ClientTransportKind::Tcp,
            std::time::Instant::now(),
        );
        table.end_session(CLIENT, USER, EPOCH, EPOCH + 1);
        assert!(matches!(
            sessions.bind_authenticated_connection(
                1,
                CLIENT,
                EPOCH,
                USER,
                attachment,
                EPOCH,
                std::time::Instant::now()
            ),
            Err(IggyError::Unauthenticated)
        ));
        assert_eq!(sessions.get_session(1), None);
    }

    #[test]
    fn disconnect_is_idempotent_and_does_not_remove_other_bindings() {
        let mut table = registry(CLIENT, USER, EPOCH);
        let mut sessions = SessionManager::new();
        bind(&mut sessions, &mut table, 1);
        bind(&mut sessions, &mut table, 2);
        assert_eq!(sessions.remove_connection(1), Some((CLIENT, EPOCH)));
        assert_eq!(sessions.remove_connection(1), None);
        assert_eq!(sessions.get_session(2), Some((CLIENT, EPOCH)));
        assert_eq!(sessions.client_count(), 1);
    }

    #[test]
    fn replies_and_rebinding_preserve_the_metadata_floor() {
        let mut table = registry(CLIENT, USER, EPOCH);
        let mut sessions = SessionManager::new();
        bind(&mut sessions, &mut table, 1);
        assert_eq!(
            sessions
                .touch_connection(1, false, Instant::now())
                .map(|context| context.metadata_watermark),
            Some(EPOCH)
        );
        sessions.record_metadata_watermark(1, 50);
        sessions.record_metadata_watermark(1, 7);
        bind(&mut sessions, &mut table, 1);
        assert_eq!(
            sessions
                .touch_connection(1, false, Instant::now())
                .map(|context| context.metadata_watermark),
            Some(50)
        );
    }

    #[test]
    fn ending_a_session_clears_the_connection_floor_before_another_user_binds() {
        let mut table = registry(CLIENT, USER, EPOCH);
        let mut sessions = SessionManager::new();
        bind(&mut sessions, &mut table, 1);
        sessions.record_metadata_watermark(1, 50);
        table.end_session(CLIENT, USER, EPOCH, EPOCH + 1);
        let context = sessions
            .touch_connection(1, true, std::time::Instant::now())
            .unwrap();
        assert_eq!(context.bound, None);
        assert_eq!(context.user_id, None);
        assert_eq!(context.metadata_watermark, 0);

        let mut replacement = registry(CLIENT + 1, USER + 1, EPOCH + 2);
        let attachment = replacement
            .attach_session(CLIENT + 1, EPOCH + 2, USER + 1)
            .unwrap();
        sessions
            .bind_authenticated_connection(
                1,
                CLIENT + 1,
                EPOCH + 2,
                USER + 1,
                attachment,
                EPOCH + 2,
                std::time::Instant::now(),
            )
            .unwrap();
        assert_eq!(
            sessions
                .touch_connection(1, false, Instant::now())
                .map(|context| context.metadata_watermark),
            Some(EPOCH + 2)
        );
    }

    #[test]
    fn a_reply_after_disconnect_does_not_recreate_connection_state() {
        let mut sessions = SessionManager::new();
        sessions.record_metadata_watermark(9, 5);
        assert!(
            sessions
                .touch_connection(9, false, Instant::now())
                .is_none()
        );
        assert_eq!(sessions.client_count(), 0);
    }
    #[test]
    fn cancellation_matches_connection_session_and_request_without_aborting_other_waits() {
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        let mut table = registry(CLIENT, USER, EPOCH);
        for connection in [1, 2] {
            bind(&mut sessions.borrow_mut(), &mut table, connection);
        }
        let (_first, first) = SessionManager::begin_deferred_request(
            &sessions,
            1,
            (10, None),
            Duration::from_secs(60),
            Instant::now(),
        )
        .unwrap();
        let (_other, other) = SessionManager::begin_deferred_request(
            &sessions,
            1,
            (11, None),
            Duration::from_secs(60),
            Instant::now(),
        )
        .unwrap();
        let (_peer, peer) = SessionManager::begin_deferred_request(
            &sessions,
            2,
            (10, None),
            Duration::from_secs(60),
            Instant::now(),
        )
        .unwrap();
        let mut first = Box::pin(futures::future::Abortable::new(
            std::future::pending::<()>(),
            first,
        ));
        let mut other = Box::pin(futures::future::Abortable::new(
            std::future::pending::<()>(),
            other,
        ));
        let mut peer = Box::pin(futures::future::Abortable::new(
            std::future::pending::<()>(),
            peer,
        ));
        assert!(
            !sessions
                .borrow()
                .cancel_deferred_request(1, CLIENT + 1, EPOCH, 10)
        );
        assert!(
            !sessions
                .borrow()
                .cancel_deferred_request(1, CLIENT, EPOCH + 1, 10)
        );
        assert!(first.as_mut().now_or_never().is_none());
        assert!(
            sessions
                .borrow()
                .cancel_deferred_request(1, CLIENT, EPOCH, 10)
        );
        assert!(first.as_mut().now_or_never().unwrap().is_err());
        assert!(other.as_mut().now_or_never().is_none());
        assert!(peer.as_mut().now_or_never().is_none());
    }

    #[test]
    fn given_committing_requests_when_cancelled_should_stop_only_the_one_whose_reads_did_not_commit()
     {
        // Every cancel aborted its request, also one whose read had already
        // stored its offset, so the client lost that batch without a reply.
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        let mut table = registry(CLIENT, USER, EPOCH);
        bind(&mut sessions.borrow_mut(), &mut table, 1);
        let open = CommitGate::default();
        let committed = CommitGate::default();
        assert!(committed.enter());
        let (_open, open_registration) = SessionManager::begin_deferred_request(
            &sessions,
            1,
            (10, Some(open.clone())),
            Duration::from_secs(60),
            Instant::now(),
        )
        .unwrap();
        let (_committed, committed_registration) = SessionManager::begin_deferred_request(
            &sessions,
            1,
            (11, Some(committed.clone())),
            Duration::from_secs(60),
            Instant::now(),
        )
        .unwrap();
        let mut open_request = Box::pin(futures::future::Abortable::new(
            std::future::pending::<()>(),
            open_registration,
        ));
        let mut committed_request = Box::pin(futures::future::Abortable::new(
            std::future::pending::<()>(),
            committed_registration,
        ));

        for request in [10, 11] {
            assert!(
                sessions
                    .borrow()
                    .cancel_deferred_request(1, CLIENT, EPOCH, request)
            );
        }

        assert!(open_request.as_mut().now_or_never().unwrap().is_err());
        assert!(open.is_cancelled());
        assert!(committed_request.as_mut().now_or_never().is_none());
        assert!(!committed.is_cancelled());
    }

    #[test]
    fn given_a_parked_committing_request_when_its_connection_is_removed_should_cancel_its_gate_before_any_store()
     {
        // The aborted request drops its reply only once its task runs again,
        // and an owner that finishes the read before that must find the gate
        // closed, or it stores an offset for a client that is gone.
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        let mut table = registry(CLIENT, USER, EPOCH);
        bind(&mut sessions.borrow_mut(), &mut table, 1);
        let gate = CommitGate::default();
        let (_request, registration) = SessionManager::begin_deferred_request(
            &sessions,
            1,
            (10, Some(gate.clone())),
            Duration::from_secs(60),
            Instant::now(),
        )
        .unwrap();
        let mut request = Box::pin(futures::future::Abortable::new(
            std::future::pending::<()>(),
            registration,
        ));

        sessions.borrow_mut().remove_connection(1);

        assert!(gate.is_cancelled());
        assert!(!gate.enter(), "a read that finishes later stores nothing");
        assert!(request.as_mut().now_or_never().unwrap().is_err());
    }

    #[test]
    fn deferred_guard_is_connection_local_finite_and_refreshes_only_after_write() {
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        let old = Instant::now().checked_sub(Duration::from_secs(30)).unwrap();
        let mut table = registry(CLIENT, USER, EPOCH);
        for connection in [1, 2] {
            let mut manager = sessions.borrow_mut();
            bind(&mut manager, &mut table, connection);
            manager
                .connections
                .get_mut(&connection)
                .unwrap()
                .last_heartbeat = old;
        }
        let (guard, _) = SessionManager::begin_deferred_request(
            &sessions,
            2,
            (10, None),
            Duration::from_secs(60),
            Instant::now(),
        )
        .unwrap();
        let now = Instant::now();
        assert_eq!(
            sessions
                .borrow()
                .collect_stale(Duration::from_secs(10), now),
            [1]
        );
        assert_eq!(sessions.borrow().connections[&1].last_heartbeat, old);
        assert_eq!(
            sessions
                .borrow()
                .collect_stale(Duration::from_secs(10), now + Duration::from_secs(61))
                .len(),
            2
        );
        let (concurrent, _) = SessionManager::begin_deferred_request(
            &sessions,
            2,
            (10, None),
            Duration::from_secs(60),
            Instant::now(),
        )
        .unwrap();
        drop(concurrent);
        assert_eq!(sessions.borrow().connections[&2].deferred_requests.len(), 1);
        guard.written(Instant::now());
        drop(guard);
        assert_eq!(
            sessions
                .borrow()
                .collect_stale(Duration::from_secs(10), Instant::now()),
            [1]
        );
        assert_eq!(sessions.borrow().connections[&1].last_heartbeat, old);
        let (failed, registration) = SessionManager::begin_deferred_request(
            &sessions,
            1,
            (10, None),
            Duration::from_secs(60),
            Instant::now(),
        )
        .unwrap();
        sessions.borrow_mut().remove_connection(1);
        let cancelled = futures::future::Abortable::new(std::future::pending::<()>(), registration);
        assert!(cancelled.now_or_never().unwrap().is_err());
        drop(failed);
    }

    #[test]
    fn dropped_deferred_guard_does_not_extend_heartbeat() {
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        sessions.borrow_mut().ensure_connection(
            1,
            addr(5000),
            ClientTransportKind::Tcp,
            Instant::now(),
        );
        let before = sessions.borrow().connections[&1].last_heartbeat;
        let (guard, _) = SessionManager::begin_deferred_request(
            &sessions,
            1,
            (10, None),
            Duration::from_secs(1),
            Instant::now(),
        )
        .unwrap();
        drop(guard);
        assert_eq!(sessions.borrow().connections[&1].last_heartbeat, before);
        assert!(
            sessions.borrow().connections[&1]
                .deferred_requests
                .is_empty()
        );
    }
}
