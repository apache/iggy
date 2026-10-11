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
use iggy_binary_protocol::ConsumerSession;
use iggy_common::IggyError;
use message_bus::installer::conn_info::ClientTransportKind;
use shard::ConnectedClientInfo;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::{Duration, Instant};

/// What the request funnel resolves from one `connections` lookup per frame.
///
/// The bound consensus session, the acting user, the transport peer address
/// and the read-your-writes floor. `Default` (everything absent, no address,
/// floor `0`) stands for a connection neither this map nor the bus knows.
#[derive(Debug, Clone, Copy, Default)]
pub struct ConnectionContext {
    /// `(client_id, session)` once register committed, `None` before.
    pub bound: Option<(u128, u64)>,
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
    ) {
        self.connections
            .entry(connection_id)
            .or_insert_with(|| Connection {
                address,
                transport,
                state: ConnectionState::Connected,
                last_heartbeat: Instant::now(),
                sdk: None,
                metadata_watermark: 0,
                consumer_session: None,
            });
    }

    /// The request funnel's per-frame view of a connection: stamp the
    /// liveness clock and read back everything the dispatch arms resolve
    /// from it, in ONE map lookup.
    ///
    /// `None` means the connection is not registered yet, which happens
    /// only on a transport's first frame; the caller installs it from the
    /// bus metadata ([`Self::ensure_connection`]) and asks again.
    pub(crate) fn touch_connection(&mut self, connection_id: u128) -> Option<ConnectionContext> {
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
        conn.last_heartbeat = Instant::now();
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
        if let Some(conn) = self.connections.remove(&connection_id)
            && let ConnectionState::Bound {
                client_id, session, ..
            } = conn.state
        {
            return Some((client_id, session));
        }
        None
    }

    pub fn bind_authenticated_connection(
        &mut self,
        connection_id: u128,
        client_id: u128,
        session: u64,
        user_id: u32,
        attachment: SessionAttachment,
        metadata_watermark: u64,
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
        connection.last_heartbeat = Instant::now();
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

    /// The highest metadata op this connection was told committed, or `0` when
    /// it was told none (an unknown or still-unbound connection, which has no
    /// write to read back).
    #[must_use]
    pub fn metadata_watermark(&self, connection_id: u128) -> u64 {
        self.connections
            .get(&connection_id)
            .map_or(0, |conn| conn.metadata_watermark)
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
    ) -> impl Iterator<Item = ConsumerSession> + '_ {
        let now = std::time::Instant::now();
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
        sessions.ensure_connection(connection, addr(5000), ClientTransportKind::Tcp);
        let attachment = table.attach_session(CLIENT, EPOCH, USER).unwrap();
        sessions
            .bind_authenticated_connection(connection, CLIENT, EPOCH, USER, attachment, EPOCH)
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
        assert_eq!(sessions.iter_consumer_sessions(TIMEOUT).count(), 0);
    }

    #[test]
    fn record_sdk_info_exposed_via_iter_clients() {
        let mut mgr = SessionManager::new();
        mgr.ensure_connection(1, addr(5000), ClientTransportKind::Tcp);

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
        sessions.ensure_connection(9, addr(5000), ClientTransportKind::Tcp);
        assert_eq!(sessions.iter_consumer_sessions(TIMEOUT).count(), 0);
        for connection in [1, 2] {
            bind(&mut sessions, &mut table, connection);
        }
        sessions.connections.get_mut(&1).unwrap().last_heartbeat =
            Instant::now().checked_sub(TIMEOUT).unwrap();
        assert_eq!(
            sessions.iter_consumer_sessions(TIMEOUT).collect::<Vec<_>>(),
            [ConsumerSession {
                client_id: CLIENT,
                session: EPOCH
            }]
        );
        sessions.touch_connection(1).unwrap();
        assert_eq!(sessions.iter_consumer_sessions(TIMEOUT).count(), 2);
        sessions.remove_connection(2);
        assert_eq!(sessions.iter_consumer_sessions(TIMEOUT).count(), 1);
        table.end_session(CLIENT, USER, EPOCH, EPOCH + 1);
        assert_eq!(sessions.iter_consumer_sessions(TIMEOUT).count(), 0);
    }

    #[test]
    fn repeated_binding_is_idempotent_and_cannot_replace_a_live_binding() {
        let mut table = registry(CLIENT, USER, EPOCH);
        let mut sessions = SessionManager::new();
        bind(&mut sessions, &mut table, 1);
        let attachment = table.attach_session(CLIENT, EPOCH, USER).unwrap();
        sessions
            .bind_authenticated_connection(1, CLIENT, EPOCH, USER, attachment.clone(), EPOCH + 5)
            .unwrap();
        assert!(matches!(
            sessions.bind_authenticated_connection(1, CLIENT + 1, EPOCH, USER, attachment, EPOCH),
            Err(IggyError::AlreadyAuthenticated)
        ));
        assert_eq!(sessions.get_session(1), Some((CLIENT, EPOCH)));
        assert_eq!(sessions.metadata_watermark(1), EPOCH + 5);
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
                EPOCH
            ),
            Err(IggyError::Unauthenticated)
        ));
        sessions.ensure_connection(1, addr(5000), ClientTransportKind::Tcp);
        table.end_session(CLIENT, USER, EPOCH, EPOCH + 1);
        assert!(matches!(
            sessions.bind_authenticated_connection(1, CLIENT, EPOCH, USER, attachment, EPOCH),
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
        assert_eq!(sessions.metadata_watermark(1), EPOCH);
        sessions.record_metadata_watermark(1, 50);
        sessions.record_metadata_watermark(1, 7);
        bind(&mut sessions, &mut table, 1);
        assert_eq!(sessions.metadata_watermark(1), 50);
    }

    #[test]
    fn ending_a_session_clears_the_connection_floor_before_another_user_binds() {
        let mut table = registry(CLIENT, USER, EPOCH);
        let mut sessions = SessionManager::new();
        bind(&mut sessions, &mut table, 1);
        sessions.record_metadata_watermark(1, 50);
        table.end_session(CLIENT, USER, EPOCH, EPOCH + 1);
        let context = sessions.touch_connection(1).unwrap();
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
            )
            .unwrap();
        assert_eq!(sessions.metadata_watermark(1), EPOCH + 2);
    }

    #[test]
    fn a_reply_after_disconnect_does_not_recreate_connection_state() {
        let mut sessions = SessionManager::new();
        sessions.record_metadata_watermark(9, 5);
        assert_eq!(sessions.metadata_watermark(9), 0);
        assert_eq!(sessions.client_count(), 0);
    }
}
