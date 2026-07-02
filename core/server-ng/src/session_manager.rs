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

//! Transport-to-consensus session bridge for server-ng.
//!
//! Maps ephemeral transport connections to durable consensus sessions.
//! Each connection goes through: `connect → login → register → bound`.
//!
//! The [`SessionManager`] is the server-side counterpart of the SDK's
//! session lifecycle. It does **not** own the `ClientTable`. That lives
//! in the consensus layer. This module tracks the binding between a
//! transport connection and the consensus-level `(client_id, session)` pair.

use message_bus::installer::conn_info::ClientTransportKind;
use shard::ConnectedClientInfo;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// Connection lifecycle states.
///
/// ```text
///   Connected ──login──> Authenticated ──register──> Bound
///
///   Bound ──evict──> Connected   (another conn binds same client_id)
///   {any} ──disconnect──> ∅
/// ```
#[derive(Debug, Clone)]
pub enum ConnectionState {
    /// Connection established, not yet authenticated.
    Connected,
    /// Login succeeded (credentials verified). `user_id` is known.
    /// Waiting for register to establish a consensus session.
    Authenticated { user_id: u32 },
    /// Register committed through consensus. Connection is bound to a
    /// `(client_id, session)` pair. Requests on this connection use
    /// these values to populate `RequestHeader.client` and
    /// `RequestHeader.session`.
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
}

/// Bridges transport connections to consensus sessions.
///
/// NOT thread-safe: each shard owns one `SessionManager` on its
/// single-threaded compio runtime, the same way the rest of server-ng
/// is structured. All mutators take `&mut self`; the type carries no
/// internal locking.
///
/// ## Invariants
///
/// - A `connection_id` appears in at most one of `connections`.
/// - A `client_id` appears in at most one `Bound` connection (one connection
///   per consensus session). If a client reconnects with the same `client_id`,
///   the old connection must be evicted first.
pub struct SessionManager {
    connections: HashMap<u128, Connection>,
    /// Reverse index: `client_id` → `connection_id` for fast lookup when
    /// a consensus reply arrives and needs routing to the right connection.
    client_to_connection: HashMap<u128, u128>,
}

impl SessionManager {
    #[must_use]
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
            client_to_connection: HashMap::new(),
        }
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
            });
    }

    /// Record a liveness heartbeat (`ping`) for a connection, resetting its
    /// staleness clock. No-op for an unknown connection.
    pub fn record_heartbeat(&mut self, connection_id: u128) {
        if let Some(conn) = self.connections.get_mut(&connection_id) {
            conn.last_heartbeat = Instant::now();
        }
    }

    /// Connection ids whose last heartbeat is older than `max_age` -- the
    /// stale set the heartbeat verifier evicts. Only `Bound`/`Authenticated`
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
            ConnectionState::Authenticated { .. } | ConnectionState::Connected => None,
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

    /// Remove a connection (disconnect). Cleans up the reverse index if bound.
    ///
    /// Returns the bound `(client_id, session)` when the removed connection had
    /// one, so the caller can submit a session-matched `Logout` (the committed
    /// apply releases the client-table slot cluster-wide).
    pub fn remove_connection(&mut self, connection_id: u128) -> Option<(u128, u64)> {
        if let Some(conn) = self.connections.remove(&connection_id)
            && let ConnectionState::Bound {
                client_id, session, ..
            } = conn.state
        {
            self.client_to_connection.remove(&client_id);
            return Some((client_id, session));
        }
        None
    }

    /// Transition to `Authenticated` after successful login.
    ///
    /// # Errors
    /// Returns `Err` if the connection doesn't exist or isn't in `Connected` state.
    pub fn login(&mut self, connection_id: u128, user_id: u32) -> Result<(), SessionError> {
        let conn = self
            .connections
            .get_mut(&connection_id)
            .ok_or(SessionError::ConnectionNotFound(connection_id))?;
        match conn.state {
            ConnectionState::Connected => {
                conn.state = ConnectionState::Authenticated { user_id };
                Ok(())
            }
            _ => Err(SessionError::InvalidTransition {
                connection_id,
                from: state_name(&conn.state),
                to: "Authenticated",
            }),
        }
    }

    /// Transition to `Bound` after register commits through consensus.
    ///
    /// The `client_id` is the ephemeral u128 the client generated.
    /// The `session` is the commit op number assigned by the consensus layer.
    ///
    /// If another connection was previously bound to this `client_id`, it is
    /// forcibly unbound (set back to `Connected`). Only one connection per
    /// session at a time.
    ///
    /// # Errors
    /// Returns `Err` if the connection doesn't exist or isn't `Authenticated`.
    ///
    /// # Panics
    /// Panics if the connection disappears between validation and mutation
    /// (impossible in single-threaded use).
    pub fn bind_session(
        &mut self,
        connection_id: u128,
        client_id: u128,
        session: u64,
    ) -> Result<(), SessionError> {
        // Validate state first (immutable borrow).
        let conn = self
            .connections
            .get(&connection_id)
            .ok_or(SessionError::ConnectionNotFound(connection_id))?;
        let ConnectionState::Authenticated { user_id } = conn.state else {
            return Err(SessionError::InvalidTransition {
                connection_id,
                from: state_name(&conn.state),
                to: "Bound",
            });
        };

        // Evict any previous connection bound to this client_id.
        if let Some(&old_conn_id) = self.client_to_connection.get(&client_id)
            && old_conn_id != connection_id
            && let Some(old_conn) = self.connections.get_mut(&old_conn_id)
        {
            old_conn.state = ConnectionState::Connected;
        }

        // Now mutate the target connection.
        self.connections.get_mut(&connection_id).unwrap().state = ConnectionState::Bound {
            user_id,
            client_id,
            session,
        };
        self.client_to_connection.insert(client_id, connection_id);
        Ok(())
    }

    /// Look up the consensus session for a connection.
    ///
    /// Returns `(client_id, session)` if the connection is `Bound`, `None` otherwise.
    #[must_use]
    pub fn get_session(&self, connection_id: u128) -> Option<(u128, u64)> {
        let conn = self.connections.get(&connection_id)?;
        match conn.state {
            ConnectionState::Bound {
                client_id, session, ..
            } => Some((client_id, session)),
            _ => None,
        }
    }

    /// Look up the authenticated user id for a connection.
    #[must_use]
    pub fn get_user_id(&self, connection_id: u128) -> Option<u32> {
        let conn = self.connections.get(&connection_id)?;
        match conn.state {
            ConnectionState::Authenticated { user_id } | ConnectionState::Bound { user_id, .. } => {
                Some(user_id)
            }
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
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub enum SessionError {
    ConnectionNotFound(u128),
    InvalidTransition {
        connection_id: u128,
        from: &'static str,
        to: &'static str,
    },
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectionNotFound(id) => write!(f, "connection {id} not found"),
            Self::InvalidTransition {
                connection_id,
                from,
                to,
            } => write!(
                f,
                "connection {connection_id}: invalid transition {from} -> {to}"
            ),
        }
    }
}

impl std::error::Error for SessionError {}

/// Flatten a connection + its id into a [`ConnectedClientInfo`].
fn record_from(connection_id: u128, conn: &Connection) -> ConnectedClientInfo {
    let user_id = match conn.state {
        ConnectionState::Authenticated { user_id } | ConnectionState::Bound { user_id, .. } => {
            Some(user_id)
        }
        ConnectionState::Connected => None,
    };
    let vsr_client_id = match conn.state {
        ConnectionState::Bound { client_id, .. } => Some(client_id),
        ConnectionState::Authenticated { .. } | ConnectionState::Connected => None,
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

const fn state_name(state: &ConnectionState) -> &'static str {
    match state {
        ConnectionState::Connected => "Connected",
        ConnectionState::Authenticated { .. } => "Authenticated",
        ConnectionState::Bound { .. } => "Bound",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
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
    fn full_lifecycle() {
        let mut mgr = SessionManager::new();

        let conn = 1;
        mgr.ensure_connection(conn, addr(5000), ClientTransportKind::Tcp);
        assert_eq!(mgr.iter_clients().count(), 1);
        assert!(mgr.get_session(conn).is_none());

        // Login
        mgr.login(conn, 42).unwrap();
        assert!(mgr.get_session(conn).is_none()); // not bound yet

        // Register committed. Bind session
        let client_id: u128 = 0xDEAD_BEEF;
        let session: u64 = 100;
        mgr.bind_session(conn, client_id, session).unwrap();

        assert_eq!(mgr.get_session(conn), Some((client_id, session)));

        // Disconnect returns the bound (client_id, session) and clears state.
        assert_eq!(mgr.remove_connection(conn), Some((client_id, session)));
        assert_eq!(mgr.iter_clients().count(), 0);
        assert!(mgr.get_session(conn).is_none());
    }

    #[test]
    fn login_requires_connected_state() {
        let mut mgr = SessionManager::new();
        let conn = 1;
        mgr.ensure_connection(conn, addr(5000), ClientTransportKind::Tcp);
        mgr.login(conn, 1).unwrap();

        // Double login should fail. Already Authenticated.
        assert!(mgr.login(conn, 2).is_err());
    }

    #[test]
    fn bind_requires_authenticated_state() {
        let mut mgr = SessionManager::new();
        let conn = 1;
        mgr.ensure_connection(conn, addr(5000), ClientTransportKind::Tcp);

        // Bind without login should fail.
        assert!(mgr.bind_session(conn, 1, 1).is_err());
    }

    #[test]
    fn bind_evicts_old_connection_for_same_client() {
        let mut mgr = SessionManager::new();

        // First connection binds to client_id 99.
        let conn1 = 1;
        mgr.ensure_connection(conn1, addr(5000), ClientTransportKind::Tcp);
        mgr.login(conn1, 1).unwrap();
        mgr.bind_session(conn1, 99, 10).unwrap();
        assert_eq!(mgr.get_session(conn1), Some((99, 10)));

        // Second connection binds to same client_id. Evicts conn1.
        let conn2 = 2;
        mgr.ensure_connection(conn2, addr(5001), ClientTransportKind::Tcp);
        mgr.login(conn2, 1).unwrap();
        mgr.bind_session(conn2, 99, 20).unwrap();

        // conn2 now owns client 99; conn1 reverted to Connected.
        assert_eq!(mgr.get_session(conn2), Some((99, 20)));
        assert!(mgr.get_session(conn1).is_none());
    }

    #[test]
    fn remove_nonexistent_connection_is_noop() {
        let mut mgr = SessionManager::new();
        mgr.remove_connection(999); // should not panic
    }

    #[test]
    fn login_nonexistent_connection_errors() {
        let mut mgr = SessionManager::new();
        assert!(mgr.login(999, 1).is_err());
    }

    #[test]
    fn multiple_independent_sessions() {
        let mut mgr = SessionManager::new();

        let c1 = 1;
        let c2 = 2;
        mgr.ensure_connection(c1, addr(5000), ClientTransportKind::Tcp);
        mgr.ensure_connection(c2, addr(5001), ClientTransportKind::Tcp);
        mgr.login(c1, 1).unwrap();
        mgr.login(c2, 2).unwrap();
        mgr.bind_session(c1, 100, 10).unwrap();
        mgr.bind_session(c2, 200, 20).unwrap();

        assert_eq!(mgr.get_session(c1), Some((100, 10)));
        assert_eq!(mgr.get_session(c2), Some((200, 20)));
        assert_eq!(mgr.iter_clients().count(), 2);

        assert_eq!(mgr.remove_connection(c1), Some((100, 10)));
        assert!(mgr.get_session(c1).is_none());
        assert_eq!(mgr.get_session(c2), Some((200, 20)));
    }
}
