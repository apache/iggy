/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
use std::collections::HashMap;
use std::net::SocketAddr;

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

/// Per-connection metadata tracked by the session manager.
#[derive(Debug, Clone)]
pub struct Connection {
    pub address: SocketAddr,
    pub transport: ClientTransportKind,
    pub state: ConnectionState,
}

/// Flattened view of one connected client for the `get_me` /
/// `get_clients` reads.
///
/// Holds everything those responses need from a single per-shard source,
/// so the caller no longer also consults the message bus. `consumer_groups`
/// membership is not modelled yet (see the TODO in the `get_clients`
/// response builder); it ships empty until the partition-reconciliation /
/// consumer-group-rebalancing work lands.
#[derive(Debug, Clone)]
pub struct ClientRecord {
    /// Transport (coordinator-minted) client id; top 16 bits are the home
    /// shard. The wire `client_id` is the `u32` seq tail.
    pub client_id: u128,
    pub user_id: Option<u32>,
    pub transport: ClientTransportKind,
    pub address: SocketAddr,
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
        self.connections.entry(connection_id).or_insert(Connection {
            address,
            transport,
            state: ConnectionState::Connected,
        });
    }

    /// Remove a connection (disconnect). Cleans up the reverse index if bound.
    ///
    /// Returns the bound consensus client id when the removed connection had
    /// one, so callers can release the corresponding client-table slot.
    pub fn remove_connection(&mut self, connection_id: u128) -> Option<u128> {
        if let Some(conn) = self.connections.remove(&connection_id)
            && let ConnectionState::Bound { client_id, .. } = conn.state
        {
            self.client_to_connection.remove(&client_id);
            return Some(client_id);
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

    /// Look up the connection ID for a client (for routing consensus replies).
    #[must_use]
    pub fn connection_for_client(&self, client_id: u128) -> Option<u128> {
        self.client_to_connection.get(&client_id).copied()
    }

    /// Get connection metadata.
    #[must_use]
    pub fn get_connection(&self, connection_id: u128) -> Option<&Connection> {
        self.connections.get(&connection_id)
    }

    /// Flatten one connection into a [`ClientRecord`] for `get_me`.
    ///
    /// This is the single per-shard source for the client-info reads:
    /// `user_id`, `transport`, and `address` all come from the local
    /// `SessionManager`, so the caller no longer consults the message
    /// bus's `client_meta`.
    #[must_use]
    pub fn client_record(&self, connection_id: u128) -> Option<ClientRecord> {
        let conn = self.connections.get(&connection_id)?;
        Some(record_from(connection_id, conn))
    }

    /// Iterate every locally-homed connected client as a [`ClientRecord`].
    /// The per-shard half of the `get_clients` scatter-gather.
    pub fn iter_clients(&self) -> impl Iterator<Item = ClientRecord> + '_ {
        self.connections
            .iter()
            .map(|(&id, conn)| record_from(id, conn))
    }

    /// Number of active connections.
    #[must_use]
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    /// Number of bound (registered) sessions.
    #[must_use]
    pub fn bound_count(&self) -> usize {
        self.client_to_connection.len()
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

/// Flatten a connection + its id into a [`ClientRecord`].
const fn record_from(connection_id: u128, conn: &Connection) -> ClientRecord {
    let user_id = match conn.state {
        ConnectionState::Authenticated { user_id } | ConnectionState::Bound { user_id, .. } => {
            Some(user_id)
        }
        ConnectionState::Connected => None,
    };
    ClientRecord {
        client_id: connection_id,
        user_id,
        transport: conn.transport,
        address: conn.address,
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
    fn full_lifecycle() {
        let mut mgr = SessionManager::new();

        let conn = 1;
        mgr.ensure_connection(conn, addr(5000), ClientTransportKind::Tcp);
        assert_eq!(mgr.connection_count(), 1);
        assert!(mgr.get_session(conn).is_none());

        // Login
        mgr.login(conn, 42).unwrap();
        assert!(mgr.get_session(conn).is_none()); // not bound yet

        // Register committed. Bind session
        let client_id: u128 = 0xDEAD_BEEF;
        let session: u64 = 100;
        mgr.bind_session(conn, client_id, session).unwrap();

        assert_eq!(mgr.get_session(conn), Some((client_id, session)));
        assert_eq!(mgr.connection_for_client(client_id), Some(conn));
        assert_eq!(mgr.bound_count(), 1);

        // Disconnect
        mgr.remove_connection(conn);
        assert_eq!(mgr.connection_count(), 0);
        assert_eq!(mgr.bound_count(), 0);
        assert!(mgr.connection_for_client(client_id).is_none());
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
        assert_eq!(mgr.connection_for_client(99), Some(conn1));

        // Second connection binds to same client_id. Evicts conn1.
        let conn2 = 2;
        mgr.ensure_connection(conn2, addr(5001), ClientTransportKind::Tcp);
        mgr.login(conn2, 1).unwrap();
        mgr.bind_session(conn2, 99, 20).unwrap();

        assert_eq!(mgr.connection_for_client(99), Some(conn2));
        // conn1 reverted to Connected.
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
        assert_eq!(mgr.bound_count(), 2);

        mgr.remove_connection(c1);
        assert_eq!(mgr.bound_count(), 1);
        assert!(mgr.connection_for_client(100).is_none());
        assert_eq!(mgr.connection_for_client(200), Some(c2));
    }
}
