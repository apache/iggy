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
//! session lifecycle. It does **not** own the [`ClientTable`]. That lives
//! in the consensus layer. This module tracks the binding between a
//! transport connection and the consensus-level `(client_id, session)` pair.

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
    pub state: ConnectionState,
}

/// Bridges transport connections to consensus sessions.
///
/// Thread-safe: intended to be shared across connection handler tasks.
/// Uses interior mutability via the caller's synchronization (single-threaded
/// shard model like the rest of iggy's server-ng).
///
/// ## Invariants
///
/// - A `connection_id` appears in at most one of `connections`.
/// - A `client_id` appears in at most one `Bound` connection (one connection
///   per consensus session). If a client reconnects with the same `client_id`,
///   the old connection must be evicted first.
pub struct SessionManager {
    connections: HashMap<u64, Connection>,
    /// Reverse index: `client_id` → `connection_id` for fast lookup when
    /// a consensus reply arrives and needs routing to the right connection.
    client_to_connection: HashMap<u128, u64>,
    next_connection_id: u64,
}

impl SessionManager {
    #[must_use]
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
            client_to_connection: HashMap::new(),
            next_connection_id: 1,
        }
    }

    /// Register a new transport connection. Returns the assigned connection ID.
    ///
    /// # Panics
    /// Panics if the connection ID counter overflows `u64::MAX`.
    pub fn add_connection(&mut self, address: SocketAddr) -> u64 {
        let id = self.next_connection_id;
        self.next_connection_id = self
            .next_connection_id
            .checked_add(1)
            .expect("connection ID overflow (u64::MAX connections without restart)");
        self.connections.insert(
            id,
            Connection {
                address,
                state: ConnectionState::Connected,
            },
        );
        id
    }

    /// Remove a connection (disconnect). Cleans up the reverse index if bound.
    pub fn remove_connection(&mut self, connection_id: u64) {
        if let Some(conn) = self.connections.remove(&connection_id)
            && let ConnectionState::Bound { client_id, .. } = conn.state
        {
            self.client_to_connection.remove(&client_id);
        }
    }

    /// Transition to `Authenticated` after successful login.
    ///
    /// # Errors
    /// Returns `Err` if the connection doesn't exist or isn't in `Connected` state.
    pub fn login(&mut self, connection_id: u64, user_id: u32) -> Result<(), SessionError> {
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

    /// Reset a connection back to `Connected` state.
    ///
    /// Used to roll back a failed register attempt so the client can retry
    /// the full login+register flow on the same connection without
    /// reconnecting.
    ///
    /// # Errors
    /// Returns `Err` if the connection doesn't exist or isn't `Authenticated`.
    pub fn reset_to_connected(&mut self, connection_id: u64) -> Result<(), SessionError> {
        let conn = self
            .connections
            .get_mut(&connection_id)
            .ok_or(SessionError::ConnectionNotFound(connection_id))?;
        match conn.state {
            ConnectionState::Authenticated { .. } => {
                conn.state = ConnectionState::Connected;
                Ok(())
            }
            _ => Err(SessionError::InvalidTransition {
                connection_id,
                from: state_name(&conn.state),
                to: "Connected",
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
        connection_id: u64,
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
    pub fn get_session(&self, connection_id: u64) -> Option<(u128, u64)> {
        let conn = self.connections.get(&connection_id)?;
        match conn.state {
            ConnectionState::Bound {
                client_id, session, ..
            } => Some((client_id, session)),
            _ => None,
        }
    }

    /// Look up the connection ID for a client (for routing consensus replies).
    #[must_use]
    pub fn connection_for_client(&self, client_id: u128) -> Option<u64> {
        self.client_to_connection.get(&client_id).copied()
    }

    /// Get connection metadata.
    #[must_use]
    pub fn get_connection(&self, connection_id: u64) -> Option<&Connection> {
        self.connections.get(&connection_id)
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
    ConnectionNotFound(u64),
    InvalidTransition {
        connection_id: u64,
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

        let conn = mgr.add_connection(addr(5000));
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
        let conn = mgr.add_connection(addr(5000));
        mgr.login(conn, 1).unwrap();

        // Double login should fail. Already Authenticated.
        assert!(mgr.login(conn, 2).is_err());
    }

    #[test]
    fn bind_requires_authenticated_state() {
        let mut mgr = SessionManager::new();
        let conn = mgr.add_connection(addr(5000));

        // Bind without login should fail.
        assert!(mgr.bind_session(conn, 1, 1).is_err());
    }

    #[test]
    fn bind_evicts_old_connection_for_same_client() {
        let mut mgr = SessionManager::new();

        // First connection binds to client_id 99.
        let conn1 = mgr.add_connection(addr(5000));
        mgr.login(conn1, 1).unwrap();
        mgr.bind_session(conn1, 99, 10).unwrap();
        assert_eq!(mgr.connection_for_client(99), Some(conn1));

        // Second connection binds to same client_id. Evicts conn1.
        let conn2 = mgr.add_connection(addr(5001));
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
    fn reset_to_connected_from_authenticated() {
        let mut mgr = SessionManager::new();
        let conn = mgr.add_connection(addr(5000));
        mgr.login(conn, 1).unwrap();
        mgr.reset_to_connected(conn).unwrap();
        // Back to Connected. Can login again.
        mgr.login(conn, 2).unwrap();
    }

    #[test]
    fn reset_to_connected_rejects_wrong_state() {
        let mut mgr = SessionManager::new();
        let conn = mgr.add_connection(addr(5000));
        // Connected - reset should fail.
        assert!(mgr.reset_to_connected(conn).is_err());
    }

    #[test]
    fn multiple_independent_sessions() {
        let mut mgr = SessionManager::new();

        let c1 = mgr.add_connection(addr(5000));
        let c2 = mgr.add_connection(addr(5001));
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
