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

//! Port pre-allocation to eliminate TOCTOU race conditions during server startup.
//!
//! When starting a test server with ephemeral ports (port 0), there's a race window:
//! 1. Server binds to port 0, OS assigns a port
//! 2. Server writes the port to config file
//! 3. Test harness reads config file to discover port
//!
//! By pre-reserving ports with SO_REUSEADDR/SO_REUSEPORT before server start,
//! we know the ports immediately and only need to wait for server readiness.

use crate::harness::config::{IpAddrKind, TestServerConfig};
use crate::harness::error::TestBinaryError;
use socket2::{Domain, Protocol, Socket, Type};
use std::collections::HashSet;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Mutex;

/// Ports already handed out by any reservation in this process.
///
/// The reservation sockets set `SO_REUSEPORT` so the server can later bind the
/// same port the reserver held. But `SO_REUSEPORT` also lets the kernel assign
/// the *same* ephemeral port to two `bind(0)` calls from different threads, so
/// under a multi-threaded test run two clusters can reserve the same port and
/// the second server aborts on the cluster config's port-conflict validator.
/// Tracking every handed-out port and rebinding on collision makes reservations
/// unique across the whole process. Ports are claimed for the process lifetime
/// (never returned to the pool): a release frees the socket so the server can
/// bind, but re-handing the number out could still collide with that server.
static RESERVED_PORTS: Mutex<Option<HashSet<u16>>> = Mutex::new(None);

/// Bind retries before giving up. The ephemeral range dwarfs the ports a single
/// test run consumes, so a fresh port is found almost immediately; the cap only
/// guards against a pathological kernel that keeps returning claimed ports.
const MAX_BIND_ATTEMPTS: usize = 100;

/// Claim `port` process-wide. Returns `false` if it was already handed out.
fn claim_port(port: u16) -> bool {
    RESERVED_PORTS
        .lock()
        .expect("reserved-ports mutex poisoned")
        .get_or_insert_with(HashSet::new)
        .insert(port)
}

/// A socket bound to a specific port, held to prevent reuse until released.
struct ReservedPort {
    socket: Socket,
    addr: SocketAddr,
}

impl ReservedPort {
    fn tcp(ip_kind: IpAddrKind) -> Result<Self, TestBinaryError> {
        Self::reserve_unique(ip_kind, Type::STREAM, Protocol::TCP)
    }

    fn udp(ip_kind: IpAddrKind) -> Result<Self, TestBinaryError> {
        Self::reserve_unique(ip_kind, Type::DGRAM, Protocol::UDP)
    }

    /// Bind an ephemeral port and claim it process-wide, rebinding until the
    /// kernel hands back a port no other reservation holds (see
    /// [`RESERVED_PORTS`]).
    fn reserve_unique(
        ip_kind: IpAddrKind,
        sock_type: Type,
        protocol: Protocol,
    ) -> Result<Self, TestBinaryError> {
        let domain = match ip_kind {
            IpAddrKind::V4 => Domain::IPV4,
            IpAddrKind::V6 => Domain::IPV6,
        };
        let bind_addr: SocketAddr = match ip_kind {
            IpAddrKind::V4 => SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0),
            IpAddrKind::V6 => SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 0),
        };

        for _ in 0..MAX_BIND_ATTEMPTS {
            let socket = Socket::new(domain, sock_type, Some(protocol)).map_err(|e| {
                TestBinaryError::InvalidState {
                    message: format!("Failed to create socket: {e}"),
                }
            })?;

            socket
                .set_reuse_address(true)
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to set SO_REUSEADDR: {e}"),
                })?;

            #[cfg(unix)]
            socket
                .set_reuse_port(true)
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to set SO_REUSEPORT: {e}"),
                })?;

            socket
                .bind(&bind_addr.into())
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to bind socket: {e}"),
                })?;

            // Bind WITHOUT listen: a listening reservation joins the port's
            // SO_REUSEPORT group and accepts early dials into a backlog nobody
            // drains. During cluster startup a peer's replica connector dials
            // ports whose reservations are still held (release happens per-node
            // right before spawn), so each such dial used to wedge until the
            // 2s handshake-ack timeout. A bound-only socket still holds the
            // port against ephemeral allocation but answers RST, so dialers
            // fail fast and the reconnect sweep retries cleanly.
            let addr = socket
                .local_addr()
                .map_err(|e| TestBinaryError::InvalidState {
                    message: format!("Failed to get local address: {e}"),
                })?
                .as_socket()
                .ok_or_else(|| TestBinaryError::InvalidState {
                    message: "Socket address is not an IP address".to_string(),
                })?;

            // SO_REUSEPORT lets the kernel reuse a port another reservation
            // already holds; rebind (dropping this socket) until the claim wins.
            if claim_port(addr.port()) {
                return Ok(Self { socket, addr });
            }
        }

        Err(TestBinaryError::InvalidState {
            message: format!("Failed to reserve a unique port after {MAX_BIND_ATTEMPTS} attempts"),
        })
    }

    fn addr(&self) -> SocketAddr {
        self.addr
    }

    fn release(self) {
        drop(self.socket);
    }
}

/// Pre-allocated ports for all enabled protocols.
pub struct PortReserver {
    tcp: Option<ReservedPort>,
    tcp_replica: Option<ReservedPort>,
    quic: Option<ReservedPort>,
    http: Option<ReservedPort>,
    websocket: Option<ReservedPort>,
}

/// Single port reservation for simpler binaries (MCP, connectors).
pub struct SinglePortReserver {
    reserved: ReservedPort,
}

impl SinglePortReserver {
    pub fn new() -> Result<Self, TestBinaryError> {
        let reserved = ReservedPort::tcp(IpAddrKind::V4)?;
        Ok(Self { reserved })
    }

    pub fn address(&self) -> SocketAddr {
        self.reserved.addr()
    }

    pub fn release(self) {
        self.reserved.release();
    }
}

/// Addresses for all enabled protocols.
#[derive(Debug, Clone)]
pub struct ProtocolAddresses {
    pub tcp: Option<SocketAddr>,
    pub tcp_replica: Option<SocketAddr>,
    pub quic: Option<SocketAddr>,
    pub http: Option<SocketAddr>,
    pub websocket: Option<SocketAddr>,
}

/// Pre-allocated ports for a cluster of servers.
pub struct ClusterPortReserver {
    nodes: Vec<PortReserver>,
}

impl ClusterPortReserver {
    /// Reserve ports for all nodes in a cluster.
    pub fn reserve(
        node_count: usize,
        ip_kind: IpAddrKind,
        config: &TestServerConfig,
    ) -> Result<Self, TestBinaryError> {
        let mut nodes = Vec::with_capacity(node_count);
        for _ in 0..node_count {
            nodes.push(PortReserver::reserve(ip_kind, config)?);
        }
        Ok(Self { nodes })
    }

    /// Get the addresses for all nodes.
    pub fn all_addresses(&self) -> Vec<ProtocolAddresses> {
        self.nodes.iter().map(|n| n.addresses()).collect()
    }

    /// Take ownership of individual port reservers for each node.
    pub fn into_reservers(self) -> Vec<PortReserver> {
        self.nodes
    }
}

impl PortReserver {
    /// Reserve ports for all protocols enabled in the config.
    pub fn reserve(
        ip_kind: IpAddrKind,
        config: &TestServerConfig,
    ) -> Result<Self, TestBinaryError> {
        let tcp = Some(ReservedPort::tcp(ip_kind)?);
        let tcp_replica = Some(ReservedPort::tcp(ip_kind)?);

        let quic = if config.quic_enabled {
            Some(ReservedPort::udp(ip_kind)?)
        } else {
            None
        };

        let http = if config.http_enabled {
            Some(ReservedPort::tcp(ip_kind)?)
        } else {
            None
        };

        let websocket = if config.websocket_enabled {
            Some(ReservedPort::tcp(ip_kind)?)
        } else {
            None
        };

        Ok(Self {
            tcp,
            tcp_replica,
            quic,
            http,
            websocket,
        })
    }

    /// Get the bound addresses for environment variable configuration.
    pub fn addresses(&self) -> ProtocolAddresses {
        ProtocolAddresses {
            tcp: self.tcp.as_ref().map(ReservedPort::addr),
            tcp_replica: self.tcp_replica.as_ref().map(ReservedPort::addr),
            quic: self.quic.as_ref().map(ReservedPort::addr),
            http: self.http.as_ref().map(ReservedPort::addr),
            websocket: self.websocket.as_ref().map(ReservedPort::addr),
        }
    }

    /// Release all held sockets. Call after server has successfully bound.
    pub fn release(self) {
        if let Some(tcp) = self.tcp {
            tcp.release();
        }
        if let Some(tcp_replica) = self.tcp_replica {
            tcp_replica.release();
        }
        if let Some(quic) = self.quic {
            quic.release();
        }
        if let Some(http) = self.http {
            http.release();
        }
        if let Some(websocket) = self.websocket {
            websocket.release();
        }
    }
}
