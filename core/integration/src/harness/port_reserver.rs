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
use std::fs::{File, OpenOptions, TryLockError};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

/// Ports this process must not hand out: either it reserved them, or it saw
/// another process holding the cross-process lock (see [`PORT_LOCKS`]).
///
/// `SO_REUSEPORT` (needed so the server can later bind the port the reserver
/// held) also lets the kernel hand the *same* ephemeral port to two `bind(0)`
/// calls, so two reservations can land on one port. This set rejects the
/// duplicate within a process; [`PORT_LOCKS`] extends the guarantee across the
/// concurrent test processes. Entries are never removed: a release frees the
/// socket so the server can bind, but re-handing the number out could still
/// collide with that server, and a number seen held elsewhere stays off-limits
/// (the ephemeral range dwarfs what one test consumes).
static RESERVED_PORTS: Mutex<Option<HashSet<u16>>> = Mutex::new(None);

/// Advisory file locks held for the process lifetime, one per reserved port.
///
/// `RESERVED_PORTS` dedups only within a process, but `nextest` runs each test
/// in its own process and they reserve ports concurrently. An exclusive `flock`
/// on a per-port file in a shared temp dir makes a claim visible to every other
/// test process: one that `bind(0)`s onto the same ephemeral port fails the lock
/// and rebinds. The lock is kept past the socket release (see
/// [`ReservedPort::release`]) so it also covers the window between releasing the
/// reservation socket and the server binding the port. The OS drops the locks on
/// process exit, so a crash never leaks a claim.
///
/// One file descriptor is held per reserved port for the process lifetime. That
/// is bounded because `nextest` gives each test its own short-lived process (a
/// test reserves at most a few dozen ports); a single-process run of the whole
/// suite would instead accumulate them, so this reserver assumes the per-test
/// process model.
static PORT_LOCKS: Mutex<Vec<File>> = Mutex::new(Vec::new());

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

/// Shared directory holding the per-port advisory lock files, created once.
fn port_lock_dir() -> &'static PathBuf {
    static PORT_LOCK_DIR: OnceLock<PathBuf> = OnceLock::new();
    PORT_LOCK_DIR.get_or_init(|| {
        let dir = std::env::temp_dir().join("iggy-test-port-locks");
        let _ = std::fs::create_dir_all(&dir);
        dir
    })
}

/// Outcome of trying to take a port's cross-process advisory lock.
enum PortClaim {
    /// Exclusive lock held; keep the file alive while the port is in use.
    Locked(File),
    /// Another test process holds the port; the caller must rebind.
    Contended,
    /// The lock directory is unusable (cannot create/open/lock the file), so no
    /// cross-process coordination is available. The caller keeps the port and
    /// degrades to the in-process guard alone rather than failing the run.
    Unsupported,
}

/// Try to take an exclusive cross-process lock on `port`.
fn lock_port(port: u16) -> PortClaim {
    let path = port_lock_dir().join(format!("{port}.lock"));
    let Ok(file) = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
    else {
        return PortClaim::Unsupported;
    };
    match file.try_lock() {
        Ok(()) => PortClaim::Locked(file),
        Err(TryLockError::WouldBlock) => PortClaim::Contended,
        Err(TryLockError::Error(_)) => PortClaim::Unsupported,
    }
}

/// A socket bound to a specific port, held to prevent reuse until released,
/// plus the cross-process advisory lock on that port (see [`PORT_LOCKS`]), or
/// `None` when the lock directory is unusable and only the in-process guard
/// applies.
struct ReservedPort {
    socket: Socket,
    addr: SocketAddr,
    lock: Option<File>,
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
            // already holds; rebind (dropping this socket) until the claim wins
            // both in-process and across the concurrent test processes.
            if claim_port(addr.port()) {
                match lock_port(addr.port()) {
                    PortClaim::Locked(lock) => {
                        return Ok(Self {
                            socket,
                            addr,
                            lock: Some(lock),
                        });
                    }
                    // Lock infra unavailable: keep the in-process claim and run
                    // without the cross-process guard rather than fail the run.
                    PortClaim::Unsupported => {
                        return Ok(Self {
                            socket,
                            addr,
                            lock: None,
                        });
                    }
                    // Held by another process; the number stays claimed (so we
                    // do not retry it) and we rebind to a different port.
                    PortClaim::Contended => {}
                }
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
        // Drop the reservation socket so the server can bind the port, but move
        // the advisory lock (when held) into the process-wide registry so it
        // outlives the socket and keeps concurrent test processes off the port
        // until the server has bound it.
        let Self { socket, lock, .. } = self;
        if let Some(lock) = lock {
            PORT_LOCKS
                .lock()
                .expect("port-locks mutex poisoned")
                .push(lock);
        }
        drop(socket);
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
