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
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

/// A socket bound to a specific port, held to prevent reuse until released.
struct ReservedPort {
    socket: Socket,
    addr: SocketAddr,
}

impl ReservedPort {
    fn tcp(ip_kind: IpAddrKind) -> Result<Self, TestBinaryError> {
        let domain = match ip_kind {
            IpAddrKind::V4 => Domain::IPV4,
            IpAddrKind::V6 => Domain::IPV6,
        };

        let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP)).map_err(|e| {
            TestBinaryError::InvalidState {
                message: format!("Failed to create TCP socket: {e}"),
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

        let bind_addr: SocketAddr = match ip_kind {
            IpAddrKind::V4 => SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0),
            IpAddrKind::V6 => SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 0),
        };

        socket
            .bind(&bind_addr.into())
            .map_err(|e| TestBinaryError::InvalidState {
                message: format!("Failed to bind TCP socket: {e}"),
            })?;

        socket
            .listen(1)
            .map_err(|e| TestBinaryError::InvalidState {
                message: format!("Failed to listen on TCP socket: {e}"),
            })?;

        let addr = socket
            .local_addr()
            .map_err(|e| TestBinaryError::InvalidState {
                message: format!("Failed to get local address: {e}"),
            })?
            .as_socket()
            .ok_or_else(|| TestBinaryError::InvalidState {
                message: "Socket address is not an IP address".to_string(),
            })?;

        Ok(Self { socket, addr })
    }

    fn udp(ip_kind: IpAddrKind) -> Result<Self, TestBinaryError> {
        let domain = match ip_kind {
            IpAddrKind::V4 => Domain::IPV4,
            IpAddrKind::V6 => Domain::IPV6,
        };

        let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP)).map_err(|e| {
            TestBinaryError::InvalidState {
                message: format!("Failed to create UDP socket: {e}"),
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

        let bind_addr: SocketAddr = match ip_kind {
            IpAddrKind::V4 => SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0),
            IpAddrKind::V6 => SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 0),
        };

        socket
            .bind(&bind_addr.into())
            .map_err(|e| TestBinaryError::InvalidState {
                message: format!("Failed to bind UDP socket: {e}"),
            })?;

        let addr = socket
            .local_addr()
            .map_err(|e| TestBinaryError::InvalidState {
                message: format!("Failed to get local address: {e}"),
            })?
            .as_socket()
            .ok_or_else(|| TestBinaryError::InvalidState {
                message: "Socket address is not an IP address".to_string(),
            })?;

        Ok(Self { socket, addr })
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
    pub quic: Option<SocketAddr>,
    pub http: Option<SocketAddr>,
    pub websocket: Option<SocketAddr>,
}

impl PortReserver {
    /// Reserve ports for all protocols enabled in the config.
    pub fn reserve(
        ip_kind: IpAddrKind,
        config: &TestServerConfig,
    ) -> Result<Self, TestBinaryError> {
        let tcp = Some(ReservedPort::tcp(ip_kind)?);

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
            quic,
            http,
            websocket,
        })
    }

    /// Get the bound addresses for environment variable configuration.
    pub fn addresses(&self) -> ProtocolAddresses {
        ProtocolAddresses {
            tcp: self.tcp.as_ref().map(ReservedPort::addr),
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
