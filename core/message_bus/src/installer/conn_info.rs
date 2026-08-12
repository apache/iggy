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

//! Per-client transport metadata exposed to the caller (`server-ng`).
//!
//! Constructed by the listener / install path on shard 0 with whatever
//! is known at accept time (`client_id`, `peer_addr`, `transport`) and
//! optionally extended post-handshake with TLS / QUIC / WS details. The
//! bus stores it in a side-table keyed by `client_id`; the caller
//! retrieves it via [`crate::IggyMessageBus::client_meta`] when it
//! needs to make per-connection policy decisions (rate limiting,
//! audit, transport-aware command gating).

use std::net::SocketAddr;

/// Wire transport carrying a given client connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClientTransportKind {
    Tcp,
    TcpTls,
    Ws,
    Wss,
    Quic,
}

/// TLS handshake details for [`ClientTransportKind::TcpTls`] and
/// [`ClientTransportKind::Wss`].
///
/// Reserved field: the install path does not populate it today
/// (`tls` stays `None` on the meta record). A future PR will extract
/// SNI + negotiated ALPN from the rustls connection post-handshake.
/// Empty / `None` values will mean the rustls connection did not
/// expose that property (e.g. ALPN was not negotiated).
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct TlsConnectionInfo {
    /// SNI hostname the client sent in `ClientHello`, if any.
    pub sni: Option<String>,
    /// Negotiated ALPN protocol, if any. The bus does not advertise an
    /// ALPN itself; this is whatever the rustls config negotiated.
    pub alpn: Option<Vec<u8>>,
}

/// QUIC connection details for [`ClientTransportKind::Quic`].
///
/// Reserved field: the install path does not populate it today
/// (`quic` stays `None` on the meta record). A future PR will fill
/// the RTT estimate from `Connection::stats()` post-handshake.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct QuicConnectionInfo {
    /// Smoothed round-trip time estimate at install time.
    pub rtt: Option<std::time::Duration>,
}

/// WebSocket upgrade details for [`ClientTransportKind::Ws`] and
/// [`ClientTransportKind::Wss`].
///
/// Reserved field: the install path does not populate it today
/// (`ws` stays `None` on the meta record). A future PR will capture
/// the `User-Agent` from the upgrade request headers.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct WsUpgradeInfo {
    /// `User-Agent` header value, if any. Used for audit.
    pub user_agent: Option<String>,
}

/// Per-connection metadata the bus surfaces to its caller.
///
/// `peer_addr` and `transport` are known at accept time; the optional
/// sub-info structs are populated post-handshake by the corresponding
/// install path (or stay `None` until a future PR wires them).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ClientConnMeta {
    pub client_id: u128,
    pub peer_addr: SocketAddr,
    pub transport: ClientTransportKind,
    pub tls: Option<TlsConnectionInfo>,
    pub quic: Option<QuicConnectionInfo>,
    pub ws: Option<WsUpgradeInfo>,
}

impl ClientConnMeta {
    /// Construct a minimal meta record with only the eagerly-known
    /// fields. Sub-info structs default to `None` and may be filled in
    /// by the install path after the corresponding handshake completes.
    #[must_use]
    pub const fn new(
        client_id: u128,
        peer_addr: SocketAddr,
        transport: ClientTransportKind,
    ) -> Self {
        Self {
            client_id,
            peer_addr,
            transport,
            tls: None,
            quic: None,
            ws: None,
        }
    }
}
