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

//! Per-transport listeners for SDK clients. Each submodule exposes a
//! `bind` + `run` pair following the same shape; only shard 0 binds.
//!
//! Cross-transport invariants:
//!
//! - Accept callbacks own the connection from acceptance; any handshake
//!   work runs in the install path (per [`crate::installer`]) so a slow
//!   peer cannot block the accept loop.
//! - The `on_accepted` callback is responsible for minting a
//!   `client_id`. The plain TCP and pre-upgrade WS paths route to the
//!   owning shard via `ShardFramePayload::ClientConnectionSetup` and
//!   `ClientWsConnectionSetup` respectively. TCP-TLS, WSS, and QUIC
//!   stay shard-0 terminal: their connection state is not serialisable
//!   so the callback installs locally on shard 0 instead of shipping a
//!   setup frame.
//!
//! `bind` signature families:
//!
//! - **plaintext** ([`tcp::bind`], [`ws::bind`]):
//!   `bind(SocketAddr) -> (TcpListener, SocketAddr)`. Listener applies
//!   `TCP_NODELAY` per accepted socket (Linux does not propagate
//!   listener options to accepted sockets).
//! - **tls-bearing** ([`tcp_tls::bind`], [`wss::bind`]):
//!   `bind(SocketAddr, TlsServerCredentials) -> (TcpListener, Arc<rustls::ServerConfig>, SocketAddr)`.
//!   Returns the shared `Arc<rustls::ServerConfig>` so the accept loop
//!   can clone it cheaply per accepted connection. Caller hands the
//!   stream + config to the matching `install_client_tcp_tls` /
//!   `install_client_wss` entry point.
//! - **QUIC** ([`quic::bind`]):
//!   `bind(SocketAddr, compio_quic::ServerConfig) -> (Endpoint, SocketAddr)`.
//!   Single UDP socket demuxes to per-connection `quinn-proto`
//!   state; no plaintext fd to hand cross-shard.
//!
//! # Authentication boundary
//!
//! The accept loops here are deliberately authentication-agnostic.
//! They establish a transport channel and hand the connection to the
//! installer; they never inspect application bytes. Authentication is
//! enforced at exactly one place per connection: the application-level
//! `LOGIN` command, which the dispatcher receives as the first
//! [`crate::Message`] on the in-channel returned by the installer.
//! Until `LOGIN` succeeds, the dispatcher must treat the connection as
//! anonymous and refuse all other commands.
//!
//! Per-transport gating that runs *before* `LOGIN`:
//!
//! - **plain TCP / WS** — none. Anyone who can reach the listener
//!   socket can complete the framing handshake and submit a `LOGIN`
//!   attempt. Operators that need link-level authentication must
//!   front the listener with mTLS or a network policy boundary.
//! - **TCP-TLS / WSS** — server-side TLS only. The listener accepts
//!   `TlsServerCredentials` (cert chain plus key) and builds a
//!   `rustls::ServerConfig` internally with `with_no_client_auth`;
//!   client-cert mTLS is not exposed through the current `bind` API.
//!   Operators needing mTLS must front the listener with a TLS
//!   terminator that enforces it, or extend the bus to accept a
//!   pre-built `Arc<rustls::ServerConfig>`.
//! - **QUIC** — TLS 1.3 handshake gated by a `rustls::ServerConfig`
//!   built the same way as TCP-TLS / WSS (no client auth). No ALPN is
//!   advertised; protocol-version validation lives in the
//!   application-level `LOGIN` command on the caller. 0-RTT data is
//!   structurally rejected by the transport layer (see
//!   [`crate::transports::quic`]).
//!
//! This split is intentional. Adding a pre-`LOGIN` gate would either
//! duplicate state across the transport and dispatcher (drift hazard)
//! or force every transport to parse application frames (layering
//! violation). DoS-shaped abuse is bounded instead by handshake-grace
//! timeouts and the bus-wide [`crate::installer`] backpressure budget.

use crate::{GenericHeader, Message};
use std::rc::Rc;

pub mod quic;
pub mod tcp;
pub mod tcp_tls;
pub mod ws;
pub mod wss;

/// Callback for the owning-shard install path in [`crate::installer`].
/// Dispatches `(client_id, request_message)` into the shard's pipeline.
///
/// Transport-agnostic: every client transport surfaces a parsed
/// `RequestHeader` message with the same handler signature, regardless
/// of whether the wire is plain TCP, TLS, WS, WSS, or QUIC.
pub type RequestHandler = Rc<dyn Fn(u128, Message<GenericHeader>)>;
