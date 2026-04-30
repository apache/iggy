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
