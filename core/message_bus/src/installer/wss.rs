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

//! WSS (WebSocket-over-TLS) client install path.

use super::conn_info::ClientConnMeta;
use super::tcp::install_client_conn;
use crate::IggyMessageBus;
use crate::client_listener::RequestHandler;
use crate::socket_opts::apply_nodelay_for_connection;
use crate::transports::wss::WssTransportConn;
use compio::net::TcpStream;
use std::rc::Rc;
use std::sync::Arc;
use tracing::warn;

/// WSS entry point for client installs.
///
/// Wraps a freshly accepted plaintext [`TcpStream`] (neither the TLS
/// handshake nor the WS HTTP-Upgrade has run yet) plus the shared
/// [`Arc<rustls::ServerConfig>`] in a [`WssTransportConn`] and delegates
/// to the existing generic [`install_client_conn`]. Both handshakes
/// then run inside the transport's `run` body on the per-connection
/// install task; the install path stays thin. No subprotocol
/// negotiation: client identity is established post-handshake by the
/// LOGIN command on the caller (server-ng).
///
/// `TCP_NODELAY` is applied pre-handshake for symmetry with
/// [`super::tcp_tls::install_client_tcp_tls`]. `SO_KEEPALIVE` is
/// intentionally NOT set; see [`crate::socket_opts`].
///
/// WSS is shard-0 terminal for the same reasons as the TCP-TLS plane;
/// see [`super::tcp_tls::install_client_tcp_tls`] for the rustls
/// non-serialisability argument.
#[allow(clippy::future_not_send)]
pub fn install_client_wss(
    bus: &Rc<IggyMessageBus>,
    meta: ClientConnMeta,
    stream: TcpStream,
    config: Arc<rustls::ServerConfig>,
    on_request: RequestHandler,
) {
    let cfg = bus.config();
    let client_id = meta.client_id;
    if let Err(e) = apply_nodelay_for_connection(&stream) {
        warn!(
            client = client_id,
            "nodelay failed on accepted WSS client fd: {e}"
        );
    }
    install_client_conn(
        bus,
        meta,
        WssTransportConn::new_server(stream, config)
            .with_close_grace(cfg.close_grace)
            .with_handshake_grace(cfg.handshake_grace)
            .with_ws_config(cfg.ws_config),
        on_request,
    );
}
