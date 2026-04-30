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

//! TCP-TLS client install path.

use super::conn_info::ClientConnMeta;
use super::tcp::install_client_conn;
use crate::IggyMessageBus;
use crate::client_listener::RequestHandler;
use crate::socket_opts::apply_nodelay_for_connection;
use crate::transports::tcp_tls::TcpTlsTransportConn;
use compio::net::TcpStream;
use std::rc::Rc;
use std::sync::Arc;
use tracing::warn;

/// TCP-TLS entry point for client installs.
///
/// Wraps a freshly accepted plaintext [`TcpStream`] (TLS handshake has
/// NOT run yet) plus the shared [`Arc<rustls::ServerConfig>`] in a
/// [`TcpTlsTransportConn`] and delegates to the existing generic
/// [`install_client_conn`]. The rustls handshake then runs inside the
/// transport's `run` body on the per-connection install task, so a slow
/// or malicious peer never blocks the listener accept loop.
///
/// `TCP_NODELAY` is applied to the underlying TCP socket before the
/// handshake starts, matching [`super::tcp::install_client_tcp`]'s
/// plaintext behaviour. Linux does not propagate it from the listener
/// to accepted sockets, so toggling here is required. `SO_KEEPALIVE`
/// is intentionally NOT set; see `crate::socket_opts` and the
/// `[message_bus]` schema rationale at
/// `core/configs/src/server_ng_config/message_bus.rs:49-52`.
///
/// TCP-TLS is shard-0 terminal: the rustls connection state machine
/// is non-serialisable and tied to the local task; pre-handshake the
/// fd is plain TCP and could in principle be dup'd to another shard,
/// but the receiving shard would then have to re-handshake against
/// shard-0-resident key material — losing the point of the cross-shard
/// handover.
#[allow(clippy::future_not_send)]
pub fn install_client_tcp_tls(
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
            "nodelay failed on accepted TLS client fd: {e}"
        );
    }
    install_client_conn(
        bus,
        meta,
        TcpTlsTransportConn::new_server(stream, config)
            .with_close_grace(cfg.close_grace)
            .with_handshake_grace(cfg.handshake_grace),
        on_request,
    );
}
