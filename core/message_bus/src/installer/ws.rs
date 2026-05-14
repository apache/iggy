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

//! WebSocket client install path (post HTTP-Upgrade).

use super::conn_info::ClientConnMeta;
use super::tcp::install_client_conn;
use crate::IggyMessageBus;
use crate::client_listener::RequestHandler;
use crate::transports::ws::WsTransportConn;
use compio::net::TcpStream;
use std::rc::Rc;

/// WebSocket entry point for client installs.
///
/// Wraps a post-upgrade [`compio_ws::WebSocketStream`] in a
/// [`WsTransportConn`] and delegates to the existing generic
/// [`install_client_conn`]. The HTTP-Upgrade handshake has already been
/// driven on shard 0; the install path never re-runs it.
///
/// Like QUIC, this never crosses an inter-shard channel:
/// `WebSocketStream<TcpStream>` is `!Send` (it holds compio `Rc<...>`
/// driver state) so shard 0 terminates locally and uses the existing
/// `ForwardClientSend` / `Consensus` variants. The post-upgrade socket
/// IS still a raw TCP fd, but compio-ws has no "rebuild from fd" API
/// and re-attaching the tungstenite state machine across shards would
/// lose protocol invariants the dispatcher already enforced.
#[allow(clippy::future_not_send)]
pub fn install_client_ws(
    bus: &Rc<IggyMessageBus>,
    meta: ClientConnMeta,
    stream: compio_ws::WebSocketStream<TcpStream>,
    on_request: RequestHandler,
) {
    let cfg = bus.config();
    install_client_conn(
        bus,
        meta,
        WsTransportConn::new_server(stream).with_close_grace(cfg.close_grace),
        on_request,
    );
}
