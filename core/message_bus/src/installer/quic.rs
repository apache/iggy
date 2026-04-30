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

//! QUIC client install path.

use super::conn_info::ClientConnMeta;
use super::tcp::install_client_conn;
use crate::client_listener::RequestHandler;
use crate::transports::quic::QuicTransportConn;
use crate::{AcceptedQuicConn, IggyMessageBus};
use std::rc::Rc;

/// QUIC entry point for client installs.
///
/// Takes the [`AcceptedQuicConn`] produced by shard 0's QUIC listener
/// (already through the QUIC handshake AND the first bidirectional
/// `accept_bi` so the install path never re-handshakes), unwraps it
/// internally, wraps the parts in a [`QuicTransportConn`], and
/// delegates to the existing generic [`install_client_conn`].
///
/// Accepting [`AcceptedQuicConn`] (rather than the raw `compio_quic`
/// types it carries) keeps `compio_quic`'s concrete types out of the
/// bus's public `SemVer` surface: a future `compio_quic` bump that
/// renames or restructures `Connection` / `SendStream` / `RecvStream`
/// does not change this signature.
///
/// No socket-options analog runs here: QUIC keepalive lives in
/// [`compio_quic::TransportConfig::keep_alive_interval`] set at endpoint
/// construction time. The connection is encrypted end-to-end and there
/// is no plaintext fd to dup, which is why this never crosses an
/// inter-shard channel: shard 0 owns the QUIC `Endpoint`, terminates
/// every connection locally, and uses the existing
/// `ForwardClientSend` / `Consensus` shard-frame variants for outbound
/// + inbound traffic respectively.
#[allow(clippy::future_not_send)]
pub fn install_client_quic(
    bus: &Rc<IggyMessageBus>,
    meta: ClientConnMeta,
    accepted: AcceptedQuicConn,
    on_request: RequestHandler,
) {
    let (connection, streams) = accepted.into_parts();
    install_client_conn(
        bus,
        meta,
        QuicTransportConn::new(connection, streams),
        on_request,
    );
}
