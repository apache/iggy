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

//! QUIC listener for consensus-protocol SDK clients.
//!
//! Runs only on shard 0. The accept loop drives the QUIC handshake to
//! completion AND accepts the first bidirectional stream pair before
//! invoking the supplied callback, so the callback receives a
//! ready-for-traffic [`compio_quic::Connection`] plus its
//! `(SendStream, RecvStream)` pair. Subprotocol enforcement runs at
//! the rustls ALPN layer (set to `iggy.consensus.v1` in
//! [`crate::transports::quic::server_config_with_cert`]); no
//! application-layer code runs for clients with the wrong ALPN.
//!
//! 0-RTT data is refused at accept time
//! ([`crate::transports::quic::server_config_with_cert`] sets
//! `max_early_data_size = 0` at the rustls layer; the accept path
//! double-checks `RecvStream::is_0rtt()` for defense in depth and
//! closes with `QUIC_PROTOCOL_VIOLATION` on mismatch).
//!
//! QUIC stays shard-0 terminal: a `compio_quic::Endpoint` binds a
//! single UDP socket and demuxes incoming packets to in-flight
//! connections by Connection ID, and per-connection TLS / packet-number
//! / congestion state lives in `quinn-proto::Connection`, which is
//! non-serialisable. Cross-shard handover is fundamentally out of
//! reach for QUIC; the callback always installs locally on shard 0.

use crate::lifecycle::ShutdownToken;
use crate::transports::quic::QuicTransportListener;
use crate::{AcceptedQuicClientFn, transports::TransportListener};
use compio_quic::{Endpoint, ServerConfig};
use futures::FutureExt;
use iggy_common::IggyError;
use std::net::SocketAddr;
use tracing::{debug, error, info};

/// Bind a QUIC [`Endpoint`] without starting the accept loop.
///
/// `server_config` is built by the bootstrap caller via
/// [`crate::transports::quic::server_config_with_cert`] from the
/// operator-provided cert chain + key DER.
///
/// # Errors
///
/// Returns [`IggyError::CannotBindToSocket`] if the bind fails.
#[allow(clippy::future_not_send)]
pub async fn bind(
    addr: SocketAddr,
    server_config: ServerConfig,
) -> Result<(Endpoint, SocketAddr), IggyError> {
    let endpoint = Endpoint::server(addr, server_config)
        .await
        .map_err(|_| IggyError::CannotBindToSocket(addr.to_string()))?;
    let actual = endpoint
        .local_addr()
        .map_err(|e| IggyError::IoError(e.to_string()))?;
    Ok((endpoint, actual))
}

/// Run the QUIC listener accept loop until the shutdown token fires.
///
/// Each accepted connection is handed to `on_accepted` along with its
/// first bidirectional stream pair. The callback owns the connection
/// from that point on; it mints a `client_id` and calls
/// [`crate::installer::install_client_quic_conn`].
#[allow(clippy::future_not_send)]
pub async fn run(endpoint: Endpoint, token: ShutdownToken, on_accepted: AcceptedQuicClientFn) {
    info!(
        "Consensus QUIC client listener accepting on {:?}",
        endpoint.local_addr().ok()
    );

    let listener = QuicTransportListener::new(endpoint);
    loop {
        futures::select! {
            () = token.wait().fuse() => {
                debug!("Consensus QUIC client listener shutting down");
                break;
            }
            result = listener.accept().fuse() => {
                match result {
                    Ok((conn, peer_addr)) => {
                        debug!(%peer_addr, "QUIC client accepted, handing to installer");
                        let (connection, streams) = conn.into_parts();
                        on_accepted(connection, streams);
                    }
                    Err(e) => {
                        // Endpoint-fatal (e.g. endpoint closed). The
                        // shutdown token will normally fire just before
                        // this; surface the error and exit.
                        error!("Consensus QUIC client listener accept failed: {e}");
                        break;
                    }
                }
            }
        }
    }
}
