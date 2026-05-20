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

//! QUIC listener for SDK clients.
//!
//! Runs only on shard 0. The accept loop drives the QUIC handshake to
//! completion AND accepts the first bidirectional stream pair before
//! invoking the supplied callback, so the callback receives a
//! ready-for-traffic [`compio_quic::Connection`] plus its
//! `(SendStream, RecvStream)` pair. No ALPN is advertised; protocol
//! version is validated by the caller (server-ng) inside the LOGIN
//! command.
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
use crate::transports::quic::{QuicTransportListener, accept_handshake};
use crate::{AcceptedQuicClientFn, AcceptedQuicConn};
use compio::runtime::JoinHandle;
use compio_quic::{Endpoint, ServerConfig};
use futures::FutureExt;
use iggy_common::IggyError;
use std::net::SocketAddr;
use std::time::Duration;
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
/// Each [`Incoming`](compio_quic::Incoming) is handed to its own spawned
/// task that drives the handshake and the first bidirectional stream
/// pair via [`accept_handshake`]. On success the task invokes
/// `on_accepted` with the ready-for-traffic
/// [`compio_quic::Connection`] plus its `(SendStream, RecvStream)`. The
/// callback mints a `client_id` and calls
/// [`crate::installer::install_client_quic`].
///
/// Spawning per-incoming is load-bearing: a single hostile or merely
/// slow peer would otherwise hold the entire client plane behind its
/// `connecting.await` or `accept_bi().await`, and subsequent peers
/// would not be served until the wedged peer's idle timeout expired.
///
/// On shutdown the loop breaks and every still-running handshake task
/// is awaited. Dropping the listener at function exit drops the
/// underlying [`Endpoint`], which causes `connecting`/`accept_bi`
/// inside the spawned tasks to fail promptly so the join completes.
///
/// `handshake_grace` bounds the wall-clock time each spawned handshake
/// task may take to reach the ready-for-traffic state. Threaded from
/// `MessageBusConfig::handshake_grace` by the bootstrap caller.
#[allow(clippy::future_not_send)]
pub async fn run(
    endpoint: Endpoint,
    token: ShutdownToken,
    on_accepted: AcceptedQuicClientFn,
    handshake_grace: Duration,
) {
    info!(
        "Client listener (QUIC) accepting on {:?}",
        endpoint.local_addr().ok()
    );

    let listener = QuicTransportListener::new(endpoint);
    let mut handshake_handles: Vec<JoinHandle<()>> = Vec::new();
    loop {
        // Drop completed handles before blocking on the next incoming so
        // the vector stays bounded under sustained QUIC connect attempts.
        // Without this sweep, every accepted connection's `JoinHandle`
        // would persist for the listener's lifetime, leaking task-table
        // slots + scopeguard state proportional to total accepted
        // connections since boot. Dropping a finished `Task` is a no-op
        // (no in-flight work to cancel).
        handshake_handles.retain(|h| !h.is_finished());

        futures::select! {
            () = token.wait().fuse() => {
                debug!("Client listener (QUIC) shutting down");
                break;
            }
            result = listener.next_incoming().fuse() => {
                match result {
                    Ok(incoming) => {
                        let on_accepted = on_accepted.clone();
                        let handle = compio::runtime::spawn(async move {
                            let Some((conn, peer_addr)) =
                                accept_handshake(incoming, handshake_grace).await
                            else {
                                return;
                            };
                            debug!(%peer_addr, "QUIC client accepted, handing to installer");
                            let (connection, streams) = conn.into_parts();
                            on_accepted(AcceptedQuicConn::new(connection, streams));
                        });
                        handshake_handles.push(handle);
                    }
                    Err(e) => {
                        // Endpoint-fatal (e.g. endpoint closed). The
                        // shutdown token will normally fire just before
                        // this; surface the error and exit.
                        error!("Client listener (QUIC) accept failed: {e}");
                        break;
                    }
                }
            }
        }
    }

    for handle in handshake_handles {
        let _ = handle.await;
    }
}
