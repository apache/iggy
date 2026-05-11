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

//! WSS (WebSocket-over-TLS) listener for SDK clients.
//!
//! Runs only on shard 0. The accept loop performs no TLS or HTTP-Upgrade
//! work: it accepts the underlying [`compio::net::TcpStream`] and hands
//! it together with a clone of [`Arc<rustls::ServerConfig>`] to the
//! supplied callback. Both the rustls handshake and the WebSocket
//! HTTP-Upgrade handshake run INSIDE the install path
//! ([`crate::installer::install_client_wss`] →
//! [`crate::transports::wss::WssTransportConn`]'s `run` method) so a
//! slow or malicious peer cannot block subsequent accepts.
//!
//! WSS is shard-0 terminal. The plain TCP fd that exists pre-handshake
//! could in principle be dup'd to another shard, but the receiving
//! shard would then have to perform both the TLS handshake and the WS
//! upgrade against state that lives on shard 0 — losing the point of
//! the cross-shard handover. Post-upgrade the rustls + tungstenite
//! state machines hold per-record sequence numbers, key schedule,
//! masking state, and write buffers tied to the local task, with no
//! dupable plaintext fd.
//!
//! Like the TCP-TLS plane, WSS structurally cannot preserve
//! `Frozen<MESSAGE_ALIGN>` ownership: rustls's encrypt step copies
//! plaintext bytes into the outbound ciphertext buffer, and the WS
//! frame builder concatenates header + body into a per-frame scratch
//! before that copy. The zero-copy guarantee on plaintext TCP does not
//! carry over.
//!
use crate::AcceptedWssClientFn;
use crate::lifecycle::ShutdownToken;
use crate::transports::tls::{TlsServerCredentials, install_default_crypto_provider};
use compio::net::{SocketOpts, TcpListener};
use futures::FutureExt;
use iggy_common::IggyError;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{debug, error, info};

/// Bind the WSS listener and pre-build the shared
/// [`Arc<rustls::ServerConfig>`] from the supplied credentials.
///
/// Identical pattern to [`crate::client_listener::tcp_tls::bind`]: a single
/// `ServerConfig` allocation is shared across all clients via
/// [`Arc::clone`] on each accept.
///
/// # Errors
///
/// - [`IggyError::IoError`] if the rustls server config cannot be built
///   from `credentials` (cert / key mismatch).
/// - [`IggyError::CannotBindToSocket`] if the TCP bind fails.
#[allow(clippy::future_not_send)]
pub async fn bind(
    addr: SocketAddr,
    credentials: TlsServerCredentials,
) -> Result<(TcpListener, Arc<rustls::ServerConfig>, SocketAddr), IggyError> {
    install_default_crypto_provider();
    let mut cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(credentials.cert_chain, credentials.key_der)
        .map_err(|e| IggyError::IoError(format!("WSS server config build failed: {e}")))?;
    cfg.max_early_data_size = 0;

    let opts = SocketOpts::new().nodelay(true);
    let listener = TcpListener::bind_with_options(addr, &opts)
        .await
        .map_err(|_| IggyError::CannotBindToSocket(addr.to_string()))?;
    let actual = listener
        .local_addr()
        .map_err(|e| IggyError::IoError(e.to_string()))?;
    Ok((listener, Arc::new(cfg), actual))
}

/// Run the WSS listener accept loop until the shutdown token fires.
///
/// Each accepted [`compio::net::TcpStream`] is handed to `on_accepted`
/// together with a clone of the shared [`Arc<rustls::ServerConfig>`].
/// The callback owns the stream from that point on; production wiring
/// routes through shard 0's coordinator (mints a `client_id`, builds
/// the install context, calls
/// [`crate::installer::install_client_wss`]).
#[allow(clippy::future_not_send)]
pub async fn run(
    listener: TcpListener,
    config: Arc<rustls::ServerConfig>,
    token: ShutdownToken,
    on_accepted: AcceptedWssClientFn,
) {
    info!(
        "Client listener (WSS) accepting on {:?}",
        listener.local_addr().ok()
    );

    loop {
        futures::select! {
            () = token.wait().fuse() => {
                debug!("Client listener (WSS) shutting down");
                break;
            }
            result = listener.accept().fuse() => {
                match result {
                    Ok((stream, peer_addr)) => {
                        debug!(
                            %peer_addr,
                            "WSS client accepted, handing to installer"
                        );
                        on_accepted(stream, Arc::clone(&config));
                    }
                    Err(e) => {
                        error!("Client listener (WSS) accept failed: {e}");
                    }
                }
            }
        }
    }
}
