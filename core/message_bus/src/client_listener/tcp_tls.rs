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

//! TCP-TLS listener for SDK clients.
//!
//! Runs only on shard 0. The accept loop performs no TLS work: it accepts
//! the underlying [`compio::net::TcpStream`] and hands it together with
//! a clone of [`Arc<rustls::ServerConfig>`] to the supplied callback.
//! The TLS handshake runs INSIDE the install path
//! ([`crate::installer::install_client_tcp_tls`]) so a slow or
//! malicious peer cannot block subsequent accepts.
//!
//! TCP-TLS is shard-0 terminal. Pre-handshake the fd is plain TCP and
//! could in principle be dup'd to another shard, but the receiving shard
//! would then have to perform the rustls handshake against a config that
//! lives on shard 0 — losing the point of the cross-shard handover.
//! Post-handshake the rustls connection state holds per-record sequence
//! numbers, key schedule, and write buffers tied to the local task, with
//! no dupable plaintext fd.
//!
//! The TLS plane structurally cannot preserve `Frozen<MESSAGE_ALIGN>`
//! ownership: rustls's encrypt step copies plaintext bytes into the
//! outbound ciphertext buffer, so the zero-copy guarantee on plaintext
//! TCP does not carry over.

use crate::AcceptedTlsClientFn;
use crate::lifecycle::ShutdownToken;
use crate::transports::tls::{TlsServerCredentials, install_default_crypto_provider};
use compio::net::{SocketOpts, TcpListener};
use futures::FutureExt;
use iggy_common::IggyError;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{debug, error, info};

/// Bind the TCP-TLS listener and pre-build the shared
/// [`Arc<rustls::ServerConfig>`] from the supplied credentials.
///
/// The returned config is cloned into every accepted connection so each
/// install path drives its own `UnbufferedServerConnection` against the
/// same key material. A single `ServerConfig` allocation is shared across
/// all clients.
///
/// Like the WS pre-upgrade listener, `TCP_NODELAY` is applied at bind
/// time; the install path re-applies on every accepted stream because
/// Linux does not propagate listener options to accepted sockets.
/// `SO_KEEPALIVE` is intentionally NOT set: see
/// `crate::socket_opts` and the `[message_bus]` schema rationale at
/// `core/configs/src/server_ng_config/message_bus.rs:49-52` (SDK
/// clients manage their own keepalive policy; replica<->replica
/// liveness is observed by VSR heartbeats).
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
        .map_err(|e| IggyError::IoError(format!("TCP-TLS server config build failed: {e}")))?;
    // Defense-in-depth alongside the quic plane: TLS 1.3 0-RTT is off by
    // default for the in-process TCP-TLS transport. The driver does not
    // expose an early-data API; pin to zero so a future rustls default
    // cannot enable it accidentally.
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

/// Run the TCP-TLS listener accept loop until the shutdown token fires.
///
/// Each accepted [`compio::net::TcpStream`] is handed to `on_accepted`
/// together with a clone of the shared [`Arc<rustls::ServerConfig>`].
/// The callback owns the stream from that point on; production wiring
/// routes through shard 0's coordinator (mints a `client_id`, builds
/// the install context, calls
/// [`crate::installer::install_client_tcp_tls`]).
#[allow(clippy::future_not_send)]
pub async fn run(
    listener: TcpListener,
    config: Arc<rustls::ServerConfig>,
    token: ShutdownToken,
    on_accepted: AcceptedTlsClientFn,
) {
    info!(
        "Client listener (TCP-TLS) accepting on {:?}",
        listener.local_addr().ok()
    );

    loop {
        futures::select! {
            () = token.wait().fuse() => {
                debug!("Client listener (TCP-TLS) shutting down");
                break;
            }
            result = listener.accept().fuse() => {
                match result {
                    Ok((stream, peer_addr)) => {
                        debug!(
                            %peer_addr,
                            "TCP-TLS client accepted, handing to installer"
                        );
                        on_accepted(stream, Arc::clone(&config));
                    }
                    Err(e) => {
                        error!("Client listener (TCP-TLS) accept failed: {e}");
                    }
                }
            }
        }
    }
}
