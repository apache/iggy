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

//! HTTPS for the shard-0 REST listener.
//!
//! server-ng is thread-per-core `compio/io_uring`, so it cannot reuse the
//! legacy `axum-server` TLS acceptor. The plain-HTTP path uses
//! `cyper_axum::serve`; the TLS path cannot, because that serve loop wraps
//! its IO in a `compio` `Split` (one `BiLock`): hyper parks a pending read
//! holding the lock, so the response write blocks on the same lock forever
//! and 0 bytes are returned. Instead this module drives each connection on a
//! hand-rolled hyper loop over an UNSPLIT [`TlsStream`] via
//! [`cyper_core::HyperStream::new_tls`], which hyper's single-task loop polls
//! read-then-write on one `&mut` - no split, no deadlock.
//!
//! A separate accept pump owns the raw TCP listener and the `compio-tls`
//! acceptor: it accepts plaintext sockets and spawns one handshake task per
//! connection, so a slow peer cannot block the accept loop (the
//! cross-transport invariant in `message_bus::client_listener`). Handshaken
//! streams flow to the serve loop over a bounded channel.

use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_channel::{Receiver, Sender};
use axum::Router;
use compio::net::{TcpListener, TcpStream};
use compio::runtime::JoinHandle;
use compio::tls::{TlsAcceptor, TlsStream};
use configs::http::HttpTlsConfig;
use cyper_core::HyperStream;
use futures::{FutureExt, pin_mut};
use hyper::rt::Executor;
use hyper_util::server::conn::auto::Builder;
use hyper_util::service::TowerToHyperService;
use message_bus::ShutdownToken;
use message_bus::transports::tls::{
    TlsServerCredentials, install_default_crypto_provider, load_pem,
};
use tracing::{debug, error};

use crate::server_error::ServerNgError;

/// hyper's auto-builder serves whichever protocol the client selects via
/// ALPN; advertise the same pair `axum-server` negotiates by default on the
/// legacy HTTP server (there inherited from its default, hand-rolled here):
/// HTTP/2 preferred, HTTP/1.1 fallback.
const ALPN_H2: &[u8] = b"h2";
const ALPN_HTTP11: &[u8] = b"http/1.1";

/// Handshake wall-clock bound for the accept pump. The HTTP listener has no
/// `MessageBusConfig` to source it from, so it mirrors the binary
/// transports' `DEFAULT_HANDSHAKE_GRACE` directly.
const HTTP_TLS_HANDSHAKE_GRACE: Duration = Duration::from_secs(10);

/// Depth of the handshaken-connection channel between the accept pump and
/// the serve loop. The serve loop drains one per accept and immediately
/// spawns a per-connection hyper task, so it rarely fills; the bound keeps
/// a handshake burst from queuing without limit.
const ACCEPT_CHANNEL_DEPTH: usize = 256;

/// A handshaken TLS stream plus the peer it came from, carried from the
/// accept pump to the serve loop.
type Handshaken = (TlsStream<TcpStream>, SocketAddr);

/// Load the HTTPS `ServerConfig` from the operator's PEM cert + key.
///
/// # Errors
///
/// [`ServerNgError::ListenerCredentials`] with `transport: "http.tls"` if
/// the PEM files cannot be read or the certificate / key pair is rejected.
pub fn load_http_tls_server_config(
    tls: &HttpTlsConfig,
) -> Result<Arc<rustls::ServerConfig>, ServerNgError> {
    let credentials =
        load_pem(Path::new(&tls.cert_file), Path::new(&tls.key_file)).map_err(|source| {
            ServerNgError::ListenerCredentials {
                transport: "http.tls",
                source,
            }
        })?;
    build_server_config(credentials)
}

/// Bind the accept pump: build the acceptor, spawn the pump task, and hand
/// back the handshaken-connection receiver for [`serve`] plus the pump's
/// join handle for the caller to track.
pub fn spawn_accept_pump(
    listener: TcpListener,
    config: Arc<rustls::ServerConfig>,
    shutdown: ShutdownToken,
) -> (Receiver<Handshaken>, JoinHandle<()>) {
    let (connections_tx, connections_rx) =
        async_channel::bounded::<Handshaken>(ACCEPT_CHANNEL_DEPTH);
    let acceptor = TlsAcceptor::from(config);
    let pump = compio::runtime::spawn(accept_pump(listener, acceptor, connections_tx, shutdown));
    (connections_rx, pump)
}

/// Drive the HTTPS serve loop until the accept pump closes the channel at
/// shutdown. Each handshaken stream gets its own detached hyper task; on
/// shutdown the pump drops its sender, this loop ends, and the in-flight
/// connection tasks drain via their own shutdown clone.
pub async fn serve(connections: Receiver<Handshaken>, router: Router, shutdown: ShutdownToken) {
    while let Ok((tls, peer)) = connections.recv().await {
        let router = router.clone();
        let shutdown = shutdown.clone();
        compio::runtime::spawn(serve_connection(tls, peer, router, shutdown)).detach();
    }
}

/// Serve one HTTPS connection: wrap the unsplit [`TlsStream`] in a duplex-safe
/// [`HyperStream`] and run hyper's auto (h1/h2) connection to completion,
/// switching to a graceful shutdown when the bus shutdown fires.
async fn serve_connection(
    tls: TlsStream<TcpStream>,
    peer: SocketAddr,
    router: Router,
    shutdown: ShutdownToken,
) {
    let io = HyperStream::new_tls(tls);
    // `Router<()>` already maps the incoming body to axum's `Body` in its own
    // `Service` impl, so it serves hyper's `Request<Incoming>` directly - no
    // `map_request` shim. `with_state(())` finalizes the routes eagerly.
    let service = TowerToHyperService::new(router.with_state(()));
    let builder = Builder::new(LocalExecutor);
    // `serve_connection_with_upgrades` borrows `builder`, so it must outlive
    // `conn`; keep it bound rather than inlined.
    let conn = builder.serve_connection_with_upgrades(Box::pin(io), service);
    pin_mut!(conn);

    // Fire the graceful-shutdown transition exactly once: a fresh
    // `shutdown.wait()` per loop turn would re-resolve immediately (recv on a
    // closed channel), so pin one fused future and poll it by `&mut`.
    let shutdown = shutdown.wait().fuse();
    pin_mut!(shutdown);
    loop {
        futures::select! {
            result = conn.as_mut().fuse() => {
                if let Err(error) = result {
                    debug!(%peer, %error, "server-ng HTTPS connection terminated with error");
                }
                break;
            }
            () = &mut shutdown => conn.as_mut().graceful_shutdown(),
        }
    }
}

/// Single-thread executor for hyper's per-connection tasks. Unlike
/// `cyper_core::CompioExecutor` it puts no `Send` bound on the spawned
/// future, so the shard-local (non-`Send`) axum service future drives
/// directly without a `SendWrapper` shim. Spawns onto the current shard's
/// compio runtime.
#[derive(Clone)]
struct LocalExecutor;

impl<F: Future<Output = ()> + 'static> Executor<F> for LocalExecutor {
    fn execute(&self, fut: F) {
        compio::runtime::spawn(fut).detach();
    }
}

/// Turn credentials into an ALPN-configured `ServerConfig`. Mirrors the
/// binary TCP-TLS path (`with_no_client_auth`, `max_early_data_size = 0`);
/// the only HTTP-specific step is the ALPN advertisement.
fn build_server_config(
    credentials: TlsServerCredentials,
) -> Result<Arc<rustls::ServerConfig>, ServerNgError> {
    install_default_crypto_provider();
    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(credentials.cert_chain, credentials.key_der)
        .map_err(|error| ServerNgError::ListenerCredentials {
            transport: "http.tls",
            source: std::io::Error::other(format!(
                "http TLS server config rejected credentials: {error}"
            )),
        })?;
    config.max_early_data_size = 0;
    config.alpn_protocols = vec![ALPN_H2.to_vec(), ALPN_HTTP11.to_vec()];
    Ok(Arc::new(config))
}

/// Accept plaintext sockets and dispatch each to its own handshake task
/// until shutdown. On shutdown the sole sender drops, closing the channel.
async fn accept_pump(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    connections: Sender<Handshaken>,
    shutdown: ShutdownToken,
) {
    loop {
        futures::select! {
            () = shutdown.wait().fuse() => {
                debug!("server-ng HTTPS accept pump shutting down");
                break;
            }
            result = listener.accept().fuse() => match result {
                Ok((stream, peer)) => spawn_handshake(&acceptor, &connections, stream, peer),
                Err(error) => error!(%error, "server-ng HTTPS accept failed"),
            },
        }
    }
}

/// Run one connection's TLS handshake in a detached, time-bounded task and,
/// on success, forward the stream to the serve loop. A handshake that times
/// out or errors is logged at debug and dropped; it never blocks the accept
/// loop.
fn spawn_handshake(
    acceptor: &TlsAcceptor,
    connections: &Sender<Handshaken>,
    stream: TcpStream,
    peer: SocketAddr,
) {
    let acceptor = acceptor.clone();
    let connections = connections.clone();
    compio::runtime::spawn(async move {
        match compio::time::timeout(HTTP_TLS_HANDSHAKE_GRACE, acceptor.accept(stream)).await {
            Ok(Ok(tls)) => {
                // Drop on send error: the channel is closed only at
                // shutdown, when the serve loop is already tearing down.
                let _ = connections.send((tls, peer)).await;
            }
            Ok(Err(error)) => debug!(%peer, %error, "server-ng HTTPS handshake failed"),
            Err(_elapsed) => {
                debug!(%peer, grace = ?HTTP_TLS_HANDSHAKE_GRACE, "server-ng HTTPS handshake timed out");
            }
        }
    })
    .detach();
}

#[cfg(test)]
mod tests {
    use super::*;
    use message_bus::transports::tls::self_signed_for_loopback;

    #[test]
    fn server_config_advertises_h2_and_http11_alpn() {
        let config =
            build_server_config(self_signed_for_loopback()).expect("self-signed cert builds");
        assert_eq!(
            config.alpn_protocols,
            vec![b"h2".to_vec(), b"http/1.1".to_vec()]
        );
    }
}
