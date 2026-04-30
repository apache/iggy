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

//! In-process WSS transport: `compio_ws::WebSocketStream` over a
//! `compio_tls::TlsStream<TcpStream>`, driven by a single connection
//! task per peer.
//!
//! # Architecture
//!
//! Replaces the prior hand-rolled rustls `UnbufferedConnection` driver +
//! manual WebSocket frame parser with the compio-native stack:
//!
//! 1. TCP accept (already done by listener) gives us a `TcpStream`.
//! 2. `compio_tls::TlsAcceptor::accept` (server) or `TlsConnector::connect`
//!    (client) drives the rustls handshake.
//! 3. `compio_ws::accept_async` (server) or `compio_ws::client_async`
//!    (client) runs the WebSocket HTTP-Upgrade exchange over the
//!    encrypted channel. No subprotocol negotiation: the bus accepts any
//!    client and lets the post-handshake LOGIN command in the caller
//!    establish identity.
//! 4. The resulting `WebSocketStream<TcpStream>` (where the inner
//!    `MaybeTlsStream<TcpStream>` is the `Tls` variant) is owned by a
//!    single connection task that `select!`s over the bus shutdown
//!    token, the outbound mailbox, and `ws.read()`.
//!
//! # Cancel safety
//!
//! Same `compio_ws` 0.3.1 narrow hazard as `super::ws`:
//! `WebSocketStream::read` awaits on `fill_read_buf` (cancel-safe; bytes
//! survive in the underlying compio-tls `AsyncStream`'s read buffer and
//! tungstenite's parser state) or on `flush` after a successful sync
//! decode (drops drop the just-decoded `Message`, parser has advanced).
//! Pure consensus traffic on a healthy connection has an empty write
//! buffer and `flush` resolves synchronously. See `super::ws` for the
//! full analysis and the `compio_ws` 0.4 fix path. See `super::tcp_tls`
//! for the TLS-layer rationale.
//!
//! Sends are run to completion outside any `select!`. The token signals
//! the *start* of close, never cancels an in-flight send.
//!
//! # Frozen ownership
//!
//! Each outbound consensus frame is wrapped in a single
//! `Message::Binary(Bytes)`. Tungstenite copies the payload through its
//! frame buffer, and rustls copies plaintext into ciphertext records:
//! two structural copies on the WSS plane (the same as the prior
//! hand-rolled implementation).

use super::tls::TlsRole;
use super::ws::decode_consensus_frame;
use super::{ActorContext, TransportConn};
use crate::lifecycle::BusMessage;
use bytes::Bytes;
use compio::net::TcpStream;
use compio::tls::{TlsAcceptor, TlsConnector, TlsStream};
use compio::ws::tungstenite::{self, Message as WsMessage, protocol::WebSocketConfig};
use compio::ws::{WebSocketStream, accept_async_with_config, client_async_with_config};
use futures::FutureExt;
use rustls::pki_types::ServerName;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Default wall-clock bound on the WS Close + TLS shutdown sequence.
const DEFAULT_CLOSE_GRACE: Duration = Duration::from_secs(2);

/// Default wall-clock bound on the combined TLS + WS handshake
/// sequence. Mirrors [`crate::MessageBusConfig::handshake_grace`]; set
/// independently here so test / single-conn callers that do not go
/// through the installer get the same DoS-hardening default.
const DEFAULT_HANDSHAKE_GRACE: Duration = Duration::from_secs(10);

/// In-process WSS transport: a [`TcpStream`] paired with a role-specific
/// rustls configuration. The WebSocket layer rides on top of the TLS
/// layer, both driven by `compio-tls` / `compio-ws`.
pub struct WssTransportConn {
    stream: TcpStream,
    role: TlsRole,
    close_grace: Duration,
    handshake_grace: Duration,
    ws_config: WebSocketConfig,
}

impl WssTransportConn {
    #[must_use]
    pub fn new_server(stream: TcpStream, config: Arc<rustls::ServerConfig>) -> Self {
        Self {
            stream,
            role: TlsRole::Server(config),
            close_grace: DEFAULT_CLOSE_GRACE,
            handshake_grace: DEFAULT_HANDSHAKE_GRACE,
            ws_config: WebSocketConfig::default(),
        }
    }

    #[must_use]
    pub fn new_client(
        stream: TcpStream,
        config: Arc<rustls::ClientConfig>,
        server_name: ServerName<'static>,
    ) -> Self {
        Self {
            stream,
            role: TlsRole::Client {
                config,
                server_name,
            },
            close_grace: DEFAULT_CLOSE_GRACE,
            handshake_grace: DEFAULT_HANDSHAKE_GRACE,
            ws_config: WebSocketConfig::default(),
        }
    }

    /// Override the wall-clock bound on the close sequence
    /// (`ws.close` + bounded TLS shutdown).
    #[must_use]
    pub const fn with_close_grace(mut self, close_grace: Duration) -> Self {
        self.close_grace = close_grace;
        self
    }

    /// Override the wall-clock bound shared across the TLS + WS
    /// handshakes. The deadline is computed once at the start of `run`
    /// (`Instant::now() + handshake_grace`); each handshake step
    /// consumes the remainder so the cumulative pre-pump time is
    /// bounded regardless of how the budget splits.
    #[must_use]
    pub const fn with_handshake_grace(mut self, handshake_grace: Duration) -> Self {
        self.handshake_grace = handshake_grace;
        self
    }

    /// Override the WebSocket frame-layer config (read/write buffer
    /// sizes, max frame / message size, accept-unmasked-frames flag).
    #[must_use]
    pub const fn with_ws_config(mut self, ws_config: WebSocketConfig) -> Self {
        self.ws_config = ws_config;
        self
    }
}

impl TransportConn for WssTransportConn {
    #[allow(clippy::future_not_send)]
    async fn run(self, ctx: ActorContext) {
        let label = ctx.label;
        let peer = ctx.peer.clone();

        let role = self.role;
        let is_server = matches!(role, TlsRole::Server(_));
        let ws_config = self.ws_config;
        let handshake_grace = self.handshake_grace;

        // Single wall-clock budget shared across both handshakes so a
        // peer cannot stall the bus by spending almost-the-full grace
        // on TLS and another full grace on the WS upgrade.
        let deadline = Instant::now() + handshake_grace;

        // 1. TLS handshake. Box::pin keeps the outer `run` future small;
        // both inner handshake futures are large (rustls + tungstenite
        // state machines) and would otherwise push `run`'s state past
        // the `clippy::large_futures` threshold.
        let tls_stream = match compio::time::timeout(
            handshake_grace,
            Box::pin(tls_handshake(role, self.stream)),
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                warn!(
                    %label,
                    %peer,
                    error = ?e,
                    "WSS: TLS handshake failed"
                );
                return;
            }
            Err(_elapsed) => {
                warn!(
                    %label,
                    %peer,
                    grace = ?handshake_grace,
                    "WSS: TLS handshake exceeded handshake_grace"
                );
                return;
            }
        };

        // 2. WebSocket HTTP-Upgrade over the encrypted channel.
        // Remainder budget; if the TLS step consumed it all, fail fast.
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            warn!(
                %label,
                %peer,
                "WSS: handshake budget exhausted by TLS step before WS upgrade"
            );
            return;
        }
        let mut ws = match compio::time::timeout(
            remaining,
            Box::pin(ws_handshake(tls_stream, is_server, &peer, ws_config)),
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                warn!(
                    %label,
                    %peer,
                    error = ?e,
                    "WSS: WebSocket upgrade failed"
                );
                return;
            }
            Err(_elapsed) => {
                warn!(
                    %label,
                    %peer,
                    remaining = ?remaining,
                    "WSS: WebSocket upgrade exceeded remaining handshake_grace"
                );
                return;
            }
        };

        // 3. Steady-state pump.
        run_pump(&mut ws, ctx).await;

        // 4. Cooperative close: WS Close frame + bounded TLS shutdown.
        drive_close(&mut ws, self.close_grace, label, &peer).await;
    }
}

/// Run the role-specific rustls handshake on `stream` and return the
/// encrypted [`TlsStream`].
#[allow(clippy::future_not_send)]
async fn tls_handshake(role: TlsRole, stream: TcpStream) -> std::io::Result<TlsStream<TcpStream>> {
    match role {
        TlsRole::Server(cfg) => TlsAcceptor::from(cfg).accept(stream).await,
        TlsRole::Client {
            config,
            server_name,
        } => {
            let domain = server_name.to_str();
            TlsConnector::from(config).connect(&domain, stream).await
        }
    }
}

/// Drive the WebSocket HTTP-Upgrade exchange over the TLS-protected
/// channel.
///
/// Server: plain `accept_async_with_config`; the bus does not negotiate
/// any subprotocol. Client identity is established post-handshake by
/// the caller via the LOGIN command.
///
/// Client: sends an Upgrade request to `wss://localhost/`, validates
/// the 101 response.
#[allow(clippy::future_not_send)]
async fn ws_handshake(
    tls_stream: TlsStream<TcpStream>,
    is_server: bool,
    peer: &str,
    ws_config: WebSocketConfig,
) -> Result<WebSocketStream<TlsStream<TcpStream>>, tungstenite::Error> {
    if is_server {
        accept_async_with_config(tls_stream, ws_config).await
    } else {
        let request = client_request(peer)?;
        let (ws, _resp) = client_async_with_config(request, tls_stream, ws_config).await?;
        Ok(ws)
    }
}

/// Build the client-side Upgrade request: `wss://{peer}/`.
fn client_request(peer: &str) -> Result<tungstenite::http::Request<()>, tungstenite::Error> {
    use compio::ws::tungstenite::client::IntoClientRequest;

    // The peer field is a free-form trace label from the bus; for the
    // request URI fall back to "localhost" so tungstenite's host
    // validation does not reject exotic identifiers (replica ids,
    // hex-encoded client ids).
    let host = if peer.is_empty() || !peer.chars().all(is_hostname_char) {
        "localhost"
    } else {
        peer
    };
    let uri = format!("wss://{host}/");
    uri.into_client_request()
}

const fn is_hostname_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-' || c == '.' || c == ':'
}

/// Per-iteration outcome of the single-task select.
enum PumpAction {
    Shutdown,
    Send(BusMessage),
    Recv(Result<WsMessage, tungstenite::Error>),
    MailboxClosed,
}

/// Drive the WSS connection until shutdown, peer Close, or an
/// unrecoverable error.
#[allow(clippy::future_not_send)]
async fn run_pump(ws: &mut WebSocketStream<TlsStream<TcpStream>>, ctx: ActorContext) {
    let ActorContext {
        in_tx,
        rx,
        shutdown,
        max_batch,
        label,
        peer,
        ..
    } = ctx;
    let mut shutdown_fut = Box::pin(shutdown.wait().fuse());
    let mut batch: Vec<BusMessage> = Vec::with_capacity(max_batch);

    loop {
        let action = {
            let read_fut = ws.read();
            let recv_fut = rx.recv();
            futures::pin_mut!(read_fut);
            futures::pin_mut!(recv_fut);

            futures::select_biased! {
                () = shutdown_fut.as_mut() => PumpAction::Shutdown,
                msg = recv_fut.fuse() => msg.map_or(PumpAction::MailboxClosed, PumpAction::Send),
                res = read_fut.fuse() => PumpAction::Recv(res),
            }
        };

        match action {
            PumpAction::Shutdown => {
                debug!(%label, %peer, "wss pump: shutdown observed");
                return;
            }
            PumpAction::MailboxClosed => {
                debug!(%label, %peer, "wss pump: mailbox closed");
                return;
            }
            PumpAction::Send(first) => {
                // Drain mailbox up to `max_batch` and flush once.
                // Each tungstenite `send` is one Binary frame; coalescing
                // the flush turns N TLS records into one writer wakeup
                // and avoids per-frame AEAD-tag amplification.
                batch.push(first);
                while batch.len() < max_batch {
                    match rx.try_recv() {
                        Ok(m) => batch.push(m),
                        Err(_) => break,
                    }
                }
                let drained = batch.len();
                for msg in batch.drain(..) {
                    if let Err(e) = ws.send(WsMessage::Binary(Bytes::from_owner(msg))).await {
                        warn!(%label, %peer, error = ?e, batch_len = drained, "wss writer: send failed");
                        return;
                    }
                }
                if let Err(e) = ws.flush().await {
                    warn!(%label, %peer, error = ?e, batch_len = drained, "wss writer: flush failed");
                    return;
                }
            }
            PumpAction::Recv(Ok(msg)) => match msg {
                WsMessage::Binary(bytes) => match decode_consensus_frame(&bytes) {
                    Ok(frame) => {
                        if in_tx.send(frame).await.is_err() {
                            debug!(%label, %peer, "wss reader: inbound queue dropped");
                            return;
                        }
                    }
                    Err(e) => {
                        warn!(%label, %peer, error = ?e, "wss reader: bad consensus frame");
                        return;
                    }
                },
                WsMessage::Ping(_) | WsMessage::Pong(_) => {
                    // Tungstenite queues an auto-Pong for inbound Pings;
                    // `compio_ws::WebSocketStream::read` flushes before
                    // returning, so no explicit reply needed.
                }
                WsMessage::Close(_) => {
                    debug!(%label, %peer, "wss reader: peer initiated close");
                    return;
                }
                WsMessage::Text(_) | WsMessage::Frame(_) => {
                    warn!(%label, %peer, "wss reader: unexpected text/raw frame, closing");
                    return;
                }
            },
            PumpAction::Recv(Err(e)) => {
                debug!(%label, %peer, error = ?e, "wss reader: read error");
                return;
            }
        }
    }
}

/// Best-effort cooperative close: send WS Close frame, flush, then drop
/// the stream. Bounded by `close_grace`; on timeout the OS sends RST.
///
/// Shutdown is a transaction (no `select!` racing against the token).
#[allow(clippy::future_not_send)]
async fn drive_close(
    ws: &mut WebSocketStream<TlsStream<TcpStream>>,
    close_grace: Duration,
    label: &'static str,
    peer: &str,
) {
    // WebSocketStream::close sends a Close frame; flush drains
    // tungstenite's write buffer to the wire. Dropping the stream after
    // closes the inner TLS layer (no explicit close_notify; WS Close is
    // already a protocol-level shutdown signal accepted as a degraded
    // TLS close).
    if compio::time::timeout(close_grace, async {
        let _ = ws.close(None).await;
        let _ = ws.flush().await;
    })
    .await
    .is_err()
    {
        warn!(
            %label,
            %peer,
            grace_ms = close_grace.as_millis(),
            "wss close: grace exceeded"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framing;
    use crate::lifecycle::Shutdown;
    use crate::transports::tls::{install_default_crypto_provider, self_signed_for_loopback};
    use async_channel::{Receiver, Sender, bounded};
    use compio::net::TcpListener;
    use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
    use iggy_binary_protocol::consensus::iobuf::Frozen;
    use iggy_binary_protocol::{Command2, GenericHeader, HEADER_SIZE, Message};
    use rustls::RootCertStore;
    use std::net::SocketAddr;
    use std::sync::OnceLock;

    fn ensure_provider() {
        static GUARD: OnceLock<()> = OnceLock::new();
        GUARD.get_or_init(install_default_crypto_provider);
    }

    fn server_cfg() -> (
        Arc<rustls::ServerConfig>,
        Vec<rustls::pki_types::CertificateDer<'static>>,
    ) {
        ensure_provider();
        let creds = self_signed_for_loopback();
        let cert_chain = creds.cert_chain.clone();
        let mut cfg = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain.clone(), creds.key_der)
            .expect("server config");
        cfg.max_early_data_size = 0;
        (Arc::new(cfg), cert_chain)
    }

    fn client_cfg_trusting(
        server_cert_chain: &[rustls::pki_types::CertificateDer<'static>],
    ) -> Arc<rustls::ClientConfig> {
        ensure_provider();
        let mut roots = RootCertStore::empty();
        for cert in server_cert_chain {
            roots.add(cert.clone()).expect("add cert");
        }
        Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(Arc::new(roots))
                .with_no_client_auth(),
        )
    }

    #[allow(clippy::future_not_send)]
    async fn connected_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let server_addr: SocketAddr = listener.local_addr().expect("local_addr");
        let connect = TcpStream::connect(server_addr);
        let accept = listener.accept();
        let (client_res, accept_res) = futures::join!(connect, accept);
        let (server, _peer) = accept_res.expect("accept");
        (client_res.expect("connect"), server)
    }

    #[allow(clippy::cast_possible_truncation)]
    fn header_only(command: Command2) -> Frozen<MESSAGE_ALIGN> {
        Message::<GenericHeader>::new(HEADER_SIZE)
            .transmute_header(|_, h: &mut GenericHeader| {
                h.command = command;
                h.size = HEADER_SIZE as u32;
            })
            .into_frozen()
    }

    #[allow(clippy::cast_possible_truncation)]
    fn padded(command: Command2, total_size: usize) -> Frozen<MESSAGE_ALIGN> {
        Message::<GenericHeader>::new(total_size)
            .transmute_header(|_, h: &mut GenericHeader| {
                h.command = command;
                h.size = total_size as u32;
            })
            .into_frozen()
    }

    #[allow(clippy::future_not_send)]
    fn drive(
        conn: WssTransportConn,
    ) -> (
        Sender<Frozen<MESSAGE_ALIGN>>,
        Receiver<Message<GenericHeader>>,
        Shutdown,
        compio::runtime::JoinHandle<()>,
    ) {
        let (out_tx, out_rx) = bounded::<Frozen<MESSAGE_ALIGN>>(16);
        let (in_tx, in_rx) = bounded::<Message<GenericHeader>>(16);
        let (shutdown, token) = Shutdown::new();
        let ctx = ActorContext {
            in_tx,
            rx: out_rx,
            shutdown: crate::lifecycle::FusedShutdown::single(token),
            conn_shutdown: shutdown.clone(),
            max_batch: 16,
            max_message_size: framing::MAX_MESSAGE_SIZE,
            label: "test",
            peer: "localhost".to_owned(),
        };
        let handle = compio::runtime::spawn(async move { conn.run(ctx).await });
        (out_tx, in_rx, shutdown, handle)
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn wss_loopback_round_trip_with_self_signed_cert() {
        let (server_config, cert_chain) = server_cfg();
        let client_config = client_cfg_trusting(&cert_chain);
        let server_name: ServerName<'static> = "localhost".try_into().expect("server name");

        let (client_stream, server_stream) = connected_pair().await;
        let server_conn = WssTransportConn::new_server(server_stream, server_config);
        let client_conn = WssTransportConn::new_client(client_stream, client_config, server_name);

        let (server_out, server_in, server_shutdown, server_handle) = drive(server_conn);
        let (client_out, client_in, client_shutdown, client_handle) = drive(client_conn);

        client_out
            .send(header_only(Command2::Request))
            .await
            .expect("client send");
        let received = compio::time::timeout(Duration::from_secs(5), server_in.recv())
            .await
            .expect("server recv within 5 s")
            .expect("server frame");
        assert_eq!(received.header().command, Command2::Request);

        server_out
            .send(header_only(Command2::Reply))
            .await
            .expect("server send");
        let reply = compio::time::timeout(Duration::from_secs(5), client_in.recv())
            .await
            .expect("client recv within 5 s")
            .expect("client frame");
        assert_eq!(reply.header().command, Command2::Reply);

        server_shutdown.trigger();
        client_shutdown.trigger();
        let _ = compio::time::timeout(Duration::from_secs(5), server_handle).await;
        let _ = compio::time::timeout(Duration::from_secs(5), client_handle).await;
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn wss_large_frame_round_trip() {
        const BODY_SIZE: usize = 1024 * 1024;
        let total = HEADER_SIZE + BODY_SIZE;

        let (server_config, cert_chain) = server_cfg();
        let client_config = client_cfg_trusting(&cert_chain);
        let server_name: ServerName<'static> = "localhost".try_into().expect("server name");

        let (client_stream, server_stream) = connected_pair().await;
        let server_conn = WssTransportConn::new_server(server_stream, server_config);
        let client_conn = WssTransportConn::new_client(client_stream, client_config, server_name);

        let (_server_out, server_in, server_shutdown, server_handle) = drive(server_conn);
        let (client_out, _client_in, client_shutdown, client_handle) = drive(client_conn);

        client_out
            .send(padded(Command2::Request, total))
            .await
            .expect("client send 1 MiB");
        let received = compio::time::timeout(Duration::from_secs(15), server_in.recv())
            .await
            .expect("server recv within 15 s")
            .expect("server frame");
        assert_eq!(received.header().command, Command2::Request);
        assert_eq!(received.header().size as usize, total);

        server_shutdown.trigger();
        client_shutdown.trigger();
        let _ = compio::time::timeout(Duration::from_secs(10), server_handle).await;
        let _ = compio::time::timeout(Duration::from_secs(10), client_handle).await;
    }
}
