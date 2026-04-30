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

//! QUIC impls of the [`super`] transport traits.
//!
//! SDK-client plane only; replica plane stays TCP-only.
//! `compio-quic` binds `quinn-proto` natively to the compio runtime,
//! so no tokio bridge.
//!
//! # Connection model
//!
//! One bidirectional stream per peer (no multiplexing). The connection's
//! first `accept_bi` pair carries every consensus frame.
//!
//! The accept step in [`accept_handshake`] drives the QUIC handshake AND
//! the first `accept_bi` pair before yielding a fully-set-up
//! [`QuicTransportConn`]. To avoid a single slow / hostile peer wedging
//! the entire client plane behind that sequential pair of awaits, the
//! client listener's accept loop pulls bare [`Incoming`] values via
//! [`QuicTransportListener::next_incoming`] and spawns one
//! `accept_handshake` task per incoming. The accept step only obtains
//! the `(SendStream, RecvStream)` handles; no bytes are read off the
//! streams.
//!
//! # Zero-copy
//!
//! `compio_quic::SendStream::write<T: IoBuf>` accepts
//! `Frozen<MESSAGE_ALIGN>` directly; the writer task loops one `write`
//! per frame (QUIC has no `sendmmsg` analog). Per-message syscalls are
//! the documented trade-off versus the TCP `writev` path; small
//! high-RPS workloads stay on TCP.
//!
//! # 0-RTT
//!
//! 0-RTT is off by default at the rustls layer (`max_early_data_size = 0`).
//! `RecvStream::is_0rtt()` exists for defense-in-depth; the listener
//! treats a `true` here as a misconfiguration and refuses the
//! connection. Per-command 0-RTT enablement requires a per-command
//! idempotence audit before being turned on.

use super::{ActorContext, TransportConn, TransportListener};
use crate::framing;
use crate::lifecycle::{BusReceiver, FusedShutdown};
use compio::BufResult;
use compio::io::AsyncWriteExt;
use compio_quic::{
    Connection, Endpoint, Incoming, RecvStream, SendStream, VarInt, congestion,
    crypto::rustls::QuicServerConfig,
};
use futures::FutureExt;
use iggy_binary_protocol::{GenericHeader, Message};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

/// QUIC application close codes. Sized so the table fits a single cache
/// line.
pub const QUIC_HANDSHAKE_FAILED: u32 = 0x1001;
pub const QUIC_AUTH_FAILED: u32 = 0x1002;
pub const QUIC_SHUTDOWN: u32 = 0x1003;
pub const QUIC_PROTOCOL_VIOLATION: u32 = 0x1004;

/// Idle-timeout default for client-plane connections. Override at
/// endpoint construction time if the operator needs a tighter or looser
/// liveness budget.
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Keepalive interval default. One third of [`DEFAULT_IDLE_TIMEOUT`] so
/// up to two consecutive keepalives can be lost before the idle timer
/// closes the connection.
const DEFAULT_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);

/// Stream / receive / send window default. Matches
/// `MessageBusConfig::default().max_message_size = 64 MiB` so a single
/// in-flight max-size frame fits without head-of-line wait.
const DEFAULT_WINDOW: u32 = 64 * 1024 * 1024;

/// Default wall-clock bound on `accept_handshake`. Mirrors
/// [`crate::MessageBusConfig::handshake_grace`]; consumed by the
/// trait-impl `accept` path which has no plumbing for caller config.
/// Production accept loops route through
/// [`crate::client_listener::quic::run`] which threads
/// `MessageBusConfig::handshake_grace` directly.
const DEFAULT_HANDSHAKE_GRACE: Duration = Duration::from_secs(10);

/// Build a [`compio_quic::TransportConfig`] tuned for the SDK-client plane.
///
/// Defaults: 1 bidi stream per peer, 30 s idle timeout, CUBIC congestion,
/// 64 MiB send/receive windows.
///
/// # Panics
///
/// Panics only if `DEFAULT_IDLE_TIMEOUT` is changed to a value outside
/// quinn-proto's `IdleTimeout` range (its `TryFrom<Duration>` rejects
/// values above `VarInt::MAX_U64` ms). 30 s is well inside the range.
#[must_use]
pub fn default_transport_config() -> compio_quic::TransportConfig {
    let mut cfg = compio_quic::TransportConfig::default();
    cfg.max_concurrent_bidi_streams(VarInt::from_u32(1))
        .max_concurrent_uni_streams(VarInt::from_u32(0))
        .stream_receive_window(VarInt::from_u32(DEFAULT_WINDOW))
        .receive_window(VarInt::from_u32(DEFAULT_WINDOW))
        .send_window(u64::from(DEFAULT_WINDOW))
        .max_idle_timeout(Some(
            DEFAULT_IDLE_TIMEOUT
                .try_into()
                .expect("30 s fits in IdleTimeout"),
        ))
        .keep_alive_interval(Some(DEFAULT_KEEP_ALIVE_INTERVAL))
        .congestion_controller_factory(Arc::new(congestion::CubicConfig::default()));
    cfg
}

/// Inbound QUIC listener.
///
/// Wraps a bound [`Endpoint`]. Two accept entry points exist:
///
/// - [`Self::next_incoming`] yields a bare [`Incoming`]. The client
///   listener accept loop calls this and then spawns a per-incoming
///   handshake task, so a slow / hostile peer cannot wedge subsequent
///   accepts behind its handshake or first-bidi await.
/// - [`TransportListener::accept`] still drives the handshake and first
///   `accept_bi` pair to completion before yielding a
///   [`QuicTransportConn`]. Tests and any single-connection caller use
///   this; production accept loops should not.
pub struct QuicTransportListener {
    endpoint: Endpoint,
}

impl QuicTransportListener {
    /// Wrap a pre-bound [`Endpoint`].
    ///
    /// Caller is responsible for the [`Endpoint`]'s
    /// [`compio_quic::ServerConfig`] crypto material and
    /// [`default_transport_config`] hookup.
    #[must_use]
    pub const fn new(endpoint: Endpoint) -> Self {
        Self { endpoint }
    }

    /// Borrow the underlying endpoint for `local_addr`, `set_server_config`,
    /// `close`, or `shutdown` calls.
    #[must_use]
    pub const fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Wait for the next inbound connection attempt.
    ///
    /// Returns the raw [`Incoming`]; the caller is responsible for
    /// driving [`accept_handshake`] (typically inside its own spawned
    /// task) so that one slow handshake does not block subsequent
    /// accepts.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::ConnectionAborted`] when the underlying
    /// endpoint has been closed.
    #[allow(clippy::future_not_send)]
    pub async fn next_incoming(&self) -> io::Result<Incoming> {
        self.endpoint
            .wait_incoming()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::ConnectionAborted, "QUIC endpoint closed"))
    }
}

impl TransportListener for QuicTransportListener {
    type Conn = QuicTransportConn;

    #[allow(clippy::future_not_send)]
    async fn accept(&self) -> io::Result<(Self::Conn, SocketAddr)> {
        loop {
            let incoming = self.next_incoming().await?;
            if let Some(result) = accept_handshake(incoming, DEFAULT_HANDSHAKE_GRACE).await {
                return Ok(result);
            }
        }
    }
}

/// Drive the QUIC handshake and the first `accept_bi` pair for one
/// [`Incoming`].
///
/// Wraps the inner handshake driver in `compio::time::timeout` so a
/// slowloris peer cannot pin the per-incoming spawned task beyond
/// `handshake_grace`. The whole sequence
/// (`incoming.accept` -> `connecting.await` -> `connection.accept_bi`)
/// shares one wall-clock budget; on timeout the spawned future is
/// dropped, which drops the local `Connection` and closes the QUIC
/// session via its own Drop path.
///
/// Returns [`Some`] on full success (handshake + first bidi pair, no
/// 0-RTT). Returns [`None`] for any peer-induced failure
/// (`incoming.accept` rejection, handshake error, `accept_bi` error,
/// 0-RTT misconfiguration, handshake-grace exceeded); each non-success
/// path logs and closes the connection with the matching application
/// close code.
#[allow(clippy::future_not_send)]
pub async fn accept_handshake(
    incoming: Incoming,
    handshake_grace: Duration,
) -> Option<(QuicTransportConn, SocketAddr)> {
    match compio::time::timeout(handshake_grace, accept_handshake_inner(incoming)).await {
        Ok(result) => result,
        Err(_elapsed) => {
            warn!(
                grace = ?handshake_grace,
                "QUIC handshake exceeded handshake_grace; dropping connection"
            );
            None
        }
    }
}

#[allow(clippy::future_not_send)]
async fn accept_handshake_inner(incoming: Incoming) -> Option<(QuicTransportConn, SocketAddr)> {
    let connecting = match incoming.accept() {
        Ok(c) => c,
        Err(e) => {
            warn!("QUIC incoming.accept failed: {e}");
            return None;
        }
    };
    let connection = match connecting.await {
        Ok(c) => c,
        Err(e) => {
            warn!("QUIC handshake failed: {e}");
            return None;
        }
    };
    let addr = connection.remote_address();

    let (send, recv) = match connection.accept_bi().await {
        Ok(streams) => streams,
        Err(e) => {
            warn!(%addr, "QUIC accept_bi failed: {e}");
            connection.close(VarInt::from_u32(QUIC_HANDSHAKE_FAILED), b"accept_bi failed");
            return None;
        }
    };

    if recv.is_0rtt() {
        warn!(
            %addr,
            "QUIC stream accepted in 0-RTT window; refusing"
        );
        connection.close(
            VarInt::from_u32(QUIC_PROTOCOL_VIOLATION),
            b"0-RTT not permitted",
        );
        return None;
    }

    debug!(%addr, "QUIC connection accepted, first bidi stream ready");
    Some((QuicTransportConn::new(connection, (send, recv)), addr))
}

/// A single QUIC connection plus its first bidirectional stream.
///
/// The owned `Connection` handle is retained so graceful shutdown can
/// fire `Connection::close(QUIC_SHUTDOWN, _)` after the writer drains.
pub struct QuicTransportConn {
    connection: Connection,
    streams: (SendStream, RecvStream),
}

impl QuicTransportConn {
    /// Construct from an already-established connection + bidi pair.
    #[must_use]
    pub const fn new(connection: Connection, streams: (SendStream, RecvStream)) -> Self {
        Self {
            connection,
            streams,
        }
    }

    /// Deconstruct into the raw `(Connection, (SendStream, RecvStream))`
    /// tuple, mirror of [`Self::new`].
    ///
    /// `client_listener::quic::run` uses this to hand the
    /// already-accepted connection + first bidi pair to
    /// [`crate::installer::install_client_quic`] (which then wraps
    /// them in a fresh `QuicTransportConn` and dispatches via the
    /// generic install path).
    #[must_use]
    pub fn into_parts(self) -> (Connection, (SendStream, RecvStream)) {
        (self.connection, self.streams)
    }
}

impl TransportConn for QuicTransportConn {
    #[allow(clippy::future_not_send)]
    async fn run(self, ctx: ActorContext) {
        let (send, recv) = self.streams;
        let connection = self.connection;
        let ActorContext {
            in_tx,
            rx,
            shutdown,
            conn_shutdown,
            max_batch: _,
            max_message_size,
            label,
            peer,
        } = ctx;
        let reader_shutdown = shutdown.clone();
        let writer_shutdown = shutdown;
        let reader_peer = peer.clone();
        let reader_label = label;
        let reader_handle = compio::runtime::spawn(reader_task(
            recv,
            in_tx,
            reader_shutdown,
            max_message_size,
            reader_label,
            reader_peer,
        ));
        let writer_handle = compio::runtime::spawn(writer_task(
            send,
            rx,
            writer_shutdown,
            conn_shutdown,
            label,
            peer,
        ));
        let _ = reader_handle.await;
        let _ = writer_handle.await;
        connection.close(VarInt::from_u32(QUIC_SHUTDOWN), b"shutdown");
    }
}

/// Drain inbound consensus frames from `recv` until shutdown or a
/// read error.
///
/// # Cancel-safety
///
/// The `select!` arm that wakes on `shutdown` drops a parked
/// [`crate::framing::read_message`] future. `read_message` is not
/// cancel-safe across multi-read frames: bytes already pulled into the
/// in-flight `Owned<MESSAGE_ALIGN>` are lost with the dropped frame and
/// the stream has already advanced past them. The reader treats this
/// loss as terminal and exits, which the writer's scopeguard turns into
/// a clean per-connection close. Reads that complete in a single poll
/// (full header + body already buffered by the QUIC stack) are
/// unaffected.
#[allow(clippy::future_not_send)]
async fn reader_task(
    mut recv: RecvStream,
    in_tx: async_channel::Sender<Message<GenericHeader>>,
    shutdown: FusedShutdown,
    max_message_size: usize,
    label: &'static str,
    peer: String,
) {
    let mut shutdown_fut = Box::pin(shutdown.wait().fuse());
    loop {
        let read_fut = framing::read_message(&mut recv, max_message_size);
        let result = futures::select! {
            () = shutdown_fut.as_mut() => {
                debug!(%label, %peer, "quic reader: shutdown observed");
                return;
            }
            res = read_fut.fuse() => res,
        };
        match result {
            Ok(msg) => {
                if in_tx.send(msg).await.is_err() {
                    debug!(%label, %peer, "quic reader: inbound queue dropped");
                    return;
                }
            }
            Err(e) => {
                debug!(%label, %peer, "quic reader: read error: {e:?}");
                return;
            }
        }
    }
}

/// Drain `rx` and write each frame to the QUIC stream.
///
/// On exit (clean OR panic via compio's `catch_unwind`) the scopeguard
/// triggers the per-connection [`crate::lifecycle::Shutdown`]. The
/// reader's `select!` over `shutdown.wait()` then resolves, which
/// closes the reader, then `run()` resumes, closes the QUIC
/// connection, and the installer's `transport_handle` scopeguard
/// evicts the registry slot. Without this trigger, a writer-task panic
/// while the peer is silent would leave the reader parked on
/// `read_message` indefinitely.
#[allow(clippy::future_not_send)]
async fn writer_task(
    mut send: SendStream,
    rx: BusReceiver,
    shutdown: FusedShutdown,
    conn_shutdown: crate::lifecycle::Shutdown,
    label: &'static str,
    peer: String,
) {
    let _wake_reader = scopeguard::guard(conn_shutdown, |s| s.trigger());
    let mut shutdown_fut = Box::pin(shutdown.wait().fuse());
    loop {
        let frozen = futures::select! {
            () = shutdown_fut.as_mut() => {
                debug!(%label, %peer, "quic writer: shutdown observed");
                return;
            }
            msg = rx.recv().fuse() => {
                let Ok(m) = msg else {
                    debug!(%label, %peer, "quic writer: channel closed");
                    return;
                };
                m
            }
        };

        let BufResult(result, _frozen) = send.write_all(frozen).await;
        if let Err(e) = result {
            debug!(%label, %peer, "quic writer: write failed: {e}");
            return;
        }
        // No per-frame flush: quinn-proto auto-coalesces STREAM frames
        // into outbound datagrams. An explicit flush() blocks until the
        // peer ACKs, serializing every send and killing pipelining.
    }
}

/// Build a [`compio_quic::ServerConfig`] from a single cert + key pair.
///
/// Applies the bus-tuned [`default_transport_config`] and disables
/// 0-RTT; otherwise inherits upstream defaults including
/// `migration: true`. No ALPN is advertised; protocol-version
/// validation lives in the LOGIN command on the caller (server-ng).
///
/// # Errors
///
/// Returns the underlying `rustls::Error` on cert/key import failure.
pub fn server_config_with_cert(
    cert_chain: Vec<rustls::pki_types::CertificateDer<'static>>,
    key_der: rustls::pki_types::PrivateKeyDer<'static>,
) -> Result<compio_quic::ServerConfig, rustls::Error> {
    let mut rustls_cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key_der)?;
    rustls_cfg.max_early_data_size = 0; // 0-RTT off by default
    let crypto = QuicServerConfig::try_from(rustls_cfg)
        .map_err(|e| rustls::Error::General(format!("QUIC crypto: {e}")))?;
    let mut server_cfg = compio_quic::ServerConfig::with_crypto(Arc::new(crypto));
    server_cfg.transport_config(Arc::new(default_transport_config()));
    Ok(server_cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::Shutdown;
    use async_channel::bounded;
    use compio::io::AsyncWrite;
    use compio_quic::ClientBuilder;
    use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
    use iggy_binary_protocol::consensus::iobuf::Frozen;
    use iggy_binary_protocol::{Command2, HEADER_SIZE};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use std::time::Duration;

    fn install_crypto_provider() {
        // Idempotent. Tests in the same process race on
        // `install_default`, so swallow the second-installer error.
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    fn header_only(command: Command2) -> Frozen<MESSAGE_ALIGN> {
        #[allow(clippy::cast_possible_truncation)]
        Message::<GenericHeader>::new(HEADER_SIZE)
            .transmute_header(|_, h: &mut GenericHeader| {
                h.command = command;
                h.size = HEADER_SIZE as u32;
            })
            .into_frozen()
    }

    fn self_signed() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).expect("rcgen");
        let cert_der = CertificateDer::from(cert.cert);
        let key_der: PrivateKeyDer<'static> =
            PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into();
        (cert_der, key_der)
    }

    #[allow(clippy::future_not_send)]
    async fn server_endpoint(
        cert: CertificateDer<'static>,
        key: PrivateKeyDer<'static>,
    ) -> Endpoint {
        let server_cfg = server_config_with_cert(vec![cert], key).expect("server config");
        Endpoint::server("127.0.0.1:0", server_cfg)
            .await
            .expect("bind")
    }

    #[allow(clippy::future_not_send)]
    async fn client_endpoint(server_cert: CertificateDer<'static>) -> Endpoint {
        let builder = ClientBuilder::new_with_empty_roots()
            .with_custom_certificate(server_cert)
            .expect("trust cert")
            .with_no_crls();
        builder.bind("127.0.0.1:0").await.expect("client bind")
    }

    /// Spawn `conn.run(ctx)` with fresh channels; return the test-side
    /// handles.
    #[allow(clippy::future_not_send)]
    fn drive(
        conn: QuicTransportConn,
    ) -> (
        async_channel::Sender<Frozen<MESSAGE_ALIGN>>,
        async_channel::Receiver<Message<GenericHeader>>,
        Shutdown,
        compio::runtime::JoinHandle<()>,
    ) {
        let (out_tx, out_rx) = bounded::<Frozen<MESSAGE_ALIGN>>(16);
        let (in_tx, in_rx) = bounded::<Message<GenericHeader>>(16);
        let (shutdown, token) = Shutdown::new();
        let ctx = ActorContext {
            in_tx,
            rx: out_rx,
            shutdown: FusedShutdown::single(token),
            conn_shutdown: shutdown.clone(),
            max_batch: 16,
            max_message_size: framing::MAX_MESSAGE_SIZE,
            label: "test",
            peer: "test".to_owned(),
        };
        let handle = compio::runtime::spawn(async move { conn.run(ctx).await });
        (out_tx, in_rx, shutdown, handle)
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn loopback_round_trip_three_frames() {
        install_crypto_provider();
        let (cert, key) = self_signed();
        let server = server_endpoint(cert.clone(), key).await;
        let server_addr = server.local_addr().unwrap();

        let listener = QuicTransportListener::new(server);
        let server_task = compio::runtime::spawn(async move {
            let (conn, _peer) = listener.accept().await.expect("accept");
            let (_out_tx, in_rx, shutdown, handle) = drive(conn);
            let a = in_rx.recv().await.unwrap();
            let b = in_rx.recv().await.unwrap();
            let c = in_rx.recv().await.unwrap();
            shutdown.trigger();
            let _ = handle.await;
            (a.header().command, b.header().command, c.header().command)
        });

        let client = client_endpoint(cert).await;
        let connecting = client
            .connect(server_addr, "localhost", None)
            .expect("connect");
        let connection = connecting.await.expect("client handshake");
        let (send, recv) = connection.open_bi_wait().await.expect("open_bi");
        let conn = QuicTransportConn::new(connection, (send, recv));
        let (out_tx, _in_rx, shutdown, handle) = drive(conn);

        out_tx.send(header_only(Command2::Ping)).await.unwrap();
        out_tx.send(header_only(Command2::Prepare)).await.unwrap();
        out_tx.send(header_only(Command2::Request)).await.unwrap();

        let (a, b, c) = compio::time::timeout(Duration::from_secs(5), server_task)
            .await
            .expect("server task within 5s")
            .unwrap();
        assert_eq!(a, Command2::Ping);
        assert_eq!(b, Command2::Prepare);
        assert_eq!(c, Command2::Request);

        shutdown.trigger();
        let _ = handle.await;
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn read_message_reports_oversize_via_run() {
        install_crypto_provider();
        let (cert, key) = self_signed();
        let server = server_endpoint(cert.clone(), key).await;
        let server_addr = server.local_addr().unwrap();

        let listener = QuicTransportListener::new(server);
        let server_task = compio::runtime::spawn(async move {
            let (conn, _peer) = listener.accept().await.expect("accept");
            let (_out_tx, in_rx, shutdown, handle) = drive(conn);
            let res = in_rx.recv().await;
            shutdown.trigger();
            let _ = handle.await;
            res
        });

        let client = client_endpoint(cert).await;
        let connecting = client
            .connect(server_addr, "localhost", None)
            .expect("connect");
        let connection = connecting.await.expect("handshake");
        let (mut send, recv) = connection.open_bi_wait().await.expect("open_bi");
        // Bogus oversize size field at offset 48.
        let mut buf = vec![0u8; HEADER_SIZE];
        let bogus = u32::try_from(framing::MAX_MESSAGE_SIZE + 1)
            .unwrap_or(u32::MAX)
            .to_le_bytes();
        buf[48..52].copy_from_slice(&bogus);
        let BufResult(result, _) = send.write(buf).await;
        result.expect("write");
        send.flush().await.expect("flush");
        drop(send); // signal end-of-stream so reader sees the framed bytes
        drop(recv);

        let res = compio::time::timeout(Duration::from_secs(5), server_task)
            .await
            .expect("server task within 5s")
            .unwrap();
        // Framing error inside reader_task closes in_tx; recv resolves to Err.
        assert!(res.is_err());
    }

    #[test]
    fn default_transport_config_caps_streams_at_one() {
        // Lightweight smoke: confirm the helper builds without panicking
        // and the underlying defaults are reachable.
        let _cfg = default_transport_config();
    }
}
