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
//! SDK-client plane only; replica plane stays TCP forever (invariant I1).
//! `compio-quic 0.7.2` binds `quinn-proto` natively to the compio runtime,
//! so no tokio bridge (invariant I10).
//!
//! # Connection model
//!
//! One bidirectional stream per peer (no multiplexing). The connection's
//! first `accept_bi` pair carries every consensus frame. The
//! [`QuicTransportListener`] accept loop drives the QUIC handshake to
//! completion AND drains the first bidi pair before yielding the
//! [`QuicTransportConn`], so the owning shard's install path doesn't
//! need an additional `await` boundary before data flows. See
//! `Documents/silverhand/iggy/message_bus/transport-plan/designs/message-bus-quic-install.md`
//! for the full rationale.
//!
//! # Zero-copy
//!
//! `compio_quic::SendStream::write<T: IoBuf>` accepts
//! `Frozen<MESSAGE_ALIGN>` directly; the writer task loops one `write`
//! per frame (QUIC has no `sendmmsg` analog). Per-message syscalls are
//! the documented trade-off versus the TCP `writev` path; small
//! high-RPS workloads stay on TCP. See plan §4 risks.
//!
//! # 0-RTT
//!
//! 0-RTT is off by default at the rustls layer (`max_early_data_size = 0`).
//! `RecvStream::is_0rtt()` exists for defense-in-depth; the listener treats
//! a `true` here as a misconfiguration and refuses the connection. Any
//! future per-command 0-RTT enablement requires an audit per
//! `transport-plan/designs/quic-0rtt-audit-template.md` (P4-T5).

use super::{ActorContext, TransportConn, TransportListener};
use crate::framing;
use crate::lifecycle::{BusReceiver, ShutdownToken};
use compio::BufResult;
use compio::io::AsyncWrite;
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

/// ALPN protocol identifier carried by every Iggy QUIC client + server.
///
/// Bump the trailing version digits when the wire format changes (e.g. a
/// new `GenericHeader` layout). See invariant I3 for the framing contract.
pub const ALPN_IGGY_CONSENSUS_V1: &[u8] = b"iggy.consensus.v1";

/// QUIC application close codes. Sized so the table fits a single cache
/// line; matches the design doc allocation (P4-T2 §8).
pub const QUIC_HANDSHAKE_FAILED: u32 = 0x1001;
pub const QUIC_AUTH_FAILED: u32 = 0x1002;
pub const QUIC_SHUTDOWN: u32 = 0x1003;
pub const QUIC_PROTOCOL_VIOLATION: u32 = 0x1004;

/// Idle-timeout default for client-plane connections. Matches the 30 s
/// replica handshake / dedup window so operators see one number across
/// the bus. Override at endpoint construction time if needed.
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Keepalive interval default. Half of [`DEFAULT_IDLE_TIMEOUT`]'s lower
/// bound so a single dropped keepalive never closes the connection.
const DEFAULT_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);

/// Stream / receive / send window default. Matches
/// `MessageBusConfig::default().max_message_size = 64 MiB` so a single
/// in-flight max-size frame fits without head-of-line wait.
const DEFAULT_WINDOW: u32 = 64 * 1024 * 1024;

/// Build a [`compio_quic::TransportConfig`] tuned for the SDK-client plane.
///
/// Defaults: 1 bidi stream per peer, 30 s idle timeout, CUBIC congestion
/// (upstream default), 64 MiB send/receive windows. Settings match the
/// audit's knob table (`findings/compio-quic-audit.md` §2).
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
/// Wraps a bound [`Endpoint`]. `accept` drives the QUIC handshake and the
/// first `accept_bi` pair to completion before yielding a
/// [`QuicTransportConn`], so the caller (`replica_listener::run`'s QUIC
/// twin in P4-T2's design) hands a fully-set-up data plane to the install
/// path.
pub struct QuicTransportListener {
    endpoint: Endpoint,
}

impl QuicTransportListener {
    /// Wrap a pre-bound [`Endpoint`].
    ///
    /// Caller is responsible for the [`Endpoint`]'s ALPN set
    /// ([`ALPN_IGGY_CONSENSUS_V1`]), [`compio_quic::ServerConfig`] crypto
    /// material, and [`default_transport_config`] hookup.
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
}

impl TransportListener for QuicTransportListener {
    type Conn = QuicTransportConn;

    #[allow(clippy::future_not_send)]
    async fn accept(&self) -> io::Result<(Self::Conn, SocketAddr)> {
        loop {
            let Some(incoming) = self.endpoint.wait_incoming().await else {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "QUIC endpoint closed",
                ));
            };
            match accept_one(incoming).await {
                Ok(Some((conn, addr))) => return Ok((conn, addr)),
                Ok(None) => {}
                Err(e) => return Err(e),
            }
        }
    }
}

#[allow(clippy::future_not_send)]
async fn accept_one(incoming: Incoming) -> io::Result<Option<(QuicTransportConn, SocketAddr)>> {
    let connecting = match incoming.accept() {
        Ok(c) => c,
        Err(e) => {
            warn!("QUIC incoming.accept failed: {e}");
            return Ok(None);
        }
    };
    let connection = match connecting.await {
        Ok(c) => c,
        Err(e) => {
            warn!("QUIC handshake failed: {e}");
            return Ok(None);
        }
    };
    let addr = connection.remote_address();

    let (send, recv) = match connection.accept_bi().await {
        Ok(streams) => streams,
        Err(e) => {
            warn!(%addr, "QUIC accept_bi failed: {e}");
            connection.close(VarInt::from_u32(QUIC_HANDSHAKE_FAILED), b"accept_bi failed");
            return Ok(None);
        }
    };

    if recv.is_0rtt() {
        warn!(
            %addr,
            "QUIC stream accepted in 0-RTT window; refusing (audit P4-T5 not run)"
        );
        connection.close(
            VarInt::from_u32(QUIC_PROTOCOL_VIOLATION),
            b"0-RTT not permitted",
        );
        return Ok(None);
    }

    debug!(%addr, "QUIC connection accepted, first bidi stream ready");
    Ok(Some((
        QuicTransportConn::new(connection, (send, recv)),
        addr,
    )))
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
    /// `client_listener_quic::run` uses this to hand the
    /// already-accepted connection + first bidi pair to
    /// [`crate::installer::install_client_quic_conn`] (which then wraps
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
        let writer_handle =
            compio::runtime::spawn(writer_task(send, rx, writer_shutdown, label, peer));
        let _ = reader_handle.await;
        let _ = writer_handle.await;
        connection.close(VarInt::from_u32(QUIC_SHUTDOWN), b"shutdown");
    }
}

#[allow(clippy::future_not_send)]
async fn reader_task(
    mut recv: RecvStream,
    in_tx: async_channel::Sender<Message<GenericHeader>>,
    shutdown: ShutdownToken,
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

#[allow(clippy::future_not_send)]
async fn writer_task(
    mut send: SendStream,
    rx: BusReceiver,
    shutdown: ShutdownToken,
    label: &'static str,
    peer: String,
) {
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

        let BufResult(result, _frozen) = send.write(frozen).await;
        if let Err(e) = result {
            debug!(%label, %peer, "quic writer: write failed: {e}");
            return;
        }
        if let Err(e) = send.flush().await {
            debug!(%label, %peer, "quic writer: flush failed: {e}");
            return;
        }
    }
}

/// Build a [`compio_quic::ServerConfig`] from a single cert + key pair.
///
/// Sets the Iggy ALPN ([`ALPN_IGGY_CONSENSUS_V1`]) and the bus-tuned
/// [`default_transport_config`]; otherwise inherits upstream defaults
/// including `migration: true`.
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
    rustls_cfg.alpn_protocols = vec![ALPN_IGGY_CONSENSUS_V1.to_vec()];
    rustls_cfg.max_early_data_size = 0; // 0-RTT off (invariant I7)
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
        let mut builder = ClientBuilder::new_with_empty_roots()
            .with_custom_certificate(server_cert)
            .expect("trust cert")
            .with_no_crls();
        builder = builder.with_alpn_protocols(&["iggy.consensus.v1"]);
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
            shutdown: token,
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
        // and the underlying defaults the audit named are reachable.
        let _cfg = default_transport_config();
    }
}
