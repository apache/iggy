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

//! In-process TCP-TLS transport: rustls `UnbufferedConnection` driver
//! stacked on top of plaintext TCP.
//!
//! Wraps the `transports::tls::driver` state machine landed by P9-T2a /
//! P9-T2b in a [`TransportConn`] adapter that accepts an already-connected
//! `compio::net::TcpStream` plus role-specific rustls config, drives the
//! TLS handshake to completion, and then attaches the bus's per-connection
//! [`ActorContext`] to the driver's reader / writer task pair.
//!
//! # Lifecycle
//!
//! [`TcpTlsTransportConn::run`] spawns four cooperating tasks:
//!
//! - **reader** + **writer** (owned by the driver): hand-rolled rustls
//!   record-layer split. Reader pushes each decrypted
//!   `Message<GenericHeader>` onto `ActorContext::in_tx`; writer drains a
//!   merged `WriterEvent` channel for outbound application frames +
//!   reader-side handshake-response flush requests.
//! - **ctx pump**: forwards each outbound `Frozen` from
//!   `ActorContext::rx` into the merged channel as `WriterEvent::Out`.
//!   The pump exits on `ActorContext::shutdown` firing OR on
//!   `ActorContext::rx` closure; either drops its `Sender` clone,
//!   contributing to the merged-channel-close shutdown signal.
//! - **bridges** (detached): forward `ActorContext::shutdown` and natural
//!   reader/writer exits onto a single internal `Shutdown` so the
//!   orchestrator wakes uniformly on either signal.
//!
//! Once the internal shutdown fires, the orchestrator awaits the pump
//! (so its `Sender` clone drops) and then defers to the driver's
//! cooperative shutdown helper, which performs `libc::shutdown(SHUT_RD)`
//! to wake any quiescent reader, drops the orchestrator's last `Sender`
//! clone (so the writer's `recv` returns `Err` and the cooperative
//! `close_notify` + `SHUT_WR` sequence runs), and reaps both task
//! handles within the configured drain budget.
//!
//! # Frozen ownership on the TLS plane
//!
//! Plaintext TCP preserves `Frozen<MESSAGE_ALIGN>` ownership end-to-end
//! (invariant I8). The TLS plane structurally cannot: rustls's
//! `WriteTraffic::encrypt` copies plaintext bytes into the outbound
//! ciphertext buffer along with per-record AEAD tags, so the wire bytes
//! are not the same allocation that left the bus. The I8 amendment
//! (zero-copy preserved on plaintext TCP only) lands in P9-T13.
//!
//! # Cancellation safety
//!
//! All `RefCell` borrows on the driver's shared state live inside
//! synchronous helpers in `transports::tls::driver`; the
//! orchestrator-level awaits here only touch cancel-safe channel
//! operations and `ShutdownToken::wait`. `compio::runtime::JoinHandle`'s
//! `Drop` detaches the spawned task rather than cancelling it; the bus's
//! outer `close_peer_timeout` therefore relies on the cooperative
//! merged-channel-close path to propagate to the writer for clean
//! `close_notify` emission.

use super::tls::{TlsConnHandles, TlsDriver, WriterEvent, shutdown as tls_shutdown};
use super::{ActorContext, TransportConn};
use crate::lifecycle::{BusReceiver, Shutdown, ShutdownToken};
use async_channel::{Sender, bounded};
use compio::net::TcpStream;
use futures::FutureExt;
use rustls::pki_types::ServerName;
use std::os::fd::AsRawFd;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

/// Default drain budget for cooperative shutdown.
///
/// Bounds how long [`super::tls::shutdown`] waits for the writer task
/// to emit `close_notify` + `SHUT_WR` after the merged channel closes.
/// Operators tune this through `MessageBusConfig::tls_drain_budget`
/// (lifted in P9-T10); per-transport-arg here keeps the seam
/// available without coupling the bus-level config to the transport's
/// constructor.
const DEFAULT_DRAIN_BUDGET: Duration = Duration::from_secs(5);

/// Local-end role on a TLS connection. Determines whether the
/// `TlsDriver` is built in server or client mode.
pub enum TlsRole {
    /// Server side: drives the rustls server state machine against the
    /// supplied [`rustls::ServerConfig`]. Constructed via
    /// [`TcpTlsTransportConn::new_server`].
    Server(Arc<rustls::ServerConfig>),
    /// Client side: drives the rustls client state machine against the
    /// supplied [`rustls::ClientConfig`] + a pre-validated server name.
    /// Constructed via [`TcpTlsTransportConn::new_client`].
    Client {
        /// Client-role rustls configuration.
        config: Arc<rustls::ClientConfig>,
        /// Server name presented in SNI and validated against the leaf
        /// certificate's SANs / CN. Owned `'static` because rustls
        /// retains it for the lifetime of the underlying
        /// `UnbufferedClientConnection`.
        server_name: ServerName<'static>,
    },
}

/// In-process TCP-TLS transport: a [`TcpStream`] paired with a
/// role-specific rustls configuration.
///
/// Construct with [`Self::new_server`] or [`Self::new_client`]; drive
/// with [`TransportConn::run`]. Cancellation lives at the
/// `ActorContext::shutdown` seam; the constructor pre-bakes a default
/// drain budget which the with-builder
/// [`Self::with_drain_budget`] can override per connection.
pub struct TcpTlsTransportConn {
    stream: TcpStream,
    role: TlsRole,
    drain_budget: Duration,
}

impl TcpTlsTransportConn {
    /// Server-side connection: drive the rustls server state machine
    /// against `config` and the bytes flowing on `stream`. The default
    /// drain budget is 5 s; override via [`Self::with_drain_budget`].
    #[must_use]
    pub const fn new_server(stream: TcpStream, config: Arc<rustls::ServerConfig>) -> Self {
        Self {
            stream,
            role: TlsRole::Server(config),
            drain_budget: DEFAULT_DRAIN_BUDGET,
        }
    }

    /// Client-side connection: drive the rustls client state machine
    /// against `config` and the bytes flowing on `stream`, presenting
    /// `server_name` in SNI. Default drain budget as in
    /// [`Self::new_server`].
    #[must_use]
    pub const fn new_client(
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
            drain_budget: DEFAULT_DRAIN_BUDGET,
        }
    }

    /// Override the cooperative-shutdown drain budget. Intended for
    /// tests and dev-loops; production callers should leave the default
    /// in place until P9-T10 hoists the value into `MessageBusConfig`.
    #[must_use]
    pub const fn with_drain_budget(mut self, drain_budget: Duration) -> Self {
        self.drain_budget = drain_budget;
        self
    }
}

impl TransportConn for TcpTlsTransportConn {
    #[allow(clippy::future_not_send)]
    async fn run(self, ctx: ActorContext) {
        let raw_fd = self.stream.as_raw_fd();
        let drain_budget = self.drain_budget;
        let role = self.role;
        let max_batch = ctx.max_batch;
        let label = ctx.label;
        let peer_for_log = ctx.peer.clone();
        let (mut read_half, mut write_half) = self.stream.into_split();

        let driver = match build_driver(role) {
            Ok(d) => d,
            Err(e) => {
                warn!(
                    %label,
                    peer = %peer_for_log,
                    error = ?e,
                    "TLS driver setup failed; closing connection"
                );
                return;
            }
        };

        if let Err(e) = driver
            .drive_handshake(&mut read_half, &mut write_half)
            .await
        {
            warn!(
                %label,
                peer = %peer_for_log,
                error = ?e,
                "TLS handshake failed; closing connection"
            );
            return;
        }

        let (writer_tx, writer_rx) = bounded::<WriterEvent>(max_batch);
        let reader_writer_tx = writer_tx.clone();
        let (reader_h, writer_h) = driver.spawn_tasks(
            read_half,
            write_half,
            ctx.in_tx,
            writer_rx,
            reader_writer_tx,
        );

        // Internal shutdown: any of (external ctx.shutdown / reader exit /
        // writer exit) triggers a single fused token the orchestrator awaits
        // before invoking the cooperative shutdown helper.
        let (internal, internal_token) = Shutdown::new();
        let internal = Rc::new(internal);

        let ext_token = ctx.shutdown.clone();
        let i_ext = Rc::clone(&internal);
        compio::runtime::spawn(async move {
            ext_token.wait().await;
            i_ext.trigger();
        })
        .detach();

        let i_reader = Rc::clone(&internal);
        let reader_watch = compio::runtime::spawn(async move {
            let res = reader_h.await;
            i_reader.trigger();
            match res {
                Ok(inner) => inner,
                Err(_panic) => Ok(()),
            }
        });
        let i_writer = Rc::clone(&internal);
        let writer_watch = compio::runtime::spawn(async move {
            let res = writer_h.await;
            i_writer.trigger();
            match res {
                Ok(inner) => inner,
                Err(_panic) => Ok(()),
            }
        });

        let pump_writer_tx = writer_tx.clone();
        let pump_token = internal_token.clone();
        let pump_handle = compio::runtime::spawn(pump_outbound(ctx.rx, pump_writer_tx, pump_token));

        internal_token.wait().await;

        // Pump observes the same internal token; awaiting its handle here
        // ensures its `Sender` clone is dropped before the cooperative
        // shutdown helper starts the writer drain.
        let _ = pump_handle.await;

        let handles = TlsConnHandles {
            reader: reader_watch,
            writer: writer_watch,
            writer_tx,
            raw_fd,
        };
        tls_shutdown(handles, drain_budget).await;
    }
}

fn build_driver(role: TlsRole) -> Result<TlsDriver, super::tls::DriveError> {
    match role {
        TlsRole::Server(cfg) => TlsDriver::new_server(cfg),
        TlsRole::Client {
            config,
            server_name,
        } => TlsDriver::new_client(config, server_name),
    }
}

/// Forward outbound `Frozen` frames from the bus's per-peer queue into
/// the driver's merged writer channel as
/// [`WriterEvent::Out`](super::tls::WriterEvent::Out).
///
/// Exits on:
/// - `shutdown` firing (external cancellation merged through the
///   internal shutdown bridge).
/// - `rx` closure (bus dropped its outbound `Sender`; nothing more to
///   pump).
/// - `writer_tx.send` returning `Err` (writer task gone; downstream
///   drain owns cleanup).
///
/// Drops its `Sender` clone on exit, contributing to the
/// merged-channel-close cooperative-shutdown signal the writer task
/// observes.
#[allow(clippy::future_not_send)]
async fn pump_outbound(rx: BusReceiver, writer_tx: Sender<WriterEvent>, shutdown: ShutdownToken) {
    let mut shutdown_fut = Box::pin(shutdown.wait().fuse());
    loop {
        futures::select_biased! {
            () = shutdown_fut.as_mut() => return,
            msg = rx.recv().fuse() => {
                let Ok(frozen) = msg else { return; };
                if writer_tx.send(WriterEvent::Out(frozen)).await.is_err() {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framing;
    use crate::lifecycle::Shutdown;
    use crate::transports::tls::{install_default_crypto_provider, self_signed_for_loopback};
    use async_channel::{Receiver, bounded};
    use compio::net::TcpListener;
    use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
    use iggy_binary_protocol::consensus::iobuf::Frozen;
    use iggy_binary_protocol::{Command2, GenericHeader, HEADER_SIZE, Message};
    use rustls::RootCertStore;
    use std::net::SocketAddr;
    use std::sync::OnceLock;
    use std::time::Duration;

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

    fn client_cfg_with_empty_roots() -> Arc<rustls::ClientConfig> {
        ensure_provider();
        Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(Arc::new(RootCertStore::empty()))
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

    /// Wire `conn.run(ctx)` against fresh channels and a fresh shutdown.
    #[allow(clippy::future_not_send)]
    fn drive(
        conn: TcpTlsTransportConn,
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
    async fn loopback_round_trip_with_self_signed_cert() {
        let (server_config, cert_chain) = server_cfg();
        let client_config = client_cfg_trusting(&cert_chain);
        let server_name: ServerName<'static> = "localhost".try_into().expect("server name");

        let (client_stream, server_stream) = connected_pair().await;
        let server_conn = TcpTlsTransportConn::new_server(server_stream, server_config);
        let client_conn =
            TcpTlsTransportConn::new_client(client_stream, client_config, server_name);

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
    async fn wrong_cert_handshake_fails_cleanly() {
        let (server_config, _cert_chain) = server_cfg();
        // Client trusts no roots, so the server's self-signed leaf is
        // rejected during handshake.
        let client_config = client_cfg_with_empty_roots();
        let server_name: ServerName<'static> = "localhost".try_into().expect("server name");

        let (client_stream, server_stream) = connected_pair().await;
        let server_conn = TcpTlsTransportConn::new_server(server_stream, server_config);
        let client_conn =
            TcpTlsTransportConn::new_client(client_stream, client_config, server_name);

        let (_server_out, server_in, _server_shutdown, server_handle) = drive(server_conn);
        let (_client_out, client_in, _client_shutdown, client_handle) = drive(client_conn);

        // Both sides bail out of the handshake; ctx is dropped on early
        // return so each `in_tx` closes and the test-side `recv` resolves
        // to `Err(channel closed)` rather than panicking.
        let server_recv = compio::time::timeout(Duration::from_secs(5), server_in.recv()).await;
        let client_recv = compio::time::timeout(Duration::from_secs(5), client_in.recv()).await;
        assert!(
            matches!(server_recv, Ok(Err(_))),
            "server in_rx must close cleanly on handshake reject, got {server_recv:?}"
        );
        assert!(
            matches!(client_recv, Ok(Err(_))),
            "client in_rx must close cleanly on handshake reject, got {client_recv:?}"
        );

        let _ = compio::time::timeout(Duration::from_secs(5), server_handle).await;
        let _ = compio::time::timeout(Duration::from_secs(5), client_handle).await;
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn large_frame_round_trip_through_tls() {
        // 1 MiB body. rustls fragments the plaintext into ~64 records
        // (16 KiB each); the round trip must succeed across the
        // fragmentation boundary.
        const BODY_SIZE: usize = 1024 * 1024;
        let total = HEADER_SIZE + BODY_SIZE;

        let (server_config, cert_chain) = server_cfg();
        let client_config = client_cfg_trusting(&cert_chain);
        let server_name: ServerName<'static> = "localhost".try_into().expect("server name");

        let (client_stream, server_stream) = connected_pair().await;
        let server_conn = TcpTlsTransportConn::new_server(server_stream, server_config);
        let client_conn =
            TcpTlsTransportConn::new_client(client_stream, client_config, server_name);

        let (_server_out, server_in, server_shutdown, server_handle) = drive(server_conn);
        let (client_out, _client_in, client_shutdown, client_handle) = drive(client_conn);

        client_out
            .send(padded(Command2::Request, total))
            .await
            .expect("client send 1 MiB");
        let received = compio::time::timeout(Duration::from_secs(10), server_in.recv())
            .await
            .expect("server recv within 10 s")
            .expect("server frame");
        assert_eq!(received.header().command, Command2::Request);
        assert_eq!(received.header().size as usize, total);

        server_shutdown.trigger();
        client_shutdown.trigger();
        let _ = compio::time::timeout(Duration::from_secs(10), server_handle).await;
        let _ = compio::time::timeout(Duration::from_secs(10), client_handle).await;
    }
}
