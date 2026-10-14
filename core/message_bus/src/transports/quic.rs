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
//! One bidirectional stream **per request** (matches the iggy SDK QUIC
//! client, which opens a fresh `open_bi` per command and `send.finish()`-s
//! after writing the request). [`QuicTransportConn::run`] serves the
//! streams of one connection at once, as many as the stream credit of
//! `[quic] max_concurrent_bidi_streams` lets the client open. Each stream:
//!
//! 1. reads exactly one inbound frame, registers a waiter under its
//!    `request` id, and forwards the frame to [`ActorContext::in_tx`];
//! 2. gets the frame that answers it from a router that reads
//!    [`ActorContext::rx`] (see `ReplyRouter`);
//! 3. writes that frame, then `finish()`-es to deliver pending data + FIN.
//!
//! A stream whose client stops it, or whose reply does not come within
//! `REPLY_WAIT_BACKSTOP`, drops its waiter and resets. The other streams
//! and the connection go on.
//!
//! The handshake itself is driven by [`accept_handshake`] (no bidi
//! captured). To avoid a single slow / hostile peer wedging the entire
//! client plane behind that handshake, the client listener's accept loop
//! pulls bare [`Incoming`] values via
//! [`QuicTransportListener::next_incoming`] and spawns one
//! `accept_handshake` task per incoming.
//!
//! # Zero-copy
//!
//! `compio_quic::SendStream::write<T: IoBuf>` accepts
//! `Frozen<MESSAGE_ALIGN>` directly; the per-bidi reply path issues one
//! `write` per frame (QUIC has no `sendmmsg` analog). Per-message
//! syscalls are the documented trade-off versus the TCP `writev` path;
//! small high-RPS workloads stay on TCP.
//!
//! # 0-RTT
//!
//! 0-RTT is off by default at the rustls layer (`max_early_data_size = 0`).
//! `RecvStream::is_0rtt()` is checked inside the `accept_bi` loop for
//! defense in depth; a `true` here closes the connection with
//! `QUIC_PROTOCOL_VIOLATION`. Per-command 0-RTT enablement requires a
//! per-command idempotence audit before being turned on.

use super::{ActorContext, TransportConn, TransportListener};
use crate::config::QuicTuning;
use crate::framing;
use crate::lifecycle::connection_registry::BusMessage;
use async_channel::Sender;
use compio::BufResult;
use compio::io::AsyncWriteExt;
use compio_quic::{
    Connection, Endpoint, IdleTimeout, Incoming, RecvStream, SendStream, VarInt, congestion,
    crypto::rustls::QuicServerConfig,
};
use futures::FutureExt;
use futures::channel::oneshot;
use futures::stream::{FuturesUnordered, StreamExt};
use iggy_binary_protocol::{Command, GenericHeader, ReplyHeader, RequestHeader};
use server_common::Message;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::mem::offset_of;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

/// QUIC application close codes. Sized so the table fits a single cache
/// line.
pub const QUIC_HANDSHAKE_FAILED: u32 = 0x1001;
pub const QUIC_AUTH_FAILED: u32 = 0x1002;
pub const QUIC_SHUTDOWN: u32 = 0x1003;
pub const QUIC_PROTOCOL_VIOLATION: u32 = 0x1004;

/// Default wall-clock bound on `accept_handshake`. Mirrors
/// [`crate::MessageBusConfig::handshake_grace`]; consumed by the
/// trait-impl `accept` path which has no plumbing for caller config.
/// Production accept loops route through
/// [`crate::client_listener::quic::run`] which threads
/// `MessageBusConfig::handshake_grace` directly.
const DEFAULT_HANDSHAKE_GRACE: Duration = Duration::from_secs(10);

/// Build a [`compio_quic::TransportConfig`] from a runtime
/// [`QuicTuning`] derived from the schema's `[quic]` block.
///
/// Architectural invariants the bus enforces here regardless of the
/// schema (so a misconfigured operator config cannot break the
/// SDK-client plane):
///
/// - Each SDK request opens a fresh bidirectional stream;
///   `tuning.max_concurrent_bidi_streams` caps how many a client
///   connection may hold open at once (clamped to a `u32` `VarInt`).
/// - Zero unidirectional streams (no datagram traffic on this plane).
/// - CUBIC congestion controller (no scheme-level switch yet exposed
///   to operators).
///
/// `keep_alive_interval` and `max_idle_timeout` follow the legacy
/// QUIC server's convention: a `Duration::ZERO` means *disabled* and
/// the corresponding quinn knob is left at quinn's own default.
///
/// # Panics
///
/// Panics if `tuning.max_idle_timeout` is non-zero but outside
/// quinn-proto's `IdleTimeout` range: `TryFrom<Duration>` counts the
/// duration in milliseconds and rejects anything above `VarInt::MAX`
/// (`2^62 - 1`). `QuicConfig::validate` rejects those values at boot.
#[must_use]
pub fn transport_config_from(tuning: &QuicTuning) -> compio_quic::TransportConfig {
    let mut cfg = compio_quic::TransportConfig::default();
    cfg.max_concurrent_bidi_streams(VarInt::from_u32(tuning.max_concurrent_bidi_streams))
        .max_concurrent_uni_streams(VarInt::from_u32(0))
        .stream_receive_window(VarInt::from_u32(tuning.stream_receive_window))
        .receive_window(VarInt::from_u32(tuning.receive_window))
        .send_window(tuning.send_window)
        .initial_mtu(tuning.initial_mtu)
        .congestion_controller_factory(Arc::new(congestion::CubicConfig::default()));

    if !tuning.max_idle_timeout.is_zero() {
        let idle = IdleTimeout::try_from(tuning.max_idle_timeout)
            .expect("quic.max_idle_timeout fits in quinn IdleTimeout (validated by schema)");
        cfg.max_idle_timeout(Some(idle));
    }
    if !tuning.keep_alive_interval.is_zero() {
        cfg.keep_alive_interval(Some(tuning.keep_alive_interval));
    }
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
    /// [`transport_config_from`] hookup.
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

/// Drive the QUIC handshake for one [`Incoming`].
///
/// Wraps the inner handshake driver in `compio::time::timeout` so a
/// slowloris peer cannot pin the per-incoming spawned task beyond
/// `handshake_grace`. The sequence
/// (`incoming.accept` -> `connecting.await`) shares one wall-clock
/// budget; on timeout the spawned future is dropped, which drops the
/// local `Connection` and closes the QUIC session via its own Drop path.
///
/// Returns [`Some`] on a successful handshake (no bidirectional stream
/// captured; per-request bidis are accepted later inside
/// [`QuicTransportConn::run`]). Returns [`None`] for any peer-induced
/// failure (`incoming.accept` rejection, handshake error,
/// handshake-grace exceeded); each non-success path logs.
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
    debug!(%addr, "QUIC handshake complete; bidi accept deferred to run()");
    Some((QuicTransportConn::new(connection), addr))
}

/// Default wall-clock budget for the joint reader+writer drain plus
/// `Connection::close(QUIC_SHUTDOWN, _)` invocation in
/// [`QuicTransportConn::run`]. Mirrors
/// [`crate::transports::tcp_tls::DEFAULT_CLOSE_GRACE`]; the installer
/// overrides it in production via [`QuicTransportConn::with_close_grace`]
/// using `MessageBusConfig::close_grace`.
pub(crate) const DEFAULT_CLOSE_GRACE: Duration = Duration::from_secs(2);

/// How long a stream waits for its reply before it resets. A request whose
/// dispatch never answers (both partition queues full, or a pathologically
/// slow commit) would otherwise hold its stream credit for the life of the
/// connection. Longer than the SDK's whole-request deadline (30s), so the
/// client always gives up first. A reply that comes after the reset is an
/// unclaimed frame (see `ReplyRouter::register`).
const REPLY_WAIT_BACKSTOP: Duration = Duration::from_mins(1);

/// Application error code that resets a stream ending without its reply.
const QUIC_NO_REPLY: u32 = 0x1005;

/// `Command` byte offset shared by every consensus header (after the
/// fixed checksum/cluster/size/view/release prefix).
const COMMAND_OFFSET: usize = 60;

/// How an outbound frame routes to the waiting streams.
enum ReplyRoute {
    /// A `Reply`, for the oldest stream that waits under this request id.
    Keyed(u64),
    /// A terminal/control frame (`Eviction`, etc.) with no `request` field:
    /// deliver to every waiting stream. Filtering these out would silently
    /// swallow session-terminal signals (e.g. `NoSession` ->
    /// `Unauthenticated`), leaving the client to time out.
    Unkeyed,
}

/// Decide how to route an outbound frame. Only `Command::Reply` carries a
/// `request` id; everything else must pass through unfiltered. The id is
/// read as raw bytes, because the deny of an operation this build does not
/// declare echoes that byte, and a checked `ReplyHeader` cast refuses it.
fn reply_route(frame: &BusMessage) -> ReplyRoute {
    let bytes = frame.first().as_slice();
    let is_reply = bytes
        .get(COMMAND_OFFSET)
        .is_some_and(|&command| command == Command::Reply as u8);
    if !is_reply {
        return ReplyRoute::Unkeyed;
    }
    read_u64(bytes, offset_of!(ReplyHeader, request)).map_or(ReplyRoute::Unkeyed, ReplyRoute::Keyed)
}

/// The request id of a client frame, read as raw bytes like [`reply_route`],
/// so a frame with an undeclared operation still has one.
fn request_id(request: &Message<GenericHeader>) -> Option<u64> {
    read_u64(request.as_slice(), offset_of!(RequestHeader, request))
}

fn read_u64(bytes: &[u8], offset: usize) -> Option<u64> {
    bytes
        .get(offset..offset + size_of::<u64>())
        .and_then(|field| field.try_into().ok())
        .map(u64::from_le_bytes)
}

/// Hands the outbound frames of one connection to the streams that wait
/// for them.
///
/// A stream registers its waiter before it forwards its request, so a reply
/// never comes before the stream it answers.
struct ReplyRouter {
    /// Waiting streams by request id, oldest first.
    waiters: HashMap<u64, VecDeque<oneshot::Sender<BusMessage>>>,
    /// Frames that came while no stream waited for them, oldest first.
    unclaimed: VecDeque<BusMessage>,
    /// When `unclaimed` holds this many frames, it drops its oldest one.
    unclaimed_max: usize,
}

impl ReplyRouter {
    fn new(unclaimed_max: usize) -> Self {
        Self {
            waiters: HashMap::new(),
            unclaimed: VecDeque::new(),
            unclaimed_max,
        }
    }

    /// Register a stream that waits for the reply to `request_id`.
    ///
    /// The stream first takes the oldest unclaimed frame that has no request
    /// id or has its own. The SDK replays an explicit transient with the same
    /// request id on a new stream, so a late reply of the first attempt
    /// answers the replay. The replies for other ids ahead of that frame are
    /// dropped, because their streams already ended.
    fn register(&mut self, request_id: u64) -> oneshot::Receiver<BusMessage> {
        let (waiter, reply) = oneshot::channel();
        while let Some(frame) = self.unclaimed.pop_front() {
            if !matches!(reply_route(&frame), ReplyRoute::Keyed(id) if id != request_id) {
                // `reply` is alive, so the send cannot fail.
                let _ = waiter.send(frame);
                return reply;
            }
        }
        self.waiters
            .entry(request_id)
            .or_default()
            .push_back(waiter);
        reply
    }

    /// Hand a reply to the oldest waiter of its request id, and a frame
    /// without one to every waiter, because an eviction ends every request
    /// of the connection. A frame no stream takes stays unclaimed.
    fn route(&mut self, frame: BusMessage) {
        let unclaimed = match reply_route(&frame) {
            ReplyRoute::Keyed(request_id) => self.deliver(request_id, frame),
            ReplyRoute::Unkeyed => self.broadcast(frame),
        };
        if let Some(frame) = unclaimed {
            if self.unclaimed.len() == self.unclaimed_max {
                self.unclaimed.pop_front();
            }
            self.unclaimed.push_back(frame);
        }
    }

    /// Drop the waiters of `request_id` whose streams ended without a reply.
    fn release(&mut self, request_id: u64) {
        if let Some(waiters) = self.waiters.get_mut(&request_id) {
            waiters.retain(|waiter| !waiter.is_canceled());
            if waiters.is_empty() {
                self.waiters.remove(&request_id);
            }
        }
    }

    /// Returns the frame if no waiter took it.
    fn deliver(&mut self, request_id: u64, mut frame: BusMessage) -> Option<BusMessage> {
        let Some(waiters) = self.waiters.get_mut(&request_id) else {
            return Some(frame);
        };
        let unclaimed = loop {
            let Some(waiter) = waiters.pop_front() else {
                break Some(frame);
            };
            match waiter.send(frame) {
                Ok(()) => break None,
                Err(returned) => frame = returned,
            }
        };
        if waiters.is_empty() {
            self.waiters.remove(&request_id);
        }
        unclaimed
    }

    /// Returns the frame if no waiter took it.
    fn broadcast(&mut self, frame: BusMessage) -> Option<BusMessage> {
        let mut delivered = false;
        for waiter in self.waiters.drain().flat_map(|(_, waiters)| waiters) {
            delivered |= waiter.send(frame.clone()).is_ok();
        }
        (!delivered).then_some(frame)
    }
}

/// What the stream futures of [`QuicTransportConn::run`] borrow from it.
struct StreamContext<'a> {
    in_tx: &'a Sender<Message<GenericHeader>>,
    router: &'a RefCell<ReplyRouter>,
    max_message_size: usize,
    reply_backstop: Duration,
    label: &'static str,
    peer: &'a str,
}

impl StreamContext<'_> {
    /// Serve one request stream. Breaks when the inbound queue is gone,
    /// because the connection then has no dispatch.
    #[allow(clippy::future_not_send)]
    async fn serve(&self, mut send: SendStream, mut recv: RecvStream) -> ControlFlow<()> {
        let Self {
            in_tx,
            router,
            max_message_size,
            reply_backstop,
            label,
            peer,
        } = *self;
        // The SDK `finish()`-es after its request, so the stream carries one
        // frame.
        let request = match framing::read_message(&mut recv, max_message_size).await {
            Ok(request) => request,
            Err(e) => {
                debug!(%label, %peer, error = ?e, "quic: bidi read error");
                return ControlFlow::Continue(());
            }
        };
        let Some(request_id) = request_id(&request) else {
            debug!(%label, %peer, "quic: frame shorter than a request header");
            return ControlFlow::Continue(());
        };
        let mut reply = router.borrow_mut().register(request_id);
        if in_tx.send(request).await.is_err() {
            debug!(%label, %peer, "quic: inbound queue dropped");
            return ControlFlow::Break(());
        }
        let replied = {
            let mut stopped = pin!(send.stopped().fuse());
            let mut backstop = pin!(compio::time::sleep(reply_backstop).fuse());
            futures::select_biased! {
                frame = reply => frame.ok(),
                _ = stopped => {
                    debug!(%label, %peer, request_id, "quic: client stopped the stream before its reply");
                    None
                }
                () = backstop => {
                    warn!(%label, %peer, request_id, "quic: no reply within the backstop; resetting the stream");
                    None
                }
            }
        };
        let Some(frame) = replied else {
            drop(reply);
            router.borrow_mut().release(request_id);
            if let Err(e) = send.reset(VarInt::from_u32(QUIC_NO_REPLY)) {
                debug!(%label, %peer, error = ?e, "quic: stream reset failed");
            }
            return ControlFlow::Continue(());
        };
        for fragment in frame.into_fragments() {
            let BufResult(result, _frozen) = send.write_all(fragment).await;
            if let Err(e) = result {
                debug!(%label, %peer, error = ?e, "quic: write failed");
                return ControlFlow::Continue(());
            }
        }
        // `finish()` flushes pending data + a FIN, which tells the SDK the
        // reply is complete.
        if let Err(e) = send.finish() {
            debug!(%label, %peer, error = ?e, "quic: send.finish() failed");
        }
        // Keep the reply alive through graceful shutdown until the peer has
        // acknowledged it. Closing the connection after FIN alone can discard it.
        let _ = send.stopped().await;
        ControlFlow::Continue(())
    }
}

/// A single accepted QUIC connection.
///
/// Per-request bidirectional streams are accepted on demand inside
/// [`Self::run`]; the connection handle is retained so graceful
/// shutdown can fire `Connection::close(QUIC_SHUTDOWN, _)` once the
/// accept loop exits.
pub struct QuicTransportConn {
    connection: Connection,
    close_grace: Duration,
    /// [`REPLY_WAIT_BACKSTOP`]. Tests shorten it.
    reply_backstop: Duration,
}

impl QuicTransportConn {
    /// Construct from an already-handshaked connection.
    #[must_use]
    pub const fn new(connection: Connection) -> Self {
        Self {
            connection,
            close_grace: DEFAULT_CLOSE_GRACE,
            reply_backstop: REPLY_WAIT_BACKSTOP,
        }
    }

    /// Override the wall-clock bound applied to the final
    /// `Connection::close(QUIC_SHUTDOWN, _)` drain. Intended for tests;
    /// the installer plumbs `MessageBusConfig::close_grace` in
    /// production.
    #[must_use]
    pub const fn with_close_grace(mut self, close_grace: Duration) -> Self {
        self.close_grace = close_grace;
        self
    }

    /// Deconstruct back to the raw `Connection`. Currently unused by
    /// production code; retained for symmetry with [`Self::new`] and
    /// for tests that need to drive the connection directly.
    #[must_use]
    pub fn into_inner(self) -> Connection {
        self.connection
    }
}

impl TransportConn for QuicTransportConn {
    #[allow(clippy::future_not_send)]
    async fn run(self, ctx: ActorContext) {
        let Self {
            connection,
            reply_backstop,
            close_grace,
        } = self;
        let ActorContext {
            in_tx,
            rx,
            shutdown,
            conn_shutdown: _,
            max_batch: _,
            max_message_size,
            label,
            peer,
        } = ctx;
        let mut shutdown_fut = Box::pin(shutdown.wait().fuse());
        // Unclaimed frames never outnumber what the outbound queue holds.
        let router = RefCell::new(ReplyRouter::new(rx.capacity().unwrap_or(usize::MAX)));
        let context = StreamContext {
            in_tx: &in_tx,
            router: &router,
            max_message_size,
            reply_backstop,
            label,
            peer: &peer,
        };
        // The stream credit of the connection bounds this set.
        let mut streams = FuturesUnordered::new();

        let drain = loop {
            futures::select! {
                () = shutdown_fut.as_mut() => {
                    debug!(%label, %peer, "quic: shutdown observed");
                    break true;
                }
                accepted = connection.accept_bi().fuse() => match accepted {
                    Ok((send, recv)) => {
                        // Defense-in-depth 0-RTT check (matches the previous
                        // handshake-path guard now that bidi accept is deferred
                        // to here).
                        if recv.is_0rtt() {
                            warn!(%label, %peer, "quic: bidi accepted in 0-RTT window; refusing");
                            connection.close(
                                VarInt::from_u32(QUIC_PROTOCOL_VIOLATION),
                                b"0-RTT not permitted",
                            );
                            break false;
                        }
                        streams.push(context.serve(send, recv));
                    }
                    Err(e) => {
                        debug!(%label, %peer, error = ?e, "quic: accept_bi failed (connection closed)");
                        break false;
                    }
                },
                frame = rx.recv().fuse() => if let Ok(frame) = frame {
                    router.borrow_mut().route(frame);
                } else {
                    debug!(%label, %peer, "quic: mailbox closed");
                    break false;
                },
                served = streams.select_next_some() => if served.is_break() {
                    break false;
                },
            }
        };

        if drain {
            let pending = async {
                while !streams.is_empty() {
                    futures::select! {
                        frame = rx.recv().fuse() => match frame {
                            Ok(frame) => router.borrow_mut().route(frame),
                            Err(_) => break,
                        },
                        served = streams.select_next_some() => if served.is_break() {
                            break;
                        },
                    }
                }
            };
            if compio::time::timeout(close_grace, pending).await.is_err() {
                warn!(%label, %peer, "quic: close grace expired with streams in flight");
            }
        }

        connection.close(VarInt::from_u32(QUIC_SHUTDOWN), b"shutdown");
    }
}

/// Build a [`compio_quic::ServerConfig`] from a cert chain + key pair
/// plus the bus's runtime [`QuicTuning`].
///
/// Applies [`transport_config_from`] and disables 0-RTT; otherwise
/// inherits upstream defaults including `migration: true`. No ALPN is
/// advertised; protocol-version validation lives in the LOGIN command
/// on the caller (the server).
///
/// # Errors
///
/// Returns the underlying `rustls::Error` on cert/key import failure.
pub fn server_config_with_cert(
    cert_chain: Vec<rustls::pki_types::CertificateDer<'static>>,
    key_der: rustls::pki_types::PrivateKeyDer<'static>,
    tuning: &QuicTuning,
) -> Result<compio_quic::ServerConfig, rustls::Error> {
    let mut rustls_cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key_der)?;
    rustls_cfg.max_early_data_size = 0; // 0-RTT off by default
    let crypto = QuicServerConfig::try_from(rustls_cfg)
        .map_err(|e| rustls::Error::General(format!("QUIC crypto: {e}")))?;
    let mut server_cfg = compio_quic::ServerConfig::with_crypto(Arc::new(crypto));
    server_cfg.transport_config(Arc::new(transport_config_from(tuning)));
    Ok(server_cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::{FusedShutdown, Shutdown};
    use async_channel::bounded;
    use compio::io::AsyncWrite;
    use compio_quic::ClientBuilder;
    use iggy_binary_protocol::{Command, GenericHeader, HEADER_SIZE, Operation, SIZE_FIELD_OFFSET};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use server_common::iobuf::Frozen;
    use server_common::{MESSAGE_ALIGN, Message};
    use std::time::Duration;

    const TEST_CLIENT: u128 = 1;
    const ONE_STREAM: u32 = 1;
    const TWO_STREAMS: u32 = 2;
    const FIRST_REQUEST: u64 = 1;
    const SECOND_REQUEST: u64 = 2;
    /// The commit of a reply that no test tells apart from another.
    const ANY_COMMIT: u64 = 0;
    const FIRST_COMMIT: u64 = 1;
    const SECOND_COMMIT: u64 = 2;
    /// Bounds every wait of a test, so a frame that never comes fails it.
    const TEST_WAIT: Duration = Duration::from_secs(5);
    const SHORT_BACKSTOP: Duration = Duration::from_millis(200);
    /// The server only sees that the client stopped, whatever the code.
    const CLIENT_STOP: u32 = 0;
    /// No `Operation` has this discriminant.
    const UNDECLARED_OPERATION: u8 = u8::MAX;
    const UNCLAIMED_MAX: usize = 16;

    fn install_crypto_provider() {
        // Idempotent. Tests in the same process race on
        // `install_default`, so swallow the second-installer error.
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    fn header_only(command: Command) -> Frozen<MESSAGE_ALIGN> {
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
        tuning: &QuicTuning,
    ) -> Endpoint {
        let server_cfg = server_config_with_cert(vec![cert], key, tuning).expect("server config");
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
        async_channel::Sender<BusMessage>,
        async_channel::Receiver<Message<GenericHeader>>,
        Shutdown,
        compio::runtime::JoinHandle<()>,
    ) {
        let (out_tx, out_rx) = bounded::<BusMessage>(16);
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

    fn tuning(max_concurrent_bidi_streams: u32) -> QuicTuning {
        QuicTuning {
            max_concurrent_bidi_streams,
            ..QuicTuning::default()
        }
    }

    /// A served connection whose client can open `max_concurrent_bidi_streams`
    /// streams at once, the client side of it, and the two endpoints, which
    /// must outlive both.
    #[allow(clippy::future_not_send)]
    async fn connect(
        max_concurrent_bidi_streams: u32,
    ) -> (
        QuicTransportConn,
        Connection,
        (QuicTransportListener, Endpoint),
    ) {
        install_crypto_provider();
        let (cert, key) = self_signed();
        let server = server_endpoint(cert.clone(), key, &tuning(max_concurrent_bidi_streams)).await;
        let server_addr = server.local_addr().unwrap();
        let listener = QuicTransportListener::new(server);
        let client = client_endpoint(cert).await;
        let connecting = client
            .connect(server_addr, "localhost", None)
            .expect("connect");
        let (accepted, connection) = futures::join!(listener.accept(), connecting);
        let (conn, _peer) = accepted.expect("accept");
        (
            conn,
            connection.expect("client handshake"),
            (listener, client),
        )
    }

    #[allow(clippy::cast_possible_truncation)]
    fn request(request_id: u64) -> Frozen<MESSAGE_ALIGN> {
        Message::<RequestHeader>::new(HEADER_SIZE)
            .transmute_header(|_, h: &mut RequestHeader| {
                h.command = Command::Request;
                h.size = HEADER_SIZE as u32;
                h.client = TEST_CLIENT;
                // Sessionless, so the header needs no bound session.
                h.operation = Operation::NonReplicated;
                h.request = request_id;
            })
            .into_frozen()
    }

    /// A reply to `request_id`. `commit` tells two replies to one id apart.
    #[allow(clippy::cast_possible_truncation)]
    fn reply_message(request_id: u64, commit: u64) -> Message<GenericHeader> {
        Message::<ReplyHeader>::new(HEADER_SIZE)
            .transmute_header(|_, h: &mut ReplyHeader| {
                h.command = Command::Reply;
                h.size = HEADER_SIZE as u32;
                h.request = request_id;
                h.commit = commit;
            })
            .into_generic()
    }

    fn reply(request_id: u64, commit: u64) -> BusMessage {
        reply_message(request_id, commit).into_frozen().into()
    }

    fn reply_header(frame: &[u8]) -> ReplyHeader {
        *bytemuck::checked::from_bytes::<ReplyHeader>(&frame[..HEADER_SIZE])
    }

    /// Open a stream and send one request on it. The returned half reads the
    /// answer.
    #[allow(clippy::future_not_send)]
    async fn send_request(client: &Connection, request_id: u64) -> RecvStream {
        let (mut send, recv) = client.open_bi_wait().await.expect("open_bi");
        let BufResult(result, _) = send.write_all(request(request_id)).await;
        result.expect("write");
        send.finish().expect("finish");
        recv
    }

    #[allow(clippy::future_not_send)]
    async fn read_answer(recv: &mut RecvStream) -> Message<GenericHeader> {
        compio::time::timeout(
            TEST_WAIT,
            framing::read_message(recv, framing::MAX_MESSAGE_SIZE),
        )
        .await
        .expect("answer within the test wait")
        .expect("answer frame")
    }

    /// The request id of the next frame the server forwards.
    #[allow(clippy::future_not_send)]
    async fn forwarded(inbound: &async_channel::Receiver<Message<GenericHeader>>) -> u64 {
        let frame = compio::time::timeout(TEST_WAIT, inbound.recv())
            .await
            .expect("frame forwarded within the test wait")
            .expect("inbound queue open");
        request_id(&frame).expect("whole request header")
    }

    /// The commit of the reply the router handed to `waiter`, if it got one.
    fn routed_commit(waiter: &mut oneshot::Receiver<BusMessage>) -> Option<u64> {
        let frame = waiter.try_recv().expect("waiter registered")?;
        Some(reply_header(frame.first().as_slice()).commit)
    }

    /// End-to-end frame delivery with one stream at a time: the client
    /// opens N bidis in turn, writing one request per bidi; the server
    /// reads each request, takes its reply from `rx`, writes it, and
    /// `finish()`-es the bidi. Verifies inbound ordering matches what the
    /// client sent.
    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn loopback_round_trip_three_frames() {
        install_crypto_provider();
        let (cert, key) = self_signed();
        let server = server_endpoint(cert.clone(), key, &tuning(ONE_STREAM)).await;
        let server_addr = server.local_addr().unwrap();

        let listener = QuicTransportListener::new(server);
        let server_task = compio::runtime::spawn(async move {
            let (conn, _peer) = listener.accept().await.expect("accept");
            let (out_tx, in_rx, shutdown, handle) = drive(conn);
            // For each accepted bidi the server reads one inbound frame
            // and then awaits exactly one reply on `rx` before
            // accepting the next bidi. Feed three replies in step.
            let a = in_rx.recv().await.unwrap();
            out_tx
                .send(header_only(Command::Reply).into())
                .await
                .unwrap();
            let b = in_rx.recv().await.unwrap();
            out_tx
                .send(header_only(Command::Reply).into())
                .await
                .unwrap();
            let c = in_rx.recv().await.unwrap();
            out_tx
                .send(header_only(Command::Reply).into())
                .await
                .unwrap();
            shutdown.trigger();
            let _ = handle.await;
            (a.header().command, b.header().command, c.header().command)
        });

        let client = client_endpoint(cert).await;
        let connecting = client
            .connect(server_addr, "localhost", None)
            .expect("connect");
        let connection = connecting.await.expect("client handshake");

        // Three sequential bidis, one frame each.
        for command in [Command::Ping, Command::Prepare, Command::Request] {
            let (mut send, _recv) = connection.open_bi_wait().await.expect("open_bi");
            let BufResult(result, _) = send.write_all(header_only(command)).await;
            result.expect("write");
            send.finish().expect("finish");
        }

        let (a, b, c) = compio::time::timeout(Duration::from_secs(5), server_task)
            .await
            .expect("server task within 5s")
            .unwrap();
        assert_eq!(a, Command::Ping);
        assert_eq!(b, Command::Prepare);
        assert_eq!(c, Command::Request);
    }

    /// An oversize size field in the request header makes
    /// `framing::read_message` reject the frame. The `accept_bi` loop
    /// must NOT forward the bad frame to `in_tx`. The test asserts no
    /// inbound frame surfaces within a small grace, then triggers
    /// shutdown.
    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn read_message_reports_oversize_via_run() {
        install_crypto_provider();
        let (cert, key) = self_signed();
        let server = server_endpoint(cert.clone(), key, &QuicTuning::default()).await;
        let server_addr = server.local_addr().unwrap();

        let listener = QuicTransportListener::new(server);
        let server_task = compio::runtime::spawn(async move {
            let (conn, _peer) = listener.accept().await.expect("accept");
            let (_out_tx, in_rx, shutdown, handle) = drive(conn);
            // The bad frame must NOT appear on `in_rx`. A short timeout
            // proves the framing layer rejected the read rather than
            // silently forwarding it: `Ok(Ok(_))` means a frame did
            // surface (test failure); `Err(Elapsed)` or `Ok(Err(...))`
            // both mean no frame was delivered.
            let received = compio::time::timeout(Duration::from_millis(500), in_rx.recv())
                .await
                .is_ok_and(|r| r.is_ok());
            shutdown.trigger();
            let _ = handle.await;
            received
        });

        let client = client_endpoint(cert).await;
        let connecting = client
            .connect(server_addr, "localhost", None)
            .expect("connect");
        let connection = connecting.await.expect("handshake");
        let (mut send, _recv) = connection.open_bi_wait().await.expect("open_bi");
        // Bogus oversize size field at SIZE_FIELD_OFFSET.
        let mut buf = vec![0u8; HEADER_SIZE];
        let bogus = u32::try_from(framing::MAX_MESSAGE_SIZE + 1)
            .unwrap_or(u32::MAX)
            .to_le_bytes();
        buf[SIZE_FIELD_OFFSET..SIZE_FIELD_OFFSET + 4].copy_from_slice(&bogus);
        let BufResult(result, _) = send.write(buf).await;
        result.expect("write");
        send.finish().expect("finish");

        let bad_frame_received = compio::time::timeout(Duration::from_secs(5), server_task)
            .await
            .expect("server task within 5s")
            .unwrap();
        assert!(
            !bad_frame_received,
            "oversize frame must not surface on in_rx",
        );
    }

    /// Streams of one connection run at once, and each reply reaches the
    /// stream that sent its request id.
    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn given_two_streams_when_the_second_reply_is_ready_first_should_answer_it_first() {
        let (conn, client, _endpoints) = connect(TWO_STREAMS).await;
        let (outbound, inbound, shutdown, server) = drive(conn);
        let mut first = send_request(&client, FIRST_REQUEST).await;
        let mut second = send_request(&client, SECOND_REQUEST).await;
        let mut requests = [forwarded(&inbound).await, forwarded(&inbound).await];
        requests.sort_unstable();
        assert_eq!(requests, [FIRST_REQUEST, SECOND_REQUEST]);

        outbound
            .send(reply(SECOND_REQUEST, ANY_COMMIT))
            .await
            .unwrap();
        let answer = read_answer(&mut second).await;
        assert_eq!(reply_header(answer.as_slice()).request, SECOND_REQUEST);
        outbound
            .send(reply(FIRST_REQUEST, ANY_COMMIT))
            .await
            .unwrap();
        let answer = read_answer(&mut first).await;
        assert_eq!(reply_header(answer.as_slice()).request, FIRST_REQUEST);

        shutdown.trigger();
        let _ = server.await;
    }

    /// An eviction ends every request of the connection, so it reaches
    /// every waiting stream.
    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn given_two_pending_streams_when_an_eviction_arrives_should_deliver_it_to_both() {
        let (conn, client, _endpoints) = connect(TWO_STREAMS).await;
        let (outbound, inbound, shutdown, server) = drive(conn);
        let mut first = send_request(&client, FIRST_REQUEST).await;
        let mut second = send_request(&client, SECOND_REQUEST).await;
        forwarded(&inbound).await;
        forwarded(&inbound).await;

        outbound
            .send(header_only(Command::Eviction).into())
            .await
            .unwrap();
        for stream in [&mut first, &mut second] {
            assert_eq!(
                read_answer(stream).await.header().command,
                Command::Eviction
            );
        }

        shutdown.trigger();
        let _ = server.await;
    }

    /// A stream that its client stops gives back its stream credit, so the
    /// next stream does not wait for a reply that never comes.
    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn given_a_stopped_stream_when_its_reply_never_comes_should_still_serve_the_next_stream()
    {
        let (conn, client, _endpoints) = connect(ONE_STREAM).await;
        let (outbound, inbound, shutdown, server) = drive(conn);
        let mut stopped = send_request(&client, FIRST_REQUEST).await;
        assert_eq!(forwarded(&inbound).await, FIRST_REQUEST);
        stopped
            .stop(VarInt::from_u32(CLIENT_STOP))
            .expect("stop the stream");

        let mut next = compio::time::timeout(TEST_WAIT, send_request(&client, SECOND_REQUEST))
            .await
            .expect("the stopped stream gives back its credit");
        assert_eq!(forwarded(&inbound).await, SECOND_REQUEST);
        outbound
            .send(reply(SECOND_REQUEST, ANY_COMMIT))
            .await
            .unwrap();
        let answer = read_answer(&mut next).await;
        assert_eq!(reply_header(answer.as_slice()).request, SECOND_REQUEST);

        shutdown.trigger();
        let _ = server.await;
    }

    /// The backstop resets the one stream that has no reply, and the
    /// connection serves the next stream.
    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn given_a_stream_without_a_reply_when_the_backstop_passes_should_reset_only_that_stream()
    {
        let (mut conn, client, _endpoints) = connect(ONE_STREAM).await;
        conn.reply_backstop = SHORT_BACKSTOP;
        let (outbound, inbound, shutdown, server) = drive(conn);
        let mut unanswered = send_request(&client, FIRST_REQUEST).await;
        assert_eq!(forwarded(&inbound).await, FIRST_REQUEST);
        let reset = compio::time::timeout(TEST_WAIT, unanswered.received_reset())
            .await
            .expect("the backstop ends the stream")
            .expect("reset by the server");
        assert_eq!(reset, Some(VarInt::from_u32(QUIC_NO_REPLY)));

        let mut next = compio::time::timeout(TEST_WAIT, send_request(&client, SECOND_REQUEST))
            .await
            .expect("the reset stream gives back its credit");
        assert_eq!(forwarded(&inbound).await, SECOND_REQUEST);
        outbound
            .send(reply(SECOND_REQUEST, ANY_COMMIT))
            .await
            .unwrap();
        let answer = read_answer(&mut next).await;
        assert_eq!(reply_header(answer.as_slice()).request, SECOND_REQUEST);

        shutdown.trigger();
        let _ = server.await;
    }

    #[test]
    fn given_two_streams_with_one_request_id_when_two_replies_arrive_should_answer_them_in_stream_order()
     {
        let mut router = ReplyRouter::new(UNCLAIMED_MAX);
        let mut first = router.register(FIRST_REQUEST);
        let mut second = router.register(FIRST_REQUEST);
        router.route(reply(FIRST_REQUEST, FIRST_COMMIT));
        router.route(reply(FIRST_REQUEST, SECOND_COMMIT));
        assert_eq!(routed_commit(&mut first), Some(FIRST_COMMIT));
        assert_eq!(routed_commit(&mut second), Some(SECOND_COMMIT));
    }

    /// A late reply waits for the stream that replays its request id. The
    /// replies for other ids ahead of it are dropped, so a later stream with
    /// such an id gets only its own reply.
    #[test]
    fn given_a_reply_without_a_waiting_stream_when_a_stream_with_its_id_registers_should_deliver_it()
     {
        let mut router = ReplyRouter::new(UNCLAIMED_MAX);
        router.route(reply(SECOND_REQUEST, FIRST_COMMIT));
        router.route(reply(FIRST_REQUEST, FIRST_COMMIT));
        let mut replay = router.register(FIRST_REQUEST);
        assert_eq!(routed_commit(&mut replay), Some(FIRST_COMMIT));

        let mut later = router.register(SECOND_REQUEST);
        assert_eq!(routed_commit(&mut later), None);
        router.route(reply(SECOND_REQUEST, SECOND_COMMIT));
        assert_eq!(routed_commit(&mut later), Some(SECOND_COMMIT));
    }

    #[test]
    fn given_a_full_unclaimed_queue_when_another_frame_comes_should_drop_the_oldest() {
        let mut router = ReplyRouter::new(1);
        router.route(reply(FIRST_REQUEST, FIRST_COMMIT));
        router.route(reply(FIRST_REQUEST, SECOND_COMMIT));
        let mut kept = router.register(FIRST_REQUEST);
        assert_eq!(routed_commit(&mut kept), Some(SECOND_COMMIT));
        let mut next = router.register(FIRST_REQUEST);
        assert_eq!(routed_commit(&mut next), None);
    }

    /// The deny of an operation this build does not declare goes to its own
    /// stream only.
    #[test]
    fn given_a_reply_with_an_undeclared_operation_when_routed_should_key_it_by_request() {
        let mut deny = reply_message(FIRST_REQUEST, ANY_COMMIT);
        deny.as_mut_slice()[offset_of!(ReplyHeader, operation)] = UNDECLARED_OPERATION;
        let deny: BusMessage = deny.into_frozen().into();
        assert!(matches!(
            reply_route(&deny),
            ReplyRoute::Keyed(FIRST_REQUEST)
        ));
    }

    #[test]
    fn transport_config_from_default_builds() {
        // Lightweight smoke: confirm the helper builds without panicking
        // when handed the built-in default tuning.
        let _cfg = transport_config_from(&QuicTuning::default());
    }

    #[test]
    fn transport_config_from_zero_durations_skips_quinn_knobs() {
        // Zero values must be treated as "disabled" so the bus does
        // not push `Duration::ZERO` into quinn's IdleTimeout (which
        // would set a 0 ms idle timer and tear every connection down
        // immediately).
        let tuning = QuicTuning {
            keep_alive_interval: Duration::ZERO,
            max_idle_timeout: Duration::ZERO,
            ..QuicTuning::default()
        };
        let _cfg = transport_config_from(&tuning);
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn given_pending_reply_when_shutdown_starts_should_deliver_it_before_closing() {
        let (conn, client, _endpoints) = connect(ONE_STREAM).await;
        let (outbound, inbound, shutdown, mut server) = drive(conn.with_close_grace(TEST_WAIT));
        let mut response = send_request(&client, FIRST_REQUEST).await;
        assert_eq!(forwarded(&inbound).await, FIRST_REQUEST);
        shutdown.trigger();
        assert!(
            compio::time::timeout(SHORT_BACKSTOP, &mut server)
                .await
                .is_err(),
            "shutdown must keep an admitted request alive during the grace"
        );
        outbound
            .send(reply(FIRST_REQUEST, ANY_COMMIT))
            .await
            .unwrap();
        assert_eq!(
            read_answer(&mut response).await.header().command,
            Command::Reply
        );
        compio::time::timeout(TEST_WAIT, server)
            .await
            .expect("the acknowledged reply releases the drain")
            .expect("the server exits normally");
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn given_missing_reply_when_shutdown_grace_expires_should_close_within_the_bound() {
        let (conn, client, _endpoints) = connect(ONE_STREAM).await;
        let (_outbound, inbound, shutdown, server) = drive(conn.with_close_grace(SHORT_BACKSTOP));
        let mut response = send_request(&client, FIRST_REQUEST).await;
        assert_eq!(forwarded(&inbound).await, FIRST_REQUEST);
        shutdown.trigger();
        compio::time::timeout(TEST_WAIT, server)
            .await
            .expect("shutdown must not wait for the 60-second reply backstop")
            .expect("the server exits normally");
        let result = compio::time::timeout(
            TEST_WAIT,
            framing::read_message(&mut response, framing::MAX_MESSAGE_SIZE),
        )
        .await
        .expect("the abandoned stream closes");
        assert!(
            result.is_err(),
            "a request without a reply must not succeed"
        );
    }
}
