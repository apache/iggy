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

//! TCP-TLS transport: `compio_tls::TlsStream<TcpStream>` driven by a
//! single connection task per peer.
//!
//! # Design
//!
//! Single-task model: one task owns the `TlsStream` and `select!`s over
//! the bus shutdown token, the outbound mailbox, and a resumable
//! framing read step. The framing accumulator lives on a [`TlsPumpState`]
//! local to the pump frame and is shared into the cancellable read
//! future via [`SharedAcc`] (an `Rc<UnsafeCell<Owned<MESSAGE_ALIGN>>>`
//! wrapper). Cancellation drops the in-flight slice handle (one `Rc`
//! clone) but the pump-side handle and the underlying allocation
//! survive, so already-committed bytes are not lost across `select!`
//! cancellation.
//!
//! Cancel-safety hinges on `compio_tls::TlsStream::read` resolving
//! through `futures-rustls`'s `Stream::poll_read`, which only consumes
//! bytes from rustls's `received_plaintext` queue on a poll that
//! returns `Ready`. A `Pending`-state cancel therefore delivers zero
//! bytes to caller memory; bytes already in rustls's queue stay there
//! for the next iteration.
//!
//! Writes are run to completion outside any `select!`; the shutdown
//! token signals the *start* of close, never cancels a write that is
//! already in flight.
//!
//! On bus shutdown the task exits the loop and drives the shutdown
//! sequence: `flush()` + `stream.shutdown()`, both wrapped in a single
//! `compio::time::timeout(close_grace, ...)`. A stalled flush counts
//! against the same wall-clock budget as the shutdown step, so a
//! peer that refuses to drain ciphertext cannot stretch close beyond
//! `close_grace`. `TlsStream::shutdown` sends `close_notify` and the
//! underlying TCP `SHUT_WR`; the peer reciprocates and our reader
//! observes EOF on the next `read` (which never fires because we
//! exited the loop already).
//!
//! # Frozen ownership
//!
//! Plaintext TCP preserves `Frozen<MESSAGE_ALIGN>` ownership end-to-end.
//! The TLS plane structurally cannot: rustls's encrypt step copies
//! plaintext into outbound ciphertext along with per-record AEAD tags.
//! `Frozen` is consumed by `write_all`; the wire bytes are a distinct
//! allocation owned by rustls. The receive path pays exactly one
//! structural memcpy (rustls's `Reader::read` from
//! `received_plaintext.chunks[0]` into the caller buffer); user-space
//! framing then reads in-place out of the persistent accumulator.

use super::tls::TlsRole;
use super::{ActorContext, TransportConn};
use crate::lifecycle::BusMessage;
use compio::buf::{BufResult, IoBuf, IoBufMut, SetLen};
use compio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use compio::net::TcpStream;
use compio::tls::{TlsAcceptor, TlsConnector, TlsStream};
use futures::FutureExt;
use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
use iggy_binary_protocol::consensus::iobuf::Owned;
use iggy_binary_protocol::{GenericHeader, HEADER_SIZE, Message};
use iggy_common::IggyError;
use rustls::pki_types::ServerName;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

/// Default wall-clock bound on `TlsStream::shutdown` invocation.
const DEFAULT_CLOSE_GRACE: Duration = Duration::from_secs(2);

/// Default wall-clock bound on the rustls handshake. Mirrors
/// [`crate::MessageBusConfig::handshake_grace`]; set independently
/// here so test / single-conn callers that do not go through the
/// installer get the same DoS-hardening default.
const DEFAULT_HANDSHAKE_GRACE: Duration = Duration::from_secs(10);

/// In-process TCP-TLS transport: a [`TcpStream`] paired with a
/// role-specific rustls configuration.
///
/// Construct with [`Self::new_server`] or [`Self::new_client`]; drive
/// with [`TransportConn::run`]. Override the close grace via
/// [`Self::with_close_grace`] and the handshake grace via
/// [`Self::with_handshake_grace`].
pub struct TcpTlsTransportConn {
    stream: TcpStream,
    role: TlsRole,
    close_grace: Duration,
    handshake_grace: Duration,
}

impl TcpTlsTransportConn {
    /// Server-side connection: drive the rustls server state machine
    /// against `config` and the bytes flowing on `stream`.
    #[must_use]
    pub const fn new_server(stream: TcpStream, config: Arc<rustls::ServerConfig>) -> Self {
        Self {
            stream,
            role: TlsRole::Server(config),
            close_grace: DEFAULT_CLOSE_GRACE,
            handshake_grace: DEFAULT_HANDSHAKE_GRACE,
        }
    }

    /// Client-side connection: drive the rustls client state machine
    /// against `config` and the bytes flowing on `stream`, presenting
    /// `server_name` in SNI.
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
            close_grace: DEFAULT_CLOSE_GRACE,
            handshake_grace: DEFAULT_HANDSHAKE_GRACE,
        }
    }

    /// Override the wall-clock bound on `TlsStream::shutdown` (the
    /// `close_notify` + TCP `SHUT_WR` step). Intended for tests; the
    /// installer plumbs `MessageBusConfig::close_grace` in production.
    #[must_use]
    pub const fn with_close_grace(mut self, close_grace: Duration) -> Self {
        self.close_grace = close_grace;
        self
    }

    /// Override the wall-clock bound on the rustls handshake. The
    /// installer plumbs `MessageBusConfig::handshake_grace` in
    /// production; tests may shrink the budget to drive the timeout
    /// path deterministically.
    #[must_use]
    pub const fn with_handshake_grace(mut self, handshake_grace: Duration) -> Self {
        self.handshake_grace = handshake_grace;
        self
    }
}

impl TransportConn for TcpTlsTransportConn {
    #[allow(clippy::future_not_send)]
    async fn run(self, ctx: ActorContext) {
        let label = ctx.label;
        let peer = ctx.peer.clone();
        let handshake_grace = self.handshake_grace;

        let mut tls =
            match compio::time::timeout(handshake_grace, handshake(self.role, self.stream)).await {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => {
                    warn!(
                        %label,
                        %peer,
                        error = ?e,
                        "TLS handshake failed; closing connection"
                    );
                    return;
                }
                Err(_elapsed) => {
                    warn!(
                        %label,
                        %peer,
                        grace = ?handshake_grace,
                        "TLS handshake exceeded handshake_grace; closing connection"
                    );
                    return;
                }
            };

        run_pump(&mut tls, ctx).await;
        drive_close(&mut tls, self.close_grace, label, &peer).await;
    }
}

/// Perform the role-specific rustls handshake on `stream` and return
/// the encrypted [`TlsStream`].
#[allow(clippy::future_not_send)]
async fn handshake(role: TlsRole, stream: TcpStream) -> std::io::Result<TlsStream<TcpStream>> {
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

/// Shared-ownership wrapper around a persistent `Owned<MESSAGE_ALIGN>`
/// accumulator. Two clones can coexist: the persistent pump-side handle
/// in [`TlsPumpState`] and a transient handle that lives inside an
/// in-flight `tls.read` future. Cancellation of the read future drops
/// the transient handle but leaves the pump-side handle and the
/// underlying allocation intact, so already-committed bytes are not
/// lost across `select!` cancellation.
///
/// # Soundness contract
///
/// * Single-threaded use only. `Rc` and the `PhantomData<*const ()>`
///   marker make the type `!Send + !Sync`; cross-thread aliasing is
///   structurally impossible.
/// * At most one clone calls [`IoBufMut::as_uninit`] / [`SetLen::set_len`]
///   at any one time. The pump invariant satisfies this: while a
///   `tls.read` future is in flight, the pump-side clone is dormant;
///   the future's clone (held inside `compio_buf::Slice`) is the only
///   one whose `IoBufMut` methods compio's read path invokes.
/// * [`Self::reserve_exact_inner`] and [`Self::into_owned`] require
///   exclusive access (Rc strong count == 1); both `debug_assert!` on
///   entry. The pump only calls them between read iterations, when the
///   future and its clone have been dropped.
struct SharedAcc {
    inner: Rc<UnsafeCell<Owned<MESSAGE_ALIGN>>>,
    _not_send: PhantomData<*const ()>,
}

impl SharedAcc {
    fn new(owned: Owned<MESSAGE_ALIGN>) -> Self {
        Self {
            inner: Rc::new(UnsafeCell::new(owned)),
            _not_send: PhantomData,
        }
    }

    fn handle(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
            _not_send: PhantomData,
        }
    }

    /// Grow the underlying `Owned` by `additional` bytes.
    ///
    /// # Panics
    ///
    /// Panics in debug builds if there are outstanding clones (Rc strong
    /// count > 1). Calling `reserve_exact` while a slice is in flight
    /// would invalidate the kernel-visible pointer and is a programming
    /// bug.
    fn reserve_exact_inner(&mut self, additional: usize) {
        debug_assert_eq!(
            Rc::strong_count(&self.inner),
            1,
            "reserve_exact called with outstanding SharedAcc clones"
        );
        // SAFETY: refcount == 1 means no other clone can observe the
        // buffer; the &mut we synthesize is therefore unique.
        let owned = unsafe { &mut *self.inner.get() };
        let _ = IoBufMut::reserve_exact(owned, additional);
    }

    /// Consume the wrapper and recover the underlying `Owned`.
    ///
    /// # Panics
    ///
    /// Panics if there are outstanding clones (Rc strong count > 1).
    fn into_owned(self) -> Owned<MESSAGE_ALIGN> {
        let Ok(cell) = Rc::try_unwrap(self.inner) else {
            panic!("SharedAcc::into_owned called with outstanding clones")
        };
        cell.into_inner()
    }
}

impl IoBuf for SharedAcc {
    fn as_init(&self) -> &[u8] {
        // SAFETY: per type-level soundness contract, no concurrent &mut
        // is derived from any clone while this & is live.
        unsafe { (*self.inner.get()).as_init() }
    }
}

impl IoBufMut for SharedAcc {
    fn as_uninit(&mut self) -> &mut [MaybeUninit<u8>] {
        // SAFETY: per type-level soundness contract.
        unsafe { (*self.inner.get()).as_uninit() }
    }
}

impl SetLen for SharedAcc {
    unsafe fn set_len(&mut self, len: usize) {
        // SAFETY: SetLen contract requires `len` initialized; propagated
        // from caller. UnsafeCell deref is sound per type contract.
        unsafe { (*self.inner.get()).set_len(len) };
    }
}

/// Resumable framing state for the TLS read pump.
///
/// Owned by [`run_pump`]'s stack frame and survives every `select!`
/// cancellation: the cancellable read future borrows `&mut state` only
/// for the duration of one `tls.read.await`; cancelling drops the
/// future (and the slice handle it holds), but `state` itself stays
/// live on the pump frame.
struct TlsPumpState {
    acc: SharedAcc,
    /// Cumulative target length: `HEADER_SIZE` until the header is
    /// parsed, then `total_size` from the header for the body phase.
    target_len: usize,
    have_header: bool,
}

impl TlsPumpState {
    fn new() -> Self {
        Self {
            acc: SharedAcc::new(Owned::<MESSAGE_ALIGN>::with_capacity(HEADER_SIZE)),
            target_len: HEADER_SIZE,
            have_header: false,
        }
    }

    /// Reset to a fresh header-phase accumulator and yield the completed
    /// frame buffer. Caller must verify `acc.as_init().len() == target_len`
    /// before calling.
    fn take_complete_frame(&mut self) -> Owned<MESSAGE_ALIGN> {
        let acc = std::mem::replace(
            &mut self.acc,
            SharedAcc::new(Owned::<MESSAGE_ALIGN>::with_capacity(HEADER_SIZE)),
        );
        self.target_len = HEADER_SIZE;
        self.have_header = false;
        acc.into_owned()
    }
}

/// Per-iteration outcome of the single-task select.
enum PumpAction {
    /// Bus shutdown observed; exit the loop.
    Shutdown,
    /// Outbound message ready; write it to the wire.
    Send(BusMessage),
    /// Inbound read step completed: `Some` if a full frame was decoded,
    /// `None` if only partial bytes were committed (loop again).
    Recv(Result<Option<Message<GenericHeader>>, ()>),
    /// Mailbox closed: nothing more to send.
    MailboxClosed,
}

/// One iteration of the resumable read state machine.
///
/// Issues exactly one `tls.read.await` per call. Compio's TLS read path
/// is byte-level cancel-safe at this granularity: rustls advances its
/// plaintext queue only when `poll_read` returns `Ready`, so a
/// `Pending`-state cancel delivers zero bytes to caller memory.
/// Persistent state on `TlsPumpState` survives the cancellation; the
/// only thing that drops with the cancelled future is the slice handle
/// (an `Rc` clone) plus a fresh per-iteration scratch allocation - none
/// of which carry committed bytes.
#[allow(clippy::future_not_send)]
async fn read_step(
    state: &mut TlsPumpState,
    tls: &mut TlsStream<TcpStream>,
    max_message_size: usize,
) -> Result<Option<Message<GenericHeader>>, IggyError> {
    let filled = state.acc.as_init().len();
    debug_assert!(filled < state.target_len);

    // Build a `Slice<SharedAcc>` over the unfilled region. The handle
    // bumps the Rc strong count to 2; on cancel-Pending the future
    // drops, the slice drops, and the count returns to 1.
    let slice = state.acc.handle().slice(filled..state.target_len);
    // The returned `Slice<SharedAcc>` carries one `Rc` clone; binding it
    // to `slice_back` (then dropping at end of statement) ensures the
    // count returns to 1 before any subsequent `state.acc` access.
    let BufResult(res, slice_back) = tls.read(slice).await;
    let n = match res {
        Ok(0) => return Err(IggyError::ConnectionClosed),
        Ok(n) => n,
        Err(_) => return Err(IggyError::TcpError),
    };
    drop(slice_back);

    let new_filled = filled + n;
    debug_assert_eq!(state.acc.as_init().len(), new_filled);

    // Header parse transition: as soon as we have the full 256-byte
    // header, validate the size field and either finalize a header-only
    // frame or grow the accumulator for the body.
    if !state.have_header && new_filled >= HEADER_SIZE {
        let bytes = state.acc.as_init();
        let total_size = u32::from_le_bytes(
            bytes[48..52]
                .try_into()
                .map_err(|_| IggyError::InvalidCommand)?,
        ) as usize;
        if !(HEADER_SIZE..=max_message_size).contains(&total_size) {
            return Err(IggyError::InvalidCommand);
        }
        if total_size == HEADER_SIZE {
            let owned = state.take_complete_frame();
            return Message::<GenericHeader>::try_from(owned)
                .map(Some)
                .map_err(|_| IggyError::InvalidCommand);
        }
        state.acc.reserve_exact_inner(total_size - HEADER_SIZE);
        state.target_len = total_size;
        state.have_header = true;
    }

    if state.acc.as_init().len() == state.target_len {
        let owned = state.take_complete_frame();
        return Message::<GenericHeader>::try_from(owned)
            .map(Some)
            .map_err(|_| IggyError::InvalidCommand);
    }

    Ok(None)
}

/// Drive the TLS connection until shutdown, peer EOF, or unrecoverable
/// I/O error.
///
/// # Cancel-safety model
///
/// The select arms are: bus shutdown (cancel-safe channel), outbound
/// mailbox (cancel-safe channel), and one resumable [`read_step`] call.
///
/// `read_step` issues exactly one `tls.read.await` per invocation.
/// `compio_tls::TlsStream::read` resolves through `futures-rustls`'s
/// `Stream::poll_read`, which only consumes bytes from rustls's
/// `received_plaintext` queue on the poll that returns `Ready`. A
/// `Pending`-state cancel therefore delivers zero bytes to caller
/// memory; bytes already in rustls's queue stay there for the next
/// iteration.
///
/// The persistent accumulator lives on [`TlsPumpState`] which is owned
/// by this function's stack frame, NOT inside the cancellable future.
/// Cancellation drops only the [`SharedAcc`] handle held by the
/// in-flight `compio_buf::Slice` (one `Rc` clone). The pump-side clone
/// outlives the cancellation; committed bytes survive.
///
/// Writes are run to completion outside any `select!`. The shutdown
/// token signals the *start* of close, never cancels an in-flight
/// write.
///
/// # F10 (WS / WSS) is unaffected by this fix
///
/// The WebSocket variant of this hazard requires an analogous resumable
/// shape; it is upstream-blocked on `compio_ws 0.4`'s `split` plus
/// `next_item` API and is tracked as a known limitation.
#[allow(clippy::future_not_send)]
async fn run_pump(tls: &mut TlsStream<TcpStream>, ctx: ActorContext) {
    let ActorContext {
        in_tx,
        rx,
        shutdown,
        max_batch,
        max_message_size,
        label,
        peer,
        ..
    } = ctx;

    let mut shutdown_fut = Box::pin(shutdown.wait().fuse());
    let mut state = TlsPumpState::new();
    let mut batch: Vec<BusMessage> = Vec::with_capacity(max_batch);

    loop {
        let action = {
            let read_fut = read_step(&mut state, tls, max_message_size);
            let recv_fut = rx.recv();
            futures::pin_mut!(read_fut);
            futures::pin_mut!(recv_fut);

            futures::select_biased! {
                () = shutdown_fut.as_mut() => PumpAction::Shutdown,
                msg = recv_fut.fuse() => msg.map_or(PumpAction::MailboxClosed, PumpAction::Send),
                res = read_fut.fuse() => PumpAction::Recv(res.map_err(|e| {
                    debug!(%label, %peer, error = ?e, "tls reader: read error");
                })),
            }
        };

        match action {
            PumpAction::Shutdown => {
                debug!(%label, %peer, "tls pump: shutdown observed");
                return;
            }
            PumpAction::MailboxClosed => {
                debug!(%label, %peer, "tls pump: mailbox closed");
                return;
            }
            PumpAction::Send(first) => {
                // Drain mailbox up to `max_batch` and emit a single
                // `tls.flush()` at the end. rustls produces one TLS
                // record per `write_all` call; coalescing the flush
                // turns N record-with-AEAD-tag emissions into one
                // wakeup of the underlying TCP writer.
                batch.push(first);
                while batch.len() < max_batch {
                    match rx.try_recv() {
                        Ok(m) => batch.push(m),
                        Err(_) => break,
                    }
                }
                let drained = batch.len();
                for msg in batch.drain(..) {
                    let len = msg.buf_len();
                    let compio::BufResult(result, _) = tls.write_all(msg).await;
                    if let Err(e) = result {
                        warn!(
                            %label,
                            %peer,
                            error = ?e,
                            bytes = len,
                            batch_len = drained,
                            "tls writer: write_all failed"
                        );
                        return;
                    }
                }
                if let Err(e) = tls.flush().await {
                    warn!(
                        %label,
                        %peer,
                        error = ?e,
                        batch_len = drained,
                        "tls writer: flush failed"
                    );
                    return;
                }
            }
            PumpAction::Recv(Ok(Some(frame))) => {
                if in_tx.send(frame).await.is_err() {
                    debug!(%label, %peer, "tls reader: inbound queue dropped");
                    return;
                }
            }
            PumpAction::Recv(Ok(None)) => {
                // Partial progress; loop and continue accumulating.
            }
            PumpAction::Recv(Err(())) => return,
        }
    }
}

/// Bounded-grace cooperative close: flush any rustls-buffered
/// ciphertext, then issue a `TlsStream::shutdown`, with both steps
/// sharing one `close_grace` budget.
///
/// Shutdown is a transaction - no `select!` racing against the token.
/// A stalled `flush` (peer refusing to drain ciphertext) counts
/// against the same wall-clock as the shutdown step, so close cannot
/// be stretched beyond `close_grace`. On timeout we drop the stream
/// regardless; the OS sends RST and the peer sees an unclean close,
/// an accepted degraded close.
#[allow(clippy::future_not_send)]
async fn drive_close(
    tls: &mut TlsStream<TcpStream>,
    close_grace: Duration,
    label: &'static str,
    peer: &str,
) {
    let result = compio::time::timeout(close_grace, async {
        if let Err(e) = tls.flush().await {
            debug!(%label, %peer, error = ?e, "tls close: flush error");
        }
        if let Err(e) = tls.shutdown().await {
            debug!(%label, %peer, error = ?e, "tls close: shutdown error");
        }
    })
    .await;
    if result.is_err() {
        warn!(
            %label,
            %peer,
            grace_ms = close_grace.as_millis(),
            "tls close: grace exceeded"
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
            shutdown: crate::lifecycle::FusedShutdown::single(token),
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
        let client_config = client_cfg_with_empty_roots();
        let server_name: ServerName<'static> = "localhost".try_into().expect("server name");

        let (client_stream, server_stream) = connected_pair().await;
        let server_conn = TcpTlsTransportConn::new_server(server_stream, server_config);
        let client_conn =
            TcpTlsTransportConn::new_client(client_stream, client_config, server_name);

        let (_server_out, server_in, _server_shutdown, server_handle) = drive(server_conn);
        let (_client_out, client_in, _client_shutdown, client_handle) = drive(client_conn);

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

    #[test]
    fn shared_acc_handle_count() {
        let acc = SharedAcc::new(Owned::<MESSAGE_ALIGN>::with_capacity(HEADER_SIZE));
        assert_eq!(Rc::strong_count(&acc.inner), 1);

        let h1 = acc.handle();
        assert_eq!(Rc::strong_count(&acc.inner), 2);
        assert_eq!(Rc::strong_count(&h1.inner), 2);

        let h2 = acc.handle();
        assert_eq!(Rc::strong_count(&acc.inner), 3);

        drop(h1);
        assert_eq!(Rc::strong_count(&acc.inner), 2);
        drop(h2);
        assert_eq!(Rc::strong_count(&acc.inner), 1);
    }

    #[test]
    fn shared_acc_into_owned_after_handle_dropped() {
        let mut acc = SharedAcc::new(Owned::<MESSAGE_ALIGN>::with_capacity(8));
        // Write some bytes through one of the IoBufMut impls and then
        // mark them initialized via SetLen, mirroring what the slice in
        // an in-flight read would do on Ready.
        unsafe {
            let dst = acc.as_uninit();
            for (i, slot) in dst.iter_mut().take(4).enumerate() {
                slot.write(u8::try_from(i).unwrap());
            }
            acc.set_len(4);
        }
        let h = acc.handle();
        drop(h);
        let owned = acc.into_owned();
        assert_eq!(owned.as_slice(), &[0u8, 1, 2, 3]);
    }

    #[test]
    #[should_panic(expected = "outstanding clones")]
    fn shared_acc_into_owned_panics_with_outstanding_clone() {
        let acc = SharedAcc::new(Owned::<MESSAGE_ALIGN>::with_capacity(HEADER_SIZE));
        let _h = acc.handle();
        let _ = acc.into_owned();
    }

    #[test]
    fn shared_acc_reserve_exact_grows_underlying() {
        let mut acc = SharedAcc::new(Owned::<MESSAGE_ALIGN>::with_capacity(HEADER_SIZE));
        // Request enough headroom that the AVec must grow past the
        // first MESSAGE_ALIGN block.
        let additional = MESSAGE_ALIGN * 4;
        acc.reserve_exact_inner(additional);
        // SAFETY: type-level invariant - exclusive access since refcount == 1.
        let cap = unsafe { (*acc.inner.get()).buf_capacity() };
        assert!(
            cap >= additional,
            "capacity must accommodate request, got {cap}"
        );
    }

    #[test]
    fn tls_pump_state_take_complete_frame_resets_phase() {
        let mut state = TlsPumpState::new();
        // Pretend we filled HEADER_SIZE worth of header.
        unsafe {
            let dst = state.acc.as_uninit();
            for (i, slot) in dst.iter_mut().take(HEADER_SIZE).enumerate() {
                slot.write(u8::try_from(i & 0xff).unwrap());
            }
            state.acc.set_len(HEADER_SIZE);
        }
        state.have_header = true;
        state.target_len = HEADER_SIZE;
        let owned = state.take_complete_frame();
        assert_eq!(owned.as_slice().len(), HEADER_SIZE);
        assert!(!state.have_header);
        assert_eq!(state.target_len, HEADER_SIZE);
    }
}
