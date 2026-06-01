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

//! Plaintext WS transport: `compio_ws::WebSocketStream<TcpStream>`
//! driven by a single connection task per peer.
//!
//! # Architecture
//!
//! Replaces the prior post-handshake unwrap-to-bare-TcpStream + manual
//! RFC 6455 frame parser with the compio-native stack:
//!
//! 1. The HTTP-Upgrade handshake runs externally
//!    (`installer::install_client_ws_fd` calls
//!    `compio_ws::accept_async`); the resulting
//!    `WebSocketStream<TcpStream>` is handed to [`WsTransportConn::new_server`].
//! 2. A single connection task owns the `WebSocketStream` and
//!    `select!`s over the bus shutdown token, the outbound mailbox,
//!    and `ws.read()`.
//! 3. On shutdown the task sends a WS Close frame, flushes, and drops.
//!
//! # Cancel safety
//!
//! `compio_ws::WebSocketStream::read` (0.3.1) is **not cancel-safe**: it
//! awaits the underlying `compio_io::compat::SyncStream::fill_read_buf`,
//! which submits the read buffer to `io_uring` and only restores it on
//! await completion. Dropping `read()` mid-fill (e.g. via a losing
//! `select!` arm) leaves `compio-io 0.9.1`'s `Buffer(Option<Slice>)` at
//! `None` permanently; the next read access panics
//! `"buffer was submitted for io and never returned"` (a compio-io
//! drop-safety bug fixed only in the 0.10 line).
//!
//! `compio_ws` 0.3.1 exposes no `split()` (no `Sink`/`Stream` impls), so
//! the TCP-transport split-reader/writer pattern can't be reused here:
//! one `&mut WebSocketStream` must serve both reads and writes. To avoid
//! ever dropping the read, the pump runs **serialized** request → reply
//! and never races the read against an outbound `recv` arm. Shutdown is
//! delivered out-of-band via `libc::shutdown(SHUT_RD)` on the underlying
//! `TcpStream` fd (see [`spawn_shutdown_watchdog`]); the parked
//! `io_uring` read SQE completes with `Ok(0)` / EOF and the loop exits.
//!
//! The remaining narrow `select!` is the reply wait (`rx.recv()` vs
//! `shutdown`). Both sides are cancel-safe (`async_channel::Receiver` +
//! `FusedShutdown`), neither touches a compio buffer.
//!
//! Limitation: if `handle_client_request` silently drops a request (e.g.
//! transient consensus failure, dedup-absorbed) no reply lands in `rx`
//! and the connection stays parked on `rx.recv()` until the bus-wide
//! shutdown token fires or the per-connection shutdown is triggered. The
//! peer's FIN is not observed mid-wait. SDK clients reconnect after their
//! read timeout; the orphaned server-side pump exits at next shutdown.
//!
//! # Pings
//!
//! Inbound `Message::Ping` is queued by tungstenite as an auto-Pong
//! reply; the `WebSocketStream::read` flush before delivery drains the
//! queue, so the application code only needs to ignore Pings. No
//! explicit Pong send required.

use super::{ActorContext, TransportConn};
use crate::lifecycle::BusMessage;
use bytes::Bytes;
use compio::driver::{SharedFd, ToSharedFd};
use compio::net::TcpStream;
use compio::ws::WebSocketStream;
use compio::ws::tungstenite::Message as WsMessage;
use futures::FutureExt;
use futures::select_biased;
use iggy_binary_protocol::{GenericHeader, read_size_field};
use server_common::{MESSAGE_ALIGN, Message};
use std::io;
use std::os::fd::AsRawFd;
use std::time::Duration;
use tracing::{debug, warn};

/// Default wall-clock bound on the WS Close + drop sequence.
const DEFAULT_CLOSE_GRACE: Duration = Duration::from_secs(2);

/// Errors decoded from a WS `Message::Binary` payload.
#[derive(Debug)]
pub(in crate::transports) enum FrameDecodeError {
    BadHeader,
    BadSize,
}

/// Decode one consensus `Message<GenericHeader>` from a raw WS Binary
/// payload. Reused by [`super::wss`] over the TLS-protected channel.
///
/// `max_message_size` is the per-bus wire cap threaded through
/// [`ActorContext::max_message_size`]; an operator-lowered limit must
/// reject the same way TCP/QUIC paths do (see `framing::read_message`).
pub(in crate::transports) fn decode_consensus_frame(
    body: &[u8],
    max_message_size: usize,
) -> Result<Message<GenericHeader>, FrameDecodeError> {
    if body.len() < iggy_binary_protocol::HEADER_SIZE {
        return Err(FrameDecodeError::BadHeader);
    }
    let total_size = read_size_field(body).ok_or(FrameDecodeError::BadHeader)? as usize;
    if !(iggy_binary_protocol::HEADER_SIZE..=max_message_size).contains(&total_size)
        || total_size != body.len()
    {
        return Err(FrameDecodeError::BadSize);
    }
    // `Message<GenericHeader>` requires `MESSAGE_ALIGN`-aligned backing
    // memory; the alignment exists so storage-path reads can land
    // straight into io_uring O_DIRECT buffers without a bounce copy. WS
    // payloads have no alignment guarantee, so the only way to satisfy
    // the invariant is one allocate-and-copy here.
    let owned = server_common::iobuf::Owned::<MESSAGE_ALIGN>::copy_from_slice(body);
    Message::<GenericHeader>::try_from(owned).map_err(|_| FrameDecodeError::BadHeader)
}

/// A single WebSocket connection.
///
/// Holds an already-upgraded `WebSocketStream<TcpStream>`. Construct
/// with [`Self::new_server`] for accepted server-role connections or
/// [`Self::new_client`] for outbound dialer paths / tests.
pub struct WsTransportConn {
    stream: WebSocketStream<TcpStream>,
    close_grace: Duration,
}

impl WsTransportConn {
    #[must_use]
    pub const fn new_server(stream: WebSocketStream<TcpStream>) -> Self {
        Self {
            stream,
            close_grace: DEFAULT_CLOSE_GRACE,
        }
    }

    #[must_use]
    pub const fn new_client(stream: WebSocketStream<TcpStream>) -> Self {
        Self {
            stream,
            close_grace: DEFAULT_CLOSE_GRACE,
        }
    }

    /// Override the wall-clock bound on the close sequence.
    #[must_use]
    pub const fn with_close_grace(mut self, close_grace: Duration) -> Self {
        self.close_grace = close_grace;
        self
    }
}

impl TransportConn for WsTransportConn {
    #[allow(clippy::future_not_send)]
    async fn run(self, ctx: ActorContext) {
        let label = ctx.label;
        let peer = ctx.peer.clone();
        let mut ws = self.stream;
        // Capture a refcounted fd handle so the shutdown watchdog can
        // wake the reader's parked `io_uring` read via
        // `libc::shutdown(SHUT_RD)`. `read()` must never sit inside a
        // `select!` against an outbound or shutdown arm: compio-io
        // 0.9.1's `SyncStream::fill_read_buf` poisons the buffer on
        // drop and panics the next access. The watchdog wakes the
        // parked SQE with `Ok(0)` instead.
        let shared_fd = ws.get_ref().to_shared_fd();
        spawn_shutdown_watchdog(shared_fd, ctx.shutdown.clone(), label, peer.clone());
        run_pump(&mut ws, ctx).await;
        drive_close(&mut ws, self.close_grace, label, &peer).await;
    }
}

/// Drive the WS connection until shutdown, peer Close, or an
/// unrecoverable error.
///
/// Serialized request → reply loop: read one inbound frame, forward to
/// `in_tx`, await its reply on `rx`, drain any additional buffered
/// replies up to `max_batch`, write all and flush once. `read()` is
/// **never** raced against an outbound `recv` arm or the shutdown
/// token, which is the only safe pattern under compio-io 0.9.1 (see
/// module header). Shutdown is observed in two places:
///
/// * Mid-read-park: the watchdog calls `libc::shutdown(SHUT_RD)` on the
///   underlying fd; the SQE completes with `Ok(0)` and `ws.read()`
///   returns an error that exits the loop.
/// * Mid-reply-wait: a narrow `select!` over `rx.recv()` vs the
///   shutdown future. Both sides are cancel-safe and neither touches a
///   compio buffer.
#[allow(clippy::future_not_send)]
async fn run_pump(ws: &mut WebSocketStream<TcpStream>, ctx: ActorContext) {
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

    // TODO(compio): when the workspace bumps `compio` past 0.18 (i.e. picks
    // up `compio-io >= 0.10`, which fixes `SyncStream::fill_read_buf` so
    // dropping the read future no longer poisons the buffer), restore the
    // original 3-arm `select_biased!(shutdown, recv, read)` pump and delete
    // the watchdog -- the serialized req->reply loop below is only here to
    // sidestep the 0.9.1 drop-safety bug. compio_ws 0.4 also gains
    // `Sink + Stream`, so a split reader/writer becomes preferable; see the
    // `tcp.rs` pattern.
    loop {
        // Read inbound. Never raced -> never dropped -> compio buffer safe.
        let msg = match ws.read().await {
            Ok(m) => m,
            Err(e) => {
                debug!(%label, %peer, error = ?e, "ws reader: read error");
                return;
            }
        };

        match msg {
            WsMessage::Binary(bytes) => {
                let frame = match decode_consensus_frame(&bytes, max_message_size) {
                    Ok(f) => f,
                    Err(e) => {
                        warn!(%label, %peer, error = ?e, "ws reader: bad consensus frame");
                        return;
                    }
                };
                if in_tx.send(frame).await.is_err() {
                    debug!(%label, %peer, "ws reader: inbound queue dropped");
                    return;
                }

                // Wait for the matching reply. Selecting `rx.recv()` against
                // `shutdown_fut` is safe: `async_channel::Receiver::recv` is
                // cancel-safe and the shutdown future holds no compio state.
                let first = select_biased! {
                    () = shutdown_fut.as_mut() => {
                        debug!(%label, %peer, "ws pump: shutdown during reply wait");
                        return;
                    }
                    res = rx.recv().fuse() => if let Ok(m) = res {
                        m
                    } else {
                        debug!(%label, %peer, "ws pump: mailbox closed");
                        return;
                    },
                };

                // Drain any additional pending replies up to `max_batch`,
                // send all, flush once. tungstenite buffers each `send`
                // into its outbound queue; a single trailing `flush`
                // collapses N writev syscalls to 1 per drain.
                let mut batch: Vec<BusMessage> = Vec::with_capacity(max_batch);
                batch.push(first);
                while batch.len() < max_batch {
                    match rx.try_recv() {
                        Ok(m) => batch.push(m),
                        Err(_) => break,
                    }
                }
                let drained = batch.len();
                #[allow(clippy::iter_with_drain)]
                for m in batch.drain(..) {
                    if let Err(e) = ws.send(WsMessage::Binary(Bytes::from_owner(m))).await {
                        warn!(%label, %peer, error = ?e, batch_len = drained, "ws writer: send failed");
                        return;
                    }
                }
                if let Err(e) = ws.flush().await {
                    warn!(%label, %peer, error = ?e, batch_len = drained, "ws writer: flush failed");
                    return;
                }
            }
            WsMessage::Ping(_) | WsMessage::Pong(_) => {
                // Tungstenite queues an auto-Pong for inbound Pings; the
                // `read()` flush before delivery drains it. No reply
                // expected from the bus, so loop straight back to the
                // next read instead of awaiting `rx`.
            }
            WsMessage::Close(_) => {
                debug!(%label, %peer, "ws reader: peer initiated close");
                return;
            }
            WsMessage::Text(_) | WsMessage::Frame(_) => {
                warn!(%label, %peer, "ws reader: unexpected text/raw frame, closing");
                return;
            }
        }
    }
}

/// Detached watchdog: wait for the bus / per-connection shutdown token,
/// then call `libc::shutdown(fd, SHUT_RD)` on the underlying socket.
/// The parked `io_uring` read SQE inside `ws.read()` completes with
/// `Ok(0)`, the read returns an error (or `Ok(0)`-style EOF), and
/// `run_pump` exits without ever dropping the read future. Mirrors the
/// shutdown model used by `tcp.rs` (necessary here because the WS
/// stream is not splittable and compio-io 0.9.1 reads are not
/// drop-safe).
#[allow(clippy::future_not_send)]
fn spawn_shutdown_watchdog(
    shared_fd: SharedFd<socket2::Socket>,
    shutdown: crate::lifecycle::FusedShutdown,
    label: &'static str,
    peer: String,
) {
    compio::runtime::spawn(async move {
        shutdown.wait().await;
        // SAFETY: `shared_fd` is a refcounted clone obtained from the
        // still-live `TcpStream` borrowed through `WebSocketStream::get_ref`;
        // the watchdog's clone keeps the kernel fd open across the syscall
        // independently of the pump's own ownership.
        let raw_fd = shared_fd.as_raw_fd();
        let rc = unsafe { libc::shutdown(raw_fd, libc::SHUT_RD) };
        if rc != 0 {
            let err = io::Error::last_os_error();
            // ENOTCONN is expected if the peer closed first.
            if err.raw_os_error() != Some(libc::ENOTCONN) {
                debug!(%label, %peer, error = ?err, "ws watchdog: SHUT_RD returned");
            }
        }
    })
    .detach();
}

/// Best-effort cooperative close: send WS Close frame, flush, then drop
/// the stream. Bounded by `close_grace`; on timeout the OS sends RST.
#[allow(clippy::future_not_send)]
async fn drive_close(
    ws: &mut WebSocketStream<TcpStream>,
    close_grace: Duration,
    label: &'static str,
    peer: &str,
) {
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
            "ws close: grace exceeded"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framing;
    use crate::lifecycle::Shutdown;
    use async_channel::{Receiver, Sender, bounded};
    use compio::net::TcpListener;
    use iggy_binary_protocol::{Command2, GenericHeader, HEADER_SIZE};
    use server_common::Message;
    use server_common::iobuf::Frozen;
    use std::time::Duration;

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
    async fn ws_pair() -> (WebSocketStream<TcpStream>, WebSocketStream<TcpStream>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let connect = async {
            let stream = TcpStream::connect(addr).await.expect("connect");
            let (ws, _resp) = compio_ws::client_async("ws://127.0.0.1/", stream)
                .await
                .expect("client_async");
            ws
        };
        let accept = async {
            let (stream, _peer) = listener.accept().await.expect("accept");
            compio_ws::accept_async(stream).await.expect("accept_async")
        };
        let (client, server) = futures::join!(connect, accept);
        (client, server)
    }

    #[allow(clippy::future_not_send)]
    fn drive(
        conn: WsTransportConn,
    ) -> (
        Sender<Frozen<MESSAGE_ALIGN>>,
        Receiver<Message<GenericHeader>>,
        Shutdown,
        compio::runtime::JoinHandle<()>,
    ) {
        drive_with_cap(conn, framing::MAX_MESSAGE_SIZE)
    }

    #[allow(clippy::future_not_send)]
    fn drive_with_cap(
        conn: WsTransportConn,
        max_message_size: usize,
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
            max_message_size,
            label: "test",
            peer: "test".to_owned(),
        };
        let handle = compio::runtime::spawn(async move { conn.run(ctx).await });
        (out_tx, in_rx, shutdown, handle)
    }

    /// Raw-send a consensus frame over a client `WebSocketStream` (the
    /// production WS transport is server-role only, so tests drive the
    /// client side directly rather than through `run()`).
    #[allow(clippy::future_not_send)]
    async fn raw_send(ws: &mut WebSocketStream<TcpStream>, frame: Frozen<MESSAGE_ALIGN>) {
        ws.send(WsMessage::Binary(Bytes::from_owner(frame)))
            .await
            .expect("client raw send");
    }

    /// Raw-read one consensus frame from a client `WebSocketStream`.
    #[allow(clippy::future_not_send)]
    async fn raw_recv(ws: &mut WebSocketStream<TcpStream>) -> Message<GenericHeader> {
        loop {
            match ws.read().await.expect("client raw read") {
                WsMessage::Binary(bytes) => {
                    return decode_consensus_frame(&bytes, framing::MAX_MESSAGE_SIZE)
                        .expect("decode client frame");
                }
                WsMessage::Ping(_) | WsMessage::Pong(_) => {}
                other => panic!("unexpected client ws frame: {other:?}"),
            }
        }
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn ws_loopback_round_trip() {
        let (mut client_ws, server_ws) = ws_pair().await;
        let server_conn = WsTransportConn::new_server(server_ws);
        let (server_out, server_in, server_shutdown, server_handle) = drive(server_conn);

        // Client raw-sends a Request; the server pump reads it.
        raw_send(&mut client_ws, header_only(Command2::Request)).await;
        let received = compio::time::timeout(Duration::from_secs(5), server_in.recv())
            .await
            .expect("server recv within 5 s")
            .expect("server frame");
        assert_eq!(received.header().command, Command2::Request);

        // Server replies via its outbound mailbox; the serial pump writes
        // the reply on the same bidi the request arrived on, client reads.
        server_out
            .send(header_only(Command2::Reply))
            .await
            .expect("server send");
        let reply = compio::time::timeout(Duration::from_secs(5), raw_recv(&mut client_ws))
            .await
            .expect("client recv within 5 s");
        assert_eq!(reply.header().command, Command2::Reply);

        server_shutdown.trigger();
        let _ = compio::time::timeout(Duration::from_secs(5), server_handle).await;
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn ws_large_frame_round_trip() {
        const BODY_SIZE: usize = 1024 * 1024;
        let total = HEADER_SIZE + BODY_SIZE;

        let (mut client_ws, server_ws) = ws_pair().await;
        let server_conn = WsTransportConn::new_server(server_ws);
        let (_server_out, server_in, server_shutdown, server_handle) = drive(server_conn);

        raw_send(&mut client_ws, padded(Command2::Request, total)).await;
        let received = compio::time::timeout(Duration::from_secs(15), server_in.recv())
            .await
            .expect("server recv within 15 s")
            .expect("server frame");
        assert_eq!(received.header().command, Command2::Request);
        assert_eq!(received.header().size as usize, total);

        server_shutdown.trigger();
        let _ = compio::time::timeout(Duration::from_secs(10), server_handle).await;
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn ws_rejects_oversize_against_custom_cap() {
        const CUSTOM_CAP: usize = HEADER_SIZE + 1024;
        const OVER_CAP: usize = HEADER_SIZE + 64 * 1024;

        let (mut client_ws, server_ws) = ws_pair().await;
        let server_conn = WsTransportConn::new_server(server_ws);
        let (_server_out, server_in, _server_shutdown, server_handle) =
            drive_with_cap(server_conn, CUSTOM_CAP);

        raw_send(&mut client_ws, padded(Command2::Request, OVER_CAP)).await;

        // Decode rejection tears the server pump down; join must complete
        // within the grace window and no frame must surface to in_rx.
        let join_res = compio::time::timeout(Duration::from_secs(5), server_handle).await;
        assert!(
            join_res.is_ok(),
            "server pump must exit on frame above max_message_size"
        );
        let recv_res = compio::time::timeout(Duration::from_millis(50), server_in.recv()).await;
        assert!(
            recv_res.is_err() || recv_res.expect("timeout outer").is_err(),
            "no frame should surface above the configured cap"
        );
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn ws_shutdown_exits_promptly() {
        let (client_ws, server_ws) = ws_pair().await;
        let server_conn = WsTransportConn::new_server(server_ws);
        let client_conn = WsTransportConn::new_client(client_ws);

        let (_server_out, _server_in, server_shutdown, server_handle) = drive(server_conn);
        let (_client_out, _client_in, client_shutdown, client_handle) = drive(client_conn);

        server_shutdown.trigger();
        client_shutdown.trigger();
        let server_done = compio::time::timeout(Duration::from_secs(5), server_handle).await;
        let client_done = compio::time::timeout(Duration::from_secs(5), client_handle).await;
        assert!(
            server_done.is_ok(),
            "server must exit within 5 s of shutdown"
        );
        assert!(
            client_done.is_ok(),
            "client must exit within 5 s of shutdown"
        );
    }
}
