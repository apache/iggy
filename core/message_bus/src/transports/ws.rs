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
//! # Cancel safety (`compio_ws` 0.3.1 narrow hazard)
//!
//! `WebSocketStream::read` in 0.3.1 awaits on two paths:
//!
//! 1. `fill_read_buf` (more bytes needed). Cancel-safe: bytes already in
//!    the underlying `compio_io::compat::SyncStream` buffer plus
//!    tungstenite's frame-parser state both live inside the
//!    `WebSocketStream` struct and survive a future drop.
//! 2. `flush` after a successful sync decode (drains tungstenite's write
//!    buffer, e.g. auto-Pong replies). The decoded `Message` lives on
//!    the `read()` stack frame; dropping the future during this flush
//!    park drops the message with it, and tungstenite's parser has
//!    already advanced past the frame.
//!
//! The path-2 window collapses for pure consensus traffic: the write
//! buffer is empty (no inbound Pings, no auto-Pong queued), so `flush`
//! resolves synchronously and the await never parks. Real-world bite
//! needs the compound: inbound Ping interleaved with our consensus frame
//! AND TCP write-buffer pressure on the auto-Pong AND the mailbox /
//! shutdown arm winning the `select_biased!` race.
//!
//! TODO(hubcio): structural fix is a reader / writer task split, but
//! `compio_ws` 0.3.1 exposes no native `split()`. The
//! `Rc<RefCell<WebSocketStream>>` design panics on the second
//! `borrow_mut()` while the reader still holds its borrow across
//! `read().await`. `compio_ws` 0.4 adds `Sink + Stream` impls (so
//! `futures_util::stream::split` works) and a `next_item` cancel-buffer
//! that lifts the decoded `Message` into struct state before the flush.
//! Bump the workspace pin and reinstate the split.
//!
//! Sends are run to completion outside any `select!`. The token signals
//! the *start* of close, never cancels an in-flight send.
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
use compio::net::TcpStream;
use compio::ws::WebSocketStream;
use compio::ws::tungstenite::{self, Message as WsMessage};
use futures::FutureExt;
use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
use iggy_binary_protocol::{GenericHeader, Message, read_size_field};
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
    let owned =
        iggy_binary_protocol::consensus::iobuf::Owned::<MESSAGE_ALIGN>::copy_from_slice(body);
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
        run_pump(&mut ws, ctx).await;
        drive_close(&mut ws, self.close_grace, label, &peer).await;
    }
}

/// Per-iteration outcome of the single-task select.
enum PumpAction {
    Shutdown,
    Send(BusMessage),
    Recv(Result<WsMessage, tungstenite::Error>),
    MailboxClosed,
}

/// Drive the WS connection until shutdown, peer Close, or an
/// unrecoverable error.
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
                debug!(%label, %peer, "ws pump: shutdown observed");
                return;
            }
            PumpAction::MailboxClosed => {
                debug!(%label, %peer, "ws pump: mailbox closed");
                return;
            }
            PumpAction::Send(first) => {
                // Drain mailbox up to `max_batch` and flush once.
                // tungstenite buffers each `send` into its outbound
                // queue; a single trailing `flush` shrinks N writev
                // syscalls to 1 per drain.
                batch.push(first);
                while batch.len() < max_batch {
                    match rx.try_recv() {
                        Ok(m) => batch.push(m),
                        Err(_) => break,
                    }
                }
                let drained = batch.len();
                // `drain(..)` consumes the batch in FIFO order while
                // preserving the Vec's allocation for the next
                // iteration; `into_iter()` would move the buffer out.
                #[allow(clippy::iter_with_drain)]
                for msg in batch.drain(..) {
                    if let Err(e) = ws.send(WsMessage::Binary(Bytes::from_owner(msg))).await {
                        warn!(%label, %peer, error = ?e, batch_len = drained, "ws writer: send failed");
                        return;
                    }
                }
                if let Err(e) = ws.flush().await {
                    warn!(%label, %peer, error = ?e, batch_len = drained, "ws writer: flush failed");
                    return;
                }
            }
            PumpAction::Recv(Ok(msg)) => match msg {
                WsMessage::Binary(bytes) => {
                    match decode_consensus_frame(&bytes, max_message_size) {
                        Ok(frame) => {
                            if in_tx.send(frame).await.is_err() {
                                debug!(%label, %peer, "ws reader: inbound queue dropped");
                                return;
                            }
                        }
                        Err(e) => {
                            warn!(%label, %peer, error = ?e, "ws reader: bad consensus frame");
                            return;
                        }
                    }
                }
                WsMessage::Ping(_) | WsMessage::Pong(_) => {
                    // Tungstenite queues an auto-Pong for inbound Pings;
                    // `compio_ws::WebSocketStream::read` flushes before
                    // delivery so no explicit reply needed.
                }
                WsMessage::Close(_) => {
                    debug!(%label, %peer, "ws reader: peer initiated close");
                    return;
                }
                WsMessage::Text(_) | WsMessage::Frame(_) => {
                    warn!(%label, %peer, "ws reader: unexpected text/raw frame, closing");
                    return;
                }
            },
            PumpAction::Recv(Err(e)) => {
                debug!(%label, %peer, error = ?e, "ws reader: read error");
                return;
            }
        }
    }
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
    use iggy_binary_protocol::consensus::iobuf::Frozen;
    use iggy_binary_protocol::{Command2, GenericHeader, HEADER_SIZE, Message};
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

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn ws_loopback_round_trip() {
        let (client_ws, server_ws) = ws_pair().await;
        let server_conn = WsTransportConn::new_server(server_ws);
        let client_conn = WsTransportConn::new_client(client_ws);

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
    async fn ws_large_frame_round_trip() {
        const BODY_SIZE: usize = 1024 * 1024;
        let total = HEADER_SIZE + BODY_SIZE;

        let (client_ws, server_ws) = ws_pair().await;
        let server_conn = WsTransportConn::new_server(server_ws);
        let client_conn = WsTransportConn::new_client(client_ws);

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

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn ws_rejects_oversize_against_custom_cap() {
        const CUSTOM_CAP: usize = HEADER_SIZE + 1024;
        const OVER_CAP: usize = HEADER_SIZE + 64 * 1024;

        let (client_ws, server_ws) = ws_pair().await;
        let server_conn = WsTransportConn::new_server(server_ws);
        let client_conn = WsTransportConn::new_client(client_ws);

        let (_server_out, server_in, _server_shutdown, server_handle) =
            drive_with_cap(server_conn, CUSTOM_CAP);
        let (client_out, _client_in, _client_shutdown, client_handle) =
            drive_with_cap(client_conn, framing::MAX_MESSAGE_SIZE);

        client_out
            .send(padded(Command2::Request, OVER_CAP))
            .await
            .expect("client send oversize");

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

        let _ = compio::time::timeout(Duration::from_secs(2), client_handle).await;
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
