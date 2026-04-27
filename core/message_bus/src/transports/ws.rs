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

//! WebSocket impls of the [`super`] transport traits.
//!
//! SDK-client plane only; replica plane stays TCP forever (invariant I1).
//! `compio-ws 0.3.1` binds `tungstenite`'s protocol state machine to
//! compio I/O directly, no tokio bridge (invariant I10).
//!
//! # Connection model
//!
//! One binary WS frame per consensus message. The 256 B `GenericHeader`
//! plus the optional body is the frame payload; the frame format
//! constraints (FIN=1, opcode=0x2 binary, no fragmentation, no
//! extensions) come from the WS spec layered on top of our 256 B
//! framing invariant (I3).
//!
//! # Stream split via actor pattern
//!
//! `tungstenite`'s state machine spans both directions: control frames
//! (Ping/Pong/Close) and the masked-from-client/unmasked-from-server
//! rules force the read and write halves to cooperate on one
//! `WebSocketStream` instance. We cannot satisfy
//! [`TransportConn::into_split`]'s ownership-split contract by handing
//! out two halves of the underlying TCP connection.
//!
//! Instead [`WsTransportConn::into_split`] spawns a **dispatcher task**
//! that owns the [`WebSocketStream`] and bridges two
//! `async_channel::bounded` queues:
//!
//! - inbound: dispatcher reads frames, pushes parsed
//!   `Message<GenericHeader>` to the reader's queue.
//! - outbound: dispatcher pulls `Frozen<MESSAGE_ALIGN>` from the
//!   writer's queue, wraps in a binary WS frame, sends.
//!
//! Per-frame channel hop costs one queue push + one queue pop. Single-
//! threaded compio means the hop is a noop scheduling-wise; the cost is
//! one bounded buffer slot per inflight frame, which the per-peer queue
//! capacity already pays for on the bus side.
//!
//! # Zero-copy payload
//!
//! `tungstenite::Message::Binary` takes `bytes::Bytes`. Wrapping
//! `Frozen<MESSAGE_ALIGN>` via [`bytes::Bytes::from_owner`] is
//! O(1) Arc-allocation, no memcpy. The 256 B header + body slice is
//! handed to the WS frame writer as-is.

use super::{TransportConn, TransportListener, TransportReader, TransportWriter};
use crate::framing;
use async_channel::{Receiver, Sender, bounded};
use bytes::Bytes;
use compio::net::{TcpListener, TcpStream};
use compio_ws::WebSocketStream;
use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
use iggy_binary_protocol::consensus::iobuf::Frozen;
use iggy_binary_protocol::{GenericHeader, Message};
use iggy_common::IggyError;
use std::io;
use std::net::SocketAddr;
use tracing::{debug, warn};
use tungstenite::protocol::CloseFrame;
use tungstenite::protocol::frame::coding::CloseCode;

/// Newtype wrapper bridging [`Frozen<MESSAGE_ALIGN>`] into the
/// [`bytes::Bytes::from_owner`] surface (which requires `AsRef<[u8]>`,
/// not `Deref`). The underlying byte slice is shared with the
/// `Bytes`'s atomic-refcount holder; no memcpy. Cost is one
/// `Arc<dyn>` allocation per outbound frame.
struct FrozenBytesOwner(Frozen<MESSAGE_ALIGN>);

impl AsRef<[u8]> for FrozenBytesOwner {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

/// Subprotocol identifier carried in `Sec-WebSocket-Protocol`. Bump the
/// trailing version digits when the wire format changes (matches the
/// QUIC ALPN constant in `transports/quic.rs`).
pub const WS_SUBPROTOCOL: &str = "iggy.consensus.v1";

/// Per-connection inbound queue depth. Bounded so an idle reader cannot
/// let memory grow unboundedly under a chatty peer; mirrors the bus's
/// per-peer outbound queue convention.
const INBOUND_QUEUE_CAPACITY: usize = 256;

/// Per-connection outbound queue depth. Same shape as the inbound side;
/// the per-peer writer task already enforces backpressure upstream.
const OUTBOUND_QUEUE_CAPACITY: usize = 256;

/// Inbound WS listener.
///
/// Wraps a [`TcpListener`]. `accept` performs the WS HTTP-Upgrade
/// handshake via `compio_ws::accept_async` before yielding a
/// [`WsTransportConn`], so the install path on the owning shard never
/// sees the upgrade request.
pub struct WsTransportListener {
    inner: TcpListener,
}

impl WsTransportListener {
    /// Wrap a pre-bound [`TcpListener`].
    #[must_use]
    pub const fn new(inner: TcpListener) -> Self {
        Self { inner }
    }
}

impl TransportListener for WsTransportListener {
    type Conn = WsTransportConn;

    #[allow(clippy::future_not_send)]
    async fn accept(&self) -> io::Result<(Self::Conn, SocketAddr)> {
        loop {
            let (stream, addr) = self.inner.accept().await?;
            match compio_ws::accept_async(stream).await {
                Ok(ws) => return Ok((WsTransportConn::new(ws), addr)),
                Err(e) => {
                    warn!(%addr, "WS handshake failed: {e}");
                    // Drop the failed stream and accept the next; do
                    // not surface as a listener-fatal error.
                }
            }
        }
    }
}

/// A single WS connection holding the bidirectional [`WebSocketStream`]
/// until [`Self::into_split`] hands ownership to a per-connection
/// dispatcher task.
pub struct WsTransportConn {
    stream: WebSocketStream<TcpStream>,
}

impl WsTransportConn {
    /// Construct from an already-upgraded [`WebSocketStream`].
    #[must_use]
    pub const fn new(stream: WebSocketStream<TcpStream>) -> Self {
        Self { stream }
    }
}

impl TransportConn for WsTransportConn {
    type Reader = WsTransportReader;
    type Writer = WsTransportWriter;

    fn into_split(self) -> (Self::Reader, Self::Writer) {
        let (in_tx, in_rx) = bounded::<Message<GenericHeader>>(INBOUND_QUEUE_CAPACITY);
        let (out_tx, out_rx) = bounded::<Frozen<MESSAGE_ALIGN>>(OUTBOUND_QUEUE_CAPACITY);
        compio::runtime::spawn(dispatcher(self.stream, in_tx, out_rx)).detach();
        (
            WsTransportReader { rx: in_rx },
            WsTransportWriter { tx: out_tx },
        )
    }
}

/// Per-connection dispatcher loop. Phase-alternating design.
///
/// `tungstenite`'s state machine and compio's "no buffer drop with
/// pending I/O" rule together rule out a `select!`-cancelling read +
/// write race on a shared `&mut WebSocketStream`. Instead the loop
/// alternates phases:
///
/// 1. **Phase A** — non-blocking drain of up to
///    [`OUTBOUND_DRAIN_BURST`] outbound frames. `try_recv` returns
///    immediately when the queue is empty; no in-flight I/O is held
///    across the await.
/// 2. **Phase B** — one blocking `stream.read()`. Returns when the
///    peer sends a frame, EOF, error, or peer-close.
///
/// Outbound latency under this design = at most one inbound RTT (the
/// time to receive the next frame). For request/reply workloads
/// (every client request triggers a server reply) this is identical
/// to a `select!`-based design. For server-push-heavy workloads
/// the upper bound is the keepalive interval; future work can add a
/// keepalive Ping/Pong sweeper that nudges the read past idle.
const OUTBOUND_DRAIN_BURST: usize = 16;

#[allow(clippy::future_not_send)]
async fn dispatcher(
    mut stream: WebSocketStream<TcpStream>,
    in_tx: Sender<Message<GenericHeader>>,
    out_rx: Receiver<Frozen<MESSAGE_ALIGN>>,
) {
    loop {
        // Phase A: drain outbound non-blocking, capped at the burst.
        for _ in 0..OUTBOUND_DRAIN_BURST {
            let Ok(frame) = out_rx.try_recv() else {
                break;
            };
            let payload = Bytes::from_owner(FrozenBytesOwner(frame));
            if let Err(e) = stream.send(tungstenite::Message::Binary(payload)).await {
                warn!("WS write failed: {e}; dispatcher exiting");
                let _ = stream.close(None).await;
                return;
            }
        }

        // Phase B: one read. The compio I/O submission is owned by
        // the await and only dropped after completion - matches
        // compio's "no buffer drop with pending I/O" rule.
        let ws_msg = stream.read().await;
        match ws_msg {
            Ok(tungstenite::Message::Binary(body)) => match decode_consensus_frame(&body) {
                Ok(msg) => {
                    if in_tx.send(msg).await.is_err() {
                        debug!("WS reader dropped; dispatcher exiting");
                        break;
                    }
                }
                Err(e) => {
                    warn!("WS frame decode failed: {e:?}; closing connection");
                    let _ = stream
                        .close(Some(CloseFrame {
                            code: CloseCode::Protocol,
                            reason: "iggy frame decode failed".into(),
                        }))
                        .await;
                    break;
                }
            },
            Ok(tungstenite::Message::Ping(payload)) => {
                if let Err(e) = stream.send(tungstenite::Message::Pong(payload)).await {
                    debug!("WS pong write failed: {e}; dispatcher exiting");
                    break;
                }
            }
            Ok(tungstenite::Message::Pong(_)) => {
                // Server-initiated keepalive replies; nothing to do.
            }
            Ok(tungstenite::Message::Close(_)) => {
                debug!("WS peer initiated close");
                break;
            }
            Ok(tungstenite::Message::Text(_)) => {
                warn!("WS text frame received on a binary plane; closing");
                let _ = stream
                    .close(Some(CloseFrame {
                        code: CloseCode::Unsupported,
                        reason: "binary frames only".into(),
                    }))
                    .await;
                break;
            }
            Ok(tungstenite::Message::Frame(_)) => {
                // Tungstenite's contract: `Message::Frame` is never
                // produced on the read path. Defensive log + drop.
                warn!("unexpected raw WS frame on read path; ignoring");
            }
            Err(e) => {
                debug!("WS read error: {e}; dispatcher exiting");
                break;
            }
        }
    }
    let _ = stream.close(None).await;
}

#[derive(Debug)]
enum FrameDecodeError {
    BadHeader,
    BadSize,
}

/// Parse a binary WS frame's payload into a `Message<GenericHeader>`.
///
/// Reuses the framing invariant (I3): `[256 B header][optional body]`
/// where `body_len = header.size - HEADER_SIZE`. No partial frames; the
/// WS layer guarantees a single message per binary frame (FIN=1).
fn decode_consensus_frame(body: &Bytes) -> Result<Message<GenericHeader>, FrameDecodeError> {
    if body.len() < iggy_binary_protocol::HEADER_SIZE {
        return Err(FrameDecodeError::BadHeader);
    }
    let total_size = u32::from_le_bytes(
        body[48..52]
            .try_into()
            .map_err(|_| FrameDecodeError::BadHeader)?,
    ) as usize;
    if !(iggy_binary_protocol::HEADER_SIZE..=framing::MAX_MESSAGE_SIZE).contains(&total_size)
        || total_size != body.len()
    {
        return Err(FrameDecodeError::BadSize);
    }
    // Aligned MESSAGE_ALIGN backing memory is required by the
    // `Message<GenericHeader>` invariant; `Bytes` carries no alignment
    // guarantee, so this is a one-time copy from the WS-framed payload
    // into a freshly allocated `Owned<MESSAGE_ALIGN>`. The cost is
    // unavoidable on inbound for the WS plane (TCP path also does a
    // copy via the staged read-into-Owned dance in framing.rs); the
    // outbound side stays zero-copy via `Bytes::from_owner`.
    let owned =
        iggy_binary_protocol::consensus::iobuf::Owned::<MESSAGE_ALIGN>::copy_from_slice(body);
    Message::<GenericHeader>::try_from(owned).map_err(|_| FrameDecodeError::BadHeader)
}

/// Read half. Polls the dispatcher's inbound queue.
pub struct WsTransportReader {
    rx: Receiver<Message<GenericHeader>>,
}

impl TransportReader for WsTransportReader {
    #[allow(clippy::future_not_send)]
    async fn read_message(
        &mut self,
        _max_message_size: usize,
    ) -> Result<Message<GenericHeader>, IggyError> {
        // Bound check happens inside `decode_consensus_frame` against
        // the build-time `framing::MAX_MESSAGE_SIZE`; the per-call
        // override exists for parity with the trait surface and will
        // be plumbed through when a runtime-configurable cap lands.
        self.rx
            .recv()
            .await
            .map_err(|_| IggyError::ConnectionClosed)
    }
}

/// Write half. Pushes frames into the dispatcher's outbound queue.
pub struct WsTransportWriter {
    tx: Sender<Frozen<MESSAGE_ALIGN>>,
}

impl TransportWriter for WsTransportWriter {
    #[allow(clippy::future_not_send)]
    async fn send_batch(&mut self, batch: &mut Vec<Frozen<MESSAGE_ALIGN>>) -> io::Result<()> {
        // Drain in order; one queue push per frame. Atomic-or-error
        // contract holds because the dispatcher writes each frame
        // before pulling the next from the queue.
        for buf in batch.drain(..) {
            self.tx
                .send(buf)
                .await
                .map_err(|_| io::Error::other("WS dispatcher queue closed"))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use compio::net::TcpStream;
    use iggy_binary_protocol::{Command2, HEADER_SIZE};

    #[allow(clippy::cast_possible_truncation)]
    fn header_only(command: Command2) -> Frozen<MESSAGE_ALIGN> {
        Message::<GenericHeader>::new(HEADER_SIZE)
            .transmute_header(|_, h: &mut GenericHeader| {
                h.command = command;
                h.size = HEADER_SIZE as u32;
            })
            .into_frozen()
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn loopback_round_trip_three_frames() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        let ws_listener = WsTransportListener::new(listener);

        let server_task = compio::runtime::spawn(async move {
            let (conn, _peer) = ws_listener.accept().await.expect("accept");
            let (mut reader, _writer) = conn.into_split();
            let a = reader
                .read_message(framing::MAX_MESSAGE_SIZE)
                .await
                .unwrap();
            let b = reader
                .read_message(framing::MAX_MESSAGE_SIZE)
                .await
                .unwrap();
            let c = reader
                .read_message(framing::MAX_MESSAGE_SIZE)
                .await
                .unwrap();
            (a.header().command, b.header().command, c.header().command)
        });

        let client_tcp = TcpStream::connect(server_addr).await.unwrap();
        let url = format!("ws://{server_addr}/");
        let (ws_client, _resp) = compio_ws::client_async(url, client_tcp)
            .await
            .expect("ws handshake");
        let conn = WsTransportConn::new(ws_client);
        let (_r, mut w) = conn.into_split();

        let mut batch = vec![
            header_only(Command2::Ping),
            header_only(Command2::Prepare),
            header_only(Command2::Request),
        ];
        w.send_batch(&mut batch).await.expect("send_batch");
        assert!(batch.is_empty(), "Vec must be drained on success");

        let (a, b, c) = server_task.await.unwrap();
        assert_eq!(a, Command2::Ping);
        assert_eq!(b, Command2::Prepare);
        assert_eq!(c, Command2::Request);
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn malformed_frame_closes_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        let ws_listener = WsTransportListener::new(listener);

        let server_task = compio::runtime::spawn(async move {
            let (conn, _peer) = ws_listener.accept().await.expect("accept");
            let (mut reader, _writer) = conn.into_split();
            reader.read_message(framing::MAX_MESSAGE_SIZE).await
        });

        let client_tcp = TcpStream::connect(server_addr).await.unwrap();
        let url = format!("ws://{server_addr}/");
        let (mut ws_client, _) = compio_ws::client_async(url, client_tcp).await.unwrap();
        // Deliberately wrong total_size at offset 48 of the 256 B header.
        let mut buf = vec![0u8; HEADER_SIZE];
        let bogus = u32::try_from(framing::MAX_MESSAGE_SIZE + 1)
            .unwrap_or(u32::MAX)
            .to_le_bytes();
        buf[48..52].copy_from_slice(&bogus);
        ws_client
            .send(tungstenite::Message::Binary(Bytes::from(buf)))
            .await
            .unwrap();
        // Server's dispatcher closes the WS; reader queue closes,
        // surfacing as ConnectionClosed.
        let res = server_task.await.unwrap();
        assert!(matches!(res, Err(IggyError::ConnectionClosed)));
    }
}
