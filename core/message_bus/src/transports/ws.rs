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
//! `compio-ws 0.3.1` drives the HTTP-Upgrade handshake; after that the
//! [`compio_ws::WebSocketStream`] is unwrapped down to the bare
//! [`compio::net::TcpStream`] and split into owned read / write halves
//! handed to a per-connection reader / writer task pair. No tokio bridge
//! (invariant I10).
//!
//! # Connection model
//!
//! One binary WS frame per consensus message. The 256 B `GenericHeader`
//! plus the optional body is the frame payload; the frame format
//! constraints (FIN=1, opcode=0x2 binary, no fragmentation, no
//! extensions) come from the WS spec layered on top of our 256 B
//! framing invariant (I3).
//!
//! # Two-task split
//!
//! `tungstenite::WebSocket` holds a single state machine that spans both
//! directions on a `&mut self`, and `compio_ws::WebSocketStream::read`
//! cannot be cancelled across its internal `fill_read_buf().await`
//! without risking the in-flight buffer. Either constraint alone
//! precludes a `select!`-cancelling read + write race on a shared
//! stream.
//!
//! [`WsTransportConn::into_split`] therefore unwraps the post-handshake
//! `WebSocketStream<TcpStream>` to the bare `TcpStream`, splits it into
//! `(OwnedReadHalf, OwnedWriteHalf)`, and spawns:
//!
//! - **reader task**: drains bytes from `OwnedReadHalf` into a persistent
//!   accumulator, parses WS frames via [`tungstenite::protocol::frame::FrameHeader`],
//!   pushes [`Message<GenericHeader>`] frames to the inbound queue,
//!   forwards Ping payloads to the writer via the control channel.
//! - **writer task**: pulls outbound consensus frames + control replies
//!   (Pong, Close), encodes them with the appropriate WS framing
//!   (server unmasked, client masked per RFC 6455), writes to
//!   `OwnedWriteHalf`.
//!
//! Reader and writer share no `&mut` state, so a parked read never
//! blocks an outbound send. Per-connection [`Shutdown`] coordinates
//! teardown: either task triggers it on EOF / I/O error / framing
//! violation, the other observes via [`ShutdownToken::wait`] and exits.
//!
//! # Zero-copy on the server-side outbound path
//!
//! Server-role writes split each frame into two compio submissions: the
//! ≤ 14 B WS header from a small scratch [`Vec<u8>`], then the
//! [`Frozen<MESSAGE_ALIGN>`] payload as an `IoBuf` directly. No
//! intermediate copy of the body. Client-role writes (test path) build a
//! single `Vec<u8>` containing the header + masked body, paying one copy
//! per frame.
//!
//! # Post-handshake leftover bytes
//!
//! RFC 6455 §4.1 forbids clients sending data frames before receiving
//! the 101 response. Defensively, [`WsTransportConn::into_split`] reads
//! any bytes already buffered in the `compio_io::compat::SyncStream`
//! (via [`std::io::BufRead::fill_buf`]) before unwrapping to the bare
//! `TcpStream`, and seeds the reader's accumulator with them. Expected
//! to be empty in production traffic; non-empty case logged at debug.

use super::{TransportConn, TransportListener, TransportReader, TransportWriter};
use crate::framing;
use crate::lifecycle::{Shutdown, ShutdownToken};
use async_channel::{Receiver, Sender, bounded};
use bytes::Bytes;
use compio::BufResult;
use compio::buf::IntoInner;
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::net::{OwnedReadHalf, OwnedWriteHalf, TcpListener, TcpStream};
use compio_ws::WebSocketStream;
use futures::FutureExt;
use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
use iggy_binary_protocol::consensus::iobuf::Frozen;
use iggy_binary_protocol::{GenericHeader, Message};
use iggy_common::IggyError;
use std::io::{self, BufRead, Cursor};
use std::net::SocketAddr;
use std::rc::Rc;
use tracing::{debug, warn};
use tungstenite::protocol::frame::FrameHeader;
use tungstenite::protocol::frame::coding::{Control, Data, OpCode};

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

/// Reader -> writer control channel depth. Pong replies + Close are
/// rare under nominal traffic; a small bound keeps a chatty peer's
/// Ping flood from displacing consensus traffic.
const CONTROL_QUEUE_CAPACITY: usize = 8;

/// Per-fill chunk size for the reader task's TCP read. Matches
/// compio-ws's [`Config::DEFAULT_BUF_SIZE`] so kernel buffer sizing on
/// `io_uring` is symmetric pre- and post-rewrite.
const READ_FILL_CHUNK: usize = 128 * 1024;

/// Maximum WS frame header length per RFC 6455 §5.2 (2 base bytes +
/// 8 extended length bytes + 4 mask bytes). Sized for a per-frame
/// scratch [`Vec<u8>`] reused across the writer's hot loop.
const WS_HEADER_MAX: usize = 14;

/// Inbound WS listener.
///
/// Wraps a [`TcpListener`]. `accept` performs the WS HTTP-Upgrade
/// handshake via `compio_ws::accept_async` before yielding a
/// [`WsTransportConn`], so the install path on the owning shard never
/// sees the upgrade request. Accepted connections take the server role
/// (outbound unmasked, inbound expected masked per RFC 6455).
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
                Ok(ws) => return Ok((WsTransportConn::new_server(ws), addr)),
                Err(e) => {
                    warn!(%addr, "WS handshake failed: {e}");
                    // Drop the failed stream and accept the next; do
                    // not surface as a listener-fatal error.
                }
            }
        }
    }
}

/// Role of the local end on a WebSocket connection. Mask handling per
/// RFC 6455 §5.3 differs by direction:
///
/// - **Server** writes unmasked, expects inbound masked.
/// - **Client** writes masked (random per-frame mask), expects inbound
///   unmasked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Server,
    Client,
}

/// A single WS connection holding the bidirectional [`WebSocketStream`]
/// until [`Self::into_split`] unwraps it to the bare [`TcpStream`] and
/// hands the read / write halves to per-task actors.
pub struct WsTransportConn {
    stream: WebSocketStream<TcpStream>,
    role: Role,
}

impl WsTransportConn {
    /// Construct a server-role connection from an already-upgraded
    /// [`WebSocketStream`]. Used by the listener accept path and by the
    /// owning-shard install path post-fd-ship.
    #[must_use]
    pub const fn new_server(stream: WebSocketStream<TcpStream>) -> Self {
        Self {
            stream,
            role: Role::Server,
        }
    }

    /// Construct a client-role connection from an already-upgraded
    /// [`WebSocketStream`]. Used by tests dialing
    /// [`compio_ws::client_async`]; production WS dialer paths (if /
    /// when shipped) follow the same shape.
    #[must_use]
    pub const fn new_client(stream: WebSocketStream<TcpStream>) -> Self {
        Self {
            stream,
            role: Role::Client,
        }
    }
}

impl TransportConn for WsTransportConn {
    type Reader = WsTransportReader;
    type Writer = WsTransportWriter;

    fn into_split(self) -> (Self::Reader, Self::Writer) {
        let (in_tx, in_rx) = bounded::<Message<GenericHeader>>(INBOUND_QUEUE_CAPACITY);
        let (out_tx, out_rx) = bounded::<Frozen<MESSAGE_ALIGN>>(OUTBOUND_QUEUE_CAPACITY);
        let (control_tx, control_rx) = bounded::<ControlFrame>(CONTROL_QUEUE_CAPACITY);
        let (shutdown, shutdown_token_reader) = Shutdown::new();
        let shutdown = Rc::new(shutdown);
        let shutdown_token_writer = shutdown_token_reader.clone();

        // Unwrap layers: WebSocketStream -> WebSocket -> SyncStream -> TcpStream.
        // Recover any post-handshake bytes from the SyncStream's read buffer
        // before consuming it (RFC 6455 §4.1 says clients must not pipeline
        // before the 101, so this is normally empty; defensive copy keeps a
        // misbehaving client from losing its first frame).
        let ws_inner = self.stream.into_inner();
        let mut sync_stream = ws_inner.into_inner();
        let leftover: Vec<u8> = match sync_stream.fill_buf() {
            Ok(buf) if !buf.is_empty() => {
                debug!(
                    "WS into_split: {} leftover bytes in SyncStream buffer post-handshake",
                    buf.len()
                );
                buf.to_vec()
            }
            _ => Vec::new(),
        };
        let tcp = sync_stream.into_inner();
        let (read_half, write_half) = tcp.into_split();

        let role = self.role;
        let reader_shutdown = Rc::clone(&shutdown);
        compio::runtime::spawn(reader_task(
            read_half,
            leftover,
            in_tx,
            control_tx,
            reader_shutdown,
            shutdown_token_reader,
            role,
        ))
        .detach();
        let writer_shutdown = Rc::clone(&shutdown);
        compio::runtime::spawn(writer_task(
            write_half,
            out_rx,
            control_rx,
            writer_shutdown,
            shutdown_token_writer,
            role,
        ))
        .detach();

        (
            WsTransportReader { rx: in_rx },
            WsTransportWriter { tx: out_tx },
        )
    }
}

/// Reader -> writer signal: outstanding control work the writer must
/// emit on the wire.
#[derive(Debug)]
enum ControlFrame {
    /// Reply to an inbound Ping. Payload is post-unmask.
    Pong(Bytes),
    /// Send a graceful close (empty body, normal closure).
    Close,
}

/// Reader-task local representation of one parsed WS frame.
#[derive(Debug)]
enum InboundFrame {
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    /// Server-initiated keepalive reply; reader silently ignores.
    Pong,
    Close,
}

/// Categorical reasons the reader may abort. Constructed from inline
/// matches in [`reader_task`] and [`parse_one_frame`]; the variants
/// only reach `tracing::warn!` via the derived `Debug` impl, so the
/// payload fields exist for diagnostic logging only.
#[derive(Debug)]
#[allow(dead_code)]
enum FramingError {
    /// Frame's mask bit conflicts with the local role (server got
    /// unmasked client frame, or client got masked server frame).
    UnexpectedMask,
    /// `is_final == false` — fragmentation is not part of the consensus
    /// plane invariant.
    Fragmented,
    /// Continuation, Text, or reserved opcode received.
    UnsupportedOpcode,
    /// Header parse failed at the tungstenite layer.
    InvalidHeader,
    /// Total frame body exceeds the bus-wide max message size.
    OversizedPayload(u64),
}

/// 4-byte XOR cycle per RFC 6455 §5.3. Re-implemented in tree because
/// tungstenite's `apply_mask` is `pub(crate)`. No SIMD; profile if mask
/// throughput ever becomes a hot path.
fn apply_mask(buf: &mut [u8], mask: [u8; 4]) {
    for (i, byte) in buf.iter_mut().enumerate() {
        *byte ^= mask[i & 3];
    }
}

/// Try to parse one complete WS frame off the front of `accumulator`.
/// Returns `Ok(Some(frame))` on a complete frame (bytes consumed),
/// `Ok(None)` if more bytes are needed (accumulator unchanged), or
/// `Err` on a protocol violation.
fn parse_one_frame(
    accumulator: &mut Vec<u8>,
    role: Role,
) -> Result<Option<InboundFrame>, FramingError> {
    let mut cursor = Cursor::new(accumulator.as_slice());
    let parse = FrameHeader::parse(&mut cursor).map_err(|_| FramingError::InvalidHeader)?;
    let Some((header, payload_len)) = parse else {
        return Ok(None);
    };
    // Header is at most 14 bytes per RFC 6455 §5.2; cursor.position()
    // returns a u64 only because of the underlying cursor type.
    let header_len = usize::try_from(cursor.position()).map_err(|_| FramingError::InvalidHeader)?;
    let payload_len_usize =
        usize::try_from(payload_len).map_err(|_| FramingError::OversizedPayload(payload_len))?;
    if payload_len_usize > framing::MAX_MESSAGE_SIZE {
        return Err(FramingError::OversizedPayload(payload_len));
    }
    let total = header_len.saturating_add(payload_len_usize);
    if accumulator.len() < total {
        return Ok(None);
    }

    if !header.is_final {
        return Err(FramingError::Fragmented);
    }
    if header.rsv1 || header.rsv2 || header.rsv3 {
        return Err(FramingError::InvalidHeader);
    }

    // RFC 6455 §5.1: client->server frames MUST be masked, server->client
    // MUST NOT be. Validate per local role.
    match (role, header.mask) {
        (Role::Server, None) | (Role::Client, Some(_)) => {
            return Err(FramingError::UnexpectedMask);
        }
        _ => {}
    }

    // Drain header + body bytes from the accumulator.
    let mut payload: Vec<u8> = accumulator.drain(..total).collect();
    payload.drain(..header_len);
    if let Some(mask) = header.mask {
        apply_mask(&mut payload, mask);
    }

    let frame = match header.opcode {
        OpCode::Data(Data::Binary) => InboundFrame::Binary(payload),
        OpCode::Data(Data::Continue | Data::Text | Data::Reserved(_))
        | OpCode::Control(Control::Reserved(_)) => return Err(FramingError::UnsupportedOpcode),
        OpCode::Control(Control::Ping) => InboundFrame::Ping(payload),
        OpCode::Control(Control::Pong) => InboundFrame::Pong,
        OpCode::Control(Control::Close) => InboundFrame::Close,
    };
    Ok(Some(frame))
}

/// Decode a binary WS frame's payload into a `Message<GenericHeader>`.
///
/// Reuses the framing invariant (I3): `[256 B header][optional body]`
/// where `body_len = header.size - HEADER_SIZE`. The WS layer guarantees
/// a single message per binary frame (FIN=1).
fn decode_consensus_frame(body: &[u8]) -> Result<Message<GenericHeader>, FrameDecodeError> {
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
    // `Message<GenericHeader>` invariant; the unmasked WS payload is a
    // plain byte slice with no alignment guarantee, so this is a
    // one-time copy from the WS-framed payload into a freshly allocated
    // `Owned<MESSAGE_ALIGN>`. Same trade-off as before the rewrite.
    let owned =
        iggy_binary_protocol::consensus::iobuf::Owned::<MESSAGE_ALIGN>::copy_from_slice(body);
    Message::<GenericHeader>::try_from(owned).map_err(|_| FrameDecodeError::BadHeader)
}

#[derive(Debug)]
enum FrameDecodeError {
    BadHeader,
    BadSize,
}

#[allow(clippy::future_not_send)]
async fn reader_task(
    mut read_half: OwnedReadHalf<TcpStream>,
    initial: Vec<u8>,
    in_tx: Sender<Message<GenericHeader>>,
    control_tx: Sender<ControlFrame>,
    shutdown: Rc<Shutdown>,
    shutdown_token: ShutdownToken,
    role: Role,
) {
    let mut accumulator: Vec<u8> = initial;
    loop {
        // Drain any complete frames already buffered.
        loop {
            match parse_one_frame(&mut accumulator, role) {
                Ok(Some(InboundFrame::Binary(payload))) => match decode_consensus_frame(&payload) {
                    Ok(msg) => {
                        if in_tx.send(msg).await.is_err() {
                            debug!("WS reader: inbound queue dropped, exiting");
                            shutdown.trigger();
                            return;
                        }
                    }
                    Err(e) => {
                        warn!("WS reader: consensus frame decode failed: {e:?}");
                        let _ = control_tx.try_send(ControlFrame::Close);
                        shutdown.trigger();
                        return;
                    }
                },
                Ok(Some(InboundFrame::Ping(payload))) => {
                    if control_tx
                        .try_send(ControlFrame::Pong(Bytes::from(payload)))
                        .is_err()
                    {
                        warn!("WS reader: control queue full, dropping inbound Ping");
                    }
                }
                Ok(Some(InboundFrame::Pong)) => {
                    // Server-initiated keepalive reply; nothing to do.
                }
                Ok(Some(InboundFrame::Close)) => {
                    debug!("WS reader: peer initiated close");
                    let _ = control_tx.try_send(ControlFrame::Close);
                    shutdown.trigger();
                    return;
                }
                Ok(None) => break,
                Err(e) => {
                    warn!("WS reader: framing violation: {e:?}");
                    let _ = control_tx.try_send(ControlFrame::Close);
                    shutdown.trigger();
                    return;
                }
            }
        }

        // Need more bytes. Race the read against the shutdown token so a
        // peer that goes silent does not pin the task across graceful
        // shutdown. compio TcpStream reads use IoBuf; cancellation
        // (drop of the future) is supported and leaks the buffer until
        // the kernel completes the SQE.
        let buf: Vec<u8> = Vec::with_capacity(READ_FILL_CHUNK);
        let read_fut = read_half.read(buf);
        let outcome = futures::select_biased! {
            () = shutdown_token.wait().fuse() => {
                debug!("WS reader: shutdown observed during fill, exiting");
                return;
            }
            BufResult(result, buf) = read_fut.fuse() => (result, buf),
        };
        let (result, buf) = outcome;
        match result {
            Ok(0) => {
                debug!("WS reader: TCP EOF");
                shutdown.trigger();
                return;
            }
            Ok(n) => accumulator.extend_from_slice(&buf[..n]),
            Err(e) => {
                debug!("WS reader: TCP read error: {e}");
                shutdown.trigger();
                return;
            }
        }
    }
}

#[allow(clippy::future_not_send)]
async fn writer_task(
    mut write_half: OwnedWriteHalf<TcpStream>,
    out_rx: Receiver<Frozen<MESSAGE_ALIGN>>,
    control_rx: Receiver<ControlFrame>,
    shutdown: Rc<Shutdown>,
    shutdown_token: ShutdownToken,
    role: Role,
) {
    let mut header_scratch: Vec<u8> = Vec::with_capacity(WS_HEADER_MAX);
    loop {
        let event = futures::select_biased! {
            () = shutdown_token.wait().fuse() => Event::Shutdown,
            ctrl = control_rx.recv().fuse() => ctrl
                .map_or(Event::Shutdown, Event::Control),
            out = out_rx.recv().fuse() => out
                .map_or(Event::Shutdown, Event::Outbound),
        };
        match event {
            Event::Outbound(frozen) => {
                if let Err(e) =
                    write_consensus_binary(&mut write_half, &mut header_scratch, frozen, role).await
                {
                    debug!("WS writer: outbound write failed: {e}");
                    shutdown.trigger();
                    return;
                }
            }
            Event::Control(ControlFrame::Pong(payload)) => {
                if let Err(e) = write_control(
                    &mut write_half,
                    &mut header_scratch,
                    payload,
                    OpCode::Control(Control::Pong),
                    role,
                )
                .await
                {
                    debug!("WS writer: pong write failed: {e}");
                    shutdown.trigger();
                    return;
                }
            }
            Event::Control(ControlFrame::Close) => {
                let _ = write_control(
                    &mut write_half,
                    &mut header_scratch,
                    Bytes::new(),
                    OpCode::Control(Control::Close),
                    role,
                )
                .await;
                shutdown.trigger();
                return;
            }
            Event::Shutdown => {
                let _ = write_control(
                    &mut write_half,
                    &mut header_scratch,
                    Bytes::new(),
                    OpCode::Control(Control::Close),
                    role,
                )
                .await;
                return;
            }
        }
    }
}

enum Event {
    Outbound(Frozen<MESSAGE_ALIGN>),
    Control(ControlFrame),
    Shutdown,
}

/// Write one outbound consensus binary frame.
///
/// Server role: header to scratch, frozen payload directly to compio
/// (zero-copy of body). Client role: header + masked body in one
/// scratch, single write.
#[allow(clippy::future_not_send)]
async fn write_consensus_binary(
    write_half: &mut OwnedWriteHalf<TcpStream>,
    header_scratch: &mut Vec<u8>,
    frozen: Frozen<MESSAGE_ALIGN>,
    role: Role,
) -> io::Result<()> {
    let payload_len = frozen.len();
    header_scratch.clear();
    match role {
        Role::Server => {
            let header = build_header(OpCode::Data(Data::Binary), None);
            format_header(&header, payload_len as u64, header_scratch);
            let scratch = std::mem::take(header_scratch);
            let BufResult(r1, returned) = write_half.write_all(scratch).await;
            *header_scratch = returned;
            r1?;
            let BufResult(r2, _) = write_half.write_all(frozen).await;
            r2
        }
        Role::Client => {
            let mask: [u8; 4] = rand::random::<u32>().to_ne_bytes();
            let header = build_header(OpCode::Data(Data::Binary), Some(mask));
            format_header(&header, payload_len as u64, header_scratch);
            let header_len = header_scratch.len();
            header_scratch.extend_from_slice(frozen.as_slice());
            apply_mask(&mut header_scratch[header_len..], mask);
            let scratch = std::mem::take(header_scratch);
            let BufResult(r, returned) = write_half.write_all(scratch).await;
            *header_scratch = returned;
            r
        }
    }
}

/// Write one outbound control frame (Pong or Close). Always single
/// write: control payloads are at most 125 B per RFC 6455 §5.5.
#[allow(clippy::future_not_send)]
async fn write_control(
    write_half: &mut OwnedWriteHalf<TcpStream>,
    header_scratch: &mut Vec<u8>,
    payload: Bytes,
    opcode: OpCode,
    role: Role,
) -> io::Result<()> {
    header_scratch.clear();
    let mask = match role {
        Role::Server => None,
        Role::Client => Some(rand::random::<[u8; 4]>()),
    };
    let header = build_header(opcode, mask);
    format_header(&header, payload.len() as u64, header_scratch);
    let header_len = header_scratch.len();
    header_scratch.extend_from_slice(&payload);
    if let Some(mask) = mask {
        apply_mask(&mut header_scratch[header_len..], mask);
    }
    let scratch = std::mem::take(header_scratch);
    let BufResult(r, returned) = write_half.write_all(scratch).await;
    *header_scratch = returned;
    r
}

const fn build_header(opcode: OpCode, mask: Option<[u8; 4]>) -> FrameHeader {
    FrameHeader {
        is_final: true,
        rsv1: false,
        rsv2: false,
        rsv3: false,
        opcode,
        mask,
    }
}

/// Format a [`FrameHeader`] into `out`. Tungstenite's
/// [`FrameHeader::format`] only fails on `Write::write_all` errors;
/// `Vec<u8>::write_all` is infallible.
fn format_header(header: &FrameHeader, payload_len: u64, out: &mut Vec<u8>) {
    header
        .format(payload_len, out)
        .expect("FrameHeader::format on Vec<u8> is infallible");
}

/// Read half. Polls the reader task's inbound queue.
pub struct WsTransportReader {
    rx: Receiver<Message<GenericHeader>>,
}

impl TransportReader for WsTransportReader {
    #[allow(clippy::future_not_send)]
    async fn read_message(
        &mut self,
        _max_message_size: usize,
    ) -> Result<Message<GenericHeader>, IggyError> {
        // Bound check happens inside [`parse_one_frame`] (against
        // [`framing::MAX_MESSAGE_SIZE`]) and inside [`decode_consensus_frame`]
        // (against the build-time max). The per-call override exists for
        // parity with the trait surface; runtime override plumbing lands
        // when a runtime-configurable cap is needed.
        self.rx
            .recv()
            .await
            .map_err(|_| IggyError::ConnectionClosed)
    }
}

/// Write half. Pushes frames into the writer task's outbound queue.
pub struct WsTransportWriter {
    tx: Sender<Frozen<MESSAGE_ALIGN>>,
}

impl TransportWriter for WsTransportWriter {
    #[allow(clippy::future_not_send)]
    async fn send_batch(&mut self, batch: &mut Vec<Frozen<MESSAGE_ALIGN>>) -> io::Result<()> {
        // Drain in order; one queue push per frame. Atomic-or-error
        // contract holds because the writer task writes each frame
        // before pulling the next from the queue.
        for buf in batch.drain(..) {
            self.tx
                .send(buf)
                .await
                .map_err(|_| io::Error::other("WS writer queue closed"))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use compio::net::TcpStream;
    use iggy_binary_protocol::{Command2, HEADER_SIZE};
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
        let conn = WsTransportConn::new_client(ws_client);
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
        // Server's reader closes; queue closes, surfacing as ConnectionClosed.
        let res = server_task.await.unwrap();
        assert!(matches!(res, Err(IggyError::ConnectionClosed)));
    }

    /// Full bidirectional round-trip: client sends one frame, server
    /// reader receives it and the server's writer pushes a Reply that
    /// the client's reader observes. This is the regression test the
    /// pre-rewrite dispatcher could not pass.
    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn server_to_client_reply_round_trip() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        let ws_listener = WsTransportListener::new(listener);

        let server_task = compio::runtime::spawn(async move {
            let (conn, _peer) = ws_listener.accept().await.expect("accept");
            let (mut reader, mut writer) = conn.into_split();
            let req = reader
                .read_message(framing::MAX_MESSAGE_SIZE)
                .await
                .expect("server read");
            assert_eq!(req.header().command, Command2::Request);
            let mut batch = vec![header_only(Command2::Reply)];
            writer.send_batch(&mut batch).await.expect("server reply");
        });

        let client_tcp = TcpStream::connect(server_addr).await.unwrap();
        let url = format!("ws://{server_addr}/");
        let (ws_client, _resp) = compio_ws::client_async(url, client_tcp)
            .await
            .expect("ws handshake");
        let (mut client_reader, mut client_writer) =
            WsTransportConn::new_client(ws_client).into_split();

        let mut batch = vec![header_only(Command2::Request)];
        client_writer
            .send_batch(&mut batch)
            .await
            .expect("client send");

        let reply = compio::time::timeout(
            Duration::from_secs(2),
            client_reader.read_message(framing::MAX_MESSAGE_SIZE),
        )
        .await
        .expect("client must receive reply within 2 s")
        .expect("reply frame");
        assert_eq!(reply.header().command, Command2::Reply);
        server_task.await.unwrap();
    }

    /// Verify a 16 MiB body round-trips intact through both reader and
    /// writer paths.
    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn large_frame_round_trip() {
        const BODY_SIZE: usize = 16 * 1024 * 1024;
        let total = HEADER_SIZE + BODY_SIZE;

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
        let (ws_client, _resp) = compio_ws::client_async(url, client_tcp)
            .await
            .expect("ws handshake");
        let (_r, mut w) = WsTransportConn::new_client(ws_client).into_split();
        let mut batch = vec![padded(Command2::Request, total)];
        w.send_batch(&mut batch).await.expect("send");

        let msg = compio::time::timeout(Duration::from_secs(5), server_task)
            .await
            .expect("server task within 5 s")
            .unwrap()
            .expect("msg");
        assert_eq!(msg.header().command, Command2::Request);
        assert_eq!(msg.header().size as usize, total);
    }

    /// Apply-mask round-trips a payload back to itself.
    #[test]
    fn apply_mask_round_trip() {
        let mask = [0x11, 0x22, 0x33, 0x44];
        let original: Vec<u8> = (0u8..200).collect();
        let mut masked = original.clone();
        apply_mask(&mut masked, mask);
        assert_ne!(masked, original);
        apply_mask(&mut masked, mask);
        assert_eq!(masked, original);
    }

    /// Server reading an unmasked client frame must be rejected.
    #[test]
    fn server_role_rejects_unmasked_inbound() {
        // Build an unmasked Binary frame with a 0-byte payload.
        let mut buf = Vec::new();
        let header = build_header(OpCode::Data(Data::Binary), None);
        format_header(&header, 0, &mut buf);
        let res = parse_one_frame(&mut buf, Role::Server);
        assert!(matches!(res, Err(FramingError::UnexpectedMask)));
    }

    /// Client reading a masked server frame must be rejected.
    #[test]
    fn client_role_rejects_masked_inbound() {
        let mut buf = Vec::new();
        let header = build_header(OpCode::Data(Data::Binary), Some([1, 2, 3, 4]));
        format_header(&header, 0, &mut buf);
        let res = parse_one_frame(&mut buf, Role::Client);
        assert!(matches!(res, Err(FramingError::UnexpectedMask)));
    }

    /// Fragmented inbound frame is rejected on this plane.
    #[test]
    fn fragmented_inbound_rejected() {
        let mut buf = Vec::new();
        let header = FrameHeader {
            is_final: false,
            rsv1: false,
            rsv2: false,
            rsv3: false,
            opcode: OpCode::Data(Data::Binary),
            mask: Some([1, 2, 3, 4]),
        };
        format_header(&header, 0, &mut buf);
        let res = parse_one_frame(&mut buf, Role::Server);
        assert!(matches!(res, Err(FramingError::Fragmented)));
    }

    /// Text frame is rejected (binary-only plane).
    #[test]
    fn text_inbound_rejected() {
        let mut buf = Vec::new();
        let header = build_header(OpCode::Data(Data::Text), Some([1, 2, 3, 4]));
        format_header(&header, 0, &mut buf);
        let res = parse_one_frame(&mut buf, Role::Server);
        assert!(matches!(res, Err(FramingError::UnsupportedOpcode)));
    }

    /// Partial frame returns `Ok(None)` and leaves the accumulator
    /// untouched.
    #[test]
    fn partial_frame_keeps_accumulator() {
        let mut buf: Vec<u8> = vec![0x82]; // FIN + Binary opcode, but no length byte
        let before = buf.clone();
        let res = parse_one_frame(&mut buf, Role::Server);
        assert!(matches!(res, Ok(None)));
        assert_eq!(buf, before, "accumulator must not be drained on partial");
    }

    /// Inbound Pong frames are silently consumed and the reader keeps
    /// going. Verified by sending a Pong followed by a real consensus
    /// frame and confirming the consensus frame still surfaces.
    #[test]
    fn pong_inbound_silently_consumed() {
        let mut buf = Vec::new();
        let header = build_header(OpCode::Control(Control::Pong), Some([1, 2, 3, 4]));
        format_header(&header, 0, &mut buf);
        let res = parse_one_frame(&mut buf, Role::Server);
        assert!(matches!(res, Ok(Some(InboundFrame::Pong))));
        assert!(buf.is_empty());
    }

    /// Peer-initiated Close on the server-side reader surfaces as
    /// `ConnectionClosed` on the inbound queue.
    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn peer_initiated_close_drains_inbound_queue() {
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
        // Client sends a clean Close. compio_ws::WebSocketStream::close
        // emits a Close frame and waits for the server's Close reply or
        // EOF.
        ws_client.close(None).await.expect("client close");

        let res = compio::time::timeout(Duration::from_secs(2), server_task)
            .await
            .expect("server task within 2 s")
            .unwrap();
        assert!(matches!(res, Err(IggyError::ConnectionClosed)));
    }
}
