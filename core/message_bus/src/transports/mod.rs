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

//! Transport abstraction for the `message_bus` wire planes.
//!
//! # Trait family
//!
//! - [`TransportListener`] binds a local address and yields inbound
//!   [`TransportConn`]s on `accept`.
//! - [`TransportConn`] is a single byte-oriented connection. Each
//!   transport owns its own internal task topology behind a single
//!   [`TransportConn::run`] entry point that consumes the connection,
//!   pushes inbound consensus frames into [`ActorContext::in_tx`], and
//!   drains outbound frames from [`ActorContext::rx`] until the bus tears
//!   the connection down.
//!
//! # Invariants the trait surface must preserve
//!
//! - **No-yield on caller-side**: producers call
//!   [`MessageBus::send_to_client`](crate::MessageBus::send_to_client) /
//!   [`send_to_replica`](crate::MessageBus::send_to_replica), which push
//!   onto a bounded `async_channel` and return `Ready` on the first
//!   poll. The single yield point in the wire path lives inside the
//!   transport's `run` body, invoked by the per-peer task.
//! - **Batch ordering**: each transport drains the per-peer
//!   `BusReceiver` in FIFO order, but the dispatch shape differs:
//!     * **TCP (vectored batch)** assembles up to `max_batch` frames
//!       into one `write_vectored_all` call. The kernel may short-write
//!       the iovec set (so `writev` is not atomic), but FIFO order
//!       across the batch is preserved and any short or failed write
//!       tears the connection down rather than retrying on a
//!       half-written batch.
//!     * **TCP-TLS, WS, WSS (drain-and-flush)** drain the same
//!       `max_batch` window into a `Vec<BusMessage>` and then write
//!       each frame through the per-record API (`AsyncWriteExt::write_all`
//!       for TLS, `WebSocketStream::send` for WS / WSS) followed by ONE
//!       trailing `flush()` per batch. This avoids per-frame TCP/TLS
//!       record overhead while staying inside the per-record API
//!       constraints of those transports.
//!     * **QUIC (per-frame)** uses one bidirectional stream per peer
//!       and writes each `BusMessage` with a separate
//!       `SendStream::write_all` call; quinn coalesces at the datagram
//!       layer.
//!   In all four cases, any write failure tears the connection down.
//! - **Zero-copy `Frozen` ownership**: outbound frames are handed to
//!   the kernel without intermediate copies on plaintext TCP. WS / QUIC
//!   / TLS planes pay structural copies in their record layers; see the
//!   per-transport module rustdoc.
//!
//! # Design notes
//!
//! - `'static` bound on [`TransportConn`]: the conn is moved into a
//!   `compio::runtime::spawn`'d task by the installer, which requires
//!   owned data.
//! - fd-delegation (`F_DUPFD_CLOEXEC`) stays TCP-only and lives outside
//!   this trait (see [`crate::fd_transfer`]). Other transports terminate
//!   on shard 0 and forward `Frozen<MESSAGE_ALIGN>` over the inter-shard
//!   flume via [`crate::ShardForwardFn`].
//!
//! Per-transport impls live in
//! `transports/{tcp,tcp_tls,ws,wss,quic}.rs`.

pub mod quic;
pub mod tcp;
pub mod tcp_tls;
pub mod tls;
pub mod ws;
pub mod wss;

use crate::lifecycle::{BusReceiver, FusedShutdown};
use async_channel::Sender;
use iggy_binary_protocol::{GenericHeader, Message};
use std::io;
use std::net::SocketAddr;

/// Inbound listener. The accept loops in [`crate::replica::listener`]
/// and [`crate::client_listener`] invoke `accept` inside a `select!`
/// against the bus-wide shutdown token.
#[allow(async_fn_in_trait)]
pub trait TransportListener {
    /// Connection type produced by this listener.
    type Conn: TransportConn;

    /// Accept the next inbound connection.
    ///
    /// Returns once a peer has completed the transport-level handshake
    /// (TCP SYN/ACK; TLS; QUIC handshake). On the replica plane a
    /// plaintext `Ping` frame announcing `replica_id` runs on the
    /// returned connection, after this call.
    ///
    /// # Errors
    ///
    /// Returns [`io::Error`] on socket faults. The caller logs and
    /// continues; a fatal error must be surfaced by dropping the
    /// listener rather than through this return value.
    async fn accept(&self) -> io::Result<(Self::Conn, SocketAddr)>;
}

/// Per-connection runtime context the bus hands to [`TransportConn::run`].
///
/// The transport is the single producer for `in_tx` (inbound framed
/// messages) and the single consumer of `rx` (outbound consensus frames
/// scheduled by the bus's `send_to_*` path). `shutdown` is a fused
/// signal merged from the bus-wide shutdown and the per-connection
/// shutdown that the installer triggers on insert-race; either firing
/// must wake any blocked I/O the transport holds.
pub struct ActorContext {
    /// Inbound channel: the transport pushes one decoded
    /// [`Message<GenericHeader>`] per received frame. The receiver side
    /// is owned by the installer's dispatch task, which forwards each
    /// message to the bus's per-plane handler (`MessageHandler` /
    /// `RequestHandler`).
    pub in_tx: Sender<Message<GenericHeader>>,
    /// Outbound channel: the bus's `send_to_*` path pushes `Frozen`
    /// frames into the matching `Sender`; the transport drains here.
    pub rx: BusReceiver,
    /// Cooperative cancellation. Fires on bus shutdown OR on
    /// per-connection shutdown (insert race); the transport must
    /// observe it on every blocking await on the wire.
    pub shutdown: FusedShutdown,
    /// Trigger handle for the per-connection shutdown that
    /// [`Self::shutdown`] folds in. Held by transports whose writer
    /// task wants to wake the reader on its own exit (panic or
    /// I/O failure) rather than waiting for the bus-wide token: a
    /// scopeguard inside the writer calls
    /// [`crate::lifecycle::Shutdown::trigger`] on exit, the watchdog
    /// observes the conn-side fire, and `libc::shutdown(SHUT_RD)`
    /// breaks the reader's parked `io_uring` read.
    pub conn_shutdown: crate::lifecycle::Shutdown,
    /// Maximum number of `BusMessage` entries coalesced into a single
    /// transport-level write. Capped at `IOV_MAX / 2` on plaintext TCP
    /// so the worst-case writev stays inside the syscall limit.
    pub max_batch: usize,
    /// Wire-level cap on a single framed message, in bytes. The
    /// transport rejects undersize / oversize frames and tears the
    /// connection down.
    pub max_message_size: usize,
    /// Plane label for tracing (`"replica"` or `"client"`). Static so
    /// tracing macros can format without an allocation.
    pub label: &'static str,
    /// Peer identifier for tracing (replica id as decimal, client id as
    /// `0x..` hex). Owned `String` because client ids serialize to 34
    /// chars and replicas to ≤ 3 chars; the cost is one allocation per
    /// installed connection.
    pub peer: String,
}

/// Single-connection handle produced by [`TransportListener::accept`]
/// or by a transport-specific dialer (`TcpStream::connect`, etc.).
///
/// `'static` so the conn can move into a `compio::runtime::spawn`'d
/// task.
#[allow(async_fn_in_trait)]
pub trait TransportConn: 'static {
    /// Take ownership of the connection, drive its internal reader and
    /// writer tasks until shutdown, EOF, or an unrecoverable I/O error.
    ///
    /// The transport must:
    ///
    /// - Push every decoded inbound frame into [`ActorContext::in_tx`]
    ///   in order.
    /// - Drain [`ActorContext::rx`] FIFO and write each batch to the
    ///   wire preserving that order; any short or failed write must
    ///   tear the connection down rather than continue on a partially
    ///   written batch.
    /// - Observe [`ActorContext::shutdown`] on every blocking await so
    ///   the bus's tear-down completes within the configured drain
    ///   budget.
    ///
    /// Returns when the connection has terminated. Errors are logged
    /// internally; the join handle's `Result<()>` is the bus's only
    /// signal that the conn ended.
    async fn run(self, ctx: ActorContext);
}
