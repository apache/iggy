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
//! - [`TransportConn`] is a single byte-oriented connection; it splits
//!   into a [`TransportReader`] and a [`TransportWriter`] that move into
//!   independent compio tasks (reader loop, writer-batch task).
//! - [`TransportReader`] reads one framed `Message<GenericHeader>` per
//!   call.
//! - [`TransportWriter`] sends a batch of `Frozen<MESSAGE_ALIGN>`
//!   atomically in a single `writev` (TCP) or per-transport analog
//!   (framed binary for WS, one STREAM per message for QUIC).
//!
//! # Invariants the trait surface must preserve
//!
//! - **No-yield on caller-side** (I2): producers call
//!   [`MessageBus::send_to_client`](crate::MessageBus::send_to_client) /
//!   [`send_to_replica`](crate::MessageBus::send_to_replica), which push
//!   onto a bounded `async_channel` and return `Ready` on the first
//!   poll. The single yield point in the wire path lives inside
//!   [`TransportWriter::send_batch`], invoked by the per-peer writer
//!   task. Consensus code relies on this for reentrancy reasoning.
//! - **Batch atomicity** (I4): `send_batch` either writes every buffer
//!   in the batch or returns an error; no partial success is exposed.
//!   TCP satisfies this via `compio::io::AsyncWriteExt::write_vectored_all`;
//!   WS/QUIC impls loop internally until the batch lands or fail.
//! - **Zero-copy `Frozen` ownership** (I8): the batch is handed to the
//!   kernel without intermediate copies. Impls MUST NOT clone
//!   `Frozen<MESSAGE_ALIGN>` or materialize a `Vec<u8>` on the hot
//!   path.
//!
//! # Design notes
//!
//! - Associated types (not trait objects): keep the hot path
//!   monomorphic. A `dyn TransportConn` surface can be introduced later
//!   if a config-time transport selector needs runtime polymorphism.
//! - `'static` bound on [`TransportConn`], [`TransportReader`], and
//!   [`TransportWriter`]: the halves are moved into
//!   `compio::runtime::spawn`'d tasks, which require owned data.
//! - fd-delegation (`F_DUPFD_CLOEXEC`) stays TCP-only and lives outside
//!   this trait (see [`crate::fd_transfer`]). Other transports terminate
//!   on shard 0 and forward `Frozen<MESSAGE_ALIGN>` over the inter-shard
//!   flume via [`crate::ShardForwardFn`].
//!
//! The TCP impl lives in `transports/tcp.rs` (P1-T2). Phase 2+ transports
//! plug in behind the same surface; see
//! `Documents/silverhand/iggy/message_bus/transport-plan/`.

pub mod quic;
pub mod tcp;

// Only `Conn` and `Writer` have crate-internal callers today
// (`installer.rs` wraps the dialed/accepted stream in a `TcpTransportConn`;
// `writer_task.rs` wraps the owned write half in a `TcpTransportWriter`).
// `TcpTransportListener` and `TcpTransportReader` stay reachable via the
// `tcp` submodule and exist primarily so the trait surface compiles
// end-to-end against a real impl; future WSS / QUIC listeners drop in
// alongside without touching call sites.
pub(crate) use tcp::{TcpTransportConn, TcpTransportWriter};

use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
use iggy_binary_protocol::consensus::iobuf::Frozen;
use iggy_binary_protocol::{GenericHeader, Message};
use iggy_common::IggyError;
use std::io;
use std::net::SocketAddr;

/// Inbound listener. The accept loops in [`crate::replica_listener`]
/// and [`crate::client_listener`] invoke `accept` inside a `select!`
/// against the bus-wide shutdown token.
#[allow(async_fn_in_trait)]
pub trait TransportListener {
    /// Connection type produced by this listener.
    type Conn: TransportConn;

    /// Accept the next inbound connection.
    ///
    /// Returns once a peer has completed the transport-level handshake
    /// (TCP SYN/ACK; TLS; QUIC handshake). The `message_bus`
    /// authenticated `Ping` handshake runs on the returned connection,
    /// after this call. See [`crate::auth`].
    ///
    /// # Errors
    ///
    /// Returns [`io::Error`] on socket faults. The caller logs and
    /// continues; a fatal error must be surfaced by dropping the
    /// listener rather than through this return value.
    async fn accept(&self) -> io::Result<(Self::Conn, SocketAddr)>;
}

/// Single-connection handle produced by [`TransportListener::accept`]
/// or by a transport-specific dialer (`TcpStream::connect`, etc.).
///
/// `'static` so the reader and writer halves can move into spawned
/// compio tasks.
pub trait TransportConn: 'static {
    /// Read half moved into the per-connection reader task.
    type Reader: TransportReader;
    /// Write half moved into the per-connection writer-batch task.
    type Writer: TransportWriter;

    /// Split the connection into independently-owned halves.
    ///
    /// Mirrors [`compio::net::TcpStream::into_split`] on TCP. WS and
    /// QUIC impls map onto their own half-splits or stream wrappers.
    fn into_split(self) -> (Self::Reader, Self::Writer);
}

/// Read half. Produces one framed [`Message<GenericHeader>`] per call.
///
/// `'static` so the half can be moved into a `compio::runtime::spawn`'d
/// task. `Unpin` is NOT required at the trait level: TCP impls satisfy it
/// trivially via `OwnedReadHalf<TcpStream>`, and a future
/// `compio-quic::RecvStream` impl that is not `Unpin` can pin internally
/// without forcing every other impl to `Box::pin`.
#[allow(async_fn_in_trait)]
pub trait TransportReader: 'static {
    /// Read the next `GenericHeader`-framed message off the wire.
    ///
    /// Validates the 256 B header and bounds the total frame size to
    /// `max_message_size`. TCP delegates to
    /// [`crate::framing::read_message`]; WS wraps the single binary
    /// frame; QUIC consumes one STREAM worth of bytes.
    ///
    /// # Errors
    ///
    /// - [`IggyError::ConnectionClosed`] on clean EOF.
    /// - [`IggyError::TcpError`] on transport I/O faults.
    /// - [`IggyError::InvalidCommand`] on framing violations (bad size
    ///   field, total frame exceeding `max_message_size`, header decode
    ///   failure).
    async fn read_message(
        &mut self,
        max_message_size: usize,
    ) -> Result<Message<GenericHeader>, IggyError>;
}

/// Write half. Atomic-or-error batched writer.
///
/// `'static` so the half can be moved into a `compio::runtime::spawn`'d
/// task. `Unpin` is NOT required at the trait level (see
/// [`TransportReader`] for the rationale).
#[allow(async_fn_in_trait)]
pub trait TransportWriter: 'static {
    /// Send every buffer in `batch` atomically.
    ///
    /// On success the Vec is drained (empty on return); the same
    /// allocation is reused by the writer loop on the next iteration,
    /// so impls must not free it. On error the buffers may or may not
    /// have partially landed on the wire; the caller MUST treat a
    /// failed batch as lost data and drop the connection. VSR
    /// retransmits via the WAL.
    ///
    /// TCP implements this via
    /// [`compio::io::AsyncWriteExt::write_vectored_all`] on the owned
    /// write half, with `max_batch <= IOV_MAX / 2 = 512`. WS/QUIC impls
    /// loop internally until every buffer lands or fail the whole
    /// batch.
    ///
    /// # Errors
    ///
    /// Returns [`io::Error`] on transport faults. No partial-success
    /// variant is exposed.
    async fn send_batch(&mut self, batch: &mut Vec<Frozen<MESSAGE_ALIGN>>) -> io::Result<()>;
}

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    //! Compile-only stubs that exercise each trait's associated-type
    //! plumbing and method signatures. Method bodies `unimplemented!()`
    //! so every compile-time obligation (bounds, lifetimes, `'static`)
    //! is enforced without needing a runtime harness.
    use super::{TransportConn, TransportListener, TransportReader, TransportWriter};
    use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
    use iggy_binary_protocol::consensus::iobuf::Frozen;
    use iggy_binary_protocol::{GenericHeader, Message};
    use iggy_common::IggyError;
    use std::io;
    use std::net::SocketAddr;

    struct StubReader;
    struct StubWriter;
    struct StubConn;
    struct StubListener;

    impl TransportReader for StubReader {
        async fn read_message(
            &mut self,
            _max_message_size: usize,
        ) -> Result<Message<GenericHeader>, IggyError> {
            unimplemented!()
        }
    }

    impl TransportWriter for StubWriter {
        async fn send_batch(&mut self, _batch: &mut Vec<Frozen<MESSAGE_ALIGN>>) -> io::Result<()> {
            unimplemented!()
        }
    }

    impl TransportConn for StubConn {
        type Reader = StubReader;
        type Writer = StubWriter;
        fn into_split(self) -> (Self::Reader, Self::Writer) {
            unimplemented!()
        }
    }

    impl TransportListener for StubListener {
        type Conn = StubConn;
        async fn accept(&self) -> io::Result<(Self::Conn, SocketAddr)> {
            unimplemented!()
        }
    }
}
