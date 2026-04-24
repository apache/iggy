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

//! TCP impls of the [`super`] transport traits.
//!
//! Behavior-preserving wrappers around `compio::net::TcpListener`,
//! `compio::net::TcpStream`, and the split halves. The hot path on
//! [`TcpTransportWriter::send_batch`] is identical to
//! [`crate::writer_task::run`]'s inner `write_vectored_all` call: one
//! syscall per batch, zero intermediate copies of `Frozen`.
//!
//! Callers that need listener / dialer ergonomics (socket options,
//! keepalive, directional accept rules) should continue to use the
//! free functions in [`crate::replica_listener`] and
//! [`crate::client_listener`]; this module is the minimal trait
//! adapter, not a replacement for those call sites. Integration with
//! `installer::install_*_stream` lands in P1-T3.

use super::{TransportConn, TransportListener, TransportReader, TransportWriter};
use crate::framing;
use compio::io::AsyncWriteExt;
use compio::net::{OwnedReadHalf, OwnedWriteHalf, TcpListener, TcpStream};
use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
use iggy_binary_protocol::consensus::iobuf::Frozen;
use iggy_binary_protocol::{GenericHeader, Message};
use iggy_common::IggyError;
use std::io;
use std::mem;
use std::net::SocketAddr;

/// Inbound TCP listener wrapper.
///
/// Constructed from an already-bound [`TcpListener`] so the caller
/// keeps control over socket options (`SO_REUSEPORT`, `nodelay`,
/// `keepalive`) via `compio::net::SocketOpts`. See
/// [`crate::replica_listener::bind`] and
/// [`crate::client_listener::bind`] for the canonical construction.
pub struct TcpTransportListener {
    inner: TcpListener,
}

impl TcpTransportListener {
    #[must_use]
    pub const fn new(inner: TcpListener) -> Self {
        Self { inner }
    }
}

impl TransportListener for TcpTransportListener {
    type Conn = TcpTransportConn;

    #[allow(clippy::future_not_send)]
    async fn accept(&self) -> io::Result<(Self::Conn, SocketAddr)> {
        let (stream, addr) = self.inner.accept().await?;
        Ok((TcpTransportConn::new(stream), addr))
    }
}

/// Single TCP connection.
///
/// Produced by [`TcpTransportListener::accept`] or by wrapping the
/// result of a `TcpStream::connect` on the dialer path. Takes ownership
/// of the stream; [`Self::into_split`] transfers that ownership into
/// the read and write halves bound to the per-connection tasks.
pub struct TcpTransportConn {
    stream: TcpStream,
}

impl TcpTransportConn {
    #[must_use]
    pub const fn new(stream: TcpStream) -> Self {
        Self { stream }
    }
}

impl TransportConn for TcpTransportConn {
    type Reader = TcpTransportReader;
    type Writer = TcpTransportWriter;

    fn into_split(self) -> (Self::Reader, Self::Writer) {
        let (read_half, write_half) = self.stream.into_split();
        (
            TcpTransportReader { inner: read_half },
            TcpTransportWriter { inner: write_half },
        )
    }
}

/// Read half bound to the per-connection reader task.
///
/// [`TransportReader::read_message`] delegates to
/// [`framing::read_message`]; the two paths share the same header
/// decode, bounds check, and zero-copy `Owned<MESSAGE_ALIGN>`
/// allocation strategy.
pub struct TcpTransportReader {
    inner: OwnedReadHalf<TcpStream>,
}

impl TcpTransportReader {
    #[must_use]
    pub const fn new(inner: OwnedReadHalf<TcpStream>) -> Self {
        Self { inner }
    }
}

impl TransportReader for TcpTransportReader {
    #[allow(clippy::future_not_send)]
    async fn read_message(
        &mut self,
        max_message_size: usize,
    ) -> Result<Message<GenericHeader>, IggyError> {
        framing::read_message(&mut self.inner, max_message_size).await
    }
}

/// Write half bound to the per-connection writer-batch task.
///
/// [`TransportWriter::send_batch`] calls
/// [`compio::io::AsyncWriteExt::write_vectored_all`] exactly once per
/// invocation. The caller (e.g. [`crate::writer_task::run`]) is
/// responsible for capping the batch size to
/// `max_batch <= IOV_MAX / 2 = 512`; this impl does not enforce a cap
/// because the Vec is already drained by the caller's admission
/// control.
pub struct TcpTransportWriter {
    inner: OwnedWriteHalf<TcpStream>,
}

impl TcpTransportWriter {
    #[must_use]
    pub const fn new(inner: OwnedWriteHalf<TcpStream>) -> Self {
        Self { inner }
    }
}

impl TransportWriter for TcpTransportWriter {
    #[allow(clippy::future_not_send)]
    async fn send_batch(&mut self, batch: &mut Vec<Frozen<MESSAGE_ALIGN>>) -> io::Result<()> {
        // `write_vectored_all` consumes the Vec via compio's `IoVectoredBuf`
        // surface and returns it through `BufResult` so we can reuse the
        // allocation. Take the inner Vec, hand it to the kernel, put the
        // (now-empty) returned Vec back into the caller's slot.
        let owned = mem::take(batch);
        let compio::BufResult(result, mut returned) = self.inner.write_vectored_all(owned).await;
        returned.clear();
        *batch = returned;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::consensus::iobuf::Frozen;
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

    #[allow(clippy::future_not_send)]
    async fn local_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connect = TcpStream::connect(addr);
        let accept = listener.accept();
        let (client_res, accept_res) = futures::join!(connect, accept);
        let (server, _) = accept_res.unwrap();
        (client_res.unwrap(), server)
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn listener_accept_yields_conn() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let wrapped = TcpTransportListener::new(listener);

        let connect = TcpStream::connect(addr);
        let accept = wrapped.accept();
        let (_client, accept_res) = futures::join!(connect, accept);
        let (_conn, _peer_addr) = accept_res.expect("accept via trait");
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn send_batch_writes_all_and_drains_vec() {
        let (client, server) = local_pair().await;
        let client_conn = TcpTransportConn::new(client);
        let (_client_read, mut client_write) = client_conn.into_split();

        let server_conn = TcpTransportConn::new(server);
        let (mut server_read, _server_write) = server_conn.into_split();

        let mut batch = vec![
            header_only(Command2::Ping),
            header_only(Command2::Prepare),
            header_only(Command2::Request),
        ];
        client_write
            .send_batch(&mut batch)
            .await
            .expect("send_batch");
        assert!(batch.is_empty(), "Vec must be drained on success");
        assert!(batch.capacity() >= 3, "allocation must be reused");

        // Verify all three frames land intact in order.
        let a = server_read
            .read_message(framing::MAX_MESSAGE_SIZE)
            .await
            .unwrap();
        let b = server_read
            .read_message(framing::MAX_MESSAGE_SIZE)
            .await
            .unwrap();
        let c = server_read
            .read_message(framing::MAX_MESSAGE_SIZE)
            .await
            .unwrap();
        assert_eq!(a.header().command, Command2::Ping);
        assert_eq!(b.header().command, Command2::Prepare);
        assert_eq!(c.header().command, Command2::Request);
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn send_batch_empty_is_noop() {
        let (client, _server) = local_pair().await;
        let (_r, mut w) = TcpTransportConn::new(client).into_split();
        let mut batch: Vec<Frozen<MESSAGE_ALIGN>> = Vec::with_capacity(8);
        w.send_batch(&mut batch).await.expect("empty batch ok");
        assert!(batch.is_empty());
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn read_message_reports_oversize_via_trait() {
        use compio::io::AsyncWriteExt;
        let (mut client, server) = local_pair().await;
        let (mut r, _w) = TcpTransportConn::new(server).into_split();

        // Hand-craft a header with a bogus oversize `size` field; the
        // trait surface must surface the same `InvalidCommand` error the
        // framing path does.
        let mut buf = vec![0u8; HEADER_SIZE];
        let bogus = u32::try_from(framing::MAX_MESSAGE_SIZE + 1)
            .unwrap_or(u32::MAX)
            .to_le_bytes();
        buf[48..52].copy_from_slice(&bogus);
        client.write_all(buf).await.0.unwrap();

        let res = r.read_message(framing::MAX_MESSAGE_SIZE).await;
        assert!(matches!(res, Err(IggyError::InvalidCommand)));
    }
}
