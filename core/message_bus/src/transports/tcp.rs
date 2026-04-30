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

//! TCP impl of the [`super::TransportConn`] / [`super::TransportListener`]
//! traits.
//!
//! Behavior-preserving wrappers around `compio::net::TcpListener` and
//! `compio::net::TcpStream`. `TcpTransportConn::run` splits the stream
//! into owned read / write halves, spawns a reader task that calls
//! [`framing::read_message`] and forwards each decoded frame to
//! `ActorContext::in_tx`, and a writer task that drains
//! `ActorContext::rx`, batches up to `max_batch` frames per submission,
//! and emits them via `compio::io::AsyncWriteExt::write_vectored_all`
//! (one writev in the common case, looping on short writes; zero
//! intermediate copies of `Frozen`).

use super::{ActorContext, TransportConn, TransportListener};
use crate::framing;
use crate::lifecycle::BusMessage;
use compio::driver::{SharedFd, ToSharedFd};
use compio::io::AsyncWriteExt;
use compio::net::{OwnedReadHalf, OwnedWriteHalf, TcpListener, TcpStream};
use futures::FutureExt;
use std::io;
use std::mem;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use tracing::{debug, error, trace};

/// Inbound TCP listener wrapper.
///
/// Constructed from an already-bound [`TcpListener`] so the caller
/// keeps control over socket options (`SO_REUSEPORT`, `nodelay`,
/// `keepalive`) via `compio::net::SocketOpts`. See
/// [`crate::replica::listener::bind`] and
/// [`crate::client_listener::tcp::bind`] for the canonical construction.
///
/// Currently only used by trait-conformance tests; production `accept`
/// loops in [`crate::replica::listener`] / [`crate::client_listener`]
/// drive a raw `TcpListener` directly. Kept `pub(crate)` so the
/// `TransportListener` impl below is actually exercisable; the
/// `dead_code` allow is intentional and will fall away when the listener
/// loops migrate behind the trait.
#[allow(dead_code)]
pub(crate) struct TcpTransportListener {
    inner: TcpListener,
}

impl TcpTransportListener {
    #[must_use]
    #[allow(dead_code)]
    pub(crate) const fn new(inner: TcpListener) -> Self {
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
/// of the stream; [`TransportConn::run`] consumes it and drives both
/// reader and writer tasks internally.
pub(crate) struct TcpTransportConn {
    stream: TcpStream,
}

impl TcpTransportConn {
    #[must_use]
    pub(crate) const fn new(stream: TcpStream) -> Self {
        Self { stream }
    }
}

impl TransportConn for TcpTransportConn {
    #[allow(clippy::future_not_send)]
    async fn run(self, ctx: ActorContext) {
        // Capture a refcounted fd handle BEFORE `into_split` so the
        // shutdown watchdog can wake the reader's parked io_uring read
        // SQE via `libc::shutdown(SHUT_RD)`. No `select!` over a TCP
        // read on a still-alive connection: the in-flight read returns
        // `Ok(0)` naturally rather than being cancelled (compio's
        // io_uring read is not cancel-safe in the protocol sense).
        let shared_fd = self.stream.to_shared_fd();
        let (read_half, write_half) = self.stream.into_split();
        let ActorContext {
            in_tx,
            rx,
            shutdown,
            conn_shutdown,
            max_batch,
            max_message_size,
            label,
            peer,
        } = ctx;

        spawn_shutdown_watchdog(shared_fd, shutdown.clone(), label, peer.clone());

        let writer_shutdown = shutdown;
        let reader_peer = peer.clone();
        let reader_handle = compio::runtime::spawn(reader_loop(
            read_half,
            in_tx,
            max_message_size,
            label,
            reader_peer,
        ));
        let writer_handle = compio::runtime::spawn(writer_loop(
            write_half,
            rx,
            writer_shutdown,
            conn_shutdown,
            max_batch,
            label,
            peer,
        ));
        let _ = reader_handle.await;
        let _ = writer_handle.await;
    }
}

/// Spawn a detached watchdog that calls `libc::shutdown(fd, SHUT_RD)`
/// when the shutdown token fires. The reader's pending `io_uring`
/// read SQE then completes with `Ok(0)`, the reader observes EOF, and the
/// reader loop exits without ever sitting inside a `select!` over a
/// TCP read.
///
/// On a natural-EOF run (peer closed first) the watchdog stays parked
/// until the bus-wide shutdown token ultimately fires; the resulting
/// `libc::shutdown` on a fd whose socket is already closed is benign
/// (returns ENOTCONN). All TCP-family transports converge on this
/// shutdown model: the reader never sits inside a `select!` against the
/// shutdown token; the watchdog wakes the parked `io_uring` read instead.
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
        // still-live `TcpStream` before `into_split`; the matching
        // clones in both owned halves keep the kernel fd open. The
        // watchdog's clone independently keeps it open across this
        // syscall.
        let raw_fd = shared_fd.as_raw_fd();
        let rc = unsafe { libc::shutdown(raw_fd, libc::SHUT_RD) };
        if rc != 0 {
            let err = io::Error::last_os_error();
            // ENOTCONN is expected when the peer closed first.
            if err.raw_os_error() != Some(libc::ENOTCONN) {
                debug!(%label, %peer, error = ?err, "tcp watchdog: SHUT_RD returned");
            }
        }
    })
    .detach();
}

/// Read framed consensus messages off the wire and forward each to
/// [`ActorContext::in_tx`]. Exits on EOF, framing error, or send-side
/// closure.
///
/// No `select!` over the TCP read. Cooperative shutdown is delivered
/// via [`spawn_shutdown_watchdog`], which calls `libc::shutdown(SHUT_RD)`
/// when the bus token fires; the in-flight read returns `Ok(0)` and
/// `framing::read_message` surfaces it as an EOF error on the next
/// iteration.
#[allow(clippy::future_not_send)]
async fn reader_loop(
    mut read_half: OwnedReadHalf<TcpStream>,
    in_tx: async_channel::Sender<
        iggy_binary_protocol::Message<iggy_binary_protocol::GenericHeader>,
    >,
    max_message_size: usize,
    label: &'static str,
    peer: String,
) {
    loop {
        match framing::read_message(&mut read_half, max_message_size).await {
            Ok(msg) => {
                if in_tx.send(msg).await.is_err() {
                    debug!(%label, %peer, "tcp reader: inbound queue dropped");
                    return;
                }
            }
            Err(e) => {
                debug!(%label, %peer, "tcp reader: read error: {e:?}");
                return;
            }
        }
    }
}

/// Drain [`ActorContext::rx`], coalesce up to `max_batch` frames into a
/// single `writev_all` syscall, exit on shutdown, channel close, or
/// write error.
///
/// On exit (clean OR panic via compio's `catch_unwind`) the scopeguard
/// triggers the per-connection [`crate::lifecycle::Shutdown`]. The
/// transport's watchdog observes the conn-side fire and calls
/// `libc::shutdown(SHUT_RD)`, breaking the reader's parked `io_uring`
/// read so the reader exits and the installer's
/// `transport_handle` scopeguard can evict the registry slot. Without
/// this trigger, a writer-task panic would leave the reader parked
/// indefinitely on the live socket.
#[allow(clippy::future_not_send)]
async fn writer_loop(
    mut write_half: OwnedWriteHalf<TcpStream>,
    rx: crate::lifecycle::BusReceiver,
    shutdown: crate::lifecycle::FusedShutdown,
    conn_shutdown: crate::lifecycle::Shutdown,
    max_batch: usize,
    label: &'static str,
    peer: String,
) {
    let _wake_reader = scopeguard::guard(conn_shutdown, |s| s.trigger());
    let mut batch: Vec<BusMessage> = Vec::with_capacity(max_batch);
    let mut shutdown_fut = Box::pin(shutdown.wait().fuse());

    loop {
        let first = futures::select! {
            () = shutdown_fut.as_mut() => {
                debug!(%label, %peer, "tcp writer: shutdown observed");
                return;
            }
            msg = rx.recv().fuse() => {
                if let Ok(m) = msg {
                    m
                } else {
                    debug!(%label, %peer, "tcp writer: channel closed");
                    return;
                }
            }
        };

        batch.push(first);
        while batch.len() < max_batch {
            match rx.try_recv() {
                Ok(m) => batch.push(m),
                Err(_) => break,
            }
        }

        let drained = batch.len();
        trace!(%label, %peer, batch = drained, "writev batch");

        let owned = mem::take(&mut batch);
        let compio::BufResult(result, mut returned) = write_half.write_vectored_all(owned).await;
        returned.clear();
        batch = returned;

        if let Err(e) = result {
            error!(
                %label,
                %peer,
                error = ?e,
                batch_len = drained,
                "tcp writer: writev failed, dropping batch"
            );
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::Shutdown;
    use async_channel::bounded;
    use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
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

    #[allow(clippy::future_not_send)]
    fn drive(
        conn: TcpTransportConn,
    ) -> (
        async_channel::Sender<Frozen<MESSAGE_ALIGN>>,
        async_channel::Receiver<Message<GenericHeader>>,
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
    async fn run_pumps_three_frames_through_writev() {
        let (client, server) = local_pair().await;
        let (client_out, _client_in, client_shutdown, client_handle) =
            drive(TcpTransportConn::new(client));
        let (_server_out, server_in, server_shutdown, server_handle) =
            drive(TcpTransportConn::new(server));

        for cmd in [Command2::Ping, Command2::Prepare, Command2::Request] {
            client_out.send(header_only(cmd)).await.unwrap();
        }

        let recv_with_timeout = |rx: &async_channel::Receiver<Message<GenericHeader>>| {
            let rx = rx.clone();
            async move {
                compio::time::timeout(Duration::from_secs(2), rx.recv())
                    .await
                    .expect("recv within 2s")
                    .expect("ok")
            }
        };
        let a = recv_with_timeout(&server_in).await;
        let b = recv_with_timeout(&server_in).await;
        let c = recv_with_timeout(&server_in).await;
        assert_eq!(a.header().command, Command2::Ping);
        assert_eq!(b.header().command, Command2::Prepare);
        assert_eq!(c.header().command, Command2::Request);

        client_shutdown.trigger();
        server_shutdown.trigger();
        let _ = client_handle.await;
        let _ = server_handle.await;
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn run_exits_on_shutdown_signal() {
        let (client, _server) = local_pair().await;
        let (_out_tx, _in_rx, shutdown, handle) = drive(TcpTransportConn::new(client));
        shutdown.trigger();
        let res = compio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(res.is_ok(), "run must exit within 2s of shutdown");
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn run_reports_oversize_frame_and_exits() {
        let (mut client, server) = local_pair().await;
        let (_out, server_in, server_shutdown, server_handle) =
            drive(TcpTransportConn::new(server));

        let mut buf = vec![0u8; HEADER_SIZE];
        let bogus = u32::try_from(framing::MAX_MESSAGE_SIZE + 1)
            .unwrap_or(u32::MAX)
            .to_le_bytes();
        buf[48..52].copy_from_slice(&bogus);
        client.write_all(buf).await.0.unwrap();

        let recv = compio::time::timeout(Duration::from_secs(2), server_in.recv()).await;
        assert!(
            recv.is_ok(),
            "in_rx should close within 2s on framing error"
        );
        assert!(
            recv.unwrap().is_err(),
            "framing error tears the reader down and closes in_tx"
        );
        server_shutdown.trigger();
        let _ = server_handle.await;
    }
}
