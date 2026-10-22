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

//! One TCP or WebSocket connection that carries many requests at once.
//!
//! A writer task and a reader task own the two halves of the connection, and
//! each reply goes to the request with its id. So a caller that goes away
//! never leaves a frame half written or half read on the connection.

use crate::links::{Frame, Reply};
use crate::vsr::{decode_eviction, eviction_reason, peek_command, reply_request_id, response_size};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use iggy_binary_protocol::consensus::{Command, EvictionReason, HEADER_SIZE};
use iggy_common::IggyError;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::{Mutex, Semaphore, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout, timeout_at};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tracing::{debug, error, trace};

/// Frames queued for the writer task before a sender waits for room.
const LINK_WRITE_QUEUE: usize = 32;
/// Requests that wait for a reply on one link at once. It matches
/// `message_bus.client_queue_capacity`, the most requests the server runs at
/// once for one connection.
const MAX_PENDING_PER_LINK: usize = 256;
/// A peer that stops reading cannot hold up a shutdown for longer than this.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const WEBSOCKET_CLOSE_REASON: &str = "Client requested shutdown";

type Waiter = oneshot::Sender<Result<Reply, IggyError>>;

/// A connection that carries many requests at once. Dropping the link stops
/// its tasks, which closes the connection.
#[derive(Debug)]
pub(crate) struct MuxLink {
    shared: Arc<MuxShared>,
    queue: mpsc::Sender<Frame>,
    tasks: LinkTasks,
}

#[derive(Debug)]
struct MuxShared {
    peer: SocketAddr,
    no_session_eviction: AtomicBool,
    pending: StdMutex<Pending>,
    permits: Semaphore,
    /// Before bind, request ids repeat, so one such exchange runs at a time.
    unbound: Mutex<()>,
    /// Turns true once, when the link closes.
    closed: watch::Sender<bool>,
}

#[derive(Debug)]
struct Pending {
    /// Cleared when the link closes. A closed link never opens again.
    open: bool,
    waiters: HashMap<u64, Waiter>,
}

#[derive(Debug)]
struct LinkTasks {
    writer: JoinHandle<()>,
    reader: JoinHandle<()>,
}

impl Drop for LinkTasks {
    fn drop(&mut self) {
        self.writer.abort();
        self.reader.abort();
    }
}

/// A request that waits for its reply. Dropping it ends the wait.
struct PendingEntry<'a> {
    shared: &'a MuxShared,
    request: u64,
    /// Set for a repeated id that reached the queue: its late reply would
    /// answer the next request with the same id.
    close_if_abandoned: bool,
}

impl MuxLink {
    pub(crate) fn no_session_eviction(&self) -> bool {
        self.shared.no_session_eviction.load(Ordering::Acquire)
    }

    /// A link over the two halves of a TCP connection, plain or TLS.
    pub(crate) fn tcp<R, W>(reader: R, writer: W, peer: SocketAddr) -> Self
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        Self::start(
            peer,
            |frames, shared| write_tcp(BufWriter::new(writer), frames, shared),
            |shared| read_tcp(BufReader::new(reader), shared),
        )
    }

    /// A link over a WebSocket connection, plain or TLS.
    pub(crate) fn websocket<S>(stream: WebSocketStream<S>, peer: SocketAddr) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (sink, stream) = stream.split();
        Self::start(
            peer,
            |frames, shared| write_websocket(sink, frames, shared),
            |shared| read_websocket(stream, shared),
        )
    }

    /// Sends `frame` and waits for its reply until `deadline`. `sent` turns
    /// true when the frame reaches the queue, so a caller that goes away
    /// before the reply still knows that the frame left.
    ///
    /// `NotConnected` means the frame never reached the queue, so the request
    /// can go again. `Disconnected` or an eviction reason means it did, and no
    /// reply came.
    pub(crate) async fn send(
        &self,
        frame: Frame,
        deadline: Instant,
        sent: &mut bool,
    ) -> Result<Reply, IggyError> {
        let request = frame.header.request;
        let unbound = frame.header.session == 0;
        let _unbound_turn = if unbound {
            let turn = timeout_at(deadline, self.shared.unbound.lock()).await;
            Some(turn.map_err(|_| IggyError::NotConnected)?)
        } else {
            None
        };
        let _permit = timeout_at(deadline, self.shared.permits.acquire())
            .await
            .map_err(|_| IggyError::NotConnected)?
            .map_err(|_| IggyError::NotConnected)?;
        let (waiter, reply) = oneshot::channel();
        let mut entry = self.shared.wait_for(request, waiter)?;
        if !matches!(
            timeout_at(deadline, self.queue.send(frame)).await,
            Ok(Ok(()))
        ) {
            return Err(IggyError::NotConnected);
        }
        *sent = true;
        entry.close_if_abandoned = unbound;
        match timeout_at(deadline, reply).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(IggyError::Disconnected),
            Err(_) => {
                error!(
                    "No reply to request {request} from {} before its deadline.",
                    self.shared.peer
                );
                Err(IggyError::Disconnected)
            }
        }
    }

    /// Fails every waiting request. The writer task then ends the connection
    /// the way its protocol expects.
    pub(crate) fn close(&self) {
        self.shared.close(&IggyError::Disconnected);
    }

    /// Returns when the link closes, for any reason.
    pub(crate) async fn closed(&self) {
        let mut closed = self.shared.closed.subscribe();
        // The sender lives as long as the link.
        let _ = closed.wait_for(|closed| *closed).await;
    }

    /// Ends the connection the way its protocol expects: a FIN for TCP, a
    /// Close message for WebSocket. The link counts as closed first, so the
    /// reader does not report the end of the connection as an error.
    pub(crate) async fn shutdown(self) {
        let Self {
            shared,
            queue,
            mut tasks,
        } = self;
        shared.close(&IggyError::Disconnected);
        drop(queue);
        let _ = timeout(SHUTDOWN_TIMEOUT, &mut tasks.writer).await;
    }

    fn start<Writer, Reader>(
        peer: SocketAddr,
        writer: impl FnOnce(mpsc::Receiver<Frame>, Arc<MuxShared>) -> Writer,
        reader: impl FnOnce(Arc<MuxShared>) -> Reader,
    ) -> Self
    where
        Writer: Future<Output = ()> + Send + 'static,
        Reader: Future<Output = ()> + Send + 'static,
    {
        let shared = Arc::new(MuxShared {
            peer,
            no_session_eviction: AtomicBool::new(false),
            pending: StdMutex::new(Pending {
                open: true,
                waiters: HashMap::new(),
            }),
            permits: Semaphore::new(MAX_PENDING_PER_LINK),
            unbound: Mutex::new(()),
            closed: watch::Sender::new(false),
        });
        let (queue, frames) = mpsc::channel(LINK_WRITE_QUEUE);
        let tasks = LinkTasks {
            writer: tokio::spawn(writer(frames, Arc::clone(&shared))),
            reader: tokio::spawn(reader(Arc::clone(&shared))),
        };
        Self {
            shared,
            queue,
            tasks,
        }
    }
}

impl MuxShared {
    fn wait_for(&self, request: u64, waiter: Waiter) -> Result<PendingEntry<'_>, IggyError> {
        let mut pending = self.lock_pending();
        if !pending.open {
            return Err(IggyError::NotConnected);
        }
        match pending.waiters.entry(request) {
            // Two requests with one id could not tell their replies apart.
            Entry::Occupied(_) => {
                error!(
                    "Request {request} already waits for a reply from {}.",
                    self.peer
                );
                Err(IggyError::NotConnected)
            }
            Entry::Vacant(entry) => {
                entry.insert(waiter);
                Ok(PendingEntry {
                    shared: self,
                    request,
                    close_if_abandoned: false,
                })
            }
        }
    }

    /// Dispatches each whole frame of `message` as a slice of it. Only a frame
    /// that spans messages is copied, into `buffer`.
    fn deliver_message(&self, buffer: &mut BytesMut, mut message: Bytes) -> Result<(), IggyError> {
        if !buffer.is_empty() {
            buffer.extend_from_slice(&message);
            return self.deliver_buffered(buffer);
        }
        while let Some(&header) = message.first_chunk::<HEADER_SIZE>() {
            let size = self.frame_size(&header)?;
            if message.len() < size {
                break;
            }
            let body = message.split_to(size).slice(HEADER_SIZE..);
            self.dispatch(Reply { header, body })?;
        }
        buffer.extend_from_slice(&message);
        Ok(())
    }

    /// Cuts every whole frame off the front of `buffer` and dispatches it.
    fn deliver_buffered(&self, buffer: &mut BytesMut) -> Result<(), IggyError> {
        while let Some(&header) = buffer.first_chunk::<HEADER_SIZE>() {
            let size = self.frame_size(&header)?;
            if buffer.len() < size {
                break;
            }
            let body = buffer.split_to(size).freeze().slice(HEADER_SIZE..);
            self.dispatch(Reply { header, body })?;
        }
        Ok(())
    }

    fn frame_size(&self, header: &[u8; HEADER_SIZE]) -> Result<usize, IggyError> {
        response_size(header).map_err(|_| {
            error!(
                "Received a frame shorter than its header from {}.",
                self.peer
            );
            IggyError::Disconnected
        })
    }

    /// Gives a reply to the request that waits for it. An error closes the link,
    /// so an eviction interrupts every request sharing that connection.
    fn dispatch(&self, reply: Reply) -> Result<(), IggyError> {
        match peek_command(&reply.header) {
            Command::Reply => {
                let request = reply_request_id(&reply.header);
                let waiter = self.lock_pending().waiters.remove(&request);
                match waiter {
                    Some(waiter) => {
                        let _ = waiter.send(Ok(reply));
                    }
                    None => trace!(
                        "Dropped a reply to request {request} from {}, which nothing waits for.",
                        self.peer
                    ),
                }
                Ok(())
            }
            Command::Eviction => {
                self.no_session_eviction.store(
                    eviction_reason(&reply.header) == Some(EvictionReason::NoSession),
                    Ordering::Release,
                );
                Err(decode_eviction(&reply.header))
            }
            _ => {
                error!(
                    "Received a frame that is neither a reply nor an eviction from {}.",
                    self.peer
                );
                Err(IggyError::Disconnected)
            }
        }
    }

    /// Fails every waiting request with `reason`. A request that comes later
    /// fails with `NotConnected`.
    fn close(&self, reason: &IggyError) {
        let waiters = {
            let mut pending = self.lock_pending();
            pending.open = false;
            std::mem::take(&mut pending.waiters)
        };
        self.permits.close();
        self.closed.send_replace(true);
        for waiter in waiters.into_values() {
            let _ = waiter.send(Err(reason.clone()));
        }
    }

    /// The next frame to write, or `None` once the link closed or every
    /// sender went away.
    async fn next_frame(&self, frames: &mut mpsc::Receiver<Frame>) -> Option<Frame> {
        let mut closed = self.closed.subscribe();
        tokio::select! {
            biased;
            _ = closed.wait_for(|closed| *closed) => None,
            frame = frames.recv() => frame,
        }
    }

    fn is_open(&self) -> bool {
        self.lock_pending().open
    }

    fn lock_pending(&self) -> MutexGuard<'_, Pending> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for PendingEntry<'_> {
    fn drop(&mut self) {
        let abandoned = self
            .shared
            .lock_pending()
            .waiters
            .remove(&self.request)
            .is_some();
        if abandoned && self.close_if_abandoned {
            debug!(
                "Closing the link to {}: request {} left before its reply.",
                self.shared.peer, self.request
            );
            self.shared.close(&IggyError::Disconnected);
        }
    }
}

async fn write_tcp<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut frames: mpsc::Receiver<Frame>,
    shared: Arc<MuxShared>,
) {
    while let Some(frame) = shared.next_frame(&mut frames).await {
        let written = async {
            // One write for both: a header sent alone makes Nagle hold the
            // body until the peer acknowledges the header, which it delays.
            let mut bytes = Buf::chain(bytemuck::bytes_of(&frame.header), &frame.body[..]);
            writer.write_all_buf(&mut bytes).await?;
            // A frame queued behind this one goes out with the same flush.
            if frames.is_empty() {
                writer.flush().await?;
            }
            Ok::<_, io::Error>(())
        }
        .await;
        if let Err(error) = written {
            error!("Failed to send a request to {}: {error}", shared.peer);
            shared.close(&IggyError::Disconnected);
            return;
        }
    }
    let _ = writer.shutdown().await;
}

async fn read_tcp<R: AsyncRead + Unpin>(mut reader: R, shared: Arc<MuxShared>) {
    let reason = loop {
        match read_tcp_reply(&mut reader).await {
            Ok(reply) => {
                if let Err(reason) = shared.dispatch(reply) {
                    break reason;
                }
            }
            Err(error) => {
                if shared.is_open() {
                    error!("Failed to read a reply from {}: {error}", shared.peer);
                }
                break IggyError::Disconnected;
            }
        }
    };
    shared.close(&reason);
}

async fn read_tcp_reply<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Reply> {
    let mut header = [0; HEADER_SIZE];
    reader.read_exact(&mut header).await?;
    let size = response_size(&header)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "frame shorter than its header"))?;
    let body_len = size - HEADER_SIZE;
    let mut body = BytesMut::with_capacity(body_len);
    // The limit stops a read at the end of the frame, even when the buffer
    // got more capacity than it asked for.
    let mut frame_rest = (&mut *reader).take(body_len as u64);
    while body.len() < body_len {
        if frame_rest.read_buf(&mut body).await? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
    }
    Ok(Reply {
        header,
        body: body.freeze(),
    })
}

async fn write_websocket<S: AsyncRead + AsyncWrite + Unpin>(
    mut sink: SplitSink<WebSocketStream<S>, Message>,
    mut frames: mpsc::Receiver<Frame>,
    shared: Arc<MuxShared>,
) {
    while let Some(frame) = shared.next_frame(&mut frames).await {
        // A message takes one contiguous payload. Two fragments would make the
        // server join them with a copy of its own.
        let mut message = BytesMut::with_capacity(HEADER_SIZE + frame.body.len());
        message.put_slice(bytemuck::bytes_of(&frame.header));
        message.put_slice(&frame.body);
        let message = Message::Binary(message.freeze());
        // A frame queued behind this one goes out with the same flush.
        let written = if frames.is_empty() {
            sink.send(message).await
        } else {
            sink.feed(message).await
        };
        if let Err(error) = written {
            error!("Failed to send a request to {}: {error}", shared.peer);
            shared.close(&IggyError::Disconnected);
            return;
        }
    }
    let close = CloseFrame {
        code: CloseCode::Normal,
        reason: WEBSOCKET_CLOSE_REASON.into(),
    };
    let _ = sink.send(Message::Close(Some(close))).await;
}

async fn read_websocket<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: SplitStream<WebSocketStream<S>>,
    shared: Arc<MuxShared>,
) {
    let mut buffer = BytesMut::new();
    let reason = loop {
        match stream.next().await {
            Some(Ok(message @ (Message::Binary(_) | Message::Text(_)))) => {
                if let Err(reason) = shared.deliver_message(&mut buffer, message.into_data()) {
                    break reason;
                }
            }
            // Tungstenite answers a Ping on the next read, and this loop
            // always reads.
            Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => {}
            Some(Ok(Message::Close(_))) | None => {
                if shared.is_open() {
                    debug!("{} closed the WebSocket connection.", shared.peer);
                }
                break IggyError::Disconnected;
            }
            Some(Err(error)) => {
                if shared.is_open() {
                    error!("Failed to read a reply from {}: {error}", shared.peer);
                }
                break IggyError::Disconnected;
            }
        }
    };
    shared.close(&reason);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::links::test_support::{
        CLIENT, DUPLEX_CAPACITY, LOCALHOST, read_request_header, reply, request, request_with_body,
    };
    use iggy_binary_protocol::consensus::{EvictionHeader, EvictionReason, RequestHeader};
    use std::io::IoSlice;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{DuplexStream, duplex};
    use tokio_tungstenite::{accept_async, client_async};

    const BOUND_SESSION: u64 = 3;
    const UNBOUND_SESSION: u64 = 0;
    const TEST_BUDGET: Duration = Duration::from_secs(5);
    /// Larger than the buffers on both sides, so a reply arrives in pieces.
    const LARGE_REPLY: usize = 3 * DUPLEX_CAPACITY;
    /// Larger than the write buffer of a TCP link.
    const LARGE_REQUEST: usize = 16 * 1024;

    /// A socket with room for every write, as a vectored TCP socket has. It
    /// records the bytes of each write call.
    #[derive(Clone, Default)]
    struct RecordingSocket {
        writes: Arc<StdMutex<Vec<usize>>>,
    }

    impl RecordingSocket {
        fn writes(&self) -> Vec<usize> {
            self.writes.lock().unwrap().clone()
        }

        fn take(&self, len: usize) -> Poll<io::Result<usize>> {
            self.writes.lock().unwrap().push(len);
            Poll::Ready(Ok(len))
        }
    }

    impl AsyncWrite for RecordingSocket {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.take(buf.len())
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            self.take(bufs.iter().map(|buf| buf.len()).sum())
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    enum Peer {
        Tcp(DuplexStream),
        WebSocket(Box<WebSocketStream<DuplexStream>>),
    }

    impl Peer {
        async fn read_request(&mut self) -> (RequestHeader, Bytes) {
            let (header, body): ([u8; HEADER_SIZE], Bytes) = match self {
                Self::Tcp(stream) => {
                    let mut header = [0; HEADER_SIZE];
                    stream.read_exact(&mut header).await.unwrap();
                    let size = read_request_header(&header).size as usize;
                    let mut body = vec![0; size - HEADER_SIZE];
                    stream.read_exact(&mut body).await.unwrap();
                    (header, body.into())
                }
                Self::WebSocket(stream) => {
                    let message = stream.next().await.unwrap().unwrap().into_data();
                    let header = *message.first_chunk::<HEADER_SIZE>().unwrap();
                    (header, message.slice(HEADER_SIZE..))
                }
            };
            (read_request_header(&header), body)
        }

        async fn write(&mut self, frame: Vec<u8>) {
            match self {
                Self::Tcp(stream) => stream.write_all(&frame).await.unwrap(),
                Self::WebSocket(stream) => {
                    stream.send(Message::Binary(frame.into())).await.unwrap()
                }
            }
        }
    }

    fn tcp_link() -> (MuxLink, Peer) {
        let (client, server) = duplex(DUPLEX_CAPACITY);
        let (reader, writer) = tokio::io::split(client);
        (MuxLink::tcp(reader, writer, LOCALHOST), Peer::Tcp(server))
    }

    async fn websocket_link() -> (MuxLink, WebSocketStream<DuplexStream>) {
        let (client, server) = duplex(DUPLEX_CAPACITY);
        let (client, server) = tokio::join!(
            client_async("ws://localhost/", client),
            accept_async(server)
        );
        (
            MuxLink::websocket(client.unwrap().0, LOCALHOST),
            server.unwrap(),
        )
    }

    fn reply_body(request: u64) -> Vec<u8> {
        vec![u8::try_from(request).unwrap(); LARGE_REPLY]
    }

    async fn send(link: &MuxLink, frame: Frame, deadline: Instant) -> Result<Reply, IggyError> {
        link.send(frame, deadline, &mut false).await
    }

    #[tokio::test]
    async fn given_two_requests_in_flight_when_replies_arrive_reversed_should_deliver_each_to_its_caller()
     {
        let (websocket, server) = websocket_link().await;
        for (link, mut peer) in [tcp_link(), (websocket, Peer::WebSocket(Box::new(server)))] {
            let deadline = Instant::now() + TEST_BUDGET;
            let answer = async {
                let (first, first_body) = peer.read_request().await;
                let (second, second_body) = peer.read_request().await;
                for (header, body) in [(second, second_body), (first, first_body)] {
                    assert_eq!(body[..], header.request.to_le_bytes());
                    peer.write(reply(header.request, &reply_body(header.request)))
                        .await;
                }
            };
            let (first, second, ()) = tokio::join!(
                send(&link, request(1, BOUND_SESSION), deadline),
                send(&link, request(2, BOUND_SESSION), deadline),
                answer
            );
            assert_eq!(first.unwrap().body[..], reply_body(1));
            assert_eq!(second.unwrap().body[..], reply_body(2));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_frame_larger_than_the_write_buffer_when_sent_should_reach_the_socket_in_one_write()
     {
        let (reader, _peer) = duplex(DUPLEX_CAPACITY);
        let socket = RecordingSocket::default();
        let link = MuxLink::tcp(reader, socket.clone(), LOCALHOST);
        let frame = request_with_body(1, BOUND_SESSION, Bytes::from(vec![0; LARGE_REQUEST]));

        // The peer never answers, so the request ends at its deadline.
        let unanswered = send(&link, frame, Instant::now() + TEST_BUDGET).await;

        assert_eq!(unanswered.unwrap_err(), IggyError::Disconnected);
        assert_eq!(socket.writes(), [HEADER_SIZE + LARGE_REQUEST]);
    }

    #[tokio::test]
    async fn given_a_cancelled_caller_when_its_late_reply_arrives_should_not_complete_the_next_request()
     {
        let (link, mut peer) = tcp_link();
        let deadline = Instant::now() + TEST_BUDGET;
        {
            let cancelled = send(&link, request(1, BOUND_SESSION), deadline);
            tokio::pin!(cancelled);
            tokio::select! {
                result = &mut cancelled => panic!("no reply was sent: {result:?}"),
                (header, _) = peer.read_request() => assert_eq!(header.request, 1),
            }
        }
        assert!(link.shared.lock_pending().waiters.is_empty());

        let answer = async {
            let (header, _) = peer.read_request().await;
            peer.write(reply(1, b"late")).await;
            peer.write(reply(header.request, b"next")).await;
        };
        let (next, ()) = tokio::join!(send(&link, request(2, BOUND_SESSION), deadline), answer);
        assert_eq!(next.unwrap().body[..], *b"next");
    }

    #[tokio::test]
    async fn given_a_lost_link_when_requests_are_pending_should_fail_all_of_them_at_once() {
        let (link, mut peer) = tcp_link();
        let deadline = Instant::now() + TEST_BUDGET;
        let lose = async move {
            peer.read_request().await;
            peer.read_request().await;
        };
        let (first, second, ()) = tokio::join!(
            send(&link, request(1, BOUND_SESSION), deadline),
            send(&link, request(2, BOUND_SESSION), deadline),
            lose
        );

        assert_eq!(first.unwrap_err(), IggyError::Disconnected);
        assert_eq!(second.unwrap_err(), IggyError::Disconnected);
        assert!(Instant::now() < deadline);
        let after = send(&link, request(3, BOUND_SESSION), deadline).await;
        assert_eq!(after.unwrap_err(), IggyError::NotConnected);
    }

    #[tokio::test]
    async fn given_an_eviction_when_requests_are_pending_should_fail_all_of_them_and_close_the_link()
     {
        let (link, mut peer) = tcp_link();
        let deadline = Instant::now() + TEST_BUDGET;
        let evict = async {
            peer.read_request().await;
            peer.read_request().await;
            let eviction = EvictionHeader::new(0, 0, 0, CLIENT, EvictionReason::StaleClient);
            peer.write(bytemuck::bytes_of(&eviction).to_vec()).await;
        };
        let (first, second, ()) = tokio::join!(
            send(&link, request(1, BOUND_SESSION), deadline),
            send(&link, request(2, BOUND_SESSION), deadline),
            evict
        );

        assert_eq!(first.unwrap_err(), IggyError::StaleClient);
        assert_eq!(second.unwrap_err(), IggyError::StaleClient);
        let after = send(&link, request(3, BOUND_SESSION), deadline).await;
        assert_eq!(after.unwrap_err(), IggyError::NotConnected);
    }

    #[tokio::test(start_paused = true)]
    async fn given_full_pending_entries_when_sending_should_wait_inside_the_deadline() {
        const WAIT: Duration = Duration::from_millis(100);
        let (link, mut peer) = tcp_link();
        let far = Instant::now() + Duration::from_secs(3600);
        let held = futures::future::join_all(
            (1..=MAX_PENDING_PER_LINK as u64)
                .map(|id| send(&link, request(id, BOUND_SESSION), far)),
        );
        tokio::pin!(held);
        let read_all = async {
            for _ in 0..MAX_PENDING_PER_LINK {
                peer.read_request().await;
            }
        };
        tokio::select! {
            results = &mut held => panic!("no reply was sent: {results:?}"),
            () = read_all => {}
        }

        let started = Instant::now();
        let overflow = tokio::select! {
            results = &mut held => panic!("no reply was sent: {results:?}"),
            result = send(&link, request(MAX_PENDING_PER_LINK as u64 + 1, BOUND_SESSION), started + WAIT) => result,
        };

        assert_eq!(overflow.unwrap_err(), IggyError::NotConnected);
        assert_eq!(started.elapsed(), WAIT);
        assert!(timeout(WAIT, peer.read_request()).await.is_err());
    }

    #[tokio::test]
    async fn given_an_unbound_link_when_a_cancelled_caller_already_sent_should_close_the_link() {
        let (link, mut peer) = tcp_link();
        let deadline = Instant::now() + TEST_BUDGET;
        {
            let cancelled = send(&link, request(1, UNBOUND_SESSION), deadline);
            tokio::pin!(cancelled);
            tokio::select! {
                result = &mut cancelled => panic!("no reply was sent: {result:?}"),
                _ = peer.read_request() => {}
            }
        }

        let after = send(&link, request(1, UNBOUND_SESSION), deadline).await;
        assert_eq!(after.unwrap_err(), IggyError::NotConnected);
    }

    #[tokio::test]
    async fn given_a_pending_request_id_when_sent_again_should_refuse_the_second_send() {
        let (link, mut peer) = tcp_link();
        let deadline = Instant::now() + TEST_BUDGET;
        let first = send(&link, request(1, BOUND_SESSION), deadline);
        tokio::pin!(first);
        tokio::select! {
            result = &mut first => panic!("no reply was sent: {result:?}"),
            _ = peer.read_request() => {}
        }

        let duplicate = send(&link, request(1, BOUND_SESSION), deadline).await;
        assert_eq!(duplicate.unwrap_err(), IggyError::NotConnected);
        peer.write(reply(1, b"first")).await;
        assert_eq!(first.await.unwrap().body[..], *b"first");
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_sender_waiting_for_a_permit_when_the_link_closes_should_fail_at_once() {
        let (link, peer) = tcp_link();
        let far = Instant::now() + Duration::from_secs(3600);
        let all_permits = u32::try_from(MAX_PENDING_PER_LINK).unwrap();
        let _held = link.shared.permits.acquire_many(all_permits).await.unwrap();
        let started = Instant::now();
        let waiting = send(&link, request(1, BOUND_SESSION), far);
        tokio::pin!(waiting);
        tokio::select! {
            result = &mut waiting => panic!("no permit was free: {result:?}"),
            () = tokio::task::yield_now() => {}
        }

        drop(peer);

        assert_eq!(waiting.await.unwrap_err(), IggyError::NotConnected);
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test]
    async fn given_websocket_messages_that_split_and_join_frames_should_deliver_each_reply() {
        const HEADER_CUT: usize = HEADER_SIZE / 2;
        const BODY_CUT: usize = HEADER_SIZE + 2;
        let (link, server) = websocket_link().await;
        let mut peer = Peer::WebSocket(Box::new(server));
        let deadline = Instant::now() + TEST_BUDGET;
        let answer = async {
            let (first, _) = peer.read_request().await;
            let (second, _) = peer.read_request().await;
            let mut joined = reply(second.request, &second.request.to_le_bytes());
            let split = reply(first.request, &first.request.to_le_bytes());
            joined.extend_from_slice(&split[..HEADER_CUT]);
            peer.write(joined).await;
            peer.write(split[HEADER_CUT..BODY_CUT].to_vec()).await;
            peer.write(split[BODY_CUT..].to_vec()).await;
        };
        let (first, second, ()) = tokio::join!(
            send(&link, request(1, BOUND_SESSION), deadline),
            send(&link, request(2, BOUND_SESSION), deadline),
            answer
        );
        assert_eq!(first.unwrap().body[..], 1u64.to_le_bytes());
        assert_eq!(second.unwrap().body[..], 2u64.to_le_bytes());
    }

    #[tokio::test]
    async fn given_whole_replies_in_one_websocket_message_should_hand_over_slices_of_it() {
        let (link, _server) = websocket_link().await;
        let mut message = reply(1, &1u64.to_le_bytes());
        message.extend_from_slice(&reply(2, &2u64.to_le_bytes()));
        let message = Bytes::from(message);
        let (first_waiter, first) = oneshot::channel();
        let (second_waiter, second) = oneshot::channel();
        let _first_entry = link.shared.wait_for(1, first_waiter).unwrap();
        let _second_entry = link.shared.wait_for(2, second_waiter).unwrap();
        let mut buffer = BytesMut::new();

        link.shared
            .deliver_message(&mut buffer, message.clone())
            .unwrap();

        let first = first.await.unwrap().unwrap();
        let second = second.await.unwrap().unwrap();
        let second_frame = message.len() / 2;
        assert_eq!(first.body.as_ptr(), message[HEADER_SIZE..].as_ptr());
        assert_eq!(
            second.body.as_ptr(),
            message[second_frame + HEADER_SIZE..].as_ptr()
        );
        assert!(buffer.is_empty());
    }

    #[tokio::test]
    async fn given_a_websocket_link_when_shut_down_should_send_a_normal_close() {
        let (link, mut server) = websocket_link().await;

        link.shutdown().await;

        let message = timeout(TEST_BUDGET, server.next()).await.unwrap();
        assert!(
            matches!(
                message,
                Some(Ok(Message::Close(Some(CloseFrame {
                    code: CloseCode::Normal,
                    ..
                }))))
            ),
            "{message:?}"
        );
    }

    #[tokio::test]
    async fn given_a_websocket_ping_when_the_reader_runs_should_answer_with_a_pong() {
        const PING: &[u8] = b"keepalive";
        let (_link, mut server) = websocket_link().await;

        server
            .send(Message::Ping(Bytes::from_static(PING)))
            .await
            .unwrap();
        let answer = timeout(TEST_BUDGET, server.next()).await.unwrap();

        assert_eq!(
            answer.unwrap().unwrap(),
            Message::Pong(Bytes::from_static(PING))
        );
    }
}
