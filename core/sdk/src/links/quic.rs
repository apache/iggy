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

//! One QUIC connection that carries each request on its own stream.
//!
//! A stream carries one request and its reply. So a caller that goes away
//! leaves nothing half read on the connection. The echoed request id must
//! still match before its result can settle that request.

use crate::links::{Frame, LateReply, Reply};
use crate::vsr::{decode_eviction, eviction_reason, peek_command, reply_request_id};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use iggy_binary_protocol::consensus::{Command, EvictionReason, HEADER_SIZE, RequestHeader};
use iggy_common::IggyError;
use quinn::{Connection, ReadToEndError, VarInt, WriteError};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::time::{Instant, timeout_at};
use tracing::{debug, error};

/// The application error code of a connection that the client closes.
const CLIENT_CLOSE_CODE: VarInt = VarInt::from_u32(0);

/// A QUIC connection. Dropping the link closes the connection.
#[derive(Debug)]
pub(crate) struct QuicLink {
    connection: Connection,
    no_session_eviction: AtomicBool,
    /// A longer reply fails its request.
    response_buffer_size: usize,
}

impl QuicLink {
    pub(crate) fn no_session_eviction(&self) -> bool {
        self.no_session_eviction.load(Ordering::Acquire)
    }

    pub(crate) fn new(connection: Connection, response_buffer_size: usize) -> Self {
        Self {
            connection,
            no_session_eviction: AtomicBool::new(false),
            response_buffer_size,
        }
    }

    /// Sends `frame` on a new stream and waits for its reply until `deadline`.
    /// `sent` turns true when the stream opens, before the first byte goes
    /// out.
    ///
    /// `NotConnected` means no stream opened, so nothing was sent.
    /// `Disconnected` or an eviction reason means the request may have
    /// reached the server, and no reply came.
    pub(crate) async fn send(
        &self,
        frame: Frame,
        deadline: Instant,
        sent: &mut bool,
        late: Option<LateReply>,
    ) -> Result<Reply, IggyError> {
        let request = frame.header.request;
        let cancellation = frame.header.deferred_cancellation();
        let (mut send, mut recv) = match timeout_at(deadline, self.connection.open_bi()).await {
            Ok(Ok(streams)) => streams,
            Ok(Err(error)) => {
                error!(
                    "Failed to open a stream to {} for request {request}: {error}",
                    self.connection.remote_address()
                );
                return Err(IggyError::NotConnected);
            }
            Err(_) => {
                error!(
                    "No stream to {} opened for request {request} before its deadline.",
                    self.connection.remote_address()
                );
                return Err(IggyError::NotConnected);
            }
        };
        *sent = true;
        let written = timeout_at(deadline, async {
            send.write_all(bytemuck::bytes_of(&frame.header)).await?;
            send.write_all(&frame.body).await?;
            send.finish()?;
            Ok::<_, WriteError>(())
        })
        .await;
        match written {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                error!(
                    "Failed to send request {request} to {}: {error}",
                    self.connection.remote_address()
                );
                return Err(IggyError::Disconnected);
            }
            Err(_) => {
                error!(
                    "Request {request} did not reach {} before its deadline.",
                    self.connection.remote_address()
                );
                return Err(IggyError::Disconnected);
            }
        }
        let response_limit =
            frame
                .response_body_limit()
                .map_or(self.response_buffer_size, |limit| {
                    limit
                        .saturating_add(iggy_binary_protocol::HEADER_SIZE)
                        .min(self.response_buffer_size)
                });
        // A stopped stream cancels the request and loses its reply, so a committing request
        // whose caller leaves cancels on a new stream and keeps reading this one.
        let mut reading = ReplyStream {
            connection: self.connection.clone(),
            reply: Some(Box::pin(
                async move { recv.read_to_end(response_limit).await },
            )),
            cancelled: cancellation.zip(late),
        };
        let Some(reply) = reading.reply.as_mut() else {
            return Err(IggyError::Disconnected);
        };
        let read = timeout_at(deadline, reply).await;
        reading.cancelled = None;
        let reply = match read {
            Ok(Ok(reply)) => reply,
            Ok(Err(error)) => {
                error!(
                    "Failed to read the reply to request {request} from {}: {error}",
                    self.connection.remote_address()
                );
                return Err(IggyError::Disconnected);
            }
            Err(_) => {
                error!(
                    "No reply to request {request} from {} before its deadline.",
                    self.connection.remote_address()
                );
                return Err(IggyError::Disconnected);
            }
        };
        let Some(&header) = reply.first_chunk::<HEADER_SIZE>() else {
            return Err(IggyError::EmptyResponse);
        };
        // An eviction detaches the connection, including its other streams.
        if peek_command(&header) == Command::Eviction {
            self.no_session_eviction.store(
                eviction_reason(&header) == Some(EvictionReason::NoSession),
                Ordering::Release,
            );
            self.close();
            return Err(decode_eviction(&header));
        }
        if peek_command(&header) != Command::Reply || reply_request_id(&header) != request {
            self.close();
            return Err(IggyError::Disconnected);
        }
        Ok(Reply {
            header,
            body: Bytes::from(reply).slice(HEADER_SIZE..),
        })
    }

    /// Fails every request on the connection, and the peer learns at once
    /// that the client went away.
    pub(crate) fn close(&self) {
        self.connection.close(CLIENT_CLOSE_CODE, b"");
    }

    /// Returns when the connection closes, for any reason.
    pub(crate) async fn closed(&self) {
        self.connection.closed().await;
    }
}

/// The reply stream of a request. Dropped while `cancelled` holds the cancel and the settle
/// callback of a committing request, it sends the cancel on a new stream and hands the one reply
/// of this stream to the callback.
struct ReplyStream {
    connection: Connection,
    reply: Option<BoxFuture<'static, Result<Vec<u8>, ReadToEndError>>>,
    cancelled: Option<(RequestHeader, LateReply)>,
}

impl Drop for ReplyStream {
    fn drop(&mut self) {
        let (Some((cancel, late)), Some(reply)) = (self.cancelled.take(), self.reply.take()) else {
            return;
        };
        let connection = self.connection.clone();
        tokio::spawn(async move {
            if let Err(error) = send_cancel(&connection, &cancel).await {
                debug!(
                    "Failed to cancel request {} on {}: {error}",
                    cancel.request,
                    connection.remote_address()
                );
            }
            let reply = match reply.await {
                Ok(reply) => late_reply(cancel.request, reply),
                Err(_) => Err(IggyError::Disconnected),
            };
            late(reply);
        });
    }
}

/// Sends the cancel of a deferred request on a stream of its own, which the server finishes
/// without an answer.
async fn send_cancel(connection: &Connection, cancel: &RequestHeader) -> Result<(), WriteError> {
    let (mut send, _no_answer) = connection
        .open_bi()
        .await
        .map_err(WriteError::ConnectionLost)?;
    send.write_all(bytemuck::bytes_of(cancel)).await?;
    send.finish().map_err(|_| WriteError::ClosedStream)
}

/// The reply to request `request` in `bytes`, read after its caller left.
fn late_reply(request: u64, bytes: Vec<u8>) -> Result<Reply, IggyError> {
    let Some(&header) = bytes.first_chunk::<HEADER_SIZE>() else {
        return Err(IggyError::EmptyResponse);
    };
    if peek_command(&header) == Command::Eviction {
        return Err(decode_eviction(&header));
    }
    if peek_command(&header) != Command::Reply || reply_request_id(&header) != request {
        return Err(IggyError::Disconnected);
    }
    Ok(Reply {
        header,
        body: Bytes::from(bytes).slice(HEADER_SIZE..),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::links::test_support::{
        CLIENT, LOCALHOST, SERVER_NAME, quic_server, read_request_header, reply, request,
        request_with_body,
    };
    use crate::links::{LateReply, Link};
    use iggy_binary_protocol::codes::{CANCEL_DEFERRED_REQUEST_CODE, POLL_MESSAGES_DEFERRED_CODE};
    use iggy_binary_protocol::consensus::{EvictionHeader, EvictionReason, RequestHeader};
    use quinn::{ClientConfig, Endpoint, SendStream};
    use rustls::RootCertStore;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    const BOUND_SESSION: u64 = 3;
    const RESPONSE_BUFFER_SIZE: usize = 64 * 1024;
    const TEST_BUDGET: Duration = Duration::from_secs(5);
    const LOST_REPLY_BUDGET: Duration = Duration::from_millis(100);
    /// Larger than the stream window of the peer, so the client still writes
    /// when the peer stops reading.
    const LARGE_REQUEST: usize = 4 * 1024 * 1024;
    const STOP_CODE: VarInt = VarInt::from_u32(1);

    /// The server side of a link. It keeps both endpoints alive.
    struct Peer {
        connection: Connection,
        _server: Endpoint,
        _client: Endpoint,
    }

    impl Peer {
        async fn read_request(&self) -> (SendStream, RequestHeader) {
            let (stream, mut request) = self.connection.accept_bi().await.unwrap();
            let request = request.read_to_end(RESPONSE_BUFFER_SIZE).await.unwrap();
            let header = read_request_header(request.first_chunk::<HEADER_SIZE>().unwrap());
            (stream, header)
        }
    }

    async fn answer(mut stream: SendStream, frame: &[u8]) {
        stream.write_all(frame).await.unwrap();
        stream.finish().unwrap();
    }

    async fn send(link: &QuicLink, frame: Frame, deadline: Instant) -> Result<Reply, IggyError> {
        link.send(frame, deadline, &mut false, None).await
    }

    async fn quic_link() -> (QuicLink, Peer) {
        let (server, certificate) = quic_server();
        let mut roots = RootCertStore::empty();
        roots.add(certificate).unwrap();
        let mut client = Endpoint::client(LOCALHOST).unwrap();
        client.set_default_client_config(
            ClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
        );
        let address = server.local_addr().unwrap();
        let (connection, accepted) = tokio::join!(
            async { client.connect(address, SERVER_NAME).unwrap().await.unwrap() },
            async { server.accept().await.unwrap().await.unwrap() }
        );
        let peer = Peer {
            connection: accepted,
            _server: server,
            _client: client,
        };
        (QuicLink::new(connection, RESPONSE_BUFFER_SIZE), peer)
    }

    #[tokio::test]
    async fn given_two_requests_on_one_link_when_replies_arrive_reversed_should_deliver_each_to_its_caller()
     {
        let (link, peer) = quic_link().await;
        let deadline = Instant::now() + TEST_BUDGET;
        let answer_reversed = async {
            let (first_stream, first) = peer.read_request().await;
            let (second_stream, second) = peer.read_request().await;
            for (stream, header) in [(second_stream, second), (first_stream, first)] {
                let body = header.request.to_le_bytes();
                answer(stream, &reply(header.request, &body)).await;
            }
        };

        let (first, second, ()) = timeout(TEST_BUDGET, async {
            tokio::join!(
                send(&link, request(1, BOUND_SESSION), deadline),
                send(&link, request(2, BOUND_SESSION), deadline),
                answer_reversed
            )
        })
        .await
        .unwrap();

        assert_eq!(first.unwrap().body[..], 1u64.to_le_bytes());
        assert_eq!(second.unwrap().body[..], 2u64.to_le_bytes());
    }

    /// The server answers a cancelled committing request once on its own stream, so the link
    /// sends the cancel on a new stream and keeps reading the stream of the request.
    #[tokio::test]
    async fn given_a_committing_deferred_request_when_its_caller_leaves_should_cancel_on_a_new_stream_and_settle_with_its_reply()
     {
        let (link, peer) = quic_link().await;
        let link = Link::Quic(link);
        let mut frame = request(1, BOUND_SESSION);
        frame.header.reserved = POLL_MESSAGES_DEFERRED_CODE.to_le_bytes();
        let (settled, late_reply) = oneshot::channel();
        let late: LateReply = Box::new(move |reply| {
            let _ = settled.send(reply);
        });
        let mut left = false;
        let stream = {
            let sent =
                link.send_settled(frame, Instant::now() + TEST_BUDGET, &mut left, Some(late));
            tokio::pin!(sent);
            tokio::select! {
                result = &mut sent => panic!("no reply was sent: {result:?}"),
                (stream, header) = peer.read_request() => {
                    assert_eq!(header.request, 1);
                    stream
                }
            }
        };

        let (_, cancel) = timeout(TEST_BUDGET, peer.read_request()).await.unwrap();
        answer(stream, &reply(1, b"stored")).await;
        let settled = timeout(TEST_BUDGET, late_reply).await.unwrap().unwrap();

        assert_eq!(cancel.reserved, CANCEL_DEFERRED_REQUEST_CODE.to_le_bytes());
        assert_eq!((cancel.request, cancel.session), (1, BOUND_SESSION));
        assert_eq!(settled.unwrap().body[..], *b"stored");
    }

    #[tokio::test]
    async fn given_a_partial_committing_reply_when_its_caller_leaves_should_keep_the_received_bytes()
     {
        let (link, peer) = quic_link().await;
        let link = Link::Quic(link);
        let mut frame = request(1, BOUND_SESSION);
        frame.header.reserved = POLL_MESSAGES_DEFERRED_CODE.to_le_bytes();
        let (settled, late_reply) = oneshot::channel();
        let late: LateReply = Box::new(move |reply| {
            let _ = settled.send(reply);
        });
        let mut left = false;
        let mut stream = {
            let sent =
                link.send_settled(frame, Instant::now() + TEST_BUDGET, &mut left, Some(late));
            tokio::pin!(sent);
            let mut stream = tokio::select! {
                result = &mut sent => panic!("no reply was sent: {result:?}"),
                (stream, _) = peer.read_request() => stream,
            };
            stream.write_all(&reply(1, b"stored")).await.unwrap();
            assert!(timeout(LOST_REPLY_BUDGET, &mut sent).await.is_err());
            stream
        };

        let (_, cancel) = timeout(TEST_BUDGET, peer.read_request()).await.unwrap();
        assert_eq!(cancel.reserved, CANCEL_DEFERRED_REQUEST_CODE.to_le_bytes());
        stream.finish().unwrap();
        let settled = timeout(TEST_BUDGET, late_reply).await.unwrap().unwrap();
        assert_eq!(settled.unwrap().body[..], *b"stored");
    }

    #[tokio::test]
    async fn given_a_closed_connection_when_sending_should_fail_before_the_send_point() {
        let (link, peer) = quic_link().await;
        peer.connection.close(CLIENT_CLOSE_CODE, b"");
        link.connection.closed().await;

        let result = send(
            &link,
            request(1, BOUND_SESSION),
            Instant::now() + TEST_BUDGET,
        )
        .await;

        assert_eq!(result.unwrap_err(), IggyError::NotConnected);
    }

    #[tokio::test]
    async fn given_a_request_without_a_reply_when_its_deadline_passes_should_keep_the_link() {
        let (link, peer) = quic_link().await;
        let hold_request = async { peer.read_request().await.0 };
        let (lost, _unanswered) = timeout(TEST_BUDGET, async {
            tokio::join!(
                send(
                    &link,
                    request(1, BOUND_SESSION),
                    Instant::now() + LOST_REPLY_BUDGET
                ),
                hold_request
            )
        })
        .await
        .unwrap();
        assert_eq!(lost.unwrap_err(), IggyError::Disconnected);

        let answer_next = async {
            let (stream, header) = peer.read_request().await;
            answer(stream, &reply(header.request, &[])).await;
        };
        let (next, ()) = tokio::join!(
            send(
                &link,
                request(2, BOUND_SESSION),
                Instant::now() + TEST_BUDGET
            ),
            answer_next
        );

        assert!(next.unwrap().body.is_empty());
    }

    #[tokio::test]
    async fn given_a_stream_stopped_while_sending_should_fail_after_the_send_point() {
        let (link, peer) = quic_link().await;
        let large = request_with_body(1, BOUND_SESSION, Bytes::from(vec![0; LARGE_REQUEST]));
        let stop = async {
            let (_, mut request) = peer.connection.accept_bi().await.unwrap();
            let mut header = [0; HEADER_SIZE];
            request.read_exact(&mut header).await.unwrap();
            request.stop(STOP_CODE).unwrap();
        };

        let (result, ()) = timeout(TEST_BUDGET, async {
            tokio::join!(send(&link, large, Instant::now() + TEST_BUDGET), stop)
        })
        .await
        .unwrap();

        assert_eq!(result.unwrap_err(), IggyError::Disconnected);
    }

    #[tokio::test]
    async fn given_an_eviction_when_sent_should_fail_and_close_the_connection() {
        let (link, peer) = quic_link().await;
        let evict = async {
            let (stream, _) = peer.read_request().await;
            let eviction = EvictionHeader::new(0, 0, 0, CLIENT, EvictionReason::StaleClient);
            answer(stream, bytemuck::bytes_of(&eviction)).await;
        };
        let deadline = Instant::now() + TEST_BUDGET;
        let (evicted, ()) = tokio::join!(send(&link, request(1, BOUND_SESSION), deadline), evict);
        assert_eq!(evicted.unwrap_err(), IggyError::StaleClient);

        let after = send(&link, request(2, BOUND_SESSION), deadline).await;

        assert_eq!(after.unwrap_err(), IggyError::NotConnected);
    }

    #[tokio::test]
    async fn given_a_reply_for_another_request_when_received_should_fail_and_close_the_connection()
    {
        let (link, peer) = quic_link().await;
        let mismatched = async {
            let (stream, request) = peer.read_request().await;
            answer(stream, &reply(request.request + 1, &[])).await;
        };
        let deadline = Instant::now() + TEST_BUDGET;

        let (result, ()) =
            tokio::join!(send(&link, request(1, BOUND_SESSION), deadline), mismatched);

        assert_eq!(result.unwrap_err(), IggyError::Disconnected);
        assert_eq!(
            send(&link, request(2, BOUND_SESSION), deadline)
                .await
                .unwrap_err(),
            IggyError::NotConnected
        );
    }
}
