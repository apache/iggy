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

//! Connection plumbing shared by the TCP, QUIC and WebSocket clients.

use crate::session_owner::SessionOwner;
use crate::vsr::{decode_reply, encode_request_header};
use bytes::Bytes;
use iggy_binary_protocol::consensus::{HEADER_SIZE, RequestHeader};
#[cfg(test)]
use iggy_common::DiagnosticEvent;
use iggy_common::IggyError;
use mux::MuxLink;
use quic::QuicLink;
use std::time::Duration;
use tokio::time::{Instant, sleep};

pub(crate) mod connect;
#[cfg(test)]
pub(crate) mod fake_cluster;
// Unused until the clients send through the driver.
#[allow(dead_code)]
mod dial;
pub(crate) mod mux;
pub(crate) mod quic;
// Unused until the clients send through the driver.
#[allow(dead_code)]
mod replicas;

// Unused until the clients send through the driver.
#[allow(unused_imports)]
pub(crate) use dial::Dialer;
// Unused until the clients send through the driver.
#[allow(unused_imports)]
pub(crate) use replicas::{ReconnectPolicy, ReplicaLinks, Target};

#[cfg(test)]
pub(crate) use crate::diagnostic_events::DIAGNOSTIC_EVENT_CAPACITY as EVENT_CHANNEL_CAPACITY;
pub(crate) use crate::diagnostic_events::diagnostic_event_channel as event_channel;

/// The longest that one request waits, from its first attempt to its last
/// reply.
pub(crate) const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// The pauses between the attempts of one request grow from the base to the
/// cap, see [`backoff`].
pub(crate) const RETRY_BACKOFF_BASE: Duration = Duration::from_millis(50);
pub(crate) const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(1);

/// A connection to one replica.
#[derive(Debug)]
pub(crate) enum Link {
    Mux(MuxLink),
    Quic(QuicLink),
}

impl Link {
    /// Sends `frame` and waits for its reply until `deadline`. `sent` turns
    /// true when the frame can reach the server, and `NotConnected` means
    /// that it cannot have. A frame never leaves at or after its deadline,
    /// because its reply could only time out.
    pub(crate) async fn send(
        &self,
        frame: Frame,
        deadline: Instant,
        sent: &mut bool,
    ) -> Result<Reply, IggyError> {
        if Instant::now() >= deadline {
            return Err(IggyError::NotConnected);
        }
        match self {
            Self::Mux(link) => link.send(frame, deadline, sent).await,
            Self::Quic(link) => link.send(frame, deadline, sent).await,
        }
    }

    /// Returns when the link closes, for any reason.
    pub(crate) async fn closed(&self) {
        match self {
            Self::Mux(link) => link.closed().await,
            Self::Quic(link) => link.closed().await,
        }
    }

    /// Fails every request that waits on the link.
    pub(crate) fn close(&self) {
        match self {
            Self::Mux(link) => link.close(),
            Self::Quic(link) => link.close(),
        }
    }

    /// Ends the connection the way its protocol expects. A link that a
    /// request still holds closes at once instead.
    pub(crate) async fn shutdown(link: std::sync::Arc<Self>) {
        match std::sync::Arc::try_unwrap(link) {
            Ok(Self::Mux(link)) => link.shutdown().await,
            Ok(Self::Quic(link)) => link.close(),
            Err(link) => link.close(),
        }
    }
}

/// Sends one request on `link` and decodes its reply, without a retry.
pub(crate) async fn exchange(
    session: &SessionOwner,
    link: &Link,
    code: u32,
    body: Bytes,
    deadline: Instant,
) -> Result<Bytes, IggyError> {
    let (header, _) = encode_request_header(&mut *session.session()?, code, &body)?;
    let reply = link
        .send(Frame { header, body }, deadline, &mut false)
        .await?;
    read_reply(session, reply)
}

/// Raises the metadata watermark from `reply` and returns its body, or the
/// refusal that it carries.
pub(crate) fn read_reply(session: &SessionOwner, reply: Reply) -> Result<Bytes, IggyError> {
    session.observe_metadata_reply(&reply.header);
    decode_reply(&reply.header, reply.body)
}

/// Whether a request failed because its link went away. Another link, or
/// the same one after a redial, can carry it again.
pub(crate) fn lost_link(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::Disconnected
            | IggyError::EmptyResponse
            | IggyError::NotConnected
            | IggyError::StaleClient
    )
}

/// The full jitter pause before attempt `attempt`: a random time in
/// `[0, min(cap, base * 2^attempt)]`. `draw` is the random fraction of that
/// range, from `[0, 1]`.
pub(crate) fn backoff(attempt: u32, base: Duration, cap: Duration, draw: f64) -> Duration {
    let ceiling = 1_u32
        .checked_shl(attempt)
        .and_then(|factor| base.checked_mul(factor))
        .map_or(cap, |pause| pause.min(cap));
    Duration::try_from_secs_f64(ceiling.as_secs_f64() * draw)
        .map_or(Duration::ZERO, |pause| pause.min(ceiling))
}

/// Sleeps the [`backoff`] pause before retry `retry` of a request. Returns
/// false instead if the retry cannot start before `deadline`. `draw` is the
/// random fraction of the pause.
pub(crate) async fn pause_before_retry(retry: u32, deadline: Instant, draw: f64) -> bool {
    let pause = backoff(retry, RETRY_BACKOFF_BASE, RETRY_BACKOFF_MAX, draw);
    if Instant::now() + pause >= deadline {
        return false;
    }
    sleep(pause).await;
    // A timer fires on a whole millisecond, so a sleep can end at the
    // deadline.
    Instant::now() < deadline
}

/// A request header and its body. TCP writes them back to back, WebSocket
/// sends them as one message, and QUIC writes them on one stream.
#[derive(Debug, Clone)]
pub(crate) struct Frame {
    pub(crate) header: RequestHeader,
    pub(crate) body: Bytes,
}

#[derive(Debug)]
pub(crate) struct Reply {
    pub(crate) header: [u8; HEADER_SIZE],
    pub(crate) body: Bytes,
}

/// Frames and a QUIC server for the tests of the links and the transports.
#[cfg(test)]
pub(crate) mod test_support {
    use super::Frame;
    use bytes::Bytes;
    use iggy_binary_protocol::codes::PING_CODE;
    use iggy_binary_protocol::consensus::{
        Command, HEADER_SIZE, Operation, RESULT_COUNT_LEN, ReplyHeader, RequestHeader,
    };
    use iggy_binary_protocol::responses::users::LoginRegisterResponse;
    use iggy_binary_protocol::{IGGY_PROTOCOL_VERSION, WireEncode, WireName};
    use iggy_common::{Client, ClientState, Credentials, IggyError, VsrSessionControl};
    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use tokio::task::JoinHandle;

    pub(crate) const CLIENT: u128 = 7;
    /// Enough turns of a current-thread test runtime for a spawned
    /// `disconnect()` to reach the link that an exchange in flight holds.
    pub(crate) const DISCONNECT_YIELDS: usize = 32;
    pub(crate) const SIGNED_IN_USER_ID: u32 = 7;
    pub(crate) const LOCALHOST: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    /// The buffer of each direction of an in-memory stream.
    pub(crate) const DUPLEX_CAPACITY: usize = 64 * 1024;
    pub(crate) const SERVER_NAME: &str = "localhost";

    /// A request whose body names its id, so a peer can tell a torn frame.
    pub(crate) fn request(id: u64, session: u64) -> Frame {
        request_with_body(id, session, Bytes::copy_from_slice(&id.to_le_bytes()))
    }

    pub(crate) fn request_with_body(id: u64, session: u64, body: Bytes) -> Frame {
        let header = RequestHeader::for_request(PING_CODE, CLIENT, id, session, &body).unwrap();
        Frame { header, body }
    }

    pub(crate) fn reply(request: u64, body: &[u8]) -> Vec<u8> {
        let header = ReplyHeader {
            command: Command::Reply,
            operation: Operation::NonReplicated,
            client: CLIENT,
            request,
            size: u32::try_from(HEADER_SIZE + body.len()).unwrap(),
            ..Default::default()
        };
        let mut frame = bytemuck::bytes_of(&header).to_vec();
        frame.extend_from_slice(body);
        frame
    }

    /// A reply that refuses the request with the status `error`.
    pub(crate) fn refusal(request: u64, error: &IggyError) -> Vec<u8> {
        let header = ReplyHeader {
            command: Command::Reply,
            operation: Operation::NonReplicated,
            client: CLIENT,
            request,
            size: u32::try_from(HEADER_SIZE).unwrap(),
            status: error.as_code(),
            ..Default::default()
        };
        bytemuck::bytes_of(&header).to_vec()
    }

    pub(crate) fn read_request_header(header: &[u8; HEADER_SIZE]) -> RequestHeader {
        bytemuck::checked::try_pod_read_unaligned(header).unwrap()
    }

    /// The reply to a login or to a session bind, for the user
    /// [`SIGNED_IN_USER_ID`] and the session `session`.
    pub(crate) fn sign_in_reply(session: u64) -> Bytes {
        LoginRegisterResponse {
            user_id: SIGNED_IN_USER_ID,
            session,
            server_protocol_version: IGGY_PROTOCOL_VERSION,
            server_version: WireName::new("test").unwrap(),
        }
        .to_bytes()
    }

    /// The reply to a Register: a result section with no rejection, then
    /// [`sign_in_reply`].
    pub(crate) fn register_reply(session: u64) -> Vec<u8> {
        let mut body = vec![0; RESULT_COUNT_LEN];
        body.extend_from_slice(&sign_in_reply(session));
        body
    }

    /// Disconnects `client`, then takes the steps that a sign-in takes after
    /// its reply binds the session, and returns the state they leave.
    pub(crate) async fn sign_in_steps_after_a_disconnect<C>(client: &C) -> ClientState
    where
        C: Client + VsrSessionControl,
    {
        Client::disconnect(client).await.unwrap();
        client.set_state(ClientState::Authenticated).await;
        client
            .remember_session_credentials(
                Credentials::UsernamePassword("iggy".to_string(), "iggy".into()),
                SIGNED_IN_USER_ID,
            )
            .await;
        client.get_state().await
    }

    /// Starts an explicit `disconnect()` of `client`, and returns once the
    /// disconnect has raised the caller flag and waits for the link that an
    /// exchange in flight holds. So the reply to that exchange arrives after
    /// the caller disconnected.
    pub(crate) async fn spawn_disconnect_behind_the_link<C: Client + 'static>(
        client: &Arc<C>,
        caller_disconnected: impl Fn(&C) -> bool,
    ) -> JoinHandle<Result<(), IggyError>> {
        let disconnect = tokio::spawn({
            let client = Arc::clone(client);
            async move { Client::disconnect(&*client).await }
        });
        wait_behind_the_link(client, caller_disconnected, &disconnect).await;
        disconnect
    }

    /// [`spawn_disconnect_behind_the_link`], with a `connect()` right after
    /// the disconnect returns, as a caller restarts a client.
    pub(crate) async fn spawn_restart_behind_the_link<C: Client + 'static>(
        client: &Arc<C>,
        caller_disconnected: impl Fn(&C) -> bool,
    ) -> JoinHandle<Result<(), IggyError>> {
        let restart = tokio::spawn({
            let client = Arc::clone(client);
            async move {
                Client::disconnect(&*client).await?;
                Client::connect(&*client).await
            }
        });
        wait_behind_the_link(client, caller_disconnected, &restart).await;
        restart
    }

    async fn wait_behind_the_link<C>(
        client: &Arc<C>,
        caller_disconnected: impl Fn(&C) -> bool,
        disconnecting: &JoinHandle<Result<(), IggyError>>,
    ) {
        while !caller_disconnected(client) {
            tokio::task::yield_now().await;
        }
        for _ in 0..DISCONNECT_YIELDS {
            tokio::task::yield_now().await;
        }
        assert!(
            !disconnecting.is_finished(),
            "the disconnect must wait for the exchange that holds the link"
        );
    }

    /// A QUIC server on a free local port, and the certificate it presents.
    pub(crate) fn quic_server() -> (quinn::Endpoint, CertificateDer<'static>) {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let certified = rcgen::generate_simple_self_signed(vec![SERVER_NAME.to_string()]).unwrap();
        let certificate = certified.cert.der().clone();
        let key = PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());
        let config =
            quinn::ServerConfig::with_single_cert(vec![certificate.clone()], key.into()).unwrap();
        (
            quinn::Endpoint::server(config, LOCALHOST).unwrap(),
            certificate,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{DUPLEX_CAPACITY, LOCALHOST, request};
    use super::*;
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    #[test]
    fn given_any_attempt_when_computing_backoff_should_stay_within_full_jitter_bounds() {
        const BASE: Duration = Duration::from_millis(50);
        const CAP: Duration = Duration::from_secs(10);
        const ROUNDING: Duration = Duration::from_nanos(1);
        let ceilings = [
            (0, Duration::from_millis(50)),
            (1, Duration::from_millis(100)),
            (7, Duration::from_millis(6400)),
            (8, CAP),
            (31, CAP),
            (32, CAP),
            (64, CAP),
            (u32::MAX, CAP),
        ];

        for (attempt, ceiling) in ceilings {
            let lowest = backoff(attempt, BASE, CAP, 0.0);
            let highest = backoff(attempt, BASE, CAP, 1.0);
            assert_eq!(lowest, Duration::ZERO, "attempt {attempt}");
            assert!(
                highest <= ceiling && highest + ROUNDING >= ceiling,
                "attempt {attempt}: {highest:?} for {ceiling:?}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_any_draw_when_pausing_before_a_retry_should_leave_time_for_the_retry() {
        const DRAWS: u32 = 100;
        let mut retries = 0;
        for step in 0..=DRAWS {
            let draw = f64::from(step) / f64::from(DRAWS);
            // Half the first pause, so a draw near one half sleeps to the
            // deadline and a larger draw reaches past it.
            let deadline = Instant::now() + RETRY_BACKOFF_BASE / 2;
            let retry = pause_before_retry(0, deadline, draw).await;
            assert!(Instant::now() <= deadline, "draw {draw}");
            if retry {
                retries += 1;
                assert!(Instant::now() < deadline, "draw {draw}");
            }
        }
        assert!(retries > 0);
    }

    #[tokio::test]
    async fn given_a_passed_deadline_when_sending_should_keep_the_frame_off_the_wire() {
        const BOUND_SESSION: u64 = 3;
        let (client, mut server) = tokio::io::duplex(DUPLEX_CAPACITY);
        let (reader, writer) = tokio::io::split(client);
        let link = Link::Mux(MuxLink::tcp(reader, writer, LOCALHOST));
        let mut sent = false;

        let result = link
            .send(request(1, BOUND_SESSION), Instant::now(), &mut sent)
            .await;

        assert_eq!(result.unwrap_err(), IggyError::NotConnected);
        assert!(!sent);
        Link::shutdown(Arc::new(link)).await;
        let mut written = Vec::new();
        server.read_to_end(&mut written).await.unwrap();
        assert!(written.is_empty());
    }

    #[test]
    fn given_a_full_event_channel_when_publishing_should_drop_the_oldest_event() {
        let (sender, _receiver) = event_channel();
        for _ in 0..EVENT_CHANNEL_CAPACITY {
            sender.try_broadcast(DiagnosticEvent::Disconnected).unwrap();
        }

        assert!(matches!(
            sender.try_broadcast(DiagnosticEvent::SignedIn),
            Ok(Some(DiagnosticEvent::Disconnected))
        ));
    }
}
