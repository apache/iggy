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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, broadcast};
use tokio::time::{timeout, timeout_at};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

use crate::error::{KafkaProtocolError, Result};
use crate::protocol::api::{BrokerAdvertise, DEFAULT_KAFKA_PORT, HandleOutcome, handle_request};
use crate::protocol::codec::Decoder;
use crate::protocol::header::{
    RequestHeader, ResponseHeader, request_header_version, response_header_version,
};
use std::io;

const READ_CHUNK: usize = 65536;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind_addr: String,
    /// Hostname or IP advertised in Metadata (`IGGY_KAFKA_ADVERTISED_HOST`). Required when
    /// `bind_addr` uses a wildcard address (`0.0.0.0` / `::`).
    pub advertised_host: Option<String>,
    /// Port advertised in Metadata (`IGGY_KAFKA_ADVERTISED_PORT`). Defaults to the bind port.
    pub advertised_port: Option<u16>,
    pub max_frame_size: usize,
    /// Maximum concurrent connections accepted before new ones are rejected.
    pub max_connections: usize,
    /// Bound on how long an accepted connection may sit idle before sending the next
    /// frame's length prefix. Kafka brokers default `connections.max.idle.ms` to 10 minutes;
    /// match that so well-behaved idle clients aren't dropped.
    pub idle_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    /// Cap on how long graceful shutdown waits for in-flight connections to finish. Without
    /// this, a connection idling inside `idle_timeout` (10 minutes by default) would otherwise
    /// hold shutdown open past typical orchestrator grace periods (e.g. Kubernetes' default
    /// 30s `terminationGracePeriodSeconds`).
    pub shutdown_drain_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: format!("127.0.0.1:{DEFAULT_KAFKA_PORT}"),
            advertised_host: None,
            advertised_port: None,
            max_frame_size: 8 * 1024 * 1024,
            max_connections: 1024,
            idle_timeout: Duration::from_mins(10),
            read_timeout: Duration::from_secs(15),
            write_timeout: Duration::from_secs(10),
            shutdown_drain_timeout: Duration::from_secs(25),
        }
    }
}

impl BrokerAdvertise {
    /// Resolve the broker endpoint advertised in Metadata.
    ///
    /// `local_addr` is the address the listener is actually bound to (from `listener.local_addr()`).
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` when `advertised_host` is empty or the listener binds to a wildcard
    /// without an explicit advertised host.
    pub fn from_server_config(config: &ServerConfig, local_addr: SocketAddr) -> Result<Self> {
        let port = config
            .advertised_port
            .map_or_else(|| i32::from(local_addr.port()), i32::from);

        let host = if let Some(ref advertised) = config.advertised_host {
            let trimmed = advertised.trim();
            if trimmed.is_empty() {
                return Err(KafkaProtocolError::InvalidConfig(
                    "IGGY_KAFKA_ADVERTISED_HOST must not be empty".into(),
                ));
            }
            if trimmed.len() > i16::MAX as usize {
                return Err(KafkaProtocolError::InvalidConfig(
                    "IGGY_KAFKA_ADVERTISED_HOST exceeds Kafka nullable string limit (32767 bytes)"
                        .into(),
                ));
            }
            trimmed.to_string()
        } else if local_addr.ip().is_unspecified() {
            return Err(KafkaProtocolError::InvalidConfig(
                "binding to a wildcard address (0.0.0.0 or ::) requires \
                 IGGY_KAFKA_ADVERTISED_HOST to be set to a reachable hostname or IP for \
                 Metadata broker advertisement"
                    .into(),
            ));
        } else {
            local_addr.ip().to_string()
        };

        Ok(Self { host, port })
    }
}

pub struct KafkaServer {
    config: Arc<ServerConfig>,
}

impl KafkaServer {
    #[must_use]
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }

    /// Accept Kafka wire connections until `shutdown` fires, then drain in-flight tasks.
    ///
    /// `listener` must already be bound by the caller. This lets tests and `main` bind
    /// the port before spawning the task, eliminating the TOCTOU race of bind-drop-rebind.
    ///
    /// # Errors
    ///
    /// Returns an error on invalid config or a non-transient `accept()` error.
    pub async fn run(
        self,
        listener: TcpListener,
        mut shutdown: broadcast::Receiver<()>,
    ) -> Result<()> {
        let local_addr = listener.local_addr()?;
        let broker = Arc::new(BrokerAdvertise::from_server_config(
            &self.config,
            local_addr,
        )?);
        info!(
            "kafka listener bound on {} (advertised as {}:{})",
            local_addr, broker.host, broker.port
        );

        let tracker = TaskTracker::new();
        let conn_limiter = Arc::new(Semaphore::new(self.config.max_connections));
        // Cancelled on shutdown so connection tasks exit instead of sitting in idle waits
        // until `idle_timeout` (or forever if that is raised).
        let cancel = CancellationToken::new();

        let drain_timeout = self.config.shutdown_drain_timeout;

        loop {
            tokio::select! {
                result = shutdown.recv() => {
                    match result {
                        Ok(()) => {
                            info!("kafka listener shutdown requested");
                            drain(&tracker, &cancel, drain_timeout).await;
                            break;
                        }
                        // Capacity-1 channel: lagged means a signal was sent before we polled - treat as shutdown.
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            info!("kafka listener shutdown requested (lagged)");
                            drain(&tracker, &cancel, drain_timeout).await;
                            break;
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            drain(&tracker, &cancel, drain_timeout).await;
                            break;
                        }
                    }
                }
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, peer)) => {
                            let Ok(permit) = Arc::clone(&conn_limiter).try_acquire_owned() else {
                                warn!(%peer, max_connections = self.config.max_connections, "connection limit reached, rejecting");
                                continue;
                            };
                            if let Err(e) = stream.set_nodelay(true) {
                                warn!(%peer, "TCP_NODELAY failed: {e}");
                            }
                            if let Err(e) = enable_tcp_keepalive(&stream) {
                                warn!(%peer, "TCP_KEEPALIVE failed: {e}");
                            }
                            let cfg = Arc::clone(&self.config);
                            let broker = Arc::clone(&broker);
                            let conn_cancel = cancel.child_token();
                            tracker.spawn(async move {
                                let _permit = permit;
                                if let Err(err) =
                                    handle_connection(stream, cfg, peer, broker, conn_cancel).await
                                {
                                    warn!(%peer, "connection closed with error: {err}");
                                }
                            });
                        }
                        Err(e) if is_transient_accept_error(&e) => {
                            // Brief backoff on fd exhaustion to avoid busy-spinning.
                            if matches!(e.raw_os_error(), Some(23 | 24)) {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                            warn!(%e, "transient accept error, continuing");
                        }
                        Err(e) => {
                            drain(&tracker, &cancel, drain_timeout).await;
                            return Err(e.into());
                        }
                    }
                }

            }
        }
        Ok(())
    }
}

/// Cancel in-flight connections, close the tracker to new spawns, and wait for tasks to finish
/// (bounded by `deadline`). Cancellation is what actually drops idle sockets; `tracker.wait`
/// alone would leave tasks parked in `read_frame` until `idle_timeout`.
async fn drain(tracker: &TaskTracker, cancel: &CancellationToken, deadline: Duration) {
    cancel.cancel();
    tracker.close();
    if timeout(deadline, tracker.wait()).await.is_err() {
        warn!(
            ?deadline,
            "shutdown drain deadline exceeded; remaining connection tasks will be dropped with the runtime"
        );
    }
}

fn is_transient_accept_error(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::Interrupted | io::ErrorKind::ConnectionAborted | io::ErrorKind::WouldBlock
    ) || matches!(
        err.raw_os_error(),
        // EMFILE / ENFILE are common across Unix platforms when fd limits are hit.
        Some(23 | 24)
    )
}

fn enable_tcp_keepalive(stream: &TcpStream) -> std::io::Result<()> {
    let sock = socket2::SockRef::from(stream);
    sock.set_keepalive(true)?;
    Ok(())
}

async fn handle_connection(
    mut stream: TcpStream,
    config: Arc<ServerConfig>,
    peer: SocketAddr,
    broker: Arc<BrokerAdvertise>,
    cancel: CancellationToken,
) -> Result<()> {
    debug!(%peer, "connection accepted");

    loop {
        let Some(frame) = read_next_frame(&mut stream, &config, &peer, &cancel).await? else {
            return Ok(());
        };

        if frame.len() < 8 {
            return Err(KafkaProtocolError::BufferUnderflow {
                needed: 8,
                remaining: frame.len(),
            });
        }
        let api_key = i16::from_be_bytes([frame[0], frame[1]]);
        let api_version = i16::from_be_bytes([frame[2], frame[3]]);
        let req_hdr_ver = request_header_version(api_key, api_version);
        let resp_hdr_ver = response_header_version(api_key, api_version);

        // `request_header_version` only ever returns 1 or 2, both of which `decode_from`
        // handles, so its `UnsupportedHeaderVersion` arm is unreachable here; any decode error
        // is a malformed header and closes the connection.
        let mut decoder = Decoder::new(frame);
        let req = RequestHeader::decode_from(&mut decoder, req_hdr_ver)?;

        debug!(
            %peer,
            api_key = req.api_key,
            api_version = req.api_version,
            correlation_id = req.correlation_id,
            client_id = req.client_id.as_deref().unwrap_or(""),
            "received request"
        );

        let body = decoder.read_bytes(decoder.remaining())?;
        let outcome = handle_request(req.api_key, req.api_version, body, &broker);
        if dispatch_outcome(&mut stream, &peer, &config, &req, resp_hdr_ver, outcome).await? {
            return Ok(());
        }
    }
}

/// Returns `Ok(None)` when shutdown cancellation wins; `Ok(Some(frame))` on a full frame.
async fn read_next_frame(
    stream: &mut TcpStream,
    config: &ServerConfig,
    peer: &SocketAddr,
    cancel: &CancellationToken,
) -> Result<Option<bytes::Bytes>> {
    tokio::select! {
        () = cancel.cancelled() => {
            debug!(%peer, "connection cancelled by shutdown");
            Ok(None)
        }
        result = read_frame(
            stream,
            config.max_frame_size,
            config.idle_timeout,
            config.read_timeout,
        ) => match result {
            Ok(frame) => Ok(Some(frame)),
            Err(KafkaProtocolError::Io(ref e))
                if e.kind() == std::io::ErrorKind::UnexpectedEof
                    || e.kind() == std::io::ErrorKind::ConnectionReset =>
            {
                info!(%peer, "connection closed by client");
                Ok(None)
            }
            Err(e) => Err(e),
        },
    }
}

/// Applies a [`HandleOutcome`]. Returns `true` when the connection should close.
async fn dispatch_outcome(
    stream: &mut TcpStream,
    peer: &SocketAddr,
    config: &ServerConfig,
    req: &RequestHeader,
    resp_hdr_ver: i16,
    outcome: HandleOutcome,
) -> Result<bool> {
    let close_after_response = matches!(outcome, HandleOutcome::RespondAndClose(_));
    match outcome {
        HandleOutcome::NoResponse => {
            // Produce with acks=0: the wire protocol forbids a response.
            Ok(false)
        }
        HandleOutcome::Close => {
            warn!(
                %peer,
                api_key = req.api_key,
                api_version = req.api_version,
                "closing connection: no parseable error response for this request version"
            );
            Ok(true)
        }
        HandleOutcome::Respond(body_response) | HandleOutcome::RespondAndClose(body_response) => {
            let resp_header = ResponseHeader {
                correlation_id: req.correlation_id,
            };
            send_response(
                stream,
                &resp_header,
                resp_hdr_ver,
                &body_response,
                config.write_timeout,
            )
            .await?;
            if close_after_response {
                warn!(
                    %peer,
                    api_key = req.api_key,
                    api_version = req.api_version,
                    "closing connection after unsupported-version error response"
                );
            }
            Ok(close_after_response)
        }
    }
}

/// Write a single length-prefixed Kafka frame using one allocation.
/// Avoids the separate header-encode + payload-concat + length-prefix allocations.
async fn send_response(
    stream: &mut TcpStream,
    header: &ResponseHeader,
    header_version: i16,
    body: Bytes,
    write_timeout: Duration,
) -> Result<()> {
    let header_size = ResponseHeader::encoded_size(header_version);
    let payload_size = header_size + body.len();
    let payload_len_i32 =
        i32::try_from(payload_size).map_err(|_| KafkaProtocolError::FrameTooLarge {
            max_bytes: i32::MAX as usize,
            actual_bytes: payload_size,
        })?;
    let mut prefix = BytesMut::with_capacity(4 + header_size);
    prefix.put_i32(payload_len_i32);
    header.encode_into(&mut prefix, header_version);
    let mut frame = prefix.freeze().chain(body);
    timeout(write_timeout, stream.write_all_buf(&mut frame))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "write timeout"))??;
    Ok(())
}

/// Read one length-prefixed Kafka frame from `stream`.
///
/// # Errors
///
/// Returns an error on timeout, invalid length, or I/O failure.
pub async fn read_frame(
    stream: &mut TcpStream,
    max_frame_size: usize,
    idle_timeout: Duration,
    read_timeout: Duration,
) -> Result<bytes::Bytes> {
    let mut len_buf = [0u8; 4];
    // Idle: bounded wait for the client to start the next frame (or EOF).
    match timeout(idle_timeout, stream.read_exact(&mut len_buf)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => return Err(e.into()),
        Err(_) => return Err(io::Error::new(io::ErrorKind::TimedOut, "idle timeout").into()),
    }

    let frame_len_i32 = i32::from_be_bytes(len_buf);
    if frame_len_i32 <= 0 {
        return Err(KafkaProtocolError::InvalidFrameLength(frame_len_i32));
    }
    // frame_len_i32 is validated > 0 above, so it always fits usize on every
    // platform this crate targets (32-bit and 64-bit).
    #[allow(clippy::cast_sign_loss)]
    let frame_len = frame_len_i32 as usize;
    if frame_len > max_frame_size {
        return Err(KafkaProtocolError::FrameTooLarge {
            max_bytes: max_frame_size,
            actual_bytes: frame_len,
        });
    }

    // In-flight: read_timeout applies only after the length prefix is complete.
    let deadline = tokio::time::Instant::now() + read_timeout;
    // Reserve incrementally, one chunk ahead of what's actually been received, instead of
    // BytesMut::with_capacity(frame_len) up front - frame_len comes straight from the wire
    // (bounded only by max_frame_size), so an attacker who sends a valid length prefix and
    // then no body would otherwise force a full max_frame_size allocation per connection
    // before a single body byte arrives (same amplification class as PREALLOC_HINT).
    let mut data = BytesMut::with_capacity(frame_len.min(READ_CHUNK));
    while data.len() < frame_len {
        let remaining = frame_len - data.len();
        let chunk = remaining.min(READ_CHUNK);
        data.reserve(chunk);
        // `.limit(chunk)` bounds how much of BytesMut's spare capacity read_buf may fill,
        // so a single OS read still can't consume bytes belonging to the next pipelined
        // frame - the same guarantee the old resize()+read() approach had - but without
        // pre-zeroing the chunk first, since read_buf only writes into its own spare
        // capacity via chunk_mut() rather than requiring pre-initialized memory.
        match timeout_at(deadline, stream.read_buf(&mut (&mut data).limit(chunk))).await {
            Err(_) => return Err(io::Error::new(io::ErrorKind::TimedOut, "read timeout").into()),
            Ok(Ok(0)) => {
                return Err(
                    io::Error::new(io::ErrorKind::UnexpectedEof, "connection closed").into(),
                );
            }
            Ok(Err(e)) => return Err(e.into()),
            Ok(Ok(_)) => {}
        }
    }
    Ok(data.freeze())
}

pub fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()
        .map_err(|e| error!("failed to initialize tracing: {e}"));
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
        let (server, _) = listener.accept().await.unwrap();
        let client = client.await.unwrap();
        (client, server)
    }

    #[test]
    fn transient_accept_error_classification_covers_all_branches() {
        for kind in [
            io::ErrorKind::Interrupted,
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::WouldBlock,
        ] {
            assert!(is_transient_accept_error(&io::Error::from(kind)));
        }

        #[cfg(unix)]
        {
            assert!(is_transient_accept_error(&io::Error::from_raw_os_error(23)));
            assert!(is_transient_accept_error(&io::Error::from_raw_os_error(24)));
        }

        assert!(!is_transient_accept_error(&io::Error::from(
            io::ErrorKind::ConnectionRefused,
        )));
    }

    #[tokio::test]
    async fn send_response_writes_header_and_body() {
        let (mut client, mut server) = tcp_pair().await;
        let header = ResponseHeader {
            correlation_id: 0x0102_0304,
        };
        let body = [9u8, 8, 7];

        send_response(
            &mut server,
            &header,
            1,
            Bytes::copy_from_slice(&body),
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        let mut len = [0u8; 4];
        client.read_exact(&mut len).await.unwrap();
        assert_eq!(i32::from_be_bytes(len), 8);

        let mut payload = [0u8; 8];
        client.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload[..4], &[0x01, 0x02, 0x03, 0x04]);
        assert_eq!(payload[4], 0);
        assert_eq!(&payload[5..], &body);
    }

    #[tokio::test]
    async fn read_frame_rejects_negative_length() {
        let (mut client, mut server) = tcp_pair().await;
        client.write_all(&(-1_i32).to_be_bytes()).await.unwrap();
        let err = read_frame(
            &mut server,
            64,
            Duration::from_secs(5),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, KafkaProtocolError::InvalidFrameLength(-1)));
    }

    #[tokio::test]
    async fn read_frame_returns_eof_after_prefix_when_body_missing() {
        let (mut client, mut server) = tcp_pair().await;
        client.write_all(&(5_i32).to_be_bytes()).await.unwrap();
        client.shutdown().await.unwrap();
        let err = read_frame(
            &mut server,
            64,
            Duration::from_secs(5),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("connection closed"));
    }

    #[tokio::test]
    async fn read_frame_times_out_after_partial_body() {
        let (mut client, mut server) = tcp_pair().await;
        client.write_all(&(5_i32).to_be_bytes()).await.unwrap();
        client.write_all(&[1, 2]).await.unwrap();
        let err = read_frame(
            &mut server,
            64,
            Duration::from_secs(5),
            Duration::from_millis(50),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("read timeout"));
    }

    #[tokio::test]
    async fn read_frame_times_out_when_client_sends_nothing() {
        let (_client, mut server) = tcp_pair().await;
        let err = read_frame(
            &mut server,
            64,
            Duration::from_millis(50),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("idle timeout"));
    }

    #[tokio::test]
    async fn server_shutdown_does_not_stall_past_drain_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = broadcast::channel(1);
        let server = KafkaServer::new(ServerConfig {
            // Idle timeout is intentionally long - cancellation + drain deadline, not the
            // idle timeout, must bound shutdown here.
            idle_timeout: Duration::from_mins(10),
            shutdown_drain_timeout: Duration::from_millis(200),
            ..ServerConfig::default()
        });
        let handle = tokio::spawn(async move { server.run(listener, rx).await });

        // Held open, never sends a frame: without cancellation the task would park in
        // read_frame's idle wait for the full 600s idle_timeout.
        let mut held = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("shutdown must return well within the 600s idle_timeout")
            .unwrap();
        assert!(result.is_ok());

        // Cancellation must close the held socket so the client sees EOF, not a silent stall.
        let mut buf = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(1), held.read(&mut buf))
            .await
            .expect("held connection must be closed on shutdown")
            .unwrap();
        assert_eq!(n, 0, "shutdown must deliver EOF to idle clients");
    }

    #[tokio::test]
    async fn server_rejects_connections_beyond_max_connections() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = broadcast::channel(1);
        let server = KafkaServer::new(ServerConfig {
            max_connections: 1,
            ..ServerConfig::default()
        });
        let handle = tokio::spawn(async move { server.run(listener, rx).await });

        // First connection holds the only permit by never sending a frame.
        let held = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Second connection should be accepted at the TCP level (backlog) but closed
        // immediately by the server once the permit acquisition fails.
        let mut rejected = TcpStream::connect(addr).await.unwrap();
        let mut buf = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(1), rejected.read(&mut buf))
            .await
            .expect("server should close rejected connection promptly")
            .unwrap();
        assert_eq!(n, 0, "rejected connection should be closed with EOF");

        tx.send(()).unwrap();
        drop(held);
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn server_run_exits_when_shutdown_channel_closed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (tx, rx) = broadcast::channel(1);
        drop(tx);
        let server = KafkaServer::new(ServerConfig::default());
        assert!(server.run(listener, rx).await.is_ok());
    }

    #[tokio::test]
    async fn server_run_exits_when_shutdown_receiver_is_lagged() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (tx, rx) = broadcast::channel(1);
        tx.send(()).unwrap();
        tx.send(()).unwrap();
        let server = KafkaServer::new(ServerConfig::default());
        assert!(server.run(listener, rx).await.is_ok());
    }

    #[tokio::test]
    async fn server_run_exits_on_shutdown_signal_ok() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = broadcast::channel(1);
        let server = KafkaServer::new(ServerConfig::default());
        let handle = tokio::spawn(async move { server.run(listener, rx).await });

        let stream = TcpStream::connect(addr).await.unwrap();
        tx.send(()).unwrap();
        drop(stream);
        assert!(handle.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn read_frame_accepts_exact_max_frame_size() {
        let (mut client, mut server) = tcp_pair().await;
        let max_frame_size = 64usize;
        let payload = vec![0xABu8; max_frame_size];
        client
            .write_all(&i32::try_from(max_frame_size).unwrap().to_be_bytes())
            .await
            .unwrap();
        client.write_all(&payload).await.unwrap();
        let frame = read_frame(
            &mut server,
            max_frame_size,
            Duration::from_secs(5),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(frame.len(), max_frame_size);
    }

    #[tokio::test]
    async fn read_frame_reassembles_frame_spanning_multiple_read_chunks() {
        // frame_len exceeds READ_CHUNK, forcing the incremental reserve()+read_buf loop
        // through more than one iteration - guards the switch away from a single
        // BytesMut::with_capacity(frame_len) upfront allocation.
        let (mut client, mut server) = tcp_pair().await;
        let frame_len = READ_CHUNK + 1024;
        let payload: Vec<u8> = (0..frame_len)
            .map(|i| u8::try_from(i % 251).expect("i % 251 < 256"))
            .collect();
        client
            .write_all(&i32::try_from(frame_len).unwrap().to_be_bytes())
            .await
            .unwrap();
        client.write_all(&payload).await.unwrap();
        let frame = read_frame(
            &mut server,
            frame_len,
            Duration::from_secs(5),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(&frame[..], &payload[..]);
    }

    #[tokio::test]
    async fn read_frame_rejects_frame_larger_than_max() {
        let (mut client, mut server) = tcp_pair().await;
        let max_frame_size = 64usize;
        client.write_all(&65_i32.to_be_bytes()).await.unwrap();
        let err = read_frame(
            &mut server,
            max_frame_size,
            Duration::from_secs(5),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            KafkaProtocolError::FrameTooLarge {
                max_bytes: 64,
                actual_bytes: 65,
            }
        ));
    }

    #[tokio::test]
    async fn send_response_v0_writes_correlation_id_only() {
        let (mut client, mut server) = tcp_pair().await;
        let header = ResponseHeader {
            correlation_id: 0x0000_00AB,
        };
        let body = [5u8, 6, 7];

        send_response(
            &mut server,
            &header,
            0,
            Bytes::copy_from_slice(&body),
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        let mut len = [0u8; 4];
        client.read_exact(&mut len).await.unwrap();
        assert_eq!(i32::from_be_bytes(len), 7);

        let mut payload = [0u8; 7];
        client.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload[..4], &[0, 0, 0, 0xAB]);
        assert_eq!(&payload[4..], &body);
    }

    #[test]
    fn init_tracing_is_idempotent() {
        init_tracing();
        init_tracing();
    }
}
