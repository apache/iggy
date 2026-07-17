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

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::time::{timeout, timeout_at};
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

use crate::error::{KafkaProtocolError, Result};
use crate::protocol::api::{
    BrokerAdvertise, DEFAULT_KAFKA_PORT, ERROR_INVALID_REQUEST, HandleOutcome,
    encode_error_only_response, handle_request,
};
use crate::protocol::codec::Decoder;
use crate::protocol::header::{
    RequestHeader, ResponseHeader, request_header_version, response_header_version,
};
use std::io;

const READ_CHUNK: usize = 65536;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind_addr: String,
    /// Hostname or IP advertised in Metadata (`KAFKA_ADVERTISED_HOST`). Required when `bind_addr`
    /// uses a wildcard address (`0.0.0.0` / `::`).
    pub advertised_host: Option<String>,
    /// Port advertised in Metadata (`KAFKA_ADVERTISED_PORT`). Defaults to the bind port.
    pub advertised_port: Option<u16>,
    pub max_frame_size: usize,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: format!("127.0.0.1:{DEFAULT_KAFKA_PORT}"),
            advertised_host: None,
            advertised_port: None,
            max_frame_size: 8 * 1024 * 1024,
            read_timeout: Duration::from_secs(15),
            write_timeout: Duration::from_secs(10),
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
                    "KAFKA_ADVERTISED_HOST must not be empty".into(),
                ));
            }
            if trimmed.len() > i16::MAX as usize {
                return Err(KafkaProtocolError::InvalidConfig(
                    "KAFKA_ADVERTISED_HOST exceeds Kafka nullable string limit (32767 bytes)"
                        .into(),
                ));
            }
            trimmed.to_string()
        } else if local_addr.ip().is_unspecified() {
            return Err(KafkaProtocolError::InvalidConfig(
                "binding to a wildcard address (0.0.0.0 or ::) requires KAFKA_ADVERTISED_HOST \
                 to be set to a reachable hostname or IP for Metadata broker advertisement"
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
        let broker = Arc::clone(&broker);

        loop {
            tokio::select! {
                result = shutdown.recv() => {
                    match result {
                        Ok(()) => {
                            info!("kafka listener shutdown requested");
                            tracker.close();
                            tracker.wait().await;
                            break;
                        }
                        // Capacity-1 channel: lagged means a signal was sent before we polled — treat as shutdown.
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            info!("kafka listener shutdown requested (lagged)");
                            tracker.close();
                            tracker.wait().await;
                            break;
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            tracker.close();
                            tracker.wait().await;
                            break;
                        }
                    }
                }
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, peer)) => {
                            if let Err(e) = stream.set_nodelay(true) {
                                warn!(%peer, "TCP_NODELAY failed: {e}");
                            }
                            if let Err(e) = enable_tcp_keepalive(&stream) {
                                warn!(%peer, "TCP_KEEPALIVE failed: {e}");
                            }
                            let cfg = Arc::clone(&self.config);
                            let broker = Arc::clone(&broker);
                            tracker.spawn(async move {
                                if let Err(err) = handle_connection(stream, cfg, peer, broker).await {
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
                        Err(e) => return Err(e.into()),
                    }
                }

            }
        }
        Ok(())
    }
}

fn is_transient_accept_error(err: &std::io::Error) -> bool {
    use std::io::ErrorKind;

    matches!(
        err.kind(),
        ErrorKind::Interrupted | ErrorKind::ConnectionAborted | ErrorKind::WouldBlock
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
) -> Result<()> {
    debug!(%peer, "connection accepted");

    loop {
        let frame = match read_frame(&mut stream, config.max_frame_size, config.read_timeout).await
        {
            Ok(f) => f,
            Err(KafkaProtocolError::Io(ref e))
                if e.kind() == std::io::ErrorKind::UnexpectedEof
                    || e.kind() == std::io::ErrorKind::ConnectionReset =>
            {
                info!(%peer, "connection closed by client");
                return Ok(());
            }
            Err(e) => return Err(e),
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
        let correlation_id = correlation_id_from_frame(&frame);

        let mut decoder = Decoder::new(frame);
        let req = match RequestHeader::decode_from(&mut decoder, req_hdr_ver) {
            Ok(req) => req,
            Err(KafkaProtocolError::UnsupportedHeaderVersion(_)) => {
                warn!(%peer, api_key, api_version, "unsupported request header version");
                let body_response = encode_error_only_response(ERROR_INVALID_REQUEST);
                let resp_header = ResponseHeader { correlation_id };
                send_response(
                    &mut stream,
                    &resp_header,
                    0,
                    &body_response,
                    config.write_timeout,
                )
                .await?;
                return Ok(());
            }
            Err(e) => return Err(e),
        };

        debug!(
            %peer,
            api_key = req.api_key,
            api_version = req.api_version,
            correlation_id = req.correlation_id,
            client_id = req.client_id.as_deref().unwrap_or(""),
            "received request"
        );

        let body = decoder.read_bytes(decoder.remaining())?;
        match handle_request(req.api_key, req.api_version, body, &broker) {
            HandleOutcome::NoResponse => {
                // Produce with acks=0: the wire protocol forbids a response.
            }
            HandleOutcome::Close => {
                warn!(
                    %peer,
                    api_key = req.api_key,
                    api_version = req.api_version,
                    "closing connection: no parseable error response for this request version"
                );
                return Ok(());
            }
            HandleOutcome::Respond(body_response) => {
                let resp_header = ResponseHeader {
                    correlation_id: req.correlation_id,
                };
                send_response(
                    &mut stream,
                    &resp_header,
                    resp_hdr_ver,
                    &body_response,
                    config.write_timeout,
                )
                .await?;
            }
        }
    }
}

/// Write a single length-prefixed Kafka frame using one allocation.
/// Avoids the separate header-encode + payload-concat + length-prefix allocations.
async fn send_response(
    stream: &mut TcpStream,
    header: &ResponseHeader,
    header_version: i16,
    body: &[u8],
    write_timeout: Duration,
) -> Result<()> {
    let header_size = ResponseHeader::encoded_size(header_version);
    let payload_size = header_size + body.len();
    let payload_len_i32 =
        i32::try_from(payload_size).map_err(|_| KafkaProtocolError::FrameTooLarge {
            max_bytes: i32::MAX as usize,
            actual_bytes: payload_size,
        })?;
    let mut frame = BytesMut::with_capacity(4 + payload_size);
    frame.put_i32(payload_len_i32);
    header.encode_into(&mut frame, header_version);
    frame.put_slice(body);
    timeout(write_timeout, stream.write_all(&frame))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "write timeout"))??;
    Ok(())
}

fn correlation_id_from_frame(frame: &bytes::Bytes) -> i32 {
    i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]])
}

/// Read one length-prefixed Kafka frame from `stream`.
///
/// # Errors
///
/// Returns an error on timeout, invalid length, or I/O failure.
pub async fn read_frame(
    stream: &mut TcpStream,
    max_frame_size: usize,
    read_timeout: Duration,
) -> Result<bytes::Bytes> {
    let mut len_buf = [0u8; 4];
    // Idle: block until client starts next frame (or EOF). No read_timeout here.
    stream.read_exact(&mut len_buf).await?;

    let frame_len_i32 = i32::from_be_bytes(len_buf);
    if frame_len_i32 <= 0 {
        return Err(KafkaProtocolError::InvalidFrameLength(frame_len_i32));
    }
    let frame_len =
        usize::try_from(frame_len_i32).map_err(|_| KafkaProtocolError::FrameTooLarge {
            max_bytes: max_frame_size,
            actual_bytes: usize::MAX,
        })?;
    if frame_len > max_frame_size {
        return Err(KafkaProtocolError::FrameTooLarge {
            max_bytes: max_frame_size,
            actual_bytes: frame_len,
        });
    }

    // In-flight: read_timeout applies only after the length prefix is complete.
    let deadline = tokio::time::Instant::now() + read_timeout;
    // read_buf() exposes all BytesMut spare capacity to the OS; after reserve(n) the
    // allocator may give more than n bytes, so the OS can fill past frame_len and silently
    // consume bytes belonging to the next pipelined frame. Use read() with a bounded slice
    // so each OS call is limited to exactly the remaining bytes needed.
    let mut data = BytesMut::with_capacity(frame_len);
    while data.len() < frame_len {
        let remaining = frame_len - data.len();
        let chunk = remaining.min(READ_CHUNK);
        let prev = data.len();
        data.resize(prev + chunk, 0);
        let n = match timeout_at(deadline, stream.read(&mut data[prev..prev + chunk])).await {
            Err(_) => return Err(io::Error::new(io::ErrorKind::TimedOut, "read timeout").into()),
            Ok(Ok(0)) => {
                return Err(
                    io::Error::new(io::ErrorKind::UnexpectedEof, "connection closed").into(),
                );
            }
            Ok(Err(e)) => return Err(e.into()),
            Ok(Ok(n)) => n,
        };
        data.truncate(prev + n);
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

    #[test]
    fn correlation_id_is_extracted_from_frame() {
        let frame =
            bytes::Bytes::from_static(&[0x00, 0x12, 0x00, 0x01, 0x11, 0x22, 0x33, 0x44, 0xaa]);
        assert_eq!(correlation_id_from_frame(&frame), 0x1122_3344);
    }

    #[tokio::test]
    async fn send_response_writes_header_and_body() {
        let (mut client, mut server) = tcp_pair().await;
        let header = ResponseHeader {
            correlation_id: 0x0102_0304,
        };
        let body = [9u8, 8, 7];

        send_response(&mut server, &header, 1, &body, Duration::from_secs(1))
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
        let err = read_frame(&mut server, 64, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(matches!(err, KafkaProtocolError::InvalidFrameLength(-1)));
    }

    #[tokio::test]
    async fn read_frame_returns_eof_after_prefix_when_body_missing() {
        let (mut client, mut server) = tcp_pair().await;
        client.write_all(&(5_i32).to_be_bytes()).await.unwrap();
        client.shutdown().await.unwrap();
        let err = read_frame(&mut server, 64, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("connection closed"));
    }

    #[tokio::test]
    async fn read_frame_times_out_after_partial_body() {
        let (mut client, mut server) = tcp_pair().await;
        client.write_all(&(5_i32).to_be_bytes()).await.unwrap();
        client.write_all(&[1, 2]).await.unwrap();
        let err = read_frame(&mut server, 64, Duration::from_millis(50))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("read timeout"));
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
        let frame = read_frame(&mut server, max_frame_size, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(frame.len(), max_frame_size);
    }

    #[tokio::test]
    async fn read_frame_rejects_frame_larger_than_max() {
        let (mut client, mut server) = tcp_pair().await;
        let max_frame_size = 64usize;
        client.write_all(&65_i32.to_be_bytes()).await.unwrap();
        let err = read_frame(&mut server, max_frame_size, Duration::from_secs(1))
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

        send_response(&mut server, &header, 0, &body, Duration::from_secs(1))
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
