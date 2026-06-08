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
use tokio::time::timeout;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

use crate::error::{KafkaProtocolError, Result};
use crate::protocol::api::{
    BrokerAdvertise, ERROR_INVALID_REQUEST, encode_error_only_response, handle_request,
};
use crate::protocol::codec::Decoder;
use crate::protocol::header::{
    RequestHeader, ResponseHeader, request_header_version, response_header_version,
};
use std::io;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind_addr: String,
    pub max_frame_size: usize,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9093".to_string(),
            max_frame_size: 8 * 1024 * 1024,
            read_timeout: Duration::from_secs(15),
            write_timeout: Duration::from_secs(10),
        }
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
    /// # Errors
    ///
    /// Returns an error if binding fails or a non-transient `accept()` error occurs.
    pub async fn run(self, mut shutdown: broadcast::Receiver<()>) -> Result<()> {
        let listener = TcpListener::bind(&self.config.bind_addr).await?;
        info!("kafka listener bound on {}", self.config.bind_addr);

        let tracker = TaskTracker::new();
        let broker = Arc::new(BrokerAdvertise::from_bind_addr(&self.config.bind_addr));

        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("kafka listener shutdown requested");
                    tracker.close();
                    tracker.wait().await;
                    break;
                }
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, peer)) => {
                            let cfg = Arc::clone(&self.config);
                            let broker = Arc::clone(&broker);
                            tracker.spawn(async move {
                                if let Err(err) = handle_connection(stream, cfg, peer, broker).await {
                                    warn!(%peer, "connection closed with error: {err}");
                                }
                            });
                        }
                        Err(e) if is_transient_accept_error(&e) => {
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

async fn handle_connection(
    mut stream: TcpStream,
    config: Arc<ServerConfig>,
    peer: SocketAddr,
    broker: Arc<BrokerAdvertise>,
) -> Result<()> {
    info!(%peer, "connection accepted");

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

        if frame.len() < 4 {
            return Err(KafkaProtocolError::BufferUnderflow {
                needed: 4,
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

        info!(
            %peer,
            api_key = req.api_key,
            api_version = req.api_version,
            correlation_id = req.correlation_id,
            client_id = req.client_id.as_deref().unwrap_or(""),
            "received request"
        );

        let body = decoder.read_bytes(decoder.remaining())?;
        let body_response = handle_request(req.api_key, req.api_version, body, &broker);

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
    if payload_size > i32::MAX as usize {
        return Err(KafkaProtocolError::FrameTooLarge {
            max_bytes: i32::MAX as usize,
            actual_bytes: payload_size,
        });
    }
    let mut frame = BytesMut::with_capacity(4 + payload_size);
    frame.put_i32(payload_size as i32);
    header.encode_into(&mut frame, header_version);
    frame.put_slice(body);
    timeout(write_timeout, stream.write_all(&frame))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "write timeout"))??;
    Ok(())
}

fn correlation_id_from_frame(frame: &bytes::Bytes) -> i32 {
    if frame.len() >= 8 {
        i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]])
    } else {
        0
    }
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
    timeout(read_timeout, stream.read_exact(&mut len_buf))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "read timeout"))??;

    let frame_len_i32 = i32::from_be_bytes(len_buf);
    if frame_len_i32 <= 0 {
        return Err(KafkaProtocolError::InvalidFrameLength(frame_len_i32));
    }

    let frame_len =
        usize::try_from(frame_len_i32).map_err(|_| KafkaProtocolError::FrameTooLarge {
            max_bytes: max_frame_size,
            actual_bytes: u32::MAX as usize,
        })?;
    if frame_len > max_frame_size {
        return Err(KafkaProtocolError::FrameTooLarge {
            max_bytes: max_frame_size,
            actual_bytes: frame_len,
        });
    }

    // read_buf fills BytesMut spare capacity without zero-initializing it first.
    let mut data = BytesMut::with_capacity(frame_len);
    timeout(read_timeout, async {
        while data.len() < frame_len {
            if stream.read_buf(&mut data).await? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed",
                ));
            }
        }
        Ok::<_, io::Error>(())
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "read timeout"))??;
    Ok(data.freeze())
}

/// Write one length-prefixed Kafka frame to `stream`.
///
/// # Errors
///
/// Returns an error on timeout, oversize payload, or I/O failure.
pub async fn write_frame(
    stream: &mut TcpStream,
    payload: &[u8],
    write_timeout: Duration,
) -> Result<()> {
    let len = payload.len();
    if len > i32::MAX as usize {
        return Err(KafkaProtocolError::FrameTooLarge {
            max_bytes: i32::MAX as usize,
            actual_bytes: len,
        });
    }
    let mut frame = BytesMut::with_capacity(4 + len);
    let len_i32 = i32::try_from(len).map_err(|_| KafkaProtocolError::FrameTooLarge {
        max_bytes: i32::MAX as usize,
        actual_bytes: len,
    })?;
    frame.put_i32(len_i32);
    frame.extend_from_slice(payload);
    timeout(write_timeout, stream.write_all(&frame))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "write timeout"))??;
    Ok(())
}

pub fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()
        .map_err(|e| error!("failed to initialize tracing: {e}"));
}
