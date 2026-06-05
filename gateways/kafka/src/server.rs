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
use tracing::{error, info, warn};

use crate::error::{KafkaProtocolError, Result};
use crate::protocol::api::handle_request;
use crate::protocol::codec::Decoder;
use crate::protocol::header::{
    RequestHeader, ResponseHeader, request_header_version, response_header_version,
};

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
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }

    pub async fn run(self, mut shutdown: broadcast::Receiver<()>) -> Result<()> {
        let listener = TcpListener::bind(&self.config.bind_addr).await?;
        info!("kafka listener bound on {}", self.config.bind_addr);

        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("kafka listener shutdown requested");
                    break;
                }
                accept_result = listener.accept() => {
                    let (stream, peer) = accept_result?;
                    let cfg = Arc::clone(&self.config);
                    tokio::spawn(async move {
                        if let Err(err) = handle_connection(stream, cfg, peer).await {
                            warn!(%peer, "connection closed with error: {err}");
                        }
                    });
                }
            }
        }
        Ok(())
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    config: Arc<ServerConfig>,
    peer: SocketAddr,
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

        let mut decoder = Decoder::new(frame);
        let req = RequestHeader::decode_from(&mut decoder, req_hdr_ver)?;
        info!(
            %peer,
            api_key = req.api_key,
            api_version = req.api_version,
            correlation_id = req.correlation_id,
            client_id = req.client_id.as_deref().unwrap_or(""),
            "received request"
        );

        let body = decoder.read_bytes(decoder.remaining())?;
        let body_response = handle_request(req.api_key, req.api_version, body);

        let resp_header = ResponseHeader {
            correlation_id: req.correlation_id,
        };
        let encoded_header = resp_header.encode(resp_hdr_ver);
        let mut payload = BytesMut::with_capacity(encoded_header.len() + body_response.len());
        payload.put_slice(&encoded_header);
        payload.put_slice(&body_response);

        write_frame(&mut stream, &payload, config.write_timeout).await?;
    }
}

pub async fn read_frame(
    stream: &mut TcpStream,
    max_frame_size: usize,
    read_timeout: Duration,
) -> Result<bytes::Bytes> {
    let mut len_buf = [0u8; 4];
    timeout(read_timeout, stream.read_exact(&mut len_buf))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "read timeout"))??;

    let frame_len = i32::from_be_bytes(len_buf);
    if frame_len <= 0 {
        return Err(KafkaProtocolError::InvalidFrameLength(frame_len));
    }

    let frame_len = frame_len as usize;
    if frame_len > max_frame_size {
        return Err(KafkaProtocolError::FrameTooLarge {
            max_bytes: max_frame_size,
            actual_bytes: frame_len,
        });
    }

    let mut data = vec![0u8; frame_len];
    timeout(read_timeout, stream.read_exact(&mut data))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "read timeout"))??;
    Ok(bytes::Bytes::from(data))
}

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
    frame.put_i32(len as i32);
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
