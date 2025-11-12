/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::websocket::websocket_stream::ConnectionStream;
use async_trait::async_trait;
use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use iggy_common::IggyError;
use std::io::ErrorKind;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};
use tracing::{debug, error, trace};

#[derive(Debug)]
pub struct WebSocketConnectionStream {
    client_address: SocketAddr,
    stream: WebSocketStream<TcpStream>,
    read_buffer: BytesMut,
}

impl WebSocketConnectionStream {
    pub fn new(client_address: SocketAddr, stream: WebSocketStream<TcpStream>) -> Self {
        Self {
            client_address,
            stream,
            read_buffer: BytesMut::new(),
        }
    }

    /// Internal method to read a WebSocket message and buffer it
    async fn read_message(&mut self) -> Result<(), IggyError> {
        loop {
            match self.stream.next().await {
                Some(Ok(Message::Binary(data))) => {
                    trace!(
                        "Received WebSocket binary message from {}, size: {} bytes",
                        self.client_address,
                        data.len()
                    );
                    self.read_buffer.extend_from_slice(&data);
                    return Ok(());
                }
                Some(Ok(Message::Text(text))) => {
                    trace!(
                        "Received WebSocket text message from {}, converting to binary",
                        self.client_address
                    );
                    self.read_buffer.extend_from_slice(text.as_bytes());
                    return Ok(());
                }
                Some(Ok(Message::Ping(data))) => {
                    trace!(
                        "Received WebSocket ping from {}, sending pong",
                        self.client_address
                    );
                    if let Err(e) = self.stream.send(Message::Pong(data)).await {
                        error!(
                            "Failed to send WebSocket pong to {}: {}",
                            self.client_address, e
                        );
                        return Err(IggyError::WebSocketSendError);
                    }
                    continue;
                }
                Some(Ok(Message::Pong(_))) => {
                    trace!("Received WebSocket pong from {}", self.client_address);
                    continue;
                }
                Some(Ok(Message::Close(_))) => {
                    debug!(
                        "WebSocket connection closed by client: {}",
                        self.client_address
                    );
                    return Err(IggyError::ConnectionClosed);
                }
                Some(Ok(Message::Frame(_))) => {
                    // Raw frames - just continue
                    continue;
                }
                Some(Err(e)) => {
                    error!(
                        "Failed to read WebSocket message from {}: {}",
                        self.client_address, e
                    );
                    return match e {
                        tokio_tungstenite::tungstenite::Error::ConnectionClosed
                        | tokio_tungstenite::tungstenite::Error::AlreadyClosed => {
                            Err(IggyError::ConnectionClosed)
                        }
                        tokio_tungstenite::tungstenite::Error::Io(io_err) => match io_err.kind() {
                            ErrorKind::UnexpectedEof
                            | ErrorKind::ConnectionAborted
                            | ErrorKind::ConnectionReset => Err(IggyError::ConnectionClosed),
                            _ => Err(IggyError::WebSocketReceiveError),
                        },
                        _ => Err(IggyError::WebSocketReceiveError),
                    };
                }
                None => {
                    debug!("WebSocket stream ended for client: {}", self.client_address);
                    return Err(IggyError::ConnectionClosed);
                }
            }
        }
    }
}

#[async_trait]
impl ConnectionStream for WebSocketConnectionStream {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, IggyError> {
        let requested_bytes = buf.len();

        // Keep reading until we have enough bytes
        while self.read_buffer.len() < requested_bytes {
            self.read_message().await?;
        }

        // Copy exactly the requested bytes
        buf.copy_from_slice(&self.read_buffer[..requested_bytes]);

        // Remove consumed bytes from buffer
        let _consumed = self.read_buffer.split_to(requested_bytes);

        trace!(
            "Read {} bytes from WebSocket stream for client: {}",
            requested_bytes, self.client_address
        );

        Ok(requested_bytes)
    }

    async fn write(&mut self, buf: &[u8]) -> Result<(), IggyError> {
        trace!(
            "Writing {} bytes to WebSocket stream for client: {}",
            buf.len(),
            self.client_address
        );

        debug!(
            "WebSocket write {} bytes: {:02x?}",
            buf.len(),
            &buf[..buf.len().min(16)]
        );

        self.stream
            .send(Message::Binary(buf.to_vec().into()))
            .await
            .map_err(|e| {
                error!(
                    "Failed to write data to WebSocket connection for client: {}: {}",
                    self.client_address, e
                );
                match e {
                    tokio_tungstenite::tungstenite::Error::ConnectionClosed
                    | tokio_tungstenite::tungstenite::Error::AlreadyClosed => {
                        IggyError::ConnectionClosed
                    }
                    tokio_tungstenite::tungstenite::Error::Io(io_err) => match io_err.kind() {
                        ErrorKind::BrokenPipe
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::ConnectionReset => IggyError::ConnectionClosed,
                        _ => IggyError::WebSocketSendError,
                    },
                    _ => IggyError::WebSocketSendError,
                }
            })
    }

    async fn flush(&mut self) -> Result<(), IggyError> {
        trace!(
            "Flushing WebSocket stream for client: {}",
            self.client_address
        );
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), IggyError> {
        debug!(
            "Shutting down WebSocket connection for client: {}",
            self.client_address
        );

        let close_message =
            Message::Close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal,
                reason: "Client requested shutdown".into(),
            }));

        self.stream.send(close_message).await.map_err(|e| {
            error!(
                "Failed to send close frame to WebSocket connection for client: {}: {}",
                self.client_address, e
            );
            IggyError::WebSocketCloseError
        })
    }
}
