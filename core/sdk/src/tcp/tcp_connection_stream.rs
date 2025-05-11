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

use crate::tcp::tcp_stream::ConnectionStream;
use async_trait::async_trait;
use iggy_common::IggyError;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tracing::error;

#[derive(Debug)]
pub struct TcpConnectionStream {
    client_address: SocketAddr,
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
}

impl TcpConnectionStream {
    pub fn new(client_address: SocketAddr, stream: TcpStream) -> Self {
        let (reader, writer) = stream.into_split();
        Self {
            client_address,
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
        }
    }
}

#[async_trait]
impl ConnectionStream for TcpConnectionStream {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, IggyError> {
        self.reader.read_exact(buf).await.map_err(|error| {
            error!(
                "Failed to read data by client: {} from the TCP connection: {error}",
                self.client_address
            );
            IggyError::TcpError
        })
    }

    async fn write(&mut self, buf: &[u8]) -> Result<(), IggyError> {
        self.writer.write_all(buf).await.map_err(|error| {
            error!(
                "Failed to write data by client: {} to the TCP connection: {error}",
                self.client_address
            );
            IggyError::TcpError
        })
    }

    async fn flush(&mut self) -> Result<(), IggyError> {
        self.writer.flush().await.map_err(|error| {
            error!(
                "Failed to flush data by client: {} to the TCP connection: {error}",
                self.client_address
            );
            IggyError::TcpError
        })
    }

    async fn shutdown(&mut self) -> Result<(), IggyError> {
        self.writer.shutdown().await.map_err(|error| {
            error!(
                "Failed to shutdown the TCP connection by client: {} to the TCP connection: {error}",
                self.client_address
            );
            IggyError::TcpError
        })
    }
}
