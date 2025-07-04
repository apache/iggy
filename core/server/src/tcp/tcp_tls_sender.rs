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

use crate::binary::sender::Sender;
use crate::streaming::utils::PooledBuffer;
use crate::tcp::COMPONENT;
use crate::{server_error::ServerError, tcp::sender};
use bytes::BytesMut;
use compio::buf::IoBufMut;
use compio::io::AsyncWrite;
use compio::net::TcpStream;
use error_set::ErrContext;
use iggy_common::IggyError;
//use tokio_rustls::server::TlsStream;
use nix::libc;

#[derive(Debug)]
pub struct TcpTlsSender {
    pub(crate) stream: TcpStream,
}

impl Sender for TcpTlsSender {
    async fn read<B: IoBufMut>(&mut self, buffer: B) -> (Result<usize, IggyError>, B) {
        todo!();
        sender::read(&mut self.stream, buffer).await
    }

    async fn send_empty_ok_response(&mut self) -> Result<(), IggyError> {
        todo!();
        sender::send_empty_ok_response(&mut self.stream).await
    }

    async fn send_ok_response(&mut self, payload: &[u8]) -> Result<(), IggyError> {
        todo!();
        sender::send_ok_response(&mut self.stream, payload).await
    }

    async fn send_error_response(&mut self, error: IggyError) -> Result<(), IggyError> {
        todo!();
        sender::send_error_response(&mut self.stream, error).await
    }

    async fn shutdown(&mut self) -> Result<(), ServerError> {
        todo!();
        self.stream
            .shutdown()
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to shutdown TCP TLS stream")
            })
            .map_err(ServerError::IoError)
    }

    async fn send_ok_response_vectored(
        &mut self,
        length: &[u8],
        slices: Vec<PooledBuffer>,
    ) -> Result<(), IggyError> {
        todo!();
        sender::send_ok_response_vectored(&mut self.stream, length, slices).await
    }
}
