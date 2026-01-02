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

use super::{PooledBuffer, Sender};
use crate::IggyError;
use compio::buf::IoBufMut;
use compio::io::AsyncWrite;
use compio::net::TcpStream;
use err_trail::ErrContext;

const COMPONENT: &str = "TCP";

#[derive(Debug)]
pub struct TcpSender {
    pub(crate) stream: Option<TcpStream>,
}

impl Sender for TcpSender {
    async fn read<B: IoBufMut>(&mut self, buffer: B) -> (Result<(), IggyError>, B) {
        match self.stream.as_mut() {
            Some(stream) => super::read(stream, buffer).await,
            None => (Err(IggyError::ConnectionClosed), buffer),
        }
    }

    async fn send_empty_ok_response(&mut self) -> Result<(), IggyError> {
        match self.stream.as_mut() {
            Some(stream) => super::send_empty_ok_response(stream).await,
            None => Err(IggyError::ConnectionClosed),
        }
    }

    async fn send_ok_response(&mut self, payload: &[u8]) -> Result<(), IggyError> {
        match self.stream.as_mut() {
            Some(stream) => super::send_ok_response(stream, payload).await,
            None => Err(IggyError::ConnectionClosed),
        }
    }

    async fn send_error_response(&mut self, error: IggyError) -> Result<(), IggyError> {
        match self.stream.as_mut() {
            Some(stream) => super::send_error_response(stream, error).await,
            None => Err(IggyError::ConnectionClosed),
        }
    }

    async fn shutdown(&mut self) -> Result<(), IggyError> {
        match self.stream.as_mut() {
            Some(stream) => stream
                .shutdown()
                .await
                .error(|e: &std::io::Error| {
                    format!("{COMPONENT} (error: {e}) - failed to shutdown TCP stream")
                })
                .map_err(|e| IggyError::IoError(e.to_string())),

            None => Err(IggyError::ConnectionClosed),
        }
    }

    async fn send_ok_response_vectored(
        &mut self,
        length: &[u8],
        slices: Vec<PooledBuffer>,
    ) -> Result<(), IggyError> {
        if self.stream.is_none() {
            tracing::error!("Tried to send but stream is None!");
        }
        match self.stream.as_mut() {
            Some(stream) => super::send_ok_response_vectored(stream, length, slices).await,

            None => Err(IggyError::ConnectionClosed),
        }
    }
}
