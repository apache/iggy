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

use crate::quic::COMPONENT;
use crate::{binary::sender::Sender, server_error::ServerError};
use bytes::BytesMut;
use error_set::ErrContext;
use iggy_common::IggyError;
use monoio::buf::IoBufMut;
use nix::libc;
use quinn::{RecvStream, SendStream};
use std::io::IoSlice;
use tracing::{debug, error};

const STATUS_OK: &[u8] = &[0; 4];

#[derive(Debug)]
pub struct QuicSender {
    pub(crate) send: SendStream,
    pub(crate) recv: RecvStream,
}

impl Sender for QuicSender {
    async fn read<B: IoBufMut>(&mut self, buffer: B) -> (Result<usize, IggyError>, B) {
        //TODO: Fixme
        // Not-so-nice code because quinn recv stream has different API for read_exact
        /*
        let read_bytes = buffer.len();
        self.recv.read_exact(buffer).await.map_err(|error| {
            error!("Failed to read from the stream: {:?}", error);
            IggyError::QuicError
        })?;
        */

        //Ok(read_bytes)
        todo!();
    }

    async fn send_empty_ok_response(&mut self) -> Result<(), IggyError> {
        self.send_ok_response(&[]).await
    }

    async fn send_ok_response(&mut self, payload: &[u8]) -> Result<(), IggyError> {
        self.send_response(STATUS_OK, payload).await
    }

    async fn send_error_response(&mut self, error: IggyError) -> Result<(), IggyError> {
        self.send_response(&error.as_code().to_le_bytes(), &[])
            .await
    }

    async fn shutdown(&mut self) -> Result<(), ServerError> {
        Ok(())
    }

    async fn send_ok_response_vectored(
        &mut self,
        length: &[u8],
        slices: Vec<libc::iovec>,
    ) -> Result<(), IggyError> {
        debug!("Sending vectored response with status: {:?}...", STATUS_OK);

        let headers = [STATUS_OK, length].concat();
        self.send
            .write_all(&headers)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to write headers to stream")
            })
            .map_err(|_| IggyError::QuicError)?;

        let mut total_bytes_written = 0;

        //TODO: Fixme
        /*
        for slice in slices {
            let slice_data = &*slice;
            if !slice_data.is_empty() {
                self.send
                    .write_all(slice_data)
                    .await
                    .with_error_context(|error| {
                        format!("{COMPONENT} (error: {error}) - failed to write slice to stream")
                    })
                    .map_err(|_| IggyError::QuicError)?;

                total_bytes_written += slice_data.len();
            }
        }
        */

        debug!(
            "Sent vectored response: {} bytes of payload",
            total_bytes_written
        );

        self.send
            .finish()
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to finish send stream")
            })
            .map_err(|_| IggyError::QuicError)?;

        debug!("Sent vectored response with status: {:?}", STATUS_OK);
        Ok(())
    }
}

impl QuicSender {
    async fn send_response(&mut self, status: &[u8], payload: &[u8]) -> Result<(), IggyError> {
        debug!(
            "Sending response of len: {} with status: {:?}...",
            payload.len(),
            status
        );
        let length = (payload.len() as u32).to_le_bytes();
        self.send
            .write_all(&[status, &length, payload].as_slice().concat())
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to write buffer to the stream")
            })
            .map_err(|_| IggyError::QuicError)?;
        self.send
            .finish()
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to finish send stream")
            })
            .map_err(|_| IggyError::QuicError)?;
        debug!("Sent response with status: {:?}", status);
        Ok(())
    }
}
