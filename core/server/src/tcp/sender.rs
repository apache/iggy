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

use bytes::BytesMut;
use iggy_common::IggyError;
use monoio::{
    buf::IoBufMut,
    io::{AsyncReadRent, AsyncReadRentExt, AsyncWriteRent, AsyncWriteRentExt},
};
use nix::libc;
use std::io::IoSlice;
use tracing::debug;

const STATUS_OK: &[u8] = &[0; 4];

pub(crate) async fn read<T>(
    stream: &mut T,
    buffer: BytesMut,
) -> (Result<usize, IggyError>, BytesMut)
where
    T: AsyncReadRent + AsyncWriteRent + Unpin,
{
    match stream.read_exact(buffer).await {
        (Ok(0), buffer) => (Err(IggyError::ConnectionClosed), buffer),
        (Ok(read_bytes), buffer) => (Ok(read_bytes), buffer),
        (Err(error), buffer) => {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                (Err(IggyError::ConnectionClosed), buffer)
            } else {
                (Err(IggyError::TcpError), buffer)
            }
        }
    }
}

pub(crate) async fn send_empty_ok_response<T>(stream: &mut T) -> Result<(), IggyError>
where
    T: AsyncReadRent + AsyncWriteRent + Unpin,
{
    send_ok_response(stream, &[]).await
}

pub(crate) async fn send_ok_response<T>(stream: &mut T, payload: &[u8]) -> Result<(), IggyError>
where
    T: AsyncReadRent + AsyncWriteRent + Unpin,
{
    send_response(stream, STATUS_OK, payload).await
}

pub(crate) async fn send_ok_response_vectored<T>(
    stream: &mut T,
    length: &[u8],
    slices: Vec<libc::iovec>,
) -> Result<(), IggyError>
where
    T: AsyncReadRentExt + AsyncWriteRentExt + Unpin,
{
    send_response_vectored(stream, STATUS_OK, length, slices).await
}

pub(crate) async fn send_error_response<T>(
    stream: &mut T,
    error: IggyError,
) -> Result<(), IggyError>
where
    T: AsyncReadRent + AsyncWriteRent + Unpin,
{
    send_response(stream, &error.as_code().to_le_bytes(), &[]).await
}

pub(crate) async fn send_response<T>(
    stream: &mut T,
    status: &[u8],
    payload: &[u8],
) -> Result<(), IggyError>
where
    T: AsyncReadRent + AsyncWriteRent + Unpin,
{
    debug!(
        "Sending response of len: {} with status: {:?}...",
        payload.len(),
        status
    );
    let length = (payload.len() as u32).to_le_bytes();
    stream
        .write_all([status, &length, payload].concat())
        .await
        .0
        .map_err(|_| IggyError::TcpError)?;
    debug!("Sent response with status: {:?}", status);
    Ok(())
}

pub(crate) async fn send_response_vectored<T>(
    stream: &mut T,
    status: &[u8],
    length: &[u8],
    mut slices: Vec<libc::iovec>,
) -> Result<(), IggyError>
where
    T: AsyncReadRentExt + AsyncWriteRentExt + Unpin,
{
    debug!(
        "Sending vectored response of len: {} with status: {:?}...",
        slices.len(),
        status
    );
    let prefix = [
        libc::iovec {
            iov_base: length.as_ptr() as _,
            iov_len: length.len(),
        },
        libc::iovec {
            iov_base: status.as_ptr() as _,
            iov_len: status.len(),
        },
    ];
    slices.splice(0..0, prefix);
    stream
        .write_vectored_all(slices)
        .await
        .0
        .map_err(|_| IggyError::TcpError)?;
    debug!("Sent response with status: {:?}", status);
    Ok(())
}
