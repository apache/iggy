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

use compio::{
    BufResult,
    buf::IoBufMut,
    io::{AsyncReadExt, AsyncWriteExt},
};
use iggy_common::IggyError;
use tracing::debug;

use crate::streaming::utils::PooledBuffer;

const STATUS_OK: &[u8] = &[0; 4];

pub(crate) async fn read<T, B>(stream: &mut T, buffer: B) -> (Result<(), IggyError>, B)
where
    T: AsyncReadExt + AsyncWriteExt + Unpin,
    B: IoBufMut,
{
    let BufResult(result, buffer) = stream.read_exact(buffer).await;
    match (result, buffer) {
        (Ok(_), buffer) => (Ok(()), buffer),
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
    T: AsyncReadExt + AsyncWriteExt + Unpin,
{
    send_ok_response(stream, &[]).await
}

pub(crate) async fn send_ok_response<T>(stream: &mut T, payload: &[u8]) -> Result<(), IggyError>
where
    T: AsyncReadExt + AsyncWriteExt + Unpin,
{
    send_response(stream, STATUS_OK, payload).await
}

pub(crate) async fn send_ok_response_vectored<T>(
    stream: &mut T,
    length: &[u8],
    slices: Vec<PooledBuffer>,
) -> Result<(), IggyError>
where
    T: AsyncReadExt + AsyncWriteExt + Unpin,
{
    send_response_vectored(stream, STATUS_OK, length, slices).await
}

pub(crate) async fn send_error_response<T>(
    stream: &mut T,
    error: IggyError,
) -> Result<(), IggyError>
where
    T: AsyncReadExt + AsyncWriteExt + Unpin,
{
    send_response(stream, &error.as_code().to_le_bytes(), &[]).await
}

pub(crate) async fn send_response<T>(
    stream: &mut T,
    status: &[u8],
    payload: &[u8],
) -> Result<(), IggyError>
where
    T: AsyncReadExt + AsyncWriteExt + Unpin,
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
    mut slices: Vec<PooledBuffer>,
) -> Result<(), IggyError>
where
    T: AsyncReadExt + AsyncWriteExt + Unpin,
{
    let resp_status = u32::from_le_bytes(status.try_into().unwrap());
    debug!(
        "Sending vectored response of len: {} with status: {:?}...",
        slices.len(),
        resp_status
    );
    let status = PooledBuffer::from(status);
    let length = PooledBuffer::from(length);
    slices.splice(0..0, [status, length]);
    stream
        .write_vectored_all(slices)
        .await
        .0
        .map_err(|_| IggyError::TcpError)?;
    debug!("Sent response with status: {:?}", resp_status);
    Ok(())
}
