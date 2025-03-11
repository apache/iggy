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

use iggy::error::IggyError;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::debug;

const STATUS_OK: &[u8] = &[0; 4];

pub(crate) async fn read<T>(stream: &mut T, buffer: &mut [u8]) -> Result<usize, IggyError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    match stream.read_exact(buffer).await {
        Ok(0) => Err(IggyError::ConnectionClosed),
        Ok(read_bytes) => Ok(read_bytes),
        Err(error) => {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                Err(IggyError::ConnectionClosed)
            } else {
                Err(IggyError::TcpError)
            }
        }
    }
}

pub(crate) async fn send_empty_ok_response<T>(stream: &mut T) -> Result<(), IggyError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    send_ok_response(stream, &[]).await
}

pub(crate) async fn send_ok_response<T>(stream: &mut T, payload: &[u8]) -> Result<(), IggyError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    send_response(stream, STATUS_OK, payload).await
}

pub(crate) async fn send_error_response<T>(
    stream: &mut T,
    error: IggyError,
) -> Result<(), IggyError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    send_response(stream, &error.as_code().to_le_bytes(), &[]).await
}

pub(crate) async fn send_response<T>(
    stream: &mut T,
    status: &[u8],
    payload: &[u8],
) -> Result<(), IggyError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    debug!("Sending response with status: {:?}...", status);
    let length = (payload.len() as u32).to_le_bytes();
    stream
        .write_all(&[status, &length, payload].as_slice().concat())
        .await
        .map_err(|_| IggyError::TcpError)?;
    debug!("Sent response with status: {:?}", status);
    Ok(())
}
