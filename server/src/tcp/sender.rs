use std::io::IoSlice;

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

pub(crate) async fn send_ok_response_vectored<T>(
    stream: &mut T,
    length: &[u8],
    slices: Vec<IoSlice<'_>>,
) -> Result<(), IggyError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    send_response_vectored(stream, STATUS_OK, length, slices).await
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

pub(crate) async fn send_response_vectored<T>(
    stream: &mut T,
    status: &[u8],
    length: &[u8],
    mut slices: Vec<IoSlice<'_>>,
) -> Result<(), IggyError>
where
    T: AsyncReadExt + AsyncWriteExt + Unpin,
{
    debug!("Sending response with status: {:?}...", status);
    let prefix = [IoSlice::new(status), IoSlice::new(length)];
    slices.splice(0..0, prefix);
    stream
        .write_vectored(&slices)
        .await
        .map_err(|_| IggyError::TcpError)?;
    debug!("Sent response with status: {:?}", status);
    Ok(())
}
