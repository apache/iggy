use crate::binary::sender::Sender;
use crate::tcp::COMPONENT;
use crate::{server_error::ServerError, tcp::sender};
use error_set::ErrContext;
use iggy::error::IggyError;
use tokio::{io::AsyncWriteExt, net::TcpStream};

#[derive(Debug)]
pub struct TcpSender {
    pub(crate) stream: TcpStream,
}

impl Sender for TcpSender {
    async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, IggyError> {
        sender::read(&mut self.stream, buffer).await
    }

    async fn send_empty_ok_response(&mut self) -> Result<(), IggyError> {
        sender::send_empty_ok_response(&mut self.stream).await
    }

    async fn send_ok_response(&mut self, payload: &[u8]) -> Result<(), IggyError> {
        sender::send_ok_response(&mut self.stream, payload).await
    }

    async fn send_error_response(&mut self, error: IggyError) -> Result<(), IggyError> {
        sender::send_error_response(&mut self.stream, error).await
    }

    async fn shutdown(&mut self) -> Result<(), ServerError> {
        self.stream
            .shutdown()
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to shutdown tcp stream")
            })
            .map_err(ServerError::IoError)
    }

    async fn send_ok_response_vectored(
        &mut self,
        length: &[u8],
        slices: Vec<std::io::IoSlice<'_>>,
    ) -> Result<(), IggyError> {
        sender::send_ok_response_vectored(&mut self.stream, length, slices).await
    }
}
