// use crate::binary::sender::Sender;
use crate::quic::COMPONENT;
use error_set::ErrContext;
use iggy::error::IggyError;
use quinn::{RecvStream, SendStream};
use tracing::{debug, error};

const STATUS_OK: &[u8] = &[0; 4];

#[derive(Debug)]
pub struct QuicSender {
    pub(crate) send: SendStream,
    pub(crate) recv: RecvStream,
}

impl QuicSender {
    pub async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, IggyError> {
        let read_bytes = self.recv.read(buffer).await.map_err(|error| {
            error!("Failed to read from the stream: {:?}", error);
            IggyError::QuicError
        })?;

        Ok(read_bytes.ok_or(IggyError::QuicError)?)
    }

    pub async fn send_empty_ok_response(&mut self) -> Result<(), IggyError> {
        self.send_ok_response(&[]).await
    }

    pub async fn send_ok_response(&mut self, payload: &[u8]) -> Result<(), IggyError> {
        self.send_response(STATUS_OK, payload).await
    }

    pub async fn send_error_response(&mut self, error: IggyError) -> Result<(), IggyError> {
        self.send_response(&error.as_code().to_le_bytes(), &[])
            .await
    }
}

impl QuicSender {
    async fn send_response(&mut self, status: &[u8], payload: &[u8]) -> Result<(), IggyError> {
        debug!("Sending response with status: {:?}...", status);
        let length = (payload.len() as u32).to_le_bytes();
        self.send
            .write_all(&[status, &length, payload].as_slice().concat())
            .await
            .with_error_context(|_| format!("{COMPONENT} - failed to write buffer to the stream"))
            .map_err(|_| IggyError::QuicError)?;
        self.send
            .finish()
            .with_error_context(|_| format!("{COMPONENT} - failed to finish send stream"))
            .map_err(|_| IggyError::QuicError)?;
        debug!("Sent response with status: {:?}", status);
        Ok(())
    }
}
