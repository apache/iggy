use crate::binary::sender::Sender;
use crate::server_error::ServerError;
use crate::streaming::utils::PooledBuffer;
use crate::websocket::COMPONENT;
use bytes::{BufMut, BytesMut};
use compio::buf::IoBufMut;
use compio::net::TcpStream;
use compio_ws::{WebSocketMessage as Message, WebSocketStream};
use error_set::ErrContext;
use iggy_common::IggyError;
use std::ptr;
use tracing::debug;

const READ_BUFFER_CAPACITY: usize = 8192;
const WRITE_BUFFER_CAPACITY: usize = 8192;
const STATUS_OK: &[u8] = &[0; 4];

pub struct WebSocketSender {
    pub(crate) stream: WebSocketStream<TcpStream>,
    pub(crate) read_buffer: BytesMut,
    pub(crate) write_buffer: BytesMut,
}

impl WebSocketSender {
    pub fn new(stream: WebSocketStream<TcpStream>) -> Self {
        Self {
            stream,
            read_buffer: BytesMut::with_capacity(READ_BUFFER_CAPACITY),
            write_buffer: BytesMut::with_capacity(WRITE_BUFFER_CAPACITY),
        }
    }

    async fn flush_write_buffer(&mut self) -> Result<(), IggyError> {
        if self.write_buffer.is_empty() {
            return Ok(());
        }
        let data = self.write_buffer.split().freeze();
        self.stream
            .send(Message::Binary(data))
            .await
            .map_err(|_| IggyError::TcpError)
    }
}

impl Sender for WebSocketSender {
    async fn read<B: IoBufMut>(&mut self, mut buffer: B) -> (Result<(), IggyError>, B) {
        let required_len = buffer.buf_capacity();
        if required_len == 0 {
            return (Ok(()), buffer);
        }

        while self.read_buffer.len() < required_len {
            match self.stream.read().await {
                Ok(Message::Binary(data)) => {
                    self.read_buffer.extend_from_slice(&data);
                }
                Ok(Message::Close(_)) => {
                    return (Err(IggyError::ConnectionClosed), buffer);
                }
                Ok(Message::Ping(data)) => {
                    if self.stream.send(Message::Pong(data)).await.is_err() {
                        return (Err(IggyError::ConnectionClosed), buffer);
                    }
                }
                Ok(_) => { /* Ignore other message types */ }
                Err(_) => {
                    return (Err(IggyError::ConnectionClosed), buffer);
                }
            }
        }

        let data_to_copy = self.read_buffer.split_to(required_len);

        unsafe {
            ptr::copy_nonoverlapping(data_to_copy.as_ptr(), buffer.as_buf_mut_ptr(), required_len);
            buffer.set_buf_init(required_len);
        }

        (Ok(()), buffer)
    }

    async fn send_empty_ok_response(&mut self) -> Result<(), IggyError> {
        self.send_ok_response(&[]).await
    }

    async fn send_ok_response(&mut self, payload: &[u8]) -> Result<(), IggyError> {
        debug!(
            "Sending WebSocket response with status: OK, payload length: {}",
            payload.len()
        );

        let length = (payload.len() as u32).to_le_bytes();
        let total_size = STATUS_OK.len() + length.len() + payload.len();

        if self.write_buffer.len() + total_size > self.write_buffer.capacity() {
            self.flush_write_buffer().await?;
        }

        self.write_buffer.put_slice(STATUS_OK);
        self.write_buffer.put_slice(&length);
        self.write_buffer.put_slice(payload);

        self.flush_write_buffer().await
    }

    async fn send_error_response(&mut self, error: IggyError) -> Result<(), IggyError> {
        let status = &error.as_code().to_le_bytes();
        debug!("Sending WebSocket error response with status: {:?}", status);
        let length = 0u32.to_le_bytes();
        let total_size = status.len() + length.len();

        if self.write_buffer.len() + total_size > self.write_buffer.capacity() {
            self.flush_write_buffer().await?;
        }
        self.write_buffer.put_slice(status);
        self.write_buffer.put_slice(&length);
        self.flush_write_buffer().await
    }

    async fn shutdown(&mut self) -> Result<(), ServerError> {
        self.flush_write_buffer().await.map_err(ServerError::from)?;
        self.stream
            .close(None)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to shutdown WebSocket stream")
            })
            .map_err(|e| {
                ServerError::from(IggyError::CannotCloseWebSocketConnection(e.to_string()))
            })
    }

    async fn send_ok_response_vectored(
        &mut self,
        length: &[u8],
        slices: Vec<PooledBuffer>,
    ) -> Result<(), IggyError> {
        self.flush_write_buffer().await?;

        let total_payload_size = slices.iter().map(|s| s.len()).sum::<usize>();
        let total_size = STATUS_OK.len() + length.len() + total_payload_size;

        let mut response_bytes = BytesMut::with_capacity(total_size);
        response_bytes.put_slice(STATUS_OK);
        response_bytes.put_slice(length);
        for slice in slices {
            response_bytes.put_slice(&slice);
        }

        self.stream
            .send(Message::Binary(response_bytes.freeze()))
            .await
            .map_err(|_| IggyError::TcpError)
    }
}
