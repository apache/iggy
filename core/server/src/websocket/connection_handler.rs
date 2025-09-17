use crate::binary::command::ServerCommandHandler;
use crate::binary::{command, sender::SenderKind};
use crate::server_error::ConnectionError;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use crate::websocket::connection_handler::command::ServerCommand;
use async_channel::Receiver;
use bytes::BytesMut;
use futures::FutureExt;
use iggy_common::IggyError;
use std::io::ErrorKind;
use std::rc::Rc;
use tracing::{debug, error, info};

const INITIAL_BYTES_LENGTH: usize = 4;

pub(crate) async fn handle_connection(
    session: &Rc<Session>,
    sender: &mut SenderKind,
    shard: &Rc<IggyShard>,
    stop_receiver: Receiver<()>,
) -> Result<(), ConnectionError> {
    let mut length_buffer = BytesMut::with_capacity(INITIAL_BYTES_LENGTH);
    let mut code_buffer = BytesMut::with_capacity(INITIAL_BYTES_LENGTH);

    loop {
        let read_future = sender.read(length_buffer);
        let (_, mut initial_buffer) = futures::select! {
            _ = stop_receiver.recv().fuse() => {
                info!("Connection stop signal received for session: {}", session);
                let _ = sender.send_error_response(IggyError::Disconnected).await;
                return Ok(());
            }
            result = read_future.fuse() => {
                match result {
                    (Ok(read_length), initial_buffer) => (read_length, initial_buffer),
                    (Err(error), initial_buffer) => {
                        length_buffer = initial_buffer;
                        if error.as_code() == IggyError::ConnectionClosed.as_code() {
                            return Err(ConnectionError::from(error));
                        } else {
                            error!("got error: {:?}", error);
                            sender.send_error_response(error).await?;
                            continue;
                        }
                    }
                }
            }
        };

        let length =
            u32::from_le_bytes(initial_buffer[0..INITIAL_BYTES_LENGTH].try_into().unwrap());
        let (res, mut code_buffer_out) = sender.read(code_buffer).await;
        let _ = res?;
        let code: u32 =
            u32::from_le_bytes(code_buffer_out[0..INITIAL_BYTES_LENGTH].try_into().unwrap());

        initial_buffer.clear();
        code_buffer_out.clear();
        length_buffer = BytesMut::from(initial_buffer);
        code_buffer = BytesMut::from(code_buffer_out);

        debug!("Received a WebSocket request, length: {length}, code: {code}");
        let command = ServerCommand::from_code_and_reader(code, sender, length - 4).await?;
        debug!("Received a WebSocket command: {command}, payload size: {length}");

        match command.handle(sender, length, session, shard).await {
            Ok(_) => {
                debug!(
                    "Command was handled successfully, session: {session}. WebSocket response was sent."
                );
            }
            Err(error) => {
                error!("Command was not handled successfully, session: {session}, error: {error}.");
                if let IggyError::ClientNotFound(_) = error {
                    sender.send_error_response(error).await?;
                    debug!("WebSocket error response was sent to: {session}.");
                    error!("Session: {session} will be deleted.");
                    return Err(ConnectionError::from(IggyError::ClientNotFound(
                        session.client_id,
                    )));
                } else {
                    sender.send_error_response(error).await?;
                    debug!("WebSocket error response was sent to: {session}.");
                }
            }
        }
    }
}

pub(crate) fn handle_error(error: ConnectionError) {
    match error {
        ConnectionError::IoError(error) => match error.kind() {
            ErrorKind::UnexpectedEof => {
                info!("WebSocket connection has been closed.");
            }
            ErrorKind::ConnectionAborted => {
                info!("WebSocket connection has been aborted.");
            }
            ErrorKind::ConnectionRefused => {
                info!("WebSocket connection has been refused.");
            }
            ErrorKind::ConnectionReset => {
                info!("WebSocket connection has been reset.");
            }
            _ => {
                error!("WebSocket connection has failed: {error}");
            }
        },
        ConnectionError::SdkError(sdk_error) => match sdk_error {
            IggyError::ConnectionClosed => {
                debug!("Client closed WebSocket connection.");
            }
            _ => {
                error!("Failure in internal SDK call: {sdk_error}");
            }
        },
        _ => {
            error!("WebSocket connection has failed: {error}");
        }
    }
}
