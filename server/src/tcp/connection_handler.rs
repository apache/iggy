use crate::binary::{command, sender::SenderKind};
use crate::command::ServerCommand;
use crate::server_error::ConnectionError;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use bytes::{BufMut, BytesMut};
use error_set::ErrContext;
use iggy::bytes_serializable::BytesSerializable;
use iggy::command::SEND_MESSAGES_CODE;
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::messages::send_messages::Partitioning;
use iggy::models::batch::{IggyBatch, IggyHeader, IggyMutableBatch, IGGY_BATCH_OVERHEAD};
use iggy::utils::sizeable::Sizeable;
use iggy::validatable::Validatable;
use std::io::ErrorKind;
use std::sync::Arc;
use tracing::{debug, error, info};

const INITIAL_BYTES_LENGTH: usize = 4;

pub(crate) async fn handle_connection(
    session: Arc<Session>,
    sender: &mut SenderKind,
    system: SharedSystem,
) -> Result<(), ConnectionError> {
    let mut initial_buffer = [0u8; INITIAL_BYTES_LENGTH];
    let mut code_buffer = [0u8; INITIAL_BYTES_LENGTH];
    loop {
        let read_length = match sender.read(&mut initial_buffer).await {
            Ok(read_length) => read_length,
            Err(error) => {
                if error.as_code() == IggyError::ConnectionClosed.as_code() {
                    return Err(ConnectionError::from(error));
                } else {
                    sender.send_error_response(error).await?;
                    continue;
                }
            }
        };

        if read_length != INITIAL_BYTES_LENGTH {
            sender.send_error_response(IggyError::CommandLengthError(format!(
                "Unable to read the TCP request length, expected: {INITIAL_BYTES_LENGTH} bytes, received: {read_length} bytes."
            ))).await?;
            continue;
        }

        let length = u32::from_le_bytes(initial_buffer) - 4;
        sender.read(&mut code_buffer).await?;
        let code = u32::from_le_bytes(code_buffer);
        // TODO: Fix me - Refactor the way the command is structured
        // `ServerCommnad` struct should have a method that takes the `sender`
        // and uses it to construct the command, rather than eagerly reading all of the data from the network.
        if code == SEND_MESSAGES_CODE {
            let mut metadata_len = [0u8; 4];
            sender.read(&mut metadata_len).await?;
            let metadata_len = u32::from_le_bytes(metadata_len);
            let mut metadata_buf = BytesMut::with_capacity(metadata_len as _);
            unsafe { metadata_buf.set_len(metadata_len as _) };
            sender.read(&mut metadata_buf).await?;
            let metadata_buf = metadata_buf.freeze();

            // TODO: This is disgusting, fuck `Bytes`.
            let stream_id = Identifier::from_bytes(metadata_buf.clone())?;
            let mut position = stream_id.get_size_bytes().as_bytes_usize();
            let topic_id = Identifier::from_bytes(metadata_buf.slice(position..))?;
            position += topic_id.get_size_bytes().as_bytes_usize();
            let partitioning = Partitioning::from_bytes(metadata_buf.slice(position..))?;

            let mut header_buffer = [0u8; IGGY_BATCH_OVERHEAD as _];
            sender.read(&mut header_buffer).await?;
            let header = IggyHeader::from_bytes(&header_buffer);

            let batch_length = length - metadata_len - IGGY_BATCH_OVERHEAD as u32 - 4;
            let mut batch_buffer = BytesMut::with_capacity(batch_length as _);
            unsafe { batch_buffer.set_len(batch_length as _) };
            sender.read(&mut batch_buffer).await?;
            let batch = IggyMutableBatch::new(header, batch_buffer);
            let system = system.read().await;
            {
                system
                .append_messages(&session, &stream_id, &topic_id, &partitioning, batch, None)
                .await
                .with_error_context(|error| {
                    format!(
                    "Tcp connection handler - (error: {error}) - failed to append messages for stream_id: {}, topic_id: {}, partitioning: {}, session: {}",
                     stream_id, topic_id, partitioning, session
                    )
                })?;
            }
            sender.send_empty_ok_response().await?;
        } else {
            debug!("Received a TCP request, length: {length}");
            let mut command_buffer = BytesMut::with_capacity(length as usize);
            command_buffer.put_bytes(0, length as usize);
            if length > 0 {
                sender.read(&mut command_buffer).await?;
            }
            let command = ServerCommand::from_code_and_payload(code, command_buffer.freeze());
            if command.is_err() {
                sender
                    .send_error_response(IggyError::InvalidCommand)
                    .await?;
                continue;
            }
            let command = command?;
            if let Err(error) = command.validate() {
                error!("Command validation failed: {error}");
                sender.send_error_response(error).await?;
                continue;
            }

            debug!("Received a TCP command: {command}, payload size: {length}");
            command::handle(command, sender, &session, system.clone()).await?;
        }
    }
}

pub(crate) fn handle_error(error: ConnectionError) {
    match error {
        ConnectionError::IoError(error) => match error.kind() {
            ErrorKind::UnexpectedEof => {
                info!("Connection has been closed.");
            }
            ErrorKind::ConnectionAborted => {
                info!("Connection has been aborted.");
            }
            ErrorKind::ConnectionRefused => {
                info!("Connection has been refused.");
            }
            ErrorKind::ConnectionReset => {
                info!("Connection has been reset.");
            }
            _ => {
                error!("Connection has failed: {error}");
            }
        },
        ConnectionError::SdkError(sdk_error) => match sdk_error {
            IggyError::ConnectionClosed => {
                debug!("Client closed connection.");
            }
            _ => {
                error!("Failure in internal SDK call: {sdk_error}");
            }
        },
        _ => {
            error!("Connection has failed: {error}");
        }
    }
}
