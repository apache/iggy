use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::streams::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::streams::get_streams::GetStreams;
use tracing::debug;

impl ServerCommandHandler for GetStreams {
    fn code(&self) -> u32 {
        iggy::command::GET_STREAMS_CODE
    }

    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let system = system.read().await;
        let streams = system.find_streams(session).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to find streams for session: {session}")
        })?;
        let response = mapper::map_streams(&streams);
        sender.send_ok_response(&response).await?;
        Ok(())
    }
}

impl BinaryServerCommand for GetStreams {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError> {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetStreams(get_streams) => Ok(get_streams),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
