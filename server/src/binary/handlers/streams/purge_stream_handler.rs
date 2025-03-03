use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::{handlers::streams::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::streams::purge_stream::PurgeStream;
use tracing::{debug, instrument};

impl ServerCommandHandler for PurgeStream {
    fn code(&self) -> u32 {
        iggy::command::PURGE_STREAM_CODE
    }

    #[instrument(skip_all, name = "trace_purge_stream", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string()))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let system = system.read().await;
        let stream_id = self.stream_id.clone();

        system
            .purge_stream(session, &self.stream_id)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to purge stream with id: {stream_id}, session: {session}")
            })?;

        system
            .state
            .apply(session.get_user_id(), &EntryCommand::PurgeStream(self))
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to apply purge stream with id: {stream_id}, session: {session}")
            })?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for PurgeStream {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError> {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::PurgeStream(purge_stream) => Ok(purge_stream),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
