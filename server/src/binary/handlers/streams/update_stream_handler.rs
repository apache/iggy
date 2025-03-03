use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::{handlers::streams::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::streams::update_stream::UpdateStream;
use tracing::{debug, instrument};

impl ServerCommandHandler for UpdateStream {
    fn code(&self) -> u32 {
        iggy::command::UPDATE_STREAM_CODE
    }

    #[instrument(skip_all, name = "trace_update_stream", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string()))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let stream_id = self.stream_id.clone();

        let mut system = system.write().await;
        system
                .update_stream(session, &self.stream_id, &self.name)
                .await
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to update stream with id: {stream_id}, session: {session}")
                })?;

        let system = system.downgrade();
        system
            .state
            .apply(session.get_user_id(), &EntryCommand::UpdateStream(self))
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to apply update stream with id: {stream_id}, session: {session}")
            })?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for UpdateStream {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError> {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::UpdateStream(update_stream) => Ok(update_stream),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
