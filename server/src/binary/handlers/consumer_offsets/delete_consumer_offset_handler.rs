use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::consumer_offsets::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::consumer_offsets::delete_consumer_offset::DeleteConsumerOffset;
use iggy::error::IggyError;
use tracing::debug;

impl ServerCommandHandler for DeleteConsumerOffset {
    fn code(&self) -> u32 {
        iggy::command::DELETE_CONSUMER_OFFSET_CODE
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
        system
            .delete_consumer_offset(
                session,
                self.consumer,
                &self.stream_id,
                &self.topic_id,
                self.partition_id,
            )
            .await
            .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to delete consumer offset for topic with ID: {} in stream with ID: {} partition ID: {:#?}, session: {}",
                self.topic_id, self.stream_id, self.partition_id, session
            ))?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for DeleteConsumerOffset {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::DeleteConsumerOffset(delete_consumer_offset) => {
                Ok(delete_consumer_offset)
            }
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
