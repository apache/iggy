use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::consumer_offsets::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use iggy::error::IggyError;
use tracing::debug;

impl ServerCommandHandler for StoreConsumerOffset {
    fn code(&self) -> u32 {
        iggy::command::STORE_CONSUMER_OFFSET_CODE
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
            .store_consumer_offset(
                session,
                self.consumer,
                &self.stream_id,
                &self.topic_id,
                self.partition_id,
                self.offset,
            )
            .await
            .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to store consumer offset for stream_id: {}, topic_id: {}, partition_id: {:?}, offset: {}, session: {}",
                self.stream_id, self.topic_id, self.partition_id, self.offset, session
            ))?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for StoreConsumerOffset {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::StoreConsumerOffset(store_consumer_offset) => Ok(store_consumer_offset),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
