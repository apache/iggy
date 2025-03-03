use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::consumer_groups::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::consumer_groups::get_consumer_groups::GetConsumerGroups;
use iggy::error::IggyError;
use tracing::debug;

impl ServerCommandHandler for GetConsumerGroups {
    fn code(&self) -> u32 {
        iggy::command::GET_CONSUMER_GROUPS_CODE
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
        let consumer_groups = system
            .get_consumer_groups(session, &self.stream_id, &self.topic_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed on getting consumer groups for stream_id: {}, topic_id: {}, session: {}",
                    self.stream_id, self.topic_id, session
                )
            })?;
        let consumer_groups = mapper::map_consumer_groups(&consumer_groups).await;
        sender.send_ok_response(&consumer_groups).await?;
        Ok(())
    }
}

impl BinaryServerCommand for GetConsumerGroups {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetConsumerGroups(get_consumer_groups) => Ok(get_consumer_groups),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
