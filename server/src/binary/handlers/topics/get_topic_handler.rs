use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::topics::get_topic::GetTopic;
use tracing::debug;

impl ServerCommandHandler for GetTopic {
    fn code(&self) -> u32 {
        iggy::command::GET_TOPIC_CODE
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
        let Ok(topic) = system.try_find_topic(session, &self.stream_id, &self.topic_id) else {
            sender.send_empty_ok_response().await?;
            return Ok(());
        };

        let Some(topic) = topic else {
            sender.send_empty_ok_response().await?;
            return Ok(());
        };

        let topic = mapper::map_topic(topic).await;
        sender.send_ok_response(&topic).await?;
        Ok(())
    }
}

impl BinaryServerCommand for GetTopic {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetTopic(get_topic) => Ok(get_topic),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
