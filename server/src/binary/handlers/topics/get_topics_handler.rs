use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::topics::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::topics::get_topics::GetTopics;
use tracing::debug;

impl ServerCommandHandler for GetTopics {
    fn code(&self) -> u32 {
        iggy::command::GET_TOPICS_CODE
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
        let topics = system
            .find_topics(session, &self.stream_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to find topics, stream_id: {}, session: {session}",
                    self.stream_id
                )
            })?;
        let response = mapper::map_topics(&topics);
        sender.send_ok_response(&response).await?;
        Ok(())
    }
}

impl BinaryServerCommand for GetTopics {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetTopics(get_topics) => Ok(get_topics),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
