use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::{handlers::consumer_groups::COMPONENT, sender::SenderKind};
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::consumer_groups::leave_consumer_group::LeaveConsumerGroup;
use iggy::error::IggyError;
use tracing::{debug, instrument};

impl ServerCommandHandler for LeaveConsumerGroup {
    fn code(&self) -> u32 {
        iggy::command::LEAVE_CONSUMER_GROUP_CODE
    }

    #[instrument(skip_all, name = "trace_leave_consumer_group", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string(), iggy_topic_id = self.topic_id.as_string(), iggy_group_id = self.group_id.as_string()))]
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
            .leave_consumer_group(
                session,
                &self.stream_id,
                &self.topic_id,
                &self.group_id,
            )
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to leave consumer group for stream_id: {}, topic_id: {}, group_id: {}, session: {}",
                    self.stream_id, self.topic_id, self.group_id, session
                )
            })?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for LeaveConsumerGroup {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::LeaveConsumerGroup(leave_consumer_group) => Ok(leave_consumer_group),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
