use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::{handlers::partitions::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::partitions::create_partitions::CreatePartitions;
use tracing::{debug, instrument};

impl ServerCommandHandler for CreatePartitions {
    fn code(&self) -> u32 {
        iggy::command::CREATE_PARTITIONS_CODE
    }

    #[instrument(skip_all, name = "trace_create_partitions", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = self.stream_id.as_string(), iggy_topic_id = self.topic_id.as_string()))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        let mut system = system.write().await;
        system
            .create_partitions(
                session,
                &self.stream_id,
                &self.topic_id,
                self.partitions_count,
            )
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to create partitions for stream_id: {}, topic_id: {}, session: {}",
                    self.stream_id, self.topic_id, session
                )
            })?;

        let system = system.downgrade();
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();

        system
        .state
        .apply(
            session.get_user_id(),
            &EntryCommand::CreatePartitions(self),
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply create partitions for stream_id: {}, topic_id: {}, session: {}",
                stream_id, topic_id, session
            )
        })?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for CreatePartitions {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::CreatePartitions(create_partitions) => Ok(create_partitions),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
