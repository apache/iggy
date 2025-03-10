use crate::binary::{handlers::consumer_groups::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use iggy::error::IggyError;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_delete_consumer_group", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = command.stream_id.as_string(), iggy_topic_id = command.topic_id.as_string()))]
pub async fn handle(
    command: DeleteConsumerGroup,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");

    let mut system = system.write().await;
    system.delete_consumer_group(
                session,
                &command.stream_id,
                &command.topic_id,
                &command.group_id,
            )
            .await.with_error_context(|error| format!(
                "{COMPONENT} (error: {error}) - failed to delete consumer group with ID: {} for topic with ID: {} in stream with ID: {} for session: {}",
                command.group_id, command.topic_id, command.stream_id, session
            ))?;

    let system = system.downgrade();
    let stream_id = command.stream_id.clone();
    let topic_id = command.topic_id.clone();
    let group_id = command.group_id.clone();

    system
        .state
        .apply(
            session.get_user_id(),
            EntryCommand::DeleteConsumerGroup(command),
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply delete consumer group for stream ID: {}, topic ID: {}, group_id: {:?}, session: {}",
                stream_id, topic_id, group_id, session
            )
        })?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
