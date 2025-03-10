use crate::binary::mapper;
use crate::binary::{handlers::consumer_groups::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::state::models::CreateConsumerGroupWithId;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::error::IggyError;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_create_consumer_group", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = command.stream_id.as_string(), iggy_topic_id = command.topic_id.as_string()))]
pub async fn handle(
    command: CreateConsumerGroup,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");

    let mut system = system.write().await;
    let consumer_group = system
            .create_consumer_group(
                session,
                &command.stream_id,
                &command.topic_id,
                command.group_id,
                &command.name,
            )
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to create consumer group for stream ID: {}, topic ID: {}, group_id: {:?}, session: {:?}",
                    command.stream_id, command.topic_id, command.group_id, session
                )
            })?;
    let consumer_group = consumer_group.read().await;
    let group_id = consumer_group.group_id;
    let response = mapper::map_consumer_group(&consumer_group).await;
    drop(consumer_group);

    let system = system.downgrade();
    let stream_id = command.stream_id.clone();
    let topic_id = command.topic_id.clone();

    system
        .state
        .apply(
            session.get_user_id(),
            EntryCommand::CreateConsumerGroup(CreateConsumerGroupWithId {
                group_id,
                command
            }),
        )
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply create consumer group for stream with ID: {stream_id}, topic ID: {topic_id}, group ID: {group_id}, session: {session}",
            )
        })?;
    sender.send_ok_response(&response).await?;
    Ok(())
}
