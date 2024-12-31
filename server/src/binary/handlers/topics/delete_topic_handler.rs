use crate::binary::handlers::topics::COMPONENT;
use crate::binary::sender::Sender;
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::topics::delete_topic::DeleteTopic;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_delete_topic", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = command.stream_id.as_string(), iggy_topic_id = command.topic_id.as_string()))]
pub async fn handle(
    command: DeleteTopic,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let stream_id = command.stream_id.clone();
    let topic_id = command.topic_id.clone();

    {
        let mut system = system.write().await;
        system
            .delete_topic(session, &command.stream_id, &command.topic_id)
            .await
            .with_error_context(|_| format!(
                "{COMPONENT} - failed to delete topic for stream_id: {stream_id}, topic_id: {topic_id}",
            ))?;
    }

    let system = system.read().await;
    system
        .state
        .apply(session.get_user_id(), EntryCommand::DeleteTopic(command))
        .await
        .with_error_context(|_| format!(
            "{COMPONENT} - failed to apply delete topic for stream_id: {stream_id}, topic_id: {topic_id}",
        ))?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
