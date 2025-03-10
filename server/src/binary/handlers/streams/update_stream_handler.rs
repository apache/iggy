use crate::binary::{handlers::streams::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::streams::update_stream::UpdateStream;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_update_stream", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id, iggy_stream_id = command.stream_id.as_string()))]
pub async fn handle(
    command: UpdateStream,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let stream_id = command.stream_id.clone();

    let mut system = system.write().await;
    system.update_stream(session, &command.stream_id, &command.name)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to update stream with id: {stream_id}, session: {session}")
            })?;

    let system = system.downgrade();
    system
        .state
        .apply(session.get_user_id(), EntryCommand::UpdateStream(command))
        .await
        .with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to apply update stream with id: {stream_id}, session: {session}")
        })?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
