use crate::binary::{handlers::users::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::users::update_user::UpdateUser;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_update_user", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle(
    command: UpdateUser,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");

    let mut system = system.write().await;
    system.update_user(
                session,
                &command.user_id,
                command.username.clone(),
                command.status,
            )
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to update user with user ID: {}, session: {session}",
                    command.user_id
                )
            })?;

    let system = system.downgrade();
    let user_id = command.user_id.clone();

    system
        .state
        .apply(session.get_user_id(), EntryCommand::UpdateUser(command))
        .await
        .with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to apply update user with user ID: {user_id}, session: {session}",
            )
        })?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
