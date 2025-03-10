use crate::binary::{handlers::users::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::users::update_permissions::UpdatePermissions;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_update_permissions", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle(
    command: UpdatePermissions,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");

    let mut system = system.write().await;
    system.update_permissions(session, &command.user_id, command.permissions.clone())
            .await
            .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to update permissions for user ID: {}, session: {session}",
                command.user_id
            ))?;

    let system = system.downgrade();
    system
        .state
        .apply(
            session.get_user_id(),
            EntryCommand::UpdatePermissions(command),
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
