use crate::binary::{handlers::personal_access_tokens::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::personal_access_tokens::delete_personal_access_token::DeletePersonalAccessToken;
use tracing::{debug, instrument};

#[instrument(skip_all, name = "trace_delete_personal_access_token", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
pub async fn handle(
    command: DeletePersonalAccessToken,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let token_name = command.name.clone();

    let mut system = system.write().await;
    system.delete_personal_access_token(session, &command.name)
            .await
            .with_error_context(|error| {format!(
                "{COMPONENT} (error: {error}) - failed to delete personal access token with name: {token_name}, session: {session}"
            )})?;

    let system = system.downgrade();
    system
        .state
        .apply(
            session.get_user_id(),
            EntryCommand::DeletePersonalAccessToken(command),
        )
        .await
        .with_error_context(|error| {format!(
            "{COMPONENT} (error: {error}) - failed to apply delete personal access token with name: {token_name}, session: {session}"
        )})?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
