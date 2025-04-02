use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::{handlers::users::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::users::update_user::UpdateUser;
use tracing::{debug, instrument};

impl ServerCommandHandler for UpdateUser {
    fn code(&self) -> u32 {
        iggy::command::UPDATE_USER_CODE
    }

    #[instrument(skip_all, name = "trace_update_user", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
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
                .update_user(
                    session,
                    &self.user_id,
                    self.username.clone(),
                    self.status,
                )
                .await
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to update user with user_id: {}, session: {session}",
                        self.user_id
                    )
                })?;

        let system = system.downgrade();
        let user_id = self.user_id.clone();

        system
            .state
            .apply(session.get_user_id(), &EntryCommand::UpdateUser(self))
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to apply update user with user_id: {}, session: {session}",
                    user_id
                )
            })?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for UpdateUser {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::UpdateUser(update_user) => Ok(update_user),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
