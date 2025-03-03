use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::{handlers::users::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use crate::streaming::utils::crypto;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::users::change_password::ChangePassword;
use tracing::{debug, instrument};

impl ServerCommandHandler for ChangePassword {
    fn code(&self) -> u32 {
        iggy::command::CHANGE_PASSWORD_CODE
    }

    #[instrument(skip_all, name = "trace_change_password", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
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
                .change_password(
                    session,
                    &self.user_id,
                    &self.current_password,
                    &self.new_password,
                )
                .await
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to change password for user_id: {}, session: {session}",
                        self.user_id
                    )
                })?;

        // For the security of the system, we hash the password before storing it in metadata.
        let system = system.downgrade();
        system
            .state
            .apply(
                session.get_user_id(),
                &EntryCommand::ChangePassword(ChangePassword {
                    user_id: self.user_id.to_owned(),
                    current_password: "".into(),
                    new_password: crypto::hash_password(&self.new_password),
                }),
            )
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to apply change password for user_id: {}, session: {session}",
                    self.user_id
                )
            })?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for ChangePassword {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::ChangePassword(change_password) => Ok(change_password),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
