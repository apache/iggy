use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::{handlers::users::COMPONENT, sender::SenderKind};
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::users::logout_user::LogoutUser;
use tracing::{debug, instrument};

impl ServerCommandHandler for LogoutUser {
    fn code(&self) -> u32 {
        iggy::command::LOGOUT_USER_CODE
    }

    #[instrument(skip_all, name = "trace_logout_user", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let system = system.read().await;
        system
            .logout_user(session)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to logout user, session: {session}")
            })?;
        session.clear_user_id();
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for LogoutUser {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::LogoutUser(logout_user) => Ok(logout_user),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
