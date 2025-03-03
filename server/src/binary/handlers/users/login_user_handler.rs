use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::{handlers::users::COMPONENT, sender::SenderKind};
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::users::login_user::LoginUser;
use tracing::{debug, instrument};

impl ServerCommandHandler for LoginUser {
    fn code(&self) -> u32 {
        iggy::command::LOGIN_USER_CODE
    }

    #[instrument(skip_all, name = "trace_login_user", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let system = system.read().await;
        let user = system
            .login_user(&self.username, &self.password, Some(session))
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to login user with name: {}, session: {session}",
                    self.username
                )
            })?;
        let identity_info = mapper::map_identity_info(user.id);
        sender.send_ok_response(&identity_info).await?;
        Ok(())
    }
}

impl BinaryServerCommand for LoginUser {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::LoginUser(login_user) => Ok(login_user),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
