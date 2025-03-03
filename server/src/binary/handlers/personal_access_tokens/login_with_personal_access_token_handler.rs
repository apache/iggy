use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::{handlers::personal_access_tokens::COMPONENT, sender::SenderKind};
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::personal_access_tokens::login_with_personal_access_token::LoginWithPersonalAccessToken;
use tracing::{debug, instrument};

impl ServerCommandHandler for LoginWithPersonalAccessToken {
    fn code(&self) -> u32 {
        iggy::command::LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE
    }

    #[instrument(skip_all, name = "trace_login_with_personal_access_token", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
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
            .login_with_personal_access_token(&self.token, Some(session))
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to login with personal access token: {}, session: {session}",
                    self.token
                )
            })?;
        let identity_info = mapper::map_identity_info(user.id);
        sender.send_ok_response(&identity_info).await?;
        Ok(())
    }
}

impl BinaryServerCommand for LoginWithPersonalAccessToken {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::LoginWithPersonalAccessToken(login_with_personal_access_token) => {
                Ok(login_with_personal_access_token)
            }
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
