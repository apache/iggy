use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::personal_access_tokens::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::personal_access_tokens::get_personal_access_tokens::GetPersonalAccessTokens;
use tracing::debug;

impl ServerCommandHandler for GetPersonalAccessTokens {
    fn code(&self) -> u32 {
        iggy::command::GET_PERSONAL_ACCESS_TOKENS_CODE
    }

    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let system = system.read().await;
        let personal_access_tokens = system
            .get_personal_access_tokens(session)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get personal access tokens with session: {session}")
            })?;
        let personal_access_tokens = mapper::map_personal_access_tokens(&personal_access_tokens);
        sender.send_ok_response(&personal_access_tokens).await?;
        Ok(())
    }
}

impl BinaryServerCommand for GetPersonalAccessTokens {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetPersonalAccessTokens(get_personal_access_tokens) => {
                Ok(get_personal_access_tokens)
            }
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
