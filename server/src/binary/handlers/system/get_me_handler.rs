use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::system::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::locking::IggySharedMutFn;
use iggy::system::get_me::GetMe;
use tracing::debug;

impl ServerCommandHandler for GetMe {
    fn code(&self) -> u32 {
        iggy::command::GET_ME_CODE
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
        let Some(client) = system
            .get_client(session, session.client_id)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get current client for session: {session}")
            })?
        else {
            return Err(IggyError::ClientNotFound(session.client_id));
        };

        let client = client.read().await;
        let bytes = mapper::map_client(&client);

        sender.send_ok_response(&bytes).await?;
        Ok(())
    }
}

impl BinaryServerCommand for GetMe {
    async fn from_sender(sender: &mut SenderKind, length: u32, code: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetMe(get_me) => Ok(get_me),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
