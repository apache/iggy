use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::system::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::system::get_clients::GetClients;
use tracing::debug;

impl ServerCommandHandler for GetClients {
    fn code(&self) -> u32 {
        iggy::command::GET_CLIENTS_CODE
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
        let clients = system
            .get_clients(session)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get clients, session: {session}")
            })?;
        let clients = mapper::map_clients(&clients).await;
        sender.send_ok_response(&clients).await?;
        Ok(())
    }
}

impl BinaryServerCommand for GetClients {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError> {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetClients(get_clients) => Ok(get_clients),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
