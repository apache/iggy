use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use iggy::error::IggyError;
use iggy::locking::IggySharedMutFn;
use iggy::system::get_client::GetClient;
use tracing::debug;

impl ServerCommandHandler for GetClient {
    fn code(&self) -> u32 {
        iggy::command::GET_CLIENT_CODE
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
        let Ok(client) = system.get_client(session, self.client_id).await else {
            sender.send_empty_ok_response().await?;
            return Ok(());
        };

        let Some(client) = client else {
            sender.send_empty_ok_response().await?;
            return Ok(());
        };

        let client = client.read().await;
        let bytes = mapper::map_client(&client);

        sender.send_ok_response(&bytes).await?;
        Ok(())
    }
}

impl BinaryServerCommand for GetClient {
    async fn from_sender(sender: &mut SenderKind, length: u32, code: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetClient(get_client) => Ok(get_client),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
