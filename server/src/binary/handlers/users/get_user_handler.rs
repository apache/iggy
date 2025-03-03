use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use iggy::error::IggyError;
use iggy::users::get_user::GetUser;
use tracing::debug;

impl ServerCommandHandler for GetUser {
    fn code(&self) -> u32 {
        iggy::command::GET_USER_CODE
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
        let Ok(user) = system.find_user(session, &self.user_id) else {
            sender.send_empty_ok_response().await?;
            return Ok(());
        };
        let Some(user) = user else {
            sender.send_empty_ok_response().await?;
            return Ok(());
        };

        let bytes = mapper::map_user(user);
        sender.send_ok_response(&bytes).await?;
        Ok(())
    }
}

impl BinaryServerCommand for GetUser {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetUser(get_user) => Ok(get_user),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
