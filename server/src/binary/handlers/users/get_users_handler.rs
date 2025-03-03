use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::users::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::users::get_users::GetUsers;
use tracing::debug;

impl ServerCommandHandler for GetUsers {
    fn code(&self) -> u32 {
        iggy::command::GET_USERS_CODE
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
        let users = system
            .get_users(session)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get users, session: {session}")
            })?;
        let users = mapper::map_users(&users);
        sender.send_ok_response(&users).await?;
        Ok(())
    }
}

impl BinaryServerCommand for GetUsers {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetUsers(get_users) => Ok(get_users),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
