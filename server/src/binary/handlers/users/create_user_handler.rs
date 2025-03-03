use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::{handlers::users::COMPONENT, sender::SenderKind};
use crate::state::command::EntryCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use crate::streaming::utils::crypto;
use anyhow::Result;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::users::create_user::CreateUser;
use tracing::{debug, instrument};

impl ServerCommandHandler for CreateUser {
    fn code(&self) -> u32 {
        iggy::command::CREATE_USER_CODE
    }

    #[instrument(skip_all, name = "trace_create_user", fields(iggy_user_id = session.get_user_id(), iggy_client_id = session.client_id))]
    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        let mut system = system.write().await;
        let user = system
                .create_user(
                    session,
                    &self.username,
                    &self.password,
                    self.status,
                    self.permissions.clone(),
                )
                .await
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to create user with name: {}, session: {session}",
                        self.username
                    )
                })?;
        let response = mapper::map_user(user);

        // For the security of the system, we hash the password before storing it in metadata.
        let system = system.downgrade();
        system
            .state
            .apply(
                session.get_user_id(),
                &EntryCommand::CreateUser(CreateUser {
                    username: self.username.to_owned(),
                    password: crypto::hash_password(&self.password),
                    status: self.status,
                    permissions: self.permissions.clone(),
                }),
            )
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to apply create user with name: {}, session: {session}",
                    self.username
                )
            })?;
        sender.send_ok_response(&response).await?;
        Ok(())
    }
}

impl BinaryServerCommand for CreateUser {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::CreateUser(create_user) => Ok(create_user),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
