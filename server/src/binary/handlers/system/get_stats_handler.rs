use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::system::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::mapper;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::system::get_stats::GetStats;
use tracing::debug;

impl ServerCommandHandler for GetStats {
    fn code(&self) -> u32 {
        iggy::command::GET_STATS_CODE
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
        let stats = system.get_stats().await.with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get stats, session: {session}")
        })?;
        let bytes = mapper::map_stats(&stats);
        sender.send_ok_response(&bytes).await?;
        Ok(())
    }
}

impl BinaryServerCommand for GetStats {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        let server_command = receive_and_validate(sender, code, length).await?;
        match server_command {
            ServerCommand::GetStats(get_stats) => Ok(get_stats),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
