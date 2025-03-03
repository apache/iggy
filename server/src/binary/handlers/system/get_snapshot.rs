use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use bytes::Bytes;
use iggy::error::IggyError;
use iggy::system::get_snapshot::GetSnapshot;
use tracing::debug;

impl ServerCommandHandler for GetSnapshot {
    fn code(&self) -> u32 {
        iggy::command::GET_SNAPSHOT_FILE_CODE
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
        let snapshot = system
            .get_snapshot(session, self.compression, &self.snapshot_types)
            .await?;
        let bytes = Bytes::copy_from_slice(&snapshot.0);
        sender.send_ok_response(&bytes).await?;
        Ok(())
    }
}

impl BinaryServerCommand for GetSnapshot {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::GetSnapshot(get_snapshot) => Ok(get_snapshot),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
