use crate::binary::handlers::consumer_offsets::COMPONENT;
use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::consumer_offsets::delete_consumer_offset::DeleteConsumerOffset;
use iggy::error::IggyError;
use tracing::debug;

pub async fn handle(
    command: DeleteConsumerOffset,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    system
        .delete_consumer_offset(
            session,
            command.consumer,
            &command.stream_id,
            &command.topic_id,
            command.partition_id,
        )
        .await
        .with_error_context(|_| format!("{COMPONENT} - failed to delete consumer offset for stream_id: {}, topic_id: {}, partition_id: {:?}, session: {}",
            command.stream_id, command.topic_id, command.partition_id, session
        ))?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
