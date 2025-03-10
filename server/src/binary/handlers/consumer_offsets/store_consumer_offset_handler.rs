use crate::binary::handlers::consumer_offsets::COMPONENT;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use iggy::error::IggyError;
use tracing::debug;

pub async fn handle(
    command: StoreConsumerOffset,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let system = system.read().await;
    system.store_consumer_offset(
            session,
            command.consumer,
            &command.stream_id,
            &command.topic_id,
            command.partition_id,
            command.offset,
        )
        .await
        .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to store consumer offset for stream ID: {}, topic ID: {}, partition_id: {:?}, offset: {}, session: {}",
            command.stream_id, command.topic_id, command.partition_id, command.offset, session
        ))?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
