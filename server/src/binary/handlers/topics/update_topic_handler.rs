use crate::binary::sender::Sender;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy::error::IggyError;
use iggy::topics::update_topic::UpdateTopic;
use tracing::debug;

pub async fn handle(
    command: &UpdateTopic,
    sender: &mut dyn Sender,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("session: {session}, command: {command}");
    let mut system = system.write();
    system
        .update_topic(
            session,
            &command.stream_id,
            &command.topic_id,
            &command.name,
            command.message_expiry,
            command.compression_algorithm,
            command.max_topic_size,
            command.replication_factor,
        )
        .await?;
    sender.send_empty_ok_response().await?;
    Ok(())
}
