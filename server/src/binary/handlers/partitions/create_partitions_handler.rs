use crate::binary::sender::Sender;
use crate::streaming::systems::system::System;
use anyhow::Result;
use iggy::error::Error;
use iggy::partitions::create_partitions::CreatePartitions;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::trace;

pub async fn handle(
    command: &CreatePartitions,
    sender: &mut dyn Sender,
    system: Arc<RwLock<System>>,
) -> Result<(), Error> {
    trace!("{}", command);
    let mut system = system.write().await;
    let topic = system
        .get_stream_mut(&command.stream_id)?
        .get_topic_mut(&command.topic_id)?;
    topic
        .add_persisted_partitions(command.partitions_count)
        .await?;
    topic.reassign_consumer_groups().await;
    sender.send_empty_ok_response().await?;
    Ok(())
}
