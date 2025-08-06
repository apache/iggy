use super::COMPONENT;
use compio::fs::create_dir_all;
use iggy_common::IggyError;
use std::path::Path;

use crate::{configs::system::SystemConfig, shard_info, streaming::topics::topic2};

async fn create_topic_file_hierarchy(
    shard_id: u16,
    stream_id: usize,
    topic: &topic2::Topic,
    config: &SystemConfig,
) -> Result<(), IggyError> {
    let topic_path = config.get_topic_path(stream_id, topic.id());
    if !Path::new(&topic_path).exists() && create_dir_all(&topic_path).await.is_err() {
        return Err(IggyError::CannotCreateTopicDirectory(
            topic.id() as u32,
            stream_id as u32,
            topic_path,
        ));
    }
    shard_info!(
        shard_id,
        "Saved topic with ID: {}. for stream with ID: {}",
        topic.id(),
        stream_id
    );
    /*
    if !Path::new(&topic.partitions_path).exists()
        && create_dir_all(&topic.partitions_path).await.is_err()
    {
        return Err(IggyError::CannotCreatePartitionsDirectory(
            topic.stream_id,
            topic.topic_id,
        ));
    }

    shard_info!(shard_id,
        "Saving {} partition(s) for topic {topic}...",
        topic.partitions.len()
    );
    for (_, partition) in topic.partitions.iter() {
        let mut partition = partition.write().await;
        partition.persist().await.with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to persist partition, topic: {topic}"
            )
        })?;
    }

    shard_info!(shard_id, "Saved topic {topic}");
    */
    Ok(())
}
